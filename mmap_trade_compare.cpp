// mmap_trade_compare.cpp
// Combined: mmap trade reader + exchange websocket trade recorder
// - Records trade events from mmap rings and exchange websocket into per-symbol CSVs
// - Supports Binance (per-symbol websocket) and Deribit (per-instrument JSON-RPC subscribe)
// - Supports optional HTTP proxy CONNECT (via http(s)_proxy env)
// - Interruptible/timed blocking connect and SSL handshake
// - Requires Boost (asio/beast), OpenSSL, nlohmann/json.hpp

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cctype>
#include <cstring>
#include <dirent.h>
#include <fcntl.h>
#include <fstream>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <nlohmann/json.hpp>
#include <set>
#include <sstream>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <vector>
#include <deque>
#include <signal.h>
#include <boost/system/error_code.hpp>
#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast.hpp>
#include <boost/beast/ssl.hpp>
#include <unordered_set>
#include <array>
#include <openssl/ssl.h>
#include <openssl/err.h>

using json = nlohmann::json;
namespace net = boost::asio;
namespace beast = boost::beast;
namespace websocket = beast::websocket;
using tcp = boost::asio::ip::tcp;
using namespace std::chrono_literals;

// ---- constants matching mmap layout ----
static const uint8_t META_MAGIC[] = {'R','I','N','G','V','1',0,0};
constexpr size_t META_HEADER_LEN = 40;
constexpr size_t INDEX_SLOT_SIZE = 24;
constexpr size_t RECORD_HEADER_LEN = 20; // u32 plen, u8 kind, 7 padding, u64 seq

static std::string now_iso_ms() {
    using namespace std::chrono;
    auto t = system_clock::now();
    auto ms = duration_cast<milliseconds>(t.time_since_epoch()) % 1000;
    std::time_t tt = system_clock::to_time_t(t);
    std::tm tm = *gmtime(&tt);
    char buf[64];
    snprintf(buf, sizeof(buf), "%04d-%02d-%02dT%02d:%02d:%02d.%03lldZ",
             tm.tm_year+1900, tm.tm_mon+1, tm.tm_mday,
             tm.tm_hour, tm.tm_min, tm.tm_sec,
             static_cast<long long>(ms.count()));
    return std::string(buf);
}

// ---- TradeEvent ----
struct TradeEvent {
    std::string symbol;
    double px = NAN;
    double sz = NAN;
    uint64_t recv_ts = 0;   // mmap timestamp if present
    uint64_t ws_ts = 0;     // ws timestamp if present
    uint64_t arrival_ms = 0;
    uint64_t seq = 0;       // mmap.seq or ws.trade id
    std::string prefix;     // ring prefix for mmap, "ws" for websocket
    bool is_ws = false;
};

// === CSVEngine (thread-safe) ===
// Records every trade event as a single CSV row; no compare rows.
class CSVEngine {
public:
    CSVEngine(const std::string &outdir,
              const std::unordered_set<std::string> &allowed_symbols,
              int flush_rows = 100, bool fsync=false, int match_ms=500, bool no_wait=false,
              bool ws_verbose=false)
        : outdir_(outdir), allowed_(allowed_symbols), flush_rows_(flush_rows),
          fsync_(fsync), match_ms_(match_ms), stop_(false),
          ws_seen_(false), mmap_seen_(false), no_wait_(no_wait), ws_verbose_(ws_verbose)
    {
        struct stat st;
        if (stat(outdir_.c_str(), &st) != 0) mkdir(outdir_.c_str(), 0755);
        worker_ = std::thread(&CSVEngine::loop, this);
        watchdog_ = std::thread(&CSVEngine::watchdog_loop, this);
    }

    ~CSVEngine() {
        stop_ = true;
        cv_.notify_all();
        if (worker_.joinable()) worker_.join();
        if (watchdog_.joinable()) watchdog_.join();
        std::lock_guard<std::mutex> lk(mu_);
        for (auto &p : ofs_) {
            try { p.second.flush(); p.second.close(); } catch(...) {}
        }
    }

    void push_event(const TradeEvent &ev) {
        TradeEvent e = ev;
        std::transform(e.symbol.begin(), e.symbol.end(), e.symbol.begin(), ::toupper);
        {
            std::lock_guard<std::mutex> lk(mu_);
            if (!writes_enabled() && !no_wait_) {
                prebuffer_.push_back(e);
                if (ws_verbose_) {
                    if (e.is_ws) std::cerr << "[engine] buffering (pre-enable) ws sym=" << e.symbol << " px=" << e.px << "\n";
                    else std::cerr << "[engine] buffering (pre-enable) mmap sym=" << e.symbol << " px=" << e.px << "\n";
                }
            } else {
                inbox_.push_back(e);
                cv_.notify_one();
            }
        }
    }

    void notify_ws_seen() {
        std::lock_guard<std::mutex> lk(mu_);
        if (!ws_seen_) {
            ws_seen_ = true;
            if (ws_verbose_) std::cerr << "[engine] notify_ws_seen()\n";
            try_enable_and_flush_locked();
        }
    }

    void notify_mmap_seen() {
        std::lock_guard<std::mutex> lk(mu_);
        if (!mmap_seen_) {
            mmap_seen_ = true;
            if (ws_verbose_) std::cerr << "[engine] notify_mmap_seen()\n";
            try_enable_and_flush_locked();
        }
    }

    bool writes_enabled() const { return ws_seen_ && mmap_seen_; }
    bool ws_verbose() const { return ws_verbose_; }

private:
    std::string outdir_;
    std::unordered_set<std::string> allowed_;
    int flush_rows_;
    bool fsync_;
    int match_ms_;
    std::atomic<bool> stop_;
    std::thread worker_;
    std::thread watchdog_;
    std::mutex mu_;
    std::condition_variable cv_;
    std::deque<TradeEvent> inbox_;
    std::deque<TradeEvent> prebuffer_;

    std::unordered_map<std::string, std::ofstream> ofs_;
    std::unordered_map<std::string, int> buf_count_;
    const size_t window_max = 5000;

    bool ws_seen_;
    bool mmap_seen_;
    bool no_wait_;
    bool ws_verbose_;

    static std::string upper(std::string s) { std::transform(s.begin(), s.end(), s.begin(), ::toupper); return s; }

    void ensure_csv_locked(const std::string &symbol_upper) {
        if (ofs_.count(symbol_upper)) return;
        if (!allowed_.empty() && !allowed_.count(symbol_upper)) return;
        std::string fname = outdir_ + "/" + symbol_upper + ".csv";
        std::ofstream of(fname, std::ios::app);
        if (!of) {
            std::cerr << "[csv] failed to open " << fname << "\n";
            return;
        }
        struct stat st;
        if (stat(fname.c_str(), &st) == 0 && st.st_size == 0) {
            of << "ts_iso,symbol,source,subsource,kind,seq_or_tradeid,px,sz,bid,ask,mid,recv_ts,ws_ts,arrival_ms,note\n";
        }
        ofs_.emplace(symbol_upper, std::move(of));
        buf_count_[symbol_upper] = 0;
    }

    void write_row_locked(const std::string &symbol_upper, const std::string &line) {
        auto it = ofs_.find(symbol_upper);
        if (it == ofs_.end()) return;
        it->second << line << "\n";
        buf_count_[symbol_upper] += 1;
        if (fsync_) {
            it->second.flush();
        } else if (buf_count_[symbol_upper] >= flush_rows_) {
            it->second.flush();
            buf_count_[symbol_upper] = 0;
        }
    }

    void emit_event_row_locked(const TradeEvent &ev) {
        std::string sym = upper(ev.symbol);
        if (!allowed_.empty() && !allowed_.count(sym)) return;
        ensure_csv_locked(sym);
        if (!ofs_.count(sym)) return;

        std::ostringstream ss;
        ss << now_iso_ms() << ",";
        ss << sym << ",";
        ss << (ev.is_ws ? "ws" : "mmap") << ",";
        ss << ev.prefix << ",";
        ss << 1 << ",";
        ss << ev.seq << ",";
        if (std::isfinite(ev.px)) ss << ev.px; ss << ",";
        if (std::isfinite(ev.sz)) ss << ev.sz; ss << ",";
        ss << "" << "," << "" << "," << "" << ","; // bid,ask,mid placeholders
        ss << ev.recv_ts << ",";
        ss << ev.ws_ts << ",";
        ss << ev.arrival_ms << ",";
        ss << ""; // note

        write_row_locked(sym, ss.str());
    }

    void try_enable_and_flush_locked() {
        if (writes_enabled() || no_wait_) {
            while (!prebuffer_.empty()) {
                inbox_.push_back(prebuffer_.front());
                prebuffer_.pop_front();
            }
            cv_.notify_one();
            if (writes_enabled()) {
                if (ws_verbose_) std::cerr << "[engine] both sources seen -> enabling writes and flushing prebuffered events (" << inbox_.size() << " pending)\n";
            } else if (no_wait_) {
                if (ws_verbose_) std::cerr << "[engine] no-wait mode -> enabling writes and flushing prebuffered events (" << inbox_.size() << " pending)\n";
            }
        } else {
            if (ws_verbose_) std::cerr << "[engine] waiting for both sources (ws_seen=" << ws_seen_ << " mmap_seen=" << mmap_seen_ << ")\n";
        }
    }

    void watchdog_loop() {
        while (!stop_) {
            {
                std::lock_guard<std::mutex> lk(mu_);
                if (!ws_seen_ && !prebuffer_.empty() && ws_verbose_) {
                    std::cerr << "[watchdog] ws events buffered but ws_seen=false? (prebuffer size=" << prebuffer_.size() << ")\n";
                }
            }
            std::this_thread::sleep_for(2000ms);
        }
    }

    void loop() {
        while (!stop_) {
            std::unique_lock<std::mutex> lk(mu_);
            if (inbox_.empty()) {
                cv_.wait_for(lk, 200ms);
                if (stop_) break;
            }
            while (!inbox_.empty()) {
                TradeEvent ev = inbox_.front();
                inbox_.pop_front();
                lk.unlock();

                if (ev.arrival_ms == 0) {
                    ev.arrival_ms = (uint64_t) (std::chrono::duration_cast<std::chrono::milliseconds>(
                                                   std::chrono::system_clock::now().time_since_epoch()).count());
                }

                {
                    std::lock_guard<std::mutex> g(mu_);
                    emit_event_row_locked(ev);
                }

                lk.lock();
            }
        }
    }
};

// === MmapWatcher (reads *_trade_chunk*.meta + .data) ===
class MmapWatcher {
public:
    MmapWatcher(const std::string &rings_dir, CSVEngine &engine,
                const std::unordered_set<std::string> &allowed_symbols,
                int poll_ms=50)
        : rings_dir_(rings_dir), engine_(engine), poll_ms_(poll_ms), stop_(false), allowed_(allowed_symbols)
    {
        worker_ = std::thread(&MmapWatcher::loop, this);
    }

    ~MmapWatcher() {
        stop_ = true;
        if (worker_.joinable()) worker_.join();
        for (auto &p : rings_) close_ring(p.second);
    }

private:
    struct RingState {
        int meta_fd = -1;
        int data_fd = -1;
        uint8_t *meta_ptr = nullptr;
        uint8_t *data_ptr = nullptr;
        size_t meta_len = 0;
        size_t data_len = 0;
        uint64_t last_seq = 0;
        std::string prefix;
    };

    std::string rings_dir_;
    CSVEngine &engine_;
    int poll_ms_;
    std::atomic<bool> stop_;
    std::thread worker_;
    std::map<std::string, RingState> rings_;
    std::unordered_set<std::string> allowed_;

    static uint64_t read_u64_le(const uint8_t *p) { uint64_t v; memcpy(&v,p,8); return v; }
    static uint32_t read_u32_le(const uint8_t *p) { uint32_t v; memcpy(&v,p,4); return v; }

    void close_ring(RingState &rs) {
        if (rs.meta_ptr && rs.meta_ptr != MAP_FAILED) { munmap(rs.meta_ptr, rs.meta_len); rs.meta_ptr = nullptr; }
        if (rs.data_ptr && rs.data_ptr != MAP_FAILED) { munmap(rs.data_ptr, rs.data_len); rs.data_ptr = nullptr; }
        if (rs.meta_fd >= 0) { close(rs.meta_fd); rs.meta_fd = -1; }
        if (rs.data_fd >= 0) { close(rs.data_fd); rs.data_fd = -1; }
    }

    void reopen_ring(RingState &rs, const std::string &prefix) {
        close_ring(rs);
        std::string meta = prefix + ".meta";
        std::string data = prefix + ".data";
        struct stat st;
        if (stat(meta.c_str(), &st) != 0) return;
        rs.meta_len = st.st_size;
        rs.meta_fd = open(meta.c_str(), O_RDONLY);
        if (rs.meta_fd < 0) { close_ring(rs); return; }
        rs.meta_ptr = (uint8_t*)mmap(nullptr, rs.meta_len, PROT_READ, MAP_SHARED, rs.meta_fd, 0);
        if (rs.meta_ptr == MAP_FAILED) { close_ring(rs); return; }

        if (stat(data.c_str(), &st) != 0) { close_ring(rs); return; }
        rs.data_len = st.st_size;
        rs.data_fd = open(data.c_str(), O_RDONLY);
        if (rs.data_fd < 0) { close_ring(rs); return; }
        rs.data_ptr = (uint8_t*)mmap(nullptr, rs.data_len, PROT_READ, MAP_SHARED, rs.data_fd, 0);
        if (rs.data_ptr == MAP_FAILED) { close_ring(rs); return; }

        if (rs.meta_len < META_HEADER_LEN) { close_ring(rs); return; }
        if (memcmp(rs.meta_ptr, META_MAGIC, 8) != 0) { close_ring(rs); return; }
        uint64_t cap = read_u64_le(rs.meta_ptr + 8);
        uint64_t tail = read_u64_le(rs.meta_ptr + 16);
        uint64_t seq = read_u64_le(rs.meta_ptr + 24);
        uint64_t slots = read_u64_le(rs.meta_ptr + 32);
        rs.last_seq = seq;
        rs.prefix = prefix;
        std::cerr << "[mmap] opened " << prefix << " cap=" << cap << " slots=" << slots << " seq=" << seq << "\n";
    }

    bool read_index_slot(RingState &rs, size_t i, uint64_t &off, uint32_t &ln, uint64_t &seq, uint8_t &kind) {
        size_t slot_base = META_HEADER_LEN + i * INDEX_SLOT_SIZE;
        if (slot_base + INDEX_SLOT_SIZE > rs.meta_len) return false;
        const uint8_t *p = rs.meta_ptr + slot_base;
        off = read_u64_le(p);
        ln  = read_u32_le(p + 8);
        seq = read_u64_le(p + 12);
        kind = *(p + 20);
        return true;
    }

    bool read_record(RingState &rs, uint64_t off, uint32_t ln, uint64_t expected_seq, uint8_t expected_kind, TradeEvent &out_ev) {
        if (rs.data_len == 0) return false;
        size_t cap = rs.data_len;
        if (ln < RECORD_HEADER_LEN) return false;
        std::vector<uint8_t> hdr(RECORD_HEADER_LEN);
        for (size_t i=0;i<RECORD_HEADER_LEN;i++) hdr[i] = rs.data_ptr[(off + i) % cap];
        uint32_t plen; memcpy(&plen, hdr.data(), 4);
        uint8_t kind = hdr[4];
        uint64_t seq; memcpy(&seq, hdr.data() + 12, 8);
        if (kind != expected_kind) return false;
        if (seq != expected_seq) return false;
        std::vector<uint8_t> payload(plen);
        for (size_t i=0;i<plen;i++) payload[i] = rs.data_ptr[(off + RECORD_HEADER_LEN + i) % cap];
        try {
            std::string s((char*)payload.data(), payload.size());
            auto j = json::parse(s);
            std::string sym;
            if (j.contains("asset") && j["asset"].is_string()) sym = j["asset"].get<std::string>();
            else if (j.contains("s") && j["s"].is_string()) sym = j["s"].get<std::string>();
            else if (j.contains("symbol") && j["symbol"].is_string()) sym = j["symbol"].get<std::string>();
            else if (j.contains("data") && j["data"].is_object()) {
                auto &d = j["data"];
                if (d.contains("s") && d["s"].is_string()) sym = d["s"].get<std::string>();
                else if (d.contains("symbol") && d["symbol"].is_string()) sym = d["symbol"].get<std::string>();
            }
            // fallback: try scanning payload for uppercase alpha-numeric token (cheap heuristic)
            if (sym.empty()) {
                std::string s((char*)payload.data(), payload.size());
                // naive regex-like scan for contiguous A-Z0-9 of length 5-12 (common symbol length)
                std::string cand;
                for (char c : s) {
                    if ((c>='A'&&c<='Z')||(c>='0'&&c<='9')) cand.push_back(c);
                    else { if (cand.size()>=5 && cand.size()<=12) { sym = cand; break; } cand.clear(); }
                }
            }

            std::transform(sym.begin(), sym.end(), sym.begin(), ::toupper);
            out_ev.symbol = sym;
            if (j.contains("px")) out_ev.px = j["px"].get<double>();
            else if (j.contains("p")) {
                try { out_ev.px = std::stod(j["p"].get<std::string>()); } catch(...) { out_ev.px = NAN; }
            }
            if (j.contains("sz")) out_ev.sz = j["sz"].get<double>();
            else if (j.contains("q")) {
                try { out_ev.sz = std::stod(j["q"].get<std::string>()); } catch(...) { out_ev.sz = NAN; }
            }
            if (j.contains("recv_ts_ms")) out_ev.recv_ts = j["recv_ts_ms"].get<uint64_t>();
            else if (j.contains("exchange_ts_ms")) out_ev.recv_ts = j["exchange_ts_ms"].get<uint64_t>();
            else if (j.contains("E")) out_ev.recv_ts = j["E"].get<uint64_t>();
            if (j.contains("u")) out_ev.seq = j["u"].get<uint64_t>();
            else out_ev.seq = seq;
        } catch (...) {
            return false;
        }
        out_ev.prefix = rs.prefix;
        out_ev.is_ws = false;
        out_ev.arrival_ms = (uint64_t) (std::chrono::duration_cast<std::chrono::milliseconds>(
                                             std::chrono::system_clock::now().time_since_epoch()).count());
        return true;
    }

    void discover_rings() {
        DIR *d = opendir(rings_dir_.c_str());
        if (!d) return;
        struct dirent *ent;
        std::set<std::string> seen;
        while ((ent = readdir(d)) != nullptr) {
            std::string name(ent->d_name);
            if (name.find("_trade_chunk") != std::string::npos && name.size() > 5 && name.rfind(".meta") == name.size() - 5) {
                std::string prefix = rings_dir_ + "/" + name.substr(0, name.size()-5);
                seen.insert(prefix);
                if (!rings_.count(prefix)) {
                    RingState rs;
                    reopen_ring(rs, prefix);
                    if (rs.meta_ptr && rs.data_ptr) rings_.emplace(prefix, std::move(rs));
                    else close_ring(rs);
                }
            }
        }
        closedir(d);
        for (auto it = rings_.begin(); it != rings_.end();) {
            if (!seen.count(it->first)) {
                close_ring(it->second);
                it = rings_.erase(it);
            } else ++it;
        }
    }

    void loop() {
        while (!stop_) {
            discover_rings();
            for (auto &p : rings_) {
                RingState &rs = p.second;
                if (!rs.meta_ptr) continue;
                uint64_t seq_now = read_u64_le(rs.meta_ptr + 24);
                uint64_t slots = read_u64_le(rs.meta_ptr + 32);
                if (seq_now < rs.last_seq) rs.last_seq = 0;
                if (seq_now > rs.last_seq) {
                    for (size_t i=0;i<slots;i++) {
                        uint64_t off; uint32_t ln; uint64_t seq; uint8_t kind;
                        if (!read_index_slot(rs, i, off, ln, seq, kind)) continue;
                        if (seq > rs.last_seq && kind == 1) {
                            TradeEvent ev;
                            if (read_record(rs, off, ln, seq, kind, ev)) {
                                if (allowed_.empty() || allowed_.count(ev.symbol)) {
                                    bool prints_now = false;
                                    if (engine_.writes_enabled()) prints_now = true;
                                    else if (engine_.ws_verbose()) prints_now = true;
                                    if (prints_now) {
                                        std::cerr << "[mmap-trade] prefix=" << ev.prefix << " seq=" << ev.seq
                                                  << " sym=" << ev.symbol << " px=" << ev.px << " sz=" << ev.sz << "\n";
                                    }
                                    engine_.push_event(ev);
                                    engine_.notify_mmap_seen();
                                }
                            }
                        }
                    }
                    rs.last_seq = seq_now;
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(std::max(10, poll_ms_)));
        }
    }
};

// === helper: parse proxy env value into host:port string (or empty) ===
static std::string pick_proxy_env() {
    // Prefer https_proxy then http_proxy then HTTPS_PROXY/HTTP_PROXY
    const char *cands[] = {"https_proxy", "HTTPS_PROXY", "http_proxy", "HTTP_PROXY", nullptr};
    for (const char **p = cands; *p; ++p) {
        const char *v = getenv(*p);
        if (!v) continue;
        std::string s = v;
        // strip "http://" or "socks://" prefix if present
        auto pos = s.find("://");
        if (pos != std::string::npos) s = s.substr(pos+3);
        // keep only host:port portion (strip path)
        pos = s.find('/');
        if (pos != std::string::npos) s = s.substr(0,pos);
        return s;
    }
    return std::string();
}

// === helper: perform HTTP CONNECT on plain socket if proxy is present ===
static bool do_proxy_connect_if_needed(tcp::socket &sock,
                                      const std::string &proxy_hostport,
                                      const std::string &target_host,
                                      const std::string &target_port,
                                      const std::string &log_prefix)
{
    if (proxy_hostport.empty()) return true; // no proxy
    // Note: At this point the socket should already be connected to the proxy endpoint.
    std::ostringstream req;
    req << "CONNECT " << target_host << ":" << target_port << " HTTP/1.1\r\n";
    req << "Host: " << target_host << ":" << target_port << "\r\n";
    req << "Proxy-Connection: Keep-Alive\r\n";
    req << "\r\n";
    boost::system::error_code ec;
    boost::asio::write(sock, boost::asio::buffer(req.str()), ec);
    if (ec) {
        std::cerr << log_prefix << " proxy CONNECT write failed: " << ec.message() << "\n";
        return false;
    }

    // read response up to headers
    std::string resp;
    std::array<char, 4096> buf;
    size_t total = 0;
    for (;;) {
        size_t n = 0;
        boost::system::error_code read_ec;
        n = sock.read_some(boost::asio::buffer(buf), read_ec);
        if (read_ec && read_ec != boost::asio::error::would_block && read_ec != boost::asio::error::try_again) {
            std::cerr << log_prefix << " proxy CONNECT read failed: " << read_ec.message() << "\n";
            return false;
        }
        if (n>0) {
            resp.append(buf.data(), n);
            total += n;
        }
        if (resp.find("\r\n\r\n") != std::string::npos) break;
        if (total > 16384) break;
    }
    // Expect "HTTP/1.1 200" (can tolerate 200 somewhere)
    if (resp.find("200") == std::string::npos) {
        std::cerr << log_prefix << " proxy CONNECT failed: response=" << (resp.size()>200 ? resp.substr(0,200) : resp) << "\n";
        return false;
    }
    return true;
}

// === helper: blocking_connect_with_timeout ===
// Runs a blocking connect() in a separate thread, allows timeout by closing socket.
static bool blocking_connect_with_timeout(tcp::socket &sock,
                                          const tcp::resolver::results_type &endpoints,
                                          int timeout_ms,
                                          bool verbose,
                                          const std::string &log_prefix)
{
    std::atomic<bool> done(false);
    std::atomic<bool> ok(false);
    std::string errstr;

    std::thread t([&](){
        try {
            if (verbose) std::cerr << log_prefix << " [connect-thread] calling connect() ...\n";
            boost::system::error_code ec;
            net::connect(sock, endpoints, ec);
            if (!ec) {
                ok.store(true);
                if (verbose) std::cerr << log_prefix << " [connect-thread] connect OK\n";
            } else {
                errstr = ec.message();
                if (verbose) std::cerr << log_prefix << " [connect-thread] connect failed: " << errstr << "\n";
            }
        } catch (std::exception &e) {
            errstr = e.what();
            if (verbose) std::cerr << log_prefix << " [connect-thread] exception: " << errstr << "\n";
        }
        done.store(true);
    });

    int waited = 0;
    const int step = 50;
    while (!done.load() && waited < timeout_ms) {
        std::this_thread::sleep_for(std::chrono::milliseconds(step));
        waited += step;
    }

    if (!done.load()) {
        if (verbose) std::cerr << log_prefix << " [connect-timeout] timeout after " << timeout_ms << "ms -> closing socket to interrupt\n";
        boost::system::error_code ec;
        sock.close(ec); // should interrupt connect
        // wait briefly for thread to notice
        int waited2 = 0;
        while (!done.load() && waited2 < 5000) { std::this_thread::sleep_for(10ms); waited2 += 10; }
    }

    if (t.joinable()) t.join();
    if (ok.load()) return true;
    if (verbose) std::cerr << log_prefix << " connect failed: " << errstr << "\n";
    return false;
}

// === helper: SSL handshake with timeout ===
template<typename SSLStream>
static bool ssl_handshake_with_timeout(SSLStream &ssl_stream, int timeout_ms, bool verbose, const std::string &log_prefix) {
    std::atomic<bool> done(false), ok(false);
        std::string err;
        std::thread t([&](){
            try {
                boost::system::error_code ec;
                ssl_stream.handshake(boost::asio::ssl::stream_base::client, ec);
                if (!ec) { ok.store(true); if (verbose) std::cerr << log_prefix << " [ssl-thread] handshake ok\n"; }
                else {
                    err = ec.message();
                    if (verbose) std::cerr << log_prefix << " [ssl-thread] handshake failed: " << err << "\n";
                    // print OpenSSL error queue
                    unsigned long e;
                    while ((e = ERR_get_error()) != 0) {
                        char buf[256];
                        ERR_error_string_n(e, buf, sizeof(buf));
                        std::cerr << log_prefix << " [openssl] " << buf << "\n";
                    }
                }
            } catch (std::exception &e) {
                err = e.what();
                if (verbose) std::cerr << log_prefix << " [ssl-thread] exception: " << err << "\n";
                unsigned long ev;
                while ((ev = ERR_get_error()) != 0) {
                    char buf[256];
                    ERR_error_string_n(ev, buf, sizeof(buf));
                    std::cerr << log_prefix << " [openssl] " << buf << "\n";
                }
            }
            done.store(true);
        });

    int waited = 0;
    const int step = 50;
    while (!done.load() && waited < timeout_ms) {
        std::this_thread::sleep_for(std::chrono::milliseconds(step));
        waited += step;
    }
    if (!done.load()) {
        if (verbose) std::cerr << log_prefix << " [ssl-timeout] timeout -> close underlying socket\n";
        boost::system::error_code ec;
        try { ssl_stream.lowest_layer().close(ec); } catch(...) {}
        int waited2 = 0;
        while (!done.load() && waited2 < 2000) { std::this_thread::sleep_for(10ms); waited2 += 10; }
    }
    if (t.joinable()) t.join();
    return ok.load();
}

// === BinanceWS (per-symbol) ===
class BinanceWS {
public:
    BinanceWS(const std::string &symbol_upper, CSVEngine &engine)
        : symbol_upper_(upper(symbol_upper)), engine_(engine), stop_(false)
    {
        th_ = std::thread(&BinanceWS::run, this);
    }
    ~BinanceWS() {
        stop_ = true;
        if (th_.joinable()) th_.join();
    }

private:
    std::string symbol_upper_;
    CSVEngine &engine_;
    std::atomic<bool> stop_;
    std::thread th_;

    static std::string upper(const std::string &s) {
        std::string r = s; std::transform(r.begin(), r.end(), r.begin(), ::toupper); return r;
    }
    static std::string lower(const std::string &s) {
        std::string r = s; std::transform(r.begin(), r.end(), r.begin(), ::tolower); return r;
    }

    void run() {
        while (!stop_) {
            try {
                do_connect_loop();
            } catch (const std::exception &e) {
                std::cerr << "[ws:" << symbol_upper_ << "] exception: " << e.what() << "\n";
            }
            if (stop_) break;
            std::this_thread::sleep_for(500ms);
        }
    }

    void do_connect_loop() {
        net::io_context ioc;
        boost::asio::ssl::context ssl_ctx{boost::asio::ssl::context::sslv23_client};
        ssl_ctx.set_default_verify_paths();

        std::string host = "stream.binance.com";
        std::string port = "9443";
        std::string stream = lower(symbol_upper_) + "@trade";
        std::string target = "/ws/" + stream;

        // pick proxy (if any)
        std::string proxy = pick_proxy_env();
        int attempt = 0;

        while (!stop_) {
            ++attempt;
            std::string log_pref = std::string("[ws:") + symbol_upper_ + "]";
            std::cerr << log_pref << " attempt=" << attempt << " connecting to wss://" << host << target
                      << (proxy.empty() ? "" : std::string(" via proxy=")+proxy) << "\n";

            try {
                tcp::resolver resolver(ioc);
                std::cerr << log_pref << " resolving " << (proxy.empty() ? host : proxy) << ":" << (proxy.empty() ? port : "80") << " ...\n";

                boost::system::error_code res_ec;
                tcp::resolver::results_type endpoints;
                if (proxy.empty()) {
                    endpoints = resolver.resolve(host, port, res_ec);
                } else {
                    // resolve proxy host/port
                    std::string proxy_host = proxy;
                    std::string proxy_port = "8080";
                    auto ppos = proxy.find(':');
                    if (ppos != std::string::npos) {
                        proxy_host = proxy.substr(0, ppos);
                        proxy_port = proxy.substr(ppos+1);
                    }
                    endpoints = resolver.resolve(proxy_host, proxy_port, res_ec);
                }
                if (res_ec) {
                    std::cerr << log_pref << " resolve failed: " << res_ec.message() << "\n";
                    throw std::runtime_error("resolve failed");
                }
                std::cerr << log_pref << " resolve succeeded, endpoints obtained\n";

                // create socket
                tcp::socket sock(ioc);
                const int connect_timeout_ms = 10000; // 10s
                bool connected_ok = blocking_connect_with_timeout(sock, endpoints, connect_timeout_ms, engine_.ws_verbose(), log_pref);
                if (!connected_ok) {
                    std::cerr << log_pref << " connect stage failed/timeout; will backoff and retry\n";
                    throw std::runtime_error("connect failed or timed out");
                }
                std::cerr << log_pref << " tcp connect OK -> performing proxy CONNECT (if needed)\n";

                // if proxy is present, do HTTP CONNECT to target host:port
                if (!proxy.empty()) {
                    if (!do_proxy_connect_if_needed(sock, proxy, host, port, log_pref + " ")) {
                        throw std::runtime_error("proxy CONNECT failed");
                    }
                    std::cerr << log_pref << " proxy CONNECT OK\n";
                }

                // wrap socket in ssl stream
                boost::asio::ssl::stream<tcp::socket> ssl_stream(std::move(sock), ssl_ctx);
                ssl_stream.set_verify_mode(boost::asio::ssl::verify_peer);
                ssl_stream.set_verify_callback(boost::asio::ssl::rfc2818_verification(host));

                // SSL handshake with timeout
                if (!ssl_handshake_with_timeout(ssl_stream, 8000, engine_.ws_verbose(), log_pref + " ")) {
                    std::cerr << log_pref << " SSL handshake failed or timed out\n";
                    throw std::runtime_error("ssl handshake failed");
                }
                std::cerr << log_pref << " SSL handshake OK\n";

                // websocket stream
                websocket::stream<boost::asio::ssl::stream<tcp::socket>> ws(std::move(ssl_stream));
                ws.set_option(websocket::stream_base::timeout::suggested(beast::role_type::client));
                ws.set_option(websocket::stream_base::decorator([](websocket::request_type &req){
                    req.set(beast::http::field::user_agent, std::string("mmap_trade_compare_cpp/1.0"));
                }));

                // websocket handshake
                try {
                    std::cerr << log_pref << " performing websocket handshake host=" << host << " target=" << target << "\n";
                    ws.handshake(host, target);
                    std::cerr << log_pref << " websocket handshake OK\n";
                } catch (const std::exception &e) {
                    std::cerr << log_pref << " websocket handshake failed: " << e.what() << "\n";
                    throw;
                }

                // connected -> read loop
                attempt = 0;
                beast::flat_buffer buffer;
                while (!stop_) {
                    beast::error_code ec;
                    ws.read(buffer, ec);
                    if (ec) {
                        std::cerr << log_pref << " read error: " << ec.message() << "\n";
                        break;
                    }
                    std::string data = beast::buffers_to_string(buffer.data());
                    buffer.consume(buffer.size());

                    try {
                        auto j = json::parse(data);
                        json rec = j;
                        if (j.is_object() && j.contains("data") && j["data"].is_object()) rec = j["data"];
                        if (rec.is_object() && rec.contains("p") && rec.contains("q")) {
                            TradeEvent ev;
                            ev.is_ws = true;
                            ev.symbol = rec.value("s", std::string());
                            try { ev.px = std::stod(rec.value("p", std::string("nan"))); } catch(...) { ev.px = NAN; }
                            try { ev.sz = std::stod(rec.value("q", std::string("nan"))); } catch(...) { ev.sz = NAN; }
                            if (rec.contains("E")) ev.ws_ts = rec.value("E", uint64_t(0));
                            else ev.ws_ts = (uint64_t)(std::chrono::duration_cast<std::chrono::milliseconds>(
                                                           std::chrono::system_clock::now().time_since_epoch()).count());
                            ev.seq = rec.value("t", uint64_t(0));
                            std::transform(ev.symbol.begin(), ev.symbol.end(), ev.symbol.begin(), ::toupper);
                            ev.prefix = "ws";
                            ev.arrival_ms = (uint64_t)(std::chrono::duration_cast<std::chrono::milliseconds>(
                                                            std::chrono::system_clock::now().time_since_epoch()).count());

                            std::cerr << "[ws-trade] sym=" << ev.symbol << " px=" << ev.px << " sz=" << ev.sz
                                      << " id=" << ev.seq << " ws_ts=" << ev.ws_ts << "\n";

                            if (allowed_for(ev.symbol)) {
                                engine_.push_event(ev);
                                engine_.notify_ws_seen();
                            } else {
                                if (engine_.ws_verbose()) std::cerr << log_pref << " dropped (not allowed): " << ev.symbol << "\n";
                            }
                        } else {
                            if (engine_.ws_verbose()) std::cerr << log_pref << " non-trade message\n";
                        }
                    } catch (std::exception &e) {
                        std::cerr << log_pref << " json parse failed: " << e.what() << "\n";
                    }
                }

                beast::error_code ec2;
                ws.close(websocket::close_code::normal, ec2);
            } catch (const std::exception &e) {
                std::cerr << "[ws:" << symbol_upper_ << "] connection exception: " << e.what() << "\n";
            }

            if (stop_) break;
            int backoff_ms = std::min(16000, (1 << std::min(10, attempt)) * 50);
            std::cerr << "[ws:" << symbol_upper_ << "] reconnect backoff " << backoff_ms << "ms\n";
            std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
        }
    }

    bool allowed_for(const std::string &sym) const {
        std::string s = sym;
        std::transform(s.begin(), s.end(), s.begin(), ::toupper);
        return s == symbol_upper_;
    }
};

// === DeribitWS (per-instrument) ===
// Connects to Deribit JSON-RPC websocket and subscribes to trades.<instrument>.raw
// Replace the existing DeribitWS class with this implementation.

class DeribitWS {
public:
    DeribitWS(const std::string &instrument, CSVEngine &engine,
              const std::string &host = "www.deribit.com", const std::string &port = "443",
              bool use_testnet = false, bool insecure = false)
        : instr_upper_(upper(instrument)), engine_(engine), stop_(false),
          host_(host), port_(port), use_testnet_(use_testnet), insecure_(insecure)
    {
        th_ = std::thread(&DeribitWS::run, this);
    }

    ~DeribitWS() {
        stop_ = true;
        if (th_.joinable()) th_.join();
    }

private:
    std::string instr_upper_;
    CSVEngine &engine_;
    std::atomic<bool> stop_;
    std::thread th_;
    std::string host_;
    std::string port_;
    bool use_testnet_;
    bool insecure_; // whether to skip server cert verification (debug only)

    static std::string upper(const std::string &s) {
        std::string r = s; std::transform(r.begin(), r.end(), r.begin(), ::toupper); return r;
    }

    // Read next websocket message into 'out' (returns true on message, false on error/close)
    bool ws_read_once(websocket::stream<boost::asio::ssl::stream<tcp::socket>> &ws, std::string &out) {
        beast::flat_buffer buffer;
        beast::error_code ec;
        ws.read(buffer, ec);
        if (ec) {
            return false;
        }
        out = beast::buffers_to_string(buffer.data());
        return true;
    }

    void run() {
        while (!stop_) {
            try { do_connect_loop(); } catch (const std::exception &e) {
                std::cerr << "[deribit:" << instr_upper_ << "] exception: " << e.what() << "\n";
            }
            if (stop_) break;
            std::this_thread::sleep_for(500ms);
        }
    }

    void do_connect_loop() {
        net::io_context ioc;
        boost::asio::ssl::context ssl_ctx{boost::asio::ssl::context::sslv23_client};
        ssl_ctx.set_default_verify_paths();

        const std::string lp = std::string("[deribit:") + instr_upper_ + "]";

        // TLS options (same as before)
        SSL_CTX_set_options(ssl_ctx.native_handle(),
            SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3 | SSL_OP_NO_COMPRESSION | SSL_OP_NO_TLSv1 | SSL_OP_NO_TLSv1_1);

        if (SSL_CTX_set_cipher_list(ssl_ctx.native_handle(), "HIGH:!aNULL:!eNULL:!kRSA:!PSK:!SRP") != 1) {
            std::cerr << lp << " warning: SSL_CTX_set_cipher_list failed\n";
        }
        #if defined(SSL_CTX_set_ciphersuites)
        if (SSL_CTX_set_ciphersuites(ssl_ctx.native_handle(),
             "TLS_AES_256_GCM_SHA384:TLS_AES_128_GCM_SHA256:TLS_CHACHA20_POLY1305_SHA256") != 1) {
            std::cerr << lp << " warning: SSL_CTX_set_ciphersuites failed\n";
        }
        #endif

        std::string target = "/ws/api/v2";
        std::string proxy = pick_proxy_env();
        int attempt = 0;

        // read client credentials from env
        const char *env_id = std::getenv("DERIBIT_CLIENT_ID");
        const char *env_secret = std::getenv("DERIBIT_CLIENT_SECRET");
        std::string client_id = env_id ? env_id : "";
        std::string client_secret = env_secret ? env_secret : "";

        while (!stop_) {
            ++attempt;
            std::string log_pref = std::string("[deribit:") + instr_upper_ + "]";
            std::cerr << log_pref << " attempt=" << attempt << " connecting to wss://" << host_ << target
                      << (proxy.empty() ? "" : std::string(" via proxy=")+proxy) << "\n";
            try {
                tcp::resolver resolver(ioc);
                boost::system::error_code res_ec;
                tcp::resolver::results_type endpoints;
                if (proxy.empty()) endpoints = resolver.resolve(host_, port_, res_ec);
                else {
                    std::string proxy_host = proxy; std::string proxy_port = "80";
                    auto ppos = proxy.find(':'); if (ppos != std::string::npos) { proxy_host = proxy.substr(0, ppos); proxy_port = proxy.substr(ppos+1); }
                    endpoints = resolver.resolve(proxy_host, proxy_port, res_ec);
                }
                if (res_ec) { std::cerr << log_pref << " resolve failed: " << res_ec.message() << "\n"; throw std::runtime_error("resolve"); }

                tcp::socket sock(ioc);
                const int connect_timeout_ms = 10000;
                bool connected_ok = blocking_connect_with_timeout(sock, endpoints, connect_timeout_ms, engine_.ws_verbose(), log_pref);
                if (!connected_ok) throw std::runtime_error("connect failed");

                if (!proxy.empty()) {
                    if (!do_proxy_connect_if_needed(sock, proxy, host_, port_, log_pref + " ")) {
                        throw std::runtime_error("proxy CONNECT failed");
                    }
                }

                // Create ssl_stream AFTER socket is ready
                boost::asio::ssl::stream<tcp::socket> ssl_stream(std::move(sock), ssl_ctx);
                if (SSL_set_tlsext_host_name(ssl_stream.native_handle(), host_.c_str()) != 1) {
                    std::cerr << log_pref << " warning: SSL_set_tlsext_host_name failed\n";
                }
                if (insecure_) {
                    ssl_stream.set_verify_mode(boost::asio::ssl::verify_none);
                } else {
                    ssl_stream.set_verify_mode(boost::asio::ssl::verify_peer);
                    ssl_stream.set_verify_callback(boost::asio::ssl::rfc2818_verification(host_));
                }

                if (!ssl_handshake_with_timeout(ssl_stream, 8000, engine_.ws_verbose(), log_pref + " ")) {
                    throw std::runtime_error("ssl handshake failed");
                }

                websocket::stream<boost::asio::ssl::stream<tcp::socket>> ws(std::move(ssl_stream));
                ws.set_option(websocket::stream_base::timeout::suggested(beast::role_type::client));
                ws.set_option(websocket::stream_base::decorator([](websocket::request_type &req){
                    req.set(beast::http::field::user_agent, std::string("mmap_trade_compare_cpp/1.0-deribit"));
                }));

                // handshake
                ws.handshake(host_, target);

                // --- AUTH step: if client_id/secret available, perform public/auth first ---
                bool authenticated = false;
                if (!client_id.empty() && !client_secret.empty()) {
                    int auth_id = 9998;
                    json auth_req = {
                        {"jsonrpc", "2.0"},
                        {"id", auth_id},
                        {"method", "public/auth"},
                        {"params", {
                            {"grant_type", "client_credentials"},
                            {"client_id", client_id},
                            {"client_secret", client_secret}
                        }}
                    };
                    std::string auth_str = auth_req.dump();
                    ws.write(net::buffer(auth_str));
                    if (engine_.ws_verbose()) std::cerr << log_pref << " sent public/auth (id=" << auth_id << ")\n";

                    // Blockingly read messages until we see the auth reply (or some other messages).
                    // This will normally arrive immediately; if it doesn't, the websocket read will block.
                    // If you want a strict timeout here we can add a watchdog thread — ask if needed.
                    for (;;) {
                        std::string reply;
                        bool ok = ws_read_once(ws, reply);
                        if (!ok) {
                            std::cerr << log_pref << " auth read failed (socket closed)\n";
                            break;
                        }
                        if (engine_.ws_verbose()) std::cerr << log_pref << " auth-recv raw: " << reply << "\n";
                        try {
                            auto jr = json::parse(reply);
                            // check if it's response to our auth id
                            if (jr.is_object() && jr.contains("id") && jr["id"].is_number_integer() && jr["id"].get<int>() == auth_id) {
                                if (jr.contains("error")) {
                                    std::cerr << log_pref << " auth error: " << jr["error"].dump() << "\n";
                                    // don't proceed with subscribe if auth failed
                                    authenticated = false;
                                    break;
                                }
                                if (jr.contains("result") && jr["result"].is_object()) {
                                    auto res = jr["result"];
                                    if (res.contains("access_token")) {
                                        std::string token = res["access_token"].get<std::string>();
                                        // we don't strictly need to store token here for later use because Deribit auth done server-side,
                                        // but we log it for debug (do NOT print tokens to production logs).
                                        if (engine_.ws_verbose()) std::cerr << log_pref << " auth succeeded, got access_token (len=" << token.size() << ")\n";
                                        authenticated = true;
                                        break;
                                    } else {
                                        // some success replies might not include token (unusual) — treat as failure
                                        std::cerr << log_pref << " auth result missing access_token\n";
                                        authenticated = false;
                                        break;
                                    }
                                }
                            } else {
                                // not the auth reply — could be other notifications (pong, test_request etc.). Keep reading.
                                if (engine_.ws_verbose()) std::cerr << log_pref << " auth: non-auth message, continue\n";
                                continue;
                            }
                        } catch (std::exception &e) {
                            std::cerr << log_pref << " auth parse failed: " << e.what() << " raw=" << reply << "\n";
                            continue;
                        }
                    } // auth read loop
                } else {
                    if (engine_.ws_verbose()) std::cerr << log_pref << " no DERIBIT_CLIENT_ID/SECRET set - attempting subscribe unauthenticated\n";
                }

                // If Deribit requires auth for raw subscriptions, we should abort if authentication failed.
                if (!client_id.empty() && !client_secret.empty() && !authenticated) {
                    std::cerr << log_pref << " authentication failed or not available; won't subscribe to raw data\n";
                    // close websocket politely and backoff
                    beast::error_code ec_close;
                    ws.close(websocket::close_code::normal, ec_close);
                    throw std::runtime_error("deribit authentication failed");
                }

                // Now send subscription (authenticated or public depending)
                std::string channel = std::string("trades.") + instr_upper_ + ".raw";
                json sub_req = {
                    {"jsonrpc","2.0"},
                    {"id", 1},
                    {"method", "public/subscribe"},
                    {"params", { {"channels", json::array({channel})} } }
                };
                std::string sub_str = sub_req.dump();
                ws.write(net::buffer(sub_str));
                std::cerr << log_pref << " sent public/subscribe for " << channel << "\n";

                // read loop
                beast::flat_buffer buffer;
                while (!stop_) {
                    beast::error_code ec;
                    ws.read(buffer, ec);
                    if (ec) { std::cerr << log_pref << " read error: " << ec.message() << "\n"; break; }
                    std::string data = beast::buffers_to_string(buffer.data());
                    buffer.consume(buffer.size());

                    // parse JSON-RPC notification and extract trade(s)
                    // --- robust Deribit message handling (replace old parsing block) ---
                    try {
                        auto j = json::parse(data);

                        // 1) Handle explicit RPC reply to subscribe (result may be array of channel names)
                        if (j.is_object() && j.contains("id") && j.contains("result")) {
                            // result can be an array (subscribe ack), an object, or a primitive
                            if (j["result"].is_array()) {
                                if (engine_.ws_verbose()) {
                                    std::cerr << "[deribit-ws] subscribe/reply (array) id=" << j["id"] << " result_len=" << j["result"].size() << "\n";
                                }
                                // Nothing to do for subscribe ack other than log
                                continue;
                            }
                            // fallthrough: if result is object, treat below like params
                        }

                        // 2) Notifications: either "params" object or "result" object containing data
                        json maybe_params;
                        if (j.contains("params") && j["params"].is_object()) {
                            maybe_params = j["params"];
                        } else if (j.contains("result") && j["result"].is_object()) {
                            maybe_params = j["result"];
                        }

                        // 3) If we have an object, try to extract data/trades
                        if (!maybe_params.is_null()) {
                            // dat may be an array of trades OR an object that contains "data" or "trades"
                            json dat;
                            if (maybe_params.contains("data")) dat = maybe_params["data"];
                            else if (maybe_params.contains("trades")) dat = maybe_params["trades"];
                            else dat = maybe_params;

                            // If dat is an array of trade objects -> parse them
                            if (dat.is_array()) {
                                for (auto &tr : dat) {
                                    // defensive: ensure tr is object
                                    if (!tr.is_object()) continue;

                                    TradeEvent ev;
                                    ev.is_ws = true;
                                    ev.arrival_ms = (uint64_t)(std::chrono::duration_cast<std::chrono::milliseconds>(
                                        std::chrono::system_clock::now().time_since_epoch()).count());
                                    ev.prefix = "ws";
                                    ev.symbol = tr.value("instrument_name", instr_upper_);
                                    // price / amount defensive mapping
                                    if (tr.contains("price")) ev.px = tr["price"].is_number() ? tr["price"].get<double>() : NAN;
                                    else if (tr.contains("trade_price")) ev.px = tr["trade_price"].is_number() ? tr["trade_price"].get<double>() : NAN;
                                    if (tr.contains("amount")) ev.sz = tr["amount"].is_number() ? tr["amount"].get<double>() : NAN;
                                    else if (tr.contains("size")) ev.sz = tr["size"].is_number() ? tr["size"].get<double>() : NAN;
                                    if (tr.contains("timestamp")) ev.ws_ts = tr["timestamp"].get<uint64_t>();
                                    else ev.ws_ts = ev.arrival_ms;
                                    if (tr.contains("trade_id")) {
                                        if (tr["trade_id"].is_number()) ev.seq = tr["trade_id"].get<uint64_t>();
                                        else ev.seq = tr.value("trade_seq", uint64_t(0));
                                    } else ev.seq = tr.value("trade_seq", uint64_t(0));
                                    std::transform(ev.symbol.begin(), ev.symbol.end(), ev.symbol.begin(), ::toupper);

                                    if (engine_.ws_verbose()) {
                                        std::cerr << "[deribit-ws] sym=" << ev.symbol << " px=" << ev.px << " sz=" << ev.sz << " id=" << ev.seq << "\n";
                                    }

                                    engine_.push_event(ev);
                                    engine_.notify_ws_seen();
                                }
                                continue;
                            }

                            // 4) dat might be an object that contains a "trades" array
                            if (dat.is_object() && dat.contains("trades") && dat["trades"].is_array()) {
                                for (auto &tr : dat["trades"]) {
                                    if (!tr.is_object()) continue;
                                    // same mapping as above (you can refactor into a helper)
                                    TradeEvent ev;
                                    ev.is_ws = true;
                                    ev.arrival_ms = (uint64_t)(std::chrono::duration_cast<std::chrono::milliseconds>(
                                        std::chrono::system_clock::now().time_since_epoch()).count());
                                    ev.prefix = "ws";
                                    ev.symbol = tr.value("instrument_name", instr_upper_);
                                    if (tr.contains("price")) ev.px = tr["price"].is_number() ? tr["price"].get<double>() : NAN;
                                    else if (tr.contains("trade_price")) ev.px = tr["trade_price"].is_number() ? tr["trade_price"].get<double>() : NAN;
                                    if (tr.contains("amount")) ev.sz = tr["amount"].is_number() ? tr["amount"].get<double>() : NAN;
                                    else if (tr.contains("size")) ev.sz = tr["size"].is_number() ? tr["size"].get<double>() : NAN;
                                    if (tr.contains("timestamp")) ev.ws_ts = tr["timestamp"].get<uint64_t>();
                                    else ev.ws_ts = ev.arrival_ms;
                                    ev.seq = tr.value("trade_seq", uint64_t(0));
                                    std::transform(ev.symbol.begin(), ev.symbol.end(), ev.symbol.begin(), ::toupper);

                                    if (engine_.ws_verbose()) {
                                        std::cerr << "[deribit-ws] sym=" << ev.symbol << " px=" << ev.px << " sz=" << ev.sz << " id=" << ev.seq << "\n";
                                    }

                                    engine_.push_event(ev);
                                    engine_.notify_ws_seen();
                                }
                                continue;
                            }
                        }

                        // 5) If we reached here, message shape was not recognized — optionally log
                        if (engine_.ws_verbose()) {
                            std::cerr << "[deribit-ws] unhandled message shape: " << data << "\n";
                        }
                    } catch (std::exception &e) {
                        std::cerr << log_pref << " json parse failed: " << e.what() << " data=" << data << "\n";
                    }

                } // read loop

                beast::error_code ec2;
                ws.close(websocket::close_code::normal, ec2);
            } catch (const std::exception &e) {
                std::cerr << "[deribit:" << instr_upper_ << "] connection exception: " << e.what() << "\n";
            }

            if (stop_) break;
            int backoff_ms = std::min(16000, (1 << std::min(10, attempt)) * 50);
            std::cerr << "[deribit:" << instr_upper_ << "] reconnect backoff " << backoff_ms << "ms\n";
            std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
        }
    }
};


// ---- Filters loader & CLI ----
struct Filters { std::vector<std::string> symbols; std::vector<std::string> exchanges; };

static std::unordered_set<std::string> make_symbol_set(const Filters &f) {
    std::unordered_set<std::string> s;
    for (auto &sym : f.symbols) {
        std::string up = sym;
        std::transform(up.begin(), up.end(), up.begin(), ::toupper);
        s.insert(up);
    }
    return s;
}

Filters load_filters(const std::string &path) {
    Filters f;
    try {
        std::ifstream ifs(path);
        if (!ifs) return f;
        json j; ifs >> j;
        if (j.contains("symbols") && j["symbols"].is_array()) {
            for (auto &v : j["symbols"]) if (v.is_string()) {
                std::string s = v.get<std::string>();
                std::transform(s.begin(), s.end(), s.begin(), ::toupper);
                f.symbols.push_back(s);
            }
        }
        if (j.contains("exchanges") && j["exchanges"].is_array()) {
            for (auto &v : j["exchanges"]) if (v.is_string()) {
                std::string s = v.get<std::string>();
                std::transform(s.begin(), s.end(), s.begin(), ::toupper);
                f.exchanges.push_back(s);
            }
        }
    } catch (...) {}
    return f;
}

struct Args {
    std::string rings = "rings";
    std::string filters = "filters.json";
    std::string out = "out";
    int poll_ms = 50;
    int match_ms = 500;
    bool no_wait = false;
    bool ws_verbose = false;
    std::string exchange = "binance"; // NEW: which exchange to use
    bool insecure_ws = false;        // NEW
    bool deribit_testnet = false;    // NEW
};

Args parse_args(int argc, char **argv) {
    Args a;
    for (int i=1;i<argc;i++) {
        std::string s(argv[i]);
        if (s=="--rings" && i+1<argc) a.rings = argv[++i];
        else if (s=="--filters" && i+1<argc) a.filters = argv[++i];
        else if (s=="--out" && i+1<argc) a.out = argv[++i];
        else if (s=="--poll-ms" && i+1<argc) a.poll_ms = atoi(argv[++i]);
        else if (s=="--match-ms" && i+1<argc) a.match_ms = atoi(argv[++i]);
        else if (s=="--no-wait-sources") a.no_wait = true;
        else if (s=="--ws-verbose") a.ws_verbose = true;
        else if ((s=="--exchange" || s=="-x") && i+1<argc) a.exchange = argv[++i];
        else if (s=="--insecure-ws") a.insecure_ws = true;
        else if (s=="--deribit-testnet") a.deribit_testnet = true;
        else { std::cerr << "unknown arg " << s << "\n"; }
    }
    return a;
}

int main(int argc, char **argv) {
    auto args = parse_args(argc, argv);
    Filters f = load_filters(args.filters);
    if (f.symbols.empty()) {
        std::cerr << "filters.json has no symbols. Example:\n{ \"symbols\": [\"SOLUSDT\"], \"exchanges\": [\"BINANCE\"] }\n";
        return 1;
    }

    std::cerr << "allowed symbols:";
    for (auto &s: f.symbols) std::cerr << " " << s;
    std::cerr << "\n";

    auto allowed_set = make_symbol_set(f);
    CSVEngine engine(args.out, allowed_set, 100, false, args.match_ms, args.no_wait, args.ws_verbose);

    // spawn websocket clients for allowed symbols (binance or deribit)
    std::vector<std::unique_ptr<BinanceWS>> binance_clients;
    std::vector<std::unique_ptr<DeribitWS>> deribit_clients;
    for (auto &sym : f.symbols) {
        std::string up = sym;
        std::transform(up.begin(), up.end(), up.begin(), ::toupper);
        if (args.ws_verbose) std::cerr << "starting ws client for " << up << "\n";
        if (args.exchange == "binance") {
            binance_clients.emplace_back(new BinanceWS(up, engine));
        } else if (args.exchange == "deribit") {
            std::string host = args.deribit_testnet ? "test.deribit.com" : "www.deribit.com";
            std::string port = "443";
            deribit_clients.emplace_back(new DeribitWS(up, engine, host, port, /*use_testnet=*/args.deribit_testnet, /*insecure=*/args.insecure_ws));
        }
        else {
            std::cerr << "unsupported exchange: " << args.exchange << "\n";
        }
    }

    MmapWatcher watcher(args.rings, engine, allowed_set, args.poll_ms);

    std::cerr << "running. Ctrl-C to quit. (no-wait-sources=" << (args.no_wait ? "1":"0") << ", ws-verbose=" << (args.ws_verbose ? "1":"0") << ", exchange=" << args.exchange << ")\n";
    std::signal(SIGINT, [](int){ std::cerr << "SIGINT\n"; std::exit(0); });

    while (true) std::this_thread::sleep_for(1s);
    return 0;
}
