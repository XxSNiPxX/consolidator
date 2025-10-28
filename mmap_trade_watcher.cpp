// mmap_trade_compare.cpp
// Single-file program: mmap trade reader + Binance websocket trade comparator
// Requires Boost (asio/beast), OpenSSL, nlohmann/json.hpp
// Compile and run as described in the message above.

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

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast.hpp>
#include <boost/beast/ssl.hpp>
#include <unordered_set>
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

// ---- iso now helper ----
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

// ---- CSVEngine: writes only allowed symbols; thread-safe; does matching & compare rows ----
class CSVEngine {
public:
    CSVEngine(const std::string &outdir,
              const std::unordered_set<std::string> &allowed_symbols,
              int flush_rows = 100, bool fsync=false, int match_ms=500)
        : outdir_(outdir), allowed_(allowed_symbols), flush_rows_(flush_rows),
          fsync_(fsync), match_ms_(match_ms), stop_(false)
    {
        struct stat st;
        if (stat(outdir_.c_str(), &st) != 0) mkdir(outdir_.c_str(), 0755);
        worker_ = std::thread(&CSVEngine::loop, this);
    }

    ~CSVEngine() {
        stop_ = true;
        cv_.notify_all();
        if (worker_.joinable()) worker_.join();

        std::lock_guard<std::mutex> lk(mu_);
        for (auto &p : ofs_) {
            try { p.second.flush(); p.second.close(); } catch(...) {}
        }
    }

    // push a parsed trade event; engine decides whether to write/match depending on allowed set
    void push_event(const TradeEvent &ev) {
        // normalize symbol uppercase for check
        TradeEvent e = ev;
        std::transform(e.symbol.begin(), e.symbol.end(), e.symbol.begin(), ::toupper);
        {
            std::lock_guard<std::mutex> lk(mu_);
            inbox_.push_back(std::move(e));
        }
        cv_.notify_one();
        // short debug log (only for allowed symbols)
        std::string s = ev.symbol;
        std::transform(s.begin(), s.end(), s.begin(), ::toupper);
        if (allowed_.empty() || allowed_.count(s)) {
            std::cerr << "[engine] push_event sym=" << s << " is_ws=" << (ev.is_ws ? "1":"0")
                      << " px=" << ev.px << " ws_ts=" << ev.ws_ts << " recv_ts=" << ev.recv_ts << "\n";
        }
    }

private:
    std::string outdir_;
    std::unordered_set<std::string> allowed_;
    int flush_rows_;
    bool fsync_;
    int match_ms_;
    std::atomic<bool> stop_;
    std::thread worker_;
    std::mutex mu_;
    std::condition_variable cv_;
    std::deque<TradeEvent> inbox_;

    // open file streams keyed by uppercase symbol
    std::unordered_map<std::string, std::ofstream> ofs_;
    std::unordered_map<std::string, int> buf_count_;

    // recent windows for matching
    std::unordered_map<std::string, std::deque<TradeEvent>> recent_mmap_;
    std::unordered_map<std::string, std::deque<TradeEvent>> recent_ws_;
    const size_t window_max = 5000;

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
            of << "ts_iso,symbol,source,subsource,kind,seq_or_tradeid,px,sz,bid,ask,mid,recv_ts,ws_ts,arrival_ms,price_diff,matched_with,note\n";
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
            // for full POSIX durability you'd keep an fd and call ::fsync(fd)
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
        ss << 1 << ","; // kind=trade
        ss << ev.seq << ",";
        if (std::isfinite(ev.px)) ss << ev.px; ss << ",";
        if (std::isfinite(ev.sz)) ss << ev.sz; ss << ",";
        ss << "" << "," << "" << "," << "" << ","; // bid,ask,mid
        ss << ev.recv_ts << ",";
        ss << ev.ws_ts << ",";
        ss << ev.arrival_ms << ",";
        ss << "" << ","; // price_diff
        ss << "" << ","; // matched_with
        ss << ""; // note

        write_row_locked(sym, ss.str());
    }

    void emit_compare_row_locked(const TradeEvent &mmap_ev, const TradeEvent &ws_ev, int64_t dt_ms, double price_diff) {
        std::string sym = upper(mmap_ev.symbol);
        if (!allowed_.empty() && !allowed_.count(sym)) return;
        ensure_csv_locked(sym);
        if (!ofs_.count(sym)) return;

        std::ostringstream ss;
        ss << now_iso_ms() << ",";
        ss << sym << ",";
        ss << "compare" << ",";
        ss << mmap_ev.prefix << "|ws" << ",";
        ss << 2 << ","; // kind=compare
        ss << mmap_ev.seq << ",";
        if (std::isfinite(mmap_ev.px)) ss << mmap_ev.px; ss << ",";
        if (std::isfinite(mmap_ev.sz)) ss << mmap_ev.sz; ss << ",";
        ss << "" << "," << "" << "," << "" << ",";
        ss << mmap_ev.recv_ts << ",";
        ss << ws_ev.ws_ts << ",";
        ss << (uint64_t)(std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::system_clock::now().time_since_epoch()).count()) << ",";
        if (std::isfinite(price_diff)) ss << price_diff; ss << ",";
        ss << ws_ev.seq << ",";
        ss << "dt_ms=" << dt_ms;
        write_row_locked(sym, ss.str());
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

                // write event row (only allowed symbols will actually be written)
                {
                    std::lock_guard<std::mutex> g(mu_);
                    emit_event_row_locked(ev);
                }

                // matching: compare ws <> mmap within match_ms_
                std::string sym = upper(ev.symbol);
                if (ev.is_ws) {
                    auto &dq = recent_ws_[sym];
                    dq.push_back(ev);
                    if (dq.size() > window_max) dq.pop_front();
                    auto &mdq = recent_mmap_[sym];
                    for (auto it = mdq.rbegin(); it != mdq.rend(); ++it) {
                        uint64_t m_ts = it->recv_ts;
                        uint64_t w_ts = ev.ws_ts;
                        if (m_ts == 0 || w_ts == 0) continue;
                        int64_t dt = (int64_t)w_ts - (int64_t)m_ts;
                        if (std::llabs(dt) <= match_ms_) {
                            double price_diff = NAN;
                            if (std::isfinite(it->px) && std::isfinite(ev.px)) price_diff = it->px - ev.px;
                            std::lock_guard<std::mutex> g(mu_);
                            emit_compare_row_locked(*it, ev, (int)std::llabs(dt), price_diff);
                            break;
                        }
                    }
                } else {
                    auto &dq = recent_mmap_[sym];
                    dq.push_back(ev);
                    if (dq.size() > window_max) dq.pop_front();
                    auto &wdq = recent_ws_[sym];
                    for (auto it = wdq.rbegin(); it != wdq.rend(); ++it) {
                        uint64_t m_ts = ev.recv_ts;
                        uint64_t w_ts = it->ws_ts;
                        if (m_ts == 0 || w_ts == 0) continue;
                        int64_t dt = (int64_t)w_ts - (int64_t)m_ts;
                        if (std::llabs(dt) <= match_ms_) {
                            double price_diff = NAN;
                            if (std::isfinite(ev.px) && std::isfinite(it->px)) price_diff = ev.px - it->px;
                            std::lock_guard<std::mutex> g(mu_);
                            emit_compare_row_locked(ev, *it, (int)std::llabs(dt), price_diff);
                            break;
                        }
                    }
                }

                lk.lock();
            } // while inbox
        } // while not stop
    }
};

// ---- MmapWatcher: reads *_trade_chunk*.meta + .data and pushes kind==1 trade events ----
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
    std::unordered_set<std::string> allowed_; // uppercase symbols allowed

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
        // header
        std::vector<uint8_t> hdr(RECORD_HEADER_LEN);
        for (size_t i=0;i<RECORD_HEADER_LEN;i++) hdr[i] = rs.data_ptr[(off + i) % cap];
        uint32_t plen; memcpy(&plen, hdr.data(), 4);
        uint8_t kind = hdr[4];
        uint64_t seq; memcpy(&seq, hdr.data() + 12, 8);
        if (kind != expected_kind) return false;
        if (seq != expected_seq) return false;
        // payload
        std::vector<uint8_t> payload(plen);
        for (size_t i=0;i<plen;i++) payload[i] = rs.data_ptr[(off + RECORD_HEADER_LEN + i) % cap];
        try {
            std::string s((char*)payload.data(), payload.size());
            auto j = json::parse(s);
            std::string sym;
            if (j.contains("asset")) sym = j["asset"].get<std::string>();
            else if (j.contains("s")) sym = j["s"].get<std::string>();
            else if (j.contains("symbol")) sym = j["symbol"].get<std::string>();
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
        // cleanup removed
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
                                // only push if allowed (or allowed_ empty)
                                if (allowed_.empty() || allowed_.count(ev.symbol)) {
                                    std::cerr << "[mmap-trade] prefix=" << ev.prefix << " seq=" << ev.seq
                                              << " sym=" << ev.symbol << " px=" << ev.px << " sz=" << ev.sz << "\n";
                                    engine_.push_event(ev);
                                }
                                // otherwise silently ignore (no prints)
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

// ---- BinanceWS: each client connects and pushes parsed trade events to engine ----
class BinanceWS {
public:
    BinanceWS(const std::string &symbol_upper, CSVEngine &engine)
        : symbol_upper_(symbol_upper), engine_(engine), stop_(false)
    {
        // symbol_upper expected uppercase
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
        // using single-stream path for straightforward parsing
        std::string stream = lower(symbol_upper_) + "@trade";
        std::string target = "/ws/" + stream;

        std::cerr << "[ws:" << symbol_upper_ << "] connecting to wss://" << host << target << "\n";

        // resolve
        tcp::resolver resolver(ioc);
        auto const results = resolver.resolve(host, port);

        // socket and connect
        tcp::socket sock(ioc);
        net::connect(sock, results);

        // wrap socket with SSL stream
        boost::asio::ssl::stream<tcp::socket> ssl_stream(std::move(sock), ssl_ctx);
        ssl_stream.set_verify_mode(boost::asio::ssl::verify_peer);
        ssl_stream.set_verify_callback(boost::asio::ssl::rfc2818_verification(host));

        // SSL handshake
        try {
            ssl_stream.handshake(boost::asio::ssl::stream_base::client);
        } catch (const std::exception &e) {
            std::cerr << "[ws:" << symbol_upper_ << "] ssl handshake failed: " << e.what() << "\n";
            return;
        }

        // websocket over SSL stream
        websocket::stream<boost::asio::ssl::stream<tcp::socket>> ws(std::move(ssl_stream));
        ws.set_option(websocket::stream_base::timeout::suggested(beast::role_type::client));
        ws.set_option(websocket::stream_base::decorator([](websocket::request_type &req){
            req.set(beast::http::field::user_agent, std::string("mmap_trade_compare_cpp/1.0"));
        }));

        // handshake
        try {
            ws.handshake(host, target);
        } catch (const std::exception &e) {
            std::cerr << "[ws:" << symbol_upper_ << "] websocket handshake failed: " << e.what() << "\n";
            return;
        }

        std::cerr << "[ws:" << symbol_upper_ << "] connected\n";

        beast::flat_buffer buffer;
        while (!stop_) {
            beast::error_code ec;
            ws.read(buffer, ec);
            if (ec) {
                std::cerr << "[ws:" << symbol_upper_ << "] read error: " << ec.message() << "\n";
                break;
            }
            std::string data = beast::buffers_to_string(buffer.data());
            buffer.consume(buffer.size());
            std::cerr << "[ws:" << symbol_upper_ << "] raw: " << data << "\n";

            try {
                auto j = json::parse(data);
                json rec = j;
                // when using /ws/<stream> we get trade JSON directly; other wrappers will be accepted too
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
                    std::cerr << "[ws-trade] parsed sym=" << ev.symbol << " px=" << ev.px << " sz=" << ev.sz << " id=" << ev.seq << " ws_ts=" << ev.ws_ts << "\n";

                    // only push if allowed (CSVEngine will also gate, but do this early to avoid extra work)
                    if (allowed_for(ev.symbol)) engine_.push_event(ev);
                } else {
                    // not a trade message
                    std::cerr << "[ws:" << symbol_upper_ << "] non-trade message\n";
                }
            } catch (std::exception &e) {
                std::cerr << "[ws:" << symbol_upper_ << "] json parse failed: " << e.what() << "\n";
            }
        }

        beast::error_code ec2;
        ws.close(websocket::close_code::normal, ec2);
    }

    bool allowed_for(const std::string &sym) const {
        // allowed check based on uppercase symbol_upper_ or configured allowed set in CSVEngine (double-check there)
        // Here we allow only the symbol we were constructed for (symbol_upper_)
        return sym == symbol_upper_;
    }
};

// ---- Filters loader ----
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

// ---- CLI args ----
struct Args {
    std::string rings = "rings";
    std::string filters = "filters.json";
    std::string out = "out";
    int poll_ms = 50;
    int match_ms = 500;
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
    CSVEngine engine(args.out, allowed_set, 100, false, args.match_ms);

    MmapWatcher watcher(args.rings, engine, allowed_set, args.poll_ms);

    // spawn websocket clients for allowed symbols
    std::vector<std::unique_ptr<BinanceWS>> clients;
    for (auto &sym : f.symbols) {
        std::string up = sym;
        std::transform(up.begin(), up.end(), up.begin(), ::toupper);
        std::cerr << "starting ws client for " << up << "\n";
        clients.emplace_back(new BinanceWS(up, engine));
    }

    std::cerr << "running. Ctrl-C to quit.\n";
    std::signal(SIGINT, [](int){ std::cerr << "SIGINT\n"; std::exit(0); });

    while (true) std::this_thread::sleep_for(1s);
    return 0;
}
