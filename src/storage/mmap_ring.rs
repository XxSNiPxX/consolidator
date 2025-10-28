// src/storage/mmap_ring.rs
//! Mmap-backed byte-tail circular buffer with index slots in .meta
//! HFT-hardened: CRC, publish-seq, background flusher, mlock+touch.

use anyhow::{bail, Context, Result};
use crc32fast::Hasher;
use memmap2::{MmapMut, MmapOptions};
use parking_lot::Mutex;
use std::fs::OpenOptions;
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

#[cfg(unix)]
use libc;

pub const DEFAULT_META_SIZE: usize = 64 * 1024; // 64 KiB minimum
pub const META_MAGIC: &[u8; 8] = b"RINGV1\x00\x00";
pub const META_HEADER_LEN: usize = 40; // 8 + 8 + 8 + 8 + 8
pub const INDEX_SLOT_SIZE: usize = 24;
pub const RECORD_HEADER_LEN: usize = 20; // [u32 len][u8 kind][7 padding][u64 seq]
pub const CRC_LEN: usize = 4;
pub const MAX_INDEX_SLOTS: usize = 10_000_000; // safety bound

#[derive(Clone, Debug)]
pub struct MmapRingConfig {
    pub path_prefix: String,
    pub data_capacity: usize,
    pub index_slots: usize,
    pub meta_size: usize,
    pub create: bool,
    pub force_reinit: bool,
    pub sync_writes: bool,        // durable mode (per-write flush) if true
    pub flusher_interval_ms: u64, // background flusher interval when sync_writes == false
}

pub struct MmapRing {
    pub meta_path: String,
    pub data_path: String,
    pub data_capacity: usize,
    meta_mmap: Arc<Mutex<MmapMut>>,
    data_mmap: Arc<Mutex<MmapMut>>,
    // serialize logical writes
    write_lock: Mutex<()>,
    pub index_slots: usize,
    pub sync_writes: bool,
}

impl MmapRing {
    pub fn compute_meta_size(index_slots: usize) -> Result<usize> {
        if index_slots == 0 {
            bail!("index_slots must be > 0");
        }
        if index_slots > MAX_INDEX_SLOTS {
            bail!(
                "index_slots {} too large (max {})",
                index_slots,
                MAX_INDEX_SLOTS
            );
        }
        let idx_area = index_slots
            .checked_mul(INDEX_SLOT_SIZE)
            .ok_or_else(|| anyhow::anyhow!("index_slots * INDEX_SLOT_SIZE overflow"))?;
        let required = META_HEADER_LEN
            .checked_add(idx_area)
            .ok_or_else(|| anyhow::anyhow!("meta size computation overflow"))?;
        // round up to 4 KiB
        let rounded = ((required + 4095) / 4096) * 4096;
        Ok(rounded)
    }

    pub fn open(cfg: &MmapRingConfig) -> Result<Self> {
        let meta_path = format!("{}.meta", cfg.path_prefix);
        let data_path = format!("{}.data", cfg.path_prefix);

        let meta_size = if cfg.meta_size < META_HEADER_LEN {
            DEFAULT_META_SIZE
        } else {
            cfg.meta_size
        };

        if cfg.data_capacity == 0 {
            bail!("data_capacity must be > 0");
        }
        if cfg.index_slots == 0 {
            bail!("index_slots must be > 0");
        }

        // ensure parent dir exists
        if let Some(parent) = Path::new(&cfg.path_prefix).parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create_dir_all {:?}", parent))?;
        }

        // open/create files
        let meta_file = OpenOptions::new()
            .create(cfg.create)
            .read(true)
            .write(true)
            .open(&meta_path)
            .with_context(|| format!("open meta {}", meta_path))?;
        meta_file.set_len(meta_size as u64)?;

        let data_file = OpenOptions::new()
            .create(cfg.create)
            .read(true)
            .write(true)
            .open(&data_path)
            .with_context(|| format!("open data {}", data_path))?;
        data_file.set_len(cfg.data_capacity as u64)?;

        // mmap files
        let mut meta_m = unsafe { MmapOptions::new().len(meta_size).map_mut(&meta_file)? };
        let data_m = unsafe {
            MmapOptions::new()
                .len(cfg.data_capacity)
                .map_mut(&data_file)?
        };

        // initialize header if magic missing or force_reinit
        let magic_present = &meta_m[0..8] == META_MAGIC;
        if !magic_present || cfg.create || cfg.force_reinit {
            // write header
            meta_m[0..8].copy_from_slice(META_MAGIC);
            meta_m[8..16].copy_from_slice(&(cfg.data_capacity as u64).to_le_bytes());
            meta_m[16..24].copy_from_slice(&0u64.to_le_bytes()); // tail_offset
            meta_m[24..32].copy_from_slice(&0u64.to_le_bytes()); // seq_counter
            meta_m[32..40].copy_from_slice(&(cfg.index_slots as u64).to_le_bytes()); // index_slots

            // zero index area
            let index_area_len = cfg.index_slots * INDEX_SLOT_SIZE;
            let needed_meta_len = META_HEADER_LEN + index_area_len;
            if meta_size < needed_meta_len {
                bail!(
                    "meta_size {} too small for index_slots {}; need {}",
                    meta_size,
                    cfg.index_slots,
                    needed_meta_len
                );
            }
            for i in META_HEADER_LEN..(META_HEADER_LEN + index_area_len) {
                meta_m[i] = 0;
            }
            // flush header immediately (creation time)
            meta_m.flush()?;
        } else {
            // header existed: validate data_capacity and index_slots
            let cap = u64::from_le_bytes(meta_m[8..16].try_into().unwrap()) as usize;
            if cap != cfg.data_capacity {
                bail!(
                    "data_capacity mismatch: meta has {} but config requests {}",
                    cap,
                    cfg.data_capacity
                );
            }
            let idx_slots = u64::from_le_bytes(meta_m[32..40].try_into().unwrap()) as usize;
            if idx_slots != cfg.index_slots {
                bail!(
                    "index_slots mismatch: meta has {} but config requests {}",
                    idx_slots,
                    cfg.index_slots
                );
            }
        }

        let ring = MmapRing {
            meta_path,
            data_path,
            data_capacity: cfg.data_capacity,
            meta_mmap: Arc::new(Mutex::new(meta_m)),
            data_mmap: Arc::new(Mutex::new(data_m)),
            write_lock: Mutex::new(()),
            index_slots: cfg.index_slots,
            sync_writes: cfg.sync_writes,
        };

        // try to mlock and touch pages to avoid page-fault jitter
        ring.mlock_and_touch_if_possible()?;

        // background flusher for non-durable mode
        if !cfg.sync_writes {
            ring.spawn_background_flusher(cfg.flusher_interval_ms);
        }

        Ok(ring)
    }

    fn read_meta_tail_locked(meta: &MmapMut) -> usize {
        let bytes = &meta[16..24];
        u64::from_le_bytes(bytes.try_into().unwrap()) as usize
    }

    fn read_meta_seq_locked(meta: &MmapMut) -> u64 {
        let bytes = &meta[24..32];
        u64::from_le_bytes(bytes.try_into().unwrap())
    }

    fn write_index_slot_locked(
        meta: &mut MmapMut,
        idx: usize,
        off: u64,
        ln: u32,
        seq: u64,
        kind: u8,
    ) {
        let slot_base = META_HEADER_LEN + idx * INDEX_SLOT_SIZE;
        meta[slot_base..slot_base + 8].copy_from_slice(&off.to_le_bytes());
        meta[slot_base + 8..slot_base + 12].copy_from_slice(&ln.to_le_bytes());
        meta[slot_base + 12..slot_base + 20].copy_from_slice(&seq.to_le_bytes());
        meta[slot_base + 20] = kind;
        // padding left as-is
    }

    /// Write record. Layout:
    /// [u32 payload_len][u8 kind][7 padding][u64 seq][payload bytes][crc32 u32]
    pub fn write_record(&self, kind: u8, payload: &[u8]) -> Result<u64> {
        let _wl = self.write_lock.lock();

        let total_len = RECORD_HEADER_LEN + payload.len() + CRC_LEN;
        if total_len > self.data_capacity {
            bail!("single record too large for data_capacity");
        }

        let mut meta_guard = self.meta_mmap.lock();
        let mut data_guard = self.data_mmap.lock();

        let tail = Self::read_meta_tail_locked(&meta_guard);
        let cur_seq = Self::read_meta_seq_locked(&meta_guard);
        let seq = cur_seq.saturating_add(1);

        // header
        let mut header = [0u8; RECORD_HEADER_LEN];
        header[0..4].copy_from_slice(&(payload.len() as u32).to_le_bytes());
        header[4] = kind;
        header[12..20].copy_from_slice(&seq.to_le_bytes());

        // crc
        let mut hasher = Hasher::new();
        hasher.update(&header);
        hasher.update(payload);
        let crc = hasher.finalize();
        let crc_bytes = crc.to_le_bytes();

        let data_cap = self.data_capacity;
        let write_off = tail % data_cap;

        // write with wrap handling (kept explicit to be safe)
        if write_off + total_len <= data_cap {
            data_guard[write_off..write_off + RECORD_HEADER_LEN].copy_from_slice(&header);
            data_guard
                [write_off + RECORD_HEADER_LEN..write_off + RECORD_HEADER_LEN + payload.len()]
                .copy_from_slice(payload);
            data_guard[write_off + RECORD_HEADER_LEN + payload.len()..write_off + total_len]
                .copy_from_slice(&crc_bytes);
        } else {
            let first_part = data_cap - write_off;
            if first_part >= RECORD_HEADER_LEN {
                let header_part = RECORD_HEADER_LEN;
                data_guard[write_off..write_off + header_part].copy_from_slice(&header);
                let remaining_first = first_part - header_part;
                let to_copy_first = std::cmp::min(remaining_first, payload.len());
                if to_copy_first > 0 {
                    let dst = write_off + header_part;
                    data_guard[dst..dst + to_copy_first].copy_from_slice(&payload[..to_copy_first]);
                }
                let remaining = payload.len() - std::cmp::min(remaining_first, payload.len());
                if remaining > 0 {
                    data_guard[..remaining].copy_from_slice(&payload[payload.len() - remaining..]);
                    // crc after that
                    data_guard[remaining..remaining + CRC_LEN].copy_from_slice(&crc_bytes);
                } else {
                    // crc may wrap
                    let crc_pos = (write_off + header_part + to_copy_first) % data_cap;
                    if crc_pos + CRC_LEN <= data_cap {
                        data_guard[crc_pos..crc_pos + CRC_LEN].copy_from_slice(&crc_bytes);
                    } else {
                        let c_first = data_cap - crc_pos;
                        data_guard[crc_pos..data_cap].copy_from_slice(&crc_bytes[..c_first]);
                        data_guard[..CRC_LEN - c_first]
                            .copy_from_slice(&crc_bytes[c_first..CRC_LEN]);
                    }
                }
            } else {
                // header crosses boundary
                let h_first = first_part;
                data_guard[write_off..data_cap].copy_from_slice(&header[..h_first]);
                let h_second = RECORD_HEADER_LEN - h_first;
                data_guard[..h_second].copy_from_slice(&header[h_first..RECORD_HEADER_LEN]);
                let payload_start = h_second;
                if payload_start + payload.len() <= data_cap {
                    data_guard[payload_start..payload_start + payload.len()]
                        .copy_from_slice(payload);
                    let crc_pos = payload_start + payload.len();
                    if crc_pos + CRC_LEN <= data_cap {
                        data_guard[crc_pos..crc_pos + CRC_LEN].copy_from_slice(&crc_bytes);
                    } else {
                        let c_first = data_cap - crc_pos;
                        data_guard[crc_pos..data_cap].copy_from_slice(&crc_bytes[..c_first]);
                        data_guard[..CRC_LEN - c_first]
                            .copy_from_slice(&crc_bytes[c_first..CRC_LEN]);
                    }
                } else {
                    let part2 = data_cap - payload_start;
                    data_guard[payload_start..data_cap].copy_from_slice(&payload[..part2]);
                    data_guard[..payload.len() - part2].copy_from_slice(&payload[part2..]);
                    let crc_pos = (payload.len() - part2) % data_cap;
                    if crc_pos + CRC_LEN <= data_cap {
                        data_guard[crc_pos..crc_pos + CRC_LEN].copy_from_slice(&crc_bytes);
                    } else {
                        let c_first = data_cap - crc_pos;
                        data_guard[crc_pos..data_cap].copy_from_slice(&crc_bytes[..c_first]);
                        data_guard[..CRC_LEN - c_first]
                            .copy_from_slice(&crc_bytes[c_first..CRC_LEN]);
                    }
                }
            }
        }

        // update index slot
        let idx = (seq as usize) % self.index_slots;
        let off_u64 = write_off as u64;
        let ln_u32 = total_len as u32;
        Self::write_index_slot_locked(&mut meta_guard, idx, off_u64, ln_u32, seq, kind);

        // update tail & seq (publish cursor)
        let new_tail = (tail + total_len) % data_cap;
        meta_guard[16..24].copy_from_slice(&(new_tail as u64).to_le_bytes());
        meta_guard[24..32].copy_from_slice(&seq.to_le_bytes());

        // durable mode flush if requested; otherwise background flusher handles it
        if self.sync_writes {
            meta_guard.flush()?;
            data_guard.flush()?;
        }

        Ok(seq)
    }

    /// Read tail and seq (for readers)
    pub fn read_tail_and_seq(&self) -> (usize, u64) {
        let meta_guard = self.meta_mmap.lock();
        let tail = Self::read_meta_tail_locked(&meta_guard);
        let seq = Self::read_meta_seq_locked(&meta_guard);
        (tail, seq)
    }

    fn spawn_background_flusher(&self, interval_ms: u64) {
        let meta = Arc::clone(&self.meta_mmap);
        let data = Arc::clone(&self.data_mmap);
        let intv = if interval_ms == 0 { 50 } else { interval_ms };
        thread::spawn(move || loop {
            thread::sleep(Duration::from_millis(intv));
            let _ = meta.lock().flush();
            let _ = data.lock().flush();
        });
    }

    fn mlock_and_touch_if_possible(&self) -> Result<()> {
        #[cfg(unix)]
        {
            let meta_len = {
                let m = self.meta_mmap.lock();
                m.len()
            };
            let data_len = {
                let d = self.data_mmap.lock();
                d.len()
            };
            unsafe {
                let meta_ptr = self.meta_mmap.lock().as_ptr();
                if libc::mlock(meta_ptr as *const _, meta_len) != 0 {
                    let err = std::io::Error::last_os_error();
                    tracing::warn!("mlock(meta) failed: {:?}", err);
                }
                let data_ptr = self.data_mmap.lock().as_ptr();
                if libc::mlock(data_ptr as *const _, data_len) != 0 {
                    let err = std::io::Error::last_os_error();
                    tracing::warn!("mlock(data) failed: {:?}", err);
                }
                let page = 4096usize;
                for i in (0..meta_len).step_by(page) {
                    std::ptr::read_volatile(meta_ptr.add(i));
                }
                for i in (0..data_len).step_by(page) {
                    std::ptr::read_volatile(data_ptr.add(i));
                }
            }
        }
        #[cfg(not(unix))]
        { /* no-op */ }
        Ok(())
    }

    /// Convenience reader that reads record by seq and validates CRC and seq in header.
    pub fn read_record_by_seq(&self, seq: u64) -> Result<(u8, Vec<u8>)> {
        let meta_guard = self.meta_mmap.lock();
        let idx = (seq as usize) % self.index_slots;
        let slot_base = META_HEADER_LEN + idx * INDEX_SLOT_SIZE;
        let off =
            u64::from_le_bytes(meta_guard[slot_base..slot_base + 8].try_into().unwrap()) as usize;
        let ln = u32::from_le_bytes(
            meta_guard[slot_base + 8..slot_base + 12]
                .try_into()
                .unwrap(),
        ) as usize;
        let slot_seq = u64::from_le_bytes(
            meta_guard[slot_base + 12..slot_base + 20]
                .try_into()
                .unwrap(),
        );
        let kind = meta_guard[slot_base + 20];
        drop(meta_guard);

        if slot_seq != seq {
            bail!(
                "index slot seq mismatch (expected {}, slot {})",
                seq,
                slot_seq
            );
        }
        if ln < RECORD_HEADER_LEN + CRC_LEN {
            bail!("index slot length too small");
        }

        let data_guard = self.data_mmap.lock();
        let data_cap = self.data_capacity;
        let mut raw = vec![0u8; ln];
        let end = off + ln;
        if end <= data_cap {
            raw.copy_from_slice(&data_guard[off..end]);
        } else {
            let part1 = data_cap - off;
            raw[..part1].copy_from_slice(&data_guard[off..data_cap]);
            raw[part1..].copy_from_slice(&data_guard[..(ln - part1)]);
        }
        let payload_len = u32::from_le_bytes(raw[0..4].try_into().unwrap()) as usize;
        let kind_b = raw[4];
        let seq_in_header = u64::from_le_bytes(raw[12..20].try_into().unwrap());
        if seq_in_header != seq {
            bail!(
                "seq mismatch in header (expected {}, found {})",
                seq,
                seq_in_header
            );
        }
        if payload_len + RECORD_HEADER_LEN + CRC_LEN != ln {
            bail!("payload length mismatch");
        }
        let payload = raw[RECORD_HEADER_LEN..RECORD_HEADER_LEN + payload_len].to_vec();
        let crc_stored = u32::from_le_bytes(
            raw[RECORD_HEADER_LEN + payload_len..RECORD_HEADER_LEN + payload_len + 4]
                .try_into()
                .unwrap(),
        );
        let mut hasher = Hasher::new();
        hasher.update(&raw[0..RECORD_HEADER_LEN]);
        hasher.update(&payload);
        let crc_calc = hasher.finalize();
        if crc_calc != crc_stored {
            bail!("crc mismatch for seq {}", seq);
        }
        Ok((kind_b, payload))
    }
}
