//! Mmap-backed byte-tail circular buffer with index slots in .meta
//!
//! Meta layout:
//! 0..8   : magic bytes (RINGV1\0\0)
//! 8..16  : data_capacity (u64 LE)
//! 16..24 : tail_offset (u64 LE)
//! 24..32 : seq_counter (u64 LE)
//! 32..40 : index_slots (u64 LE)
//! 40..   : index_slots * INDEX_SLOT_SIZE  (index slot array)
//!
//! Index slot layout (INDEX_SLOT_SIZE = 24):
//! 0..8   : off (u64 LE)
//! 8..12  : len (u32 LE)
//! 12..20 : seq (u64 LE)
//! 20     : kind (u8)
//! 21..24 : padding
//!
//! Data layout:
//! For each record:
//! [0..4)   payload_len: u32 LE
//! [4]      kind: u8
//! [5..12)  padding (7 bytes)
//! [12..20) seq: u64 LE
//! [20..]   payload payload_len bytes (utf-8 JSON)

use anyhow::{bail, Context, Result};
use memmap2::{MmapMut, MmapOptions};
use parking_lot::Mutex;
use std::fs::OpenOptions;
use std::path::Path;
use std::sync::Arc;

pub const DEFAULT_META_SIZE: usize = 64 * 1024; // 64 KiB minimum
pub const META_MAGIC: &[u8; 8] = b"RINGV1\x00\x00";
pub const META_HEADER_LEN: usize = 40; // 8 + 8 + 8 + 8 + 8
pub const INDEX_SLOT_SIZE: usize = 24;
pub const RECORD_HEADER_LEN: usize = 20;
pub const MAX_INDEX_SLOTS: usize = 10_000_000; // safety bound

#[derive(Clone, Debug)]
pub struct MmapRingConfig {
    pub path_prefix: String,
    pub data_capacity: usize,
    pub index_slots: usize,
    pub meta_size: usize,
    pub create: bool,
    pub force_reinit: bool,
    pub sync_writes: bool,
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
    /// Compute the minimal meta size required for given index_slots. Rounds up to 4 KiB.
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

        Ok(MmapRing {
            meta_path,
            data_path,
            data_capacity: cfg.data_capacity,
            meta_mmap: Arc::new(Mutex::new(meta_m)),
            data_mmap: Arc::new(Mutex::new(data_m)),
            write_lock: Mutex::new(()),
            index_slots: cfg.index_slots,
            sync_writes: cfg.sync_writes,
        })
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
        // padding remains
    }

    /// Write a record and update index slot. Returns sequence number.
    pub fn write_record(&self, kind: u8, payload: &[u8]) -> Result<u64> {
        let _wl = self.write_lock.lock();

        let record_header_len = 4 + 1 + 7 + 8; // 20
        if payload.len() + record_header_len > self.data_capacity {
            bail!("single record too large for data_capacity");
        }

        // locks
        let mut meta_guard = self.meta_mmap.lock();
        let mut data_guard = self.data_mmap.lock();

        let tail = Self::read_meta_tail_locked(&meta_guard);
        let cur_seq = Self::read_meta_seq_locked(&meta_guard);
        let seq = cur_seq.saturating_add(1);

        // prepare header: [payload_len:u32][kind:u8][pad:7][seq:u64]
        let mut header = [0u8; RECORD_HEADER_LEN];
        header[0..4].copy_from_slice(&(payload.len() as u32).to_le_bytes());
        header[4] = kind;
        header[12..20].copy_from_slice(&seq.to_le_bytes());

        let total_len = record_header_len + payload.len();
        let data_cap = self.data_capacity;
        let write_off = tail % data_cap;

        // write record with wrap awareness
        if write_off + total_len <= data_cap {
            data_guard[write_off..write_off + record_header_len].copy_from_slice(&header);
            data_guard[write_off + record_header_len..write_off + total_len]
                .copy_from_slice(payload);
        } else {
            let first_part = data_cap - write_off;
            if first_part >= record_header_len {
                let header_part = record_header_len;
                data_guard[write_off..write_off + header_part].copy_from_slice(&header);
                let remaining_first = first_part - header_part;
                let to_copy_first = std::cmp::min(remaining_first, payload.len());
                if to_copy_first > 0 {
                    let dst_start = write_off + header_part;
                    data_guard[dst_start..dst_start + to_copy_first]
                        .copy_from_slice(&payload[..to_copy_first]);
                }
                let remaining = payload.len() - std::cmp::min(remaining_first, payload.len());
                if remaining > 0 {
                    data_guard[..remaining].copy_from_slice(&payload[payload.len() - remaining..]);
                }
            } else {
                // header itself crosses boundary
                let h_first = first_part;
                data_guard[write_off..data_cap].copy_from_slice(&header[..h_first]);
                let h_second = record_header_len - h_first;
                data_guard[..h_second].copy_from_slice(&header[h_first..record_header_len]);
                // then payload
                let payload_start = h_second;
                if payload_start + payload.len() <= data_cap {
                    data_guard[payload_start..payload_start + payload.len()]
                        .copy_from_slice(payload);
                } else {
                    let part2 = data_cap - payload_start;
                    data_guard[payload_start..data_cap].copy_from_slice(&payload[..part2]);
                    data_guard[..payload.len() - part2].copy_from_slice(&payload[part2..]);
                }
            }
        }

        // flush data iff requested (sync_writes). Otherwise only meta is flushed.
        if self.sync_writes {
            data_guard.flush()?;
        }

        // update index slot
        let idx = (seq as usize) % self.index_slots;
        let off_u64 = write_off as u64;
        let ln_u32 = total_len as u32;
        Self::write_index_slot_locked(&mut meta_guard, idx, off_u64, ln_u32, seq, kind);

        // update meta tail and seq
        let new_tail = (tail + total_len) % data_cap;
        meta_guard[16..24].copy_from_slice(&(new_tail as u64).to_le_bytes());
        meta_guard[24..32].copy_from_slice(&seq.to_le_bytes());

        // persist meta (so readers see tail/seq change)
        meta_guard.flush()?;

        Ok(seq)
    }

    /// Read tail and seq (for readers)
    pub fn read_tail_and_seq(&self) -> (usize, u64) {
        let meta_guard = self.meta_mmap.lock();
        let tail = Self::read_meta_tail_locked(&meta_guard);
        let seq = Self::read_meta_seq_locked(&meta_guard);
        (tail, seq)
    }
}
