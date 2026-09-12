use std::path::Path;

use rsf_core::{
    CommitVersion, Epoch, ErrorCode, LogIndex, PlacementGroupId, RsfError, RsfResult,
    storage::WalRecordKind,
};
use rsf_storage::{SegmentScanFilter, SegmentStore, WalAppend};

use crate::{
    CacheAdmission, CacheEntry, CacheHit, CacheKey, CachePolicy, CacheRead, CacheSurface,
    ReadAheadCandidate, ReadAheadPlan, ReadCache, StrictCacheRead,
};

const CACHE_PG: PlacementGroupId = PlacementGroupId(55);
const CACHE_WAL_MAGIC: &[u8; 7] = b"RSFCCH1";
const CACHE_WAL_VERSION: u8 = 1;

#[derive(Debug)]
pub struct DurableReadCache {
    cache: ReadCache,
    store: SegmentStore,
    epoch: Epoch,
    next_index: u64,
}

impl DurableReadCache {
    pub fn open(path: impl AsRef<Path>, epoch: Epoch, policy: CachePolicy) -> RsfResult<Self> {
        let store = SegmentStore::open(path)?;
        let scan = store.scan(SegmentScanFilter { pg: Some(CACHE_PG) })?;
        let mut cache = ReadCache::new(policy)?;
        let mut next_index = 1;

        for record in scan
            .wal_records
            .iter()
            .filter(|record| record.kind == WalRecordKind::Cache)
        {
            apply_cache_record(&mut cache, decode_cache_wal_payload(&record.payload)?)?;
            next_index = next_index.max(record.index.0.saturating_add(1));
        }

        Ok(Self {
            cache,
            store,
            epoch,
            next_index,
        })
    }

    pub fn put(&mut self, entry: CacheEntry) -> RsfResult<CacheAdmission> {
        let mut cache = self.cache.clone();
        let admission = cache.put(entry.clone())?;
        if admission.is_stored() {
            self.append_record(&CacheWalRecord::Put { entry })?;
            self.cache = cache;
        }
        Ok(admission)
    }

    pub fn read(&mut self, key: &CacheKey, min_version: CommitVersion) -> RsfResult<CacheRead> {
        let mut cache = self.cache.clone();
        let read = cache.read(key, min_version);
        match &read {
            CacheRead::Hit(CacheHit { key, version, .. }) => {
                self.append_record(&CacheWalRecord::Touch {
                    key: key.clone(),
                    version: *version,
                })?;
                self.cache = cache;
            }
            CacheRead::Stale {
                key,
                cached_version,
                required_version,
            } => {
                self.append_record(&CacheWalRecord::StaleInvalidate {
                    key: key.clone(),
                    cached_version: *cached_version,
                    required_version: *required_version,
                })?;
                self.cache = cache;
            }
            CacheRead::Miss { .. } => {}
        }
        Ok(read)
    }

    pub fn strict_read(
        &mut self,
        key: &CacheKey,
        required_version: CommitVersion,
    ) -> RsfResult<StrictCacheRead> {
        match self.read(key, required_version)? {
            CacheRead::Hit(hit) => Ok(StrictCacheRead::Serve(hit)),
            CacheRead::Miss { key, reason } => Ok(StrictCacheRead::ReadThrough {
                key,
                required_version,
                reason,
            }),
            CacheRead::Stale {
                key,
                cached_version,
                required_version,
            } => Ok(StrictCacheRead::ReadThrough {
                key,
                required_version,
                reason: format!(
                    "cached version {} is older than required {}",
                    cached_version.0, required_version.0
                ),
            }),
        }
    }

    pub fn read_ahead(
        &mut self,
        anchor: CacheKey,
        required_version: CommitVersion,
        candidates: impl IntoIterator<Item = ReadAheadCandidate>,
    ) -> RsfResult<ReadAheadPlan> {
        let mut cache = self.cache.clone();
        let plan = cache.read_ahead(anchor, required_version, candidates)?;
        for key in &plan.evicted {
            self.append_record(&CacheWalRecord::Invalidate { key: key.clone() })?;
        }
        self.cache = cache;
        Ok(plan)
    }

    pub fn invalidate(&mut self, key: &CacheKey) -> RsfResult<Option<CacheEntry>> {
        let mut cache = self.cache.clone();
        let removed = cache.invalidate(key);
        if removed.is_some() {
            self.append_record(&CacheWalRecord::Invalidate { key: key.clone() })?;
            self.cache = cache;
        }
        Ok(removed)
    }

    pub fn len(&self) -> usize {
        self.cache.len()
    }

    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }

    pub fn used_bytes(&self) -> u64 {
        self.cache.used_bytes()
    }

    fn append_record(&mut self, record: &CacheWalRecord) -> RsfResult<()> {
        let payload = encode_cache_wal_payload(record)?;
        let recovery = self.store.recover_current()?.recovery;
        let tail = recovery.pg_tail(CACHE_PG);
        let previous_hash = tail.map_or([0_u8; 32], |tail| tail.record_hash);
        let index = LogIndex(self.next_index);
        let (_, _, record) = self.store.append_wal(WalAppend {
            pg: CACHE_PG,
            epoch: self.epoch,
            index,
            kind: WalRecordKind::Cache,
            previous_hash,
            payload: &payload,
        })?;
        self.next_index = record.index.0.saturating_add(1);
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum CacheWalRecord {
    Put {
        entry: CacheEntry,
    },
    Touch {
        key: CacheKey,
        version: CommitVersion,
    },
    StaleInvalidate {
        key: CacheKey,
        cached_version: CommitVersion,
        required_version: CommitVersion,
    },
    Invalidate {
        key: CacheKey,
    },
}

fn apply_cache_record(cache: &mut ReadCache, record: CacheWalRecord) -> RsfResult<()> {
    match record {
        CacheWalRecord::Put { entry } => match cache.put(entry)? {
            CacheAdmission::Stored { .. } => Ok(()),
            CacheAdmission::Rejected { reason, .. } => Err(corrupt(format!(
                "cache WAL put replay was rejected: {reason}"
            ))),
        },
        CacheWalRecord::Touch { key, version } => match cache.read(&key, version) {
            CacheRead::Hit(_) => Ok(()),
            other => Err(corrupt(format!(
                "cache WAL touch replay did not hit expected entry: {other:?}"
            ))),
        },
        CacheWalRecord::StaleInvalidate {
            key,
            cached_version,
            required_version,
        } => match cache.read(&key, required_version) {
            CacheRead::Stale {
                cached_version: recovered,
                ..
            } if recovered == cached_version => Ok(()),
            other => Err(corrupt(format!(
                "cache WAL stale invalidation replay did not remove expected entry: {other:?}"
            ))),
        },
        CacheWalRecord::Invalidate { key } => cache
            .invalidate(&key)
            .map(|_| ())
            .ok_or_else(|| corrupt("cache WAL invalidated missing entry")),
    }
}

fn encode_cache_wal_payload(record: &CacheWalRecord) -> RsfResult<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(CACHE_WAL_MAGIC);
    out.push(CACHE_WAL_VERSION);
    match record {
        CacheWalRecord::Put { entry } => {
            out.push(1);
            write_entry(&mut out, entry)?;
        }
        CacheWalRecord::Touch { key, version } => {
            out.push(2);
            write_key(&mut out, key)?;
            write_u64(&mut out, version.0);
        }
        CacheWalRecord::StaleInvalidate {
            key,
            cached_version,
            required_version,
        } => {
            out.push(3);
            write_key(&mut out, key)?;
            write_u64(&mut out, cached_version.0);
            write_u64(&mut out, required_version.0);
        }
        CacheWalRecord::Invalidate { key } => {
            out.push(4);
            write_key(&mut out, key)?;
        }
    }
    Ok(out)
}

fn decode_cache_wal_payload(bytes: &[u8]) -> RsfResult<CacheWalRecord> {
    let mut offset = 0;
    take_magic(bytes, &mut offset)?;
    let version = read_u8(bytes, &mut offset)?;
    if version != CACHE_WAL_VERSION {
        return Err(corrupt(format!("unsupported cache WAL version {version}")));
    }
    let record = match read_u8(bytes, &mut offset)? {
        1 => CacheWalRecord::Put {
            entry: read_entry(bytes, &mut offset)?,
        },
        2 => CacheWalRecord::Touch {
            key: read_key(bytes, &mut offset)?,
            version: CommitVersion(read_u64(bytes, &mut offset)?),
        },
        3 => CacheWalRecord::StaleInvalidate {
            key: read_key(bytes, &mut offset)?,
            cached_version: CommitVersion(read_u64(bytes, &mut offset)?),
            required_version: CommitVersion(read_u64(bytes, &mut offset)?),
        },
        4 => CacheWalRecord::Invalidate {
            key: read_key(bytes, &mut offset)?,
        },
        other => return Err(corrupt(format!("unknown cache WAL record kind {other}"))),
    };
    if offset != bytes.len() {
        return Err(corrupt("cache WAL payload has trailing bytes"));
    }
    Ok(record)
}

fn write_entry(out: &mut Vec<u8>, entry: &CacheEntry) -> RsfResult<()> {
    write_key(out, &entry.key)?;
    write_u64(out, entry.version.0);
    write_u64(out, entry.bytes);
    out.push(entry.cost);
    out.push(u8::from(entry.pinned));
    Ok(())
}

fn read_entry(bytes: &[u8], offset: &mut usize) -> RsfResult<CacheEntry> {
    Ok(CacheEntry {
        key: read_key(bytes, offset)?,
        version: CommitVersion(read_u64(bytes, offset)?),
        bytes: read_u64(bytes, offset)?,
        cost: read_u8(bytes, offset)?,
        pinned: read_bool(bytes, offset)?,
    })
}

fn write_key(out: &mut Vec<u8>, key: &CacheKey) -> RsfResult<()> {
    out.push(surface_to_u8(key.surface));
    write_string(out, &key.namespace)?;
    write_string(out, &key.key)?;
    Ok(())
}

fn read_key(bytes: &[u8], offset: &mut usize) -> RsfResult<CacheKey> {
    let surface = surface_from_u8(read_u8(bytes, offset)?)?;
    let namespace = read_string(bytes, offset)?;
    let key = read_string(bytes, offset)?;
    Ok(CacheKey {
        surface,
        namespace,
        key,
    })
}

fn surface_to_u8(surface: CacheSurface) -> u8 {
    match surface {
        CacheSurface::Sql => 1,
        CacheSurface::Kv => 2,
        CacheSurface::Object => 3,
        CacheSurface::Blob => 4,
    }
}

fn surface_from_u8(value: u8) -> RsfResult<CacheSurface> {
    match value {
        1 => Ok(CacheSurface::Sql),
        2 => Ok(CacheSurface::Kv),
        3 => Ok(CacheSurface::Object),
        4 => Ok(CacheSurface::Blob),
        other => Err(corrupt(format!("unknown cache surface {other}"))),
    }
}

fn write_string(out: &mut Vec<u8>, value: &str) -> RsfResult<()> {
    let len =
        u32::try_from(value.len()).map_err(|_| invalid("cache WAL string length exceeds u32"))?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

fn read_string(bytes: &[u8], offset: &mut usize) -> RsfResult<String> {
    let len = read_u32(bytes, offset)? as usize;
    let raw = take(bytes, offset, len)?;
    String::from_utf8(raw.to_vec()).map_err(|_| corrupt("cache WAL string is not utf8"))
}

fn write_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn read_u64(bytes: &[u8], offset: &mut usize) -> RsfResult<u64> {
    let raw = take(bytes, offset, 8)?;
    let mut buf = [0; 8];
    buf.copy_from_slice(raw);
    Ok(u64::from_le_bytes(buf))
}

fn read_u32(bytes: &[u8], offset: &mut usize) -> RsfResult<u32> {
    let raw = take(bytes, offset, 4)?;
    let mut buf = [0; 4];
    buf.copy_from_slice(raw);
    Ok(u32::from_le_bytes(buf))
}

fn read_u8(bytes: &[u8], offset: &mut usize) -> RsfResult<u8> {
    Ok(*take(bytes, offset, 1)?
        .first()
        .expect("one requested byte must exist"))
}

fn read_bool(bytes: &[u8], offset: &mut usize) -> RsfResult<bool> {
    match read_u8(bytes, offset)? {
        0 => Ok(false),
        1 => Ok(true),
        other => Err(corrupt(format!("invalid cache WAL bool {other}"))),
    }
}

fn take_magic(bytes: &[u8], offset: &mut usize) -> RsfResult<()> {
    let magic = take(bytes, offset, CACHE_WAL_MAGIC.len())?;
    if magic != CACHE_WAL_MAGIC {
        return Err(corrupt("cache WAL payload has invalid magic"));
    }
    Ok(())
}

fn take<'a>(bytes: &'a [u8], offset: &mut usize, len: usize) -> RsfResult<&'a [u8]> {
    let end = offset
        .checked_add(len)
        .ok_or_else(|| corrupt("cache WAL offset overflow"))?;
    if end > bytes.len() {
        return Err(corrupt("cache WAL payload is truncated"));
    }
    let out = &bytes[*offset..end];
    *offset = end;
    Ok(out)
}

fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message)
}

fn corrupt(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::CorruptionDetected, message)
}

#[cfg(test)]
mod tests;
