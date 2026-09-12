use std::path::PathBuf;

use rsf_core::{CommitVersion, Epoch, LogIndex, PlacementGroupId, storage::WalRecordKind};
use rsf_storage::{SegmentStore, WalAppend};

use super::*;
use crate::{CacheAdmission, CacheRead};

#[test]
fn durable_cache_recovers_entries_after_restart() {
    let path = test_path("recovers_entries_after_restart");
    let policy = CachePolicy::bounded(512, 256, 2);
    let key = CacheKey::new(CacheSurface::Sql, "app", "users/1");

    {
        let mut cache = DurableReadCache::open(&path, Epoch(7), policy.clone()).expect("open");
        assert_eq!(
            cache.put(entry(key.clone(), 8, 128, false)).expect("put"),
            CacheAdmission::Stored {
                key: key.clone(),
                used_bytes: 128,
                evicted: vec![]
            }
        );
    }

    let mut recovered = DurableReadCache::open(&path, Epoch(7), policy).expect("recover");
    assert_eq!(recovered.used_bytes(), 128);
    assert!(matches!(
        recovered.read(&key, CommitVersion(8)).expect("read"),
        CacheRead::Hit(_)
    ));
}

#[test]
fn durable_cache_recovers_touch_order_for_eviction() {
    let path = test_path("recovers_touch_order_for_eviction");
    let policy = CachePolicy::bounded(300, 200, 2);
    let sql = CacheKey::new(CacheSurface::Sql, "app", "users/1");
    let kv = CacheKey::new(CacheSurface::Kv, "app", "config/a");
    let object = CacheKey::new(CacheSurface::Object, "app", "media/hero.png");

    {
        let mut cache = DurableReadCache::open(&path, Epoch(7), policy.clone()).expect("open");
        cache.put(entry(sql.clone(), 4, 100, false)).expect("put");
        cache.put(entry(kv.clone(), 5, 100, false)).expect("put");
        assert!(matches!(
            cache.read(&sql, CommitVersion(4)).expect("touch"),
            CacheRead::Hit(_)
        ));
    }

    let mut recovered = DurableReadCache::open(&path, Epoch(7), policy).expect("recover");
    assert_eq!(
        recovered
            .put(entry(object.clone(), 6, 200, false))
            .expect("put"),
        CacheAdmission::Stored {
            key: object,
            used_bytes: 300,
            evicted: vec![kv.clone()]
        }
    );
    assert!(matches!(
        recovered.read(&sql, CommitVersion(4)).expect("sql read"),
        CacheRead::Hit(_)
    ));
    assert_eq!(
        recovered.read(&kv, CommitVersion(5)).expect("kv read"),
        CacheRead::Miss {
            key: kv,
            reason: "not cached".to_string()
        }
    );
}

#[test]
fn durable_cache_recovers_stale_and_explicit_invalidations() {
    let path = test_path("recovers_invalidations");
    let policy = CachePolicy::bounded(512, 256, 2);
    let stale = CacheKey::new(CacheSurface::Object, "app", "media/stale");
    let removed = CacheKey::new(CacheSurface::Blob, "app", "digest-a");

    {
        let mut cache = DurableReadCache::open(&path, Epoch(7), policy.clone()).expect("open");
        cache
            .put(entry(stale.clone(), 4, 100, false))
            .expect("put stale");
        cache
            .put(entry(removed.clone(), 5, 100, false))
            .expect("put removed");
        assert!(matches!(
            cache.read(&stale, CommitVersion(6)).expect("stale read"),
            CacheRead::Stale { .. }
        ));
        assert_eq!(
            cache
                .invalidate(&removed)
                .expect("invalidate")
                .map(|entry| entry.key),
            Some(removed.clone())
        );
    }

    let mut recovered = DurableReadCache::open(&path, Epoch(7), policy).expect("recover");
    assert!(recovered.is_empty());
    assert_eq!(
        recovered
            .read(&stale, CommitVersion(4))
            .expect("stale gone"),
        CacheRead::Miss {
            key: stale,
            reason: "not cached".to_string()
        }
    );
    assert_eq!(
        recovered
            .read(&removed, CommitVersion(5))
            .expect("removed gone"),
        CacheRead::Miss {
            key: removed,
            reason: "not cached".to_string()
        }
    );
}

#[test]
fn cache_wal_decode_rejects_trailing_bytes() {
    let mut payload = encode_cache_wal_payload(&CacheWalRecord::Put {
        entry: entry(
            CacheKey::new(CacheSurface::Sql, "app", "users/1"),
            1,
            64,
            false,
        ),
    })
    .expect("encode");
    payload.push(99);

    let err = decode_cache_wal_payload(&payload).expect_err("trailing bytes rejected");
    assert_eq!(err.code, ErrorCode::CorruptionDetected);
}

#[test]
fn durable_cache_replay_rejects_touch_for_missing_entry() {
    let path = test_path("rejects_missing_touch");
    append_raw(
        &path,
        &CacheWalRecord::Touch {
            key: CacheKey::new(CacheSurface::Sql, "app", "users/1"),
            version: CommitVersion(1),
        },
        1,
    );

    let err = DurableReadCache::open(&path, Epoch(7), CachePolicy::bounded(512, 256, 2))
        .expect_err("missing touch must fail closed");
    assert_eq!(err.code, ErrorCode::CorruptionDetected);
}

fn entry(key: CacheKey, version: u64, bytes: u64, pinned: bool) -> CacheEntry {
    CacheEntry {
        key,
        version: CommitVersion(version),
        bytes,
        cost: 1,
        pinned,
    }
}

fn append_raw(path: &PathBuf, record: &CacheWalRecord, index: u64) {
    let mut store = SegmentStore::open(path).expect("store");
    let payload = encode_cache_wal_payload(record).expect("encode");
    let recovery = store.recover_current().expect("recover").recovery;
    let tail = recovery.pg_tail(PlacementGroupId(55));
    let previous_hash = tail.map_or([0_u8; 32], |tail| tail.record_hash);
    store
        .append_wal(WalAppend {
            pg: PlacementGroupId(55),
            epoch: Epoch(7),
            index: LogIndex(index),
            kind: WalRecordKind::Cache,
            previous_hash,
            payload: &payload,
        })
        .expect("append raw cache wal");
}

fn test_path(name: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!("rsf-cache-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);
    path
}
