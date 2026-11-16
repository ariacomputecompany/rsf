use super::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

fn reserved(label: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    loop {
        let path = root(&format!(
            "owned-{label}-{}",
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        match fs::create_dir(&path) {
            Ok(()) => return path,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => panic!("reserve {}: {error}", path.display()),
        }
    }
}
fn owned(payload: &[u8]) -> WalGroupEntry<Vec<u8>> {
    WalGroupEntry {
        epoch: Epoch(1),
        kind: WalRecordKind::Prepare,
        payload: payload.to_vec(),
    }
}
fn producer_error() -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, "injected owned producer rejection")
}

#[test]
fn owned_groups_preserve_borrowed_and_single_append_wal_bytes_across_rotation() {
    let payloads = [vec![1; 17], vec![2; 128 * 1024], vec![3; 91], vec![4; 1024]];
    let paths = [reserved("single"), reserved("borrowed"), reserved("owned")];
    for (mode, path) in paths.iter().enumerate() {
        let mut store =
            SegmentStore::open_with_options(path, options(SyncPolicy::Full, 1024)).unwrap();
        store
            .append_next_wal(
                PlacementGroupId(9),
                Epoch(1),
                WalRecordKind::Commit,
                b"other pg",
            )
            .unwrap();
        match mode {
            0 => {
                for payload in &payloads {
                    store
                        .append_next_wal(
                            PlacementGroupId(1),
                            Epoch(1),
                            WalRecordKind::Prepare,
                            payload,
                        )
                        .unwrap();
                }
            }
            1 => store
                .append_next_wal_group(PlacementGroupId(1), payloads.iter().map(|p| record(p)))
                .unwrap(),
            _ => store
                .try_append_next_wal_group(
                    PlacementGroupId(1),
                    payloads.iter().map(|p| Ok(owned(p))),
                )
                .unwrap(),
        }
    }
    let expected = SegmentStore::recover_existing(&paths[0])
        .unwrap()
        .recovery
        .wal_records;
    for path in &paths[1..] {
        assert_eq!(
            SegmentStore::recover_existing(path)
                .unwrap()
                .recovery
                .wal_records,
            expected
        );
        assert_eq!(bytes(path), bytes(&paths[0]));
    }
    for path in paths {
        fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn owned_producer_error_is_not_eof_and_late_failure_fences_actual_prefix() {
    for visible in [0, 2, 4] {
        let path = reserved("producer-error");
        let mut store = SegmentStore::open(&path).unwrap();
        let mut polls = 0;
        let items = std::iter::from_fn(|| {
            let index = polls;
            polls += 1;
            if index < visible {
                Some(Ok(owned(b"prefix")))
            } else if index == visible {
                Some(Err(producer_error()))
            } else {
                panic!("producer requested after error");
            }
        });
        let error = store
            .try_append_next_wal_group(PlacementGroupId(1), items)
            .unwrap_err();
        assert_eq!(error.message, producer_error().message);
        assert_eq!(polls, visible + 1);
        assert_eq!(store.write_failed, visible != 0);
        if visible != 0 {
            assert!(store.cursor.is_none());
            assert!(
                store
                    .append_next_wal(
                        PlacementGroupId(1),
                        Epoch(1),
                        WalRecordKind::Commit,
                        b"forbidden"
                    )
                    .is_err()
            );
        } else {
            assert!(
                store.revision().is_ok(),
                "pre-mutation error changed the existing contract"
            );
        }
        drop(store);
        let recovered = SegmentStore::recover_existing(&path).unwrap();
        assert_eq!(recovered.recovery.wal_records.len(), visible);
        assert!(
            recovered
                .recovery
                .wal_records
                .iter()
                .all(|r| r.payload == b"prefix")
        );
        fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn owned_group_releases_each_payload_before_polling_next_and_fences_unwind() {
    struct Payload {
        bytes: Vec<u8>,
        live: Arc<AtomicUsize>,
    }
    impl AsRef<[u8]> for Payload {
        fn as_ref(&self) -> &[u8] {
            &self.bytes
        }
    }
    impl Drop for Payload {
        fn drop(&mut self) {
            assert_eq!(self.live.fetch_sub(1, Ordering::SeqCst), 1);
        }
    }
    let path = reserved("laziness");
    let mut store = SegmentStore::open(&path).unwrap();
    let live = Arc::new(AtomicUsize::new(0));
    let mut produced = 0;
    let records = std::iter::from_fn(|| {
        assert_eq!(
            live.load(Ordering::SeqCst),
            0,
            "previous owned item was retained"
        );
        if produced == 5 {
            return None;
        }
        produced += 1;
        live.fetch_add(1, Ordering::SeqCst);
        Some(Ok(WalGroupEntry {
            epoch: Epoch(1),
            kind: WalRecordKind::Prepare,
            payload: Payload {
                bytes: vec![7; 100],
                live: live.clone(),
            },
        }))
    });
    store
        .try_append_next_wal_group(PlacementGroupId(1), records)
        .unwrap();
    assert_eq!(produced, 5);
    assert_eq!(live.load(Ordering::SeqCst), 0);
    let mut first = true;
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        store
            .try_append_next_wal_group(
                PlacementGroupId(1),
                std::iter::from_fn(|| {
                    if first {
                        first = false;
                        Some(Ok(owned(b"unacknowledged")))
                    } else {
                        panic!("injected owned producer unwind");
                    }
                }),
            )
            .unwrap();
    }));
    assert!(result.is_err());
    assert!(store.write_failed && store.cursor.is_none());
    drop(store);
    assert_eq!(
        SegmentStore::recover_existing(&path)
            .unwrap()
            .recovery
            .wal_records
            .len(),
        6
    );
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn owned_groups_keep_real_sync_fault_and_rotation_boundaries() {
    for policy in [SyncPolicy::Data, SyncPolicy::Full] {
        for failure in [None, Some(0), Some(1), Some(2)] {
            let path = reserved("sync-fault");
            let payload = b"owned durable record";
            let mut store =
                SegmentStore::open_with_options(&path, options(policy, size(payload) * 2)).unwrap();
            let observed = sync_observation::watch(&path, failure);
            let result = store
                .try_append_next_wal_group(PlacementGroupId(1), (0..5).map(|_| Ok(owned(payload))));
            let events = observed.events();
            if let Some(index) = failure {
                assert!(result.is_err());
                assert_eq!(events.len(), index + 1);
                assert!(!events[index].completed);
                assert!(store.write_failed && store.cursor.is_none());
            } else {
                result.unwrap();
                assert_eq!(events.len(), 3);
                assert!(events.iter().all(|e| e.completed && e.policy == policy));
            }
            drop(observed);
            drop(store);
            assert_prefix(&path, 5, payload);
            fs::remove_dir_all(path).unwrap();
        }
    }
}

#[test]
fn owned_empty_retry_renews_closed_and_active_bytes_after_complete_visible_error() {
    for policy in [SyncPolicy::Data, SyncPolicy::Full] {
        let path = reserved("complete-visible");
        let opts = options(policy, 1);
        let mut store = SegmentStore::open_with_options(&path, opts).unwrap();
        let items = (0..4)
            .map(|_| Ok(owned(b"retained")))
            .chain(std::iter::once(Err(producer_error())));
        assert!(
            store
                .try_append_next_wal_group(PlacementGroupId(1), items)
                .is_err()
        );
        assert!(store.write_failed);
        drop(store);
        let mut store = SegmentStore::open_with_options(&path, opts).unwrap();
        let stamp = store.capture_owned_custody().unwrap();
        let failed = sync_observation::watch(&path, Some(0));
        assert!(store.sync_owned_closed_segments(&stamp).is_err());
        assert_eq!(failed.events().len(), 1);
        assert!(store.write_failed);
        drop(failed);
        drop(store);
        let mut store = SegmentStore::open_with_options(&path, opts).unwrap();
        let expected = store
            .manifest
            .segments
            .iter()
            .filter(|segment| {
                fs::metadata(path.join(SEGMENT_DIR).join(&segment.file_name))
                    .unwrap()
                    .len()
                    != 0
            })
            .count();
        let stamp = store.capture_owned_custody().unwrap();
        // The last retained segment is still active; fail its empty-group
        // barrier after all closed segments have been renewed successfully.
        assert!(store.active.file.metadata().unwrap().len() != 0);
        let active_failure = sync_observation::watch(&path, Some(expected - 1));
        store.sync_owned_closed_segments(&stamp).unwrap();
        assert!(
            store
                .try_append_next_wal_group(
                    PlacementGroupId(1),
                    std::iter::empty::<RsfResult<WalGroupEntry<Vec<u8>>>>()
                )
                .is_err()
        );
        assert_eq!(active_failure.events().len(), expected);
        assert!(!active_failure.events().last().unwrap().completed);
        assert!(store.write_failed && store.cursor.is_none());
        drop(active_failure);
        drop(store);
        let mut store = SegmentStore::open_with_options(&path, opts).unwrap();
        let stamp = store.capture_owned_custody().unwrap();
        let observed = sync_observation::watch(&path, None);
        store.sync_owned_closed_segments(&stamp).unwrap();
        store
            .try_append_next_wal_group(
                PlacementGroupId(1),
                std::iter::empty::<RsfResult<WalGroupEntry<Vec<u8>>>>(),
            )
            .unwrap();
        let events = observed.events();
        assert_eq!(
            events.len(),
            expected,
            "each retained nonempty segment requires its data barrier"
        );
        assert!(events.iter().all(|e| e.completed && e.policy == policy));
        drop(observed);
        drop(store);
        assert_eq!(
            SegmentStore::recover_existing(&path)
                .unwrap()
                .recovery
                .wal_records
                .len(),
            4
        );
        fs::remove_dir_all(path).unwrap();
    }
}
