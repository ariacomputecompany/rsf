use super::*;
use crate::{Epoch, SyncPolicy, sync_observation};

fn root(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "rsf-group-{label}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}
fn options(policy: SyncPolicy, limit: u64) -> SegmentStoreOptions {
    SegmentStoreOptions {
        log_options: SegmentLogOptions {
            sync_policy: policy,
        },
        max_segment_bytes: limit,
    }
}
fn record(payload: &[u8]) -> WalGroupRecord<'_> {
    WalGroupRecord {
        epoch: Epoch(1),
        kind: WalRecordKind::Prepare,
        payload,
    }
}
fn size(payload: &[u8]) -> u64 {
    let encoded = encode_wal_record(
        WalAppend {
            pg: PlacementGroupId(1),
            epoch: Epoch(1),
            index: LogIndex(1),
            kind: WalRecordKind::Prepare,
            previous_hash: [0; 32],
            payload,
        },
        [0; 32],
        [0; 32],
    )
    .unwrap();
    record_disk_len(encoded.len()).unwrap()
}
fn bytes(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    let mut result = BTreeMap::new();
    result.insert(
        PathBuf::from(MANIFEST_FILE),
        fs::read(root.join(MANIFEST_FILE)).unwrap(),
    );
    for entry in fs::read_dir(root.join(SEGMENT_DIR)).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_some_and(|ext| ext == "segment") {
            result.insert(
                path.strip_prefix(root).unwrap().to_path_buf(),
                fs::read(path).unwrap(),
            );
        }
    }
    result
}
fn assert_prefix(root: &Path, count: usize, payload: &[u8]) {
    let recovered = SegmentStore::recover_existing(root).unwrap();
    assert!(recovered.recovery.wal_records.len() <= count);
    for (position, wal) in recovered.recovery.wal_records.iter().enumerate() {
        assert_eq!(wal.index, LogIndex(position as u64 + 1));
        assert_eq!(wal.payload, payload);
    }
}

#[test]
fn durable_group_flushes_each_dirty_segment_before_successor_manifest_and_return() {
    for policy in [SyncPolicy::Data, SyncPolicy::Full] {
        let path = root("flush-order");
        let payload = b"two records per segment";
        let mut store =
            SegmentStore::open_with_options(&path, options(policy, size(payload) * 2)).unwrap();
        let observed = sync_observation::watch(&path, None);
        store
            .append_next_wal_group(PlacementGroupId(1), [record(payload); 5])
            .unwrap();
        let events = observed.events();
        assert_eq!(
            events.len(),
            3,
            "one required append flush per dirty segment"
        );
        for (index, event) in events.iter().enumerate() {
            let id = index as u64 + 1;
            assert!(
                event.completed,
                "success returned before real flush completed"
            );
            assert_eq!(event.policy, policy);
            assert_eq!(
                event.path,
                path.join(SEGMENT_DIR).join(segment_file_name(id))
            );
            let manifest = SegmentManifest::decode(&event.manifest).unwrap();
            assert_eq!(
                manifest.active_id, id,
                "old segment flushed after successor publication"
            );
            assert_eq!(manifest.segments.len(), id as usize);
        }
        assert_eq!(
            store.wal_tail(PlacementGroupId(1)).unwrap().unwrap().index,
            LogIndex(5)
        );
        drop(observed);
        drop(store);
        let reopened = SegmentStore::open(&path).unwrap();
        assert_eq!(
            reopened
                .recover_current()
                .unwrap()
                .recovery
                .wal_records
                .len(),
            5
        );
        drop(reopened);
        fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn durable_group_is_byte_identical_to_sequential_appends_across_rotation() {
    let payloads = [
        vec![1; 20],
        vec![2; 20],
        vec![3; 1024],
        vec![4; 20],
        vec![5; 20],
    ];
    for policy in [SyncPolicy::Data, SyncPolicy::Full] {
        for limit in [0, size(&payloads[0]) * 2, 1] {
            let left = root("sequential");
            let right = root("grouped");
            let mut sequential =
                SegmentStore::open_with_options(&left, options(policy, limit)).unwrap();
            let mut grouped =
                SegmentStore::open_with_options(&right, options(policy, limit)).unwrap();
            for (index, payload) in payloads.iter().enumerate() {
                sequential
                    .append_next_wal(
                        PlacementGroupId(1),
                        Epoch(index as u64 + 1),
                        WalRecordKind::Prepare,
                        payload,
                    )
                    .unwrap();
            }
            grouped
                .append_next_wal_group(
                    PlacementGroupId(1),
                    payloads
                        .iter()
                        .enumerate()
                        .map(|(index, payload)| WalGroupRecord {
                            epoch: Epoch(index as u64 + 1),
                            kind: WalRecordKind::Prepare,
                            payload,
                        }),
                )
                .unwrap();
            assert_eq!(bytes(&left), bytes(&right));
            drop(sequential);
            drop(grouped);
            let a = SegmentStore::recover_existing(&left).unwrap();
            let b = SegmentStore::recover_existing(&right).unwrap();
            assert_eq!(a.recovery.wal_records, b.recovery.wal_records);
            assert_eq!(a.recovery.pg_tails, b.recovery.pg_tails);
            fs::remove_dir_all(left).unwrap();
            fs::remove_dir_all(right).unwrap();
        }
    }
}

#[test]
fn durable_group_rotated_and_final_flush_failures_fence_without_acknowledging() {
    for policy in [SyncPolicy::Data, SyncPolicy::Full] {
        for fail_at in [0, 1, 2] {
            let path = root("sync-failure");
            let payload = b"durable group prefix";
            let mut store =
                SegmentStore::open_with_options(&path, options(policy, size(payload) * 2)).unwrap();
            let observed = sync_observation::watch(&path, Some(fail_at));
            let result = store.append_next_wal_group(PlacementGroupId(1), [record(payload); 5]);
            assert!(
                result.is_err(),
                "failed flush must never acknowledge the group"
            );
            let events = observed.events();
            assert_eq!(events.len(), fail_at + 1);
            assert!(events[..fail_at].iter().all(|event| event.completed));
            assert!(!events[fail_at].completed);
            assert_eq!(
                store.segments().len(),
                fail_at + 1,
                "failed old-segment flush published a successor manifest"
            );
            assert!(store.write_failed);
            assert!(store.cursor.is_none());
            assert!(
                store
                    .append_next_wal(
                        PlacementGroupId(1),
                        Epoch(1),
                        WalRecordKind::Commit,
                        b"must not append"
                    )
                    .is_err()
            );
            assert!(
                store
                    .append_next_wal_group(PlacementGroupId(1), [record(payload)])
                    .is_err()
            );
            drop(observed);
            drop(store);
            // Sync failure is uncertain, not a power-loss model: bytes may be
            // present. Cold recovery must return only a valid submitted prefix.
            assert_prefix(&path, 5, payload);
            let mut reopened = SegmentStore::open(&path).unwrap();
            reopened
                .append_next_wal(
                    PlacementGroupId(1),
                    Epoch(1),
                    WalRecordKind::Commit,
                    b"after reopen",
                )
                .unwrap();
            drop(reopened);
            fs::remove_dir_all(path).unwrap();
        }
    }
}

#[test]
fn group_rotation_failure_keeps_the_flushed_prefix_and_fences_the_owner() {
    let path = root("rotation-error");
    let payload = b"before failed rotation";
    let mut store =
        SegmentStore::open_with_options(&path, options(SyncPolicy::Data, size(payload) * 2))
            .unwrap();
    let original_manifest = fs::read(path.join(MANIFEST_FILE)).unwrap();
    let blocked = path.join("MANIFEST.tmp");
    fs::create_dir(&blocked).unwrap(); // Actual manifest temporary-file publication failure.
    let observed = sync_observation::watch(&path, None);
    assert!(
        store
            .append_next_wal_group(PlacementGroupId(1), [record(payload); 3])
            .is_err()
    );
    let events = observed.events();
    assert_eq!(events.len(), 1);
    assert!(
        events[0].completed,
        "dirty predecessor was not flushed before rotation"
    );
    assert_eq!(store.segments().len(), 1);
    assert_eq!(
        fs::read(path.join(MANIFEST_FILE)).unwrap(),
        original_manifest
    );
    assert!(path.join(SEGMENT_DIR).join(segment_file_name(2)).is_file());
    assert!(store.write_failed && store.cursor.is_none());
    assert!(store.revision().is_err());
    fs::remove_dir(&blocked).unwrap();
    assert!(
        store
            .append_next_wal_group(PlacementGroupId(1), [record(payload)])
            .is_err()
    );
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
    drop(observed);
    drop(store);
    assert_eq!(
        SegmentStore::recover_existing(&path)
            .unwrap()
            .recovery
            .wal_records
            .len(),
        2
    );
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn group_post_flush_stamp_loss_never_recaptures_missing_custody() {
    let path = root("stamp-loss");
    let mut store = SegmentStore::open(&path).unwrap();
    let active = store.active.path().to_path_buf();
    let retained = path.join("retained-segment");
    let mut supplied = false;
    let records = std::iter::from_fn(|| {
        if !supplied {
            supplied = true;
            Some(record(b"prefix"))
        } else {
            fs::rename(&active, &retained).unwrap();
            None
        }
    });
    let observed = sync_observation::watch(&path, None);
    assert!(
        store
            .append_next_wal_group(PlacementGroupId(1), records)
            .is_err()
    );
    let events = observed.events();
    assert_eq!(events.len(), 1);
    assert!(events[0].completed);
    assert_eq!(events[0].policy, SyncPolicy::Data);
    assert_eq!(events[0].path, active);
    assert!(store.write_failed && store.cursor.is_none());
    assert!(store.capture_owned_custody().is_err());
    assert!(
        !active.exists(),
        "failure must not repair the missing segment"
    );
    assert!(SegmentStore::recover_existing(&path).is_err());
    fs::rename(&retained, &active).unwrap();
    assert!(
        store
            .append_next_wal_group(PlacementGroupId(1), [record(b"forbidden")])
            .is_err()
    );
    drop(observed);
    drop(store);
    let recovered = SegmentStore::recover_existing(&path).unwrap();
    assert_eq!(recovered.recovery.wal_records.len(), 1);
    assert_eq!(recovered.recovery.wal_records[0].payload, b"prefix");
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn empty_suffix_after_failed_sync_renews_active_data_before_success() {
    for policy in [SyncPolicy::Data, SyncPolicy::Full] {
        let path = root("empty-retry");
        let opts = options(policy, 0);
        let mut store = SegmentStore::open_with_options(&path, opts).unwrap();
        for _ in 0..2 {
            store
                .append_next_wal(
                    PlacementGroupId(1),
                    Epoch(1),
                    WalRecordKind::Prepare,
                    b"retained",
                )
                .unwrap();
        }
        let observed = sync_observation::watch(&path, Some(0));
        assert!(
            store
                .append_next_wal(
                    PlacementGroupId(1),
                    Epoch(1),
                    WalRecordKind::Prepare,
                    b"retained"
                )
                .is_err()
        );
        drop(observed);
        drop(store);
        let (mut store, recovered) = SegmentStore::open_recovered(&path, opts).unwrap();
        // The injected error follows real writes. Seeing all bytes on reopen
        // is not evidence that the failed data barrier can now be omitted.
        assert_eq!(recovered.recovery.wal_records.len(), 3);
        let observed = sync_observation::watch(&path, Some(0));
        assert!(
            store
                .append_next_wal_group(PlacementGroupId(1), [])
                .is_err()
        );
        assert_eq!(observed.events().len(), 1);
        assert!(store.write_failed && store.cursor.is_none());
        drop(observed);
        drop(store);
        let mut store = SegmentStore::open_with_options(&path, opts).unwrap();
        let before = bytes(&path);
        let observed = sync_observation::watch(&path, None);
        store
            .append_next_wal_group(PlacementGroupId(1), [])
            .unwrap();
        let events = observed.events();
        assert_eq!(events.len(), 1);
        assert!(events[0].completed);
        assert_eq!(events[0].policy, policy);
        assert_eq!(bytes(&path), before);
        assert_eq!(
            store.wal_tail(PlacementGroupId(1)).unwrap().unwrap().index,
            LogIndex(3)
        );
        drop(observed);
        drop(store);
        fs::remove_dir_all(path).unwrap();
    }
}

#[cfg(unix)]
#[test]
fn legacy_failed_single_sync_then_reopen_rotation_requires_closed_prefix_renewal() {
    for policy in [SyncPolicy::Data, SyncPolicy::Full] {
        let path = root("legacy-closed");
        let payload = b"visible after failed sync";
        let opts = options(policy, size(payload));
        let mut old = SegmentStore::open_with_options(&path, opts).unwrap();
        let observed = sync_observation::watch(&path, Some(0));
        assert!(
            old.append_next_wal(
                PlacementGroupId(1),
                Epoch(1),
                WalRecordKind::Prepare,
                payload
            )
            .is_err()
        );
        drop(observed);
        drop(old);
        let (mut old, recovered) = SegmentStore::open_recovered(&path, opts).unwrap();
        assert_eq!(recovered.recovery.wal_records.len(), 1);
        let observed = sync_observation::watch(&path, None);
        old.append_next_wal(
            PlacementGroupId(1),
            Epoch(1),
            WalRecordKind::Prepare,
            payload,
        )
        .unwrap();
        let events = observed.events();
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].path,
            path.join(SEGMENT_DIR).join(segment_file_name(2))
        );
        drop(observed);
        drop(old);

        let mut store = SegmentStore::open_with_options(&path, opts).unwrap();
        let custody = store.capture_owned_custody().unwrap();
        let observed = sync_observation::watch(&path, Some(0));
        assert!(store.sync_owned_closed_segments(&custody).is_err());
        let events = observed.events();
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].path,
            path.join(SEGMENT_DIR).join(segment_file_name(1))
        );
        assert!(!events[0].completed);
        assert!(store.write_failed && store.cursor.is_none());
        assert!(
            store
                .append_next_wal_group(PlacementGroupId(1), [])
                .is_err()
        );
        drop(observed);
        drop(store);

        let mut store = SegmentStore::open_with_options(&path, opts).unwrap();
        let before = bytes(&path);
        let custody = store.capture_owned_custody().unwrap();
        let observed = sync_observation::watch(&path, None);
        store.sync_owned_closed_segments(&custody).unwrap();
        store
            .append_next_wal_group(PlacementGroupId(1), [])
            .unwrap();
        let events = observed.events();
        assert_eq!(
            events.len(),
            2,
            "closed and active retained files each require renewal"
        );
        assert!(
            events
                .iter()
                .all(|event| event.completed && event.policy == policy)
        );
        assert_eq!(
            events[0].path,
            path.join(SEGMENT_DIR).join(segment_file_name(1))
        );
        assert_eq!(
            events[1].path,
            path.join(SEGMENT_DIR).join(segment_file_name(2))
        );
        assert_eq!(bytes(&path), before);
        drop(observed);
        drop(store);
        assert_eq!(
            SegmentStore::recover_existing(&path)
                .unwrap()
                .recovery
                .wal_records
                .len(),
            2
        );
        fs::remove_dir_all(path).unwrap();
    }
}

#[cfg(unix)]
#[test]
fn closed_prefix_renewal_rejects_alias_and_stays_fenced_after_restoration() {
    use std::os::unix::fs::symlink;
    let path = root("closed-alias");
    let mut store = SegmentStore::open_with_options(&path, options(SyncPolicy::Data, 1)).unwrap();
    store
        .append_next_wal_group(PlacementGroupId(1), [record(b"first"); 2])
        .unwrap();
    let custody = store.capture_owned_custody().unwrap();
    let closed = path.join(SEGMENT_DIR).join(segment_file_name(1));
    let kept = path.join("retained-closed");
    fs::rename(&closed, &kept).unwrap();
    symlink(&kept, &closed).unwrap();
    let observed = sync_observation::watch(&path, None);
    assert!(store.sync_owned_closed_segments(&custody).is_err());
    assert!(
        observed.events().is_empty(),
        "foreign alias was synced before custody rejection"
    );
    assert!(store.write_failed && store.cursor.is_none());
    fs::remove_file(&closed).unwrap();
    fs::rename(&kept, &closed).unwrap();
    assert!(store.sync_owned_closed_segments(&custody).is_err());
    assert!(
        store
            .append_next_wal_group(PlacementGroupId(1), [])
            .is_err()
    );
    drop(observed);
    drop(store);
    assert_eq!(
        SegmentStore::recover_existing(&path)
            .unwrap()
            .recovery
            .wal_records
            .len(),
        2
    );
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn ordinary_append_keeps_original_sync_policy_after_group_success() {
    for policy in [SyncPolicy::Data, SyncPolicy::Full] {
        let path = root("ordinary-policy");
        let mut store = SegmentStore::open_with_options(&path, options(policy, 0)).unwrap();
        let observed = sync_observation::watch(&path, Some(1));
        store
            .append_next_wal_group(PlacementGroupId(1), [record(b"first"); 3])
            .unwrap();
        assert_eq!(store.options.log_options.sync_policy, policy);
        assert_eq!(store.active.options.sync_policy, policy);
        let result = store.append_next_wal(
            PlacementGroupId(1),
            Epoch(1),
            WalRecordKind::Commit,
            b"ordinary",
        );
        assert!(
            result.is_err(),
            "ordinary append skipped its original sync boundary"
        );
        let events = observed.events();
        assert_eq!(events.len(), 2);
        assert!(events[0].completed && !events[1].completed);
        assert_eq!(events[1].policy, policy);
        assert!(store.write_failed);
        drop(observed);
        drop(store);
        fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn group_first_validation_is_retryable_but_late_rejection_fences_partial_owner() {
    let path = root("validation");
    let mut store = SegmentStore::open(&path).unwrap();
    store
        .append_next_wal(
            PlacementGroupId(1),
            Epoch(2),
            WalRecordKind::Prepare,
            b"seed",
        )
        .unwrap();
    let original = bytes(&path);
    assert!(
        store
            .append_next_wal_group(PlacementGroupId(1), [record(b"old epoch")])
            .is_err()
    );
    assert!(!store.write_failed);
    assert_eq!(bytes(&path), original);
    store
        .append_next_wal(
            PlacementGroupId(1),
            Epoch(2),
            WalRecordKind::Prepare,
            b"retry",
        )
        .unwrap();
    let records = [
        WalGroupRecord {
            epoch: Epoch(3),
            ..record(b"valid")
        },
        record(b"old epoch"),
    ];
    assert!(
        store
            .append_next_wal_group(PlacementGroupId(1), records)
            .is_err()
    );
    assert!(store.write_failed);
    assert!(store.cursor.is_none());
    assert!(store.revision().is_err());
    drop(store);
    let recovered = SegmentStore::recover_existing(&path).unwrap();
    assert!(
        recovered
            .recovery
            .wal_records
            .iter()
            .all(|wal| wal.payload != b"old epoch")
    );
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn group_empty_and_none_policy_never_claim_a_flush() {
    let path = root("empty");
    let mut store = SegmentStore::open(&path).unwrap();
    let original = bytes(&path);
    let observed = sync_observation::watch(&path, None);
    store
        .append_next_wal_group(PlacementGroupId(1), [])
        .unwrap();
    assert_eq!(bytes(&path), original);
    assert!(observed.events().is_empty());
    drop(observed);
    drop(store);
    let mut unsynced =
        SegmentStore::open_with_options(&path, options(SyncPolicy::None, 0)).unwrap();
    assert!(
        unsynced
            .append_next_wal_group(PlacementGroupId(1), [record(b"rejected")])
            .is_err()
    );
    assert_eq!(bytes(&path), original);
    drop(unsynced);
    fs::remove_dir_all(path).unwrap();
}

#[cfg(unix)]
#[test]
fn group_refreshes_other_handles_and_preserves_own_revision_and_custody_boundaries() {
    let path = root("two-handles");
    let opts = options(SyncPolicy::Data, 1);
    let mut first = SegmentStore::open_with_options(&path, opts).unwrap();
    let mut second = SegmentStore::open_with_options(&path, opts).unwrap();
    let before = first.revision().unwrap();
    let second_before = second.revision().unwrap();
    let custody = first.capture_owned_custody().unwrap();
    first
        .append_next_wal_group(PlacementGroupId(1), [record(b"one"); 3])
        .unwrap();
    let own_after = first.revision().unwrap();
    assert!(before.same_writer_epoch(&own_after));
    assert_ne!(before, own_after);
    assert!(first.validate_owned_custody(&custody).is_err());
    let current = first.capture_owned_custody().unwrap();
    first.validate_owned_custody(&current).unwrap();
    let second_after = second.revision().unwrap();
    assert!(!second_before.same_writer_epoch(&second_after));
    second
        .append_next_wal(
            PlacementGroupId(2),
            Epoch(1),
            WalRecordKind::Commit,
            b"other",
        )
        .unwrap();
    first
        .append_next_wal_group(PlacementGroupId(1), [record(b"two"); 2])
        .unwrap();
    assert_eq!(
        first.wal_tail(PlacementGroupId(1)).unwrap().unwrap().index,
        LogIndex(5)
    );
    assert_eq!(
        first.wal_tail(PlacementGroupId(2)).unwrap().unwrap().index,
        LogIndex(1)
    );
    assert!(!own_after.same_writer_epoch(&first.revision().unwrap()));
    drop(first);
    drop(second);
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
fn input_iterator_unwind_cannot_expose_an_unflushed_live_cursor() {
    let path = root("unwind");
    let mut store = SegmentStore::open(&path).unwrap();
    let mut count = 0;
    let records = std::iter::from_fn(|| {
        count += 1;
        if count == 1 {
            Some(record(b"prefix"))
        } else {
            panic!("injected iterator unwind")
        }
    });
    let failure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        store
            .append_next_wal_group(PlacementGroupId(1), records)
            .unwrap();
    }));
    assert!(failure.is_err());
    assert!(store.write_failed);
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
    drop(store);
    assert_prefix(&path, 1, b"prefix");
    fs::remove_dir_all(path).unwrap();
}
