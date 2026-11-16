use super::*;

#[test]
fn owned_group_matches_sequential_bytes_and_returns_actual_tail_and_authorized_custody() {
    for policy in [SyncPolicy::Data, SyncPolicy::Full] {
        let path = root("equivalence");
        let a = path.join("owned");
        let b = path.join("ordinary");
        let payload = b"bounded group records";
        let limit = wal_record_disk_bytes(payload.len() as u64).unwrap() * 2;
        let mut owned = SegmentStore::open_with_options(&a, options(policy, limit)).unwrap();
        let original = owned.capture_owned_custody().unwrap();
        let observed = sync_observation::watch(&a, None);
        let result = owned
            .try_append_owned_wal_group(
                &original,
                PlacementGroupId(1),
                (0..5).map(|_| record(payload)),
            )
            .unwrap();
        assert_eq!(result.tail().unwrap().index, LogIndex(5));
        let events = observed.events();
        assert_eq!(events.len(), 3);
        for (position, event) in events.iter().enumerate() {
            assert!(event.completed);
            assert_eq!(event.policy, policy);
            let manifest = SegmentManifest::decode(&event.manifest).unwrap();
            assert_eq!(manifest.active_id, position as u64 + 1);
            assert_eq!(
                event.path,
                a.join(SEGMENT_DIR)
                    .join(segment_file_name(position as u64 + 1))
            );
        }
        let current = result.into_custody();
        owned.validate_owned_custody(&current).unwrap();
        assert!(owned.validate_owned_custody(&original).is_err());
        drop(observed);
        let mut ordinary = SegmentStore::open_with_options(&b, options(policy, limit)).unwrap();
        for _ in 0..5 {
            ordinary
                .append_next_wal(
                    PlacementGroupId(1),
                    Epoch(1),
                    WalRecordKind::Prepare,
                    payload,
                )
                .unwrap();
        }
        assert_eq!(content(&a), content(&b));
        let observed = sync_observation::watch(&a, None);
        owned
            .append_next_wal(
                PlacementGroupId(1),
                Epoch(1),
                WalRecordKind::Commit,
                b"after group",
            )
            .unwrap();
        assert_eq!(observed.events().len(), 1);
        assert!(observed.events()[0].completed);
        assert_eq!(observed.events()[0].policy, policy);
        ordinary
            .append_next_wal(
                PlacementGroupId(1),
                Epoch(1),
                WalRecordKind::Commit,
                b"after group",
            )
            .unwrap();
        assert_eq!(content(&a), content(&b));
        drop(observed);
        drop(owned);
        drop(ordinary);
        let recovered = SegmentStore::recover_existing(&a).unwrap();
        assert_eq!(recovered.recovery.wal_records.len(), 6);
        assert_eq!(
            recovered.recovery.wal_records.last().unwrap().payload,
            b"after group"
        );
        fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn owned_group_clean_first_input_error_keeps_original_owner_but_late_error_is_not_eof() {
    let path = root("producer");
    let mut store = seeded(&path, SyncPolicy::Data, 0);
    let original = store.capture_owned_custody().unwrap();
    let before = content(&path);
    let error = store
        .try_append_owned_wal_group::<&[u8]>(&original, PlacementGroupId(1), [Err(input_error())])
        .unwrap_err();
    assert_eq!(error.message, "injected producer rejection");
    assert!(!store.write_failed);
    store.validate_owned_custody(&original).unwrap();
    assert_eq!(content(&path), before);
    let error = store
        .try_append_owned_wal_group(
            &original,
            PlacementGroupId(1),
            [
                record(b"prefix"),
                Err(input_error()),
                record(b"never requested"),
            ],
        )
        .unwrap_err();
    assert_eq!(error.message, "injected producer rejection");
    assert!(store.write_failed && store.cursor.is_none());
    assert!(
        store
            .try_append_owned_wal_group(&original, PlacementGroupId(1), [record(b"forbidden")])
            .is_err()
    );
    drop(store);
    let records = SegmentStore::recover_existing(&path)
        .unwrap()
        .recovery
        .wal_records;
    assert_eq!(records.len(), 2);
    assert_eq!(records[1].payload, b"prefix");
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn owned_group_empty_retry_renews_active_and_none_policy_never_claims_durability() {
    for policy in [SyncPolicy::Data, SyncPolicy::Full] {
        let path = root("empty");
        let mut store = seeded(&path, policy, 0);
        let original = store.capture_owned_custody().unwrap();
        let before = content(&path);
        let reads = crate::reader::payload_reads();
        let observed = sync_observation::watch(&path, None);
        let result = store
            .try_append_owned_wal_group::<&[u8]>(&original, PlacementGroupId(1), [])
            .unwrap();
        assert_eq!(result.tail().unwrap().index, LogIndex(1));
        assert_eq!(observed.events().len(), 1);
        assert!(observed.events()[0].completed);
        assert_eq!(crate::reader::payload_reads(), reads);
        assert_eq!(content(&path), before);
        store
            .validate_owned_custody(&result.into_custody())
            .unwrap();
        store.validate_owned_custody(&original).unwrap();
        drop(observed);
        drop(store);
        fs::remove_dir_all(path).unwrap();
    }
    let path = root("none");
    let mut store = seeded(&path, SyncPolicy::None, 0);
    let original = store.capture_owned_custody().unwrap();
    assert_eq!(
        store
            .try_append_owned_wal_group(&original, PlacementGroupId(1), [record(b"no")])
            .unwrap_err()
            .code,
        ErrorCode::InvalidInput
    );
    assert!(!store.write_failed);
    store.validate_owned_custody(&original).unwrap();
    drop(store);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn owned_group_actual_sync_failure_or_producer_unwind_never_returns_authorized_custody() {
    for policy in [SyncPolicy::Data, SyncPolicy::Full] {
        let path = root("sync-error");
        let mut store = seeded(&path, policy, 0);
        let original = store.capture_owned_custody().unwrap();
        let observed = sync_observation::watch(&path, Some(0));
        let error = store
            .try_append_owned_wal_group(&original, PlacementGroupId(1), [record(b"visible")])
            .unwrap_err();
        assert!(error.message.contains("injected append sync failure"));
        assert_eq!(observed.events().len(), 1);
        assert!(!observed.events()[0].completed);
        assert!(store.write_failed && store.cursor.is_none());
        drop(observed);
        drop(store);
        // The hook reports uncertain durability, not physical loss. Complete
        // bytes may remain visible and cold replay must retain their prefix.
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
    let path = root("unwind");
    let mut store = seeded(&path, SyncPolicy::Full, 0);
    let original = store.capture_owned_custody().unwrap();
    let mut calls = 0;
    let producer = std::iter::from_fn(|| {
        calls += 1;
        if calls == 1 {
            Some(record(b"prefix"))
        } else {
            panic!("injected producer unwind")
        }
    });
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        store
            .try_append_owned_wal_group(&original, PlacementGroupId(1), producer)
            .unwrap();
    }));
    assert!(failed.is_err());
    assert!(store.write_failed && store.cursor.is_none());
    assert!(store.capture_owned_custody().is_err());
    drop(store);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn owned_group_releases_each_owned_payload_before_polling_the_next() {
    use std::{cell::Cell, rc::Rc};
    struct Payload {
        bytes: Vec<u8>,
        live: Rc<Cell<usize>>,
    }
    impl AsRef<[u8]> for Payload {
        fn as_ref(&self) -> &[u8] {
            &self.bytes
        }
    }
    impl Drop for Payload {
        fn drop(&mut self) {
            assert_eq!(self.live.get(), 1);
            self.live.set(0);
        }
    }
    let path = root("payload-lifetime");
    let mut store = seeded(&path, SyncPolicy::Full, 1);
    let original = store.capture_owned_custody().unwrap();
    let live = Rc::new(Cell::new(0));
    let observed = live.clone();
    let mut ordinal = 0;
    let records = std::iter::from_fn(move || {
        assert_eq!(
            observed.get(),
            0,
            "previous owned payload still retained at producer poll"
        );
        if ordinal == 5 {
            return None;
        }
        ordinal += 1;
        observed.set(1);
        Some(Ok(WalGroupEntry {
            epoch: Epoch(1),
            kind: WalRecordKind::Prepare,
            payload: Payload {
                bytes: vec![ordinal; 32],
                live: observed.clone(),
            },
        }))
    });
    let result = store
        .try_append_owned_wal_group(&original, PlacementGroupId(1), records)
        .unwrap();
    assert_eq!(result.tail().unwrap().index, LogIndex(6));
    assert_eq!(live.get(), 0);
    store
        .validate_owned_custody(&result.into_custody())
        .unwrap();
    drop(store);
    let recovered = SegmentStore::recover_existing(&path).unwrap();
    assert_eq!(recovered.recovery.wal_records.len(), 6);
    for (index, record) in recovered.recovery.wal_records[1..].iter().enumerate() {
        assert_eq!(record.payload, vec![index as u8 + 1; 32]);
    }
    fs::remove_dir_all(path).unwrap();
}
