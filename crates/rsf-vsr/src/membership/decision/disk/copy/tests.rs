use super::*;
use std::sync::atomic::{AtomicU64, Ordering};
fn reserved(label: &str) -> std::path::PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    loop {
        let path = crate::membership::tests::root(&format!(
            "group-copy-{label}-{}",
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        match fs::create_dir(&path) {
            Ok(()) => return path,
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => panic!("{}: {e}", path.display()),
        }
    }
}
fn history() -> Vec<VsrHistoryRecord> {
    (0..70)
        .map(|n| VsrHistoryRecord {
            epoch: Epoch(1),
            kind: WalRecordKind::Prepare,
            payload: vec![n as u8; if n == 33 { 128 * 1024 } else { 40 }],
        })
        .collect()
}
fn options() -> SegmentStoreOptions {
    SegmentStoreOptions {
        log_options: rsf_storage::SegmentLogOptions {
            sync_policy: rsf_storage::SyncPolicy::Full,
        },
        max_segment_bytes: 1024,
    }
}
#[test]
fn local_copy_stream_preserves_exact_packing_bytes_for_partial_and_empty_suffixes() {
    let path = reserved("bytes");
    let records = history();
    let mut single = SegmentStore::open_with_options(path.join("single"), options()).unwrap();
    let mut ordinal = 0;
    let mut physical = 0;
    while ordinal < records.len() {
        let batch = VsrHistoryBatch::pack(ordinal as u64, &records[ordinal..]).unwrap();
        ordinal += batch.records.len();
        physical += 1;
        single
            .append_next_wal(
                PlacementGroupId(1),
                Epoch(2),
                WalRecordKind::ViewInstallation,
                &batch.encode().unwrap(),
            )
            .unwrap();
    }
    assert!(
        physical >= 4,
        "fixture must exercise count and oversized-record boundaries"
    );
    drop(single);
    let mut grouped = SegmentStore::open_with_options(path.join("grouped"), options()).unwrap();
    let first = VsrHistoryBatch::pack(0, &records).unwrap();
    grouped
        .append_next_wal(
            PlacementGroupId(1),
            Epoch(2),
            WalRecordKind::ViewInstallation,
            &first.encode().unwrap(),
        )
        .unwrap();
    append_history_suffix(
        &mut grouped,
        PlacementGroupId(1),
        Epoch(2),
        &records,
        first.records.len(),
    )
    .unwrap();
    append_history_suffix(
        &mut grouped,
        PlacementGroupId(1),
        Epoch(2),
        &records,
        records.len(),
    )
    .unwrap();
    drop(grouped);
    let single = SegmentStore::recover_existing(path.join("single"))
        .unwrap()
        .recovery
        .wal_records;
    let grouped = SegmentStore::recover_existing(path.join("grouped"))
        .unwrap()
        .recovery
        .wal_records;
    assert_eq!(single, grouped);
    fs::remove_dir_all(path).unwrap();
}
#[test]
fn local_copy_late_encoding_error_fences_store_and_existing_caller_latch() {
    let path = reserved("encode");
    let mut records = history();
    records.push(VsrHistoryRecord {
        epoch: Epoch(1),
        kind: WalRecordKind::Prepare,
        payload: vec![1; crate::transfer::MAX_HISTORY_RECORD_BYTES + 1],
    });
    let mut store = SegmentStore::open_with_options(&path, options()).unwrap();
    let mut failed = false;
    assert!(
        super::super::copy_with_failure(&mut failed, || {
            append_history_suffix(&mut store, PlacementGroupId(1), Epoch(2), &records, 0)
        })
        .is_err()
    );
    assert!(failed);
    assert!(
        store.revision().is_err(),
        "producer error followed actual mutation"
    );
    let mut ran = false;
    assert!(
        super::super::copy_with_failure(&mut failed, || {
            ran = true;
            Ok(())
        })
        .is_err()
    );
    assert!(!ran, "already-failed caller reentered a copy");
    drop(store);
    let recovered = SegmentStore::recover_existing(&path).unwrap();
    assert!(!recovered.recovery.wal_records.is_empty());
    let retained = recovered
        .recovery
        .wal_records
        .iter()
        .flat_map(|r| VsrHistoryBatch::decode(&r.payload).unwrap().records)
        .collect::<Vec<_>>();
    assert!(history().starts_with(&retained));
    assert!(retained.len() < records.len());
    fs::remove_dir_all(path).unwrap();
}
#[test]
fn local_copy_failure_guard_survives_unwind_without_reenabling_caller() {
    let mut failed = false;
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = super::super::copy_with_failure(&mut failed, || -> RsfResult<()> {
            panic!("copy producer unwind")
        });
    }));
    assert!(panic.is_err());
    assert!(failed);
    assert!(super::super::copy_with_failure(&mut failed, || Ok(())).is_err());
}
#[test]
fn local_copy_complete_suffix_still_reaches_closed_and_active_renewal_boundaries() {
    for failure in [0, 1] {
        let path = reserved("empty-renewal");
        let records = history();
        let mut store = SegmentStore::open_with_options(&path, options()).unwrap();
        append_history_suffix(&mut store, PlacementGroupId(1), Epoch(2), &records, 0).unwrap();
        drop(store);
        let mut store = SegmentStore::open_with_options(&path, options()).unwrap();
        let observed = observation::watch(Some(failure));
        let mut failed = false;
        let error = super::super::copy_with_failure(&mut failed, || {
            append_history_suffix(
                &mut store,
                PlacementGroupId(1),
                Epoch(2),
                &records,
                records.len(),
            )
        })
        .unwrap_err();
        assert_eq!(error.message, "injected local-copy boundary");
        assert!(failed);
        let events = observed.events();
        assert_eq!(events.len(), failure + 1);
        assert!(
            events
                .iter()
                .all(|e| e.retained == records.len() && e.total == records.len())
        );
        assert_eq!(
            events.last().unwrap().boundary,
            if failure == 0 {
                observation::Boundary::Closed
            } else {
                observation::Boundary::Active
            }
        );
        drop(observed);
        drop(store);
        fs::remove_dir_all(path).unwrap();
    }
}
