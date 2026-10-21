use super::*;
use std::path::PathBuf;

fn path(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "rsf-bootstrap-migration-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}
fn open(path: &Path) -> DurableVsrCell {
    DurableVsrCell::open(
        path,
        PlacementGroupId(1),
        Epoch(1),
        NodeId(0),
        [NodeId(0), NodeId(1), NodeId(2)],
    )
    .unwrap()
}
fn migrate(path: &Path) -> RsfResult<bool> {
    DurableVsrCell::migrate_legacy_bootstrap(
        path,
        PlacementGroupId(1),
        Epoch(1),
        NodeId(0),
        [NodeId(0), NodeId(1), NodeId(2)],
    )
}
fn legacy(root: &Path) -> PathBuf {
    let mut source = open(&root.join("source"));
    source
        .prepare(
            NodeId(0),
            VsrOperation::ReplicatedCommand {
                operation_id: OperationId(1),
                payload: b"data".to_vec(),
            },
            4,
        )
        .unwrap();
    source.acknowledge(NodeId(1), Epoch(1), LogIndex(1));
    source.commit(NodeId(0), LogIndex(1)).unwrap();
    source
        .remember_client_request(VsrClientRequestId::new("client", 7), LogIndex(1))
        .unwrap();
    source.reconfigure(
        Epoch(2),
        NodeId(0),
        [NodeId(0), NodeId(1), NodeId(2), NodeId(3)],
    );
    drop(source);
    let records = SegmentStore::recover(root.join("source"))
        .unwrap()
        .recovery
        .wal_records;
    let target = root.join("legacy");
    let mut store = SegmentStore::open(&target).unwrap();
    let mut previous_hash = [0; 32];
    for (position, record) in records.iter().skip(1).enumerate() {
        let (_, _, record) = store
            .append_wal(WalAppend {
                pg: record.pg,
                epoch: record.epoch,
                index: LogIndex(position as u64 + 1),
                kind: record.kind,
                previous_hash,
                payload: &record.payload,
            })
            .unwrap();
        previous_hash = record.record_hash;
    }
    target
}
#[test]
fn migration_preserves_prefix_receipts_and_history_and_survives_later_configuration_changes() {
    let root = path("roundtrip");
    let target = legacy(&root);
    let before = SegmentStore::recover(&target).unwrap().recovery.wal_records;
    assert!(migrate(&target).unwrap());
    assert!(!migrate(&target).unwrap());
    let after = SegmentStore::recover(&target).unwrap().recovery.wal_records;
    assert_eq!(&after[..before.len()], before.as_slice());
    assert_eq!(after.len(), before.len() + 1);
    let mut recovered = DurableVsrCell::open(
        &target,
        PlacementGroupId(1),
        Epoch(99),
        NodeId(9),
        [NodeId(9)],
    )
    .unwrap();
    assert_eq!(recovered.view(), Epoch(2));
    assert_eq!(
        recovered.replicas(),
        [NodeId(0), NodeId(1), NodeId(2), NodeId(3)]
    );
    assert_eq!(
        recovered.request_commit(&VsrClientRequestId::new("client", 7)),
        Some(LogIndex(1))
    );
    let prefix = recovered.committed_prefix().unwrap();
    assert!(matches!(
        recovered.reconfigure(Epoch(3), NodeId(2), [NodeId(0), NodeId(2), NodeId(3)]),
        VsrDecision::MembershipChanged { .. }
    ));
    assert_eq!(recovered.catch_up_from(&prefix).unwrap(), 0);
    drop(recovered);
    assert_eq!(open(&target).view(), Epoch(3));
    assert!(!migrate(&target).unwrap());
    assert!(
        DurableVsrCell::migrate_legacy_bootstrap(
            &target,
            PlacementGroupId(1),
            Epoch(2),
            NodeId(0),
            [NodeId(0), NodeId(1), NodeId(2), NodeId(3)]
        )
        .is_err()
    );
}
#[test]
fn migration_rejects_wrong_original_configuration_without_appending() {
    let target = legacy(&path("wrong-original"));
    let before = SegmentStore::recover(&target).unwrap().recovery.wal_records;
    assert!(
        DurableVsrCell::migrate_legacy_bootstrap(
            &target,
            PlacementGroupId(1),
            Epoch(1),
            NodeId(0),
            [NodeId(0), NodeId(2), NodeId(3)]
        )
        .is_err()
    );
    assert!(
        DurableVsrCell::migrate_legacy_bootstrap(
            &target,
            PlacementGroupId(1),
            Epoch(1),
            NodeId(0),
            [NodeId(0), NodeId(0)]
        )
        .is_err()
    );
    assert_eq!(
        SegmentStore::recover(&target).unwrap().recovery.wal_records,
        before
    );
    let missing = path("missing");
    assert!(migrate(&missing).is_err());
    assert!(!missing.exists());
}
#[test]
fn torn_migration_tail_recovers_original_prefix_and_can_be_retried() {
    let target = legacy(&path("torn"));
    let store = SegmentStore::open(&target).unwrap();
    let segment = target
        .join("segments")
        .join(&store.active_segment().file_name);
    drop(store);
    let before = std::fs::metadata(&segment).unwrap().len();
    assert!(migrate(&target).unwrap());
    let after = std::fs::metadata(&segment).unwrap().len();
    assert!(after > before);
    std::fs::OpenOptions::new()
        .write(true)
        .open(&segment)
        .unwrap()
        .set_len(before + (after - before) / 2)
        .unwrap();
    let mut recovered = open(&target);
    assert_eq!(recovered.committed_count(), 1);
    assert!(matches!(
        recovered.view_change(Epoch(3), NodeId(1)),
        VsrDecision::Rejected { .. }
    ));
    drop(recovered);
    assert!(migrate(&target).unwrap());
    assert!(!migrate(&target).unwrap());
}
#[test]
fn migration_decoder_rejects_wrong_prefix_duplicate_and_truncated_records() {
    let target = legacy(&path("malformed"));
    assert!(migrate(&target).unwrap());
    let records = SegmentStore::recover(&target).unwrap().recovery.wal_records;
    let last = records.last().unwrap().clone();
    for end in 5..last.payload.len() {
        let mut damaged = records.clone();
        damaged.last_mut().unwrap().payload = last.payload[..end].to_vec();
        assert!(
            recover_bootstrap(&damaged.iter().collect::<Vec<_>>(), PlacementGroupId(1)).is_err()
        );
    }
    let mut damaged = records.clone();
    let payload = damaged.last_mut().unwrap().payload.to_vec();
    let mut payload = payload;
    let n = payload.len();
    payload[n - 1] ^= 1;
    damaged.last_mut().unwrap().payload = payload;
    assert!(recover_bootstrap(&damaged.iter().collect::<Vec<_>>(), PlacementGroupId(1)).is_err());
    let mut damaged = records.clone();
    damaged.push(last);
    assert!(recover_bootstrap(&damaged.iter().collect::<Vec<_>>(), PlacementGroupId(1)).is_err());
}
