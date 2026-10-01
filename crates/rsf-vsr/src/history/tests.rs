use super::*;
use std::path::PathBuf;

fn path(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "rsf-history-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}
fn open(path: &std::path::Path) -> DurableVsrCell {
    DurableVsrCell::open(
        path,
        PlacementGroupId(1),
        Epoch(1),
        NodeId(0),
        [NodeId(0), NodeId(1), NodeId(2)],
    )
    .unwrap()
}
fn commit(cell: &mut DurableVsrCell, signers: &[NodeId]) -> VsrCommittedEntry {
    let primary = cell.primary();
    let VsrDecision::Prepared(prepare) = cell
        .prepare(
            primary,
            VsrOperation::ReplicatedCommand {
                operation_id: OperationId(cell.next_index().0),
                payload: b"historical value".to_vec(),
            },
            16,
        )
        .unwrap()
    else {
        panic!("prepare")
    };
    for signer in signers {
        cell.acknowledge(*signer, cell.view(), prepare.index);
    }
    assert!(matches!(
        cell.commit(primary, prepare.index).unwrap(),
        VsrDecision::Committed(_)
    ));
    cell.committed_prefix().unwrap().pop().unwrap()
}
fn signers(entry: &mut VsrCommittedEntry, voters: &[NodeId]) {
    entry.commit.certificate.signers = voters.iter().copied().collect();
    entry.commit.acknowledgements = voters.len();
}

#[test]
fn historical_certificates_use_their_original_voters_and_quorum_after_restart() {
    let path = path("voters");
    let mut cell = open(&path);
    let first = commit(&mut cell, &[NodeId(1)]);
    assert!(matches!(
        cell.reconfigure(
            Epoch(2),
            NodeId(0),
            [NodeId(0), NodeId(1), NodeId(2), NodeId(3)]
        ),
        VsrDecision::MembershipChanged { .. }
    ));
    let second = commit(&mut cell, &[NodeId(1), NodeId(2)]);
    assert!(matches!(
        cell.reconfigure(Epoch(3), NodeId(0), [NodeId(0), NodeId(2), NodeId(3)]),
        VsrDecision::MembershipChanged { .. }
    ));
    let third = commit(&mut cell, &[NodeId(2)]);
    assert!(matches!(
        cell.view_change(Epoch(5), NodeId(2)),
        VsrDecision::ViewChanged { .. }
    ));
    let entries = vec![first.clone(), second.clone(), third];
    for reopen in [false, true] {
        if reopen {
            drop(cell);
            cell = open(&path);
        }
        assert_eq!(cell.view(), Epoch(5));
        assert_eq!(cell.catch_up_from(&entries).unwrap(), 0);
        let mut wrong = first.clone();
        signers(&mut wrong, &[NodeId(0), NodeId(3)]);
        assert!(
            cell.catch_up_from(&[wrong])
                .unwrap_err()
                .message
                .contains("non-replica signer")
        );
        let mut wrong = second.clone();
        signers(&mut wrong, &[NodeId(0), NodeId(2)]);
        assert!(
            cell.catch_up_from(&[wrong])
                .unwrap_err()
                .message
                .contains("quorum")
        );
        assert_eq!(cell.committed_prefix().unwrap(), entries);
        assert_eq!(cell.next_index(), LogIndex(4));
    }
}

#[test]
fn stale_view_cannot_extend_a_sealed_prefix_or_claim_an_uninstalled_view() {
    let path = path("sealed");
    let mut cell = open(&path);
    let entry = commit(&mut cell, &[NodeId(1)]);
    assert!(matches!(
        cell.view_change(Epoch(3), NodeId(2)),
        VsrDecision::ViewChanged { .. }
    ));
    let mut wrong = entry.clone();
    wrong.prepare.index = LogIndex(2);
    wrong.commit.index = LogIndex(2);
    wrong.commit.certificate.index = LogIndex(2);
    assert!(
        cell.catch_up_from(&[wrong.clone()])
            .unwrap_err()
            .message
            .contains("historical view boundary")
    );
    wrong.prepare.view = Epoch(2);
    wrong.commit.view = Epoch(2);
    wrong.commit.certificate.view = Epoch(2);
    assert!(
        cell.catch_up_from(&[wrong.clone()])
            .unwrap_err()
            .message
            .contains("no installed historical configuration")
    );
    wrong.prepare.view = Epoch(3);
    wrong.commit.view = Epoch(3);
    wrong.commit.certificate.view = Epoch(3);
    assert!(
        cell.catch_up_from(&[wrong])
            .unwrap_err()
            .message
            .contains("primary differs")
    );
    assert_eq!(cell.next_index(), LogIndex(2));
    assert_eq!(cell.committed_count(), 1);
    drop(cell);
    let mut reopened = open(&path);
    let entry = commit(&mut reopened, &[NodeId(0)]);
    assert_eq!(entry.prepare.index, LogIndex(2));
    assert_eq!(entry.prepare.primary, NodeId(2));
}

#[test]
fn unknown_historical_configuration_is_not_inferred_from_current_voters() {
    let path = path("unknown");
    let mut source = open(&path.join("source"));
    let entry = commit(&mut source, &[NodeId(1)]);
    let mut target = DurableVsrCell::open(
        path.join("target"),
        PlacementGroupId(1),
        Epoch(3),
        NodeId(0),
        [NodeId(0), NodeId(1), NodeId(2)],
    )
    .unwrap();
    assert!(
        target
            .catch_up_from(&[entry])
            .unwrap_err()
            .message
            .contains("no installed historical configuration")
    );
    assert_eq!(target.committed_count(), 0);
    assert_eq!(target.next_index(), LogIndex(1));
}

#[test]
fn replay_rejects_a_duplicate_configuration_epoch() {
    let path = path("duplicate-view");
    let mut cell = open(&path);
    cell.append(
        WalRecordKind::Membership,
        &encode_view_change(&VsrViewChange {
            pg: PlacementGroupId(1),
            view: Epoch(1),
            primary: NodeId(1),
            committed_tail: LogIndex(0),
        }),
    )
    .unwrap();
    drop(cell);
    assert!(
        DurableVsrCell::open(
            &path,
            PlacementGroupId(1),
            Epoch(1),
            NodeId(0),
            [NodeId(0), NodeId(1), NodeId(2)]
        )
        .is_err()
    );
}

#[test]
fn recovery_rejects_a_commit_appended_after_its_view_was_sealed() {
    let path = path("late-old-commit");
    let mut cell = open(&path);
    let mut entry = commit(&mut cell, &[NodeId(1)]);
    assert!(matches!(
        cell.view_change(Epoch(2), NodeId(0)),
        VsrDecision::ViewChanged { .. }
    ));
    entry.prepare.index = LogIndex(2);
    entry.commit.index = LogIndex(2);
    entry.commit.certificate.index = LogIndex(2);
    // A well-formed old-view certificate is not authority to extend the prefix
    // after a later installed view sealed it, even when voters did not change.
    cell.append(WalRecordKind::Prepare, &entry.prepare.encode().unwrap())
        .unwrap();
    cell.append(WalRecordKind::Commit, &entry.commit.encode().unwrap())
        .unwrap();
    drop(cell);
    let error = DurableVsrCell::open(
        &path,
        PlacementGroupId(1),
        Epoch(1),
        NodeId(0),
        [NodeId(0), NodeId(1), NodeId(2)],
    )
    .unwrap_err();
    assert!(
        error.message.contains("historical view boundary"),
        "{}",
        error.message
    );
}

#[test]
fn legacy_wal_preserves_data_and_requires_bootstrap_migration_before_topology_changes() {
    let path = path("legacy-bootstrap");
    let mut source = open(&path.join("source"));
    let entry = commit(&mut source, &[NodeId(1)]);
    let legacy = path.join("legacy");
    let mut store = SegmentStore::open(&legacy).unwrap();
    let mut previous_hash = [0; 32];
    for (position, (kind, payload)) in [
        (WalRecordKind::Prepare, entry.prepare.encode().unwrap()),
        (WalRecordKind::Commit, entry.commit.encode().unwrap()),
    ]
    .into_iter()
    .enumerate()
    {
        store
            .append_wal(WalAppend {
                pg: PlacementGroupId(1),
                epoch: Epoch(1),
                index: LogIndex(position as u64 + 1),
                kind,
                previous_hash,
                payload: &payload,
            })
            .unwrap();
        previous_hash = store
            .recover_current()
            .unwrap()
            .recovery
            .pg_tail(PlacementGroupId(1))
            .unwrap()
            .record_hash;
    }
    drop(store);
    let before = SegmentStore::recover(&legacy).unwrap().recovery.wal_records;
    let mut recovered = open(&legacy);
    assert_eq!(recovered.committed_prefix().unwrap(), [entry.clone()]);
    for decision in [
        recovered.view_change(Epoch(2), NodeId(1)),
        recovered.timeout_election(),
        recovered.reconfigure(Epoch(2), NodeId(0), [NodeId(0), NodeId(1)]),
    ] {
        assert!(
            matches!(decision, VsrDecision::Rejected { reason, .. } if reason.contains("bootstrap migration"))
        );
    }
    assert_eq!(recovered.view(), Epoch(1));
    assert_eq!(recovered.replicas(), [NodeId(0), NodeId(1), NodeId(2)]);
    drop(recovered);
    assert_eq!(
        SegmentStore::recover(&legacy).unwrap().recovery.wal_records,
        before
    );
    assert_eq!(open(&legacy).committed_prefix().unwrap(), [entry]);
}
