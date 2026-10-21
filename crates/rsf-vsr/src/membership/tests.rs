use super::*;
use std::path::Path;
fn root(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "rsf-membership-{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}
fn proposal() -> VsrMembershipProposal {
    VsrMembershipProposal {
        election: VsrViewChangeRequest {
            pg: PlacementGroupId(1),
            previous_view: Epoch(1),
            target_view: Epoch(2),
            candidate: NodeId(1),
            voters: vec![NodeId(0), NodeId(1), NodeId(2)],
        },
        old_primary: NodeId(0),
        new_voters: vec![NodeId(0), NodeId(1), NodeId(2), NodeId(3)],
    }
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
fn fixture(path: &Path) -> Vec<DurableVsrCell> {
    let mut cells = (0..3)
        .map(|node| open(&path.join(node.to_string())))
        .collect::<Vec<_>>();
    for value in 1..=2 {
        let VsrDecision::Prepared(prepare) = cells[0]
            .prepare(
                NodeId(0),
                VsrOperation::ProtocolOperation {
                    operation_id: OperationId(value),
                },
                1,
            )
            .unwrap()
        else {
            panic!("prepare");
        };
        for (node, cell) in cells.iter_mut().enumerate().skip(1) {
            cell.receive_prepare(NodeId(node as u64), NodeId(0), prepare.clone())
                .unwrap();
        }
        if value == 1 {
            cells[0].acknowledge(NodeId(1), Epoch(1), prepare.index);
            let VsrDecision::Committed(commit) = cells[0].commit(NodeId(0), prepare.index).unwrap()
            else {
                panic!("commit");
            };
            for (node, cell) in cells.iter_mut().enumerate().skip(1) {
                cell.receive_commit(NodeId(node as u64), NodeId(0), commit.clone())
                    .unwrap();
            }
            for cell in &mut cells {
                cell.remember_client_request(VsrClientRequestId::new("original", 1), prepare.index)
                    .unwrap();
            }
        }
    }
    cells
}
#[test]
fn membership_proposal_codec_binds_old_authority_new_voters_and_target() {
    let proposal = proposal();
    let encoded = proposal.encode().unwrap();
    assert_eq!(VsrMembershipProposal::decode(&encoded).unwrap(), proposal);
    for length in 0..encoded.len() {
        assert!(VsrMembershipProposal::decode(&encoded[..length]).is_err());
    }
    let mut trailing = encoded.clone();
    trailing.push(0);
    assert!(VsrMembershipProposal::decode(&trailing).is_err());
    for voters in [
        vec![],
        vec![NodeId(0), NodeId(0), NodeId(1)],
        vec![NodeId(0), NodeId(2)],
        proposal.election.voters.clone(),
    ] {
        let mut bad = proposal.clone();
        bad.new_voters = voters;
        assert!(bad.encode().is_err());
    }
    let mut changed = proposal.clone();
    changed.new_voters.push(NodeId(4));
    assert_ne!(proposal.id().unwrap(), changed.id().unwrap());
    assert_eq!(WalRecordKind::MembershipPromise.to_u8(), 41);
    assert_eq!(
        WalRecordKind::from_u8(41),
        Some(WalRecordKind::MembershipPromise)
    );
}
#[test]
fn membership_fence_survives_restart_preserves_prefix_and_blocks_authority_shortcuts() {
    let root = root("membership-fence");
    let mut cells = fixture(&root);
    let before = cells[1].history_snapshot().unwrap().records.len();
    assert!(
        cells[1]
            .begin_membership(NodeId(1), NodeId(2), proposal())
            .is_err()
    );
    assert_eq!(cells[1].history_snapshot().unwrap().records.len(), before);
    let snapshot = cells[1]
        .begin_membership(NodeId(1), NodeId(0), proposal())
        .unwrap();
    let repeated = cells[1]
        .begin_membership(NodeId(1), NodeId(0), proposal())
        .unwrap();
    assert_eq!(snapshot.records, repeated.records);
    snapshot.validate().unwrap();
    assert_eq!(
        VsrMembershipReport::decode(&snapshot.report.encode().unwrap()).unwrap(),
        snapshot.report
    );
    drop(cells);
    for _ in 0..2 {
        let mut cell = DurableVsrCell::open_existing(root.join("1"), PlacementGroupId(1)).unwrap();
        assert_eq!(cell.membership_fence(), Some(&proposal()));
        assert_eq!(
            cell.request_commit(&VsrClientRequestId::new("original", 1)),
            Some(LogIndex(1))
        );
        assert_eq!(cell.committed_tail(), LogIndex(1));
        assert_eq!(cell.next_index(), LogIndex(3));
        assert_eq!(cell.replica_count(), 3);
        assert!(!cell.contains_replica(NodeId(3)));
        assert!(cell.primary_probe().is_err());
        assert!(
            cell.prepare(
                NodeId(0),
                VsrOperation::ProtocolOperation {
                    operation_id: OperationId(99)
                },
                1
            )
            .is_err()
        );
        let mut election = proposal().election;
        election.target_view = Epoch(3);
        assert!(
            cell.begin_view_change(NodeId(1), NodeId(1), election)
                .is_err()
        );
        assert!(matches!(
            cell.reconfigure(Epoch(3), NodeId(1), [NodeId(1), NodeId(2)]),
            VsrDecision::Rejected { .. }
        ));
        assert!(cell.membership_snapshot(NodeId(1), NodeId(3)).is_err());
        assert_eq!(
            cell.membership_snapshot(NodeId(1), NodeId(2))
                .unwrap()
                .records,
            snapshot.records
        );
        let mut changed = proposal();
        changed.new_voters.push(NodeId(4));
        assert!(
            cell.begin_membership(NodeId(1), NodeId(0), changed)
                .is_err()
        );
    }
}
#[test]
fn membership_old_quorum_approval_rejects_mixed_duplicate_and_tampered_histories() {
    let root = root("membership-quorum");
    let mut cells = fixture(&root);
    let a = cells[1]
        .begin_membership(NodeId(1), NodeId(0), proposal())
        .unwrap();
    let b = cells[2]
        .begin_membership(NodeId(2), NodeId(0), proposal())
        .unwrap();
    assert!(VsrMembershipApproval::from_snapshots(vec![a.clone()]).is_err());
    assert!(VsrMembershipApproval::from_snapshots(vec![a.clone(), a.clone()]).is_err());
    let mut changed = b.clone();
    changed.report.proposal.new_voters.push(NodeId(4));
    assert!(VsrMembershipApproval::from_snapshots(vec![a.clone(), changed]).is_err());
    let mut changed = b.clone();
    changed.records.last_mut().unwrap().kind = WalRecordKind::ViewPromise;
    assert!(changed.validate().is_err());
    let mut fork = open(&root.join("fork"));
    fork.prepare(
        NodeId(0),
        VsrOperation::ProtocolOperation {
            operation_id: OperationId(99),
        },
        1,
    )
    .unwrap();
    let fork = fork
        .begin_membership(NodeId(2), NodeId(0), proposal())
        .unwrap();
    assert!(VsrMembershipApproval::from_snapshots(vec![a.clone(), fork]).is_err());
    let approval = VsrMembershipApproval::from_snapshots(vec![a, b]).unwrap();
    assert_eq!(approval.proposal(), &proposal());
    assert_eq!(approval.snapshots().len(), 2);
    // Even a separately quorum-selected ordinary plan for the same target cannot
    // overwrite the membership choice on a locally fenced replica.
    let mut normal = fixture(&root.join("normal"));
    let snapshots = [
        normal[0]
            .begin_view_change(NodeId(0), NodeId(1), proposal().election)
            .unwrap(),
        normal[2]
            .begin_view_change(NodeId(2), NodeId(1), proposal().election)
            .unwrap(),
    ];
    let plan = VsrViewInstallation::select(&snapshots).unwrap();
    assert!(cells[1].install_view(&plan).is_err());
    assert!(
        cells[1]
            .begin_installation_input(
                NodeId(1),
                NodeId(1),
                VsrInstallationInput::from_snapshots(&snapshots).unwrap()
            )
            .is_err()
    );
}
#[test]
fn malformed_membership_fence_envelopes_fail_repeated_recovery_without_rewrite() {
    let root = root("membership-malformed");
    let mut cell = open(&root);
    cell.store
        .append_next_wal(
            PlacementGroupId(1),
            Epoch(1),
            WalRecordKind::Prepare,
            &proposal().encode().unwrap(),
        )
        .unwrap();
    drop(cell);
    let before = rsf_storage::SegmentStore::recover_existing(&root)
        .unwrap()
        .recovery
        .wal_records;
    for _ in 0..2 {
        assert!(DurableVsrCell::open_existing(&root, PlacementGroupId(1)).is_err());
        let after = rsf_storage::SegmentStore::recover_existing(&root)
            .unwrap()
            .recovery
            .wal_records;
        assert_eq!(after, before);
    }
}

#[test]
fn membership_fence_child() {
    let Some(root) = std::env::var_os("RSF_MEMBER_FENCE_CHILD_ROOT") else {
        return;
    };
    let root = std::path::PathBuf::from(root);
    let mut cell = DurableVsrCell::open_existing(root.join("1"), PlacementGroupId(1)).unwrap();
    cell.begin_membership_with_hook(NodeId(1), NodeId(0), proposal(), || {
        std::fs::write(root.join("synced-fence"), b"before reply").unwrap();
        loop {
            std::thread::park();
        }
    })
    .unwrap();
}
#[test]
fn killed_membership_writer_recovers_synced_fence_before_acknowledgement() {
    let root = root("membership-kill-before-ack");
    drop(fixture(&root));
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "membership::tests::membership_fence_child",
            "--nocapture",
        ])
        .env("RSF_MEMBER_FENCE_CHILD_ROOT", &root)
        .stdout(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let until = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while !root.join("synced-fence").exists() && std::time::Instant::now() < until {
        assert!(
            child.try_wait().unwrap().is_none(),
            "membership writer exited before its durability boundary"
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    if !root.join("synced-fence").exists() {
        let _ = child.kill();
        let _ = child.wait();
        panic!("membership fence durability boundary not reached");
    }
    child.kill().unwrap();
    child.wait().unwrap();
    let mut recovered = DurableVsrCell::open_existing(root.join("1"), PlacementGroupId(1)).unwrap();
    assert_eq!(recovered.membership_fence(), Some(&proposal()));
    assert_eq!(
        recovered.request_commit(&VsrClientRequestId::new("original", 1)),
        Some(LogIndex(1))
    );
    assert_eq!(recovered.next_index(), LogIndex(3));
    assert_eq!(recovered.committed_tail(), LogIndex(1));
    assert!(recovered.primary_probe().is_err());
    recovered
        .membership_snapshot(NodeId(1), NodeId(2))
        .unwrap()
        .validate()
        .unwrap();
}
