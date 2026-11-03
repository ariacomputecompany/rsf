use super::*;
use crate::membership::tests::{fixture as old_fixture, proposal, root};
struct Fixture {
    cells: Vec<DurableVsrCell>,
    approval: VsrMembershipApproval,
    chosen: VsrMembershipChosen,
    alternate: VsrMembershipChosen,
    learner: LearnerHistoryStore,
}
fn fixture(path: &Path) -> Fixture {
    let mut cells = old_fixture(&path.join("old"));
    cells[0].acknowledge(NodeId(1), Epoch(1), LogIndex(2));
    let VsrDecision::Committed(second) = cells[0].commit(NodeId(0), LogIndex(2)).unwrap() else {
        panic!("second commit");
    };
    cells[2]
        .receive_commit(NodeId(2), NodeId(0), second)
        .unwrap();
    cells[2]
        .remember_client_request(VsrClientRequestId::new("second-owned", 2), LogIndex(2))
        .unwrap();
    let VsrDecision::Prepared(third) = cells[0]
        .prepare(
            NodeId(0),
            VsrOperation::ProtocolOperation {
                operation_id: OperationId(3),
            },
            1,
        )
        .unwrap()
    else {
        panic!("third prepare");
    };
    cells[1]
        .receive_prepare(NodeId(1), NodeId(0), third)
        .unwrap();
    let mut learner = LearnerHistoryStore::open(
        path.join("learner"),
        PlacementGroupId(1),
        NodeId(3),
        NodeId(0),
    )
    .unwrap();
    let snapshot = cells[0].history_snapshot().unwrap();
    let id = snapshot.descriptor.id().unwrap();
    learner.begin(snapshot.descriptor).unwrap();
    for (n, record) in snapshot.records.into_iter().enumerate() {
        learner.append(id, n as u64, record).unwrap();
    }
    learner.finish(id).unwrap();
    // Receiver-local metadata absent from the selected old-quorum reports.
    cells[0]
        .remember_client_request(VsrClientRequestId::new("receiver-extra", 9), LogIndex(1))
        .unwrap();
    let approval = VsrMembershipApproval::from_snapshots(vec![
        cells[1]
            .begin_membership(NodeId(1), NodeId(0), proposal())
            .unwrap(),
        cells[2]
            .begin_membership(NodeId(2), NodeId(0), proposal())
            .unwrap(),
    ])
    .unwrap();
    let input = VsrMembershipInput::from_approval(&approval).unwrap();
    let round = VsrMembershipRound {
        counter: 1,
        coordinator: NodeId(1),
    };
    let mut decisions = (0..4)
        .map(|n| {
            MembershipDecisionStore::open(path.join(format!("decision-{n}")), NodeId(n), &approval)
                .unwrap()
        })
        .collect::<Vec<_>>();
    let promises = decisions
        .iter_mut()
        .map(|s| s.promise(NodeId(1), round).unwrap())
        .collect::<Vec<_>>();
    let decision = VsrMembershipDecision::from_promises(promises, input).unwrap();
    let nomination = decisions[1].nominate(&decision).unwrap();
    let reports = [
        nomination.clone(),
        decisions[0]
            .accept(NodeId(1), &decision, &nomination)
            .unwrap(),
        decisions[2]
            .accept(NodeId(1), &decision, &nomination)
            .unwrap(),
        decisions[3]
            .accept(NodeId(1), &decision, &nomination)
            .unwrap(),
    ];
    let chosen = VsrMembershipChosen::from_accepted(reports[..3].to_vec()).unwrap();
    let alternate = VsrMembershipChosen::from_accepted(reports[1..].to_vec()).unwrap();
    Fixture {
        cells,
        approval,
        chosen,
        alternate,
        learner,
    }
}
fn plan(f: &Fixture, alternate: bool) -> VsrMembershipStagedPlan {
    VsrMembershipStagedPlan::select(
        if alternate {
            f.alternate.clone()
        } else {
            f.chosen.clone()
        },
        VsrMembershipApproval::from_snapshots(f.approval.snapshots().to_vec()).unwrap(),
    )
    .unwrap()
}
#[test]
fn chosen_membership_retention_retry_rejects_missing_input_without_recreation() {
    let path = root("chosen-retention-retry");
    let f = fixture(&path);
    let decision_path = path.join("decision-1");
    let mut decision =
        MembershipDecisionStore::open_existing(&decision_path, PlacementGroupId(1), NodeId(1))
            .unwrap();
    decision
        .retain_chosen_input(f.chosen.clone(), &f.approval)
        .unwrap();
    let count = SegmentStore::recover_existing(decision_path.join("journal"))
        .unwrap()
        .recovery
        .wal_records
        .len();
    decision
        .retain_chosen_input(f.alternate.clone(), &f.approval)
        .unwrap();
    assert_eq!(
        SegmentStore::recover_existing(decision_path.join("journal"))
            .unwrap()
            .recovery
            .wal_records
            .len(),
        count,
        "an equivalent chosen-proof subset appended another durable decision"
    );
    let name = f
        .chosen
        .input_id()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    let descriptor = decision_path.join("inputs").join(name).join("DESCRIPTOR");
    fs::remove_file(&descriptor).unwrap();
    assert!(
        decision
            .retain_chosen_input(f.chosen.clone(), &f.approval)
            .is_err()
    );
    assert!(!descriptor.exists(), "retry recreated missing chosen input");
    drop(decision);
    assert!(
        MembershipDecisionStore::open_existing(&decision_path, PlacementGroupId(1), NodeId(1))
            .is_err()
    );
    drop(f);
    fs::remove_dir_all(path).unwrap();
}
#[test]
fn chosen_membership_stage_merges_commit_and_receipt_evidence_deterministically() {
    let path = root("stage-merge");
    let mut f = fixture(&path);
    let first = plan(&f, false);
    let other = plan(&f, true);
    assert_ne!(f.chosen.encode().unwrap(), f.alternate.encode().unwrap());
    assert_eq!(first.id(), other.id());
    assert_eq!(first.records, other.records);
    assert_eq!(first.committed_tail(), LogIndex(2));
    assert_eq!(first.accepted_tail(), LogIndex(3));
    let committed = first.committed_entries().unwrap();
    assert_eq!(committed.len(), 2);
    assert_eq!(committed[1], f.cells[2].committed_prefix().unwrap()[1]);
    assert_eq!(first.accepted_entries().len(), 3);
    assert_eq!(
        first.accepted_entries()[2].operation,
        VsrOperation::ProtocolOperation {
            operation_id: OperationId(3)
        }
    );
    assert_eq!(
        first.request_commit(&VsrClientRequestId::new("second-owned", 2)),
        Some(LogIndex(2))
    );
    assert_eq!(
        first.request_commit(&VsrClientRequestId::new("original", 1)),
        Some(LogIndex(1))
    );
    assert!(
        first
            .request_commit(&VsrClientRequestId::new("receiver-extra", 9))
            .is_none()
    );
    let reversed = VsrMembershipApproval::from_snapshots(
        f.approval.snapshots().iter().rev().cloned().collect(),
    )
    .unwrap();
    assert_eq!(
        VsrMembershipStagedPlan::select(f.chosen.clone(), reversed)
            .unwrap()
            .id(),
        first.id()
    );
    assert!(f.cells[1].primary_probe().is_err());
    assert!(!f.cells[1].contains_replica(NodeId(3)));
    let before = f.cells[0].recovery_state_token().unwrap();
    f.cells[0]
        .remember_client_request(
            VsrClientRequestId::new("cache-stamp-change", 10),
            LogIndex(1),
        )
        .unwrap();
    assert_ne!(before, f.cells[0].recovery_state_token().unwrap());
    assert!(f.cells[1].state_token().is_err());
    assert!(f.cells[1].recovery_state_token().is_ok());
    assert!(f.cells[1].primary_probe().is_err());
    let additional = f.cells[0]
        .begin_membership(NodeId(0), NodeId(0), proposal())
        .unwrap();
    let another =
        VsrMembershipApproval::from_snapshots(vec![additional, f.approval.snapshots()[0].clone()])
            .unwrap();
    assert!(VsrMembershipStagedPlan::select(f.chosen.clone(), another).is_err());
    let mut invalid = plan(&f, false);
    let mut records = invalid.records.clone();
    records.push(VsrHistoryRecord {
        epoch: Epoch(1),
        kind: WalRecordKind::ViewInstallation,
        payload: installation::membership_client_delta(&VsrClientRequestCommit {
            pg: PlacementGroupId(1),
            request: VsrClientRequestId::new("uncommitted", 3),
            index: LogIndex(3),
        })
        .unwrap(),
    });
    assert!(invalid.apply_local_suffix(&records).is_err());
}
#[test]
fn membership_stage_materialization_is_boot_scoped_and_preserves_receiver_receipts() {
    let path = root("stage-materialize");
    let f = fixture(&path);
    let mut stage = MembershipStagingStore::stage(
        path.join("stage-0"),
        NodeId(0),
        plan(&f, false),
        &f.cells[0],
    )
    .unwrap();
    assert!(stage.report().is_err());
    assert_eq!(
        stage
            .plan()
            .request_commit(&VsrClientRequestId::new("receiver-extra", 9)),
        Some(LogIndex(1))
    );
    let mut applied = BTreeMap::new();
    assert!(
        stage
            .materialize(&mut |entry| {
                if entry.prepare.index == LogIndex(2) {
                    return Err(invalid_input("injected materializer error"));
                }
                applied.insert(entry.prepare.index, entry.prepare.operation.clone());
                Ok(())
            })
            .is_err()
    );
    assert!(stage.report().is_err());
    let report = stage
        .materialize(&mut |entry| {
            applied.insert(entry.prepare.index, entry.prepare.operation.clone());
            Ok(())
        })
        .unwrap();
    assert_eq!(applied.len(), 2);
    assert_eq!(report.committed_tail, LogIndex(2));
    assert_eq!(report.accepted_tail, LogIndex(3));
    let id = stage.stage_id();
    drop(stage);
    let mut stage =
        MembershipStagingStore::open_existing(path.join("stage-0"), PlacementGroupId(1), NodeId(0))
            .unwrap();
    assert!(stage.report().is_err());
    assert_eq!(stage.stage_id(), id);
    let mut calls = 0;
    assert_eq!(
        stage
            .materialize(&mut |_| {
                calls += 1;
                Ok(())
            })
            .unwrap(),
        report
    );
    assert_eq!(calls, 2);
    drop(stage);
    let mut retry =
        MembershipStagingStore::stage(path.join("stage-0"), NodeId(0), plan(&f, true), &f.cells[0])
            .unwrap();
    assert_eq!(retry.stage_id(), id);
    assert!(retry.report().is_err());
    assert_eq!(retry.materialize(&mut |_| Ok(())).unwrap(), report);
    // Wrapped selected-history batches are not a writable VSR generation.
    assert!(
        DurableVsrCell::open(
            path.join("stage-0/selected"),
            PlacementGroupId(1),
            Epoch(2),
            NodeId(1),
            [NodeId(0), NodeId(1), NodeId(2), NodeId(3)]
        )
        .is_err()
    );
    assert_eq!(f.cells[0].replica_count(), 3);
    assert!(!f.cells[0].contains_replica(NodeId(3)));
}
#[test]
fn staged_membership_quorums_reject_mixed_duplicate_and_invalid_receipts() {
    let path = root("stage-quorum");
    let f = fixture(&path);
    let mut zero = MembershipStagingStore::stage(
        path.join("stage-0"),
        NodeId(0),
        plan(&f, false),
        &f.cells[0],
    )
    .unwrap();
    let mut two = MembershipStagingStore::stage(
        path.join("stage-2"),
        NodeId(2),
        plan(&f, false),
        &f.cells[2],
    )
    .unwrap();
    let mut three = f
        .learner
        .stage_membership(path.join("stage-3"), plan(&f, true))
        .unwrap();
    let reports = vec![
        zero.materialize(&mut |_| Ok(())).unwrap(),
        two.materialize(&mut |_| Ok(())).unwrap(),
        three.materialize(&mut |_| Ok(())).unwrap(),
    ];
    assert_ne!(reports[0].stage, reports[1].stage);
    assert_eq!(reports[0].plan, reports[1].plan);
    assert!(VsrMembershipStagedQuorum::from_reports(reports[..2].to_vec()).is_err());
    assert!(VsrMembershipStagedQuorum::from_reports(vec![reports[0].clone(); 3]).is_err());
    let mut mixed = reports.clone();
    mixed[1].plan[0] ^= 1;
    assert!(VsrMembershipStagedQuorum::from_reports(mixed).is_err());
    let mut invalid = reports[0].clone();
    invalid.replica = NodeId(99);
    assert!(invalid.encode().is_err());
    for report in &reports {
        let bytes = report.encode().unwrap();
        assert_eq!(VsrMembershipStagedReport::decode(&bytes).unwrap(), *report);
        for n in 0..bytes.len() {
            assert!(VsrMembershipStagedReport::decode(&bytes[..n]).is_err());
        }
    }
    let quorum = VsrMembershipStagedQuorum::from_reports(reports).unwrap();
    let bytes = quorum.encode().unwrap();
    assert_eq!(VsrMembershipStagedQuorum::decode(&bytes).unwrap(), quorum);
    for n in 0..bytes.len() {
        assert!(VsrMembershipStagedQuorum::decode(&bytes[..n]).is_err());
    }
    assert_eq!(f.learner.committed_entries().unwrap().len(), 2);
    assert_eq!(quorum.input_id(), f.chosen.input_id());
}
#[test]
fn membership_stage_reopen_rejects_torn_or_missing_sealed_history_without_rewrite() {
    let path = root("stage-corrupt");
    let f = fixture(&path);
    let stage = MembershipStagingStore::stage(
        path.join("missing"),
        NodeId(0),
        plan(&f, false),
        &f.cells[0],
    )
    .unwrap();
    drop(stage);
    let meta = fs::read(path.join("missing/META")).unwrap();
    fs::remove_dir_all(path.join("missing/selected")).unwrap();
    for _ in 0..2 {
        assert!(
            MembershipStagingStore::open_existing(
                path.join("missing"),
                PlacementGroupId(1),
                NodeId(0)
            )
            .is_err()
        );
        assert!(
            MembershipStagingStore::stage(
                path.join("missing"),
                NodeId(0),
                plan(&f, false),
                &f.cells[0]
            )
            .is_err()
        );
        assert_eq!(fs::read(path.join("missing/META")).unwrap(), meta);
        assert!(!path.join("missing/selected").exists());
    }
    let stage =
        MembershipStagingStore::stage(path.join("torn"), NodeId(2), plan(&f, false), &f.cells[2])
            .unwrap();
    drop(stage);
    let store = SegmentStore::open(path.join("torn/selected")).unwrap();
    let segment = path
        .join("torn/selected/segments")
        .join(&store.active_segment().file_name);
    drop(store);
    let file = OpenOptions::new().write(true).open(segment).unwrap();
    file.set_len(file.metadata().unwrap().len() - 1).unwrap();
    file.sync_all().unwrap();
    drop(file);
    for _ in 0..2 {
        assert!(
            MembershipStagingStore::open_existing(
                path.join("torn"),
                PlacementGroupId(1),
                NodeId(2)
            )
            .is_err()
        );
    }
    let staged = MembershipStagingStore::stage(
        path.join("bad-meta"),
        NodeId(0),
        plan(&f, false),
        &f.cells[0],
    )
    .unwrap();
    drop(staged);
    let mut metadata = fs::read(path.join("bad-meta/META")).unwrap();
    let last = metadata.len() - 1;
    metadata[last] ^= 1;
    fs::write(path.join("bad-meta/META"), &metadata).unwrap();
    for _ in 0..2 {
        assert!(
            MembershipStagingStore::open_existing(
                path.join("bad-meta"),
                PlacementGroupId(1),
                NodeId(0)
            )
            .is_err()
        );
        assert_eq!(fs::read(path.join("bad-meta/META")).unwrap(), metadata);
    }
}
#[test]
fn membership_staging_child() {
    let Some(path) = std::env::var_os("RSF_MEMBERSHIP_STAGING_CHILD") else {
        return;
    };
    let path = PathBuf::from(path);
    let mut stage =
        MembershipStagingStore::open_existing(path.join("stage-3"), PlacementGroupId(1), NodeId(3))
            .unwrap();
    stage
        .materialize_with_hook(&mut |entry| write_application(&path, entry), || {
            fs::write(path.join("receipt-durable"), b"before reply").unwrap();
            loop {
                std::thread::park();
            }
        })
        .unwrap();
}
fn write_application(path: &Path, entry: &VsrCommittedEntry) -> RsfResult<()> {
    let directory = path.join("application");
    rsf_storage::create_dir_all_synced(&directory).map_err(io_error)?;
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(directory.join(entry.prepare.index.0.to_string()))
        .map_err(io_error)?;
    file.write_all(&entry.prepare.encode()?)
        .and_then(|()| file.sync_all())
        .map_err(io_error)?;
    File::open(directory)
        .and_then(|f| f.sync_all())
        .map_err(io_error)?;
    File::open(path)
        .and_then(|f| f.sync_all())
        .map_err(io_error)
}
#[test]
fn killed_membership_stage_recovers_receipt_and_replays_materialization_before_report() {
    let path = root("stage-kill-before-report");
    let f = fixture(&path);
    let stage = f
        .learner
        .stage_membership(path.join("stage-3"), plan(&f, false))
        .unwrap();
    let id = stage.stage_id();
    drop(stage);
    drop(f);
    fs::remove_dir_all(path.join("old")).unwrap();
    for n in 0..4 {
        fs::remove_dir_all(path.join(format!("decision-{n}"))).unwrap();
    }
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "membership::staging::store::tests::membership_staging_child",
            "--nocapture",
        ])
        .env("RSF_MEMBERSHIP_STAGING_CHILD", &path)
        .stdout(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let until = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while !path.join("receipt-durable").exists() && std::time::Instant::now() < until {
        assert!(child.try_wait().unwrap().is_none());
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    let reached = path.join("receipt-durable").exists();
    let _ = child.kill();
    child.wait().unwrap();
    assert!(reached);
    let original =
        VsrMembershipStagedReport::decode(&fs::read(path.join("stage-3/INSTALLED")).unwrap())
            .unwrap();
    fs::remove_dir_all(path.join("application")).unwrap();
    let mut recovered =
        MembershipStagingStore::open_existing(path.join("stage-3"), PlacementGroupId(1), NodeId(3))
            .unwrap();
    assert_eq!(recovered.stage_id(), id);
    assert!(recovered.report().is_err());
    assert_eq!(
        recovered
            .materialize(&mut |entry| write_application(&path, entry))
            .unwrap(),
        original
    );
    for index in 1..=2 {
        assert!(path.join(format!("application/{index}")).is_file());
    }
    assert!(!path.join("application/3").exists());
    assert_eq!(recovered.plan().accepted_tail(), LogIndex(3));
}

#[path = "activation_tests.rs"]
mod activation_tests;

#[path = "promotion_tests.rs"]
mod promotion_tests;
