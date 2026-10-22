use super::*;
use crate::installation::publication::Boundary;

fn staged(path: &Path, f: &Fixture) -> (Vec<MembershipStagingStore>, VsrMembershipStagedQuorum) {
    let mut stores = (0..3)
        .map(|node| {
            MembershipStagingStore::stage(
                path.join(format!("stage-{node}")),
                NodeId(node as u64),
                plan(f, false),
                &f.cells[node],
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let reports = stores
        .iter_mut()
        .enumerate()
        .map(|(n, stage)| {
            stage
                .materialize(&mut |entry| write_application(&path.join(format!("app-{n}")), entry))
                .unwrap()
        })
        .collect();
    let quorum = VsrMembershipStagedQuorum::from_reports(reports).unwrap();
    (stores, quorum)
}
fn reopen(path: &Path, n: usize) -> DurableVsrCell {
    DurableVsrCell::open_existing(path.join(format!("old/{n}")), PlacementGroupId(1)).unwrap()
}
fn fence_primary(f: &mut Fixture) {
    f.cells[0]
        .begin_membership(NodeId(0), NodeId(0), proposal())
        .unwrap();
}
#[test]
fn membership_activation_preserves_certificates_receipts_pending_data_and_later_commits() {
    let path = root("activation-prefix");
    let mut f = fixture(&path);
    fence_primary(&mut f);
    let old_commits = f.cells[2].committed_prefix().unwrap();
    let (stages, quorum) = staged(&path, &f);
    assert_ne!(stages[0].stage_id(), stages[1].stage_id());
    for (cell, stage) in f.cells.iter_mut().zip(&stages) {
        cell.install_membership(stage, &quorum).unwrap();
        assert_eq!(cell.view(), Epoch(2));
        assert_eq!(cell.primary(), NodeId(1));
        assert!(cell.contains_replica(NodeId(3)));
        assert_eq!(cell.committed_prefix().unwrap(), old_commits);
        assert!(cell.membership_fence().is_none());
    }
    assert_eq!(
        f.cells[0].request_commit(&VsrClientRequestId::new("receiver-extra", 9)),
        Some(LogIndex(1))
    );
    assert!(matches!(
        f.cells[1].commit(NodeId(1), LogIndex(3)).unwrap(),
        VsrDecision::Rejected { .. }
    ));
    let prepared = f.cells[1].reprepare(NodeId(1), LogIndex(3)).unwrap();
    for n in [0, 2] {
        f.cells[n]
            .receive_prepare(NodeId(n as u64), NodeId(1), prepared.clone())
            .unwrap();
        f.cells[1].acknowledge(NodeId(n as u64), Epoch(2), LogIndex(3));
    }
    let VsrDecision::Committed(commit) = f.cells[1].commit(NodeId(1), LogIndex(3)).unwrap() else {
        panic!("new quorum commit")
    };
    for n in [0, 2] {
        f.cells[n]
            .receive_commit(NodeId(n as u64), NodeId(1), commit.clone())
            .unwrap();
    }
    f.cells[1]
        .remember_client_request(VsrClientRequestId::new("after-activation", 1), LogIndex(3))
        .unwrap();
    f.cells[1].install_membership(&stages[1], &quorum).unwrap();
    let expected = f.cells[1].committed_prefix().unwrap();
    drop(std::mem::take(&mut f.cells));
    let mut cell = reopen(&path, 1);
    assert_eq!(cell.committed_prefix().unwrap(), expected);
    assert_eq!(
        cell.request_commit(&VsrClientRequestId::new("after-activation", 1)),
        Some(LogIndex(3))
    );
    cell.install_membership(&stages[1], &quorum).unwrap();
    assert_eq!(cell.committed_tail(), LogIndex(3));
    assert!(matches!(
        cell.reconfigure(Epoch(3), NodeId(1), [NodeId(1)]),
        VsrDecision::Rejected { .. }
    ));
    drop(cell);
    drop(f);
    drop(stages);
    fs::remove_dir_all(path).unwrap();
}
#[test]
fn membership_activation_requires_boot_materialization_matching_quorum_and_root_fence() {
    let path = root("activation-admission");
    let mut f = fixture(&path);
    let unmaterialized = MembershipStagingStore::stage(
        path.join("unmaterialized"),
        NodeId(1),
        plan(&f, false),
        &f.cells[1],
    )
    .unwrap();
    let (stages, quorum) = staged(&path, &f);
    assert!(
        f.cells[1]
            .install_membership(&unmaterialized, &quorum)
            .is_err()
    );
    assert!(f.cells[0].install_membership(&stages[0], &quorum).is_err());
    assert!(!path.join("old/0/VSR_ACTIVE").exists());
    for kind in 0..3 {
        let mut reports = quorum.reports().to_vec();
        for r in &mut reports {
            match kind {
                0 => r.input[0] ^= 1,
                1 => r.plan[0] ^= 1,
                _ => r.committed_tail = LogIndex(1),
            }
        }
        let invalid = VsrMembershipStagedQuorum::from_reports(reports).unwrap();
        assert!(f.cells[1].install_membership(&stages[1], &invalid).is_err());
        assert!(!path.join("old/1/VSR_ACTIVE").exists());
        assert!(f.cells[1].primary_probe().is_err());
    }
    let stage_path = path.join("stage-1");
    drop(stages);
    let mut restarted =
        MembershipStagingStore::open_existing(stage_path, PlacementGroupId(1), NodeId(1)).unwrap();
    assert!(f.cells[1].install_membership(&restarted, &quorum).is_err());
    restarted
        .materialize(&mut |entry| write_application(&path.join("replayed-app"), entry))
        .unwrap();
    f.cells[1].install_membership(&restarted, &quorum).unwrap();
    drop(f);
    drop(unmaterialized);
    drop(restarted);
    fs::remove_dir_all(path).unwrap();
}
#[test]
fn membership_activation_replay_rejects_truncation_corrupt_boundaries_and_ordinary_markers() {
    let path = root("activation-replay");
    let mut f = fixture(&path);
    fence_primary(&mut f);
    let (stages, quorum) = staged(&path, &f);
    f.cells[1].install_membership(&stages[1], &quorum).unwrap();
    let records = f.cells[1].history_snapshot().unwrap().records;
    assert!(installation::recover_records(&records).is_ok());
    let marker = records.last().unwrap();
    for cut in [0, 1, marker.payload.len() / 2, marker.payload.len() - 1] {
        let mut bad = records.clone();
        bad.last_mut().unwrap().payload.truncate(cut);
        assert!(installation::recover_records(&bad).is_err());
    }
    let mut missing = records.clone();
    missing.pop();
    assert!(installation::recover_records(&missing).is_err());
    let mut wrong_epoch = records.clone();
    wrong_epoch.last_mut().unwrap().epoch = Epoch(2);
    assert!(installation::recover_records(&wrong_epoch).is_err());
    let mut wrong_count = records.clone();
    let bytes = &mut wrong_count.last_mut().unwrap().payload;
    let end = bytes.len();
    bytes[end - 8..].copy_from_slice(&0_u64.to_le_bytes());
    assert!(installation::recover_records(&wrong_count).is_err());
    let invalid_path = path.join("invalid-activation");
    let mut invalid_store = SegmentStore::open(&invalid_path).unwrap();
    for record in &wrong_count {
        invalid_store
            .append_next_wal(
                PlacementGroupId(1),
                record.epoch,
                record.kind,
                &record.payload,
            )
            .unwrap();
    }
    drop(invalid_store);
    let before = SegmentStore::recover_existing(&invalid_path)
        .unwrap()
        .recovery
        .wal_records;
    for _ in 0..2 {
        assert!(
            DurableVsrCell::open(
                &invalid_path,
                PlacementGroupId(1),
                Epoch(1),
                NodeId(0),
                [NodeId(0), NodeId(1), NodeId(2)]
            )
            .is_err()
        );
        assert_eq!(
            SegmentStore::recover_existing(&invalid_path)
                .unwrap()
                .recovery
                .wal_records,
            before
        );
    }
    let mut ordinary = records.clone();
    ordinary.last_mut().unwrap().payload = b"VSRI2".to_vec();
    assert!(installation::recover_records(&ordinary).is_err());
    drop(f);
    drop(stages);
    fs::remove_dir_all(path).unwrap();
}
#[test]
fn membership_activation_publication_boundaries_recover_old_fence_or_complete_new_authority() {
    for boundary in 0..3 {
        let path = root("activation-boundary");
        let mut f = fixture(&path);
        fence_primary(&mut f);
        let (stages, quorum) = staged(&path, &f);
        let result = f.cells[1].install_membership_with_hook(&stages[1], &quorum, &mut |at| {
            let matches = matches!(
                (boundary, at),
                (0, Boundary::Staged) | (1, Boundary::Renamed) | (2, Boundary::Published)
            );
            if matches {
                Err(corrupt("injected activation publication error"))
            } else {
                Ok(())
            }
        });
        assert!(result.is_err());
        if boundary > 0 {
            assert!(f.cells[1].primary_probe().is_err());
        }
        drop(std::mem::take(&mut f.cells));
        let mut cell = reopen(&path, 1);
        if boundary == 0 {
            assert_eq!(cell.view(), Epoch(1));
            assert!(cell.primary_probe().is_err());
        } else {
            assert_eq!(cell.view(), Epoch(2));
            assert!(cell.membership_fence().is_none());
        }
        cell.install_membership(&stages[1], &quorum).unwrap();
        assert_eq!(cell.view(), Epoch(2));
        drop(cell);
        drop(f);
        drop(stages);
        fs::remove_dir_all(path).unwrap();
    }
}
#[test]
fn copied_membership_activation_history_retains_quorum_authority_guards() {
    let path = root("activation-copied-authority");
    let mut f = fixture(&path);
    fence_primary(&mut f);
    let (stages, quorum) = staged(&path, &f);
    f.cells[1].install_membership(&stages[1], &quorum).unwrap();
    let records = f.cells[1].history_snapshot().unwrap().records;
    let archive = path.join("copied-history");
    let mut store = SegmentStore::open(&archive).unwrap();
    for r in &records {
        store
            .append_next_wal(PlacementGroupId(1), r.epoch, r.kind, &r.payload)
            .unwrap();
    }
    drop(store);
    drop(f);
    drop(stages);
    for n in 0..4 {
        if n < 3 {
            fs::remove_dir_all(path.join(format!("stage-{n}"))).unwrap();
        }
        fs::remove_dir_all(path.join(format!("decision-{n}"))).unwrap();
    }
    fs::remove_dir_all(path.join("learner")).unwrap();
    let recovered = reopen(&path, 1);
    assert_eq!(recovered.view(), Epoch(2));
    assert_eq!(recovered.committed_tail(), LogIndex(2));
    drop(recovered);
    fs::remove_file(path.join("old/1/VSR_ACTIVE")).unwrap();
    assert!(
        DurableVsrCell::open_existing(path.join("old/1"), PlacementGroupId(1)).is_err(),
        "missing pointer restored an older published root"
    );
    let mut copied = DurableVsrCell::open(
        &archive,
        PlacementGroupId(1),
        Epoch(1),
        NodeId(0),
        [NodeId(0), NodeId(1), NodeId(2)],
    )
    .unwrap();
    assert!(copied.has_installed_view());
    assert!(matches!(
        copied.reconfigure(Epoch(3), NodeId(1), [NodeId(1)]),
        VsrDecision::Rejected { .. }
    ));
    assert!(matches!(
        copied.view_change(Epoch(3), NodeId(2)),
        VsrDecision::Rejected { .. }
    ));
    drop(copied);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn membership_activation_child() {
    let Some(path) = std::env::var_os("RSF_MEMBERSHIP_ACTIVATION_CHILD") else {
        return;
    };
    let path = PathBuf::from(path);
    let boundary: u8 = std::env::var("RSF_MEMBERSHIP_ACTIVATION_BOUNDARY")
        .unwrap()
        .parse()
        .unwrap();
    let quorum =
        VsrMembershipStagedQuorum::decode(&fs::read(path.join("quorum")).unwrap()).unwrap();
    let mut stage =
        MembershipStagingStore::open_existing(path.join("stage-1"), PlacementGroupId(1), NodeId(1))
            .unwrap();
    stage
        .materialize(&mut |entry| write_application(&path.join("app-1"), entry))
        .unwrap();
    let mut cell = reopen(&path, 1);
    cell.install_membership_with_hook(&stage, &quorum, &mut |at| {
        if matches!(
            (boundary, at),
            (0, Boundary::Staged) | (1, Boundary::Renamed) | (2, Boundary::Published)
        ) {
            let ready = path.join("activation-ready");
            fs::write(&ready, b"kill before reply").unwrap();
            File::open(ready).unwrap().sync_all().unwrap();
            File::open(&path).unwrap().sync_all().unwrap();
            loop {
                std::thread::park();
            }
        }
        Ok(())
    })
    .unwrap();
}
#[test]
fn killed_membership_activation_recovers_sealed_authority_before_any_reply() {
    for boundary in 0..3 {
        let path = root("activation-killed");
        let mut f = fixture(&path);
        fence_primary(&mut f);
        let (stages, quorum) = staged(&path, &f);
        fs::write(path.join("quorum"), quorum.encode().unwrap()).unwrap();
        File::open(path.join("quorum")).unwrap().sync_all().unwrap();
        File::open(&path).unwrap().sync_all().unwrap();
        drop(f);
        drop(stages);
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "membership::staging::store::tests::activation_tests::membership_activation_child",
                "--nocapture",
            ])
            .env("RSF_MEMBERSHIP_ACTIVATION_CHILD", &path)
            .env("RSF_MEMBERSHIP_ACTIVATION_BOUNDARY", boundary.to_string())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::inherit())
            .spawn()
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        while !path.join("activation-ready").exists() {
            assert!(
                child.try_wait().unwrap().is_none(),
                "activation child exited early"
            );
            if std::time::Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("activation child did not reach boundary");
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        child.kill().unwrap();
        child.wait().unwrap();
        let mut cell = reopen(&path, 1);
        assert_eq!(cell.view(), if boundary == 0 { Epoch(1) } else { Epoch(2) });
        assert_eq!(
            cell.committed_tail(),
            if boundary == 0 {
                LogIndex(1)
            } else {
                LogIndex(2)
            }
        );
        let mut stage = MembershipStagingStore::open_existing(
            path.join("stage-1"),
            PlacementGroupId(1),
            NodeId(1),
        )
        .unwrap();
        assert!(stage.report().is_err());
        stage
            .materialize(&mut |entry| write_application(&path.join("app-1"), entry))
            .unwrap();
        assert!(
            !path.join("app-1/application/3").exists(),
            "uncommitted entry was materialized"
        );
        cell.install_membership(&stage, &quorum).unwrap();
        assert_eq!(cell.view(), Epoch(2));
        assert!(cell.contains_replica(NodeId(3)));
        assert!(cell.membership_fence().is_none());
        drop(cell);
        drop(stage);
        fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn successive_membership_activations_preserve_authority_and_fence_removed_voters() {
    let path = root("activation-successive");
    let mut f = fixture(&path);
    fence_primary(&mut f);
    let (stages, quorum) = staged(&path, &f);
    for (cell, stage) in f.cells.iter_mut().zip(&stages) {
        cell.install_membership(stage, &quorum).unwrap();
    }
    f.cells[1]
        .remember_client_request(
            VsrClientRequestId::new("between-generations", 1),
            LogIndex(1),
        )
        .unwrap();
    let proposal = VsrMembershipProposal {
        election: VsrViewChangeRequest {
            pg: PlacementGroupId(1),
            previous_view: Epoch(2),
            target_view: Epoch(3),
            candidate: NodeId(2),
            voters: vec![NodeId(0), NodeId(1), NodeId(2), NodeId(3)],
        },
        old_primary: NodeId(1),
        new_voters: vec![NodeId(1), NodeId(2), NodeId(3)],
    };
    let reports = f
        .cells
        .iter_mut()
        .enumerate()
        .map(|(n, c)| {
            c.begin_membership(NodeId(n as u64), NodeId(1), proposal.clone())
                .unwrap()
        })
        .collect();
    let approval = VsrMembershipApproval::from_snapshots(reports).unwrap();
    let input = VsrMembershipInput::from_approval(&approval).unwrap();
    let mut voters = [1, 2]
        .into_iter()
        .map(|n| {
            MembershipDecisionStore::open(
                path.join(format!("second-decision-{n}")),
                NodeId(n),
                &approval,
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let round = VsrMembershipRound {
        counter: 1,
        coordinator: NodeId(2),
    };
    let promises = voters
        .iter_mut()
        .map(|v| v.promise(NodeId(2), round).unwrap())
        .collect();
    let decision = VsrMembershipDecision::from_promises(promises, input).unwrap();
    let nomination = voters[1].nominate(&decision).unwrap();
    let accepted = voters[0].accept(NodeId(2), &decision, &nomination).unwrap();
    let chosen = VsrMembershipChosen::from_accepted(vec![accepted, nomination]).unwrap();
    let mut next = [1, 2]
        .into_iter()
        .map(|n| {
            let plan = VsrMembershipStagedPlan::select(chosen.clone(), approval.clone()).unwrap();
            MembershipStagingStore::stage(
                path.join(format!("second-stage-{n}")),
                NodeId(n as u64),
                plan,
                &f.cells[n],
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let reports = next
        .iter_mut()
        .enumerate()
        .map(|(n, s)| {
            s.materialize(&mut |entry| {
                write_application(&path.join(format!("second-app-{n}")), entry)
            })
            .unwrap()
        })
        .collect();
    let quorum = VsrMembershipStagedQuorum::from_reports(reports).unwrap();
    for (n, s) in [1, 2].into_iter().zip(&next) {
        f.cells[n].install_membership(s, &quorum).unwrap();
        assert_eq!(f.cells[n].view(), Epoch(3));
        assert!(!f.cells[n].contains_replica(NodeId(0)));
        assert_eq!(
            f.cells[n].request_commit(&VsrClientRequestId::new("between-generations", 1)),
            Some(LogIndex(1))
        );
        assert_eq!(
            f.cells[n].request_commit(&VsrClientRequestId::new("receiver-extra", 9)),
            Some(LogIndex(1))
        );
    }
    assert_eq!(f.cells[0].view(), Epoch(2));
    assert!(f.cells[0].primary_probe().is_err());
    let prepared = f.cells[2].reprepare(NodeId(2), LogIndex(3)).unwrap();
    f.cells[1]
        .receive_prepare(NodeId(1), NodeId(2), prepared)
        .unwrap();
    f.cells[2].acknowledge(NodeId(1), Epoch(3), LogIndex(3));
    let VsrDecision::Committed(commit) = f.cells[2].commit(NodeId(2), LogIndex(3)).unwrap() else {
        panic!("new smaller quorum")
    };
    f.cells[1]
        .receive_commit(NodeId(1), NodeId(2), commit)
        .unwrap();
    let request = VsrViewChangeRequest {
        pg: PlacementGroupId(1),
        previous_view: Epoch(3),
        target_view: Epoch(4),
        candidate: NodeId(1),
        voters: vec![NodeId(1), NodeId(2), NodeId(3)],
    };
    let reports = vec![
        f.cells[1]
            .begin_view_change(NodeId(1), NodeId(1), request.clone())
            .unwrap(),
        f.cells[2]
            .begin_view_change(NodeId(2), NodeId(1), request)
            .unwrap(),
    ];
    let ordinary = crate::VsrViewInstallation::select(&reports).unwrap();
    f.cells[1].install_view(&ordinary).unwrap();
    f.cells[2].install_view(&ordinary).unwrap();
    let expected = f.cells[2].committed_prefix().unwrap();
    drop(f);
    drop(stages);
    drop(next);
    drop(voters);
    let recovered = reopen(&path, 2);
    assert_eq!(recovered.view(), Epoch(4));
    assert_eq!(recovered.committed_prefix().unwrap(), expected);
    drop(recovered);
    let retired = reopen(&path, 0);
    assert!(retired.primary_probe().is_err());
    drop(retired);
    fs::remove_dir_all(path).unwrap();
}
