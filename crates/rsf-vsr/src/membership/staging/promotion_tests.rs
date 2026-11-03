use super::*;
use crate::learner::promotion::Boundary;

struct Ready {
    f: Fixture,
    stages: Vec<MembershipStagingStore>,
    quorum: VsrMembershipStagedQuorum,
}
fn ready(path: &Path) -> Ready {
    let f = fixture(path);
    let mut stages = [0, 1]
        .into_iter()
        .map(|n| {
            MembershipStagingStore::stage(
                path.join(format!("promotion-stage-{n}")),
                NodeId(n as u64),
                plan(&f, false),
                &f.cells[n],
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    stages.push(
        f.learner
            .stage_membership(path.join("promotion-stage-3"), plan(&f, false))
            .unwrap(),
    );
    let reports = stages
        .iter_mut()
        .zip([0, 1, 3])
        .map(|(s, n)| {
            s.materialize(&mut |e| write_application(&path.join(format!("promotion-app-{n}")), e))
                .unwrap()
        })
        .collect();
    let quorum = VsrMembershipStagedQuorum::from_reports(reports).unwrap();
    Ready { f, stages, quorum }
}
fn assert_prefix(cell: &DurableVsrCell) {
    assert_eq!(cell.committed_tail(), LogIndex(2));
    assert_eq!(cell.next_index(), LogIndex(4));
    assert_eq!(
        cell.request_commit(&VsrClientRequestId::new("original", 1)),
        Some(LogIndex(1))
    );
    assert_eq!(
        cell.request_commit(&VsrClientRequestId::new("second-owned", 2)),
        Some(LogIndex(2))
    );
    assert!(cell.contains_replica(NodeId(3)));
    assert_eq!(cell.promoted_replica().unwrap(), Some(NodeId(3)));
}

#[test]
fn learner_promotion_publishes_owned_new_voter_and_cold_recovers_original_receipts() {
    let path = root("promotion-owned-prefix");
    let r = ready(&path);
    let target = path.join("voter-3");
    let cell =
        r.f.learner
            .promote_membership(&target, &r.stages[2], &r.quorum)
            .unwrap();
    assert_prefix(&cell);
    assert_eq!(cell.view(), Epoch(2));
    assert_eq!(cell.primary(), NodeId(1));
    assert!(!target.join("MANIFEST").exists());
    assert_eq!(fs::read(target.join("VSR_PROMOTION")).unwrap().len(), 118);
    assert_eq!(fs::read(target.join("VSR_ACTIVE")).unwrap().len(), 140);
    assert!(DurableVsrCell::open_existing(&target, PlacementGroupId(1)).is_err());
    let expected = cell.committed_prefix().unwrap();
    drop(cell);
    drop(r);
    // The promoted voter carries its complete authority and history; restart
    // must not depend on either the original voters or learner directories.
    fs::remove_dir_all(path.join("old")).unwrap();
    fs::remove_dir_all(path.join("learner")).unwrap();
    let cell = DurableVsrCell::open_existing(&target, PlacementGroupId(1)).unwrap();
    assert_prefix(&cell);
    assert_eq!(cell.committed_prefix().unwrap(), expected);
    for entry in expected {
        assert_eq!(
            fs::read(path.join(format!(
                "promotion-app-3/application/{}",
                entry.prepare.index.0
            )))
            .unwrap(),
            entry.prepare.encode().unwrap()
        );
    }
    drop(cell);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn learner_promotion_retry_preserves_later_quorum_commit_and_ordinary_view_change() {
    let path = root("promotion-later-view");
    let mut r = ready(&path);
    let target = path.join("voter-3");
    let mut promoted =
        r.f.learner
            .promote_membership(&target, &r.stages[2], &r.quorum)
            .unwrap();
    r.f.cells[0]
        .begin_membership(NodeId(0), NodeId(0), proposal())
        .unwrap();
    for n in [0, 1] {
        r.f.cells[n]
            .install_membership(&r.stages[n], &r.quorum)
            .unwrap();
    }
    let prepared = r.f.cells[1].reprepare(NodeId(1), LogIndex(3)).unwrap();
    r.f.cells[0]
        .receive_prepare(NodeId(0), NodeId(1), prepared.clone())
        .unwrap();
    promoted
        .receive_prepare(NodeId(3), NodeId(1), prepared)
        .unwrap();
    r.f.cells[1].acknowledge(NodeId(0), Epoch(2), LogIndex(3));
    r.f.cells[1].acknowledge(NodeId(3), Epoch(2), LogIndex(3));
    let VsrDecision::Committed(commit) = r.f.cells[1].commit(NodeId(1), LogIndex(3)).unwrap()
    else {
        panic!("new quorum commit");
    };
    r.f.cells[0]
        .receive_commit(NodeId(0), NodeId(1), commit.clone())
        .unwrap();
    promoted
        .receive_commit(NodeId(3), NodeId(1), commit)
        .unwrap();
    promoted
        .remember_client_request(VsrClientRequestId::new("after-promotion", 3), LogIndex(3))
        .unwrap();
    let request = VsrViewChangeRequest {
        pg: PlacementGroupId(1),
        previous_view: Epoch(2),
        target_view: Epoch(3),
        candidate: NodeId(3),
        voters: (0..4).map(NodeId).collect(),
    };
    let snapshots = vec![
        r.f.cells[0]
            .begin_view_change(NodeId(0), NodeId(3), request.clone())
            .unwrap(),
        r.f.cells[1]
            .begin_view_change(NodeId(1), NodeId(3), request.clone())
            .unwrap(),
        promoted
            .begin_view_change(NodeId(3), NodeId(3), request)
            .unwrap(),
    ];
    let next = VsrViewInstallation::select(&snapshots).unwrap();
    promoted.install_view(&next).unwrap();
    let publication = promoted
        .membership_publication(r.stages[2].plan().id())
        .unwrap()
        .unwrap();
    assert_eq!(
        publication.input_id(),
        r.stages[2].plan().chosen().input_id()
    );
    r.stages[2].validate_published_anchor(&promoted).unwrap();
    let expected = promoted.committed_prefix().unwrap();
    drop(promoted);
    let cell =
        r.f.learner
            .promote_membership(&target, &r.stages[2], &r.quorum)
            .unwrap();
    assert_eq!(cell.view(), Epoch(3));
    assert_eq!(cell.primary(), NodeId(3));
    assert_eq!(cell.committed_prefix().unwrap(), expected);
    assert_eq!(
        cell.request_commit(&VsrClientRequestId::new("after-promotion", 3)),
        Some(LogIndex(3))
    );
    assert_eq!(cell.promoted_replica().unwrap(), Some(NodeId(3)));
    assert!(
        cell.membership_publication(r.stages[2].plan().id())
            .unwrap()
            .is_some()
    );
    r.stages[2].validate_published_anchor(&cell).unwrap();
    drop(cell);
    drop(r);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn learner_promotion_rejects_unowned_overlapping_symlink_and_wrong_reporter_targets() {
    let path = root("promotion-target-ownership");
    let r = ready(&path);
    let unowned = path.join("unowned");
    fs::create_dir_all(&unowned).unwrap();
    fs::write(unowned.join("precious"), b"leave unchanged").unwrap();
    assert!(
        r.f.learner
            .promote_membership(&unowned, &r.stages[2], &r.quorum)
            .is_err()
    );
    assert_eq!(
        fs::read(unowned.join("precious")).unwrap(),
        b"leave unchanged"
    );
    for target in [
        path.clone(),
        path.join("learner"),
        path.join("learner/child"),
    ] {
        assert!(
            r.f.learner
                .promote_membership(&target, &r.stages[2], &r.quorum)
                .is_err()
        );
    }
    assert!(!path.join("learner/child").exists());
    let nested = path.join("old/0/vsr-views").join("a".repeat(64));
    assert!(
        r.f.learner
            .promote_membership(&nested, &r.stages[2], &r.quorum)
            .is_err()
    );
    assert!(!nested.exists());
    let target = path.join("wrong-reporter");
    assert!(
        r.f.learner
            .promote_membership(&target, &r.stages[0], &r.quorum)
            .is_err()
    );
    assert!(!target.exists());
    #[cfg(unix)]
    {
        let link = path.join("linked-target");
        std::os::unix::fs::symlink(&unowned, &link).unwrap();
        assert!(
            r.f.learner
                .promote_membership(&link, &r.stages[2], &r.quorum)
                .is_err()
        );
    }
    drop(r);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn learner_promotion_missing_or_changed_owner_and_pointer_fail_without_republication() {
    for damage in [
        "missing-owner",
        "changed-owner",
        "rebound-owner",
        "foreign-plan",
        "foreign-bootstrap",
        "missing-pointer",
        "corrupt-pointer",
    ] {
        let path = root(damage);
        let r = ready(&path);
        let target = path.join("voter-3");
        drop(
            r.f.learner
                .promote_membership(&target, &r.stages[2], &r.quorum)
                .unwrap(),
        );
        match damage {
            "missing-owner" => fs::remove_file(target.join("VSR_PROMOTION")).unwrap(),
            "changed-owner" => {
                let mut bytes = fs::read(target.join("VSR_PROMOTION")).unwrap();
                bytes[14..22].copy_from_slice(&2u64.to_le_bytes());
                let sum = Digest::blake3_bytes(&bytes[..86]);
                bytes[86..].copy_from_slice(sum.as_bytes());
                fs::write(target.join("VSR_PROMOTION"), bytes).unwrap();
            }
            "rebound-owner" | "foreign-plan" | "foreign-bootstrap" => {
                let mut owner = fs::read(target.join("VSR_PROMOTION")).unwrap();
                match damage {
                    "rebound-owner" => owner[14..22].copy_from_slice(&2u64.to_le_bytes()),
                    "foreign-plan" => owner[22..54].fill(9),
                    _ => owner[54..86].fill(9),
                }
                let sum = Digest::blake3_bytes(&owner[..86]);
                owner[86..].copy_from_slice(sum.as_bytes());
                let mut pointer = fs::read(target.join("VSR_ACTIVE")).unwrap();
                pointer[76..108].copy_from_slice(Digest::blake3_bytes(&owner).as_bytes());
                let sum = Digest::blake3_bytes(&pointer[..108]);
                pointer[108..].copy_from_slice(sum.as_bytes());
                fs::write(target.join("VSR_PROMOTION"), owner).unwrap();
                fs::write(target.join("VSR_ACTIVE"), pointer).unwrap();
            }
            "missing-pointer" => fs::remove_file(target.join("VSR_ACTIVE")).unwrap(),
            _ => fs::write(target.join("VSR_ACTIVE"), b"invalid").unwrap(),
        }
        let pointer = fs::read(target.join("VSR_ACTIVE")).ok();
        let owner = fs::read(target.join("VSR_PROMOTION")).ok();
        for _ in 0..2 {
            assert!(DurableVsrCell::open_existing(&target, PlacementGroupId(1)).is_err());
            assert!(
                r.f.learner
                    .promote_membership(&target, &r.stages[2], &r.quorum)
                    .is_err()
            );
            assert_eq!(fs::read(target.join("VSR_ACTIVE")).ok(), pointer);
            assert_eq!(fs::read(target.join("VSR_PROMOTION")).ok(), owner);
        }
        drop(r);
        fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn promotion_process_child() {
    let Some(path) = std::env::var_os("RSF_PROMOTION_ROOT") else {
        return;
    };
    let path = PathBuf::from(path);
    let boundary = std::env::var("RSF_PROMOTION_BOUNDARY").unwrap();
    let learner = LearnerHistoryStore::open(
        path.join("learner"),
        PlacementGroupId(1),
        NodeId(3),
        NodeId(0),
    )
    .unwrap();
    let mut stage = MembershipStagingStore::open_existing(
        path.join("promotion-stage-3"),
        PlacementGroupId(1),
        NodeId(3),
    )
    .unwrap();
    stage
        .materialize(&mut |e| write_application(&path.join("promotion-app-3"), e))
        .unwrap();
    let quorum =
        VsrMembershipStagedQuorum::decode(&fs::read(path.join("quorum.bin")).unwrap()).unwrap();
    learner
        .promote_membership_with_hook(&path.join("voter-3"), &stage, &quorum, &mut |b| {
            let name = match b {
                Boundary::Owned => "owned",
                Boundary::Staged => "staged",
                Boundary::Renamed => "renamed",
                Boundary::Published => "published",
            };
            if name == boundary {
                println!("RSF_PROMOTION_BOUNDARY_READY={name}");
                std::io::stdout().flush().unwrap();
                loop {
                    std::thread::park();
                }
            }
            Ok(())
        })
        .unwrap();
    panic!("promotion boundary did not fire");
}

#[test]
fn killed_learner_promotion_resumes_owned_target_without_exposing_old_authority() {
    for boundary in ["owned", "staged", "renamed", "published"] {
        let path = root(&format!("promotion-kill-{boundary}"));
        let r = ready(&path);
        fs::write(path.join("quorum.bin"), r.quorum.encode().unwrap()).unwrap();
        drop(r);
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "membership::staging::store::tests::promotion_tests::promotion_process_child",
                "--nocapture",
            ])
            .env("RSF_PROMOTION_ROOT", &path)
            .env("RSF_PROMOTION_BOUNDARY", boundary)
            .stdout(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let (send, receive) = std::sync::mpsc::channel();
        let reader = std::thread::spawn(move || {
            for line in std::io::BufRead::lines(std::io::BufReader::new(stdout)) {
                let Ok(line) = line else { break };
                if line.contains(&format!("RSF_PROMOTION_BOUNDARY_READY={boundary}")) {
                    let _ = send.send(());
                    break;
                }
            }
        });
        let ready = receive.recv_timeout(std::time::Duration::from_secs(15));
        let killed = child.kill();
        let status = child.wait().unwrap();
        reader.join().unwrap();
        assert!(
            ready.is_ok(),
            "child did not reach {boundary}: {ready:?}, {status}"
        );
        killed.unwrap();
        assert!(!status.success());
        #[cfg(unix)]
        assert_eq!(
            std::os::unix::process::ExitStatusExt::signal(&status),
            Some(9)
        );
        let recovered = DurableVsrCell::open_existing(path.join("voter-3"), PlacementGroupId(1));
        if matches!(boundary, "owned" | "staged") {
            assert!(recovered.is_err());
            assert!(
                DurableVsrCell::open(
                    path.join("voter-3"),
                    PlacementGroupId(1),
                    Epoch(1),
                    NodeId(0),
                    [NodeId(0), NodeId(1), NodeId(2)]
                )
                .is_err()
            );
            assert!(!path.join("voter-3/MANIFEST").exists());
        } else {
            assert_prefix(&recovered.unwrap());
        }
        let learner = LearnerHistoryStore::open(
            path.join("learner"),
            PlacementGroupId(1),
            NodeId(3),
            NodeId(0),
        )
        .unwrap();
        assert_eq!(
            learner.promotion_pending(path.join("voter-3")).unwrap(),
            matches!(boundary, "owned" | "staged")
        );
        let mut stage = MembershipStagingStore::open_existing(
            path.join("promotion-stage-3"),
            PlacementGroupId(1),
            NodeId(3),
        )
        .unwrap();
        let quorum =
            VsrMembershipStagedQuorum::decode(&fs::read(path.join("quorum.bin")).unwrap()).unwrap();
        assert!(
            learner
                .promote_membership(path.join("voter-3"), &stage, &quorum)
                .is_err()
        );
        stage
            .materialize(&mut |e| write_application(&path.join("promotion-app-3"), e))
            .unwrap();
        let cell = learner
            .promote_membership(path.join("voter-3"), &stage, &quorum)
            .unwrap();
        assert_prefix(&cell);
        drop(cell);
        drop(stage);
        drop(learner);
        fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn learner_promotion_retry_cannot_reactivate_a_voter_removed_by_later_membership() {
    let path = root("promotion-later-removal");
    let mut r = ready(&path);
    let target = path.join("voter-3");
    let mut promoted =
        r.f.learner
            .promote_membership(&target, &r.stages[2], &r.quorum)
            .unwrap();
    r.f.cells[0]
        .begin_membership(NodeId(0), NodeId(0), proposal())
        .unwrap();
    for n in [0, 1] {
        r.f.cells[n]
            .install_membership(&r.stages[n], &r.quorum)
            .unwrap();
    }
    let proposal = VsrMembershipProposal {
        election: VsrViewChangeRequest {
            pg: PlacementGroupId(1),
            previous_view: Epoch(2),
            target_view: Epoch(3),
            candidate: NodeId(1),
            voters: (0..4).map(NodeId).collect(),
        },
        old_primary: NodeId(1),
        new_voters: (0..3).map(NodeId).collect(),
    };
    let approval = VsrMembershipApproval::from_snapshots(vec![
        r.f.cells[0]
            .begin_membership(NodeId(0), NodeId(1), proposal.clone())
            .unwrap(),
        r.f.cells[1]
            .begin_membership(NodeId(1), NodeId(1), proposal.clone())
            .unwrap(),
        promoted
            .begin_membership(NodeId(3), NodeId(1), proposal)
            .unwrap(),
    ])
    .unwrap();
    let fenced = promoted.recovery_state_token().unwrap();
    assert!(promoted.membership_fence().is_some());
    assert!(promoted.primary_probe().is_err());
    assert!(
        promoted
            .membership_publication(r.stages[2].plan().id())
            .unwrap()
            .is_some()
    );
    r.stages[2].validate_published_anchor(&promoted).unwrap();
    assert_eq!(promoted.recovery_state_token().unwrap(), fenced);
    let mut decisions = (0..2)
        .map(|n| {
            MembershipDecisionStore::open(
                path.join(format!("removal-decision-{n}")),
                NodeId(n),
                &approval,
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let round = VsrMembershipRound {
        counter: 1,
        coordinator: NodeId(1),
    };
    let promises = decisions
        .iter_mut()
        .map(|s| s.promise(NodeId(1), round).unwrap())
        .collect();
    let decision = VsrMembershipDecision::from_promises(
        promises,
        VsrMembershipInput::from_approval(&approval).unwrap(),
    )
    .unwrap();
    let nominated = decisions[1].nominate(&decision).unwrap();
    let accepted = decisions[0]
        .accept(NodeId(1), &decision, &nominated)
        .unwrap();
    let chosen = VsrMembershipChosen::from_accepted(vec![nominated, accepted]).unwrap();
    let plan = VsrMembershipStagedPlan::select(chosen, approval).unwrap();
    let mut stage =
        MembershipStagingStore::stage(path.join("removal-stage-0"), NodeId(0), plan, &r.f.cells[0])
            .unwrap();
    stage
        .materialize(&mut |e| write_application(&path.join("removal-app-0"), e))
        .unwrap();
    // A second independently owned new-voter stage provides its real receipt.
    let approval = decisions[1]
        .approval(stage.plan().chosen().input_id())
        .unwrap();
    let plan = VsrMembershipStagedPlan::select(stage.plan().chosen().clone(), approval).unwrap();
    let mut second =
        MembershipStagingStore::stage(path.join("removal-stage-1"), NodeId(1), plan, &r.f.cells[1])
            .unwrap();
    second
        .materialize(&mut |e| write_application(&path.join("removal-app-1"), e))
        .unwrap();
    let quorum = VsrMembershipStagedQuorum::from_reports(vec![
        stage.report().unwrap(),
        second.report().unwrap(),
    ])
    .unwrap();
    promoted.install_membership(&stage, &quorum).unwrap();
    assert!(!promoted.contains_replica(NodeId(3)));
    let expected = promoted.committed_prefix().unwrap();
    drop(promoted);
    assert!(
        r.f.learner
            .promote_membership(&target, &r.stages[2], &r.quorum)
            .is_err()
    );
    let recovered = DurableVsrCell::open_existing(&target, PlacementGroupId(1)).unwrap();
    assert!(!recovered.contains_replica(NodeId(3)));
    assert_eq!(recovered.view(), Epoch(3));
    assert_eq!(recovered.committed_prefix().unwrap(), expected);
    assert!(
        recovered
            .membership_publication(r.stages[2].plan().id())
            .unwrap()
            .is_some()
    );
    r.stages[2].validate_published_anchor(&recovered).unwrap();
    // Historical proof remains available, but the removed voter stays removed.
    assert!(!recovered.contains_replica(NodeId(3)));
    assert_eq!(recovered.promoted_replica().unwrap(), Some(NodeId(3)));
    drop(recovered);
    drop(second);
    drop(stage);
    drop(decisions);
    drop(r);
    fs::remove_dir_all(path).unwrap();
}
