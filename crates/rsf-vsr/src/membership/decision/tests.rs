use super::*;
use crate::membership::tests::{fixture, proposal, root};
use std::path::Path;
fn approvals(path: &Path) -> (VsrMembershipApproval, VsrMembershipApproval) {
    let mut cells = fixture(&path.join("old"));
    let snapshots = cells
        .iter_mut()
        .enumerate()
        .map(|(n, c)| {
            c.begin_membership(NodeId(n as u64), NodeId(0), proposal())
                .unwrap()
        })
        .collect::<Vec<_>>();
    (
        VsrMembershipApproval::from_snapshots(vec![snapshots[0].clone(), snapshots[1].clone()])
            .unwrap(),
        VsrMembershipApproval::from_snapshots(vec![snapshots[1].clone(), snapshots[2].clone()])
            .unwrap(),
    )
}
fn round(counter: u64, coordinator: u64) -> VsrMembershipRound {
    VsrMembershipRound {
        counter,
        coordinator: NodeId(coordinator),
    }
}
fn stores(path: &Path, approval: &VsrMembershipApproval) -> Vec<MembershipDecisionStore> {
    (0..4)
        .map(|n| {
            MembershipDecisionStore::open(path.join(format!("decision-{n}")), NodeId(n), approval)
                .unwrap()
        })
        .collect()
}
#[test]
fn membership_input_and_decision_codecs_bind_exact_quorums_and_reject_truncation() {
    let path = root("decision-codec");
    let (a, b) = approvals(&path);
    let input = VsrMembershipInput::from_approval(&a).unwrap();
    let other = VsrMembershipInput::from_approval(&b).unwrap();
    assert_ne!(input.id().unwrap(), other.id().unwrap());
    assert_eq!(input.proposal(), other.proposal());
    let mut snapshots = a.snapshots().to_vec();
    snapshots.reverse();
    let reversed = VsrMembershipApproval::from_snapshots(snapshots).unwrap();
    assert_eq!(VsrMembershipInput::from_approval(&reversed).unwrap(), input);
    let records = input.flattened_records(&a).unwrap();
    assert_eq!(
        input.validate_records(&records).unwrap().proposal(),
        a.proposal()
    );
    assert!(
        input
            .validate_records(&records[..records.len() - 1])
            .is_err()
    );
    let mut corrupt = records.clone();
    corrupt.last_mut().unwrap().payload.push(0);
    assert!(input.validate_records(&corrupt).is_err());
    let bytes = input.encode().unwrap();
    for n in 0..bytes.len() {
        assert!(VsrMembershipInput::decode(&bytes[..n]).is_err());
    }
    assert_eq!(VsrMembershipInput::decode(&bytes).unwrap(), input);
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(VsrMembershipInput::decode(&trailing).is_err());
    let mut state = stores(&path, &a);
    let promises = state
        .iter_mut()
        .take(3)
        .map(|s| s.promise(NodeId(1), round(1, 1)).unwrap())
        .collect::<Vec<_>>();
    for report in &promises {
        let bytes = report.encode().unwrap();
        assert_eq!(VsrMembershipPromise::decode(&bytes).unwrap(), *report);
        for n in 0..bytes.len() {
            assert!(VsrMembershipPromise::decode(&bytes[..n]).is_err());
        }
    }
    assert!(VsrMembershipDecision::from_promises(promises[..2].to_vec(), input.clone()).is_err());
    assert!(
        VsrMembershipDecision::from_promises(vec![promises[0].clone(); 3], input.clone()).is_err()
    );
    let decision = VsrMembershipDecision::from_promises(promises.clone(), input.clone()).unwrap();
    let encoded_decision = decision.encode().unwrap();
    assert_eq!(
        VsrMembershipDecision::decode(&encoded_decision)
            .unwrap()
            .input(),
        decision.input()
    );
    for length in 0..encoded_decision.len() {
        assert!(VsrMembershipDecision::decode(&encoded_decision[..length]).is_err());
    }
    let nomination = state[1].nominate(&decision).unwrap();
    let mut accepted = vec![nomination.clone()];
    for n in [0, 2] {
        accepted.push(state[n].accept(NodeId(1), &decision, &nomination).unwrap());
    }
    let choice = VsrMembershipChosen::from_accepted(accepted.clone()).unwrap();
    for report in &accepted {
        let bytes = report.encode().unwrap();
        assert_eq!(VsrMembershipAccepted::decode(&bytes).unwrap(), *report);
        for n in 0..bytes.len() {
            assert!(VsrMembershipAccepted::decode(&bytes[..n]).is_err());
        }
    }
    let bytes = choice.encode().unwrap();
    assert_eq!(VsrMembershipChosen::decode(&bytes).unwrap(), choice);
    for n in 0..bytes.len() {
        assert!(VsrMembershipChosen::decode(&bytes[..n]).is_err());
    }
    assert!(VsrMembershipChosen::from_accepted(accepted[..2].to_vec()).is_err());
    assert!(VsrMembershipChosen::from_accepted(vec![accepted[0].clone(); 3]).is_err());
    let mut mixed = accepted;
    mixed[0].input = other.id().unwrap();
    assert!(VsrMembershipChosen::from_accepted(mixed).is_err());
}
#[test]
fn membership_higher_round_retains_minority_nomination_and_chosen_plan_after_old_data_loss() {
    let path = root("decision-recovery");
    let (a, b) = approvals(&path);
    let input = VsrMembershipInput::from_approval(&a).unwrap();
    let other = VsrMembershipInput::from_approval(&b).unwrap();
    let mut state = stores(&path, &a);
    let promises = state
        .iter_mut()
        .take(3)
        .map(|s| s.promise(NodeId(1), round(1, 1)).unwrap())
        .collect::<Vec<_>>();
    let decision = VsrMembershipDecision::from_promises(promises.clone(), input.clone()).unwrap();
    let nomination = state[1].nominate(&decision).unwrap();
    // A coordinator cannot reuse cached promises to nominate another input.
    let stale = VsrMembershipDecision::from_promises(promises, other.clone()).unwrap();
    assert!(
        state[1]
            .begin_input(NodeId(1), round(1, 1), other.clone())
            .is_err()
    );
    assert!(state[1].nominate(&stale).is_err());
    assert!(VsrMembershipChosen::from_accepted(vec![nomination]).is_err());
    drop(state);
    std::fs::remove_dir_all(path.join("old")).unwrap();
    let mut state = (0..4)
        .map(|n| {
            MembershipDecisionStore::open_existing(
                path.join(format!("decision-{n}")),
                PlacementGroupId(1),
                NodeId(n),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let promises = [1, 2, 3]
        .into_iter()
        .map(|n| state[n].promise(NodeId(2), round(2, 2)).unwrap())
        .collect::<Vec<_>>();
    assert!(VsrMembershipDecision::from_promises(promises.clone(), other).is_err());
    let decision = VsrMembershipDecision::from_promises(promises, input.clone()).unwrap();
    let nomination = state[2].nominate(&decision).unwrap();
    let accepted = vec![
        nomination.clone(),
        state[1].accept(NodeId(2), &decision, &nomination).unwrap(),
        state[3].accept(NodeId(2), &decision, &nomination).unwrap(),
    ];
    let choice = VsrMembershipChosen::from_accepted(accepted).unwrap();
    for store in &mut state {
        store.learn_chosen(choice.clone()).unwrap();
        assert_eq!(store.chosen().unwrap().input_id(), input.id().unwrap());
    }
    drop(state);
    for n in 0..4 {
        let mut store = MembershipDecisionStore::open_existing(
            path.join(format!("decision-{n}")),
            PlacementGroupId(1),
            NodeId(n),
        )
        .unwrap();
        assert_eq!(store.chosen(), Some(&choice));
        let promise = store.promise(NodeId(3), round(3, 3)).unwrap();
        assert_eq!(promise.accepted.unwrap().1, input.id().unwrap());
        assert!(
            store
                .begin_input(
                    NodeId(3),
                    round(3, 3),
                    VsrMembershipInput::from_approval(&b).unwrap()
                )
                .is_err()
        );
        assert_eq!(
            store.approval(input.id().unwrap()).unwrap().snapshots()[0]
                .report
                .proposal,
            proposal()
        );
    }
}
#[test]
fn membership_upload_retries_are_atomic_bounded_and_fenced_by_new_rounds() {
    let path = root("decision-upload");
    let (a, b) = approvals(&path);
    let mut state = stores(&path, &a);
    let input = VsrMembershipInput::from_approval(&b).unwrap();
    let records = input.flattened_records(&b).unwrap();
    let id = input.id().unwrap();
    let r = round(1, 1);
    state[0].promise(NodeId(1), r).unwrap();
    assert!(state[0].begin_input(NodeId(2), r, input.clone()).is_err());
    assert_eq!(
        state[0].begin_input(NodeId(1), r, input.clone()).unwrap(),
        0
    );
    let batch = VsrHistoryBatch {
        ordinal: 0,
        records: records[..2].to_vec(),
    };
    assert_eq!(
        state[0]
            .append_input(NodeId(1), r, id, batch.clone())
            .unwrap(),
        2
    );
    assert_eq!(
        state[0]
            .append_input(NodeId(1), r, id, batch.clone())
            .unwrap(),
        2
    );
    let mut changed = batch;
    changed.records[1].payload.push(0);
    assert!(state[0].append_input(NodeId(1), r, id, changed).is_err());
    assert!(
        state[0]
            .append_input(
                NodeId(1),
                r,
                id,
                VsrHistoryBatch {
                    ordinal: 3,
                    records: records[3..4].to_vec()
                }
            )
            .is_err()
    );
    assert_eq!(state[0].input(id).unwrap().1, records[..2]);
    drop(state);
    let mut store = MembershipDecisionStore::open_existing(
        path.join("decision-0"),
        PlacementGroupId(1),
        NodeId(0),
    )
    .unwrap();
    assert_eq!(store.begin_input(NodeId(1), r, input.clone()).unwrap(), 2);
    assert_eq!(
        store
            .append_input(
                NodeId(1),
                r,
                id,
                VsrHistoryBatch {
                    ordinal: 1,
                    records: records[1..].to_vec()
                }
            )
            .unwrap(),
        records.len() as u64
    );
    store.approval(id).unwrap();
    store.promise(NodeId(2), round(2, 2)).unwrap();
    assert!(
        store
            .append_input(
                NodeId(1),
                r,
                id,
                VsrHistoryBatch {
                    ordinal: 0,
                    records: records[..1].to_vec()
                }
            )
            .is_err()
    );
    assert!(store.promise(NodeId(1), r).is_err());
    assert_eq!(store.input(id).unwrap().1, records);
}
#[test]
fn membership_decision_rejects_missing_proofs_wrong_roles_and_malformed_durable_state() {
    let path = root("decision-corruption");
    let (a, _) = approvals(&path);
    assert!(MembershipDecisionStore::open(path.join("invalid-node"), NodeId(99), &a).is_err());
    let mut store = MembershipDecisionStore::open(path.join("valid"), NodeId(3), &a).unwrap();
    assert!(store.promise(NodeId(1), round(0, 1)).is_err());
    assert!(store.promise(NodeId(9), round(1, 9)).is_err());
    let authority = store.authority;
    let dir = store.directory(authority);
    drop(store);
    assert!(
        MembershipDecisionStore::open_existing(path.join("valid"), PlacementGroupId(2), NodeId(3))
            .is_err()
    );
    std::fs::remove_file(dir.join("DESCRIPTOR")).unwrap();
    for _ in 0..2 {
        assert!(
            MembershipDecisionStore::open_existing(
                path.join("valid"),
                PlacementGroupId(1),
                NodeId(3)
            )
            .is_err()
        );
    }
    let mut store = MembershipDecisionStore::open(path.join("malformed"), NodeId(3), &a).unwrap();
    store
        .journal
        .append_next_wal(
            PlacementGroupId(1),
            Epoch(2),
            WalRecordKind::Membership,
            b"VSRMDS1broken",
        )
        .unwrap();
    drop(store);
    for _ in 0..2 {
        assert!(
            MembershipDecisionStore::open_existing(
                path.join("malformed"),
                PlacementGroupId(1),
                NodeId(3)
            )
            .is_err()
        );
    }
}
#[test]
fn membership_decision_child() {
    let Some(path) = std::env::var_os("RSF_MEMBERSHIP_DECISION_CHILD") else {
        return;
    };
    let path = std::path::PathBuf::from(path);
    let mut store = MembershipDecisionStore::open_existing(
        path.join("decision-1"),
        PlacementGroupId(1),
        NodeId(1),
    )
    .unwrap();
    let input = VsrMembershipInput::from_approval(&store.authority().unwrap()).unwrap();
    let reports = std::fs::read(path.join("PROMISES")).unwrap();
    let mut r = Reader::new(&reports, b"TESTPROMISES1").unwrap();
    let count = r.u64().unwrap();
    let mut promises = Vec::new();
    for _ in 0..count {
        promises.push(VsrMembershipPromise::decode(r.bytes().unwrap()).unwrap());
    }
    r.finish().unwrap();
    let decision = VsrMembershipDecision::from_promises(promises, input).unwrap();
    store
        .nominate_with_hook(&decision, || {
            std::fs::write(path.join("accepted-before-reply"), b"synced").unwrap();
            loop {
                std::thread::park();
            }
        })
        .unwrap();
}
#[test]
fn killed_membership_coordinator_recovers_synced_nomination_before_reply() {
    let path = root("decision-killed-nomination");
    let (a, b) = approvals(&path);
    let mut state = stores(&path, &a);
    let promises = state
        .iter_mut()
        .take(3)
        .map(|s| s.promise(NodeId(1), round(1, 1)).unwrap())
        .collect::<Vec<_>>();
    let mut bytes = b"TESTPROMISES1".to_vec();
    put_u64(&mut bytes, promises.len() as u64);
    for p in &promises {
        put_bytes(&mut bytes, &p.encode().unwrap()).unwrap();
    }
    std::fs::write(path.join("PROMISES"), bytes).unwrap();
    drop(state);
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "membership::decision::store::tests::membership_decision_child",
            "--nocapture",
        ])
        .env("RSF_MEMBERSHIP_DECISION_CHILD", &path)
        .stdout(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let until = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while !path.join("accepted-before-reply").exists() && std::time::Instant::now() < until {
        assert!(child.try_wait().unwrap().is_none());
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    let synced = path.join("accepted-before-reply").exists();
    let _ = child.kill();
    child.wait().unwrap();
    assert!(synced);
    let mut store = MembershipDecisionStore::open_existing(
        path.join("decision-1"),
        PlacementGroupId(1),
        NodeId(1),
    )
    .unwrap();
    let promised = store.promise(NodeId(2), round(2, 2)).unwrap();
    assert_eq!(
        promised.accepted.unwrap().1,
        VsrMembershipInput::from_approval(&a).unwrap().id().unwrap()
    );
    let mut others = Vec::new();
    for n in [2, 3] {
        let mut other = MembershipDecisionStore::open_existing(
            path.join(format!("decision-{n}")),
            PlacementGroupId(1),
            NodeId(n),
        )
        .unwrap();
        others.push(other.promise(NodeId(2), round(2, 2)).unwrap());
    }
    others.push(promised);
    assert!(
        VsrMembershipDecision::from_promises(
            others.clone(),
            VsrMembershipInput::from_approval(&b).unwrap()
        )
        .is_err()
    );
    VsrMembershipDecision::from_promises(others, VsrMembershipInput::from_approval(&a).unwrap())
        .unwrap();
}

#[test]
fn every_new_quorum_intersection_preserves_chosen_input_across_recovery_rounds() {
    let path = root("decision-quorum-intersection");
    let (a, b) = approvals(&path);
    let input = VsrMembershipInput::from_approval(&a).unwrap();
    let other = VsrMembershipInput::from_approval(&b).unwrap();
    let id = input.id().unwrap();
    let conflicting = other.id().unwrap();
    for old_mask in 0u8..16 {
        if old_mask.count_ones() < 3 {
            continue;
        }
        for new_mask in 0u8..16 {
            if new_mask.count_ones() < 3 {
                continue;
            }
            for coordinator in 0..4 {
                let mut reports = (0..4)
                    .filter(|n| new_mask & (1 << n) != 0)
                    .map(|n| VsrMembershipPromise {
                        proposal: proposal(),
                        replica: NodeId(n),
                        round: round(2, coordinator),
                        accepted: if old_mask & (1 << n) != 0 {
                            Some((round(1, 1), id))
                        } else {
                            None
                        },
                    })
                    .collect::<Vec<_>>();
                for _ in 0..reports.len() {
                    assert!(
                        VsrMembershipDecision::from_promises(reports.clone(), other.clone())
                            .is_err()
                    );
                    assert_eq!(
                        VsrMembershipDecision::from_promises(reports.clone(), input.clone())
                            .unwrap()
                            .input()
                            .id()
                            .unwrap(),
                        id
                    );
                    reports.rotate_left(1);
                }
            }
        }
    }
    // Conflicts in any accepted round reject, even below a newer accepted plan.
    let reports = vec![
        VsrMembershipPromise {
            proposal: proposal(),
            replica: NodeId(0),
            round: round(3, 2),
            accepted: Some((round(2, 1), id)),
        },
        VsrMembershipPromise {
            proposal: proposal(),
            replica: NodeId(1),
            round: round(3, 2),
            accepted: Some((round(1, 1), id)),
        },
        VsrMembershipPromise {
            proposal: proposal(),
            replica: NodeId(2),
            round: round(3, 2),
            accepted: Some((round(1, 1), conflicting)),
        },
    ];
    assert!(VsrMembershipDecision::from_promises(reports, input).is_err());
}

#[test]
fn torn_membership_input_batch_recovers_previous_prefix_and_cannot_be_accepted() {
    let path = root("decision-torn-upload");
    let (a, b) = approvals(&path);
    let mut state = stores(&path, &a);
    let r = round(1, 1);
    let promises = state
        .iter_mut()
        .take(3)
        .map(|s| s.promise(NodeId(1), r).unwrap())
        .collect::<Vec<_>>();
    let input = VsrMembershipInput::from_approval(&b).unwrap();
    let id = input.id().unwrap();
    let records = input.flattened_records(&b).unwrap();
    let decision = VsrMembershipDecision::from_promises(promises, input.clone()).unwrap();
    state[1].begin_input(NodeId(1), r, input.clone()).unwrap();
    state[1]
        .append_input(
            NodeId(1),
            r,
            id,
            VsrHistoryBatch {
                ordinal: 0,
                records: records[..1].to_vec(),
            },
        )
        .unwrap();
    state[1]
        .append_input(
            NodeId(1),
            r,
            id,
            VsrHistoryBatch {
                ordinal: 1,
                records: records[1..].to_vec(),
            },
        )
        .unwrap();
    let directory = state[1].directory(id).join("records");
    drop(state);
    let store = SegmentStore::open(&directory).unwrap();
    let segment = directory
        .join("segments")
        .join(&store.active_segment().file_name);
    drop(store);
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(segment)
        .unwrap();
    file.set_len(file.metadata().unwrap().len() - 1).unwrap();
    file.sync_all().unwrap();
    drop(file);
    let mut store = MembershipDecisionStore::open_existing(
        path.join("decision-1"),
        PlacementGroupId(1),
        NodeId(1),
    )
    .unwrap();
    assert_eq!(store.begin_input(NodeId(1), r, input).unwrap(), 1);
    assert!(store.nominate(&decision).is_err());
    assert!(store.accepted().is_none());
    store
        .append_input(
            NodeId(1),
            r,
            id,
            VsrHistoryBatch {
                ordinal: 1,
                records: records[1..].to_vec(),
            },
        )
        .unwrap();
    assert_eq!(store.nominate(&decision).unwrap().input, id);
    drop(store);
    let store = MembershipDecisionStore::open_existing(
        path.join("decision-1"),
        PlacementGroupId(1),
        NodeId(1),
    )
    .unwrap();
    assert_eq!(store.accepted(), Some((r, id)));
}

#[test]
fn membership_authority_inbox_resumes_validated_proof_without_voting_authority() {
    let path = root("authority-inbox");
    let (a, _) = approvals(&path);
    let input = VsrMembershipInput::from_approval(&a).unwrap();
    let records = input.flattened_records(&a).unwrap();
    let id = input.id().unwrap();
    let mut inbox =
        MembershipAuthorityInbox::open(path.join("inbox"), PlacementGroupId(1), NodeId(3)).unwrap();
    assert!(inbox.begin(NodeId(99), input.clone()).is_err());
    assert_eq!(inbox.begin(NodeId(1), input.clone()).unwrap(), 0);
    inbox
        .append(
            NodeId(1),
            id,
            VsrHistoryBatch {
                ordinal: 0,
                records: records[..2].to_vec(),
            },
        )
        .unwrap();
    assert!(inbox.finish(NodeId(1), id).is_err());
    drop(inbox);
    let mut inbox =
        MembershipAuthorityInbox::open(path.join("inbox"), PlacementGroupId(1), NodeId(3)).unwrap();
    assert_eq!(inbox.begin(NodeId(2), input).unwrap(), 2);
    inbox
        .append(
            NodeId(2),
            id,
            VsrHistoryBatch {
                ordinal: 1,
                records: records[1..].to_vec(),
            },
        )
        .unwrap();
    let approved = inbox.finish(NodeId(2), id).unwrap();
    assert_eq!(approved.proposal(), a.proposal());
    let cell = DurableVsrCell::open_existing(path.join("old/1"), PlacementGroupId(1)).unwrap();
    cell.validate_membership_authority(&approved).unwrap();
    assert!(cell.primary_probe().is_err());
    assert!(!cell.contains_replica(NodeId(3)));
    let mut learner = LearnerHistoryStore::open(
        path.join("learner"),
        PlacementGroupId(1),
        NodeId(3),
        NodeId(0),
    )
    .unwrap();
    assert!(learner.validate_membership_authority(&approved).is_err());
    let source = &a.snapshots()[0];
    let prior = source.records[..source.records.len() - 1].to_vec();
    let descriptor = VsrHistoryDescriptor {
        probe: source.report.voter.history.probe.clone(),
        records: prior.len() as u64,
        digest: transfer::history_digest(&prior).unwrap(),
    };
    let history_id = descriptor.id().unwrap();
    learner.begin(descriptor).unwrap();
    for (n, record) in prior.into_iter().enumerate() {
        learner.append(history_id, n as u64, record).unwrap();
    }
    learner.finish(history_id).unwrap();
    learner.validate_membership_authority(&approved).unwrap();
    let foreign = DurableVsrCell::open(
        path.join("foreign"),
        PlacementGroupId(1),
        Epoch(1),
        NodeId(1),
        [NodeId(0), NodeId(1), NodeId(2)],
    )
    .unwrap();
    assert!(foreign.validate_membership_authority(&approved).is_err());
    drop(inbox);
    assert!(
        MembershipAuthorityInbox::open(path.join("inbox"), PlacementGroupId(2), NodeId(3)).is_err()
    );
}
