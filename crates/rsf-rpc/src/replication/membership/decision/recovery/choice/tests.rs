use super::*;
use rsf_core::{Epoch, OperationId};
use rsf_vsr::{VsrDecision, VsrOperation, VsrViewChangeRequest};
use std::path::PathBuf;

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
        new_voters: vec![NodeId(1), NodeId(2), NodeId(3)],
    }
}
fn state(
    replica: u64,
    round: VsrMembershipRound,
    accepted: Option<(VsrMembershipRound, [u8; 32])>,
) -> VsrMembershipDecisionState {
    VsrMembershipDecisionState {
        pg: PlacementGroupId(1),
        proposal: proposal().id().unwrap(),
        replica: NodeId(replica),
        bootstrap: [4; 32],
        authority: [5; 32],
        promised: Some(round),
        accepted,
        chosen: None,
    }
}
#[test]
fn accepted_majority_reconstruction_never_mixes_rounds_or_inputs() {
    let now = Instant::now();
    let mut choice = Choice::default();
    let a = VsrMembershipRound {
        counter: 1,
        coordinator: NodeId(1),
    };
    let b = VsrMembershipRound {
        counter: 2,
        coordinator: NodeId(2),
    };
    choice.observe(state(2, a, Some((a, [8; 32]))), now);
    choice.observe(state(3, b, Some((b, [8; 32]))), now);
    assert!(choice.accepted_choice(&proposal()).unwrap().is_none());
    choice.observe(state(3, b, Some((a, [9; 32]))), now);
    assert!(choice.accepted_choice(&proposal()).unwrap().is_none());
    choice.observe(state(3, b, Some((a, [8; 32]))), now);
    let chosen = choice.accepted_choice(&proposal()).unwrap().unwrap();
    assert_eq!(chosen.round(), a);
    assert_eq!(chosen.input_id(), [8; 32]);
}
#[test]
fn relayed_or_unchanged_promises_do_not_keep_dead_coordinator_preferred() {
    let now = Instant::now();
    let grace = Duration::from_secs(1);
    let mut detector = Detector::default();
    let round = VsrMembershipRound {
        counter: 9,
        coordinator: NodeId(2),
    };
    detector.observe(&state(2, round, None), now);
    assert!(
        detector
            .wait_for(NodeId(3), &proposal(), now, grace, now)
            .unwrap(),
        "higher-rank return must respect active older coordinator"
    );
    let late = now + Duration::from_secs(2);
    detector.observe(&state(2, round, None), late);
    detector.observe(&state(1, round, None), late);
    assert!(
        !detector
            .wait_for(NodeId(3), &proposal(), now, grace, late)
            .unwrap()
    );
    detector.copied(NodeId(2), round, [7; 32], 10, late);
    assert!(
        detector
            .wait_for(NodeId(3), &proposal(), now, grace, late)
            .unwrap()
    );
    let later = late + Duration::from_secs(2);
    detector.copied(NodeId(2), round, [7; 32], 10, later);
    detector.copied(NodeId(2), round, [6; 32], 11, later);
    assert!(
        !detector
            .wait_for(NodeId(3), &proposal(), now, grace, later)
            .unwrap()
    );
    detector.copied(NodeId(1), round, [7; 32], 999, later);
    assert!(
        !detector
            .wait_for(NodeId(3), &proposal(), now, grace, later)
            .unwrap()
    );
}
#[test]
fn recovery_grace_covers_global_fair_cycles_and_checks_arithmetic() {
    let timing = PeerMembershipRecoveryTiming {
        interval: Duration::from_millis(20),
    };
    let timings = BTreeMap::from([(PlacementGroupId(1), timing), (PlacementGroupId(2), timing)]);
    assert_eq!(
        grace(Duration::from_millis(800), &timings, 2).unwrap(),
        Duration::from_millis(19440)
    );
    assert!(grace(Duration::MAX, &timings, 2).is_err());
    let mut detector = Detector::default();
    for _ in 0..31 {
        detector.contended().unwrap();
    }
    assert!(detector.contended().is_err());
    let floor = VsrMembershipRound {
        counter: u64::MAX,
        coordinator: NodeId(3),
    };
    assert!(VsrMembershipRound::after(&proposal(), NodeId(2), Some(floor)).is_err());
}

struct Fixture {
    root: PathBuf,
    local: Option<MembershipDecisionStore>,
    remote: Option<MembershipDecisionStore>,
    input: VsrMembershipInput,
    original: Option<MembershipDecisionStore>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        drop(self.local.take());
        drop(self.remote.take());
        drop(self.original.take());
        let _ = std::fs::remove_dir_all(&self.root);
    }
}
fn minority() -> Fixture {
    seed(true, false)
}
fn seed(accept_second: bool, retain_original: bool) -> Fixture {
    let root = std::env::temp_dir().join(format!(
        "rsf-choice-driver-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let mut cells = (0..3)
        .map(|n| {
            DurableVsrCell::open(
                root.join(format!("cell-{n}")),
                PlacementGroupId(1),
                Epoch(1),
                NodeId(0),
                [NodeId(0), NodeId(1), NodeId(2)],
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    for index in 0..40 {
        let payload = vec![index as u8; 4096];
        let VsrDecision::Prepared(prepare) = cells[0]
            .prepare(
                NodeId(0),
                VsrOperation::ReplicatedCommand {
                    operation_id: OperationId(index + 1),
                    payload,
                },
                4096,
            )
            .unwrap()
        else {
            panic!("prepare")
        };
        for node in [1, 2] {
            cells[node]
                .receive_prepare(NodeId(node as u64), NodeId(0), prepare.clone())
                .unwrap();
        }
    }
    let snapshots = cells
        .iter_mut()
        .enumerate()
        .map(|(n, c)| {
            c.begin_membership(NodeId(n as u64), NodeId(0), proposal())
                .unwrap()
        })
        .collect::<Vec<_>>();
    let authority = VsrMembershipApproval::from_snapshots(snapshots[1..].to_vec()).unwrap();
    let complete = VsrMembershipApproval::from_snapshots(snapshots).unwrap();
    let input = VsrMembershipInput::from_approval(&complete).unwrap();
    let records = input.flattened_records(&complete).unwrap();
    let mut stores = [1, 2, 3].map(|n| {
        MembershipDecisionStore::open(root.join(format!("decision-{n}")), NodeId(n), &authority)
            .unwrap()
    });
    let round = VsrMembershipRound {
        counter: 1,
        coordinator: NodeId(1),
    };
    let promises = stores
        .iter_mut()
        .map(|s| s.promise(NodeId(1), round).unwrap())
        .collect();
    for store in stores.iter_mut().take(if accept_second { 2 } else { 1 }) {
        store.begin_input(NodeId(1), round, input.clone()).unwrap();
        let mut at = 0;
        while at < records.len() {
            let batch = VsrHistoryBatch::pack(at as u64, &records[at..]).unwrap();
            at += batch.records.len();
            store
                .append_input(NodeId(1), round, input.id().unwrap(), batch)
                .unwrap();
        }
    }
    let decision = VsrMembershipDecision::from_promises(promises, input.clone()).unwrap();
    let nomination = stores[0].nominate(&decision).unwrap();
    if accept_second {
        stores[1].accept(NodeId(1), &decision, &nomination).unwrap();
    }
    let [coordinator, remote, local] = stores;
    let original = if retain_original {
        Some(coordinator)
    } else {
        drop(coordinator);
        None
    };
    drop(cells);
    if !retain_original {
        std::fs::remove_dir_all(root.join("decision-1")).unwrap();
    }
    for n in [0, 1] {
        std::fs::remove_dir_all(root.join(format!("cell-{n}"))).unwrap();
    }
    Fixture {
        root,
        local: Some(local),
        remote: Some(remote),
        input,
        original,
    }
}
fn perform(operation: Operation, remote: &mut MembershipDecisionStore) -> Response {
    perform_as(operation, remote, NodeId(3))
}
fn perform_as(
    operation: Operation,
    remote: &mut MembershipDecisionStore,
    sender: NodeId,
) -> Response {
    match operation {
        Operation::State => Response::State(remote.decision_state().unwrap()),
        Operation::Promise(round) => Response::Promise(remote.try_promise(sender, round).unwrap()),
        Operation::Source(id) => Response::Source(remote.input_progress(id).unwrap()),
        Operation::Read { input, ordinal, .. } => {
            let (_, records) = remote.input(input.id().unwrap()).unwrap();
            Response::Records(VsrHistoryBatch::pack(ordinal, &records[ordinal as usize..]).unwrap())
        }
        Operation::RoundBegin { round, input } => {
            Response::Offset(remote.begin_input(sender, round, (*input).clone()).unwrap())
        }
        Operation::RoundBatch {
            round,
            input,
            batch,
        } => Response::Offset(
            remote
                .append_input(sender, round, input.id().unwrap(), batch)
                .unwrap(),
        ),
        Operation::Accept {
            decision,
            nomination,
        } => Response::Accepted(remote.accept(sender, &decision, &nomination).unwrap()),
        _ => panic!("unexpected driver operation"),
    }
}
#[test]
fn durable_driver_preserves_minority_b_across_partial_copy_restart() {
    let mut fixture = minority();
    let start = Instant::now();
    let mut choice = Choice::default();
    choice.inspected.extend([NodeId(1), NodeId(2)]);
    choice.observe(
        fixture.remote.as_ref().unwrap().decision_state().unwrap(),
        start,
    );
    let mut restarted = false;
    let mut frozen = None;
    let mut batches = 0;
    for _ in 0..100 {
        let local = fixture.local.as_mut().unwrap();
        choice
            .local_step(local, NodeId(3), start, Duration::ZERO, Instant::now())
            .unwrap();
        if local.chosen().is_some() {
            break;
        }
        let operation = choice.operation(local, NodeId(2)).unwrap();
        let request = Request {
            context: context(local).unwrap(),
            scope: choice.scope(),
            operation,
        };
        let kind = request.kind().unwrap();
        let scope = request.scope;
        let response = perform(request.operation, fixture.remote.as_mut().unwrap());
        choice
            .finish(local, NodeId(2), scope, kind, Ok(response), Instant::now())
            .unwrap();
        if matches!(kind, Kind::Read { .. }) {
            batches += 1;
            if !restarted {
                let before = local.input_progress(fixture.input.id().unwrap()).unwrap();
                assert!(
                    matches!(before,VsrMembershipInputProgress::Partial { records,.. } if records>0)
                );
                drop(fixture.local.take());
                fixture.local = Some(
                    MembershipDecisionStore::open_existing(
                        fixture.root.join("decision-3"),
                        PlacementGroupId(1),
                        NodeId(3),
                    )
                    .unwrap(),
                );
                choice = Choice::default();
                choice.inspected.extend([NodeId(1), NodeId(2)]);
                choice.observe(
                    fixture.remote.as_ref().unwrap().decision_state().unwrap(),
                    Instant::now(),
                );
                restarted = true;
            }
        }
        if let Some(ballot) = &choice.ballot
            && let Some(proof) = &ballot.frozen
        {
            let bytes = proof
                .iter()
                .map(|p| p.encode().unwrap())
                .collect::<Vec<_>>();
            if let Some(prior) = &frozen {
                assert_eq!(prior, &bytes);
            } else {
                frozen = Some(bytes);
            }
        }
    }
    assert!(restarted);
    assert!(batches > 1);
    let store = fixture.local.as_ref().unwrap();
    let chosen = store.chosen().unwrap();
    assert_eq!(chosen.input_id(), fixture.input.id().unwrap());
    assert_ne!(store.decision_state().unwrap().authority, chosen.input_id());
    assert_eq!(chosen.round().coordinator, NodeId(3));
    assert!(
        fixture.remote.as_ref().unwrap().chosen().is_none(),
        "remote certificate need not survive to reconstruct the accepted majority"
    );
    let reopened = chosen.clone();
    drop(fixture.local.take());
    let cold = MembershipDecisionStore::open_existing(
        fixture.root.join("decision-3"),
        PlacementGroupId(1),
        NodeId(3),
    )
    .unwrap();
    assert_eq!(cold.chosen(), Some(&reopened));
}

#[test]
fn delayed_older_state_never_downgrades_or_renews_newer_direct_activity() {
    let now = Instant::now();
    let grace = Duration::from_secs(1);
    let mut detector = Detector::default();
    let newer = VsrMembershipRound {
        counter: 3,
        coordinator: NodeId(2),
    };
    let older = VsrMembershipRound {
        counter: 2,
        coordinator: NodeId(2),
    };
    detector.copied(NodeId(2), newer, [7; 32], 8, now);
    let late = now + Duration::from_secs(2);
    detector.observe(&state(2, older, Some((older, [8; 32]))), late);
    assert!(
        !detector
            .wait_for(NodeId(3), &proposal(), now, grace, late)
            .unwrap()
    );
    detector.copied(NodeId(2), older, [8; 32], 999, late);
    assert!(
        !detector
            .wait_for(NodeId(3), &proposal(), now, grace, late)
            .unwrap()
    );
    detector.copied(NodeId(2), newer, [7; 32], 9, late);
    assert!(
        detector
            .wait_for(NodeId(3), &proposal(), now, grace, late)
            .unwrap()
    );
}

#[test]
fn another_survivor_restarts_phase_one_after_unaccepted_selected_source_dies() {
    let mut f = seed(false, true);
    let now = Instant::now();
    let grace = Duration::from_secs(1);
    // Only source1 nominated B. Healthy survivors2+3 have A and no accepted B.
    for store in [f.local.as_ref().unwrap(), f.remote.as_ref().unwrap()] {
        assert_eq!(
            store.input_progress(f.input.id().unwrap()).unwrap(),
            VsrMembershipInputProgress::Missing
        );
    }
    let mut third = Choice::default();
    third.inspected.extend([NodeId(1), NodeId(2)]);
    third.observe(f.original.as_ref().unwrap().decision_state().unwrap(), now);
    third
        .local_step(
            f.local.as_mut().unwrap(),
            NodeId(3),
            now,
            Duration::ZERO,
            now,
        )
        .unwrap();
    let operation = third
        .operation(f.local.as_ref().unwrap(), NodeId(1))
        .unwrap();
    assert!(matches!(operation, Operation::Promise(_)));
    let request = Request {
        context: context(f.local.as_ref().unwrap()).unwrap(),
        scope: third.scope(),
        operation,
    };
    let kind = request.kind().unwrap();
    let response = perform(request.operation, f.original.as_mut().unwrap());
    third
        .finish(
            f.local.as_mut().unwrap(),
            NodeId(1),
            request.scope,
            kind,
            Ok(response),
            now,
        )
        .unwrap();
    third
        .local_step(
            f.local.as_mut().unwrap(),
            NodeId(3),
            now,
            Duration::ZERO,
            now,
        )
        .unwrap();
    assert_eq!(
        third.ballot.as_ref().unwrap().selected,
        Some(f.input.id().unwrap())
    );
    assert!(f.local.as_ref().unwrap().accepted().is_none());
    drop(f.original.take());
    std::fs::remove_dir_all(f.root.join("decision-1")).unwrap();
    let frozen = third.ballot.as_ref().unwrap().round;
    let mut second = Choice::default();
    second.inspected.extend([NodeId(1), NodeId(3)]);
    second.observe(f.local.as_ref().unwrap().decision_state().unwrap(), now);
    // The active higher-rank coordinator is respected until its direct facts
    // stop changing. No selected B is discarded within that old round.
    second
        .local_step(f.remote.as_mut().unwrap(), NodeId(2), now, grace, now)
        .unwrap();
    assert!(second.ballot.is_none());
    let late = now + Duration::from_secs(2);
    second
        .local_step(f.remote.as_mut().unwrap(), NodeId(2), now, grace, late)
        .unwrap();
    assert!(second.ballot.as_ref().unwrap().round > frozen);
    for _ in 0..30 {
        let local = f.remote.as_mut().unwrap();
        second
            .local_step(local, NodeId(2), now, grace, late)
            .unwrap();
        if local.chosen().is_some() {
            break;
        }
        let operation = second.operation(local, NodeId(3)).unwrap();
        let request = Request {
            context: context(local).unwrap(),
            scope: second.scope(),
            operation,
        };
        let kind = request.kind().unwrap();
        let response = perform_as(request.operation, f.local.as_mut().unwrap(), NodeId(2));
        second
            .finish(local, NodeId(3), request.scope, kind, Ok(response), late)
            .unwrap();
        if kind == Kind::Promise {
            let round = f
                .local
                .as_ref()
                .unwrap()
                .decision_state()
                .unwrap()
                .promised
                .unwrap();
            third.detector.coordinator(NodeId(2), round, late);
            third
                .local_step(f.local.as_mut().unwrap(), NodeId(3), now, grace, late)
                .unwrap();
            assert!(
                third.ballot.is_none(),
                "superseded frozen ballot must yield to directly active successor"
            );
        }
    }
    let chosen = f.remote.as_ref().unwrap().chosen().unwrap();
    assert_ne!(chosen.input_id(), f.input.id().unwrap());
    assert_eq!(
        chosen.input_id(),
        f.remote
            .as_ref()
            .unwrap()
            .decision_state()
            .unwrap()
            .authority
    );
    assert!(chosen.round() > frozen);
}

#[test]
fn additional_copy_source_is_promised_without_replacing_frozen_phase_one_proof() {
    let mut f = seed(true, true);
    let now = Instant::now();
    let mut choice = Choice::default();
    choice.inspected.extend([NodeId(1), NodeId(2)]);
    choice.observe(f.remote.as_ref().unwrap().decision_state().unwrap(), now);
    let local = f.local.as_mut().unwrap();
    choice
        .local_step(local, NodeId(3), now, Duration::ZERO, now)
        .unwrap();
    let operation = choice.operation(local, NodeId(2)).unwrap();
    let request = Request {
        context: context(local).unwrap(),
        scope: choice.scope(),
        operation,
    };
    let kind = request.kind().unwrap();
    let reply = perform(request.operation, f.remote.as_mut().unwrap());
    choice
        .finish(local, NodeId(2), request.scope, kind, Ok(reply), now)
        .unwrap();
    choice
        .local_step(local, NodeId(3), now, Duration::ZERO, now)
        .unwrap();
    let frozen = choice
        .ballot
        .as_ref()
        .unwrap()
        .frozen
        .as_ref()
        .unwrap()
        .clone();
    assert_eq!(
        frozen.iter().map(|p| p.replica).collect::<Vec<_>>(),
        vec![NodeId(2), NodeId(3)]
    );
    let selected = choice.ballot.as_ref().unwrap().selected;
    // Node1 has the complete chosen input but was not in the frozen promises.
    let response = Response::Source(
        f.original
            .as_ref()
            .unwrap()
            .input_progress(f.input.id().unwrap())
            .unwrap(),
    );
    choice
        .finish(
            local,
            NodeId(1),
            choice.scope(),
            Kind::Source,
            Ok(response),
            now,
        )
        .unwrap();
    choice
        .local_step(local, NodeId(3), now, Duration::ZERO, now)
        .unwrap();
    let operation = choice.operation(local, NodeId(1)).unwrap();
    assert!(
        matches!(operation, Operation::Promise(_)),
        "source must bind exact active round before copying"
    );
    let request = Request {
        context: context(local).unwrap(),
        scope: choice.scope(),
        operation,
    };
    let kind = request.kind().unwrap();
    let reply = perform(request.operation, f.original.as_mut().unwrap());
    choice
        .finish(local, NodeId(1), request.scope, kind, Ok(reply), now)
        .unwrap();
    assert_eq!(
        choice.ballot.as_ref().unwrap().frozen.as_ref().unwrap(),
        &frozen
    );
    assert_eq!(choice.ballot.as_ref().unwrap().selected, selected);
    assert_eq!(choice.ballot.as_ref().unwrap().promises.len(), 3);
    let expected = choice.ballot.as_ref().unwrap().round;
    assert!(
        matches!(choice.operation(local,NodeId(1)).unwrap(),Operation::Read { round:Some(round),.. } if round==expected)
    );
    for _ in 0..40 {
        choice
            .local_step(local, NodeId(3), now, Duration::ZERO, now)
            .unwrap();
        if choice.ballot.as_ref().unwrap().nomination.is_some() {
            break;
        }
        let operation = choice.operation(local, NodeId(1)).unwrap();
        let request = Request {
            context: context(local).unwrap(),
            scope: choice.scope(),
            operation,
        };
        let kind = request.kind().unwrap();
        let reply = perform(request.operation, f.original.as_mut().unwrap());
        choice
            .finish(local, NodeId(1), request.scope, kind, Ok(reply), now)
            .unwrap();
    }
    let ballot = choice.ballot.as_ref().unwrap();
    assert_eq!(
        ballot.decision.as_ref().unwrap().promises(),
        frozen.as_slice()
    );
    assert_eq!(Some(ballot.nomination.as_ref().unwrap().input), selected);
}

#[test]
fn lost_accept_reply_then_metadata_majority_rebinds_complete_copy_as_chosen() {
    let mut f = minority();
    let now = Instant::now();
    let mut choice = Choice::default();
    choice.inspected.extend([NodeId(1), NodeId(2)]);
    choice.observe(f.remote.as_ref().unwrap().decision_state().unwrap(), now);
    let mut lost = false;
    for _ in 0..100 {
        let local = f.local.as_mut().unwrap();
        choice
            .local_step(local, NodeId(3), now, Duration::ZERO, now)
            .unwrap();
        let operation = choice.operation(local, NodeId(2)).unwrap();
        let request = Request {
            context: context(local).unwrap(),
            scope: choice.scope(),
            operation,
        };
        let kind = request.kind().unwrap();
        let reply = perform(request.operation, f.remote.as_mut().unwrap());
        if kind == Kind::Accept {
            assert!(matches!(reply, Response::Accepted(_)));
            assert!(local.chosen().is_none());
            assert!(f.remote.as_ref().unwrap().chosen().is_none());
            // The actual durable accept happened; its reply is lost. The
            // scheduler's next authenticated STATE reveals that acceptance.
            let state = f.remote.as_ref().unwrap().decision_state().unwrap();
            choice
                .finish(
                    local,
                    NodeId(2),
                    request.scope,
                    Kind::State,
                    Ok(Response::State(state)),
                    now,
                )
                .unwrap();
            lost = true;
            break;
        }
        choice
            .finish(local, NodeId(2), request.scope, kind, Ok(reply), now)
            .unwrap();
    }
    assert!(lost);
    let local = f.local.as_mut().unwrap();
    let before = local.input(f.input.id().unwrap()).unwrap();
    let promise = local.decision_state().unwrap().promised;
    for _ in 0..4 {
        choice
            .local_step(local, NodeId(3), now, Duration::ZERO, now)
            .unwrap();
        if local.chosen().is_some() {
            break;
        }
    }
    let chosen = local.chosen().unwrap().clone();
    assert_eq!(chosen.input_id(), f.input.id().unwrap());
    assert_eq!(
        local.input(chosen.input_id()).unwrap(),
        before,
        "complete retained bytes must not be recopied"
    );
    assert_eq!(local.decision_state().unwrap().promised, promise);
    drop(f.local.take());
    let cold = MembershipDecisionStore::open_existing(
        f.root.join("decision-3"),
        PlacementGroupId(1),
        NodeId(3),
    )
    .unwrap();
    assert_eq!(cold.chosen(), Some(&chosen));
    assert_eq!(cold.decision_state().unwrap().promised, promise);
}
