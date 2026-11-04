//! Real control histories distinguish committed origins from accepted simulation.
use super::*;

fn origin(
    state: &ControlState,
    cell: &DurableVsrCell,
    begin: &MembershipIntent,
) -> (LogIndex, VsrMembershipLineage, [u8; 32]) {
    let live = state.committed_intent(cell, begin.id().unwrap()).unwrap();
    live.validate_live(cell).unwrap();
    assert_eq!(live.anchor(), &anchor(state.catalog()));
    (
        live.begin_index(),
        live.old_control().clone(),
        live.operation_digest(),
    )
}

#[test]
fn committed_origin_never_moves_on_retry_conflict_reopen_or_publication_and_expires_after_finalize()
{
    let (root, initial, mut control, mut data) = fixture("intent-origin");
    let trusted = anchor(&initial);
    let begin = intent(&initial, 1, 71, 1, 0, &[1, 2]);
    let first = prepare(
        &mut control,
        NodeId(0),
        ControlCommand::Begin(begin.clone()),
    );
    commit(
        &mut control,
        NodeId(0),
        first.index,
        &[NodeId(1), NodeId(2)],
    );
    let state = ControlState::recover(&trusted, &control[&NodeId(0)]).unwrap();
    let live = state
        .committed_intent(&control[&NodeId(0)], begin.id().unwrap())
        .unwrap();
    let expected = origin(&state, &control[&NodeId(0)], &begin);
    assert_eq!(expected.0, LogIndex(2));
    assert_eq!(expected.1, authority(&initial));
    assert!(
        live.validate_live(&control[&NodeId(1)]).is_err(),
        "same history at another cell is not the same live custody"
    );
    assert!(
        state
            .committed_intent(&control[&NodeId(0)], IntentId([90; 32]))
            .is_err()
    );

    let retry = prepare(
        &mut control,
        NodeId(0),
        ControlCommand::Begin(begin.clone()),
    );
    assert_ne!(retry.index, first.index);
    assert_ne!(
        retry.operation, first.operation,
        "fixture uses different operation IDs at different indices"
    );
    assert!(live.validate_live(&control[&NodeId(0)]).is_err());
    commit(
        &mut control,
        NodeId(0),
        retry.index,
        &[NodeId(1), NodeId(2)],
    );
    let mut conflict = begin.clone();
    conflict.groups[1].proposal.old_primary = NodeId(1);
    let conflict = prepare(
        &mut control,
        NodeId(0),
        ControlCommand::Begin(conflict.clone()),
    );
    commit(
        &mut control,
        NodeId(0),
        conflict.index,
        &[NodeId(1), NodeId(2)],
    );
    for cell in control.values() {
        let state = ControlState::recover(&trusted, cell).unwrap();
        assert_eq!(
            state.last_outcome(),
            &ControlOutcome::Rejected(Rejection::RequestConflict)
        );
        assert_eq!(origin(&state, cell, &begin), expected);
    }
    reopen(&root, initial.control_pg, &mut control);
    assert!(live.validate_live(&control[&NodeId(0)]).is_err());
    for cell in control.values() {
        assert_eq!(
            origin(
                &ControlState::recover(&trusted, cell).unwrap(),
                cell,
                &begin
            ),
            expected
        );
    }

    let data_proof = publish(&root, &mut data, &begin.groups[1].proposal);
    let control_proof = publish(&root, &mut control, &begin.groups[0].proposal);
    let state = ControlState::recover(&trusted, &control[&NodeId(1)]).unwrap();
    assert!(state.admission(&control[&NodeId(1)]).is_err());
    assert_ne!(
        control[&NodeId(1)].membership_lineage().unwrap(),
        expected.1
    );
    assert_eq!(origin(&state, &control[&NodeId(1)], &begin), expected);
    let before_finalize = state
        .committed_intent(&control[&NodeId(1)], begin.id().unwrap())
        .unwrap();
    let finalization = ControlCommand::Finalize(MembershipFinalization {
        intent: begin.id().unwrap(),
        parent: begin.parent,
        proofs: vec![control_proof, data_proof],
    });
    let final_prepare = prepare(&mut control, NodeId(1), finalization);
    commit(&mut control, NodeId(1), final_prepare.index, &[NodeId(2)]);
    assert!(before_finalize.validate_live(&control[&NodeId(1)]).is_err());
    let retry = prepare(
        &mut control,
        NodeId(1),
        ControlCommand::Begin(begin.clone()),
    );
    commit(&mut control, NodeId(1), retry.index, &[NodeId(2)]);
    reopen(&root, initial.control_pg, &mut control);
    for node in [NodeId(1), NodeId(2)] {
        let state = ControlState::recover(&trusted, &control[&node]).unwrap();
        assert!(state.pending().is_none());
        assert_eq!(state.request_begin_index(begin.request), Some(first.index));
        assert_eq!(
            state.last_outcome(),
            &ControlOutcome::IntentAccepted {
                intent: begin.id().unwrap()
            }
        );
        assert!(
            state
                .committed_intent(&control[&node], begin.id().unwrap())
                .is_err(),
            "an accepted historical retry cannot resurrect completed work"
        );
    }
    drop(control);
    drop(data);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn ordinary_reprepare_preserves_full_operation_origin_but_not_the_old_live_token() {
    let (root, initial, mut control, data) = fixture("intent-reprepare");
    let trusted = anchor(&initial);
    let begin = intent(&initial, 1, 72, 1, 0, &[1, 2]);
    let original = prepare(
        &mut control,
        NodeId(0),
        ControlCommand::Begin(begin.clone()),
    );
    commit(&mut control, NodeId(0), original.index, &[]);
    let state = ControlState::recover(&trusted, &control[&NodeId(0)]).unwrap();
    let expected = origin(&state, &control[&NodeId(0)], &begin);
    for node in [NodeId(1), NodeId(2)] {
        let state = ControlState::recover(&trusted, &control[&node]).unwrap();
        assert_eq!(
            state.preflight_accepted_tail(&control[&node]).unwrap(),
            Some(original.index)
        );
        assert_eq!(state.request_begin_index(begin.request), None);
        assert!(
            state
                .committed_intent(&control[&node], begin.id().unwrap())
                .is_err()
        );
    }
    let before = ControlState::recover(&trusted, &control[&NodeId(1)]).unwrap();
    elect(&mut control, 1, 2, NodeId(1), &[NodeId(1), NodeId(2)]);
    assert!(
        before
            .preflight_accepted_tail(&control[&NodeId(1)])
            .is_err()
    );
    let state = ControlState::recover(&trusted, &control[&NodeId(1)]).unwrap();
    assert_eq!(
        state.preflight_accepted_tail(&control[&NodeId(1)]).unwrap(),
        Some(original.index)
    );
    let reprepared = control
        .get_mut(&NodeId(1))
        .unwrap()
        .reprepare(NodeId(1), original.index)
        .unwrap();
    let follower = ControlState::recover(&trusted, &control[&NodeId(2)]).unwrap();
    assert!(
        follower
            .preflight_prepare(&control[&NodeId(2)], &original)
            .is_err(),
        "old view envelope cannot ACK as current"
    );
    assert_eq!(
        follower
            .preflight_prepare(&control[&NodeId(2)], &reprepared)
            .unwrap(),
        reprepared.bytes as usize
    );
    control
        .get_mut(&NodeId(2))
        .unwrap()
        .receive_prepare(NodeId(2), NodeId(1), reprepared.clone())
        .unwrap();
    control
        .get_mut(&NodeId(1))
        .unwrap()
        .acknowledge(NodeId(2), Epoch(2), original.index);
    commit(&mut control, NodeId(1), original.index, &[NodeId(2)]);
    reopen(&root, initial.control_pg, &mut control);
    for cell in control.values() {
        let state = ControlState::recover(&trusted, cell).unwrap();
        assert_eq!(origin(&state, cell, &begin), expected);
    }
    drop(control);
    drop(data);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn accepted_business_rejections_reconcile_in_order_without_fabricating_a_committed_intent() {
    let (root, initial, mut control, data) = fixture("intent-rejected-tail");
    let trusted = anchor(&initial);
    let mut rejected = intent(&initial, 1, 73, 1, 0, &[1, 2]);
    rejected.parent = CatalogRoot([91; 32]);
    let stale = prepare(
        &mut control,
        NodeId(0),
        ControlCommand::Begin(rejected.clone()),
    );
    let begin = intent(&initial, 1, 74, 1, 0, &[1, 2]);
    let accepted = prepare(
        &mut control,
        NodeId(0),
        ControlCommand::Begin(begin.clone()),
    );
    let retry = prepare(
        &mut control,
        NodeId(0),
        ControlCommand::Begin(begin.clone()),
    );
    let state = ControlState::recover(&trusted, &control[&NodeId(1)]).unwrap();
    let stamp = control[&NodeId(1)].recovery_state_token().unwrap();
    assert_eq!(
        state.preflight_accepted_tail(&control[&NodeId(1)]).unwrap(),
        Some(stale.index)
    );
    assert!(state.pending().is_none());
    assert_eq!(state.request_begin_index(begin.request), None);
    assert!(
        state
            .committed_intent(&control[&NodeId(1)], begin.id().unwrap())
            .is_err()
    );
    assert_eq!(
        control[&NodeId(1)].recovery_state_token().unwrap(),
        stamp,
        "preflight must be read-only"
    );
    for index in [stale.index, accepted.index, retry.index] {
        commit(&mut control, NodeId(0), index, &[NodeId(1), NodeId(2)]);
    }
    reopen(&root, initial.control_pg, &mut control);
    for cell in control.values() {
        let state = ControlState::recover(&trusted, cell).unwrap();
        assert_eq!(
            state.request_outcome(rejected.request),
            Some(&ControlOutcome::Rejected(Rejection::StaleParent))
        );
        assert_eq!(state.request_begin_index(rejected.request), None);
        assert!(
            state
                .committed_intent(cell, rejected.id().unwrap())
                .is_err()
        );
        assert_eq!(
            state
                .committed_intent(cell, begin.id().unwrap())
                .unwrap()
                .begin_index(),
            accepted.index
        );
        assert_eq!(state.preflight_accepted_tail(cell).unwrap(), None);
    }
    drop(control);
    drop(data);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn incoming_prepare_checks_exact_index_operation_domain_and_current_authority_without_writes() {
    let (root, initial, mut control, data) = fixture("intent-prepare-guard");
    let trusted = anchor(&initial);
    let begin = intent(&initial, 1, 75, 1, 0, &[1, 2]);
    let retained = prepare(
        &mut control,
        NodeId(0),
        ControlCommand::Begin(begin.clone()),
    );
    let state = ControlState::recover(&trusted, &control[&NodeId(1)]).unwrap();
    let cell = &control[&NodeId(1)];
    let stamp = cell.recovery_state_token().unwrap();
    assert_eq!(
        state.preflight_prepare(cell, &retained).unwrap(),
        retained.bytes as usize
    );
    assert!(
        state
            .preflight_command(cell, &ControlCommand::Begin(begin))
            .is_err()
    );
    let mut bad = Vec::new();
    let mut different = retained.clone();
    different.pg = PlacementGroupId(99);
    bad.push(different);
    let mut different = retained.clone();
    different.view = Epoch(2);
    bad.push(different);
    let mut different = retained.clone();
    different.primary = NodeId(2);
    bad.push(different);
    let mut different = retained.clone();
    different.index = LogIndex(retained.index.0 + 2);
    bad.push(different);
    let mut different = retained.clone();
    different.bytes += 1;
    bad.push(different);
    let mut different = retained.clone();
    let VsrOperation::ReplicatedCommand { operation_id, .. } = &mut different.operation else {
        unreachable!()
    };
    *operation_id = OperationId(99);
    bad.push(different);
    let mut different = retained.clone();
    let VsrOperation::ReplicatedCommand { payload, .. } = &mut different.operation else {
        unreachable!()
    };
    payload.push(0);
    different.bytes += 1;
    bad.push(different);
    let mut oversized = retained.clone();
    oversized.index = cell.next_index();
    oversized.operation = VsrOperation::ReplicatedCommand {
        operation_id: OperationId(100),
        payload: vec![0; MAX_COMMAND_BYTES + 1],
    };
    oversized.bytes = (MAX_COMMAND_BYTES + 1) as u32;
    bad.push(oversized);
    for different in bad {
        assert!(state.preflight_prepare(cell, &different).is_err());
    }
    let mut append = retained.clone();
    append.index = cell.next_index();
    assert!(
        state.preflight_prepare(cell, &append).is_ok(),
        "canonical duplicate request may append an idempotent result"
    );
    assert_eq!(cell.recovery_state_token().unwrap(), stamp);
    assert_eq!(cell.prepared(retained.index), Some(&retained));
    let fresh = ControlState::recover(&trusted, cell).unwrap();
    let promise = VsrViewChangeRequest {
        pg: initial.control_pg,
        previous_view: Epoch(1),
        target_view: Epoch(2),
        candidate: NodeId(1),
        voters: (0..3).map(NodeId).collect(),
    };
    control
        .get_mut(&NodeId(1))
        .unwrap()
        .begin_view_change(NodeId(1), NodeId(1), promise)
        .unwrap();
    assert!(
        fresh
            .preflight_prepare(&control[&NodeId(1)], &append)
            .is_err()
    );
    let promised = ControlState::recover(&trusted, &control[&NodeId(1)]).unwrap();
    assert!(
        promised
            .preflight_accepted_tail(&control[&NodeId(1)])
            .is_err(),
        "a recovered projection does not override an outstanding view promise"
    );
    drop(control);
    drop(data);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn malformed_or_foreign_accepted_control_commands_are_never_repaired_or_skipped() {
    for (name, operation, bytes) in [
        (
            "foreign",
            VsrOperation::CatalogMutation {
                namespace: "app".into(),
                item: "item".into(),
            },
            1,
        ),
        (
            "malformed",
            VsrOperation::ReplicatedCommand {
                operation_id: OperationId(9),
                payload: b"not a control command".to_vec(),
            },
            21,
        ),
    ] {
        let (root, initial, mut control, data) = fixture(name);
        let VsrDecision::Prepared(raw) = control
            .get_mut(&NodeId(0))
            .unwrap()
            .prepare(NodeId(0), operation, bytes)
            .unwrap()
        else {
            panic!("raw prepare")
        };
        acknowledge(&mut control, &raw);
        let trusted = anchor(&initial);
        for cell in control.values() {
            let state = ControlState::recover(&trusted, cell).unwrap();
            let before = cell.recovery_state_token().unwrap();
            assert!(state.preflight_accepted_tail(cell).is_err());
            assert!(state.preflight_prepare(cell, &raw).is_err());
            assert!(state.pending().is_none());
            assert_eq!(cell.prepared(raw.index), Some(&raw));
            assert_eq!(cell.recovery_state_token().unwrap(), before);
        }
        reopen(&root, initial.control_pg, &mut control);
        for cell in control.values() {
            let state = ControlState::recover(&trusted, cell).unwrap();
            assert!(state.preflight_accepted_tail(cell).is_err());
            assert_eq!(cell.prepared(raw.index), Some(&raw));
        }
        drop(control);
        drop(data);
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn pending_binding_survives_a_recovery_promise_only_after_fresh_owned_replay() {
    let (root, initial, mut control, data) = fixture("intent-promised");
    let trusted = anchor(&initial);
    let begin = intent(&initial, 1, 76, 1, 0, &[1, 2]);
    let prepare = prepare(
        &mut control,
        NodeId(0),
        ControlCommand::Begin(begin.clone()),
    );
    commit(
        &mut control,
        NodeId(0),
        prepare.index,
        &[NodeId(1), NodeId(2)],
    );
    let state = ControlState::recover(&trusted, &control[&NodeId(1)]).unwrap();
    let live = state
        .committed_intent(&control[&NodeId(1)], begin.id().unwrap())
        .unwrap();
    let expected = origin(&state, &control[&NodeId(1)], &begin);
    let promise = VsrViewChangeRequest {
        pg: initial.control_pg,
        previous_view: Epoch(1),
        target_view: Epoch(2),
        candidate: NodeId(1),
        voters: (0..3).map(NodeId).collect(),
    };
    control
        .get_mut(&NodeId(1))
        .unwrap()
        .begin_view_change(NodeId(1), NodeId(1), promise)
        .unwrap();
    let cell = &control[&NodeId(1)];
    assert!(live.validate_live(cell).is_err());
    assert!(state.committed_intent(cell, begin.id().unwrap()).is_err());
    let refreshed = ControlState::recover(&trusted, cell).unwrap();
    assert_eq!(origin(&refreshed, cell, &begin), expected);
    assert!(refreshed.admission(cell).is_err());
    assert!(refreshed.preflight_accepted_tail(cell).is_err());
    reopen(&root, initial.control_pg, &mut control);
    let cold = ControlState::recover(&trusted, &control[&NodeId(1)]).unwrap();
    assert_eq!(origin(&cold, &control[&NodeId(1)], &begin), expected);
    drop(control);
    drop(data);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn intent_operation_digest_distinguishes_operation_ids_at_the_same_index_and_command() {
    let mut observed = Vec::new();
    for operation_id in [OperationId(2), OperationId(99)] {
        let (root, initial, mut control, data) = fixture("intent-operation-id");
        let begin = intent(&initial, 1, 77, 1, 0, &[1, 2]);
        let payload = ControlCommand::Begin(begin.clone()).encode().unwrap();
        let bytes = payload.len() as u32;
        let VsrDecision::Prepared(prepare) = control
            .get_mut(&NodeId(0))
            .unwrap()
            .prepare(
                NodeId(0),
                VsrOperation::ReplicatedCommand {
                    operation_id,
                    payload,
                },
                bytes,
            )
            .unwrap()
        else {
            panic!("prepare")
        };
        acknowledge(&mut control, &prepare);
        commit(
            &mut control,
            NodeId(0),
            prepare.index,
            &[NodeId(1), NodeId(2)],
        );
        let state = ControlState::recover(&anchor(&initial), &control[&NodeId(1)]).unwrap();
        let bound = state
            .committed_intent(&control[&NodeId(1)], begin.id().unwrap())
            .unwrap();
        observed.push((
            bound.anchor().clone(),
            bound.id(),
            bound.begin_index(),
            bound.operation_digest(),
        ));
        drop(control);
        drop(data);
        std::fs::remove_dir_all(root).unwrap();
    }
    assert_eq!(observed[0].0, observed[1].0);
    assert_eq!(observed[0].1, observed[1].1);
    assert_eq!(observed[0].2, observed[1].2);
    assert_ne!(
        observed[0].3, observed[1].3,
        "64-bit operation identity is part of full stable-operation evidence"
    );
}

#[test]
fn canonical_outcome_distinguishes_exact_rejected_retry_from_uncommitted_request_conflict() {
    let (root, initial, mut control, data) = fixture("writer-outcome");
    let trusted = anchor(&initial);
    let mut rejected_begin = intent(&initial, 1, 81, 1, 0, &[1, 2]);
    rejected_begin.parent = CatalogRoot([91; 32]);
    let command = prepare(
        &mut control,
        NodeId(0),
        ControlCommand::Begin(rejected_begin.clone()),
    );
    commit(
        &mut control,
        NodeId(0),
        command.index,
        &[NodeId(1), NodeId(2)],
    );
    reopen(&root, initial.control_pg, &mut control);
    let state = ControlState::recover(&trusted, &control[&NodeId(0)]).unwrap();
    assert_eq!(
        state.intent_outcome(&rejected_begin).unwrap(),
        Some(ControlOutcome::Rejected(Rejection::StaleParent))
    );
    assert_eq!(state.request_begin_index(rejected_begin.request), None);
    let mut changed = rejected_begin.clone();
    changed.parent = initial.root().unwrap();
    assert!(
        state
            .intent_outcome(&changed)
            .unwrap_err()
            .message
            .contains("conflicts with its retained canonical command")
    );
    assert_eq!(control[&NodeId(0)].next_index(), LogIndex(3));
    drop(control);
    drop(data);
    std::fs::remove_dir_all(root).unwrap();
}
#[test]
fn exhausted_receipt_budget_blocks_fresh_writer_but_preserves_accepted_capacity_outcome() {
    // Pure canonical replay exercises the 4096-receipt boundary without making
    // a timing-sensitive test perform thousands of durable TLS/fsync rounds.
    let initial = catalog(2, 4);
    let mut replay = start(&initial);
    for number in 1..=MAX_RECEIPTS {
        let mut begin = intent(&initial, 1, 1, 1, 0, &[1, 2]);
        begin.request.0[..8].copy_from_slice(&(number as u64).to_le_bytes());
        begin.parent = CatalogRoot([99; 32]);
        assert_eq!(
            replay
                .apply(
                    LogIndex(number as u64 + 1),
                    &authority(&initial),
                    ControlCommand::Begin(begin)
                )
                .unwrap(),
            ControlOutcome::Rejected(Rejection::StaleParent)
        );
    }
    let fresh = ControlCommand::Begin(intent(&initial, 1, 91, 1, 0, &[1, 2]));
    assert!(
        replay
            .reserve_begin_receipt(&fresh)
            .unwrap_err()
            .message
            .contains("receipt capacity exhausted")
    );
    assert_eq!(
        replay
            .apply(
                LogIndex(MAX_RECEIPTS as u64 + 2),
                &authority(&initial),
                fresh.clone()
            )
            .unwrap(),
        ControlOutcome::Rejected(Rejection::Capacity)
    );
    assert!(
        replay.reserve_begin_receipt(&fresh).is_err(),
        "unreceipted Capacity became an append retry"
    );
}
