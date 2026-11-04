use super::*;
use rsf_vsr::LearnerHistoryStore;

fn publish_learner(source: &mut DurableVsrCell, learner: &mut LearnerHistoryStore) {
    let snapshot = source.history_snapshot().unwrap();
    let id = snapshot.descriptor.id().unwrap();
    let count = learner.begin(snapshot.descriptor).unwrap();
    for (ordinal, record) in snapshot
        .records
        .into_iter()
        .enumerate()
        .skip(count as usize)
    {
        learner.append(id, ordinal as u64, record).unwrap();
    }
    learner.finish(id).unwrap();
}

#[test]
fn configured_genesis_receive_is_exact_and_does_not_expose_an_empty_control_state() {
    let (root, initial, control, data) = fixture("receiver-genesis");
    let trusted = anchor(&initial);
    let entry = control[&NodeId(0)].committed_prefix().unwrap().remove(0);
    let mut empty = DurableVsrCell::open(
        root.join("empty"),
        initial.control_pg,
        Epoch(1),
        NodeId(0),
        (0..3).map(NodeId),
    )
    .unwrap();
    ControlState::validate_initial_replica(&trusted, &initial, &empty).unwrap();
    assert!(ControlState::recover(&trusted, &empty).is_err());
    let before = empty.recovery_state_token().unwrap();
    let mut malformed = entry.prepare.clone();
    malformed.operation = VsrOperation::ReplicatedCommand {
        operation_id: OperationId(1),
        payload: b"RSFC1".to_vec(),
    };
    malformed.bytes = 5;
    assert!(
        ControlState::preflight_received_prepare(&trusted, &initial, &empty, &malformed).is_err()
    );
    let mut wrong = trusted.clone();
    wrong.genesis_catalog = CatalogRoot([99; 32]);
    assert!(ControlState::validate_initial_replica(&wrong, &initial, &empty).is_err());
    assert_eq!(empty.recovery_state_token().unwrap(), before);
    ControlState::preflight_received_prepare(&trusted, &initial, &empty, &entry.prepare).unwrap();
    empty
        .receive_prepare(NodeId(1), NodeId(0), entry.prepare.clone())
        .unwrap();
    ControlState::validate_initial_replica(&trusted, &initial, &empty).unwrap();
    assert!(ControlState::recover(&trusted, &empty).is_err());
    ControlState::preflight_committed_extension(&trusted, &initial, &empty, &entry).unwrap();
    empty
        .receive_commit(NodeId(1), NodeId(0), entry.commit.clone())
        .unwrap();
    let recovered = ControlState::recover(&trusted, &empty).unwrap();
    assert_eq!(recovered.catalog(), &initial);
    assert_eq!(recovered.committed_tail(), LogIndex(1));
    assert!(ControlState::validate_initial_replica(&trusted, &initial, &empty).is_err());
    drop(empty);
    let empty = DurableVsrCell::open_existing(root.join("empty"), initial.control_pg).unwrap();
    assert_eq!(
        ControlState::recover(&trusted, &empty).unwrap().root(),
        initial.root().unwrap()
    );
    drop(empty);
    drop(control);
    drop(data);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn newer_certified_genesis_replaces_older_accepted_id_without_changing_configured_catalog() {
    let (root, initial, control, data) = fixture("receiver-genesis-replacement");
    let trusted = anchor(&initial);
    let mut old = DurableVsrCell::open(
        root.join("minority"),
        initial.control_pg,
        Epoch(1),
        NodeId(0),
        (0..3).map(NodeId),
    )
    .unwrap();
    let first = control[&NodeId(0)].committed_prefix().unwrap().remove(0);
    old.receive_prepare(NodeId(2), NodeId(0), first.prepare.clone())
        .unwrap();
    // The public pre-installation ordinary view transition preserves this
    // accepted tail. A quorum installation may legitimately discard it, so the
    // assertion below is essential to exercising certified replacement.
    assert!(matches!(
        old.view_change(Epoch(2), NodeId(1)),
        VsrDecision::ViewChanged { .. }
    ));
    assert_eq!(old.prepared(LogIndex(1)), Some(&first.prepare));
    let mut winners = (0..2)
        .map(|node| {
            (
                NodeId(node),
                DurableVsrCell::open(
                    root.join(format!("winner-{node}")),
                    initial.control_pg,
                    Epoch(1),
                    NodeId(0),
                    (0..3).map(NodeId),
                )
                .unwrap(),
            )
        })
        .collect::<Cells>();
    for cell in winners.values_mut() {
        assert!(matches!(
            cell.view_change(Epoch(2), NodeId(1)),
            VsrDecision::ViewChanged { .. }
        ));
    }
    let payload = ControlCommand::Genesis(initial.clone()).encode().unwrap();
    let VsrDecision::Prepared(second) = winners
        .get_mut(&NodeId(1))
        .unwrap()
        .prepare(
            NodeId(1),
            VsrOperation::ReplicatedCommand {
                operation_id: OperationId(900),
                payload: payload.clone(),
            },
            payload.len() as u32,
        )
        .unwrap()
    else {
        panic!("new Genesis rejected")
    };
    winners
        .get_mut(&NodeId(0))
        .unwrap()
        .receive_prepare(NodeId(0), NodeId(1), second.clone())
        .unwrap();
    winners
        .get_mut(&NodeId(1))
        .unwrap()
        .acknowledge(NodeId(0), second.view, second.index);
    let VsrDecision::Committed(commit) = winners
        .get_mut(&NodeId(1))
        .unwrap()
        .commit(NodeId(1), second.index)
        .unwrap()
    else {
        panic!("new Genesis commit rejected")
    };
    assert_ne!(first.prepare.operation, second.operation);
    let entry = VsrCommittedEntry {
        prepare: second,
        commit,
    };
    ControlState::preflight_committed_extension(&trusted, &initial, &old, &entry).unwrap();
    old.catch_up_from(std::slice::from_ref(&entry)).unwrap();
    assert_eq!(
        ControlState::recover(&trusted, &old).unwrap().catalog(),
        &initial
    );
    assert_eq!(
        old.committed_operation(LogIndex(1)),
        Some(&entry.prepare.operation)
    );
    drop(old);
    drop(winners);
    drop(control);
    drop(data);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn historical_control_transfer_preserves_begin_origin_and_retained_validation_under_promise() {
    let (root, initial, mut control, data) = fixture("receiver-historical");
    let trusted = anchor(&initial);
    let begin = intent(&initial, 1, 91, 1, 0, &[1, 2]);
    let prepare = prepare(
        &mut control,
        NodeId(0),
        ControlCommand::Begin(begin.clone()),
    );
    let commit = commit(&mut control, NodeId(0), prepare.index, &[]);
    let original = ControlState::recover(&trusted, &control[&NodeId(0)])
        .unwrap()
        .committed_intent(&control[&NodeId(0)], begin.id().unwrap())
        .unwrap();
    elect(&mut control, 1, 2, NodeId(1), &[NodeId(1), NodeId(2)]);
    let target = control.get_mut(&NodeId(2)).unwrap();
    assert_eq!(target.view(), Epoch(2));
    assert_eq!(target.committed_tail(), LogIndex(1));
    let entry = VsrCommittedEntry { prepare, commit };
    assert!(
        ControlState::recover(&trusted, target)
            .unwrap()
            .preflight_prepare(target, &entry.prepare)
            .is_err()
    );
    ControlState::preflight_committed_extension(&trusted, &initial, target, &entry).unwrap();
    // The v2 quorum knew only acceptance at index 2, so it sealed view 1 at
    // committed index 1. Semantic preflight does not authorize extending that
    // historical boundary through ordinary TRANSFER.
    fn stored_bytes(path: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
        let mut files = BTreeMap::new();
        for entry in std::fs::read_dir(path).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                files.extend(stored_bytes(&path));
            } else {
                files.insert(path.clone(), std::fs::read(path).unwrap());
            }
        }
        files
    }
    let target_path = cell_path(&root, initial.control_pg, NodeId(2));
    let before_bytes = stored_bytes(&target_path);
    let before = target.recovery_state_token().unwrap();
    let prefix = target.committed_prefix().unwrap();
    assert!(
        target
            .catch_up_from(std::slice::from_ref(&entry))
            .unwrap_err()
            .message
            .contains("historical view boundary")
    );
    assert_eq!(target.recovery_state_token().unwrap(), before);
    assert_eq!(target.committed_prefix().unwrap(), prefix);
    assert_eq!(stored_bytes(&target_path), before_bytes);
    // Learn the original certificate through a real mixed-view installation.
    // Node 0 retained the committed Begin; node 2 has the newer accepted-only
    // installation. This is the kernel's exact learned-commit authority path.
    elect(&mut control, 1, 3, NodeId(2), &[NodeId(0), NodeId(2)]);
    let target = control.get_mut(&NodeId(2)).unwrap();
    assert_eq!(target.view(), Epoch(3));
    assert_eq!(target.committed_tail(), LogIndex(2));
    assert_eq!(target.prepared(entry.prepare.index), Some(&entry.prepare));
    assert_eq!(
        target.catch_up_from(std::slice::from_ref(&entry)).unwrap(),
        0,
        "exact historical retry uses the learned certificate, without rewriting it"
    );
    let state = ControlState::recover(&trusted, target).unwrap();
    let recovered = state.committed_intent(target, begin.id().unwrap()).unwrap();
    assert_eq!(recovered.begin_index(), original.begin_index());
    assert_eq!(recovered.old_control(), original.old_control());
    assert_eq!(recovered.operation_digest(), original.operation_digest());
    let token = target.recovery_state_token().unwrap();
    ControlState::preflight_committed_extension(&trusted, &initial, target, &entry).unwrap();
    assert_eq!(target.recovery_state_token().unwrap(), token);
    let request = VsrViewChangeRequest {
        pg: initial.control_pg,
        previous_view: Epoch(3),
        target_view: Epoch(4),
        candidate: NodeId(2),
        voters: target.replicas(),
    };
    target
        .begin_view_change(NodeId(2), NodeId(2), request.clone())
        .unwrap();
    let state = ControlState::recover(&trusted, target).unwrap();
    state.validate_retained_history(target).unwrap();
    assert!(state.preflight_accepted_tail(target).is_err());
    assert_eq!(target.view_change_promise(), Some(&request));
    drop(control);
    drop(data);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn published_control_observation_preserves_first_begin_and_expires_with_real_learner_custody() {
    let (root, initial, mut control, data) = fixture("receiver-learner");
    let trusted = anchor(&initial);
    let begin = intent(&initial, 1, 92, 1, 0, &[1, 2]);
    let prepared = prepare(
        &mut control,
        NodeId(0),
        ControlCommand::Begin(begin.clone()),
    );
    // Accepted-only input must not create a pending committed observation.
    let mut learner = LearnerHistoryStore::open(
        root.join("learner"),
        initial.control_pg,
        NodeId(3),
        NodeId(0),
    )
    .unwrap();
    assert!(PublishedControlState::recover(&trusted, &learner).is_err());
    publish_learner(control.get_mut(&NodeId(0)).unwrap(), &mut learner);
    let observed = PublishedControlState::recover(&trusted, &learner).unwrap();
    assert!(observed.pending().is_none());
    assert!(
        observed
            .pending_intent(&learner, begin.id().unwrap())
            .is_err()
    );
    commit(
        &mut control,
        NodeId(0),
        prepared.index,
        &[NodeId(1), NodeId(2)],
    );
    publish_learner(control.get_mut(&NodeId(0)).unwrap(), &mut learner);
    assert!(observed.validate_live(&learner).is_err());
    let observed = PublishedControlState::recover(&trusted, &learner).unwrap();
    let learner_intent = observed
        .pending_intent(&learner, begin.id().unwrap())
        .unwrap();
    let voter_state = ControlState::recover(&trusted, &control[&NodeId(0)]).unwrap();
    let voter_intent = voter_state
        .committed_intent(&control[&NodeId(0)], begin.id().unwrap())
        .unwrap();
    assert_eq!(learner_intent.begin_index(), voter_intent.begin_index());
    assert_eq!(learner_intent.old_control(), voter_intent.old_control());
    assert_eq!(
        learner_intent.operation_digest(),
        voter_intent.operation_digest()
    );
    assert_eq!(observed.catalog(), voter_state.catalog());
    learner_intent.validate_live(&learner).unwrap();
    let duplicate = prepare(
        &mut control,
        NodeId(0),
        ControlCommand::Begin(begin.clone()),
    );
    commit(
        &mut control,
        NodeId(0),
        duplicate.index,
        &[NodeId(1), NodeId(2)],
    );
    publish_learner(control.get_mut(&NodeId(0)).unwrap(), &mut learner);
    assert!(learner_intent.validate_live(&learner).is_err());
    let current = PublishedControlState::recover(&trusted, &learner).unwrap();
    let current_intent = current
        .pending_intent(&learner, begin.id().unwrap())
        .unwrap();
    assert_eq!(current_intent.begin_index(), prepared.index);
    assert_eq!(
        current_intent.operation_digest(),
        voter_intent.operation_digest()
    );
    drop(learner);
    let learner = LearnerHistoryStore::open(
        root.join("learner"),
        initial.control_pg,
        NodeId(3),
        NodeId(0),
    )
    .unwrap();
    assert!(current_intent.validate_live(&learner).is_err());
    let cold = PublishedControlState::recover(&trusted, &learner).unwrap();
    assert_eq!(
        cold.pending_intent(&learner, begin.id().unwrap())
            .unwrap()
            .begin_index(),
        prepared.index
    );
    std::fs::remove_file(root.join("learner/ACTIVE")).unwrap();
    assert!(cold.validate_live(&learner).is_err());
    assert!(cold.pending_intent(&learner, begin.id().unwrap()).is_err());
    assert!(!root.join("learner/ACTIVE").exists());
    drop(learner);
    drop(control);
    drop(data);
    std::fs::remove_dir_all(root).unwrap();
}
