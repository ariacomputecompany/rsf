//! Ordinary application HISTORY remains usable on a mixed initial-control owner.
use super::*;

fn commit_data(
    source: &mut DurableVsrCell,
    network: &mut NetworkVsrTransport,
    payload: &[u8],
) -> VsrCommittedEntry {
    let operation = VsrOperation::ReplicatedCommand {
        operation_id: OperationId(7_000 + source.next_index().0),
        payload: payload.to_vec(),
    };
    let VsrDecision::Prepared(prepare) = source
        .prepare(NodeId(2), operation, payload.len() as u32)
        .unwrap()
    else {
        panic!("actual DATA primary prepare");
    };
    assert_eq!(prepare.pg, DATA);
    assert_eq!(prepare.primary, NodeId(2));
    assert_eq!(
        network
            .acknowledge_prepare(VsrPrepareMessage {
                replica: NodeId(1),
                prepare: prepare.clone()
            })
            .unwrap(),
        NodeId(1)
    );
    assert!(matches!(
        source.acknowledge(NodeId(1), prepare.view, prepare.index),
        VsrDecision::Acknowledged { .. }
    ));
    let VsrDecision::Committed(commit) = source.commit(NodeId(2), prepare.index).unwrap() else {
        panic!("actual DATA quorum commit");
    };
    network
        .acknowledge_commit(NodeId(1), commit.clone())
        .unwrap();
    VsrCommittedEntry { prepare, commit }
}

#[test]
fn real_tls_application_history_publishes_before_and_after_initial_control_handoff() {
    let mut f = Fixture::new("initial-mixed-application");
    let (writer, one, observed) = source::source(&mut f);
    let p = policy(&f);
    let export = writer
        .export_initial_control_source(&observed, p.limits)
        .unwrap();
    let mut data = f.data.remove(&NodeId(2)).unwrap();
    let path = f.root.join("initial-control");
    let application_path = f.root.join("initial-data");
    let running = InitialRunning::start(
        receiver(&f, ControlInitialState::Reserved),
        ControlInitialState::Reserved,
        path.clone(),
    );
    let mut network = transport(&f, 2, running.address);
    network.peers.get_mut(&NodeId(1)).unwrap().address = one.address;
    let initial_control = tree(&path);
    let first = commit_data(
        &mut data,
        &mut network,
        b"application payload before initial control",
    );
    assert_eq!(first.prepare.index, LogIndex(1));
    let first_id = data.history_snapshot().unwrap().descriptor.id().unwrap();
    assert_eq!(
        network.sync_learner(NodeId(3), &mut data).unwrap(),
        LogIndex(1)
    );
    assert_eq!(
        std::fs::read(application_path.join("ACTIVE")).unwrap(),
        first_id
    );
    assert_eq!(*running.applied.lock().unwrap(), 1);
    assert_eq!(
        *running.ledger.lock().unwrap(),
        ControlInitialState::Reserved
    );
    assert!(running.events.lock().unwrap().is_empty());
    assert_eq!(tree(&path), initial_control);
    let mut server = running.finish();
    assert_eq!(
        server.learners[&DATA].published_history_id().unwrap(),
        Some(first_id)
    );
    assert_eq!(
        server.learners[&DATA].committed_entries().unwrap(),
        vec![first.clone()]
    );
    assert_eq!(
        server.committed_application_entries().unwrap(),
        vec![first.clone()]
    );
    assert!(server.learners[&CONTROL].control_observation().is_err());

    // Keep the same actual application owner while the other PG becomes a
    // published control observer. Its history must never enter Fabric callbacks.
    let application_before = tree(&application_path);
    let running = InitialRunning::start(server, ControlInitialState::Reserved, path.clone());
    let sender = transport(&f, 0, running.address);
    let selection = copy(&writer, &export, &sender, &p);
    assert!(matches!(
        sender.initial_control_exchange(NodeId(3), &p, &Request::Finish(selection)).unwrap(),
        Reply::Published { origin, current, tail: LogIndex(1) }
            if origin == selection && current == export.descriptor().id().unwrap()
    ));
    assert_eq!(*running.applied.lock().unwrap(), 0);
    assert_eq!(tree(&application_path), application_before);
    let control_before = tree(&path);
    let native_before = *running.ledger.lock().unwrap();
    let native_events = running.events.lock().unwrap().clone();
    assert_eq!(native_events.len(), 3);

    network.peers.get_mut(&NodeId(3)).unwrap().address = running.address;
    let second = commit_data(
        &mut data,
        &mut network,
        b"application payload after initial control",
    );
    assert_eq!(second.prepare.index, LogIndex(2));
    let second_id = data.history_snapshot().unwrap().descriptor.id().unwrap();
    assert_ne!(second_id, first_id);
    assert_eq!(
        network.sync_learner(NodeId(3), &mut data).unwrap(),
        LogIndex(2)
    );
    // Application materialization intentionally visits the complete prefix.
    assert_eq!(*running.applied.lock().unwrap(), 2);
    assert_eq!(
        network.sync_learner(NodeId(3), &mut data).unwrap(),
        LogIndex(2)
    );
    assert_eq!(
        *running.applied.lock().unwrap(),
        2,
        "same-ID retry must reuse materialization"
    );
    assert_eq!(tree(&path), control_before);
    assert_eq!(*running.ledger.lock().unwrap(), native_before);
    assert_eq!(*running.events.lock().unwrap(), native_events);
    assert_eq!(
        std::fs::read(application_path.join("ACTIVE")).unwrap(),
        second_id
    );
    let mut server = running.finish();
    assert!(server.cells.is_empty());
    assert_eq!(
        server.learners[&DATA].published_history_id().unwrap(),
        Some(second_id)
    );
    assert_eq!(
        server.learners[&DATA].committed_entries().unwrap(),
        vec![first.clone(), second.clone()]
    );
    assert_eq!(
        server.committed_application_entries().unwrap(),
        vec![first, second]
    );
    let control = server.learners[&CONTROL].control_observation().unwrap();
    assert_eq!(control.catalog(), &f.catalog);
    assert_eq!(control.committed_tail(), LogIndex(1));
    assert!(matches!(
        &server.learners[&DATA],
        crate::replication::learner::LearnerOwner::Application(_)
    ));
    drop(server);
    drop(data);
    drop(one.finish());
    drop(writer);
    f.cleanup();
}
