use super::*;

#[test]
fn real_tls_control_genesis_begin_commit_and_transfer_do_not_enter_fabric() {
    let mut f = Fixture::new("control-domain-tls");
    let running = Running::start(f.server.take().unwrap());
    let mut sender = f.transport(0, running.address);
    let source = f.control.get_mut(&NodeId(0)).unwrap();
    let genesis = prepare_command(source, ControlCommand::Genesis(f.catalog.clone()));
    let first = commit_tls(source, &mut sender, genesis);
    sender
        .acknowledge_commit(NodeId(1), first.commit.clone())
        .unwrap();
    let intent = f.intent(17);
    let source = f.control.get_mut(&NodeId(0)).unwrap();
    let begin = prepare_command(source, ControlCommand::Begin(intent.clone()));
    let original = commit_tls(source, &mut sender, begin);
    sender
        .transfer_committed(NodeId(1), original.clone())
        .unwrap();
    // A semantic business rejection is still a deterministic committed outcome.
    let rejected_command = prepare_command(source, ControlCommand::Genesis(f.catalog.clone()));
    commit_tls(source, &mut sender, rejected_command);
    let malformed = VsrPrepare {
        pg: CONTROL,
        view: Epoch(1),
        primary: NodeId(0),
        index: LogIndex(4),
        operation: VsrOperation::ReplicatedCommand {
            operation_id: OperationId(4),
            payload: b"fabric-command".to_vec(),
        },
        bytes: 14,
    };
    let control_before = tree(&f.root.join("pg-0/node-1"));
    rejected(
        sender.exchange_io(NodeId(1), PREPARE, &malformed.encode().unwrap()),
        "control command",
    );
    assert_eq!(tree(&f.root.join("pg-0/node-1")), control_before);
    let mut wrong_domain = original.prepare.clone();
    wrong_domain.pg = DATA;
    wrong_domain.primary = NodeId(2);
    wrong_domain.index = LogIndex(1);
    let data_before = tree(&f.root.join("pg-1/node-1"));
    rejected(
        f.transport(2, running.address).exchange_io(
            NodeId(1),
            PREPARE,
            &wrong_domain.encode().unwrap(),
        ),
        "control command cannot enter",
    );
    assert_eq!(tree(&f.root.join("pg-1/node-1")), data_before);
    assert!(running.applied.lock().unwrap().is_empty());
    // Reserving the control owner does not prevent a distinct data primary
    // from using its existing application callback and ordinary VSR route.
    let mut data_sender = f.transport(2, running.address);
    let data = f.data.get_mut(&NodeId(2)).unwrap();
    let VsrDecision::Prepared(data_prepare) = data
        .prepare(
            NodeId(2),
            VsrOperation::ReplicatedCommand {
                operation_id: OperationId(201),
                payload: b"application-data".to_vec(),
            },
            16,
        )
        .unwrap()
    else {
        panic!("data prepare rejected")
    };
    data_sender
        .acknowledge_prepare(VsrPrepareMessage {
            replica: NodeId(1),
            prepare: data_prepare.clone(),
        })
        .unwrap();
    data.acknowledge(NodeId(1), data_prepare.view, data_prepare.index);
    let VsrDecision::Committed(data_commit) = data.commit(NodeId(2), data_prepare.index).unwrap()
    else {
        panic!("data commit rejected")
    };
    data_sender
        .acknowledge_commit(NodeId(1), data_commit)
        .unwrap();
    assert_eq!(running.applied.lock().unwrap().len(), 1);
    assert_eq!(running.applied.lock().unwrap()[0].prepare, data_prepare);
    let mut server = running.finish();
    let state = ControlState::recover(&f.anchor, &server.cells[&CONTROL]).unwrap();
    assert_eq!(state.committed_tail(), LogIndex(3));
    assert_eq!(
        state
            .committed_intent(&server.cells[&CONTROL], intent.id().unwrap())
            .unwrap()
            .begin_index(),
        original.prepare.index
    );
    assert_eq!(server.cells[&CONTROL].next_index(), LogIndex(4));
    assert_eq!(server.cells[&DATA].next_index(), LogIndex(2));
    assert_eq!(
        server.committed_application_entries().unwrap()[0].prepare,
        data_prepare
    );
    assert_eq!(server.committed_entries().unwrap().len(), 4);
    drop(server);
    let cold = DurableVsrCell::open_existing(f.root.join("pg-0/node-1"), CONTROL).unwrap();
    let state = ControlState::recover(&f.anchor, &cold).unwrap();
    assert_eq!(
        state
            .committed_intent(&cold, intent.id().unwrap())
            .unwrap()
            .begin_index(),
        original.prepare.index
    );
    drop(cold);
    f.cleanup();
}
