use super::*;

#[test]
fn real_tls_sender_current_primary_relays_original_historical_source_after_both_source_losses() {
    let mut f = Fixture::new("initial-historical-source");
    let one = Running::start(voter(&mut f, 1));
    let mut original = voter(&mut f, 0);
    original
        .control_transport
        .peers
        .get_mut(&NodeId(1))
        .unwrap()
        .address = one.address;
    original.request_control_genesis().unwrap();
    assert!(matches!(
        original.poll_control_writer().unwrap(),
        ControlWriterProgress::Accepted { index: LogIndex(1) }
    ));
    let prepare = original.cells[&CONTROL]
        .prepared(LogIndex(1))
        .unwrap()
        .clone();
    let mut original_transport = original.control_transport.clone();
    assert_eq!(
        original_transport
            .acknowledge_prepare(VsrPrepareMessage {
                replica: NodeId(1),
                prepare: prepare.clone()
            })
            .unwrap(),
        NodeId(1)
    );
    let mut first = one.finish();
    drop(original);
    std::fs::remove_dir_all(f.root.join("pg-0/node-0")).unwrap();
    let two = Running::start(voter(&mut f, 2));
    first
        .control_transport
        .peers
        .get_mut(&NodeId(2))
        .unwrap()
        .address = two.address;
    install_control(&mut first, 1, 2, 1);
    first.request_control_genesis().unwrap();
    let committed = delivery(&mut first);
    assert_eq!(committed.historical_primary(), NodeId(1));
    assert_eq!(committed.historical_view(), Epoch(2));
    assert_eq!(
        first.cells[&CONTROL]
            .prepared(LogIndex(1))
            .unwrap()
            .operation,
        prepare.operation
    );
    let mut second = two.finish();
    let one = Running::start(first);
    second
        .control_transport
        .peers
        .get_mut(&NodeId(1))
        .unwrap()
        .address = one.address;
    install_control(&mut second, 2, 3, 2);
    second.request_control_genesis().unwrap();
    let delivered = delivery(&mut second);
    assert_eq!(second.cells[&CONTROL].primary(), NodeId(2));
    assert_eq!(delivered.historical_primary(), NodeId(1));
    let p = policy(&f);
    let export = second
        .export_initial_control_source(&delivered, p.limits)
        .unwrap();
    assert_eq!(export.descriptor().probe.primary, NodeId(1));
    assert_eq!(export.descriptor().probe.view, Epoch(2));
    // The supported cut contains an actual ordinary commit record. No probe,
    // certificate, voter report or descriptor field is rewritten by this test.
    drop(one.finish());
    std::fs::remove_dir_all(f.root.join("pg-0/node-1")).unwrap();
    let initial = InitialRunning::start(
        receiver(&f, ControlInitialState::Reserved),
        ControlInitialState::Reserved,
        f.root.join("initial-control"),
    );
    point(&mut second, initial.address);
    second
        .request_initial_control_transfer(&delivered, NodeId(3), p.limits, None)
        .unwrap();
    let publication = complete(&mut second);
    let selection = publication.origin();
    assert_eq!(
        publication.current_history(),
        export.descriptor().id().unwrap()
    );
    assert_eq!(
        *initial.ledger.lock().unwrap(),
        ControlInitialState::Published {
            selection,
            source: NodeId(1)
        }
    );
    assert_eq!(*initial.applied.lock().unwrap(), 0);
    let server = initial.finish();
    assert_eq!(
        server.learners[&CONTROL].configured_source(),
        Some(NodeId(1))
    );
    assert_eq!(
        server.learners[&CONTROL]
            .control_observation()
            .unwrap()
            .catalog(),
        &f.catalog
    );
    drop(server);
    drop(second);
    f.cleanup();
}
