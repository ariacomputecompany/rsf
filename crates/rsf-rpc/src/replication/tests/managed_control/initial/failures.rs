//! Real receiver effects precede intentionally lost TLS replies or refusals.
use super::*;

#[test]
fn real_tls_initial_finished_reply_loss_cold_retries_without_republication() {
    let mut f = Fixture::new("initial-lost-finish");
    let (writer, one, observed) = source::source(&mut f);
    let p = policy(&f);
    let export = writer
        .export_initial_control_source(&observed, p.limits)
        .unwrap();
    let running = InitialRunning::start(
        receiver(&f, ControlInitialState::Reserved),
        ControlInitialState::Reserved,
        f.root.join("initial-control"),
    );
    let sender = transport(&f, 0, running.address);
    let selected = copy(&writer, &export, &sender, &p);
    let mut native = *running.ledger.lock().unwrap();
    let mut server = running.finish();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let (done, observed_effect) = std::sync::mpsc::sync_channel(1);
    let path = f.root.join("initial-control");
    let worker = std::thread::spawn(move || {
        let (socket, _) = listener.accept().unwrap();
        let mut stream = StreamOwned::new(
            ServerConnection::new(Arc::clone(&server.identity.server)).unwrap(),
            DeadlineStream::new(socket, Instant::now() + Duration::from_millis(800)).unwrap(),
        );
        while stream.conn.is_handshaking() {
            stream.conn.complete_io(&mut stream.sock).unwrap();
        }
        let certificate = stream.conn.peer_certificates().unwrap().first().unwrap();
        let sender = server.nodes[&fingerprint(certificate.as_ref())];
        let bytes = read_frame(&mut stream).unwrap();
        let (kind, payload) = parse(&bytes, &server.cluster, sender, server.local).unwrap();
        assert_eq!(kind, CONTROL_INITIAL);
        let result = server.dispatch_initial_control(
            sender,
            payload,
            Some(&mut |event| {
                assert_eq!(event.expected(), native);
                if matches!(event.next(), ControlInitialState::Publishing { .. }) {
                    assert!(!path.join("ACTIVE").exists());
                }
                if matches!(event.next(), ControlInitialState::Published { .. }) {
                    assert_eq!(
                        std::fs::read(path.join("ACTIVE")).unwrap(),
                        selected.history_id()
                    );
                }
                native = event.next();
                Ok(())
            }),
        );
        let response =
            result.expect("actual initial FINISH must succeed before losing only its reply");
        let id = crate::replication::managed_control::initial::wire::policy_id(
            server.cluster,
            server.local,
            &p,
        );
        assert!(matches!(
            crate::replication::managed_control::initial::wire::decode_reply(
                &response,
                id,
                payload,
                &Request::Finish(selected),
                p.limits
            )
            .unwrap(),
            Reply::Published { .. }
        ));
        assert_eq!(
            native,
            ControlInitialState::Published {
                selection: selected,
                source: NodeId(0)
            }
        );
        assert_eq!(
            server.learners[&CONTROL]
                .control_observation()
                .unwrap()
                .committed_tail(),
            LogIndex(1)
        );
        done.send(()).unwrap();
        // Actual handler and mandatory mock native events completed. Dropping
        // TLS here loses the network result; no fabricated success is returned.
        drop(stream);
        (server, native)
    });
    let sender = transport(&f, 0, address);
    let error = sender
        .initial_control_exchange(NodeId(3), &policy(&f), &Request::Finish(selected))
        .unwrap_err();
    observed_effect
        .recv_timeout(Duration::from_millis(800))
        .expect("fault did not follow actual FINISH success");
    assert!(
        !error.to_string().starts_with("peer rejected request:"),
        "{error}"
    );
    let (server, state) = worker.join().unwrap();
    drop(server);
    let running = InitialRunning::start(receiver(&f, state), state, f.root.join("initial-control"));
    let sender = transport(&f, 2, running.address);
    assert!(
        matches!(sender.initial_control_exchange(NodeId(3), &policy(&f), &Request::Finish(selected)).unwrap(), Reply::Published { origin, .. } if origin == selected)
    );
    assert!(
        running.events.lock().unwrap().is_empty(),
        "cold replay must not invoke another native transition"
    );
    assert_eq!(*running.applied.lock().unwrap(), 0);
    drop(running.finish());
    drop(one.finish());
    drop(writer);
    f.cleanup();
}

#[test]
fn real_tls_initial_semantic_rejection_allows_exact_generation_replacement() {
    let mut f = Fixture::new("initial-rejected-genesis");
    let (writer, one, observed) = source::source(&mut f);
    let p = policy(&f);
    let export = writer
        .export_initial_control_source(&observed, p.limits)
        .unwrap();
    // Authentic kernel history with a different committed catalog is an invalid
    // control import for this receiver. No certificate/report bytes are forged.
    let mut wrong_catalog = f.catalog.clone();
    wrong_catalog.routing.policy[0] ^= 1;
    let payload = ControlCommand::Genesis(wrong_catalog).encode().unwrap();
    let mut wrong = DurableVsrCell::open(
        f.root.join("wrong-control"),
        CONTROL,
        Epoch(1),
        NodeId(0),
        (0..3).map(NodeId),
    )
    .unwrap();
    let VsrDecision::Prepared(prepare) = wrong
        .prepare(
            NodeId(0),
            VsrOperation::ReplicatedCommand {
                operation_id: OperationId(1),
                payload: payload.clone(),
            },
            payload.len() as u32,
        )
        .unwrap()
    else {
        panic!("actual prepare");
    };
    wrong.acknowledge(NodeId(1), prepare.view, prepare.index);
    assert!(matches!(
        wrong.commit(NodeId(0), prepare.index).unwrap(),
        VsrDecision::Committed(_)
    ));
    let snapshot = wrong.history_snapshot().unwrap();
    let running = InitialRunning::start(
        receiver(&f, ControlInitialState::Reserved),
        ControlInitialState::Reserved,
        f.root.join("initial-control"),
    );
    let sender = transport(&f, 0, running.address);
    let Reply::Cursor {
        selection: rejected_selection,
        records: 0,
    } = sender
        .initial_control_exchange(NodeId(3), &p, &Request::Start(snapshot.descriptor.clone()))
        .unwrap()
    else {
        panic!("initial cursor");
    };
    let mut ordinal = 0;
    while ordinal < snapshot.descriptor.records {
        let batch =
            rsf_vsr::VsrHistoryBatch::pack(ordinal, &snapshot.records[ordinal as usize..]).unwrap();
        let Reply::Cursor { selection, records } = sender
            .initial_control_exchange(
                NodeId(3),
                &p,
                &Request::Batch {
                    selection: rejected_selection,
                    batch,
                },
            )
            .unwrap()
        else {
            panic!("retained cursor");
        };
        assert_eq!(selection, rejected_selection);
        assert!(records > ordinal);
        ordinal = records;
    }
    reject(
        sender.initial_control_exchange(NodeId(3), &p, &Request::Finish(rejected_selection)),
        "committed genesis does not match provisioned authority",
    );
    assert!(!f.root.join("initial-control/ACTIVE").exists());
    assert_eq!(
        *running.ledger.lock().unwrap(),
        ControlInitialState::Selected(rejected_selection)
    );
    assert_eq!(running.events.lock().unwrap().len(), 1);
    let Reply::Cursor {
        selection,
        mut records,
    } = sender
        .initial_control_exchange(
            NodeId(3),
            &p,
            &Request::Replace {
                expected: rejected_selection,
                descriptor: export.descriptor().clone(),
            },
        )
        .unwrap()
    else {
        panic!("replacement cursor");
    };
    assert_eq!(selection.generation(), rejected_selection.generation() + 1);
    assert_eq!(selection.history_id(), export.descriptor().id().unwrap());
    assert!(
        matches!(sender.initial_control_exchange(NodeId(3), &p, &Request::Finish(rejected_selection)).unwrap(), Reply::Conflict(ControlInitialState::Selected(actual)) if actual == selection)
    );
    while records < export.descriptor().records {
        let batch = writer.read_initial_control_batch(&export, records).unwrap();
        let Reply::Cursor { records: next, .. } = sender
            .initial_control_exchange(NodeId(3), &p, &Request::Batch { selection, batch })
            .unwrap()
        else {
            panic!("new cursor");
        };
        assert!(next > records);
        records = next;
    }
    assert!(
        matches!(sender.initial_control_exchange(NodeId(3), &p, &Request::Finish(selection)).unwrap(), Reply::Published { origin, .. } if origin == selection)
    );
    assert_eq!(*running.applied.lock().unwrap(), 0);
    let server = running.finish();
    assert_eq!(
        server.learners[&CONTROL]
            .control_observation()
            .unwrap()
            .catalog(),
        &f.catalog
    );
    drop(server);
    drop(wrong);
    drop(one.finish());
    drop(writer);
    f.cleanup();
}

#[test]
fn real_tls_old_initial_finish_observes_later_control_publication_without_rewind() {
    let mut f = Fixture::new("initial-later-control");
    let (mut writer, one, observed) = source::source(&mut f);
    let p = policy(&f);
    let export = writer
        .export_initial_control_source(&observed, p.limits)
        .unwrap();
    let running = InitialRunning::start(
        receiver(&f, ControlInitialState::Reserved),
        ControlInitialState::Reserved,
        f.root.join("initial-control"),
    );
    let mut sender = transport(&f, 0, running.address);
    sender.peers.get_mut(&NodeId(1)).unwrap().address = one.address;
    let selection = copy(&writer, &export, &sender, &p);
    assert!(matches!(
        sender
            .initial_control_exchange(NodeId(3), &p, &Request::Finish(selection))
            .unwrap(),
        Reply::Published { .. }
    ));
    let native = *running.ledger.lock().unwrap();
    writer.request_membership_intent(f.intent(119)).unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        assert!(
            Instant::now() < deadline,
            "actual next control command delivery"
        );
        match writer.poll_control_writer().unwrap() {
            ControlWriterProgress::ObservedOldControlDelivery(_) => break,
            _ => std::thread::sleep(Duration::from_millis(1)),
        }
    }
    assert_eq!(writer.cells[&CONTROL].committed_tail(), LogIndex(2));
    assert_eq!(
        sender
            .sync_learner(NodeId(3), writer.cells.get_mut(&CONTROL).unwrap())
            .unwrap(),
        LogIndex(2)
    );
    let current: [u8; 32] = std::fs::read(f.root.join("initial-control/ACTIVE"))
        .unwrap()
        .try_into()
        .unwrap();
    assert_ne!(current, selection.history_id());
    let before = tree(&f.root.join("initial-control"));
    assert_eq!(
        sender
            .initial_control_exchange(NodeId(3), &p, &Request::Finish(selection))
            .unwrap(),
        Reply::Initialized {
            origin: selection,
            current,
            tail: LogIndex(2)
        }
    );
    assert_eq!(tree(&f.root.join("initial-control")), before);
    assert_eq!(*running.ledger.lock().unwrap(), native);
    assert_eq!(running.events.lock().unwrap().len(), 3);
    assert_eq!(*running.applied.lock().unwrap(), 0);
    drop(running.finish());
    let running =
        InitialRunning::start(receiver(&f, native), native, f.root.join("initial-control"));
    let sender = transport(&f, 2, running.address);
    assert_eq!(
        sender
            .initial_control_exchange(NodeId(3), &p, &Request::Finish(selection))
            .unwrap(),
        Reply::Initialized {
            origin: selection,
            current,
            tail: LogIndex(2)
        }
    );
    assert!(running.events.lock().unwrap().is_empty());
    assert_eq!(
        std::fs::read(f.root.join("initial-control/ACTIVE")).unwrap(),
        current
    );
    let server = running.finish();
    let actual = server.learners[&CONTROL].control_observation().unwrap();
    assert_eq!(actual.committed_tail(), LogIndex(2));
    assert_eq!(actual.pending(), Some(&f.intent(119)));
    drop(server);
    drop(one.finish());
    drop(writer);
    f.cleanup();
}
