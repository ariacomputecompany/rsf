use super::*;

enum Fault {
    LoseReply(u8),
    RefuseAndLoseSource { original: PathBuf, saved: PathBuf },
}
struct FaultRunning {
    address: SocketAddr,
    stop: Arc<AtomicBool>,
    effects: Arc<Mutex<Vec<u8>>>,
    faults: Arc<Mutex<usize>>,
    native: Arc<Mutex<ControlInitialState>>,
    events: Arc<Mutex<Vec<ControlInitialState>>>,
    worker: Option<std::thread::JoinHandle<PeerReplicaServer>>,
}
impl FaultRunning {
    fn start(mut server: PeerReplicaServer, p: ControlInitialPolicy, fault: Fault) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let finished = stop.clone();
        let effects = Arc::new(Mutex::new(Vec::new()));
        let observed = effects.clone();
        let faults = Arc::new(Mutex::new(0));
        let injected = faults.clone();
        let native = Arc::new(Mutex::new(ControlInitialState::Reserved));
        let ledger = native.clone();
        let events = Arc::new(Mutex::new(Vec::new()));
        let transitions = events.clone();
        let worker = std::thread::spawn(move || {
            let mut fault = Some(fault);
            while !finished.load(Ordering::Acquire) {
                let socket = match listener.accept() {
                    Ok((socket, _)) => socket,
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(1));
                        continue;
                    }
                    Err(e) => panic!("fault listener: {e}"),
                };
                let mut stream = StreamOwned::new(
                    ServerConnection::new(Arc::clone(&server.identity.server)).unwrap(),
                    DeadlineStream::new(socket, Instant::now() + Duration::from_millis(800))
                        .unwrap(),
                );
                while stream.conn.is_handshaking() {
                    stream.conn.complete_io(&mut stream.sock).unwrap();
                }
                let cert = stream.conn.peer_certificates().unwrap().first().unwrap();
                let sender = server.nodes[&fingerprint(cert.as_ref())];
                let bytes = read_frame(&mut stream).unwrap();
                let (kind, payload) = parse(&bytes, &server.cluster, sender, server.local).unwrap();
                assert_eq!(kind, CONTROL_INITIAL);
                let id = crate::replication::managed_control::initial::wire::policy_id(
                    server.cluster,
                    server.local,
                    &p,
                );
                let request = crate::replication::managed_control::initial::wire::decode_request(
                    payload, id, p.limits,
                )
                .unwrap();
                let op = payload[38];
                let refusing = op == 1 && matches!(fault, Some(Fault::RefuseAndLoseSource { .. }));
                let mut persist = |event: &crate::replication::ControlInitialTransition<'_>| {
                    assert_eq!(event.expected(), *ledger.lock().unwrap());
                    *ledger.lock().unwrap() = event.next();
                    transitions.lock().unwrap().push(event.next());
                    Ok(())
                };
                let result = if refusing {
                    server.dispatch_initial_control(sender, payload, None)
                } else {
                    server.dispatch_initial_control(sender, payload, Some(&mut persist))
                };
                let (response_kind, response) = match result {
                    Ok(response) => {
                        crate::replication::managed_control::initial::wire::decode_reply(
                            &response, id, payload, &request, p.limits,
                        )
                        .unwrap();
                        observed.lock().unwrap().push(op);
                        (kind, response)
                    }
                    Err(error) => {
                        assert!(refusing, "unexpected actual handler failure: {error}");
                        assert!(
                            error
                                .to_string()
                                .contains("requires native restriction persistence")
                        );
                        (255, error.to_string().into_bytes())
                    }
                };
                if refusing {
                    let Some(Fault::RefuseAndLoseSource { original, saved }) = fault.take() else {
                        unreachable!()
                    };
                    let mut count = injected.lock().unwrap();
                    std::fs::rename(original, saved).unwrap();
                    *count += 1;
                } else if matches!(fault, Some(Fault::LoseReply(expected)) if expected == op) {
                    assert_eq!(
                        response_kind, CONTROL_INITIAL,
                        "lose only a successful actual effect"
                    );
                    fault.take();
                    *injected.lock().unwrap() += 1;
                    // Actual successful handler/validated payload precede loss.
                    drop(stream);
                    continue;
                }
                let response = frame(
                    &server.cluster,
                    server.local,
                    sender,
                    response_kind,
                    &response,
                )
                .unwrap();
                write_frame(&mut stream, &response).unwrap();
            }
            server
        });
        Self {
            address,
            stop,
            effects,
            faults,
            native,
            events,
            worker: Some(worker),
        }
    }
    fn finish(mut self) -> PeerReplicaServer {
        self.stop.store(true, Ordering::Release);
        self.worker.take().unwrap().join().unwrap()
    }
}
impl Drop for FaultRunning {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}
#[test]
fn real_tls_sender_recovers_actual_lost_batch_and_finish_replies_from_receiver_status() {
    lost_replies(true);
}
#[test]
fn real_tls_sender_automatically_reconciles_lost_batch_and_finish_without_cancellation() {
    lost_replies(false);
}
fn lost_replies(restart: bool) {
    for op in [4, 5] {
        let mut f = Fixture::new(match (restart, op) {
            (true, 4) => "sender-lost-batch",
            (true, _) => "sender-lost-finish",
            (false, 4) => "sender-auto-lost-batch",
            (false, _) => "sender-auto-lost-finish",
        });
        let (mut writer, one, delivered) = source::source(&mut f);
        let p = policy(&f);
        let running = FaultRunning::start(
            receiver(&f, ControlInitialState::Reserved),
            p.clone(),
            Fault::LoseReply(op),
        );
        point(&mut writer, running.address);
        writer
            .request_initial_control_transfer(&delivered, NodeId(3), p.limits, None)
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            assert!(
                Instant::now() < deadline,
                "lost result must become uncertain, not success"
            );
            match writer.poll_initial_control_transfer().unwrap() {
                ControlInitialProgress::Retrying { target, .. } => {
                    assert_eq!(target, NodeId(3));
                    break;
                }
                ControlInitialProgress::Observed(_) => panic!("lost reply fabricated success"),
                _ => std::thread::sleep(Duration::from_millis(1)),
            }
        }
        assert_eq!(*running.faults.lock().unwrap(), 1);
        let selection = match *running.native.lock().unwrap() {
            ControlInitialState::Selected(selection)
            | ControlInitialState::Published { selection, .. } => selection,
            other => panic!("lost actual reply has unexpected durable state: {other:?}"),
        };
        if restart {
            // Only this branch discards volatile sender state. The automatic
            // branch must traverse its real Retrying/backoff/Status path.
            assert!(writer.cancel_initial_control_transfer());
            writer
                .request_initial_control_transfer(&delivered, NodeId(3), p.limits, None)
                .unwrap();
        }
        let published = complete(&mut writer);
        assert_eq!(published.origin(), selection);
        assert_eq!(published.origin().generation(), 1);
        assert_eq!(published.committed_tail(), LogIndex(1));
        let effects = running.effects.lock().unwrap().clone();
        assert!(
            effects.iter().filter(|&&v| v == 0).count() >= 2,
            "uncertain or restarted transfer must query actual Status"
        );
        assert_eq!(
            effects.iter().filter(|&&v| v == 1).count(),
            1,
            "never re-Start selected state"
        );
        assert_eq!(
            effects.iter().filter(|&&v| v == 5).count(),
            1,
            "lost FINISH requires no re-publication"
        );
        assert_eq!(
            *running.events.lock().unwrap(),
            vec![
                ControlInitialState::Selected(selection),
                ControlInitialState::Publishing {
                    selection,
                    source: NodeId(0)
                },
                ControlInitialState::Published {
                    selection,
                    source: NodeId(0)
                },
            ],
            "actual receiver transitions must occur once in exact order"
        );
        assert_eq!(
            *running.native.lock().unwrap(),
            ControlInitialState::Published {
                selection,
                source: NodeId(0),
            }
        );
        let server = running.finish();
        assert_eq!(
            server.learners[&CONTROL]
                .control_observation()
                .unwrap()
                .catalog(),
            &f.catalog
        );
        drop(server);
        drop(one.finish());
        drop(writer);
        f.cleanup();
    }
}
// The first poll check may detect this actual inode fault before consuming the
// pending semantic refusal. This proves dominance, not isolation of the second
// post-result source validation; no synthetic response/owner is substituted.
#[test]
fn real_tls_sender_source_custody_loss_dominates_semantic_refusal_and_clears_attempt() {
    let mut f = Fixture::new("sender-semantic-source-loss");
    let (mut writer, one, delivered) = source::source(&mut f);
    let original = f.root.join("pg-0/node-0/MANIFEST.lock");
    let saved = f.root.join("original-manifest-lock");
    let before = std::fs::read(&original).unwrap();
    let running = FaultRunning::start(
        receiver(&f, ControlInitialState::Reserved),
        policy(&f),
        Fault::RefuseAndLoseSource {
            original: original.clone(),
            saved: saved.clone(),
        },
    );
    point(&mut writer, running.address);
    writer
        .request_initial_control_transfer(&delivered, NodeId(3), policy(&f).limits, None)
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    let error = loop {
        assert!(Instant::now() < deadline);
        match writer.poll_initial_control_transfer() {
            Err(error) => break error,
            Ok(ControlInitialProgress::Observed(_)) => panic!("lost source fabricated completion"),
            _ => std::thread::sleep(Duration::from_millis(1)),
        }
    };
    assert_eq!(*running.faults.lock().unwrap(), 1);
    assert!(
        !error.contains("peer rejected request:"),
        "physical source error must dominate: {error}"
    );
    assert!(!original.exists());
    assert!(
        !writer.cancel_initial_control_transfer(),
        "failed attempt cleared"
    );
    std::fs::rename(&saved, &original).unwrap();
    assert_eq!(std::fs::read(&original).unwrap(), before);
    let error = writer
        .export_initial_control_source(&delivered, policy(&f).limits)
        .unwrap_err();
    let ControlInitialSourceError::Failure(error) = error else {
        panic!("sticky physical source fence required: {error:?}")
    };
    assert!(error.message.contains("VSR durability failed"), "{error:?}");
    assert_eq!(
        *running.native.lock().unwrap(),
        ControlInitialState::Reserved
    );
    drop(running.finish());
    drop(one.finish());
    drop(writer);
    f.cleanup();
}
#[test]
fn unavailable_initial_target_can_be_cancelled_and_pending_control_work_progresses() {
    let mut f = Fixture::new("sender-unavailable-cancel");
    let (mut writer, one, delivered) = source::source(&mut f);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    // Retain listener so no other test can claim it. No TLS service accepts.
    point(&mut writer, address);
    writer.request_membership_intent(f.intent(121)).unwrap();
    writer
        .request_initial_control_transfer(&delivered, NodeId(3), policy(&f).limits, None)
        .unwrap();
    assert!(matches!(
        writer.poll_initial_control_transfer().unwrap(),
        ControlInitialProgress::Waiting
    ));
    assert!(matches!(
        writer.poll_control_writer().unwrap(),
        ControlWriterProgress::Waiting
    ));
    let accepted = prepare_command(
        writer.cells.get_mut(&CONTROL).unwrap(),
        ControlCommand::Begin(f.intent(121)),
    );
    assert_eq!(accepted.index, LogIndex(2));
    assert_eq!(writer.cells[&CONTROL].committed_tail(), LogIndex(1));
    assert!(
        writer.cancel_initial_control_transfer(),
        "joins bounded in-flight handshake"
    );
    drop(listener);
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        assert!(
            Instant::now() < deadline,
            "cancelled sender must not strand control writer"
        );
        if matches!(
            writer.poll_control_writer().unwrap(),
            ControlWriterProgress::ObservedOldControlDelivery(_)
        ) {
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(writer.cells[&CONTROL].committed_tail(), LogIndex(2));
    assert_eq!(
        writer.cells[&CONTROL].committed_operation(LogIndex(2)),
        Some(&accepted.operation)
    );
    drop(one.finish());
    drop(writer);
    f.cleanup();
}
