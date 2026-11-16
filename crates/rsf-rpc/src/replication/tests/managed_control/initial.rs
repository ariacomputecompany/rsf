//! Real TLS and actual writer delivery. Native mock proves callback order only.
mod sender;
use super::*;
use crate::replication::managed_control::initial::{
    ControlInitialBinding, ControlInitialPolicy, ControlInitialState,
    wire::{Reply, Request},
};
use rsf_vsr::{LearnerHistoryStore, VsrInitialSelection, VsrPrefixLimits};
fn policy(f: &Fixture) -> ControlInitialPolicy {
    ControlInitialPolicy {
        anchor: f.anchor.clone(),
        genesis: f.catalog.clone(),
        limits: VsrPrefixLimits {
            max_records: 4096,
            max_encoded_bytes: 16 * 1024 * 1024,
        },
    }
}
fn voter(f: &mut Fixture, node: u64) -> PeerReplicaServer {
    if node == 1 {
        return f.server.take().unwrap();
    }
    PeerReplicaServer::new(
        NodeId(node),
        &cluster(),
        identity(&f.root, node),
        f.peers.clone(),
        [
            (CONTROL, f.control.remove(&NodeId(node)).unwrap()),
            (DATA, f.data.remove(&NodeId(node)).unwrap()),
        ]
        .into(),
        Duration::from_millis(800),
    )
    .unwrap()
    .with_control_replication(f.anchor.clone(), f.catalog.clone())
    .unwrap()
}
fn delivery(writer: &mut PeerReplicaServer) -> ObservedControlGenesisDelivery {
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut last = String::new();
    while Instant::now() < deadline {
        match writer.poll_control_writer().unwrap() {
            ControlWriterProgress::ObservedGenesisDelivery(value) => return value,
            other => last = format!("{other:?}"),
        };
        std::thread::sleep(Duration::from_millis(1));
    }
    panic!("actual delivery did not complete within unchanged budget: {last}")
}
fn receiver(f: &Fixture, state: ControlInitialState) -> PeerReplicaServer {
    let data =
        LearnerHistoryStore::open(f.root.join("initial-data"), DATA, NodeId(3), NodeId(2)).unwrap();
    PeerReplicaServer::with_learners(
        NodeId(3),
        &cluster(),
        identity(&f.root, 3),
        f.peers.clone(),
        BTreeMap::new(),
        [(DATA, data)].into(),
        Duration::from_millis(800),
    )
    .unwrap()
    .with_initial_control_learner(
        f.root.join("initial-control"),
        ControlInitialBinding {
            policy: policy(f),
            state,
        },
    )
    .unwrap()
}
struct InitialRunning {
    address: SocketAddr,
    stop: Arc<AtomicBool>,
    ledger: Arc<Mutex<ControlInitialState>>,
    events: Arc<Mutex<Vec<ControlInitialState>>>,
    applied: Arc<Mutex<usize>>,
    worker: Option<std::thread::JoinHandle<PeerReplicaServer>>,
}
impl InitialRunning {
    fn start(mut server: PeerReplicaServer, state: ControlInitialState, path: PathBuf) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let finished = stop.clone();
        let ledger = Arc::new(Mutex::new(state));
        let native = ledger.clone();
        let events = Arc::new(Mutex::new(Vec::new()));
        let observed = events.clone();
        let applied = Arc::new(Mutex::new(0));
        let effects = applied.clone();
        let worker = std::thread::spawn(move || {
            while !finished.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((socket, _)) => {
                        let _ = server.serve_connection_with_initial_control(
                            socket,
                            &mut |_| {
                                *effects.lock().unwrap() += 1;
                                Ok(())
                            },
                            &mut |event| {
                                assert_eq!(event.local(), NodeId(3));
                                assert_eq!(event.expected(), *native.lock().unwrap());
                                if matches!(event.next(), ControlInitialState::Publishing { .. }) {
                                    assert!(!path.join("ACTIVE").exists());
                                }
                                if let ControlInitialState::Published { selection, .. } =
                                    event.next()
                                {
                                    assert_eq!(
                                        std::fs::read(path.join("ACTIVE")).unwrap(),
                                        selection.history_id()
                                    );
                                }
                                *native.lock().unwrap() = event.next();
                                observed.lock().unwrap().push(event.next());
                                Ok(())
                            },
                        );
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(1))
                    }
                    Err(e) => panic!("listener: {e}"),
                }
            }
            server
        });
        Self {
            address,
            stop,
            ledger,
            events,
            applied,
            worker: Some(worker),
        }
    }
    fn finish(mut self) -> PeerReplicaServer {
        self.stop.store(true, Ordering::Release);
        self.worker.take().unwrap().join().unwrap()
    }
}
impl Drop for InitialRunning {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}
fn transport(f: &Fixture, node: u64, address: SocketAddr) -> NetworkVsrTransport {
    let mut peers = f.peers.clone();
    peers.get_mut(&NodeId(3)).unwrap().address = address;
    NetworkVsrTransport::new(
        NodeId(node),
        &cluster(),
        identity(&f.root, node),
        peers,
        Duration::from_millis(800),
    )
    .unwrap()
}
fn reject(reply: io::Result<Reply>, reason: &str) {
    let error = reply.unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{error}");
    let text = error.to_string();
    assert!(text.starts_with("peer rejected request:"), "{text}");
    assert!(text.contains(reason), "expected {reason}: {text}");
}
fn copy(
    source: &PeerReplicaServer,
    export: &ControlInitialSource,
    sender: &NetworkVsrTransport,
    p: &ControlInitialPolicy,
) -> VsrInitialSelection {
    let Reply::Cursor {
        selection,
        mut records,
    } = sender
        .initial_control_exchange(NodeId(3), p, &Request::Start(export.descriptor().clone()))
        .unwrap()
    else {
        panic!("cursor")
    };
    while records < export.descriptor().records {
        let batch = source.read_initial_control_batch(export, records).unwrap();
        let Reply::Cursor {
            selection: actual,
            records: next,
        } = sender
            .initial_control_exchange(NodeId(3), p, &Request::Batch { selection, batch })
            .unwrap()
        else {
            panic!("batch")
        };
        assert_eq!(actual, selection);
        assert!(next > records);
        records = next;
    }
    selection
}
#[test]
fn real_tls_initial_genesis_requires_native_barriers_and_cold_published_origin() {
    let mut f = Fixture::new("initial-complete");
    let one = Running::start(voter(&mut f, 1));
    let mut writer = voter(&mut f, 0);
    writer
        .control_transport
        .peers
        .get_mut(&NodeId(1))
        .unwrap()
        .address = one.address;
    writer.request_control_genesis().unwrap();
    let delivered = delivery(&mut writer);
    let p = policy(&f);
    let export = writer
        .export_initial_control_source(&delivered, p.limits)
        .unwrap();
    let initial = InitialRunning::start(
        receiver(&f, ControlInitialState::Reserved),
        ControlInitialState::Reserved,
        f.root.join("initial-control"),
    );
    let sender = transport(&f, 0, initial.address);
    let selected = copy(&writer, &export, &sender, &p);
    assert!(
        matches!(sender.initial_control_exchange(NodeId(3),&p,&Request::Finish(selected)).unwrap(),Reply::Published{origin,current,tail:LogIndex(1)}if origin==selected&&current==selected.history_id())
    );
    assert_eq!(initial.events.lock().unwrap().len(), 3);
    assert_eq!(*initial.applied.lock().unwrap(), 0);
    let state = *initial.ledger.lock().unwrap();
    let mut server = initial.finish();
    assert_eq!(
        server.learners[&CONTROL]
            .control_observation()
            .unwrap()
            .catalog(),
        &f.catalog
    );
    assert!(server.committed_application_entries().unwrap().is_empty());
    drop(server);
    let initial = InitialRunning::start(receiver(&f, state), state, f.root.join("initial-control"));
    let sender = transport(&f, 2, initial.address);
    assert!(
        matches!(sender.initial_control_exchange(NodeId(3),&p,&Request::Finish(selected)).unwrap(),Reply::Published{origin,..}if origin==selected)
    );
    assert!(initial.events.lock().unwrap().is_empty());
    drop(initial.finish());
    drop(one.finish());
    drop(writer);
    f.cleanup();
}
#[test]
fn real_tls_initial_rejects_ordinary_history_missing_adapter_and_foreign_policy_before_effects() {
    let mut f = Fixture::new("initial-refusal");
    let one = Running::start(voter(&mut f, 1));
    let mut writer = voter(&mut f, 0);
    writer
        .control_transport
        .peers
        .get_mut(&NodeId(1))
        .unwrap()
        .address = one.address;
    writer.request_control_genesis().unwrap();
    let observed = delivery(&mut writer);
    let p = policy(&f);
    let export = writer
        .export_initial_control_source(&observed, p.limits)
        .unwrap();
    let ordinary = Running::start(receiver(&f, ControlInitialState::Reserved));
    let sender = transport(&f, 0, ordinary.address);
    let before = tree(&f.root.join("initial-control"));
    reject(
        sender.initial_control_exchange(
            NodeId(3),
            &p,
            &Request::Start(export.descriptor().clone()),
        ),
        "requires native restriction persistence",
    );
    rejected(
        sender.exchange_once(
            NodeId(3),
            HISTORY_BEGIN,
            &export.descriptor().encode().unwrap(),
        ),
        "initial control requires its distinct bounded native-persisted protocol",
    );
    assert_eq!(tree(&f.root.join("initial-control")), before);
    assert!(ordinary.applied.lock().unwrap().is_empty());
    let initial = InitialRunning::start(
        ordinary.finish(),
        ControlInitialState::Reserved,
        f.root.join("initial-control"),
    );
    let sender = transport(&f, 0, initial.address);
    let mut foreign = p.clone();
    foreign.limits.max_records -= 1;
    reject(
        sender.initial_control_exchange(
            NodeId(3),
            &foreign,
            &Request::Start(export.descriptor().clone()),
        ),
        "differs from configured policy",
    );
    assert_eq!(tree(&f.root.join("initial-control")), before);
    assert!(initial.events.lock().unwrap().is_empty());
    drop(initial.finish());
    drop(one.finish());
    drop(writer);
    f.cleanup();
}
#[test]
fn actual_initial_source_is_bounded_and_invalidated_by_real_view_promise() {
    let mut f = Fixture::new("initial-token");
    let one = Running::start(voter(&mut f, 1));
    let mut writer = voter(&mut f, 0);
    writer
        .control_transport
        .peers
        .get_mut(&NodeId(1))
        .unwrap()
        .address = one.address;
    writer.request_control_genesis().unwrap();
    let observed = delivery(&mut writer);
    let p = policy(&f);
    assert!(matches!(
        writer.export_initial_control_source(
            &observed,
            VsrPrefixLimits {
                max_records: 1,
                max_encoded_bytes: 18
            }
        ),
        Err(ControlInitialSourceError::Prefix(
            rsf_vsr::VsrPrefixExportError::LimitExceeded
        ))
    ));
    let export = writer
        .export_initial_control_source(&observed, p.limits)
        .unwrap();
    assert!(
        !writer
            .read_initial_control_batch(&export, 0)
            .unwrap()
            .records
            .is_empty()
    );
    writer
        .cells
        .get_mut(&CONTROL)
        .unwrap()
        .begin_view_change(
            NodeId(0),
            NodeId(2),
            rsf_vsr::VsrViewChangeRequest {
                pg: CONTROL,
                previous_view: Epoch(1),
                target_view: Epoch(2),
                candidate: NodeId(2),
                voters: (0..3).map(NodeId).collect(),
            },
        )
        .unwrap();
    assert!(matches!(
        writer.read_initial_control_batch(&export, 0),
        Err(ControlInitialSourceError::Ownership(_))
    ));
    assert!(matches!(
        writer.export_initial_control_source(&observed, p.limits),
        Err(ControlInitialSourceError::Ownership(_))
    ));
    drop(one.finish());
    drop(writer);
    f.cleanup();
}
#[test]
fn initial_reply_codec_rejects_other_request_policy_and_noncanonical_length() {
    use crate::replication::managed_control::initial::wire;
    let policy = [4; 32];
    let limits = VsrPrefixLimits {
        max_records: 128,
        max_encoded_bytes: 1024 * 1024,
    };
    let a = wire::encode_request(policy, &Request::Status).unwrap();
    let selected = VsrInitialSelection::new(1, [8; 32]).unwrap();
    let b = wire::encode_request(policy, &Request::Finish(selected)).unwrap();
    let response = wire::encode_reply(
        &a,
        policy,
        &Reply::Progress {
            state: ControlInitialState::Reserved,
            records: 0,
        },
    )
    .unwrap();
    assert!(wire::decode_reply(&response, policy, &b, &Request::Finish(selected), limits).is_err());
    assert!(wire::decode_reply(&response, [5; 32], &a, &Request::Status, limits).is_err());
    // A valid correlation digest is not permission to change response kind or
    // attribute another generation's publication to this request.
    let wrong_kind = wire::encode_reply(
        &b,
        policy,
        &Reply::Cursor {
            selection: selected,
            records: 0,
        },
    )
    .unwrap();
    assert!(
        wire::decode_reply(&wrong_kind, policy, &b, &Request::Finish(selected), limits).is_err()
    );
    let wrong_origin = VsrInitialSelection::new(2, selected.history_id()).unwrap();
    let wrong_generation = wire::encode_reply(
        &b,
        policy,
        &Reply::Published {
            origin: wrong_origin,
            current: selected.history_id(),
            tail: LogIndex(1),
        },
    )
    .unwrap();
    assert!(
        wire::decode_reply(
            &wrong_generation,
            policy,
            &b,
            &Request::Finish(selected),
            limits
        )
        .is_err()
    );
    let mut extra = response.clone();
    extra.push(0);
    assert!(wire::decode_reply(&extra, policy, &a, &Request::Status, limits).is_err());
    assert_eq!(
        wire::decode_reply(&response, policy, &a, &Request::Status, limits).unwrap(),
        Reply::Progress {
            state: ControlInitialState::Reserved,
            records: 0
        }
    );
}

fn install_control(candidate: &mut PeerReplicaServer, previous: u64, target: u64, local: u64) {
    let transport = candidate.control_transport.clone();
    let request = rsf_vsr::VsrViewChangeRequest {
        pg: CONTROL,
        previous_view: Epoch(previous),
        target_view: Epoch(target),
        candidate: NodeId(local),
        voters: (0..3).map(NodeId).collect(),
    };
    let cell = candidate.cells.get_mut(&CONTROL).unwrap();
    let (input, snapshots) = transport
        .prepare_installation_input(cell, &request)
        .unwrap();
    assert_eq!(
        input
            .reports()
            .iter()
            .map(|r| r.replica)
            .collect::<BTreeSet<_>>(),
        [NodeId(1), NodeId(2)].into()
    );
    transport
        .stage_installation_input(cell, &input, &snapshots)
        .unwrap();
    assert_eq!(
        transport
            .resume_view_installation(cell, &mut |entry| {
                assert_eq!(entry.prepare.index, LogIndex(1));
                let rsf_vsr::VsrOperation::ReplicatedCommand { payload, .. } =
                    &entry.prepare.operation
                else {
                    panic!("actual control command")
                };
                assert!(matches!(
                    ControlCommand::decode(payload).unwrap(),
                    ControlCommand::Genesis(_)
                ));
                Ok(())
            })
            .unwrap(),
        Epoch(target)
    );
}
#[test]
fn real_tls_current_primary_two_relays_historical_genesis_one_after_original_source_loss() {
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
    let sender = transport(&f, 2, initial.address);
    let selection = copy(&second, &export, &sender, &p);
    assert!(
        matches!(sender.initial_control_exchange(NodeId(3),&p,&Request::Finish(selection)).unwrap(),Reply::Published{origin,..}if origin==selection)
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

mod source;

mod failures;

mod accepted_tail;
mod application;
