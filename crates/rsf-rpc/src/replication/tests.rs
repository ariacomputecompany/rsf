use super::*;
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose,
};
use rsf_core::{Epoch, OperationId};
use rsf_vsr::{VsrDecision, VsrOperation};
use std::io::{BufRead, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};

fn root(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "rsf-peer-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}
fn cluster() -> ClusterId {
    ClusterId::new("test-cluster").unwrap()
}
fn provision(root: &Path) {
    let mut params = CertificateParams::new(Vec::new()).unwrap();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    let key = KeyPair::generate().unwrap();
    let ca = params.self_signed(&key).unwrap();
    let issuer = Issuer::new(params, key);
    for node in 0..4 {
        let mut params = CertificateParams::new(vec!["localhost".into()]).unwrap();
        params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ];
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        let key = KeyPair::generate().unwrap();
        let certificate = params.signed_by(&key, &issuer).unwrap();
        let dir = root.join(format!("tls-{node}"));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("certificate.pem"), certificate.pem()).unwrap();
        std::fs::write(dir.join("key.pem"), key.serialize_pem()).unwrap();
        std::fs::write(dir.join("ca.pem"), ca.pem()).unwrap();
    }
}
fn identity(root: &Path, node: u64) -> PeerIdentity {
    PeerIdentity::load(&root.join(format!("tls-{node}"))).unwrap()
}
fn endpoints(root: &Path) -> BTreeMap<NodeId, PeerEndpoint> {
    (0..3)
        .map(|node| {
            (
                NodeId(node),
                PeerEndpoint {
                    address: "127.0.0.1:1".parse().unwrap(),
                    server_name: "localhost".into(),
                    fingerprint: identity(root, node).fingerprint,
                },
            )
        })
        .collect()
}
fn cell(root: &Path, node: u64) -> DurableVsrCell {
    DurableVsrCell::open(
        root.join(format!("store-{node}")),
        PlacementGroupId(1),
        Epoch(1),
        NodeId(0),
        [NodeId(0), NodeId(1), NodeId(2)],
    )
    .unwrap()
}
fn transport(root: &Path, peers: BTreeMap<NodeId, PeerEndpoint>) -> NetworkVsrTransport {
    NetworkVsrTransport::new(
        NodeId(0),
        &cluster(),
        identity(root, 0),
        peers,
        Duration::from_millis(800),
    )
    .unwrap()
}
fn prepare(cell: &mut DurableVsrCell, value: &[u8]) -> VsrPrepare {
    let index = cell.next_index();
    let operation = VsrOperation::ReplicatedCommand {
        operation_id: OperationId(index.0),
        payload: value.to_vec(),
    };
    let VsrDecision::Prepared(prepare) = cell
        .prepare(NodeId(0), operation, value.len() as u32)
        .unwrap()
    else {
        panic!("prepare rejected")
    };
    prepare
}

struct PeerProcess {
    child: Child,
    address: SocketAddr,
}
impl PeerProcess {
    fn spawn(root: &Path, node: u64) -> Self {
        Self::spawn_with_learner(root, node, false)
    }
    fn spawn_with_learner(root: &Path, node: u64, include_learner: bool) -> Self {
        Self::spawn_config(root, node, include_learner, false, false, false)
    }
    fn spawn_decision(root: &Path, node: u64, learner: bool) -> Self {
        Self::spawn_config(root, node, true, true, learner, false)
    }
    fn spawn_staging(root: &Path, node: u64, learner: bool) -> Self {
        Self::spawn_config(root, node, true, true, learner, true)
    }
    fn spawn_config(
        root: &Path,
        node: u64,
        include_learner: bool,
        decisions: bool,
        learner: bool,
        stage_apply: bool,
    ) -> Self {
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "replication::tests::peer_process", "--nocapture"])
            .env("RSF_RPC_TEST_ROOT", root)
            .env("RSF_RPC_TEST_NODE", node.to_string())
            .env(
                "RSF_RPC_TEST_INCLUDE_LEARNER",
                if include_learner { "1" } else { "0" },
            )
            .env("RSF_RPC_TEST_DECISIONS", if decisions { "1" } else { "0" })
            .env("RSF_RPC_TEST_LEARNER_ROLE", if learner { "1" } else { "0" })
            .env(
                "RSF_RPC_TEST_STAGE_APPLY",
                if stage_apply { "1" } else { "0" },
            )
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let (send, receive) = std::sync::mpsc::channel();
        let reader = std::thread::spawn(move || {
            for line in std::io::BufReader::new(stdout).lines() {
                if let Some(address) = line.unwrap().strip_prefix("RSF_PEER_READY=") {
                    send.send(address.parse::<SocketAddr>().unwrap()).unwrap();
                    return;
                }
            }
        });
        let address = receive.recv_timeout(Duration::from_secs(10));
        if address.is_err() {
            let _ = child.kill();
            let _ = child.wait();
        }
        reader.join().unwrap();
        Self {
            child,
            address: address.expect("peer startup"),
        }
    }
}
impl Drop for PeerProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[test]
fn peer_process() {
    let Some(root) = std::env::var_os("RSF_RPC_TEST_ROOT") else {
        return;
    };
    let root = PathBuf::from(root);
    let node = std::env::var("RSF_RPC_TEST_NODE")
        .unwrap()
        .parse::<u64>()
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let mut peers = endpoints(&root);
    if std::env::var("RSF_RPC_TEST_INCLUDE_LEARNER").as_deref() == Ok("1") {
        peers.insert(
            NodeId(3),
            PeerEndpoint {
                address: "127.0.0.1:1".parse().unwrap(),
                server_name: "localhost".into(),
                fingerprint: identity(&root, 3).fingerprint,
            },
        );
    }
    let mut server = if std::env::var("RSF_RPC_TEST_LEARNER_ROLE").as_deref() == Ok("1") {
        PeerReplicaServer::with_learners(
            NodeId(node),
            &cluster(),
            identity(&root, node),
            peers,
            BTreeMap::new(),
            [(
                PlacementGroupId(1),
                rsf_vsr::LearnerHistoryStore::open(
                    root.join(format!("learner-{node}")),
                    PlacementGroupId(1),
                    NodeId(node),
                    NodeId(0),
                )
                .unwrap(),
            )]
            .into(),
            Duration::from_millis(800),
        )
        .unwrap()
    } else {
        PeerReplicaServer::new(
            NodeId(node),
            &cluster(),
            identity(&root, node),
            peers,
            [(PlacementGroupId(1), cell(&root, node))].into(),
            Duration::from_millis(800),
        )
        .unwrap()
    };
    if std::env::var("RSF_RPC_TEST_DECISIONS").as_deref() == Ok("1") {
        server = server
            .with_membership_paths(
                [(PlacementGroupId(1), root.join(format!("membership-{node}")))].into(),
            )
            .unwrap();
    }
    println!("RSF_PEER_READY={}", listener.local_addr().unwrap());
    std::io::stdout().flush().unwrap();
    for socket in listener.incoming() {
        let socket = socket.unwrap();
        let started = Instant::now();
        let result = if std::env::var("RSF_RPC_TEST_STAGE_APPLY").as_deref() == Ok("1") {
            server.serve_connection_with_apply(socket, &mut |entry| {
                persist_staged_entry(&root, node, entry)
            })
        } else {
            server.serve_connection(socket)
        };
        if started.elapsed() >= Duration::from_millis(100) || result.is_err() {
            eprintln!(
                "peer {node} root={} service {:?}: {result:?}",
                root.display(),
                started.elapsed()
            );
        }
    }
}

fn persist_staged_entry(root: &Path, node: u64, entry: &VsrCommittedEntry) -> Result<(), String> {
    let directory = root.join(format!("materialized-{node}"));
    std::fs::create_dir_all(&directory).map_err(|e| e.to_string())?;
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(directory.join(entry.prepare.index.0.to_string()))
        .map_err(|e| e.to_string())?;
    file.write_all(&entry.prepare.encode().map_err(|e| e.message)?)
        .and_then(|()| file.sync_all())
        .map_err(|e| e.to_string())?;
    let mut calls = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(directory.join(".applies"))
        .map_err(|e| e.to_string())?;
    calls
        .write_all(&entry.prepare.index.0.to_le_bytes())
        .and_then(|()| calls.sync_all())
        .map_err(|e| e.to_string())?;
    std::fs::File::open(directory)
        .and_then(|f| f.sync_all())
        .map_err(|e| e.to_string())?;
    std::fs::File::open(root)
        .and_then(|f| f.sync_all())
        .map_err(|e| e.to_string())
}

#[test]
fn real_peer_processes_preserve_quorum_payloads_and_repair_after_kill() {
    let root = root("quorum");
    provision(&root);
    let first = PeerProcess::spawn(&root, 1);
    let second = PeerProcess::spawn(&root, 2);
    let mut peers = endpoints(&root);
    peers.get_mut(&NodeId(1)).unwrap().address = first.address;
    peers.get_mut(&NodeId(2)).unwrap().address = second.address;
    let mut network = transport(&root, peers.clone());
    let mut leader = cell(&root, 0);
    let one = prepare(&mut leader, b"tenant=A\0key=shared\0value=first\xff");
    leader
        .replicate_with_transport(NodeId(0), one.index, &mut network)
        .unwrap();
    assert_eq!(
        network
            .committed_tail(NodeId(1), PlacementGroupId(1))
            .unwrap(),
        one.index
    );
    assert_eq!(
        network
            .committed_tail(NodeId(2), PlacementGroupId(1))
            .unwrap(),
        one.index
    );
    drop(second);
    let two = prepare(&mut leader, b"tenant=A\0key=shared\0value=second");
    leader
        .replicate_with_transport(NodeId(0), two.index, &mut network)
        .unwrap();
    drop(first);
    let three = prepare(&mut leader, b"value=third");
    assert!(
        leader
            .replicate_with_transport(NodeId(0), three.index, &mut network)
            .is_err()
    );
    assert_eq!(leader.committed_tail(), two.index);
    // A killed follower reopens its own WAL, catches up the missing second
    // command, then supplies a real durable vote for the third.
    let second = PeerProcess::spawn(&root, 2);
    peers.get_mut(&NodeId(2)).unwrap().address = second.address;
    let mut network = transport(&root, peers);
    leader
        .replicate_with_transport(NodeId(0), three.index, &mut network)
        .unwrap();
    assert_eq!(
        network
            .committed_tail(NodeId(2), PlacementGroupId(1))
            .unwrap(),
        three.index
    );
    drop(second);
    let recovered = cell(&root, 2);
    assert_eq!(
        recovered.committed_prefix().unwrap(),
        leader.committed_prefix().unwrap()
    );
    assert_eq!(
        recovered.committed_operation(three.index),
        Some(&three.operation)
    );
    drop(recovered);
    drop(leader);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn tls_node_cluster_and_primary_bindings_reject_spoofing_without_writes() {
    let root = root("auth");
    provision(&root);
    let peer = PeerProcess::spawn(&root, 1);
    let mut peers = endpoints(&root);
    peers.get_mut(&NodeId(1)).unwrap().address = peer.address;
    let mut leader = cell(&root, 0);
    let prepare = prepare(&mut leader, b"must-not-be-written");
    let mut wrong_cluster = NetworkVsrTransport::new(
        NodeId(0),
        &ClusterId::new("other-cluster").unwrap(),
        identity(&root, 0),
        peers.clone(),
        Duration::from_millis(800),
    )
    .unwrap();
    assert!(
        wrong_cluster
            .acknowledge_prepare(VsrPrepareMessage {
                prepare: prepare.clone(),
                replica: NodeId(1)
            })
            .is_err()
    );
    let mut wrong_target = peers.clone();
    wrong_target.get_mut(&NodeId(1)).unwrap().fingerprint = identity(&root, 3).fingerprint;
    assert!(
        transport(&root, wrong_target)
            .acknowledge_prepare(VsrPrepareMessage {
                prepare: prepare.clone(),
                replica: NodeId(1)
            })
            .is_err()
    );
    let mut impersonator = peers.clone();
    impersonator.get_mut(&NodeId(0)).unwrap().fingerprint = identity(&root, 3).fingerprint;
    let mut impersonator = NetworkVsrTransport::new(
        NodeId(0),
        &cluster(),
        identity(&root, 3),
        impersonator,
        Duration::from_millis(800),
    )
    .unwrap();
    assert!(
        impersonator
            .acknowledge_prepare(VsrPrepareMessage {
                prepare: prepare.clone(),
                replica: NodeId(1)
            })
            .is_err()
    );
    let mut wrong_primary = NetworkVsrTransport::new(
        NodeId(2),
        &cluster(),
        identity(&root, 2),
        peers.clone(),
        Duration::from_millis(800),
    )
    .unwrap();
    assert!(
        wrong_primary
            .acknowledge_prepare(VsrPrepareMessage {
                prepare: prepare.clone(),
                replica: NodeId(1)
            })
            .is_err()
    );
    let mut stale = prepare.clone();
    stale.view = Epoch(0);
    assert!(
        transport(&root, peers.clone())
            .acknowledge_prepare(VsrPrepareMessage {
                prepare: stale,
                replica: NodeId(1)
            })
            .is_err()
    );
    assert_eq!(
        transport(&root, peers)
            .committed_tail(NodeId(1), PlacementGroupId(1))
            .unwrap(),
        LogIndex(0)
    );
    drop(peer);
    let recovered = cell(&root, 1);
    assert!(recovered.prepared(LogIndex(1)).is_none());
    drop(recovered);
    drop(leader);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn frame_limits_and_absolute_deadlines_reject_unbounded_requests() {
    assert!(wire::read_frame(&mut u32::MAX.to_le_bytes().as_slice()).is_err());
    assert!(wire::read_frame(&mut 1_u32.to_le_bytes().as_slice()).is_err());
    let root = root("timeout");
    provision(&root);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let mut peers = endpoints(&root);
    peers.get_mut(&NodeId(1)).unwrap().address = listener.local_addr().unwrap();
    let started = Instant::now();
    let mut network = NetworkVsrTransport::new(
        NodeId(0),
        &cluster(),
        identity(&root, 0),
        peers,
        Duration::from_millis(100),
    )
    .unwrap();
    assert!(
        network
            .committed_tail(NodeId(1), PlacementGroupId(1))
            .is_err()
    );
    assert!(started.elapsed() < Duration::from_secs(2));
    std::fs::remove_dir_all(root).unwrap();
}

struct LostCommitReplies {
    inner: NetworkVsrTransport,
    lose: bool,
}
impl VsrTransport for LostCommitReplies {
    fn acknowledge_prepare(
        &mut self,
        message: VsrPrepareMessage,
    ) -> Result<NodeId, VsrTransportError> {
        self.inner.acknowledge_prepare(message)
    }
    fn acknowledge_commit(
        &mut self,
        replica: NodeId,
        commit: VsrCommit,
    ) -> Result<NodeId, VsrTransportError> {
        let result = self.inner.acknowledge_commit(replica, commit)?;
        if self.lose {
            Err(VsrTransportError {
                replica,
                reason: "injected lost commit response after follower fsync".into(),
            })
        } else {
            Ok(result)
        }
    }
    fn committed_tail(
        &mut self,
        replica: NodeId,
        pg: PlacementGroupId,
    ) -> Result<LogIndex, VsrTransportError> {
        self.inner.committed_tail(replica, pg)
    }
    fn transfer_committed(
        &mut self,
        replica: NodeId,
        entry: VsrCommittedEntry,
    ) -> Result<(), VsrTransportError> {
        self.inner.transfer_committed(replica, entry)
    }
}

#[test]
fn protocol_certificate_waits_for_durable_peer_commits_and_retry_is_idempotent() {
    use rsf_protocol::{ClientRequestId, ConsistencyMode, FabricOperation, VsrBackedFabricRouter};
    let root = root("certificate");
    provision(&root);
    let first = PeerProcess::spawn(&root, 1);
    let second = PeerProcess::spawn(&root, 2);
    let mut peers = endpoints(&root);
    peers.get_mut(&NodeId(1)).unwrap().address = first.address;
    peers.get_mut(&NodeId(2)).unwrap().address = second.address;
    let network = LostCommitReplies {
        inner: transport(&root, peers.clone()),
        lose: true,
    };
    let mut router = VsrBackedFabricRouter::open_with_transport(
        root.join("protocol"),
        [PlacementGroupId(1)],
        Epoch(1),
        NodeId(0),
        [NodeId(0), NodeId(1), NodeId(2)],
        network,
    )
    .unwrap();
    let request = ClientRequestId::new("client", 7);
    let operation = FabricOperation::KvMutation {
        key: b"tenant/a".to_vec(),
    };
    assert!(
        router
            .commit_request(
                PlacementGroupId(1),
                request.clone(),
                ConsistencyMode::Strict,
                operation.clone()
            )
            .is_err()
    );
    assert!(
        router
            .recovered_certificates(PlacementGroupId(1))
            .unwrap()
            .is_empty()
    );
    drop(router);
    let mut router = VsrBackedFabricRouter::open_with_transport(
        root.join("protocol"),
        [PlacementGroupId(1)],
        Epoch(1),
        NodeId(0),
        [NodeId(0), NodeId(1), NodeId(2)],
        transport(&root, peers),
    )
    .unwrap();
    let committed = router
        .commit_request(
            PlacementGroupId(1),
            request.clone(),
            ConsistencyMode::Strict,
            operation.clone(),
        )
        .unwrap();
    assert_eq!(committed.certificate.index, LogIndex(1));
    let retry = router
        .commit_request(
            PlacementGroupId(1),
            request,
            ConsistencyMode::Strict,
            operation,
        )
        .unwrap();
    assert!(retry.replayed);
    assert_eq!(retry.certificate, committed.certificate);
    assert_eq!(
        router
            .recovered_certificates(PlacementGroupId(1))
            .unwrap()
            .len(),
        1
    );
    drop(router);
    drop(first);
    drop(second);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn equal_tail_with_conflicting_committed_command_cannot_satisfy_retry_quorum() {
    let root = root("conflicting-tail");
    provision(&root);
    let mut leader = cell(&root, 0);
    let good = prepare(&mut leader, b"authoritative command");
    leader.acknowledge(NodeId(1), Epoch(1), good.index);
    assert!(matches!(
        leader.commit(NodeId(0), good.index).unwrap(),
        VsrDecision::Committed(_)
    ));
    let mut divergent = cell(&root, 1);
    let bad = prepare(&mut divergent, b"conflicting command");
    divergent.acknowledge(NodeId(1), Epoch(1), bad.index);
    assert!(matches!(
        divergent.commit(NodeId(0), bad.index).unwrap(),
        VsrDecision::Committed(_)
    ));
    drop(divergent);
    let peer = PeerProcess::spawn(&root, 1);
    let mut peers = endpoints(&root);
    peers.get_mut(&NodeId(1)).unwrap().address = peer.address;
    let mut network = transport(&root, peers);
    assert!(
        leader
            .replicate_with_transport(NodeId(0), good.index, &mut network)
            .is_err()
    );
    drop(peer);
    drop(leader);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn protocol_application_commands_survive_independent_peer_processes() {
    use rsf_protocol::{
        ApplicationPayload, ApplicationSurface, ClientRequestId, ConsistencyMode, FabricOperation,
        ReplicatedFabricCommand, VsrBackedFabricRouter,
    };
    let root = root("application-command");
    provision(&root);
    let first = PeerProcess::spawn(&root, 1);
    let second = PeerProcess::spawn(&root, 2);
    let mut peers = endpoints(&root);
    peers.get_mut(&NodeId(1)).unwrap().address = first.address;
    peers.get_mut(&NodeId(2)).unwrap().address = second.address;
    let mut router = VsrBackedFabricRouter::open_with_transport(
        root.join("protocol"),
        [PlacementGroupId(1)],
        Epoch(1),
        NodeId(0),
        [NodeId(0), NodeId(1), NodeId(2)],
        transport(&root, peers.clone()),
    )
    .unwrap();
    let operation = FabricOperation::KvMutation {
        key: b"key".to_vec(),
    };
    let application = ApplicationPayload {
        namespace: "tenant/database".into(),
        surface: ApplicationSurface::Kv,
        bytes: b"full mutation\0value\xff".to_vec(),
    };
    let request = ClientRequestId::new("application", 9);
    let result = router
        .commit_application(
            PlacementGroupId(1),
            request.clone(),
            ConsistencyMode::Strict,
            operation.clone(),
            application.clone(),
        )
        .unwrap();
    drop(first);
    drop(second);
    drop(router);
    for node in [1, 2] {
        let follower = cell(&root, node);
        let VsrOperation::ReplicatedCommand { payload, .. } = follower
            .committed_operation(result.certificate.index)
            .unwrap()
        else {
            panic!("expected complete command");
        };
        let command = ReplicatedFabricCommand::decode(payload).unwrap();
        assert_eq!(command.application, application);
        assert_eq!(command.request, request);
        assert_eq!(command.certificate(), result.certificate);
    }
    // A persisted local receipt is insufficient to acknowledge a retry without
    // the configured network quorum.
    let mut router = VsrBackedFabricRouter::open_with_transport(
        root.join("protocol"),
        [PlacementGroupId(1)],
        Epoch(1),
        NodeId(0),
        [NodeId(0), NodeId(1), NodeId(2)],
        transport(&root, peers.clone()),
    )
    .unwrap();
    assert!(
        router
            .commit_application(
                PlacementGroupId(1),
                request.clone(),
                ConsistencyMode::Strict,
                operation.clone(),
                application.clone()
            )
            .is_err()
    );
    drop(router);
    let first = PeerProcess::spawn(&root, 1);
    peers.get_mut(&NodeId(1)).unwrap().address = first.address;
    let mut router = VsrBackedFabricRouter::open_with_transport(
        root.join("protocol"),
        [PlacementGroupId(1)],
        Epoch(1),
        NodeId(0),
        [NodeId(0), NodeId(1), NodeId(2)],
        transport(&root, peers),
    )
    .unwrap();
    let retry = router
        .commit_application(
            PlacementGroupId(1),
            request,
            ConsistencyMode::Strict,
            operation,
            application,
        )
        .unwrap();
    assert!(retry.replayed);
    assert_eq!(retry.certificate, result.certificate);
    assert_eq!(router.committed_application_commands().len(), 1);
}

#[test]
fn authenticated_primary_probe_binds_view_voters_and_tip_without_writes() {
    let root = root("primary-probe");
    provision(&root);
    let peer = PeerProcess::spawn(&root, 1);
    let mut peers = endpoints(&root);
    peers.get_mut(&NodeId(1)).unwrap().address = peer.address;
    let mut network = transport(&root, peers.clone());
    let mut leader = cell(&root, 0);
    let empty = leader.primary_probe().unwrap();
    network.confirm_primary(NodeId(1), &empty).unwrap();
    let mut changed = empty.clone();
    changed.view = Epoch(2);
    assert!(network.confirm_primary(NodeId(1), &changed).is_err());
    changed = empty.clone();
    changed.voters.pop();
    assert!(network.confirm_primary(NodeId(1), &changed).is_err());
    changed = empty.clone();
    changed.primary = NodeId(1);
    assert!(network.confirm_primary(NodeId(1), &changed).is_err());
    let prepare = prepare(&mut leader, b"probe-data");
    leader
        .replicate_with_transport(NodeId(0), prepare.index, &mut network)
        .unwrap();
    assert!(network.confirm_primary(NodeId(1), &empty).is_err());
    let current = leader.primary_probe().unwrap();
    network.confirm_primary(NodeId(1), &current).unwrap();
    changed = current.clone();
    changed.committed_tip = rsf_core::Digest::blake3_bytes(b"other data at same height");
    assert!(network.confirm_primary(NodeId(1), &changed).is_err());
    let bytes = current.encode().unwrap();
    for payload in [&bytes[..12], &[bytes.clone(), vec![0]].concat()] {
        assert!(network.request(NodeId(1), PRIMARY_PROBE, payload).is_err());
    }
    drop(peer);
    {
        let mut follower = cell(&root, 1);
        assert_eq!(follower.primary_probe().unwrap(), current);
        assert_eq!(follower.committed_count(), 1);
        assert_eq!(follower.next_index(), LogIndex(2));
        assert!(matches!(
            follower.view_change(Epoch(2), NodeId(0)),
            VsrDecision::ViewChanged { .. }
        ));
    }
    let peer = PeerProcess::spawn(&root, 1);
    peers.get_mut(&NodeId(1)).unwrap().address = peer.address;
    let mut network = transport(&root, peers);
    // The same primary and committed height cannot mask a newer remote view.
    assert!(network.confirm_primary(NodeId(1), &current).is_err());
    drop(peer);
}

#[test]
fn prefix_repair_after_voter_removal_preserves_historical_certificate_authority() {
    let root = root("historical-repair");
    provision(&root);
    let first = PeerProcess::spawn(&root, 1);
    let second = PeerProcess::spawn(&root, 2);
    let mut peers = endpoints(&root);
    peers.get_mut(&NodeId(1)).unwrap().address = first.address;
    peers.get_mut(&NodeId(2)).unwrap().address = second.address;
    let mut network = transport(&root, peers.clone());
    let mut leader = cell(&root, 0);
    let one = prepare(&mut leader, b"before membership change");
    leader
        .replicate_with_transport(NodeId(0), one.index, &mut network)
        .unwrap();
    let original = leader.committed_prefix().unwrap();
    assert!(original[0].commit.certificate.signers.contains(&NodeId(1)));
    drop(first);
    drop(second);
    // This exercises recovery after installed configurations, not a distributed
    // membership decision. Quorum-certified network installation is separate work.
    for node in [0, 2] {
        if node == 0 {
            assert!(matches!(
                leader.reconfigure(Epoch(2), NodeId(0), [NodeId(0), NodeId(2)]),
                VsrDecision::MembershipChanged { .. }
            ));
        } else {
            let mut follower = cell(&root, node);
            assert!(matches!(
                follower.reconfigure(Epoch(2), NodeId(0), [NodeId(0), NodeId(2)]),
                VsrDecision::MembershipChanged { .. }
            ));
        }
    }
    drop(leader);
    let mut leader = cell(&root, 0);
    let second = PeerProcess::spawn(&root, 2);
    peers.get_mut(&NodeId(2)).unwrap().address = second.address;
    let mut network = transport(&root, peers);
    // Repair validates the common tip using its original three-voter view.
    leader
        .replicate_with_transport(NodeId(0), one.index, &mut network)
        .unwrap();
    let two = prepare(&mut leader, b"after membership change");
    leader
        .replicate_with_transport(NodeId(0), two.index, &mut network)
        .unwrap();
    network
        .confirm_primary(NodeId(2), &leader.primary_probe().unwrap())
        .unwrap();
    drop(second);
    let mut recovered = cell(&root, 2);
    assert_eq!(recovered.view(), Epoch(2));
    assert_eq!(recovered.replicas(), [NodeId(0), NodeId(2)]);
    assert_eq!(
        recovered.committed_prefix().unwrap(),
        leader.committed_prefix().unwrap()
    );
    assert_eq!(recovered.catch_up_from(&original).unwrap(), 0);
}

struct LearnerPeer {
    address: SocketAddr,
    stop: Arc<std::sync::atomic::AtomicBool>,
    worker: Option<std::thread::JoinHandle<()>>,
}
impl LearnerPeer {
    fn start(
        root: &Path,
        peers: BTreeMap<NodeId, PeerEndpoint>,
        applied: Arc<std::sync::atomic::AtomicUsize>,
    ) -> Self {
        Self::start_with_reply_loss(root, peers, applied, false)
    }
    fn start_with_reply_loss(
        root: &Path,
        peers: BTreeMap<NodeId, PeerEndpoint>,
        applied: Arc<std::sync::atomic::AtomicUsize>,
        drop_first_reply: bool,
    ) -> Self {
        let learner = rsf_vsr::LearnerHistoryStore::open(
            root.join("learner-3"),
            PlacementGroupId(1),
            NodeId(3),
            NodeId(0),
        )
        .unwrap();
        let mut server = PeerReplicaServer::with_learners(
            NodeId(3),
            &cluster(),
            identity(root, 3),
            peers,
            BTreeMap::new(),
            [(PlacementGroupId(1), learner)].into_iter().collect(),
            Duration::from_secs(2),
        )
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = Arc::clone(&stop);
        let worker = std::thread::spawn(move || {
            let mut lost_reply = false;
            while !flag.load(std::sync::atomic::Ordering::Relaxed) {
                match listener.accept() {
                    Ok((socket, _)) => {
                        let shutdown = socket.try_clone().unwrap();
                        let _ = server.serve_connection_with_apply(socket, &mut |_| {
                            applied.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            if drop_first_reply && !lost_reply {
                                shutdown.shutdown(std::net::Shutdown::Both).unwrap();
                                lost_reply = true;
                            }
                            Ok(())
                        });
                    }
                    Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(1))
                    }
                    Err(err) => panic!("learner listener: {err}"),
                }
            }
        });
        Self {
            address,
            stop,
            worker: Some(worker),
        }
    }
}
impl Drop for LearnerPeer {
    fn drop(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::Relaxed);
        self.worker.take().unwrap().join().unwrap();
    }
}

#[test]
fn learner_finish_resolves_a_lost_tls_reply_once_and_materializes_again_after_restart() {
    let root = root("learner-lost-reply");
    provision(&root);
    let voter = PeerProcess::spawn(&root, 1);
    let mut peers = endpoints(&root);
    peers.get_mut(&NodeId(1)).unwrap().address = voter.address;
    peers.insert(
        NodeId(3),
        PeerEndpoint {
            address: "127.0.0.1:1".parse().unwrap(),
            server_name: "localhost".into(),
            fingerprint: identity(&root, 3).fingerprint,
        },
    );
    let applied = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let learner =
        LearnerPeer::start_with_reply_loss(&root, peers.clone(), Arc::clone(&applied), true);
    peers.get_mut(&NodeId(3)).unwrap().address = learner.address;
    let mut network = transport(&root, peers.clone());
    let mut leader = cell(&root, 0);
    let prepared = prepare(&mut leader, b"durable before lost learner finish reply");
    leader
        .replicate_with_transport(NodeId(0), prepared.index, &mut network)
        .unwrap();
    assert_eq!(
        network.sync_learner(NodeId(3), &mut leader).unwrap(),
        prepared.index
    );
    assert_eq!(applied.load(std::sync::atomic::Ordering::Relaxed), 1);
    assert_eq!(
        network.sync_learner(NodeId(3), &mut leader).unwrap(),
        prepared.index
    );
    assert_eq!(applied.load(std::sync::atomic::Ordering::Relaxed), 1);
    drop(learner);
    let learner = LearnerPeer::start(&root, peers.clone(), Arc::clone(&applied));
    peers.get_mut(&NodeId(3)).unwrap().address = learner.address;
    let mut network = transport(&root, peers);
    assert_eq!(
        network.sync_learner(NodeId(3), &mut leader).unwrap(),
        prepared.index
    );
    assert_eq!(applied.load(std::sync::atomic::Ordering::Relaxed), 2);
    assert!(
        network
            .confirm_primary(NodeId(3), &leader.primary_probe().unwrap())
            .is_err()
    );
    drop(learner);
    drop(voter);
}

#[test]
fn authenticated_learner_transfer_resumes_and_never_acknowledges_voting_requests() {
    let root = root("learner-transfer");
    provision(&root);
    let first = PeerProcess::spawn(&root, 1);
    let mut peers = endpoints(&root);
    peers.get_mut(&NodeId(1)).unwrap().address = first.address;
    peers.insert(
        NodeId(3),
        PeerEndpoint {
            address: "127.0.0.1:1".parse().unwrap(),
            server_name: "localhost".into(),
            fingerprint: identity(&root, 3).fingerprint,
        },
    );
    let applied = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let learner = LearnerPeer::start(&root, peers.clone(), Arc::clone(&applied));
    peers.get_mut(&NodeId(3)).unwrap().address = learner.address;
    let mut network = transport(&root, peers.clone());
    let mut leader = cell(&root, 0);
    let one = prepare(&mut leader, b"learner durable payload");
    leader
        .replicate_with_transport(NodeId(0), one.index, &mut network)
        .unwrap();
    leader
        .remember_client_request(rsf_vsr::VsrClientRequestId::new("client", 1), one.index)
        .unwrap();
    let snapshot = leader.history_snapshot().unwrap();
    let id = snapshot.descriptor.id().unwrap();
    assert_eq!(
        network
            .request(
                NodeId(3),
                HISTORY_BEGIN,
                &snapshot.descriptor.encode().unwrap()
            )
            .unwrap(),
        LogIndex(0)
    );
    let mut first_record = 1_u64.to_le_bytes().to_vec();
    first_record.extend_from_slice(&id);
    first_record.extend_from_slice(&0_u64.to_le_bytes());
    first_record.extend_from_slice(&snapshot.records[0].encode().unwrap());
    assert_eq!(
        network
            .request(NodeId(3), HISTORY_RECORD, &first_record)
            .unwrap(),
        LogIndex(1)
    );
    drop(learner);
    let learner = LearnerPeer::start(&root, peers.clone(), Arc::clone(&applied));
    peers.get_mut(&NodeId(3)).unwrap().address = learner.address;
    let mut network = transport(&root, peers.clone());
    assert_eq!(
        network
            .request(
                NodeId(3),
                HISTORY_BEGIN,
                &snapshot.descriptor.encode().unwrap()
            )
            .unwrap(),
        LogIndex(1)
    );
    assert_eq!(
        network.sync_learner(NodeId(3), &mut leader).unwrap(),
        LogIndex(1)
    );
    assert_eq!(applied.load(std::sync::atomic::Ordering::Relaxed), 1);
    assert!(
        network
            .confirm_primary(NodeId(3), &leader.primary_probe().unwrap())
            .is_err()
    );
    assert!(
        network
            .acknowledge_prepare(VsrPrepareMessage {
                replica: NodeId(3),
                prepare: one.clone()
            })
            .is_err()
    );
    assert!(
        network
            .acknowledge_commit(
                NodeId(3),
                leader.committed_prefix().unwrap()[0].commit.clone()
            )
            .is_err()
    );
    assert!(
        network
            .committed_tail(NodeId(3), PlacementGroupId(1))
            .is_err()
    );
    let foreign = NetworkVsrTransport::new(
        NodeId(1),
        &cluster(),
        identity(&root, 1),
        peers,
        Duration::from_secs(2),
    )
    .unwrap();
    assert!(
        foreign
            .request(
                NodeId(3),
                HISTORY_BEGIN,
                &snapshot.descriptor.encode().unwrap()
            )
            .is_err()
    );
    drop(first);
    // The learner is up and caught up, but it cannot replace a lost voter quorum.
    assert!(
        network
            .sync_learner(NodeId(3), &mut leader)
            .unwrap_err()
            .reason
            .contains("lacks current primary quorum")
    );
    assert_eq!(applied.load(std::sync::atomic::Ordering::Relaxed), 1);
    drop(learner);
    let recovered = rsf_vsr::LearnerHistoryStore::open(
        root.join("learner-3"),
        PlacementGroupId(1),
        NodeId(3),
        NodeId(0),
    )
    .unwrap();
    assert_eq!(
        recovered.committed_entries().unwrap(),
        leader.committed_prefix().unwrap()
    );
}

#[test]
fn real_peer_view_change_reports_fence_old_primary_and_survive_restart() {
    use rsf_vsr::{VsrViewChangeReport, VsrViewChangeRequest};
    let root = root("view-change");
    provision(&root);
    let peer = PeerProcess::spawn(&root, 2);
    let mut peers = endpoints(&root);
    peers.get_mut(&NodeId(2)).unwrap().address = peer.address;
    let mut old_transport = transport(&root, peers.clone());
    let mut old_primary = cell(&root, 0);
    let committed = prepare(&mut old_primary, b"committed-before-failover");
    old_primary
        .replicate_with_transport(NodeId(0), committed.index, &mut old_transport)
        .unwrap();
    let pending = prepare(&mut old_primary, b"accepted-without-commit");
    old_transport
        .acknowledge_prepare(VsrPrepareMessage {
            prepare: pending.clone(),
            replica: NodeId(2),
        })
        .unwrap();
    drop(old_primary);
    let request = VsrViewChangeRequest {
        pg: PlacementGroupId(1),
        previous_view: Epoch(1),
        target_view: Epoch(2),
        candidate: NodeId(1),
        voters: vec![NodeId(0), NodeId(1), NodeId(2)],
    };
    let mut candidate = cell(&root, 1);
    let mut candidate_transport = NetworkVsrTransport::new(
        NodeId(1),
        &cluster(),
        identity(&root, 1),
        peers.clone(),
        Duration::from_millis(800),
    )
    .unwrap();
    let snapshots = candidate_transport
        .collect_view_change(&mut candidate, &request)
        .unwrap();
    assert_eq!(snapshots.len(), 2);
    let remote = snapshots
        .iter()
        .find(|snapshot| snapshot.report.replica == NodeId(2))
        .unwrap();
    assert_eq!(remote.report.accepted_tail, LogIndex(2));
    assert_eq!(remote.report.history.probe.committed_tail, LogIndex(1));
    assert!(
        remote
            .records
            .iter()
            .any(|record| record.payload == pending.encode().unwrap())
    );
    let report = remote.report.clone();
    let records = remote.records.clone();
    assert_eq!(
        VsrViewChangeReport::decode(&report.encode().unwrap()).unwrap(),
        report
    );
    // The certificate from the old view cannot cross the durable fence, even
    // if its prepare was already acknowledged before the candidate collected.
    let mut old_primary = cell(&root, 0);
    assert!(
        old_primary
            .replicate_with_transport(NodeId(0), pending.index, &mut old_transport)
            .is_err()
    );
    let probe = old_primary.primary_probe().unwrap();
    assert!(old_transport.confirm_primary(NodeId(2), &probe).is_err());
    let conflict = VsrViewChangeRequest {
        candidate: NodeId(0),
        ..request.clone()
    };
    assert!(
        old_transport
            .fetch_view_change(NodeId(2), &conflict)
            .is_err()
    );
    // Fetched records are bound to the exact frozen snapshot and voter.
    let mut wrong = report.clone();
    wrong.accepted_tail = LogIndex(3);
    let mut query = wrong.encode().unwrap();
    query.extend_from_slice(&0u64.to_le_bytes());
    assert!(
        candidate_transport
            .exchange_io(NodeId(2), VIEW_CHANGE_RECORD, &query)
            .is_err()
    );
    drop(peer);
    let peer = PeerProcess::spawn(&root, 2);
    peers.get_mut(&NodeId(2)).unwrap().address = peer.address;
    candidate_transport = NetworkVsrTransport::new(
        NodeId(1),
        &cluster(),
        identity(&root, 1),
        peers,
        Duration::from_millis(800),
    )
    .unwrap();
    // A record fetch alone after restart reconstructs the immutable snapshot.
    let mut query = report.encode().unwrap();
    query.extend_from_slice(&1u64.to_le_bytes());
    assert_eq!(
        candidate_transport
            .exchange_io(NodeId(2), VIEW_CHANGE_RECORD, &query)
            .unwrap(),
        records[1].encode().unwrap()
    );
    let recovered = candidate_transport
        .fetch_view_change(NodeId(2), &request)
        .unwrap();
    assert_eq!(recovered.report, report);
    assert_eq!(recovered.records, records);
    drop(peer);
    assert!(
        candidate_transport
            .collect_view_change(&mut candidate, &request)
            .is_err()
    );
    assert!(
        candidate
            .prepare(
                NodeId(1),
                VsrOperation::ProtocolOperation {
                    operation_id: OperationId(999)
                },
                1
            )
            .is_err()
    );
    assert_eq!(candidate.view(), Epoch(1));
    assert_eq!(candidate.primary(), NodeId(0));
}

#[test]
fn selected_installed_view_replicates_after_primary_loss_and_rejects_the_old_primary() {
    use rsf_vsr::VsrViewChangeRequest;
    let root = root("installed-view");
    provision(&root);
    let peer = PeerProcess::spawn(&root, 2);
    let mut peers = endpoints(&root);
    peers.get_mut(&NodeId(2)).unwrap().address = peer.address;
    let mut old_transport = transport(&root, peers.clone());
    let mut primary = cell(&root, 0);
    let committed = prepare(&mut primary, b"acknowledged before primary loss");
    primary
        .replicate_with_transport(NodeId(0), committed.index, &mut old_transport)
        .unwrap();
    let pending = prepare(&mut primary, b"accepted before primary loss");
    old_transport
        .acknowledge_prepare(VsrPrepareMessage {
            prepare: pending,
            replica: NodeId(2),
        })
        .unwrap();
    drop(primary);
    let request = VsrViewChangeRequest {
        pg: PlacementGroupId(1),
        previous_view: Epoch(1),
        target_view: Epoch(2),
        candidate: NodeId(1),
        voters: vec![NodeId(0), NodeId(1), NodeId(2)],
    };
    let mut candidate = cell(&root, 1);
    let mut new_transport = NetworkVsrTransport::new(
        NodeId(1),
        &cluster(),
        identity(&root, 1),
        peers.clone(),
        Duration::from_millis(800),
    )
    .unwrap();
    let (input, snapshots) = new_transport
        .prepare_installation_input(&mut candidate, &request)
        .unwrap();
    new_transport
        .stage_installation_input(&mut candidate, &input, &snapshots)
        .unwrap();
    // The recipient remains running throughout publication; the candidate can
    // restart and recover the complete quorum input without recollection.
    drop(candidate);
    let mut candidate = cell(&root, 1);
    assert_eq!(
        new_transport
            .resume_view_installation(&mut candidate, &mut |_| Ok(()))
            .unwrap(),
        Epoch(2)
    );
    assert_eq!(
        new_transport
            .resume_view_installation(&mut candidate, &mut |_| Ok(()))
            .unwrap(),
        Epoch(2)
    );
    candidate.reprepare(NodeId(1), LogIndex(2)).unwrap();
    candidate
        .replicate_with_transport(NodeId(1), LogIndex(2), &mut new_transport)
        .unwrap();
    assert_eq!(candidate.committed_tail(), LogIndex(2));
    let mut stale = cell(&root, 0);
    assert!(
        stale
            .replicate_with_transport(NodeId(0), LogIndex(2), &mut old_transport)
            .is_err()
    );
    drop(candidate);
    drop(peer);
    let candidate = cell(&root, 1);
    let follower = cell(&root, 2);
    assert_eq!(candidate.view(), Epoch(2));
    assert_eq!(
        candidate.committed_prefix().unwrap(),
        follower.committed_prefix().unwrap()
    );
    assert_eq!(candidate.committed_count(), 2);
}

#[test]
fn tls_collection_recovers_after_only_the_candidate_published_the_previous_view() {
    use rsf_vsr::VsrViewChangeRequest;
    let root = root("mixed-views");
    provision(&root);
    let peer = PeerProcess::spawn(&root, 2);
    let mut peers = endpoints(&root);
    peers.get_mut(&NodeId(2)).unwrap().address = peer.address;
    let mut old_transport = transport(&root, peers.clone());
    let mut old = cell(&root, 0);
    let prepared = prepare(&mut old, b"survives interrupted installation");
    old.replicate_with_transport(NodeId(0), prepared.index, &mut old_transport)
        .unwrap();
    drop(old);
    let mut candidate = cell(&root, 1);
    let mut new_transport = NetworkVsrTransport::new(
        NodeId(1),
        &cluster(),
        identity(&root, 1),
        peers.clone(),
        Duration::from_millis(800),
    )
    .unwrap();
    let request = VsrViewChangeRequest {
        pg: PlacementGroupId(1),
        previous_view: Epoch(1),
        target_view: Epoch(2),
        candidate: NodeId(1),
        voters: vec![NodeId(0), NodeId(1), NodeId(2)],
    };
    let plan = new_transport
        .prepare_view_installation(&mut candidate, &request)
        .unwrap();
    candidate.install_view(&plan).unwrap();
    drop(candidate); // Restart after only the candidate activated view 2.
    let mut candidate = cell(&root, 1);
    assert_eq!(candidate.view(), Epoch(2));
    let request = VsrViewChangeRequest {
        target_view: Epoch(3),
        ..request
    };
    let snapshots = new_transport
        .collect_view_change(&mut candidate, &request)
        .unwrap();
    assert!(
        snapshots
            .iter()
            .any(|snapshot| snapshot.report.history.probe.view == Epoch(1))
    );
    assert!(
        snapshots
            .iter()
            .any(|snapshot| snapshot.report.history.probe.view == Epoch(2))
    );
    let plan = rsf_vsr::VsrViewInstallation::select(&snapshots).unwrap();
    candidate.install_view(&plan).unwrap();
    drop(peer);
    let mut follower = cell(&root, 2);
    follower.install_view(&plan).unwrap();
    drop(follower);
    let peer = PeerProcess::spawn(&root, 2);
    peers.get_mut(&NodeId(2)).unwrap().address = peer.address;
    new_transport = NetworkVsrTransport::new(
        NodeId(1),
        &cluster(),
        identity(&root, 1),
        peers,
        Duration::from_millis(800),
    )
    .unwrap();
    let VsrDecision::Prepared(prepared) = candidate
        .prepare(
            NodeId(1),
            VsrOperation::ReplicatedCommand {
                operation_id: OperationId(2),
                payload: b"new view write".to_vec(),
            },
            14,
        )
        .unwrap()
    else {
        panic!("new prepare");
    };
    candidate
        .replicate_with_transport(NodeId(1), prepared.index, &mut new_transport)
        .unwrap();
    drop(candidate);
    drop(peer);
    let candidate = cell(&root, 1);
    let follower = cell(&root, 2);
    assert_eq!(candidate.view(), Epoch(3));
    assert_eq!(
        candidate.committed_prefix().unwrap(),
        follower.committed_prefix().unwrap()
    );
    assert_eq!(candidate.committed_count(), 2);
}

#[test]
fn remote_installation_resumes_after_recipient_kill_and_candidate_publication_restart() {
    use rsf_vsr::VsrViewChangeRequest;
    let root = root("remote-install-restart");
    provision(&root);
    let peer = PeerProcess::spawn(&root, 2);
    let mut peers = endpoints(&root);
    peers.get_mut(&NodeId(2)).unwrap().address = peer.address;
    let mut old_transport = transport(&root, peers.clone());
    let mut primary = cell(&root, 0);
    let prepared = prepare(&mut primary, b"survive interrupted view upload");
    primary
        .replicate_with_transport(NodeId(0), prepared.index, &mut old_transport)
        .unwrap();
    drop(primary);
    let request = VsrViewChangeRequest {
        pg: PlacementGroupId(1),
        previous_view: Epoch(1),
        target_view: Epoch(2),
        candidate: NodeId(1),
        voters: vec![NodeId(0), NodeId(1), NodeId(2)],
    };
    let mut candidate = cell(&root, 1);
    let mut network = NetworkVsrTransport::new(
        NodeId(1),
        &cluster(),
        identity(&root, 1),
        peers.clone(),
        Duration::from_millis(800),
    )
    .unwrap();
    let (input, snapshots) = network
        .prepare_installation_input(&mut candidate, &request)
        .unwrap();
    network
        .stage_installation_input(&mut candidate, &input, &snapshots)
        .unwrap();
    let id = input.id().unwrap();
    let snapshots = candidate.installation_snapshots(id).unwrap();
    assert_eq!(
        network
            .request_io(NodeId(2), INSTALL_BEGIN, &input.encode().unwrap())
            .unwrap(),
        0
    );
    let mut prefix = PlacementGroupId(1).0.to_le_bytes().to_vec();
    prefix.extend_from_slice(&id);
    let records = snapshots
        .iter()
        .flat_map(|s| s.records.iter())
        .collect::<Vec<_>>();
    let halfway = records.len() / 2;
    for (ordinal, record) in records.iter().enumerate().take(halfway) {
        let mut payload = prefix.clone();
        payload.extend_from_slice(&(ordinal as u64).to_le_bytes());
        payload.extend_from_slice(&record.encode().unwrap());
        assert_eq!(
            network
                .request_io(NodeId(2), INSTALL_RECORD, &payload)
                .unwrap(),
            ordinal as u64 + 1
        );
    }
    drop(peer); // OS kill: the recipient cannot run Rust destructors.
    // Local publication alone must never produce a successful takeover result.
    assert!(
        network
            .resume_view_installation(&mut candidate, &mut |_| Ok(()))
            .is_err()
    );
    assert_eq!(candidate.view(), Epoch(2));
    drop(candidate);
    let peer = PeerProcess::spawn(&root, 2);
    peers.get_mut(&NodeId(2)).unwrap().address = peer.address;
    network = NetworkVsrTransport::new(
        NodeId(1),
        &cluster(),
        identity(&root, 1),
        peers,
        Duration::from_millis(800),
    )
    .unwrap();
    let mut candidate = cell(&root, 1);
    assert_eq!(
        network
            .request_io(NodeId(2), INSTALL_BEGIN, &input.encode().unwrap())
            .unwrap(),
        halfway as u64
    );
    assert_eq!(
        network
            .resume_view_installation(&mut candidate, &mut |_| Ok(()))
            .unwrap(),
        Epoch(2)
    );
    assert_eq!(candidate.committed_tail(), LogIndex(1));
    assert_eq!(
        network
            .resume_view_installation(&mut candidate, &mut |_| Ok(()))
            .unwrap(),
        Epoch(2)
    );
    drop(candidate);
    drop(peer);
    let follower = cell(&root, 2);
    assert_eq!(follower.view(), Epoch(2));
    assert_eq!(follower.committed_tail(), LogIndex(1));
    drop(follower);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn installed_peer_requires_successful_materialization_before_normal_traffic() {
    use rsf_vsr::{VsrInstallationInput, VsrViewChangeRequest};
    let root = root("installation-materializer");
    provision(&root);
    let mut primary = cell(&root, 0);
    let mut candidate = cell(&root, 1);
    let prepared = prepare(&mut primary, b"apply before serving");
    candidate
        .receive_prepare(NodeId(1), NodeId(0), prepared.clone())
        .unwrap();
    primary.acknowledge(NodeId(1), Epoch(1), prepared.index);
    let VsrDecision::Committed(commit) = primary.commit(NodeId(0), prepared.index).unwrap() else {
        panic!("commit")
    };
    candidate
        .receive_commit(NodeId(1), NodeId(0), commit)
        .unwrap();
    let request = VsrViewChangeRequest {
        pg: PlacementGroupId(1),
        previous_view: Epoch(1),
        target_view: Epoch(2),
        candidate: NodeId(1),
        voters: vec![NodeId(0), NodeId(1), NodeId(2)],
    };
    let snapshots = vec![
        primary
            .begin_view_change(NodeId(0), NodeId(1), request.clone())
            .unwrap(),
        candidate
            .begin_view_change(NodeId(1), NodeId(1), request)
            .unwrap(),
    ];
    let input = VsrInstallationInput::from_snapshots(&snapshots).unwrap();
    let id = input.id().unwrap();
    let mut server = PeerReplicaServer::new(
        NodeId(2),
        &cluster(),
        identity(&root, 2),
        endpoints(&root),
        [(PlacementGroupId(1), cell(&root, 2))].into(),
        Duration::from_millis(800),
    )
    .unwrap();
    assert!(
        server
            .dispatch_installation(
                NodeId(0),
                INSTALL_BEGIN,
                &input.encode().unwrap(),
                &mut |_| Ok(())
            )
            .is_err()
    );
    server
        .dispatch_installation(
            NodeId(1),
            INSTALL_BEGIN,
            &input.encode().unwrap(),
            &mut |_| Ok(()),
        )
        .unwrap();
    let mut prefix = PlacementGroupId(1).0.to_le_bytes().to_vec();
    prefix.extend_from_slice(&id);
    for (ordinal, record) in snapshots.iter().flat_map(|s| s.records.iter()).enumerate() {
        let mut payload = prefix.clone();
        payload.extend_from_slice(&(ordinal as u64).to_le_bytes());
        payload.extend_from_slice(&record.encode().unwrap());
        server
            .dispatch_installation(NodeId(1), INSTALL_RECORD, &payload, &mut |_| Ok(()))
            .unwrap();
    }
    assert!(
        server
            .dispatch_installation(NodeId(1), INSTALL_FINISH, &prefix, &mut |_| Err(
                "materializer unavailable".into()
            ))
            .is_err()
    );
    assert_eq!(server.cells[&PlacementGroupId(1)].view(), Epoch(2));
    assert!(
        server
            .cell(NodeId(1), PlacementGroupId(1), &mut |_| Err(
                "still unavailable".into()
            ))
            .is_err()
    );
    assert!(server.installation_pending.contains(&PlacementGroupId(1)));
    drop(server);
    let mut server = PeerReplicaServer::new(
        NodeId(2),
        &cluster(),
        identity(&root, 2),
        endpoints(&root),
        [(PlacementGroupId(1), cell(&root, 2))].into(),
        Duration::from_millis(800),
    )
    .unwrap();
    assert!(
        server
            .cell(NodeId(1), PlacementGroupId(1), &mut |_| Err(
                "restart must not skip apply".into()
            ))
            .is_err()
    );
    let mut applied = Vec::new();
    let response = server
        .dispatch_installation(NodeId(1), INSTALL_FINISH, &prefix, &mut |entry| {
            applied.push(entry.prepare.index);
            Ok(())
        })
        .unwrap();
    assert_eq!(response, 2u64.to_le_bytes());
    assert_eq!(applied, vec![LogIndex(1)]);
    assert!(!server.installation_pending.contains(&PlacementGroupId(1)));
    assert_eq!(
        server
            .dispatch_installation(NodeId(1), INSTALL_FINISH, &prefix, &mut |_| panic!(
                "a completed unchanged installation must not rematerialize"
            ))
            .unwrap(),
        2u64.to_le_bytes()
    );
    server
        .cell(NodeId(1), PlacementGroupId(1), &mut |_| {
            panic!("already materialized")
        })
        .unwrap();
    drop(server);
    drop(primary);
    drop(candidate);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn heartbeat_requires_materialization_and_fenced_status_does_not_grant_old_leadership() {
    let root = root("control-heartbeat");
    provision(&root);
    let mut leader = cell(&root, 0);
    let prepare = prepare(&mut leader, b"materialize before heartbeat");
    leader.acknowledge(NodeId(2), Epoch(1), prepare.index);
    let VsrDecision::Committed(commit) = leader.commit(NodeId(0), prepare.index).unwrap() else {
        panic!("commit")
    };
    let mut server = PeerReplicaServer::new(
        NodeId(2),
        &cluster(),
        identity(&root, 2),
        endpoints(&root),
        [(PlacementGroupId(1), cell(&root, 2))].into(),
        Duration::from_millis(800),
    )
    .unwrap();
    server
        .dispatch(NodeId(0), PREPARE, &prepare.encode().unwrap(), &mut |_| {
            Ok(())
        })
        .unwrap();
    assert!(
        server
            .dispatch(NodeId(0), COMMIT, &commit.encode().unwrap(), &mut |_| Err(
                "application unavailable".into()
            ))
            .is_err()
    );
    let probe = leader.primary_probe().unwrap();
    assert!(
        server
            .dispatch_heartbeat(NodeId(0), &probe.encode().unwrap(), &mut |_| Err(
                "still unavailable".into()
            ))
            .is_err()
    );
    let mut applied = Vec::new();
    server
        .dispatch_heartbeat(NodeId(0), &probe.encode().unwrap(), &mut |entry| {
            applied.push(entry.prepare.index);
            Ok(())
        })
        .unwrap();
    assert_eq!(applied, vec![LogIndex(1)]);
    let request = rsf_vsr::VsrViewChangeRequest {
        pg: PlacementGroupId(1),
        previous_view: Epoch(1),
        target_view: Epoch(2),
        candidate: NodeId(1),
        voters: vec![NodeId(0), NodeId(1), NodeId(2)],
    };
    server
        .dispatch_election(NodeId(1), VIEW_CHANGE_BEGIN, &request.encode().unwrap())
        .unwrap();
    assert!(
        server
            .dispatch_heartbeat(NodeId(0), &probe.encode().unwrap(), &mut |_| Ok(()))
            .is_err()
    );
    server.pending_contact = None;
    let status = server
        .dispatch_status(NodeId(0), &1u64.to_le_bytes())
        .unwrap();
    assert_eq!(
        rsf_vsr::VsrReplicaStatus::decode(&status).unwrap().promise,
        Some(request)
    );
    assert!(
        server.pending_contact.is_none(),
        "read-only inventory cannot refresh primary liveness"
    );
    drop(server);
    drop(leader);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn network_router_cannot_change_authority_with_or_without_a_live_quorum() {
    use rsf_protocol::{ClientRequestId, ConsistencyMode, FabricOperation, VsrBackedFabricRouter};
    let root = root("network-authority-shortcuts");
    provision(&root);
    let mut first = Some(PeerProcess::spawn(&root, 1));
    let mut second = Some(PeerProcess::spawn(&root, 2));
    let mut peers = endpoints(&root);
    peers.get_mut(&NodeId(1)).unwrap().address = first.as_ref().unwrap().address;
    peers.get_mut(&NodeId(2)).unwrap().address = second.as_ref().unwrap().address;
    let path = root.join("protocol");
    let mut router = VsrBackedFabricRouter::open_with_transport(
        &path,
        [PlacementGroupId(1)],
        Epoch(1),
        NodeId(0),
        [NodeId(0), NodeId(1), NodeId(2)],
        transport(&root, peers.clone()),
    )
    .unwrap();
    for online in [true, false] {
        if !online {
            drop(first.take());
            drop(second.take());
        }
        let before = rsf_storage::SegmentStore::recover(path.with_extension("vsr").join("pg-1"))
            .unwrap()
            .recovery
            .wal_records;
        for decision in [
            router.view_change(PlacementGroupId(1), Epoch(2), NodeId(1)),
            router.timeout_election(PlacementGroupId(1)),
            router.reconfigure(PlacementGroupId(1), Epoch(2), NodeId(0), [NodeId(0)]),
        ] {
            let VsrDecision::Rejected { reason, .. } = decision else {
                panic!("network authority changed locally")
            };
            assert!(reason.contains("quorum-installed authority"), "{reason}");
        }
        assert_eq!(
            router.membership(PlacementGroupId(1)).unwrap(),
            (Epoch(1), NodeId(0), vec![NodeId(0), NodeId(1), NodeId(2)])
        );
        assert_eq!(
            rsf_storage::SegmentStore::recover(path.with_extension("vsr").join("pg-1"))
                .unwrap()
                .recovery
                .wal_records,
            before
        );
        if online {
            router
                .commit_request(
                    PlacementGroupId(1),
                    ClientRequestId::new("before", 1),
                    ConsistencyMode::Strict,
                    FabricOperation::CatalogMutation {
                        namespace: "app".into(),
                        item: "schema".into(),
                    },
                )
                .unwrap();
        } else {
            assert!(router.confirm_primary_quorum().is_err());
        }
    }
    drop(router);
    let mut first = PeerProcess::spawn(&root, 1);
    peers.get_mut(&NodeId(1)).unwrap().address = first.address;
    let mut router = VsrBackedFabricRouter::open_with_transport(
        &path,
        [PlacementGroupId(1)],
        Epoch(1),
        NodeId(0),
        [NodeId(0), NodeId(1), NodeId(2)],
        transport(&root, peers),
    )
    .unwrap();
    router.confirm_read_quorum().unwrap();
    let committed = router
        .commit_request(
            PlacementGroupId(1),
            ClientRequestId::new("after", 1),
            ConsistencyMode::Strict,
            FabricOperation::CatalogMutation {
                namespace: "app".into(),
                item: "next-schema".into(),
            },
        )
        .unwrap();
    assert_eq!(committed.certificate.index, LogIndex(2));
    assert_eq!(router.membership(PlacementGroupId(1)).unwrap().0, Epoch(1));
    assert!(first.child.try_wait().unwrap().is_none());
    drop(router);
    drop(first);
    std::fs::remove_dir_all(root).unwrap();
}

#[path = "tests/membership.rs"]
mod membership;

#[path = "tests/membership_decision.rs"]
mod membership_decision;

#[path = "tests/control_recovery.rs"]
mod control_recovery;

#[path = "tests/membership_publication.rs"]
mod membership_publication;
