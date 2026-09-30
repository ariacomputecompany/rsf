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
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "replication::tests::peer_process", "--nocapture"])
            .env("RSF_RPC_TEST_ROOT", root)
            .env("RSF_RPC_TEST_NODE", node.to_string())
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
    let mut server = PeerReplicaServer::new(
        NodeId(node),
        &cluster(),
        identity(&root, node),
        endpoints(&root),
        [(PlacementGroupId(1), cell(&root, node))].into(),
        Duration::from_millis(800),
    )
    .unwrap();
    println!("RSF_PEER_READY={}", listener.local_addr().unwrap());
    std::io::stdout().flush().unwrap();
    for socket in listener.incoming() {
        let _ = server.serve_connection(socket.unwrap());
    }
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
