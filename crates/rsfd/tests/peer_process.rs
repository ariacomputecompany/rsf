use std::collections::BTreeMap;
use std::io::BufRead;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose,
};
use rsf_core::{ClusterId, Epoch, LogIndex, NodeId, OperationId, PlacementGroupId};
use rsf_rpc::replication::{NetworkVsrTransport, PeerEndpoint, PeerIdentity};
use rsf_vsr::{DurableVsrCell, VsrDecision, VsrOperation};

struct Process {
    child: Child,
    address: SocketAddr,
}
impl Process {
    fn start(config: &Path) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_rsfd"))
            .args(["peer", "serve", "--config"])
            .arg(config)
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let (send, receive) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            for line in std::io::BufReader::new(stdout).lines() {
                let Ok(line) = line else {
                    break;
                };
                let Ok(event) = serde_json::from_str::<serde_json::Value>(&line) else {
                    continue;
                };
                if event["event"] == "peer.ready" {
                    let _ = send.send(
                        event["listen"]
                            .as_str()
                            .unwrap()
                            .parse::<SocketAddr>()
                            .unwrap(),
                    );
                }
            }
        });
        let address = receive.recv_timeout(Duration::from_secs(10));
        if address.is_err() {
            let _ = child.kill();
            let _ = child.wait();
        }
        Self {
            child,
            address: address.expect("daemon peer readiness"),
        }
    }
}
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
fn root() -> PathBuf {
    static NEXT_ROOT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    std::env::temp_dir().join(format!(
        "rsfd-network-{}-{}-{}",
        std::process::id(),
        NEXT_ROOT.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}
fn provision(root: &Path) -> BTreeMap<NodeId, PeerEndpoint> {
    let mut params = CertificateParams::new(Vec::new()).unwrap();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    let key = KeyPair::generate().unwrap();
    let ca = params.self_signed(&key).unwrap();
    let issuer = Issuer::new(params, key);
    let mut peers = BTreeMap::new();
    for node in 0..3 {
        let mut params = CertificateParams::new(vec!["localhost".into()]).unwrap();
        params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ClientAuth,
            ExtendedKeyUsagePurpose::ServerAuth,
        ];
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        let key = KeyPair::generate().unwrap();
        let cert = params.signed_by(&key, &issuer).unwrap();
        let dir = root.join(format!("tls-{node}"));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("certificate.pem"), cert.pem()).unwrap();
        std::fs::write(dir.join("key.pem"), key.serialize_pem()).unwrap();
        std::fs::write(dir.join("ca.pem"), ca.pem()).unwrap();
        let identity = PeerIdentity::load(&dir).unwrap();
        peers.insert(
            NodeId(node),
            PeerEndpoint {
                address: "127.0.0.1:1".parse().unwrap(),
                server_name: "localhost".into(),
                fingerprint: identity.fingerprint,
            },
        );
    }
    let mapped=peers.iter().map(|(node,peer)|serde_json::json!({"node":node.0,"address":peer.address.to_string(),"server_name":peer.server_name,"fingerprint":peer.fingerprint.iter().map(|b|format!("{b:02x}")).collect::<String>()})).collect::<Vec<_>>();
    for node in 1..3 {
        let config = serde_json::json!({"cluster":"cli-cluster","node":node,"state":format!("store-{node}"),"tls":format!("tls-{node}"),"listen":"127.0.0.1:0","timeout_ms":1000,"peers":mapped,"cells":[{"pg":1,"view":1,"primary":0,"voters":[0,1,2]}]});
        std::fs::write(
            root.join(format!("peer-{node}.json")),
            serde_json::to_vec(&config).unwrap(),
        )
        .unwrap();
    }
    peers
}

#[test]
fn daemon_peer_cli_replicates_commands_and_recovers_after_forced_termination() {
    let root = root();
    let mut peers = provision(&root);
    let first = Process::start(&root.join("peer-1.json"));
    let second = Process::start(&root.join("peer-2.json"));
    peers.get_mut(&NodeId(1)).unwrap().address = first.address;
    peers.get_mut(&NodeId(2)).unwrap().address = second.address;
    let mut network = NetworkVsrTransport::new(
        NodeId(0),
        &ClusterId::new("cli-cluster").unwrap(),
        PeerIdentity::load(&root.join("tls-0")).unwrap(),
        peers,
        Duration::from_secs(1),
    )
    .unwrap();
    let mut leader = DurableVsrCell::open(
        root.join("leader"),
        PlacementGroupId(1),
        Epoch(1),
        NodeId(0),
        [NodeId(0), NodeId(1), NodeId(2)],
    )
    .unwrap();
    let payload = b"tenant/alpha\0SET customer/7\0independently durable payload";
    let operation = VsrOperation::ReplicatedCommand {
        operation_id: OperationId(1),
        payload: payload.to_vec(),
    };
    let VsrDecision::Prepared(prepare) = leader
        .prepare(NodeId(0), operation.clone(), payload.len() as u32)
        .unwrap()
    else {
        panic!("prepare")
    };
    leader
        .replicate_with_transport(NodeId(0), prepare.index, &mut network)
        .unwrap();
    drop(first);
    drop(second);
    for node in 1..3 {
        let recovered = DurableVsrCell::open(
            root.join(format!("store-{node}/pg-1")),
            PlacementGroupId(1),
            Epoch(1),
            NodeId(0),
            [NodeId(0), NodeId(1), NodeId(2)],
        )
        .unwrap();
        assert_eq!(recovered.committed_operation(LogIndex(1)), Some(&operation));
    }
    let restart = Process::start(&root.join("peer-1.json"));
    drop(restart);
    let path = root.join("peer-1.json");
    let mut config: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    config["cluster"] = "wrong-cluster".into();
    std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
    let rejected = Command::new(env!("CARGO_BIN_EXE_rsfd"))
        .args(["peer", "serve", "--config"])
        .arg(path)
        .output()
        .unwrap();
    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("different cluster or node"));
    drop(leader);
    std::fs::remove_dir_all(root).unwrap();
}

#[allow(dead_code)]
#[path = "../src/test_support.rs"]
mod test_support;
#[allow(dead_code)]
#[path = "../src/tls.rs"]
mod tls;

fn cli(args: &[String]) {
    let output = Command::new(env!("CARGO_BIN_EXE_rsfd"))
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}
fn initialize_network_state(root: &Path, peers: &BTreeMap<NodeId, PeerEndpoint>) -> PathBuf {
    let state = root.join("primary");
    for node in 0..3 {
        let fingerprint = peers[&NodeId(node)]
            .fingerprint
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        cli(&[
            if node == 0 { "init" } else { "join" }.to_string(),
            "--state".into(),
            state.display().to_string(),
            "--token".into(),
            "token".into(),
            "--node-key".into(),
            format!("peer-node-public-key-{node}"),
            "--node-nonce".into(),
            format!("peer-node-nonce-{node}"),
            "--mtls-trust-domain".into(),
            "cli-cluster".into(),
            "--tls-cert-sha256".into(),
            fingerprint,
            "--region".into(),
            "iad".into(),
            "--zone".into(),
            format!("zone-{node}"),
        ]);
    }
    test_support::provision(&state);
    let router = rsf_protocol::VsrBackedFabricRouter::open(
        state.join("protocol"),
        [PlacementGroupId(1)],
        Epoch(1),
        NodeId(0),
        [NodeId(0), NodeId(1), NodeId(2)],
    )
    .unwrap();
    let (view, _, _) = router.membership(PlacementGroupId(1)).unwrap();
    drop(router);
    for node in 1..3 {
        let path = root.join(format!("peer-{node}.json"));
        let mut config: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        config["cells"][0]["view"] = view.0.into();
        std::fs::write(path, serde_json::to_vec(&config).unwrap()).unwrap();
    }
    state
}
fn configure_primary(root: &Path, state: &Path, peers: &BTreeMap<NodeId, PeerEndpoint>) {
    let peers=peers.iter().map(|(node,peer)|serde_json::json!({"node":node.0,"address":peer.address.to_string(),"server_name":peer.server_name,"fingerprint":peer.fingerprint.iter().map(|b|format!("{b:02x}")).collect::<String>()})).collect::<Vec<_>>();
    let config = serde_json::json!({"cluster":"cli-cluster","node":0,"tls":root.join("tls-0"),"timeout_ms":500,"peers":peers});
    std::fs::write(
        state.join("replication.json"),
        serde_json::to_vec(&config).unwrap(),
    )
    .unwrap();
}
impl Process {
    fn primary(state: &Path) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_rsfd"))
            .args(["serve", "--state"])
            .arg(state)
            .args(["--data-listen", "127.0.0.1:0", "--tick-ms", "10"])
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let (send, receive) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            for line in std::io::BufReader::new(stdout).lines() {
                let Ok(line) = line else {
                    return;
                };
                if let Some(rest) = line.strip_prefix("daemon data_listener addr=") {
                    let addr = rest
                        .split_whitespace()
                        .next()
                        .unwrap()
                        .parse::<SocketAddr>()
                        .unwrap();
                    let _ = send.send(addr);
                }
            }
        });
        let address = receive.recv_timeout(Duration::from_secs(15));
        if address.is_err() {
            let _ = child.kill();
            let _ = child.wait();
        }
        Self {
            child,
            address: address.expect("primary startup"),
        }
    }
}
fn kv_request(address: SocketAddr, operation: &str, request: &str, value: &[u8]) -> String {
    surface_request(
        address,
        rsf_wire::WireProtocol::Kv,
        operation,
        request,
        value,
    )
}
fn surface_request(
    address: SocketAddr,
    protocol: rsf_wire::WireProtocol,
    operation: &str,
    request: &str,
    value: &[u8],
) -> String {
    let frame = rsf_wire::RawFrame::with_body(
        protocol,
        "app-client",
        Some("app".into()),
        operation,
        Some(request.into()),
        value.to_vec(),
    );
    test_support::send(
        address,
        &rsf_wire::WireCodec.encode(&frame).unwrap(),
        test_support::pki().client.clone(),
    )
    .unwrap()
}
fn follower_kv(root: &Path, node: u64) -> rsf_kv::DurableKvEngine {
    use sha2::{Digest, Sha256};
    let namespace = Sha256::digest(b"app")
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    rsf_kv::DurableKvEngine::open_replicated(
        root.join(format!(
            "store-{node}/application/namespaces/{namespace}/kv"
        )),
        Epoch(1),
        "app",
    )
    .unwrap()
}

#[test]
fn native_kv_ingress_replicates_and_materializes_on_real_daemon_peers() {
    let root = root();
    let mut peers = provision(&root);
    let state = initialize_network_state(&root, &peers);
    // A three-voter daemon must not silently substitute local acknowledgements.
    let primary = Process::primary(&state);
    let rejected = kv_request(primary.address, "SET user/1", "set-1", b"first");
    assert!(rejected.contains("replication.json"), "{rejected}");
    drop(primary);
    let first = Process::start(&root.join("peer-1.json"));
    let second = Process::start(&root.join("peer-2.json"));
    peers.get_mut(&NodeId(1)).unwrap().address = first.address;
    peers.get_mut(&NodeId(2)).unwrap().address = second.address;
    configure_primary(&root, &state, &peers);
    let primary = Process::primary(&state);
    let config_path = state.join("replication.json");
    let original_config = std::fs::read(&config_path).unwrap();
    let mut wrong: serde_json::Value = serde_json::from_slice(&original_config).unwrap();
    wrong["peers"][1]["fingerprint"] = "00".repeat(32).into();
    std::fs::write(&config_path, serde_json::to_vec(&wrong).unwrap()).unwrap();
    let rejected = kv_request(primary.address, "SET user/1", "set-1", b"first");
    assert!(
        rejected.contains("not the admitted node identity"),
        "{rejected}"
    );
    std::fs::write(&config_path, original_config).unwrap();
    let empty = kv_request(primary.address, "GET user/1", "empty-read", b"");
    assert!(empty.contains("RSF/1 executed"), "{empty}");
    let first_result = kv_request(primary.address, "SET user/1", "set-1", b"first");
    assert!(first_result.contains("RSF/1 executed"), "{first_result}");
    drop(second);
    let second_result = kv_request(primary.address, "SET user/1", "set-2", b"second");
    assert!(second_result.contains("RSF/1 executed"), "{second_result}");
    // Simultaneously lose the primary and its only live follower after success.
    drop(primary);
    drop(first);
    {
        let follower = follower_kv(&root, 1);
        assert_eq!(follower.get(b"user/1").unwrap().value, b"second");
        assert_eq!(follower.changes_since(rsf_core::CommitVersion(0)).len(), 2);
    }
    {
        let follower = follower_kv(&root, 2);
        assert_eq!(follower.get(b"user/1").unwrap().value, b"first");
    }
    // Restart the lagging peer; retry repairs its missing prefix and applies it
    // before success, without duplicating the primary's or follower's history.
    let second = Process::start(&root.join("peer-2.json"));
    peers.get_mut(&NodeId(2)).unwrap().address = second.address;
    configure_primary(&root, &state, &peers);
    let primary = Process::primary(&state);
    let retry = kv_request(primary.address, "SET user/1", "set-2", b"second");
    assert!(retry.contains("RSF/1 executed"), "{retry}");
    let changed = kv_request(primary.address, "SET user/1", "set-2", b"different");
    assert!(changed.contains("RSF/1 rejected"), "{changed}");
    let read = kv_request(primary.address, "GET user/1", "read", b"");
    assert!(read.contains("RSF/1 executed"), "{read}");
    drop(second);
    let read = kv_request(primary.address, "GET user/1", "read", b"");
    assert!(read.contains("RSF/1 rejected"), "{read}");
    let no_quorum = kv_request(primary.address, "SET user/1", "set-2", b"second");
    assert!(no_quorum.contains("RSF/1 rejected"), "{no_quorum}");
    drop(primary);
    let follower = follower_kv(&root, 2);
    assert_eq!(follower.get(b"user/1").unwrap().value, b"second");
    assert_eq!(follower.changes_since(rsf_core::CommitVersion(0)).len(), 2);
}

fn follower_blob(root: &Path, node: u64) -> rsf_blob::DurableBlobEngine {
    use sha2::{Digest, Sha256};
    let namespace = Sha256::digest(b"app")
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    rsf_blob::DurableBlobEngine::open_replicated(
        root.join(format!(
            "store-{node}/application/namespaces/{namespace}/blob"
        )),
        "app",
    )
    .unwrap()
}

#[test]
fn native_blob_ingress_preserves_payload_on_peers_after_primary_loss() {
    use rsf_wire::WireProtocol::Blob;
    let root = root();
    let mut peers = provision(&root);
    let state = initialize_network_state(&root, &peers);
    let first = Process::start(&root.join("peer-1.json"));
    let second = Process::start(&root.join("peer-2.json"));
    peers.get_mut(&NodeId(1)).unwrap().address = first.address;
    peers.get_mut(&NodeId(2)).unwrap().address = second.address;
    configure_primary(&root, &state, &peers);
    let primary = Process::primary(&state);
    let bytes = b"blob bytes survive loss of primary and quorum peer";
    let id = rsf_core::blob::ContentId::for_bytes(bytes);
    let first_result = surface_request(primary.address, Blob, "PUT payload", "blob-1", bytes);
    assert!(first_result.contains("RSF/1 executed"), "{first_result}");
    drop(second);
    let later = b"second immutable value";
    let later_id = rsf_core::blob::ContentId::for_bytes(later);
    let result = surface_request(primary.address, Blob, "PUT later", "blob-2", later);
    assert!(result.contains("RSF/1 executed"), "{result}");
    let changed = surface_request(primary.address, Blob, "PUT payload", "blob-1", b"changed");
    assert!(changed.contains("RSF/1 rejected"), "{changed}");
    drop(primary);
    drop(first);
    {
        let mut follower = follower_blob(&root, 1);
        assert_eq!(follower.read_verified(&id).unwrap(), bytes);
        assert_eq!(follower.read_verified(&later_id).unwrap(), later);
        assert_eq!(follower.verify_all().unwrap().pinned, 2);
    }
    {
        let mut follower = follower_blob(&root, 2);
        assert_eq!(follower.read_verified(&id).unwrap(), bytes);
        assert!(follower.read_verified(&later_id).is_err());
    }
    let second = Process::start(&root.join("peer-2.json"));
    peers.get_mut(&NodeId(2)).unwrap().address = second.address;
    configure_primary(&root, &state, &peers);
    let primary = Process::primary(&state);
    let retry = surface_request(primary.address, Blob, "PUT later", "blob-2", later);
    assert!(retry.contains("RSF/1 executed"), "{retry}");
    drop(second);
    let no_quorum = surface_request(primary.address, Blob, "PUT later", "blob-2", later);
    assert!(no_quorum.contains("RSF/1 rejected"), "{no_quorum}");
    // An immutable verified GET remains available with its content proof.
    let digest = id
        .as_bytes()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    let read = surface_request(
        primary.address,
        Blob,
        &format!("GET blake3:{digest}"),
        "read",
        b"",
    );
    assert!(read.contains("RSF/1 executed"), "{read}");
    drop(primary);
    let mut follower = follower_blob(&root, 2);
    assert_eq!(follower.read_verified(&later_id).unwrap(), later);
    assert_eq!(follower.verify_all().unwrap().pinned, 2);
}
