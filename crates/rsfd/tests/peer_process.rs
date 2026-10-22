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

// Durability tests share the host with the workspace suite. Short-deadline
// enforcement has dedicated RPC tests; allow fsync and TLS scheduling here.
const PEER_TIMEOUT_MS: u64 = 2_000;

#[path = "peer_process/automatic.rs"]
mod automatic;
#[path = "peer_process/client_takeover.rs"]
mod client_takeover;
#[path = "peer_process/sql_stream.rs"]
mod sql_stream;
#[path = "peer_process/stress.rs"]
mod stress;
#[path = "peer_process/takeover.rs"]
mod takeover;
#[path = "peer_process/uncertain_begin.rs"]
mod uncertain_begin;

struct Process {
    child: Child,
    address: SocketAddr,
    health: Option<SocketAddr>,
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
            health: None,
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
        let config = serde_json::json!({"cluster":"cli-cluster","node":node,"state":format!("store-{node}"),"tls":format!("tls-{node}"),"listen":"127.0.0.1:0","timeout_ms":PEER_TIMEOUT_MS,"peers":mapped,"cells":[{"pg":1,"view":1,"primary":0,"voters":[0,1,2]}]});
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
        Duration::from_millis(PEER_TIMEOUT_MS),
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
    let config = serde_json::json!({"cluster":"cli-cluster","node":0,"tls":root.join("tls-0"),"timeout_ms":PEER_TIMEOUT_MS,"peers":peers});
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
            .args([
                "--data-listen",
                "127.0.0.1:0",
                "--listen",
                "127.0.0.1:0",
                "--tick-ms",
                "10",
            ])
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
                for (prefix, health) in [
                    ("daemon listener addr=", true),
                    ("daemon data_listener addr=", false),
                ] {
                    if let Some(rest) = line.strip_prefix(prefix) {
                        let addr = rest
                            .split_whitespace()
                            .next()
                            .unwrap()
                            .parse::<SocketAddr>()
                            .unwrap();
                        let _ = send.send((health, addr));
                    }
                }
            }
        });
        let addresses = (|| {
            let first = receive.recv_timeout(Duration::from_secs(15))?;
            let second = receive.recv_timeout(Duration::from_secs(15))?;
            Ok::<_, std::sync::mpsc::RecvTimeoutError>((first, second))
        })();
        let address = addresses.map(|(first, second)| {
            if first.0 {
                (second.1, first.1)
            } else {
                (first.1, second.1)
            }
        });
        if address.is_err() {
            let _ = child.kill();
            let _ = child.wait();
        }
        let (address, health) = address.expect("primary startup");
        Self {
            child,
            address,
            health: Some(health),
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
    .unwrap_or_else(|err| panic!("{protocol:?} operation {operation}, request {request}: {err}"))
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

fn follower_objects(root: &Path, node: u64) -> rsf_object::DurableObjectStore {
    use sha2::{Digest, Sha256};
    let namespace = Sha256::digest(b"app")
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    rsf_object::DurableObjectStore::open_replicated(
        root.join(format!(
            "store-{node}/application/namespaces/{namespace}/object"
        )),
        "app",
    )
    .unwrap()
}

#[test]
fn native_object_ingress_recovers_heads_and_bytes_from_independent_peers() {
    use rsf_wire::WireProtocol::Object;
    let root = root();
    let mut peers = provision(&root);
    let state = initialize_network_state(&root, &peers);
    let first = Process::start(&root.join("peer-1.json"));
    let second = Process::start(&root.join("peer-2.json"));
    peers.get_mut(&NodeId(1)).unwrap().address = first.address;
    peers.get_mut(&NodeId(2)).unwrap().address = second.address;
    configure_primary(&root, &state, &peers);
    let primary = Process::primary(&state);
    let bucket = surface_request(
        primary.address,
        Object,
        "CREATE_BUCKET media",
        "bucket-1",
        b"",
    );
    assert!(bucket.contains("RSF/1 executed"), "{bucket}");
    let alias = surface_request(
        primary.address,
        Object,
        "CREATE_BUCKET media",
        "bucket-alias",
        b"",
    );
    assert_eq!(alias, bucket);
    let first_result = surface_request(
        primary.address,
        Object,
        "PUT media/key",
        "object-1",
        b"first object bytes",
    );
    assert!(first_result.contains("RSF/1 executed"), "{first_result}");
    drop(second);
    let second_result = surface_request(
        primary.address,
        Object,
        "PUT media/key",
        "object-2",
        b"second object bytes",
    );
    assert!(second_result.contains("RSF/1 executed"), "{second_result}");
    let changed = surface_request(
        primary.address,
        Object,
        "PUT media/key",
        "object-2",
        b"changed bytes",
    );
    assert!(changed.contains("RSF/1 rejected"), "{changed}");
    drop(primary);
    drop(first);
    {
        let mut objects = follower_objects(&root, 1);
        assert_eq!(
            objects.read_verified("media", "key").unwrap(),
            b"second object bytes"
        );
        assert_eq!(objects.versions("media", "key").unwrap().len(), 2);
    }
    {
        let mut objects = follower_objects(&root, 2);
        assert_eq!(
            objects.read_verified("media", "key").unwrap(),
            b"first object bytes"
        );
    }
    let second = Process::start(&root.join("peer-2.json"));
    peers.get_mut(&NodeId(2)).unwrap().address = second.address;
    configure_primary(&root, &state, &peers);
    let primary = Process::primary(&state);
    let retry = surface_request(
        primary.address,
        Object,
        "PUT media/key",
        "object-2",
        b"second object bytes",
    );
    assert_eq!(retry, second_result);
    let old = surface_request(
        primary.address,
        Object,
        "PUT media/key",
        "object-1",
        b"first object bytes",
    );
    assert_eq!(old, first_result);
    let read = surface_request(primary.address, Object, "GET media/key", "read", b"");
    assert!(read.contains("RSF/1 executed"), "{read}");
    let expected = b"second object bytes"
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    assert!(read.contains(&format!("payload_hex={expected}")), "{read}");
    drop(second);
    let read = surface_request(primary.address, Object, "GET media/key", "read", b"");
    assert!(read.contains("RSF/1 rejected"), "{read}");
    let retry = surface_request(
        primary.address,
        Object,
        "PUT media/key",
        "object-2",
        b"second object bytes",
    );
    assert!(retry.contains("RSF/1 rejected"), "{retry}");
    drop(primary);
    let mut objects = follower_objects(&root, 2);
    assert_eq!(
        objects.read_verified("media", "key").unwrap(),
        b"second object bytes"
    );
    assert_eq!(objects.versions("media", "key").unwrap().len(), 2);
}

fn follower_sql(root: &Path, node: u64) -> rsf_sql::DurableSqlEngine {
    use sha2::{Digest, Sha256};
    let namespace = Sha256::digest(b"app")
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    rsf_sql::DurableSqlEngine::open_replicated(
        root.join(format!(
            "store-{node}/application/namespaces/{namespace}/sql"
        )),
        "app",
    )
    .unwrap()
}

#[test]
fn native_sql_ingress_replicates_rows_indexes_and_tombstones_to_real_peers() {
    use rsf_wire::WireProtocol::Sql;
    let root = root();
    let mut peers = provision(&root);
    let state = initialize_network_state(&root, &peers);
    // An actual V4 empty checkpoint selects the legacy physical key domain.
    // New peers must learn that format from certified SQL history, not assume V2.
    let namespace = {
        use sha2::{Digest, Sha256};
        Sha256::digest(b"app")
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    };
    let legacy_root = state.join("namespaces").join(&namespace).join("sql");
    std::fs::create_dir_all(&legacy_root).unwrap();
    let body = "RSF_SQL_STATE_V4\n";
    let digest = rsf_core::Digest::blake3_bytes(body.as_bytes());
    let checksum = digest
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    std::fs::write(
        legacy_root.join("sql.state"),
        format!("{body}checksum\tblake3\t{checksum}\n"),
    )
    .unwrap();
    let first = Process::start(&root.join("peer-1.json"));
    let second = Process::start(&root.join("peer-2.json"));
    peers.get_mut(&NodeId(1)).unwrap().address = first.address;
    peers.get_mut(&NodeId(2)).unwrap().address = second.address;
    configure_primary(&root, &state, &peers);
    let primary = Process::primary(&state);
    let create = surface_request(
        primary.address,
        Sql,
        "CREATE TABLE users(id PRIMARY KEY, name)",
        "create",
        b"",
    );
    assert!(create.contains("RSF/1 executed"), "{create}");
    let inserted = surface_request(
        primary.address,
        Sql,
        "INSERT INTO users (id, name) VALUES (1, ada)",
        "insert",
        b"",
    );
    assert!(inserted.contains("RSF/1 executed"), "{inserted}");
    let index = surface_request(
        primary.address,
        Sql,
        "CREATE INDEX users_name ON users(name)",
        "index",
        b"",
    );
    assert!(index.contains("RSF/1 executed"), "{index}");
    drop(second);
    for (statement, request) in [
        ("UPDATE users SET name=grace WHERE id=1", "update"),
        ("INSERT INTO users (id, name) VALUES (2, alan)", "insert-2"),
    ] {
        let result = surface_request(primary.address, Sql, statement, request, b"");
        assert!(result.contains("RSF/1 executed"), "{result}");
    }
    let deleted = surface_request(
        primary.address,
        Sql,
        "DELETE FROM users WHERE id=1",
        "delete",
        b"",
    );
    assert!(deleted.contains("RSF/1 executed"), "{deleted}");
    let changed = surface_request(
        primary.address,
        Sql,
        "DELETE FROM users WHERE id=2",
        "delete",
        b"",
    );
    assert!(changed.contains("RSF/1 rejected"), "{changed}");
    drop(primary);
    drop(first);
    {
        let mut sql = follower_sql(&root, 1);
        assert_eq!(
            sql.read_snapshot("SELECT * FROM users WHERE name=alan")
                .unwrap()
                .rows,
            1
        );
        assert_eq!(
            sql.read_snapshot("SELECT * FROM users WHERE id=1")
                .unwrap()
                .rows,
            0
        );
        assert_eq!(sql.recovery().live_rows, 1);
        assert_eq!(sql.recovery().tombstones, 1);
    }
    {
        let mut sql = follower_sql(&root, 2);
        assert_eq!(
            sql.read_snapshot("SELECT * FROM users WHERE name=ada")
                .unwrap()
                .rows,
            1
        );
        assert_eq!(
            sql.read_snapshot("SELECT * FROM users WHERE id=2")
                .unwrap()
                .rows,
            0
        );
    }
    let lost_sql = root
        .join("store-2/application/namespaces")
        .join(&namespace)
        .join("sql");
    std::fs::remove_dir_all(&lost_sql).unwrap();
    let second = Process::start(&root.join("peer-2.json"));
    peers.get_mut(&NodeId(2)).unwrap().address = second.address;
    configure_primary(&root, &state, &peers);
    let primary = Process::primary(&state);
    let retry = surface_request(
        primary.address,
        Sql,
        "DELETE FROM users WHERE id=1",
        "delete",
        b"",
    );
    assert_eq!(retry, deleted.replace("replayed=false", "replayed=true"));
    let old = surface_request(
        primary.address,
        Sql,
        "INSERT INTO users (id, name) VALUES (1, ada)",
        "insert",
        b"",
    );
    assert_eq!(old, inserted.replace("replayed=false", "replayed=true"));
    let selected = surface_request(
        primary.address,
        Sql,
        "SELECT * FROM users WHERE name=alan",
        "read",
        b"",
    );
    assert!(selected.contains("RSF/1 executed"), "{selected}");
    assert!(selected.contains("rows=1"), "{selected}");
    drop(second);
    let selected = surface_request(primary.address, Sql, "SELECT * FROM users", "read", b"");
    assert!(selected.contains("RSF/1 rejected"), "{selected}");
    let retry = surface_request(
        primary.address,
        Sql,
        "DELETE FROM users WHERE id=1",
        "delete",
        b"",
    );
    assert!(retry.contains("RSF/1 rejected"), "{retry}");
    drop(primary);
    let mut sql = follower_sql(&root, 2);
    assert_eq!(
        sql.read_snapshot("SELECT * FROM users WHERE name=alan")
            .unwrap()
            .rows,
        1
    );
    assert_eq!(
        sql.read_snapshot("SELECT * FROM users WHERE id=1")
            .unwrap()
            .rows,
        0
    );
    assert!(
        std::fs::read_to_string(lost_sql.join("sql.state"))
            .unwrap()
            .starts_with("RSF_SQL_STATE_V6\n")
    );
}

fn health_request(primary: &Process, endpoint: &str) -> String {
    use std::io::{Read, Write};
    let mut stream = std::net::TcpStream::connect(primary.health.unwrap()).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    write!(stream, "GET {endpoint} HTTP/1.1\r\nHost: localhost\r\n\r\n").unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    response
}
fn await_quorum(primary: &mut Process, expected: bool) -> String {
    let until = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        assert!(
            primary.child.try_wait().unwrap().is_none(),
            "daemon stopped on quorum loss"
        );
        let response = health_request(primary, "/healthz");
        let expected_field = format!("\"quorum_available\":{expected}");
        if response.contains(&expected_field) {
            assert!(
                response.contains(&format!("\"ready\":{expected}")),
                "{response}"
            );
            assert!(
                response.starts_with(if expected {
                    "HTTP/1.1 200"
                } else {
                    "HTTP/1.1 503"
                }),
                "{response}"
            );
            return response;
        }
        assert!(std::time::Instant::now() < until, "{response}");
        std::thread::sleep(Duration::from_millis(20));
    }
}
#[test]
fn readiness_tracks_authenticated_quorum_loss_recovery_and_configuration_errors() {
    let root = root();
    let mut peers = provision(&root);
    let state = initialize_network_state(&root, &peers);
    let first = Process::start(&root.join("peer-1.json"));
    let second = Process::start(&root.join("peer-2.json"));
    peers.get_mut(&NodeId(1)).unwrap().address = first.address;
    peers.get_mut(&NodeId(2)).unwrap().address = second.address;
    configure_primary(&root, &state, &peers);
    let mut primary = Process::primary(&state);
    assert!(await_quorum(&mut primary, true).contains("\"replication\":\"network\""));
    // A committed command must be probed without creating another command.
    assert!(
        kv_request(primary.address, "SET readiness", "write", b"durable")
            .contains("RSF/1 executed")
    );
    let ready = await_quorum(&mut primary, true);
    assert!(
        ready.contains("\"protocol_certificate_count\":1"),
        "{ready}"
    );
    drop(second);
    await_quorum(&mut primary, true);
    drop(first);
    await_quorum(&mut primary, false);
    assert!(health_request(&primary, "/livez").starts_with("HTTP/1.1 200"));
    assert!(health_request(&primary, "/metrics").contains("rsf_daemon_quorum_available 0\n"));
    assert!(kv_request(primary.address, "GET readiness", "read", b"").contains("RSF/1 rejected"));
    let first = Process::start(&root.join("peer-1.json"));
    peers.get_mut(&NodeId(1)).unwrap().address = first.address;
    configure_primary(&root, &state, &peers);
    let recovered = await_quorum(&mut primary, true);
    assert!(
        recovered.contains("\"protocol_certificate_count\":1"),
        "{recovered}"
    );
    let path = state.join("replication.json");
    std::fs::write(&path, b"invalid configuration").unwrap();
    let degraded = await_quorum(&mut primary, false);
    assert!(degraded.contains("\"replication\":\"unavailable\""));
    assert!(health_request(&primary, "/livez").starts_with("HTTP/1.1 200"));
    configure_primary(&root, &state, &peers);
    await_quorum(&mut primary, true);
    std::fs::remove_file(path).unwrap();
    assert!(await_quorum(&mut primary, false).contains("requires authenticated replication.json"));
    configure_primary(&root, &state, &peers);
    await_quorum(&mut primary, true);
    drop(primary);
    let status = Command::new(env!("CARGO_BIN_EXE_rsfd"))
        .args(["status", "--state"])
        .arg(&state)
        .output()
        .unwrap();
    assert!(status.status.success());
    assert!(
        String::from_utf8_lossy(&status.stdout).contains("placement-health: ready"),
        "{}",
        String::from_utf8_lossy(&status.stdout)
    );
    drop(first);
    let status = Command::new(env!("CARGO_BIN_EXE_rsfd"))
        .args(["status", "--state"])
        .arg(&state)
        .output()
        .unwrap();
    assert!(status.status.success());
    assert!(
        String::from_utf8_lossy(&status.stdout).contains("placement-health: degraded"),
        "{}",
        String::from_utf8_lossy(&status.stdout)
    );
}

#[test]
fn learner_cli_transfers_historical_configurations_and_materializes_after_process_restart() {
    let root = root();
    let mut peers = provision(&root);
    let state = initialize_network_state(&root, &peers);
    let first = Process::start(&root.join("peer-1.json"));
    peers.get_mut(&NodeId(1)).unwrap().address = first.address;
    configure_primary(&root, &state, &peers);
    let primary = Process::primary(&state);
    let response = kv_request(
        primary.address,
        "SET learner/key",
        "learner-write",
        b"transferred value",
    );
    assert!(response.contains("RSF/1 executed"), "{response}");
    drop(primary);
    drop(first);
    // Install the same configuration locally to exercise history transfer. This
    // is not evidence for the separate distributed membership-install protocol.
    let source_directory = state.join("protocol.vsr/pg-1");
    let mut source = DurableVsrCell::open(
        &source_directory,
        PlacementGroupId(1),
        Epoch(1),
        NodeId(0),
        [NodeId(0)],
    )
    .unwrap();
    let view = Epoch(source.view().0 + 1);
    assert!(matches!(
        source.reconfigure(view, NodeId(0), [NodeId(0), NodeId(1)]),
        VsrDecision::MembershipChanged { .. }
    ));
    let expected = source.committed_prefix().unwrap();
    drop(source);
    {
        let mut follower = DurableVsrCell::open(
            root.join("store-1/pg-1"),
            PlacementGroupId(1),
            Epoch(1),
            NodeId(0),
            [NodeId(0), NodeId(1), NodeId(2)],
        )
        .unwrap();
        assert!(matches!(
            follower.reconfigure(view, NodeId(0), [NodeId(0), NodeId(1)]),
            VsrDecision::MembershipChanged { .. }
        ));
    }
    let learner_config = root.join("peer-2.json");
    let mut config: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&learner_config).unwrap()).unwrap();
    config["cells"][0] =
        serde_json::json!({"pg":1,"view":view.0,"primary":0,"voters":[0,1],"learner_source":0});
    std::fs::write(&learner_config, serde_json::to_vec(&config).unwrap()).unwrap();
    let first = Process::start(&root.join("peer-1.json"));
    let learner = Process::start(&learner_config);
    peers.get_mut(&NodeId(1)).unwrap().address = first.address;
    peers.get_mut(&NodeId(2)).unwrap().address = learner.address;
    let mapped=peers.iter().map(|(node,peer)|serde_json::json!({"node":node.0,"address":peer.address.to_string(),"server_name":peer.server_name,"fingerprint":peer.fingerprint.iter().map(|b|format!("{b:02x}")).collect::<String>()})).collect::<Vec<_>>();
    let source_config = root.join("source-peer.json");
    let configuration = serde_json::json!({"cluster":"cli-cluster","node":0,"state":state.join("protocol.vsr"),"tls":root.join("tls-0"),"listen":"127.0.0.1:0","timeout_ms":PEER_TIMEOUT_MS,"peers":mapped,"cells":[{"pg":1,"view":view.0,"primary":0,"voters":[0,1]}]});
    std::fs::write(&source_config, serde_json::to_vec(&configuration).unwrap()).unwrap();
    let sync = || {
        Command::new(env!("CARGO_BIN_EXE_rsfd"))
            .args(["peer", "sync-learner", "--config"])
            .arg(&source_config)
            .args(["--pg", "1", "--target", "2"])
            .output()
            .unwrap()
    };
    let output = sync();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("peer.learner-synced"));
    let output = sync();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    drop(learner);
    {
        let recovered = rsf_vsr::LearnerHistoryStore::open(
            root.join("store-2/learner-1"),
            PlacementGroupId(1),
            NodeId(2),
            NodeId(0),
        )
        .unwrap();
        assert_eq!(recovered.committed_entries().unwrap(), expected);
        let kv = follower_kv(&root, 2);
        assert_eq!(kv.get(b"learner/key").unwrap().value, b"transferred value");
        assert_eq!(kv.changes_since(rsf_core::CommitVersion(0)).len(), 1);
    }
    let learner = Process::start(&learner_config);
    drop(learner);
    assert_eq!(
        follower_kv(&root, 2)
            .changes_since(rsf_core::CommitVersion(0))
            .len(),
        1
    );
    // Changing a config flag cannot silently turn the staged store into a voter.
    config["cells"][0] = serde_json::json!({"pg":1,"view":view.0,"primary":0,"voters":[0,1,2]});
    std::fs::write(&learner_config, serde_json::to_vec(&config).unwrap()).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_rsfd"))
        .args(["peer", "serve", "--config"])
        .arg(&learner_config)
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("certified membership"));
    drop(first);
    let output = sync();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("lacks current primary quorum"));
}
