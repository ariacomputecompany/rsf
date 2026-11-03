//! Native client admission follows authoritative ancestry acquired over TLS,
//! even when two real membership transitions return to the admitted voter set.
use super::*;
use rsf_vsr::{VsrTransport, VsrViewChangeRequest};

#[path = "membership_admission/fixture.rs"]
mod fixture;

const PG: PlacementGroupId = PlacementGroupId(1);
const RPC_MS: u64 = 800;

#[test]
fn native_client_rechecks_membership_ancestry_after_live_ordinary_installation() {
    let root = root();
    let mut peers = provision_count(&root, 4);
    let control = initialize_network_state(&root, &peers);
    let mut candidate = fixture::returned_membership(&root);
    assert_eq!(candidate.replicas(), (0..3).map(NodeId).collect::<Vec<_>>());
    assert!(candidate.has_membership_publication().unwrap());
    let old = DurableVsrCell::open_existing(root.join("store-0/pg-1"), PG).unwrap();
    assert_eq!(old.view(), Epoch(2));
    assert_eq!(old.replicas(), candidate.replicas());
    assert!(!old.has_membership_publication().unwrap());
    assert!(old.membership_fence().is_none());
    drop(old);

    let reservations = (0..4)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect::<Vec<_>>();
    let clients = reservations
        .iter()
        .map(|listener| listener.local_addr().unwrap())
        .collect::<Vec<_>>();
    let mapped = peers
        .iter()
        .map(|(node, peer)| {
            serde_json::json!({
                "node":node.0,"address":peer.address.to_string(),"server_name":peer.server_name,
                "fingerprint":peer.fingerprint.iter().map(|b|format!("{b:02x}")).collect::<String>()
            })
        })
        .collect::<Vec<_>>();
    for node in [0, 2] {
        let mut config = serde_json::json!({
            "cluster":"cli-cluster","node":node,"state":format!("store-{node}"),
            "tls":format!("tls-{node}"),"listen":"127.0.0.1:0","timeout_ms":RPC_MS,
            "peers":mapped,"cells":[{"pg":1,"view":1,"primary":0,"voters":[0,1,2]}]
        });
        if node == 0 {
            config["elections"] = serde_json::json!({
                "heartbeat_ms":50,"timeout_ms":8000,"rank_delay_ms":1000
            });
            config["client"] = serde_json::json!({
                "state":control,"listen":clients[0].to_string(),
                "endpoints":clients.iter().enumerate().map(|(node,address)|
                    serde_json::json!({"node":node,"address":address.to_string()})).collect::<Vec<_>>()
            });
        }
        std::fs::write(
            root.join(format!("admission-{node}.json")),
            serde_json::to_vec(&config).unwrap(),
        )
        .unwrap();
    }
    drop(reservations);
    let second = Process::start(&root.join("admission-2.json"));
    let first = Process::start(&root.join("admission-0.json"));
    peers.get_mut(&NodeId(0)).unwrap().address = first.address;
    peers.get_mut(&NodeId(2)).unwrap().address = second.address;
    let mut network = NetworkVsrTransport::new(
        NodeId(1),
        &ClusterId::new("cli-cluster").unwrap(),
        PeerIdentity::load(&root.join("tls-1")).unwrap(),
        peers.clone(),
        Duration::from_millis(RPC_MS),
    )
    .unwrap();
    // Ordinary-view-only ancestry remains admitted. A valid client reaches
    // normal follower routing with the exact same local AdminPlane as below.
    let baseline = kv_request(clients[0], "GET admission/key", "before-history", b"");
    assert!(baseline.starts_with("RSF/1 redirect"), "{baseline}");
    assert!(baseline.contains("primary=1"), "{baseline}");
    let identity = std::fs::read(root.join("store-0/IDENTITY.json")).unwrap();
    let decoded: serde_json::Value = serde_json::from_slice(&identity).unwrap();
    assert_eq!(decoded["version"], 1);
    assert!(decoded.get("managed_membership").is_none());

    let request = VsrViewChangeRequest {
        pg: PG,
        previous_view: Epoch(2),
        target_view: Epoch(5),
        candidate: NodeId(1),
        voters: (0..3).map(NodeId).collect(),
    };
    let (input, snapshots) = network
        .prepare_installation_input(&mut candidate, &request)
        .unwrap();
    assert_eq!(
        snapshots.len(),
        3,
        "all independent TLS reporters must participate"
    );
    network
        .stage_installation_input(&mut candidate, &input, &snapshots)
        .unwrap();
    assert_eq!(
        network
            .resume_view_installation(&mut candidate, &mut |_| Ok(()))
            .unwrap(),
        Epoch(5)
    );
    let status = network.replica_status(NodeId(0), PG).unwrap();
    assert_eq!(status.normal.view, Epoch(5));
    assert_eq!(status.normal.voters, request.voters);
    assert_eq!(status.normal.primary, NodeId(1));
    for sequence in 0..3 {
        let rejected = kv_request(
            clients[0],
            "SET admission/key",
            &format!("after-history-{sequence}"),
            b"must not commit",
        );
        assert!(rejected.starts_with("RSF/1 rejected"), "{rejected}");
        assert!(rejected.contains("mutation=not-attempted"), "{rejected}");
        assert!(
            rejected.contains("membership publication requires replicated control admission"),
            "{rejected}"
        );
        // Denial occurs before cell lending; it must not latch application
        // pending and prevent the installed voter from acknowledging probes.
        network
            .confirm_primary(NodeId(0), &candidate.primary_probe().unwrap())
            .unwrap();
    }
    assert_eq!(
        std::fs::read(root.join("store-0/IDENTITY.json")).unwrap(),
        identity
    );
    assert!(!root.join("store-0/membership-1").exists());
    drop(first);
    drop(second);
    let recovered = DurableVsrCell::open_existing(root.join("store-0/pg-1"), PG).unwrap();
    assert!(recovered.has_membership_publication().unwrap());
    assert_eq!(recovered.replicas(), request.voters);
    assert_eq!(recovered.committed_tail(), LogIndex(0));
    assert_eq!(recovered.view(), Epoch(5));
    let server = rsf_rpc::replication::PeerReplicaServer::new(
        NodeId(0),
        &ClusterId::new("cli-cluster").unwrap(),
        PeerIdentity::load(&root.join("tls-0")).unwrap(),
        peers,
        [(PG, recovered)].into(),
        Duration::from_millis(RPC_MS),
    )
    .unwrap();
    assert!(!server.membership_recovery_enabled());
    assert!(server.membership_admission_required().unwrap());
    std::fs::remove_file(root.join("store-0/pg-1/VSR_ACTIVE")).unwrap();
    assert!(server.membership_admission_required().is_err());
    drop(server);
    drop(candidate);
    std::fs::remove_dir_all(root).unwrap();
}
