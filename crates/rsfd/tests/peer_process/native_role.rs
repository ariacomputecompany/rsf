//! Native role recovery from complete publication and non-voting partial claims.
//! Opaque pre-RSFC1 payloads exercise peer durability without claiming client admission.
use super::*;
use rsf_rpc::replication::PeerReplicaServer;
use rsf_vsr::{
    LearnerHistoryStore, MembershipDecisionStore, MembershipStagingStore, VsrMembershipApproval,
    VsrMembershipChosen, VsrMembershipDecision, VsrMembershipInput, VsrMembershipProposal,
    VsrMembershipRound, VsrMembershipStagedPlan, VsrMembershipStagedQuorum, VsrViewChangeRequest,
};

struct Ready {
    root: PathBuf,
    peers: BTreeMap<NodeId, PeerEndpoint>,
    primary: DurableVsrCell,
    promoted: DurableVsrCell,
}
fn fixture() -> Ready {
    let root = root();
    let mut peers = provision(&root);
    for node in [1, 2] {
        let path = root.join(format!("peer-{node}.json"));
        let mut config: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        config["cells"][0] = serde_json::json!({"pg":1,"view":1,"primary":0,"voters":[0,1]});
        if node == 2 {
            config["cells"][0]["learner_source"] = 0.into();
        }
        std::fs::write(path, serde_json::to_vec(&config).unwrap()).unwrap();
    }
    let first = Process::start(&root.join("peer-1.json"));
    let learner_process = Process::start(&root.join("peer-2.json"));
    peers.get_mut(&NodeId(1)).unwrap().address = first.address;
    peers.get_mut(&NodeId(2)).unwrap().address = learner_process.address;
    let network = |local, peers| {
        NetworkVsrTransport::new(
            NodeId(local),
            &ClusterId::new("cli-cluster").unwrap(),
            PeerIdentity::load(&root.join(format!("tls-{local}"))).unwrap(),
            peers,
            Duration::from_millis(PEER_TIMEOUT_MS),
        )
        .unwrap()
    };
    let mut source = DurableVsrCell::open(
        root.join("leader"),
        PlacementGroupId(1),
        Epoch(1),
        NodeId(0),
        [NodeId(0), NodeId(1)],
    )
    .unwrap();
    let payload = b"opaque durable payload before native promotion";
    let VsrDecision::Prepared(prepared) = source
        .prepare(
            NodeId(0),
            VsrOperation::ReplicatedCommand {
                operation_id: OperationId(1),
                payload: payload.to_vec(),
            },
            payload.len() as u32,
        )
        .unwrap()
    else {
        panic!("prepare")
    };
    source
        .replicate_with_transport(NodeId(0), prepared.index, &mut network(0, peers.clone()))
        .unwrap();
    source
        .remember_client_request(
            rsf_vsr::VsrClientRequestId::new("before-native-promotion", 1),
            LogIndex(1),
        )
        .unwrap();
    network(0, peers.clone())
        .sync_learner(NodeId(2), &mut source)
        .unwrap();
    let proposal = VsrMembershipProposal {
        election: VsrViewChangeRequest {
            pg: PlacementGroupId(1),
            previous_view: Epoch(1),
            target_view: Epoch(2),
            candidate: NodeId(1),
            voters: vec![NodeId(0), NodeId(1)],
        },
        old_primary: NodeId(0),
        new_voters: (0..3).map(NodeId).collect(),
    };
    let approval = network(0, peers.clone())
        .approve_membership(&mut source, &proposal)
        .unwrap();
    drop(first);
    drop(learner_process);
    let mut primary =
        DurableVsrCell::open_existing(root.join("store-1/pg-1"), PlacementGroupId(1)).unwrap();
    let learner = LearnerHistoryStore::open(
        root.join("store-2/learner-1"),
        PlacementGroupId(1),
        NodeId(2),
        NodeId(0),
    )
    .unwrap();
    let mut decisions = [1, 2]
        .into_iter()
        .map(|node| {
            MembershipDecisionStore::open(
                root.join(format!("store-{node}/transition-1/decision")),
                NodeId(node),
                &approval,
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let round = VsrMembershipRound {
        counter: 1,
        coordinator: NodeId(1),
    };
    let promises = decisions
        .iter_mut()
        .map(|s| s.promise(NodeId(1), round).unwrap())
        .collect();
    let decision = VsrMembershipDecision::from_promises(
        promises,
        VsrMembershipInput::from_approval(&approval).unwrap(),
    )
    .unwrap();
    let nominated = decisions[0].nominate(&decision).unwrap();
    let accepted = decisions[1]
        .accept(NodeId(1), &decision, &nominated)
        .unwrap();
    let chosen = VsrMembershipChosen::from_accepted(vec![nominated, accepted]).unwrap();
    let plan = || {
        VsrMembershipStagedPlan::select(
            chosen.clone(),
            VsrMembershipApproval::from_snapshots(approval.snapshots().to_vec()).unwrap(),
        )
        .unwrap()
    };
    let mut first_stage = MembershipStagingStore::stage(
        root.join("store-1/transition-1/staging"),
        NodeId(1),
        plan(),
        &primary,
    )
    .unwrap();
    let mut learner_stage = learner
        .stage_membership(root.join("store-2/transition-1/staging"), plan())
        .unwrap();
    // These opaque replication entries have no derived application decoder.
    let first_report = first_stage.materialize(&mut |_| Ok(())).unwrap();
    let learner_report = learner_stage.materialize(&mut |_| Ok(())).unwrap();
    let quorum =
        VsrMembershipStagedQuorum::from_reports(vec![first_report, learner_report]).unwrap();
    let promoted = learner
        .promote_membership(root.join("store-2/pg-1"), &learner_stage, &quorum)
        .unwrap();
    primary.install_membership(&first_stage, &quorum).unwrap();
    drop(learner_stage);
    drop(first_stage);
    drop(decisions);
    drop(learner);
    drop(source);
    std::fs::remove_dir_all(root.join("leader")).unwrap();
    Ready {
        root,
        peers,
        primary,
        promoted,
    }
}
fn rejected_start(path: &Path) -> std::process::Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_rsfd"))
        .args(["peer", "serve", "--config"])
        .arg(path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while child.try_wait().unwrap().is_none() {
        if std::time::Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("invalid role/timing configuration started a live peer");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let output = child.wait_with_output().unwrap();
    assert!(!output.status.success());
    output
}

#[test]
fn native_peer_recovers_certified_promotion_and_rejects_foreign_owner_or_group() {
    let mut r = fixture();
    let wrong = PeerReplicaServer::new(
        NodeId(1),
        &ClusterId::new("cli-cluster").unwrap(),
        PeerIdentity::load(&r.root.join("tls-1")).unwrap(),
        r.peers.clone(),
        [(PlacementGroupId(1), r.promoted)].into(),
        Duration::from_millis(800),
    )
    .expect_err("another authenticated voter must not host this promotion");
    assert!(wrong.contains("promotion owner"), "{wrong}");
    r.promoted =
        DurableVsrCell::open_existing(r.root.join("store-2/pg-1"), PlacementGroupId(1)).unwrap();
    let wrong = PeerReplicaServer::new(
        NodeId(2),
        &ClusterId::new("cli-cluster").unwrap(),
        PeerIdentity::load(&r.root.join("tls-2")).unwrap(),
        r.peers.clone(),
        [(PlacementGroupId(9), r.promoted)].into(),
        Duration::from_millis(800),
    )
    .expect_err("a durable group cannot be remapped");
    assert!(wrong.contains("hosted group"), "{wrong}");
    // Restart must rely on the promoted target, not recreate the old learner.
    std::fs::remove_dir_all(r.root.join("store-2/learner-1")).unwrap();
    let peer = Process::start(&r.root.join("peer-2.json"));
    r.peers.get_mut(&NodeId(2)).unwrap().address = peer.address;
    assert!(!r.root.join("store-2/learner-1").exists());
    let mut network = transport_without_cell(&r.root, &r.peers);
    let bytes = b"committed through native promoted voter";
    let VsrDecision::Prepared(prepared) = r
        .primary
        .prepare(
            NodeId(1),
            VsrOperation::ReplicatedCommand {
                operation_id: OperationId(2),
                payload: bytes.to_vec(),
            },
            bytes.len() as u32,
        )
        .unwrap()
    else {
        panic!("new view prepare")
    };
    r.primary
        .replicate_with_transport(NodeId(1), prepared.index, &mut network)
        .unwrap();
    let status = network
        .replica_status(NodeId(2), PlacementGroupId(1))
        .unwrap();
    assert_eq!(status.normal.committed_tail, LogIndex(2));
    assert_eq!(status.normal.view, Epoch(2));
    drop(peer);
    let recovered =
        DurableVsrCell::open_existing(r.root.join("store-2/pg-1"), PlacementGroupId(1)).unwrap();
    assert_eq!(recovered.promoted_replica().unwrap(), Some(NodeId(2)));
    assert_eq!(
        recovered.committed_operation(LogIndex(2)),
        Some(&prepared.operation)
    );
    assert_eq!(
        recovered.request_commit(&rsf_vsr::VsrClientRequestId::new(
            "before-native-promotion",
            1
        )),
        Some(LogIndex(1))
    );
    drop(recovered);
    drop(r.primary);
    std::fs::remove_dir_all(r.root).unwrap();
}
fn transport_without_cell(
    root: &Path,
    peers: &BTreeMap<NodeId, PeerEndpoint>,
) -> NetworkVsrTransport {
    NetworkVsrTransport::new(
        NodeId(1),
        &ClusterId::new("cli-cluster").unwrap(),
        PeerIdentity::load(&root.join("tls-1")).unwrap(),
        peers.clone(),
        Duration::from_millis(PEER_TIMEOUT_MS),
    )
    .unwrap()
}

#[test]
fn native_peer_keeps_an_unpublished_owned_promotion_nonvoting_then_recovers_after_publication() {
    let mut r = fixture();
    drop(r.promoted);
    let target = r.root.join("store-2/pg-1");
    for name in ["VSR_ACTIVE", "VSR_PUBLISHED"] {
        std::fs::rename(
            target.join(name),
            target.join(format!("{name}.staged-fixture")),
        )
        .unwrap();
    }
    // This is the sealed/staged filesystem boundary exercised with real SIGKILL
    // by the VSR factory tests; keep both target bytes and the learner anchor.
    let peer = Process::start(&r.root.join("peer-2.json"));
    r.peers.get_mut(&NodeId(2)).unwrap().address = peer.address;
    let rejected = transport_without_cell(&r.root, &r.peers)
        .replica_status(NodeId(2), PlacementGroupId(1))
        .unwrap_err();
    assert!(
        rejected.reason.contains("not hosted as a voter"),
        "{}",
        rejected.reason
    );
    assert!(!target.join("MANIFEST").exists());
    assert!(!target.join("VSR_ACTIVE").exists());
    drop(peer);
    for name in ["VSR_ACTIVE", "VSR_PUBLISHED"] {
        std::fs::rename(
            target.join(format!("{name}.staged-fixture")),
            target.join(name),
        )
        .unwrap();
    }
    let peer = Process::start(&r.root.join("peer-2.json"));
    r.peers.get_mut(&NodeId(2)).unwrap().address = peer.address;
    assert_eq!(
        transport_without_cell(&r.root, &r.peers)
            .replica_status(NodeId(2), PlacementGroupId(1))
            .unwrap()
            .normal
            .view,
        Epoch(2)
    );
    drop(peer);
    drop(r.primary);
    std::fs::remove_dir_all(r.root).unwrap();
}

#[test]
fn native_peer_rejects_a_valid_promoted_store_under_another_node_identity() {
    let r = fixture();
    drop(r.promoted);
    drop(r.primary);
    std::fs::rename(r.root.join("store-1/pg-1"), r.root.join("saved-primary")).unwrap();
    std::fs::rename(r.root.join("store-2/pg-1"), r.root.join("store-1/pg-1")).unwrap();
    let output = rejected_start(&r.root.join("peer-1.json"));
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("promotion owner"),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    std::fs::remove_dir_all(r.root).unwrap();
}

#[test]
fn native_peer_rejects_corrupt_publication_without_downgrading_or_recreating_authority() {
    let r = fixture();
    drop(r.promoted);
    let target = r.root.join("store-2/pg-1");
    let names = ["VSR_PROMOTION", "VSR_ACTIVE", "VSR_PUBLISHED"];
    let original = names.map(|name| std::fs::read(target.join(name)).unwrap());
    for damage in [
        "missing-pointer",
        "corrupt-pointer",
        "missing-owner",
        "corrupt-owner",
        "pending-foreign-owner",
        "pending-foreign-bootstrap",
    ] {
        for (name, bytes) in names.iter().zip(&original) {
            std::fs::write(target.join(name), bytes).unwrap();
        }
        match damage {
            "missing-pointer" => std::fs::remove_file(target.join("VSR_ACTIVE")).unwrap(),
            "corrupt-pointer" => std::fs::write(target.join("VSR_ACTIVE"), b"invalid").unwrap(),
            "missing-owner" => std::fs::remove_file(target.join("VSR_PROMOTION")).unwrap(),
            "corrupt-owner" => std::fs::write(target.join("VSR_PROMOTION"), b"invalid").unwrap(),
            _ => {
                // A valid checksum cannot authorize another node or bootstrap,
                // even before the target has an active pointer.
                std::fs::remove_file(target.join("VSR_ACTIVE")).unwrap();
                std::fs::remove_file(target.join("VSR_PUBLISHED")).unwrap();
                let mut owner = original[0].clone();
                if damage == "pending-foreign-owner" {
                    owner[14..22].copy_from_slice(&1_u64.to_le_bytes());
                } else {
                    owner[54..86].fill(9);
                }
                let sum = rsf_core::Digest::blake3_bytes(&owner[..86]);
                owner[86..].copy_from_slice(sum.as_bytes());
                std::fs::write(target.join("VSR_PROMOTION"), owner).unwrap();
            }
        }
        let before = names.map(|name| std::fs::read(target.join(name)).ok());
        for _ in 0..2 {
            let output = rejected_start(&r.root.join("peer-2.json"));
            assert!(!String::from_utf8_lossy(&output.stdout).contains("peer.ready"));
            assert_eq!(
                names.map(|name| std::fs::read(target.join(name)).ok()),
                before,
                "startup rewrote damaged authority: {damage}"
            );
            assert!(!target.join("MANIFEST").exists());
        }
    }
    drop(r.primary);
    std::fs::remove_dir_all(r.root).unwrap();
}

#[test]
fn native_peer_requires_explicit_promoted_voter_configuration_for_mixed_group_elections() {
    let r = fixture();
    drop(r.promoted);
    let path = r.root.join("peer-2.json");
    let mut config: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    config["cells"]
        .as_array_mut()
        .unwrap()
        .push(serde_json::json!({"pg":9,"view":1,"primary":0,"voters":[0,1,2]}));
    config["timeout_ms"] = 100.into();
    config["elections"] =
        serde_json::json!({"heartbeat_ms":50,"timeout_ms":800,"rank_delay_ms":100});
    std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
    let output = rejected_start(&path);
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("explicit voting configuration"),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!String::from_utf8_lossy(&output.stdout).contains("peer.ready"));
    drop(r.primary);
    std::fs::remove_dir_all(r.root).unwrap();
}

#[test]
fn native_peer_validates_election_budget_against_recovered_membership() {
    let r = fixture();
    drop(r.promoted);
    let path = r.root.join("peer-2.json");
    let mut config: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    config["cells"][0] = serde_json::json!({"pg":1,"view":2,"primary":1,"voters":[1,2]});
    config["timeout_ms"] = 100.into();
    config["elections"] =
        serde_json::json!({"heartbeat_ms":50,"timeout_ms":500,"rank_delay_ms":100});
    std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
    let output = rejected_start(&path);
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("election timeout must cover"),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        !String::from_utf8_lossy(&output.stdout).contains("peer.ready"),
        "invalid recovered timing bound a listener"
    );
    drop(r.primary);
    std::fs::remove_dir_all(r.root).unwrap();
}

#[test]
fn native_published_voters_require_admission_even_with_unmanaged_v1_identity() {
    let r = fixture();
    assert!(r.primary.has_membership_publication().unwrap());
    assert!(r.promoted.has_membership_publication().unwrap());
    drop(r.primary);
    drop(r.promoted);
    let control = initialize_network_state(&r.root, &r.peers);
    let reservations = (0..3)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect::<Vec<_>>();
    let addresses = reservations
        .iter()
        .map(|listener| listener.local_addr().unwrap())
        .collect::<Vec<_>>();
    drop(reservations);
    for node in [1, 2] {
        let state = r.root.join(format!("store-{node}"));
        let identity = std::fs::read(state.join("IDENTITY.json")).unwrap();
        let decoded: serde_json::Value = serde_json::from_slice(&identity).unwrap();
        assert_eq!(decoded["version"], 1);
        assert!(decoded.get("managed_membership").is_none());
        assert!(!state.join("membership-1").exists());
        let pointer = std::fs::read(state.join("pg-1/VSR_ACTIVE")).unwrap();
        let path = r.root.join(format!("peer-{node}.json"));
        let mut config: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        config["cells"][0] = serde_json::json!({"pg":1,"view":2,"primary":1,"voters":[0,1,2]});
        config["timeout_ms"] = 800.into();
        config["elections"] =
            serde_json::json!({"heartbeat_ms":50,"timeout_ms":8000,"rank_delay_ms":1000});
        config["client"] = serde_json::json!({
            "state":control,"listen":addresses[node].to_string(),
            "endpoints":addresses.iter().enumerate().map(|(node,address)|
                serde_json::json!({"node":node,"address":address.to_string()})).collect::<Vec<_>>()
        });
        std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
        for _ in 0..2 {
            let output = rejected_start(&path);
            assert!(
                String::from_utf8_lossy(&output.stderr)
                    .contains("membership publication requires replicated control admission"),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(!String::from_utf8_lossy(&output.stdout).contains("peer.ready"));
            assert_eq!(
                std::fs::read(state.join("IDENTITY.json")).unwrap(),
                identity
            );
            assert_eq!(
                std::fs::read(state.join("pg-1/VSR_ACTIVE")).unwrap(),
                pointer
            );
            assert!(!state.join("client-protocol").exists());
        }
    }
    std::fs::remove_dir_all(r.root).unwrap();
}
