//! Real native peers resume existing chosen membership; local fixture APIs only
//! construct certified pre-crash custody and valid application command bytes.
use super::super::*;
use rsf_protocol::{ClientRequestId, ReplicatedFabricCommand, VsrBackedFabricRouter};
use rsf_vsr::{
    LearnerHistoryStore, MembershipAuthorityInbox, MembershipDecisionStore, MembershipStagingStore,
    VsrClientRequestId, VsrHistoryBatch, VsrMembershipApproval, VsrMembershipChosen,
    VsrMembershipDecision, VsrMembershipInput, VsrMembershipProposal, VsrMembershipRound,
    VsrMembershipStagedPlan, VsrViewChangeRequest,
};

pub(super) const PG: PlacementGroupId = PlacementGroupId(1);
const RPC_MS: u64 = 800;
pub(super) const RECOVERY_MS: u64 = 20;

pub(super) struct Ready {
    pub(super) root: PathBuf,
    pub(super) peers: BTreeMap<NodeId, PeerEndpoint>,
    pub(super) commands: Vec<ReplicatedFabricCommand>,
    pub(super) request: VsrClientRequestId,
    pub(super) plan: [u8; 32],
    pub(super) input: [u8; 32],
}
impl Drop for Ready {
    fn drop(&mut self) {
        // Declared before peer handles, so unwinding drops those owners first.
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn provision_four(root: &Path) -> BTreeMap<NodeId, PeerEndpoint> {
    let mut params = CertificateParams::new(Vec::new()).unwrap();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    let key = KeyPair::generate().unwrap();
    let ca = params.self_signed(&key).unwrap();
    let issuer = Issuer::new(params, key);
    let mut peers = BTreeMap::new();
    // Reserve both survivor addresses together before writing either config.
    let reservations =
        [2, 3].map(|node| (node, std::net::TcpListener::bind("127.0.0.1:0").unwrap()));
    for node in 0..4 {
        let mut params = CertificateParams::new(vec!["localhost".into()]).unwrap();
        params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ClientAuth,
            ExtendedKeyUsagePurpose::ServerAuth,
        ];
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        let key = KeyPair::generate().unwrap();
        let certificate = params.signed_by(&key, &issuer).unwrap();
        let directory = root.join(format!("tls-{node}"));
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(directory.join("certificate.pem"), certificate.pem()).unwrap();
        std::fs::write(directory.join("key.pem"), key.serialize_pem()).unwrap();
        std::fs::write(directory.join("ca.pem"), ca.pem()).unwrap();
        let identity = PeerIdentity::load(&directory).unwrap();
        let address = reservations
            .iter()
            .find(|(n, _)| *n == node)
            .map(|(_, listener)| listener.local_addr().unwrap())
            .unwrap_or_else(|| "127.0.0.1:1".parse().unwrap());
        peers.insert(
            NodeId(node),
            PeerEndpoint {
                address,
                server_name: "localhost".into(),
                fingerprint: identity.fingerprint,
            },
        );
        let state = root.join(format!("store-{node}"));
        std::fs::create_dir_all(&state).unwrap();
        // This is the pre-crash V1 identity, before managed enrollment.
        std::fs::write(
            state.join("IDENTITY.json"),
            serde_json::to_vec(
                &serde_json::json!({"version":1,"cluster":"cli-cluster","node":node}),
            )
            .unwrap(),
        )
        .unwrap();
    }
    let mapped = peers.iter().map(|(node, peer)| serde_json::json!({
        "node":node.0,"address":peer.address.to_string(),"server_name":peer.server_name,
        "fingerprint":peer.fingerprint.iter().map(|b|format!("{b:02x}")).collect::<String>(),
    })).collect::<Vec<_>>();
    for node in [2, 3] {
        let mut cell = serde_json::json!({
            "pg":1,"view":1,"primary":0,"voters":[0,1,2],
            "membership_recovery":{"interval_ms":RECOVERY_MS},
        });
        if node == 3 {
            cell["learner_source"] = 0.into();
        }
        let config = serde_json::json!({
            "cluster":"cli-cluster","node":node,"state":format!("store-{node}"),
            "tls":format!("tls-{node}"),"listen":peers[&NodeId(node)].address.to_string(),
            "timeout_ms":RPC_MS,"peers":mapped,"cells":[cell],
        });
        std::fs::write(
            root.join(format!("peer-{node}.json")),
            serde_json::to_vec(&config).unwrap(),
        )
        .unwrap();
    }
    drop(reservations);
    peers
}

fn command_fixture(root: &Path) -> Vec<ReplicatedFabricCommand> {
    // Use the surface's public writer to produce real RSFC1/KV encodings; its
    // one-node certificates are never used as authority in the old/new peers.
    let mut router = VsrBackedFabricRouter::open(
        root.join("command-fixture"),
        [PG],
        Epoch(1),
        NodeId(0),
        [NodeId(0)],
    )
    .unwrap();
    let mut kv =
        rsf_kv::DurableKvEngine::open_replicated(root.join("kv-fixture"), Epoch(1), "app").unwrap();
    let values = [
        (0..128 * 1024)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>(),
        b"accepted before original source and candidate disappeared".to_vec(),
        b"committed through the new native voter after election".to_vec(),
    ];
    for (index, value) in values.into_iter().enumerate() {
        let snapshot = kv.snapshot(&router);
        kv.commit_batch(
            &mut router,
            PG,
            rsf_kv::KvBatch {
                client_request: Some(ClientRequestId::new("native-managed", index as u64 + 1)),
                snapshot,
                mutations: vec![rsf_kv::KvMutation::Set {
                    key: b"membership/value".to_vec(),
                    value,
                }],
            },
        )
        .unwrap();
    }
    router.committed_application_commands()
}

fn prepared(cell: &mut DurableVsrCell, command: &ReplicatedFabricCommand) -> rsf_vsr::VsrPrepare {
    let payload = command.encode().unwrap();
    let VsrDecision::Prepared(prepare) = cell
        .prepare(
            NodeId(0),
            VsrOperation::ReplicatedCommand {
                operation_id: command.commit.operation_id,
                payload: payload.clone(),
            },
            payload.len() as u32,
        )
        .unwrap()
    else {
        panic!("old primary prepare");
    };
    assert_eq!(prepare.index, command.commit.index);
    prepare
}

pub(super) fn fixture() -> Ready {
    let root = root();
    let peers = provision_four(&root);
    let commands = command_fixture(&root);
    let mut cells = (0..3)
        .map(|node| {
            DurableVsrCell::open(
                root.join(format!("store-{node}/pg-1")),
                PG,
                Epoch(1),
                NodeId(0),
                (0..3).map(NodeId),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let first = prepared(&mut cells[0], &commands[0]);
    for node in [1, 2] {
        cells[node]
            .receive_prepare(NodeId(node as u64), NodeId(0), first.clone())
            .unwrap();
        cells[0].acknowledge(NodeId(node as u64), Epoch(1), first.index);
    }
    let VsrDecision::Committed(commit) = cells[0].commit(NodeId(0), first.index).unwrap() else {
        panic!("old quorum commit");
    };
    for node in [1, 2] {
        cells[node]
            .receive_commit(NodeId(node as u64), NodeId(0), commit.clone())
            .unwrap();
    }
    let request = VsrClientRequestId::new("original-native-membership", 7);
    cells[0]
        .remember_client_request(request.clone(), first.index)
        .unwrap();
    let accepted = prepared(&mut cells[0], &commands[1]);
    for node in [1, 2] {
        cells[node]
            .receive_prepare(NodeId(node as u64), NodeId(0), accepted.clone())
            .unwrap();
    }
    let mut learner =
        LearnerHistoryStore::open(root.join("store-3/learner-1"), PG, NodeId(3), NodeId(0))
            .unwrap();
    let history = cells[0].history_snapshot().unwrap();
    let history_id = history.descriptor.id().unwrap();
    learner.begin(history.descriptor).unwrap();
    for (ordinal, record) in history.records.into_iter().enumerate() {
        learner.append(history_id, ordinal as u64, record).unwrap();
    }
    learner.finish(history_id).unwrap();
    let proposal = VsrMembershipProposal {
        election: VsrViewChangeRequest {
            pg: PG,
            previous_view: Epoch(1),
            target_view: Epoch(2),
            candidate: NodeId(1),
            voters: (0..3).map(NodeId).collect(),
        },
        old_primary: NodeId(0),
        new_voters: (1..4).map(NodeId).collect(),
    };
    let snapshots = cells
        .iter_mut()
        .enumerate()
        .map(|(node, cell)| {
            cell.begin_membership(NodeId(node as u64), NodeId(0), proposal.clone())
                .unwrap()
        })
        .collect::<Vec<_>>();
    let authority = VsrMembershipApproval::from_snapshots(snapshots[1..].to_vec()).unwrap();
    let chosen_approval = VsrMembershipApproval::from_snapshots(snapshots).unwrap();
    let a = VsrMembershipInput::from_approval(&authority).unwrap();
    let b = VsrMembershipInput::from_approval(&chosen_approval).unwrap();
    assert_ne!(a.id().unwrap(), b.id().unwrap());
    // The old uploader selected A. Surviving journals accepted B, while only
    // node2 learned its chosen certificate before the crash.
    for node in [2, 3] {
        let mut inbox = MembershipAuthorityInbox::open(
            root.join(format!("store-{node}/membership-1/inbox")),
            PG,
            NodeId(node),
        )
        .unwrap();
        inbox.begin(NodeId(1), a.clone()).unwrap();
        let records = a.flattened_records(&authority).unwrap();
        let mut ordinal = 0;
        while ordinal < records.len() {
            let batch = VsrHistoryBatch::pack(ordinal as u64, &records[ordinal..]).unwrap();
            ordinal += batch.records.len();
            inbox.append(NodeId(1), a.id().unwrap(), batch).unwrap();
        }
        inbox.finish(NodeId(1), a.id().unwrap()).unwrap();
    }
    let mut decisions = (1..4)
        .map(|node| {
            MembershipDecisionStore::open(
                root.join(format!("store-{node}/membership-1/decision")),
                NodeId(node),
                &authority,
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
        .map(|store| store.promise(NodeId(1), round).unwrap())
        .collect();
    let decision = VsrMembershipDecision::from_promises(promises, b.clone()).unwrap();
    let records = b.flattened_records(&chosen_approval).unwrap();
    for store in &mut decisions {
        store.begin_input(NodeId(1), round, b.clone()).unwrap();
        let mut ordinal = 0;
        while ordinal < records.len() {
            let batch = VsrHistoryBatch::pack(ordinal as u64, &records[ordinal..]).unwrap();
            ordinal += batch.records.len();
            store
                .append_input(NodeId(1), round, b.id().unwrap(), batch)
                .unwrap();
        }
    }
    let nomination = decisions[0].nominate(&decision).unwrap();
    let mut votes = vec![nomination.clone()];
    for store in &mut decisions[1..] {
        votes.push(store.accept(NodeId(1), &decision, &nomination).unwrap());
    }
    let chosen = VsrMembershipChosen::from_accepted(votes).unwrap();
    decisions[1].learn_chosen(chosen.clone()).unwrap();
    assert!(decisions[2].chosen().is_none());
    let plan = VsrMembershipStagedPlan::select(chosen, chosen_approval).unwrap();
    let id = plan.id();
    let stage = MembershipStagingStore::stage(
        root.join("store-2/membership-1/staging"),
        NodeId(2),
        plan,
        &cells[2],
    )
    .unwrap();
    assert!(stage.report().is_err());
    drop(stage);
    drop(decisions);
    drop(learner);
    drop(cells);
    for path in ["store-0", "store-1", "command-fixture", "kv-fixture"] {
        std::fs::remove_dir_all(root.join(path)).unwrap();
    }
    Ready {
        root,
        peers,
        commands,
        request,
        plan: id,
        input: b.id().unwrap(),
    }
}

pub(super) fn transport(r: &Ready) -> NetworkVsrTransport {
    NetworkVsrTransport::new(
        NodeId(2),
        &ClusterId::new("cli-cluster").unwrap(),
        PeerIdentity::load(&r.root.join("tls-2")).unwrap(),
        r.peers.clone(),
        Duration::from_millis(RPC_MS),
    )
    .unwrap()
}
pub(super) fn config(root: &Path, node: u64, edit: impl FnOnce(&mut serde_json::Value)) {
    let path = root.join(format!("peer-{node}.json"));
    let mut value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    edit(&mut value);
    std::fs::write(path, serde_json::to_vec(&value).unwrap()).unwrap();
}
pub(super) fn wait_published(r: &Ready) {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let second = transport(r);
    let third = NetworkVsrTransport::new(
        NodeId(3),
        &ClusterId::new("cli-cluster").unwrap(),
        PeerIdentity::load(&r.root.join("tls-3")).unwrap(),
        r.peers.clone(),
        Duration::from_millis(RPC_MS),
    )
    .unwrap();
    let mut last = String::new();
    while std::time::Instant::now() < deadline {
        let reports = [
            second.query_membership_publication(NodeId(3), PG, r.plan),
            third.query_membership_publication(NodeId(2), PG, r.plan),
        ];
        if reports.iter().all(Result::is_ok) {
            for report in reports {
                assert_eq!(report.unwrap().input, r.input);
            }
            return;
        }
        last = format!("{reports:?}");
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("native membership publication did not converge: {last}");
}
pub(super) fn rejected_start(path: &Path) -> String {
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
            panic!("invalid managed configuration started a live peer");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let output = child.wait_with_output().unwrap();
    assert!(!output.status.success());
    assert!(!String::from_utf8_lossy(&output.stdout).contains("peer.ready"));
    String::from_utf8(output.stderr).unwrap()
}
