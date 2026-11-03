//! Real native peers resume retained membership; local fixture APIs only
//! construct certified pre-crash custody and valid application command bytes.
use super::super::*;
use rsf_protocol::{
    ClientRequestId, FabricOperation, FabricRouterPort, ReplicatedFabricCommand,
    VsrBackedFabricRouter,
};
use rsf_vsr::{
    LearnerHistoryStore, MembershipAuthorityInbox, MembershipDecisionStore, MembershipStagingStore,
    VsrClientRequestId, VsrHistoryBatch, VsrMembershipApproval, VsrMembershipChosen,
    VsrMembershipDecision, VsrMembershipInput, VsrMembershipProposal, VsrMembershipRound,
    VsrMembershipStagedPlan, VsrViewChangeRequest,
};

pub(super) const PG: PlacementGroupId = PlacementGroupId(1);
pub(super) const RPC_MS: u64 = 800;
pub(super) const RECOVERY_MS: u64 = 20;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Mode {
    PostChoice,
    AcceptedMajority,
    MinorityAccepted,
    MinorityWithPartial,
}

pub(super) struct GroupReady {
    pub(super) pg: PlacementGroupId,
    pub(super) namespace: String,
    pub(super) key: Vec<u8>,
    pub(super) commands: Vec<ReplicatedFabricCommand>,
    pub(super) committed: usize,
    pub(super) value: Vec<u8>,
    pub(super) request: VsrClientRequestId,
    pub(super) plan: [u8; 32],
    pub(super) input: [u8; 32],
    pub(super) initial: [u8; 32],
    pub(super) proposal: VsrMembershipProposal,
    pub(super) bootstrap: [u8; 32],
    pub(super) descriptor: VsrMembershipInput,
    pub(super) records: u64,
    pub(super) batches: usize,
    pub(super) initial_round: VsrMembershipRound,
    pub(super) prefix: Vec<rsf_vsr::VsrHistoryRecord>,
}

pub(super) struct Ready {
    pub(super) root: PathBuf,
    pub(super) peers: BTreeMap<NodeId, PeerEndpoint>,
    pub(super) groups: Vec<GroupReady>,
}
impl Drop for Ready {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!(
                "preserving native membership failure fixture: {}",
                self.root.display()
            );
        } else {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }
}

fn provision_four(root: &Path, groups: &[PlacementGroupId]) -> BTreeMap<NodeId, PeerEndpoint> {
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
        let cells = groups
            .iter()
            .map(|pg| {
                let mut cell = serde_json::json!({
                    "pg":pg.0,"view":1,"primary":0,"voters":[0,1,2],
                    "membership_recovery":{"interval_ms":RECOVERY_MS},
                });
                if node == 3 {
                    cell["learner_source"] = 0.into();
                }
                cell
            })
            .collect::<Vec<_>>();
        let config = serde_json::json!({
            "cluster":"cli-cluster","node":node,"state":format!("store-{node}"),
            "tls":format!("tls-{node}"),"listen":peers[&NodeId(node)].address.to_string(),
            "timeout_ms":RPC_MS,"peers":mapped,"cells":cells,
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

struct CommandFixture {
    commands: Vec<ReplicatedFabricCommand>,
    key: Vec<u8>,
    value: Vec<u8>,
}

fn routed_fixture_key(router: &impl FabricRouterPort, pg: PlacementGroupId) -> Vec<u8> {
    // Keep the original single-group key, then search through the same public
    // routing contract used by DurableKvEngine for a real owner-specific key.
    std::iter::once(b"membership/value".to_vec())
        .chain((0..4096).map(|suffix| format!("membership/value/{suffix}").into_bytes()))
        .find(|key| {
            router.route_operation(&FabricOperation::KvBatch {
                keys: vec![key.clone()],
            }) == Some(pg)
        })
        .unwrap_or_else(|| {
            panic!(
                "no routed native fixture key for PG {} in 4097 candidates",
                pg.0
            )
        })
}

fn command_fixture(
    root: &Path,
    specs: &[(PlacementGroupId, Mode, usize)],
) -> BTreeMap<PlacementGroupId, CommandFixture> {
    // One public router allocates globally unique versions across both PGs.
    // Per-group command envelopes are never rewritten by fixture code.
    let mut router = VsrBackedFabricRouter::open(
        root.join("command-fixture"),
        specs.iter().map(|(pg, _, _)| *pg),
        Epoch(1),
        NodeId(0),
        [NodeId(0)],
    )
    .unwrap();
    let mut fixtures = BTreeMap::new();
    for (pg, _, committed) in specs {
        assert!(*committed > 0);
        let namespace = namespace(*pg);
        let key = routed_fixture_key(&router, *pg);
        // Distinct group values catch cross-group materialization, while PG1
        // retains the original fixture bytes and every payload remains 128 KiB.
        let group_offset = (pg.0.checked_sub(PG.0).unwrap() % 251) as usize;
        let mut kv = rsf_kv::DurableKvEngine::open_replicated(
            root.join("kv-fixture").join(pg.0.to_string()),
            Epoch(1),
            &namespace,
        )
        .unwrap();
        let value = (0..128 * 1024)
            .map(|index| ((index + *committed - 1 + group_offset) % 251) as u8)
            .collect::<Vec<_>>();
        for index in 0..(*committed + 2) {
            let payload = if index < *committed {
                (0..128 * 1024)
                    .map(|byte| ((byte + index + group_offset) % 251) as u8)
                    .collect()
            } else if index == *committed {
                b"accepted before original source and candidate disappeared".to_vec()
            } else {
                b"committed through the new native voter after election".to_vec()
            };
            let snapshot = kv.snapshot(&router);
            kv.commit_batch(
                &mut router,
                *pg,
                rsf_kv::KvBatch {
                    client_request: Some(ClientRequestId::new(
                        format!("native-managed-{}", pg.0),
                        index as u64 + 1,
                    )),
                    snapshot,
                    mutations: vec![rsf_kv::KvMutation::Set {
                        key: key.clone(),
                        value: payload,
                    }],
                },
            )
            .unwrap();
        }
        fixtures.insert(
            *pg,
            CommandFixture {
                commands: Vec::new(),
                key,
                value,
            },
        );
    }
    let commands = router.committed_application_commands();
    for (pg, fixture) in &mut fixtures {
        fixture.commands = commands
            .iter()
            .filter(|command| command.commit.pg == *pg)
            .cloned()
            .collect();
        assert!(fixture.commands.iter().all(|command| {
            command.application.namespace == namespace(*pg)
                && router.route_operation(&command.commit.operation) == Some(*pg)
        }));
    }
    fixtures
}

pub(super) fn namespace(pg: PlacementGroupId) -> String {
    if pg == PG {
        "app".into()
    } else {
        format!("app-pg-{}", pg.0)
    }
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
    fixture_groups(&[(PG, Mode::PostChoice, 1)])
}
pub(super) fn fixture_mode(mode: Mode, committed: usize) -> Ready {
    fixture_groups(&[(PG, mode, committed)])
}
pub(super) fn fixture_groups(specs: &[(PlacementGroupId, Mode, usize)]) -> Ready {
    let root = root();
    eprintln!("native membership fixture: {}", root.display());
    let peers = provision_four(
        &root,
        &specs.iter().map(|(pg, _, _)| *pg).collect::<Vec<_>>(),
    );
    let mut commands = command_fixture(&root, specs);
    let groups = specs
        .iter()
        .map(|(pg, mode, committed)| {
            let command_fixture = commands.remove(pg).unwrap();
            group_fixture(&root, *pg, *mode, *committed, command_fixture)
        })
        .collect();
    for path in ["store-0", "store-1", "command-fixture", "kv-fixture"] {
        std::fs::remove_dir_all(root.join(path)).unwrap();
    }
    Ready {
        root,
        peers,
        groups,
    }
}
fn group_fixture(
    root: &Path,
    pg: PlacementGroupId,
    mode: Mode,
    committed: usize,
    fixture: CommandFixture,
) -> GroupReady {
    let CommandFixture {
        commands,
        key,
        value,
    } = fixture;
    assert_eq!(commands.len(), committed + 2);
    let mut cells = (0..3)
        .map(|node| {
            DurableVsrCell::open(
                root.join(format!("store-{node}/pg-{}", pg.0)),
                pg,
                Epoch(1),
                NodeId(0),
                (0..3).map(NodeId),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    for command in &commands[..committed] {
        let prepared = prepared(&mut cells[0], command);
        for node in [1, 2] {
            cells[node]
                .receive_prepare(NodeId(node as u64), NodeId(0), prepared.clone())
                .unwrap();
            cells[0].acknowledge(NodeId(node as u64), Epoch(1), prepared.index);
        }
        let VsrDecision::Committed(commit) = cells[0].commit(NodeId(0), prepared.index).unwrap()
        else {
            panic!("old quorum commit");
        };
        for node in [1, 2] {
            cells[node]
                .receive_commit(NodeId(node as u64), NodeId(0), commit.clone())
                .unwrap();
        }
    }
    let request = VsrClientRequestId::new(format!("original-native-membership-{}", pg.0), 7);
    cells[0]
        .remember_client_request(request.clone(), LogIndex(1))
        .unwrap();
    let accepted = prepared(&mut cells[0], &commands[committed]);
    for node in [1, 2] {
        cells[node]
            .receive_prepare(NodeId(node as u64), NodeId(0), accepted.clone())
            .unwrap();
    }
    let mut learner = LearnerHistoryStore::open(
        root.join(format!("store-3/learner-{}", pg.0)),
        pg,
        NodeId(3),
        NodeId(0),
    )
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
            pg,
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
    // The old uploader selected A; every pre-choice fixture keeps that exact inbox.
    for node in [2, 3] {
        let mut inbox = MembershipAuthorityInbox::open(
            root.join(format!("store-{node}/membership-{}/inbox", pg.0)),
            pg,
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
                root.join(format!("store-{node}/membership-{}/decision", pg.0)),
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
    let mut batches = 0;
    let mut at = 0;
    while at < records.len() {
        let batch = VsrHistoryBatch::pack(at as u64, &records[at..]).unwrap();
        at += batch.records.len();
        batches += 1;
    }
    for (index, store) in decisions.iter_mut().enumerate() {
        let keep = if index < 2 || matches!(mode, Mode::PostChoice | Mode::AcceptedMajority) {
            records.len()
        } else if mode == Mode::MinorityWithPartial {
            1
        } else {
            0
        };
        if keep == 0 {
            continue;
        }
        store.begin_input(NodeId(1), round, b.clone()).unwrap();
        let mut at = 0;
        while at < keep {
            let batch = VsrHistoryBatch::pack(at as u64, &records[at..keep]).unwrap();
            at += batch.records.len();
            store
                .append_input(NodeId(1), round, b.id().unwrap(), batch)
                .unwrap();
        }
    }
    let nomination = decisions[0].nominate(&decision).unwrap();
    let second = decisions[1]
        .accept(NodeId(1), &decision, &nomination)
        .unwrap();
    let mut votes = vec![nomination.clone(), second];
    if matches!(mode, Mode::PostChoice | Mode::AcceptedMajority) {
        votes.push(
            decisions[2]
                .accept(NodeId(1), &decision, &nomination)
                .unwrap(),
        );
    }
    // The oracle exists only in test memory. Pre-choice modes never persist it,
    // and no later test sends it, stages it or injects publication/readiness.
    let chosen = VsrMembershipChosen::from_accepted(votes).unwrap();
    if mode == Mode::PostChoice {
        decisions[1].learn_chosen(chosen.clone()).unwrap();
    }
    let bootstrap = rsf_core::Digest::blake3_bytes(
        &chosen_approval.snapshots()[0].records[0].encode().unwrap(),
    )
    .bytes;
    let plan = VsrMembershipStagedPlan::select(chosen, chosen_approval).unwrap();
    let id = plan.id();
    assert_eq!(plan.request_commit(&request), Some(LogIndex(1)));
    if mode == Mode::PostChoice {
        let stage = MembershipStagingStore::stage(
            root.join(format!("store-2/membership-{}/staging", pg.0)),
            NodeId(2),
            plan,
            &cells[2],
        )
        .unwrap();
        assert!(stage.report().is_err());
    } else {
        for (index, store) in decisions.iter().enumerate() {
            assert!(
                store.chosen().is_none(),
                "node {} unexpectedly learned a choice",
                index + 1
            );
            assert!(
                !root
                    .join(format!("store-{}/membership-{}/staging", index + 1, pg.0))
                    .exists()
            );
        }
    }
    let prefix = if mode == Mode::MinorityWithPartial {
        records[..1].to_vec()
    } else {
        Vec::new()
    };
    GroupReady {
        pg,
        namespace: namespace(pg),
        key,
        commands,
        committed,
        value,
        request,
        plan: id,
        input: b.id().unwrap(),
        initial: a.id().unwrap(),
        proposal,
        bootstrap,
        records: records.len() as u64,
        descriptor: b,
        batches,
        initial_round: round,
        prefix,
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
        let mut complete = true;
        for g in &r.groups {
            let reports = [
                second.query_membership_publication(NodeId(3), g.pg, g.plan),
                third.query_membership_publication(NodeId(2), g.pg, g.plan),
            ];
            for report in &reports {
                if let Ok(report) = report {
                    assert_eq!(report.input, g.input);
                } else {
                    complete = false;
                }
            }
            last = format!("pg={} {reports:?}", g.pg.0);
        }
        if complete {
            return;
        }
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
