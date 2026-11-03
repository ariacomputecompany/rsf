//! Real TLS peers retain publication input, hand off learner custody, and keep
//! historical publication acknowledgements distinct from current primary votes.
use super::*;
use rsf_vsr::{
    LearnerHistoryStore, MembershipDecisionStore, MembershipStagingStore, VsrClientRequestId,
    VsrMembershipApproval, VsrMembershipChosen, VsrMembershipInput, VsrMembershipProposal,
    VsrMembershipRound, VsrMembershipStagedPlan, VsrMembershipStagedQuorum, VsrViewChangeRequest,
};

const PG: PlacementGroupId = PlacementGroupId(1);

fn proposal() -> VsrMembershipProposal {
    VsrMembershipProposal {
        election: VsrViewChangeRequest {
            pg: PG,
            previous_view: Epoch(1),
            target_view: Epoch(2),
            candidate: NodeId(1),
            voters: vec![NodeId(0), NodeId(1), NodeId(2)],
        },
        old_primary: NodeId(0),
        new_voters: vec![NodeId(1), NodeId(2), NodeId(3)],
    }
}
fn peers(root: &Path) -> BTreeMap<NodeId, PeerEndpoint> {
    let mut result = endpoints(root);
    result.insert(
        NodeId(3),
        PeerEndpoint {
            address: "127.0.0.1:1".parse().unwrap(),
            server_name: "localhost".into(),
            fingerprint: identity(root, 3).fingerprint,
        },
    );
    result
}
fn transport_for(
    root: &Path,
    local: u64,
    peers: BTreeMap<NodeId, PeerEndpoint>,
) -> NetworkVsrTransport {
    NetworkVsrTransport::new(
        NodeId(local),
        &cluster(),
        identity(root, local),
        peers,
        Duration::from_millis(800),
    )
    .unwrap()
}

fn server(
    root: &Path,
    node: u64,
    learner: bool,
    peers: BTreeMap<NodeId, PeerEndpoint>,
) -> Result<PeerReplicaServer, String> {
    let target = root.join(format!("store-{node}"));
    let mut cells = BTreeMap::new();
    let mut learners = BTreeMap::new();
    if learner {
        match std::fs::symlink_metadata(&target) {
            Ok(_) => {
                // A stale fixture flag cannot recreate a learner over a complete
                // target or downgrade a damaged published owner/pointer.
                let cell = DurableVsrCell::open_existing(&target, PG).map_err(|e| e.message)?;
                if cell.promoted_replica().map_err(|e| e.message)? != Some(NodeId(node))
                    || !cell.contains_replica(NodeId(node))
                {
                    return Err("publication fixture recovered a foreign promotion owner".into());
                }
                cells.insert(PG, cell);
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                learners.insert(
                    PG,
                    LearnerHistoryStore::open(
                        root.join(format!("learner-{node}")),
                        PG,
                        NodeId(node),
                        NodeId(0),
                    )
                    .map_err(|e| e.message)?,
                );
            }
            Err(e) => return Err(e.to_string()),
        }
    } else {
        cells.insert(PG, cell(root, node));
    }
    let server = PeerReplicaServer::with_learners(
        NodeId(node),
        &cluster(),
        identity(root, node),
        peers,
        cells,
        learners,
        Duration::from_millis(800),
    )?
    .with_membership_paths([(PG, root.join(format!("membership-{node}")))].into())?;
    if learner {
        server.with_promotion_paths([(PG, target)].into())
    } else {
        Ok(server)
    }
}

struct PublicationPeer {
    child: Child,
    address: SocketAddr,
}
impl PublicationPeer {
    fn start(root: &Path, node: u64, learner: bool, apply: bool) -> Result<Self, String> {
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "replication::tests::membership_publication::publication_process",
                "--nocapture",
            ])
            .env("RSF_PUBLICATION_TEST_ROOT", root)
            .env("RSF_PUBLICATION_TEST_NODE", node.to_string())
            .env(
                "RSF_PUBLICATION_TEST_LEARNER",
                if learner { "1" } else { "0" },
            )
            .env("RSF_PUBLICATION_TEST_APPLY", if apply { "1" } else { "0" })
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|e| e.to_string())?;
        let stdout = child.stdout.take().unwrap();
        let (send, receive) = std::sync::mpsc::channel();
        let reader = std::thread::spawn(move || {
            for line in std::io::BufReader::new(stdout).lines() {
                let Ok(line) = line else {
                    break;
                };
                if let Some(address) = line.strip_prefix("RSF_PUBLICATION_READY=") {
                    let _ = send.send(address.parse::<SocketAddr>());
                    break;
                }
            }
        });
        let ready = receive.recv_timeout(Duration::from_secs(10));
        if !matches!(&ready, Ok(Ok(_))) {
            let _ = child.kill();
            let status = child.wait().map_err(|e| e.to_string())?;
            reader.join().unwrap();
            return Err(format!(
                "publication peer startup failed: {ready:?}; {status}"
            ));
        }
        reader.join().unwrap();
        Ok(Self {
            child,
            address: ready.unwrap().unwrap(),
        })
    }
}
impl Drop for PublicationPeer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
#[test]
fn publication_process() {
    let Some(root) = std::env::var_os("RSF_PUBLICATION_TEST_ROOT") else {
        return;
    };
    let root = PathBuf::from(root);
    let node = std::env::var("RSF_PUBLICATION_TEST_NODE")
        .unwrap()
        .parse::<u64>()
        .unwrap();
    let learner = std::env::var("RSF_PUBLICATION_TEST_LEARNER").as_deref() == Ok("1");
    let apply = std::env::var("RSF_PUBLICATION_TEST_APPLY").as_deref() == Ok("1");
    let mut server = server(&root, node, learner, peers(&root)).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    println!("RSF_PUBLICATION_READY={}", listener.local_addr().unwrap());
    std::io::stdout().flush().unwrap();
    for socket in listener.incoming() {
        let socket = socket.unwrap();
        let result = if apply {
            server.serve_connection_with_apply(socket, &mut |entry| {
                if root.join(format!("fail-apply-{node}")).exists() {
                    return Err("injected publication materializer failure".into());
                }
                persist_staged_entry(&root, node, entry)
            })
        } else {
            server.serve_connection(socket)
        };
        if let Err(error) = result {
            eprintln!("publication peer {node}: {error}");
        }
    }
}

struct Ready {
    root: PathBuf,
    peers: BTreeMap<NodeId, PeerEndpoint>,
    local: Option<PeerReplicaServer>,
    remote: Option<PublicationPeer>,
    chosen: VsrMembershipChosen,
    quorum: VsrMembershipStagedQuorum,
    alternate: VsrMembershipStagedQuorum,
    committed: VsrPrepare,
    accepted: VsrPrepare,
    request: VsrClientRequestId,
}
impl Ready {
    fn local(&mut self) -> &mut PeerReplicaServer {
        self.local.as_mut().unwrap()
    }
    fn network(&self) -> NetworkVsrTransport {
        transport_for(&self.root, 2, self.peers.clone())
    }
    fn restart_local(&mut self) {
        drop(self.local.take());
        self.local = Some(server(&self.root, 2, false, self.peers.clone()).unwrap());
    }
    fn restart_remote(&mut self, apply: bool) {
        drop(self.remote.take());
        let peer = PublicationPeer::start(&self.root, 3, true, apply).unwrap();
        self.peers.get_mut(&NodeId(3)).unwrap().address = peer.address;
        self.remote = Some(peer);
        let network = self.network();
        if let Some(local) = &mut self.local {
            local.control_transport = network;
        }
    }
    fn publish(&mut self) {
        let path = self.root.clone();
        let quorum = self.quorum.clone();
        let local = self
            .local()
            .publish_membership_locally(&quorum, &mut |entry| persist_staged_entry(&path, 2, entry))
            .unwrap();
        let proof = self
            .network()
            .collect_membership_publications(local, &quorum)
            .unwrap();
        assert_eq!(
            proof
                .reports()
                .iter()
                .map(|r| r.replica)
                .collect::<Vec<_>>(),
            vec![NodeId(2), NodeId(3)]
        );
    }
}
impl Drop for Ready {
    fn drop(&mut self) {
        drop(self.remote.take());
        drop(self.local.take());
        let _ = std::fs::remove_dir_all(&self.root);
    }
}
fn fixture() -> Ready {
    let root = root("native-membership-publication");
    provision(&root);
    let first = PublicationPeer::start(&root, 1, false, true).unwrap();
    let second = PublicationPeer::start(&root, 2, false, true).unwrap();
    let learner = PublicationPeer::start(&root, 3, true, true).unwrap();
    let mut peers = peers(&root);
    for (node, address) in [
        (1, first.address),
        (2, second.address),
        (3, learner.address),
    ] {
        peers.get_mut(&NodeId(node)).unwrap().address = address;
    }
    let mut source = cell(&root, 0);
    let mut old_network = transport_for(&root, 0, peers.clone());
    let payload = (0..128 * 1024).map(|n| (n % 251) as u8).collect::<Vec<_>>();
    let committed = prepare(&mut source, &payload);
    source
        .replicate_with_transport(NodeId(0), committed.index, &mut old_network)
        .unwrap();
    let request = VsrClientRequestId::new("original-before-publication", 7);
    source
        .remember_client_request(request.clone(), committed.index)
        .unwrap();
    let accepted = prepare(
        &mut source,
        b"accepted before source and nominated candidate disappear",
    );
    for node in [1, 2] {
        old_network
            .acknowledge_prepare(VsrPrepareMessage {
                replica: NodeId(node),
                prepare: accepted.clone(),
            })
            .unwrap();
    }
    old_network.sync_learner(NodeId(3), &mut source).unwrap();
    let approval = old_network
        .approve_membership(&mut source, &proposal())
        .unwrap();
    // Initial authority A omits source0's unique receipt; accepted input B has it.
    // Cold recovery must retain B, not validate publication only against A's ID.
    let authority =
        VsrMembershipApproval::from_snapshots(approval.snapshots()[1..].to_vec()).unwrap();
    assert_ne!(
        VsrMembershipInput::from_approval(&authority)
            .unwrap()
            .id()
            .unwrap(),
        VsrMembershipInput::from_approval(&approval)
            .unwrap()
            .id()
            .unwrap()
    );
    drop(first);
    let anchor = cell(&root, 1);
    let mut decision =
        MembershipDecisionStore::open(root.join("membership-1/decision"), NodeId(1), &authority)
            .unwrap();
    let candidate = transport_for(&root, 1, peers.clone());
    let chosen = candidate
        .choose_membership_plan(
            &mut decision,
            VsrMembershipRound {
                counter: 1,
                coordinator: NodeId(1),
            },
            &approval,
        )
        .unwrap();
    let plan = VsrMembershipStagedPlan::select(chosen.clone(), approval).unwrap();
    let mut stage =
        MembershipStagingStore::stage(root.join("membership-1/staging"), NodeId(1), plan, &anchor)
            .unwrap();
    let report = stage
        .materialize(&mut |entry| {
            persist_staged_entry(&root, 1, entry).map_err(|reason| {
                rsf_core::RsfError::new(rsf_core::ErrorCode::InvalidInput, reason)
            })
        })
        .unwrap();
    let alternate = candidate
        .collect_membership_stages(report, stage.plan())
        .unwrap();
    assert_eq!(alternate.reports().len(), 3);
    drop(stage);
    drop(decision);
    drop(anchor);
    drop(source);
    drop(old_network);
    drop(candidate);
    for path in ["store-0", "store-1", "membership-1"] {
        std::fs::remove_dir_all(root.join(path)).unwrap();
    }
    drop(second);
    let mut local = server(&root, 2, false, peers.clone()).unwrap();
    let chosen = local
        .with_membership_decision(PG, |decision, network| {
            let retained = decision.approval(chosen.input_id()).unwrap();
            network.choose_membership_plan(
                decision,
                VsrMembershipRound {
                    counter: 2,
                    coordinator: NodeId(2),
                },
                &retained,
            )
        })
        .unwrap()
        .unwrap();
    let report = local
        .stage_membership_locally(chosen.clone(), &mut |entry| {
            persist_staged_entry(&root, 2, entry)
        })
        .unwrap();
    let quorum = transport_for(&root, 2, peers.clone())
        .collect_membership_stages(report, local.membership_staging[&PG].plan())
        .unwrap();
    assert_eq!(
        quorum
            .reports()
            .iter()
            .map(|r| r.replica)
            .collect::<Vec<_>>(),
        vec![NodeId(2), NodeId(3)]
    );
    assert_eq!(quorum.plan_id(), alternate.plan_id());
    assert_eq!(quorum.proposal().election.candidate, NodeId(1));
    Ready {
        root,
        peers,
        local: Some(local),
        remote: Some(learner),
        chosen,
        quorum,
        alternate,
        committed,
        accepted,
        request,
    }
}

#[derive(Debug, Eq, PartialEq)]
struct Inventory {
    pointer: Vec<u8>,
    generations: Vec<String>,
    manifest: Vec<u8>,
    segments: BTreeMap<PathBuf, Vec<u8>>,
    input: Vec<u8>,
    metadata: Vec<u8>,
}
fn inventory(root: &Path, node: u64) -> Inventory {
    let store = root.join(format!("store-{node}"));
    let pointer = std::fs::read(store.join("VSR_ACTIVE")).unwrap();
    let generation = pointer[12..44]
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    let mut generations = std::fs::read_dir(store.join("vsr-views"))
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect::<Vec<_>>();
    generations.sort();
    // Every caller observes a completed, serialized operation. Read bytes only:
    // opening a second SegmentStore would contend with the child peer's lock
    // and recovery could repair a torn tail before this assertion observes it.
    // Exact manifest/segment equality detects extra or rewritten records, not
    // merely a changed WAL record count. Lock sidecars contain no log state.
    let active = store.join("vsr-views").join(generation);
    let manifest = std::fs::read(active.join("MANIFEST")).unwrap();
    let segments = std::fs::read_dir(active.join("segments"))
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            assert!(entry.file_type().unwrap().is_file());
            (PathBuf::from(entry.file_name()), entry.path())
        })
        .filter(|(name, _)| name.extension() != Some(std::ffi::OsStr::new("lock")))
        .map(|(name, path)| (name, std::fs::read(path).unwrap()))
        .collect::<BTreeMap<_, _>>();
    assert!(!segments.is_empty());
    assert_eq!(std::fs::read(store.join("VSR_ACTIVE")).unwrap(), pointer);
    Inventory {
        pointer,
        generations,
        manifest,
        segments,
        input: std::fs::read(root.join(format!("membership-{node}/staging/PUBLICATION"))).unwrap(),
        metadata: std::fs::read(root.join(format!("membership-{node}/staging/META"))).unwrap(),
    }
}

#[test]
fn real_tls_membership_publication_survives_candidate_loss_cold_roles_and_newer_views() {
    let mut r = fixture();
    let path = r.root.clone();
    let original_request = r.request.clone();
    let accepted_operation = r.accepted.operation.clone();
    r.publish();
    let published = r.network().replica_status(NodeId(3), PG).unwrap();
    assert_eq!(published.normal.view, Epoch(2));
    assert_eq!(published.normal.voters, proposal().new_voters);
    assert_eq!(published.normal.committed_tail, LogIndex(1));
    assert_eq!(
        r.local().cells[&PG]
            .prepared(LogIndex(2))
            .unwrap()
            .operation,
        accepted_operation
    );
    assert_eq!(
        r.local().cells[&PG].request_commit(&original_request),
        Some(LogIndex(1))
    );
    let first_inputs = [2, 3].map(|node| inventory(&path, node).input);
    // The original source and nominated primary cannot help any following step.
    for name in ["store-0", "store-1", "membership-1"] {
        assert!(!path.join(name).exists());
    }
    drop(r.remote.take());
    std::fs::remove_dir_all(path.join("learner-3")).unwrap();
    for node in [2, 3] {
        std::fs::remove_dir_all(path.join(format!("materialized-{node}"))).unwrap();
    }
    r.restart_local();
    r.restart_remote(true);
    assert!(
        !path.join("learner-3").exists(),
        "cold promotion recreated the retired learner"
    );
    let id = r.quorum.plan_id();
    let local = r
        .local()
        .query_membership_publication(PG, id, &mut |entry| persist_staged_entry(&path, 2, entry))
        .unwrap();
    let remote = r
        .network()
        .query_membership_publication(NodeId(3), PG, id)
        .unwrap();
    assert_eq!(local.plan, remote.plan);
    for node in [2, 3] {
        assert_eq!(
            std::fs::read(path.join(format!("materialized-{node}/1"))).unwrap(),
            r.committed.encode().unwrap()
        );
    }

    // Establish a successful same-boot stage receipt before later authority
    // changes invalidate its cache and force current-prefix materialization.
    let staged = r
        .network()
        .exchange_once(NodeId(3), MEMBERSHIP_STAGE, &r.chosen.encode().unwrap())
        .unwrap();
    assert_eq!(
        rsf_vsr::VsrMembershipStagedReport::decode(&staged)
            .unwrap()
            .plan,
        id
    );

    // Elect a living candidate through the actual newly promoted TLS voter.
    let mut network = r.network();
    let request = VsrViewChangeRequest {
        pg: PG,
        previous_view: Epoch(2),
        target_view: Epoch(3),
        candidate: NodeId(2),
        voters: proposal().new_voters,
    };
    let cell = r.local().cells.get_mut(&PG).unwrap();
    let (input, snapshots) = network.prepare_installation_input(cell, &request).unwrap();
    network
        .stage_installation_input(cell, &input, &snapshots)
        .unwrap();
    assert_eq!(
        network
            .resume_view_installation(cell, &mut |entry| persist_staged_entry(&path, 2, entry))
            .unwrap(),
        Epoch(3)
    );
    let accepted = cell.reprepare(NodeId(2), LogIndex(2)).unwrap();
    cell.replicate_with_transport(NodeId(2), accepted.index, &mut network)
        .unwrap();
    let payload = b"new quorum commit after nominated primary is permanently gone";
    let VsrDecision::Prepared(later) = cell
        .prepare(
            NodeId(2),
            VsrOperation::ReplicatedCommand {
                operation_id: OperationId(3),
                payload: payload.to_vec(),
            },
            payload.len() as u32,
        )
        .unwrap()
    else {
        panic!("new membership prepare");
    };
    cell.replicate_with_transport(NodeId(2), later.index, &mut network)
        .unwrap();
    assert_eq!(accepted.operation, r.accepted.operation);
    assert_eq!(
        network
            .replica_status(NodeId(3), PG)
            .unwrap()
            .normal
            .committed_tail,
        LogIndex(3)
    );

    // A newer durable promise fences writes; historical proof must remain usable
    // without reinstalling the old view or reviving its dead nominated primary.
    let promise = VsrViewChangeRequest {
        previous_view: Epoch(3),
        target_view: Epoch(4),
        ..request
    };
    network
        .prepare_installation_input(r.local().cells.get_mut(&PG).unwrap(), &promise)
        .unwrap();
    let before = [2, 3].map(|node| inventory(&path, node));
    let stamp = r.local().cells[&PG].recovery_state_token().unwrap();
    let clock = format!("{:?}", r.local().election_clock[&PG]);
    let alternative = r.alternate.clone();
    for _ in 0..2 {
        assert_eq!(
            r.local()
                .publish_membership_locally(&alternative, &mut |entry| persist_staged_entry(
                    &path, 2, entry
                ))
                .unwrap(),
            local
        );
        assert_eq!(
            network.publish_membership(NodeId(3), &alternative).unwrap(),
            remote
        );
        assert_eq!(
            network
                .query_membership_publication(NodeId(3), PG, id)
                .unwrap(),
            remote
        );
    }
    assert_eq!(r.local().cells[&PG].recovery_state_token().unwrap(), stamp);
    assert_eq!(format!("{:?}", r.local().election_clock[&PG]), clock);
    assert!(r.local().cells[&PG].primary_probe().is_err());
    assert_eq!(r.local().cells[&PG].view_change_promise(), Some(&promise));
    assert_eq!([2, 3].map(|node| inventory(&path, node)), before);
    assert_eq!(
        [2, 3].map(|node| inventory(&path, node).input),
        first_inputs
    );
    for node in [2, 3] {
        assert_eq!(
            std::fs::read(path.join(format!("materialized-{node}/3"))).unwrap(),
            later.encode().unwrap()
        );
    }

    // A warm stage retry after the view changed must materialize CURRENT state.
    // An apply failure cannot leave an old successful receipt cached for retry.
    std::fs::write(path.join("fail-apply-3"), b"reject both attempts").unwrap();
    std::fs::remove_file(path.join("materialized-3/3")).unwrap();
    let chosen = r.chosen.encode().unwrap();
    for _ in 0..2 {
        let failure = network
            .exchange_once(NodeId(3), MEMBERSHIP_STAGE, &chosen)
            .unwrap_err();
        assert!(
            failure
                .to_string()
                .contains("injected publication materializer failure"),
            "{failure}"
        );
        assert!(!path.join("materialized-3/3").exists());
    }
    std::fs::remove_file(path.join("fail-apply-3")).unwrap();
    let staged = network
        .exchange_once(NodeId(3), MEMBERSHIP_STAGE, &chosen)
        .unwrap();
    let staged = rsf_vsr::VsrMembershipStagedReport::decode(&staged).unwrap();
    assert_eq!(staged.plan, id);
    assert_eq!(
        std::fs::read(path.join("materialized-3/3")).unwrap(),
        later.encode().unwrap()
    );
    assert_eq!([2, 3].map(|node| inventory(&path, node)), before);

    r.restart_local();
    r.restart_remote(true);
    let network = r.network();
    assert_eq!(
        network
            .query_membership_publication(NodeId(3), PG, id)
            .unwrap(),
        remote
    );
    assert_eq!(
        r.local()
            .query_membership_publication(PG, id, &mut |entry| persist_staged_entry(
                &path, 2, entry
            ))
            .unwrap(),
        local
    );
    assert_eq!(r.local().cells[&PG].view(), Epoch(3));
    assert_eq!(r.local().cells[&PG].view_change_promise(), Some(&promise));
    assert_eq!(
        r.local().cells[&PG].request_commit(&original_request),
        Some(LogIndex(1))
    );
    drop(r.remote.take());
    let promoted = DurableVsrCell::open_existing(path.join("store-3"), PG).unwrap();
    assert_eq!(promoted.promoted_replica().unwrap(), Some(NodeId(3)));
    assert_eq!(promoted.view_change_promise(), Some(&promise));
    assert_eq!(
        promoted.committed_operation(LogIndex(1)),
        Some(&r.committed.operation)
    );
    assert_eq!(
        promoted.committed_operation(LogIndex(2)),
        Some(&r.accepted.operation)
    );
    assert_eq!(
        promoted.committed_operation(LogIndex(3)),
        Some(&later.operation)
    );
    assert_eq!(promoted.request_commit(&r.request), Some(LogIndex(1)));
    drop(promoted);
}

#[path = "membership_publication/failures.rs"]
mod failures;
#[path = "membership_publication/reply_loss.rs"]
mod reply_loss;
