use super::*;

pub(super) const CONTROL: PlacementGroupId = PlacementGroupId(0);
pub(super) const DATA: PlacementGroupId = PlacementGroupId(1);
const TIMEOUT: Duration = Duration::from_millis(800);

pub(super) struct Fixture {
    pub root: PathBuf,
    pub anchor: ControlBootstrap,
    pub catalog: ControlCatalog,
    pub peers: BTreeMap<NodeId, PeerEndpoint>,
    pub control: BTreeMap<NodeId, DurableVsrCell>,
    pub data: BTreeMap<NodeId, DurableVsrCell>,
    pub server: Option<PeerReplicaServer>,
}
impl Fixture {
    pub fn new(name: &str) -> Self {
        let mut fixture = Self::unbound(name);
        fixture.bind();
        fixture
    }
    pub fn bind(&mut self) {
        self.server = Some(
            self.server
                .take()
                .unwrap()
                .with_control_replication(self.anchor.clone(), self.catalog.clone())
                .unwrap(),
        );
    }
    pub fn unbound(name: &str) -> Self {
        let root = root(name);
        provision(&root);
        let peers = (0..4)
            .map(|node| {
                (
                    NodeId(node),
                    PeerEndpoint {
                        address: "127.0.0.1:1".parse().unwrap(),
                        server_name: "localhost".into(),
                        fingerprint: identity(&root, node).fingerprint,
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        let cells = |pg: PlacementGroupId, primary: NodeId| {
            (0..3)
                .map(|node| {
                    let cell = DurableVsrCell::open(
                        root.join(format!("pg-{}/node-{node}", pg.0)),
                        pg,
                        Epoch(1),
                        primary,
                        (0..3).map(NodeId),
                    )
                    .unwrap();
                    (NodeId(node), cell)
                })
                .collect::<BTreeMap<_, _>>()
        };
        let mut control = cells(CONTROL, NodeId(0));
        let mut data = cells(DATA, NodeId(2));
        let catalog = ControlCatalog {
            cluster: cluster(),
            control_pg: CONTROL,
            routing: CatalogRouting {
                generation: RoutingGeneration(1),
                placement: [3; 32],
                policy: [4; 32],
            },
            nodes: (0..4)
                .map(|node| CatalogNode {
                    id: NodeId(node),
                    incarnation: 1,
                    public_key: format!("node-public-key-{node}"),
                    nonce: format!("nonce-{node}-000"),
                    trust_domain: "rsf.test".into(),
                    spiffe_id: format!("spiffe://rsf.test/rsf/node/node-public-key-{node}"),
                    certificate: peers[&NodeId(node)].fingerprint,
                    role: if node == 3 {
                        AdmissionRole::Learner
                    } else {
                        AdmissionRole::Voter
                    },
                })
                .collect(),
            groups: [(CONTROL, &control), (DATA, &data)]
                .into_iter()
                .map(|(pg, cells)| CatalogGroup {
                    pg,
                    lineage: MembershipLineage {
                        bootstrap: cells[&NodeId(0)].membership_lineage().unwrap().bootstrap,
                        plan: None,
                    },
                    voters: (0..3).map(NodeId).collect(),
                })
                .collect(),
        };
        let anchor = ControlBootstrap {
            cluster: cluster(),
            control_pg: CONTROL,
            control_bootstrap: catalog.group(CONTROL).unwrap().lineage.bootstrap,
            genesis_catalog: catalog.root().unwrap(),
        };
        let hosted = [
            (CONTROL, control.remove(&NodeId(1)).unwrap()),
            (DATA, data.remove(&NodeId(1)).unwrap()),
        ]
        .into_iter()
        .collect();
        let server = PeerReplicaServer::new(
            NodeId(1),
            &cluster(),
            identity(&root, 1),
            peers.clone(),
            hosted,
            TIMEOUT,
        )
        .unwrap()
        .with_membership_paths(
            [
                (CONTROL, root.join("transition-control")),
                (DATA, root.join("transition-data")),
            ]
            .into_iter()
            .collect(),
        )
        .unwrap();
        Self {
            root,
            anchor,
            catalog,
            peers,
            control,
            data,
            server: Some(server),
        }
    }
    pub fn transport(&self, node: u64, address: SocketAddr) -> NetworkVsrTransport {
        let mut peers = self.peers.clone();
        peers.get_mut(&NodeId(1)).unwrap().address = address;
        NetworkVsrTransport::new(
            NodeId(node),
            &cluster(),
            identity(&self.root, node),
            peers,
            TIMEOUT,
        )
        .unwrap()
    }
    pub fn intent(&self, request: u8) -> MembershipIntent {
        MembershipIntent {
            request: ControlRequestId([request; 32]),
            parent: self.catalog.root().unwrap(),
            revision: ControlRevision(1),
            routing: self.catalog.routing,
            nodes: self
                .catalog
                .nodes
                .iter()
                .cloned()
                .map(|mut node| {
                    node.role = if node.id == NodeId(2) {
                        AdmissionRole::Learner
                    } else {
                        AdmissionRole::Voter
                    };
                    node
                })
                .collect(),
            groups: self
                .catalog
                .groups
                .iter()
                .map(|group| GroupIntent {
                    pg: group.pg,
                    previous: group.lineage,
                    proposal: proposal(group.pg, if group.pg == CONTROL { 0 } else { 2 }),
                })
                .collect(),
        }
    }
    pub fn cleanup(mut self) {
        self.server.take();
        self.control.clear();
        self.data.clear();
        std::fs::remove_dir_all(&self.root).unwrap();
    }
}
pub(super) fn proposal(pg: PlacementGroupId, primary: u64) -> rsf_vsr::VsrMembershipProposal {
    rsf_vsr::VsrMembershipProposal {
        election: rsf_vsr::VsrViewChangeRequest {
            pg,
            previous_view: Epoch(1),
            target_view: Epoch(2),
            candidate: NodeId(0),
            voters: (0..3).map(NodeId).collect(),
        },
        old_primary: NodeId(primary),
        new_voters: [NodeId(0), NodeId(1), NodeId(3)].into_iter().collect(),
    }
}
pub(super) fn prepare_command(source: &mut DurableVsrCell, command: ControlCommand) -> VsrPrepare {
    let payload = command.encode().unwrap();
    let bytes = payload.len() as u32;
    let VsrDecision::Prepared(prepare) = source
        .prepare(
            NodeId(0),
            VsrOperation::ReplicatedCommand {
                operation_id: OperationId(source.next_index().0),
                payload,
            },
            bytes,
        )
        .unwrap()
    else {
        panic!("control prepare rejected")
    };
    prepare
}
pub(super) fn commit_tls(
    source: &mut DurableVsrCell,
    transport: &mut NetworkVsrTransport,
    prepare: VsrPrepare,
) -> VsrCommittedEntry {
    assert_eq!(
        transport
            .acknowledge_prepare(VsrPrepareMessage {
                replica: NodeId(1),
                prepare: prepare.clone()
            })
            .unwrap(),
        NodeId(1)
    );
    assert!(matches!(
        source.acknowledge(NodeId(1), prepare.view, prepare.index),
        VsrDecision::Acknowledged { .. }
    ));
    let VsrDecision::Committed(commit) = source.commit(NodeId(0), prepare.index).unwrap() else {
        panic!("commit rejected")
    };
    transport
        .acknowledge_commit(NodeId(1), commit.clone())
        .unwrap();
    VsrCommittedEntry { prepare, commit }
}
pub(super) struct Running {
    pub address: SocketAddr,
    stop: Arc<AtomicBool>,
    pub applied: Arc<Mutex<Vec<VsrCommittedEntry>>>,
    worker: Option<std::thread::JoinHandle<PeerReplicaServer>>,
}
impl Running {
    pub fn start(mut server: PeerReplicaServer) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let finished = Arc::clone(&stop);
        let applied = Arc::new(Mutex::new(Vec::new()));
        let effects = Arc::clone(&applied);
        let worker = std::thread::spawn(move || {
            while !finished.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((socket, _)) => {
                        let _ = server.serve_connection_with_apply(socket, &mut |entry| {
                            assert_ne!(
                                entry.prepare.pg, CONTROL,
                                "control escaped into Fabric materializer"
                            );
                            effects.lock().unwrap().push(entry.clone());
                            Ok(())
                        });
                    }
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(1))
                    }
                    Err(error) => panic!("listener failed: {error}"),
                }
            }
            server
        });
        Self {
            address,
            stop,
            applied,
            worker: Some(worker),
        }
    }
    pub fn finish(mut self) -> PeerReplicaServer {
        self.stop.store(true, Ordering::Release);
        self.worker.take().unwrap().join().unwrap()
    }
}
impl Drop for Running {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

pub(super) fn rejected(result: io::Result<Vec<u8>>, expected: &str) {
    let error = result.unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{error}");
    let message = error.to_string();
    assert!(message.starts_with("peer rejected request:"), "{message}");
    assert!(message.contains(expected), "expected {expected}: {message}");
}

pub(super) fn tree(root: &Path) -> BTreeMap<PathBuf, Option<Vec<u8>>> {
    fn visit(base: &Path, path: &Path, entries: &mut BTreeMap<PathBuf, Option<Vec<u8>>>) {
        for entry in std::fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            let relative = path.strip_prefix(base).unwrap().to_path_buf();
            if entry.file_type().unwrap().is_dir() {
                entries.insert(relative, None);
                visit(base, &path, entries);
            } else {
                entries.insert(relative, Some(std::fs::read(path).unwrap()));
            }
        }
    }
    let mut entries = BTreeMap::new();
    visit(root, root, &mut entries);
    entries
}
