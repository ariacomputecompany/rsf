//! Real peer control: bootstrap, primary death, stale-primary return, minority
//! fencing, and another election. Client-listener takeover is a separate gate.
use super::*;
use std::sync::{Arc, Mutex};
use std::time::Instant;
const RPC_MS: u64 = 150;
const HEARTBEAT_MS: u64 = 100;
const ELECTION_MS: u64 = 1500;

struct AutomaticProcess {
    process: Process,
    elections: Arc<Mutex<Vec<serde_json::Value>>>,
}
impl AutomaticProcess {
    fn start(path: &Path) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_rsfd"))
            .args(["peer", "serve", "--config"])
            .arg(path)
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let elections = Arc::new(Mutex::new(Vec::new()));
        let events = Arc::clone(&elections);
        let (send, receive) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            for line in std::io::BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
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
                if event["event"] == "peer.election" {
                    let mut log = events.lock().unwrap();
                    if log.len() == 128 {
                        log.remove(0);
                    }
                    log.push(event);
                }
            }
        });
        let address = receive.recv_timeout(Duration::from_secs(15));
        if address.is_err() {
            let _ = child.kill();
            let _ = child.wait();
        }
        Self {
            process: Process {
                child,
                address: address.expect("automatic peer readiness"),
                health: None,
            },
            elections,
        }
    }
    fn latest(&self) -> Option<serde_json::Value> {
        self.elections.lock().unwrap().last().cloned()
    }
}
fn state(root: &Path, node: u64) -> PathBuf {
    if node == 0 {
        root.join("seed-protocol.vsr")
    } else {
        root.join(format!("store-{node}"))
    }
}
fn configure(root: &Path, peers: &BTreeMap<NodeId, PeerEndpoint>, automatic: bool) {
    // Seed writes and frozen-input setup use the normal host-test RPC budget.
    // Short deadlines belong to the election phase, where payloads already exist.
    let rpc_ms = if automatic { RPC_MS } else { PEER_TIMEOUT_MS };
    let mapped=peers.iter().map(|(node,peer)|serde_json::json!({"node":node.0,"address":peer.address.to_string(),"server_name":peer.server_name,"fingerprint":peer.fingerprint.iter().map(|b|format!("{b:02x}")).collect::<String>()})).collect::<Vec<_>>();
    for node in 0..3 {
        let mut config = serde_json::json!({"cluster":"cli-cluster","node":node,"state":state(root,node),"tls":root.join(format!("tls-{node}")),"listen":peers[&NodeId(node)].address.to_string(),"timeout_ms":rpc_ms,"peers":mapped,"cells":[{"pg":1,"view":1,"primary":0,"voters":[0,1,2]}]});
        if automatic {
            config["elections"] = serde_json::json!({"heartbeat_ms":HEARTBEAT_MS,"timeout_ms":ELECTION_MS,"rank_delay_ms":1000});
        }
        std::fs::write(
            root.join(format!("auto-{node}.json")),
            serde_json::to_vec(&config).unwrap(),
        )
        .unwrap();
    }
}
fn reserve(peers: &mut BTreeMap<NodeId, PeerEndpoint>) -> Vec<Option<std::net::TcpListener>> {
    (0..3)
        .map(|node| {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            peers.get_mut(&NodeId(node)).unwrap().address = listener.local_addr().unwrap();
            Some(listener)
        })
        .collect()
}
fn observe(
    root: &Path,
    peers: &BTreeMap<NodeId, PeerEndpoint>,
    node: u64,
) -> Result<rsf_vsr::VsrReplicaStatus, rsf_vsr::VsrTransportError> {
    let observer = (node + 1) % 3;
    NetworkVsrTransport::new(
        NodeId(observer),
        &ClusterId::new("cli-cluster").unwrap(),
        PeerIdentity::load(&root.join(format!("tls-{observer}"))).unwrap(),
        peers.clone(),
        Duration::from_millis(500),
    )
    .unwrap()
    .replica_status(NodeId(node), PlacementGroupId(1))
}
fn await_primary(
    root: &Path,
    peers: &BTreeMap<NodeId, PeerEndpoint>,
    processes: &mut [Option<AutomaticProcess>],
    nodes: &[u64],
    minimum: u64,
) -> (u64, u64) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let statuses = nodes
            .iter()
            .map(|node| observe(root, peers, *node))
            .collect::<Result<Vec<_>, _>>();
        if let Ok(statuses) = statuses {
            let first = &statuses[0];
            let primary = first.normal.primary.0;
            let view = first.normal.view.0;
            if view >= minimum
                && nodes.contains(&primary)
                && statuses
                    .iter()
                    .all(|s| s.promise.is_none() && s.normal == first.normal)
                && processes[primary as usize]
                    .as_ref()
                    .and_then(AutomaticProcess::latest)
                    .is_some_and(|e| {
                        e["view"] == view
                            && e["primary"] == primary
                            && e["quorum_confirmed"] == true
                    })
            {
                return (primary, view);
            }
        }
        for node in nodes {
            assert!(
                processes[*node as usize]
                    .as_mut()
                    .unwrap()
                    .process
                    .child
                    .try_wait()
                    .unwrap()
                    .is_none(),
                "peer {node} exited"
            );
        }
        assert!(
            Instant::now() < deadline,
            "automatic election did not converge: {:?}",
            processes
                .iter()
                .map(|p| p.as_ref().and_then(AutomaticProcess::latest))
                .collect::<Vec<_>>()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}
fn await_minority(
    root: &Path,
    peers: &BTreeMap<NodeId, PeerEndpoint>,
    process: &mut AutomaticProcess,
    node: u64,
    view: u64,
    needs_promise: bool,
) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        assert!(process.process.child.try_wait().unwrap().is_none());
        if let Ok(status) = observe(root, peers, node) {
            assert_eq!(
                status.normal.view.0, view,
                "a minority must not install a new view"
            );
            if (!needs_promise || status.promise.is_some())
                && process
                    .latest()
                    .is_some_and(|e| e["quorum_confirmed"] == false)
            {
                return;
            }
        }
        assert!(
            Instant::now() < deadline,
            "minority did not lose quorum / persist its attempted election: {:?}",
            process.latest()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn automatic_peer_elections_preserve_payloads_across_primary_loss_and_stale_primary_rejoin() {
    let root = root();
    let mut peers = provision(&root);
    let mut listeners = reserve(&mut peers);
    configure(&root, &peers, false);
    drop(listeners[0].take());
    let initial = Process::start(&root.join("auto-0.json"));
    drop(initial);
    drop(listeners[1].take());
    let first = Process::start(&root.join("auto-1.json"));
    drop(listeners[2].take());
    let second = Process::start(&root.join("auto-2.json"));
    let network = NetworkVsrTransport::new(
        NodeId(0),
        &ClusterId::new("cli-cluster").unwrap(),
        PeerIdentity::load(&root.join("tls-0")).unwrap(),
        peers.clone(),
        Duration::from_millis(PEER_TIMEOUT_MS),
    )
    .unwrap();
    let mut router = rsf_protocol::VsrBackedFabricRouter::open_with_transport(
        root.join("seed-protocol"),
        [PlacementGroupId(1)],
        Epoch(1),
        NodeId(0),
        [NodeId(0), NodeId(1), NodeId(2)],
        network,
    )
    .unwrap();
    let mut kv =
        rsf_kv::DurableKvEngine::open_replicated(root.join("seed-kv"), Epoch(1), "app").unwrap();
    let snapshot = kv.snapshot(&router);
    kv.commit_batch_routed(
        &mut router,
        rsf_kv::KvBatch {
            client_request: Some(rsf_protocol::ClientRequestId::new("automatic-seed", 1)),
            snapshot,
            mutations: vec![rsf_kv::KvMutation::Set {
                key: b"automatic/key".to_vec(),
                value: b"acknowledged before automatic failover".to_vec(),
            }],
        },
    )
    .unwrap();
    drop(kv);
    drop(router);
    drop(first);
    drop(second);
    let mut listeners = reserve(&mut peers);
    configure(&root, &peers, true);
    let mut processes = (0..3)
        .map(|node| {
            drop(listeners[node].take());
            Some(AutomaticProcess::start(
                &root.join(format!("auto-{node}.json")),
            ))
        })
        .collect::<Vec<_>>();
    let (bootstrap, view) = await_primary(&root, &peers, &mut processes, &[0, 1, 2], 2);
    drop(processes[bootstrap as usize].take());
    std::fs::remove_dir_all(root.join("seed-protocol")).unwrap();
    std::fs::remove_dir_all(root.join("seed-kv")).unwrap();
    let survivors = (0..3).filter(|node| *node != bootstrap).collect::<Vec<_>>();
    let (primary, view) = await_primary(&root, &peers, &mut processes, &survivors, view + 1);
    processes[bootstrap as usize] = Some(AutomaticProcess::start(
        &root.join(format!("auto-{bootstrap}.json")),
    ));
    let (recovered, view) = await_primary(&root, &peers, &mut processes, &[0, 1, 2], view);
    assert_eq!(
        recovered, primary,
        "a stale returning primary must adopt the selected view"
    );
    let standby = *survivors.iter().find(|node| **node != primary).unwrap();
    drop(processes[bootstrap as usize].take());
    drop(processes[standby as usize].take());
    await_minority(
        &root,
        &peers,
        processes[primary as usize].as_mut().unwrap(),
        primary,
        view,
        false,
    );
    processes[standby as usize] = Some(AutomaticProcess::start(
        &root.join(format!("auto-{standby}.json")),
    ));
    let (same, view) = await_primary(&root, &peers, &mut processes, &[primary, standby], view);
    assert_eq!(same, primary);
    drop(processes[primary as usize].take());
    await_minority(
        &root,
        &peers,
        processes[standby as usize].as_mut().unwrap(),
        standby,
        view,
        true,
    );
    processes[bootstrap as usize] = Some(AutomaticProcess::start(
        &root.join(format!("auto-{bootstrap}.json")),
    ));
    let (next, view) = await_primary(
        &root,
        &peers,
        &mut processes,
        &[bootstrap, standby],
        view + 1,
    );
    processes[primary as usize] = Some(AutomaticProcess::start(
        &root.join(format!("auto-{primary}.json")),
    ));
    let (final_primary, final_view) =
        await_primary(&root, &peers, &mut processes, &[0, 1, 2], view);
    assert_eq!(final_primary, next);
    drop(processes);
    use sha2::{Digest, Sha256};
    let namespace = Sha256::digest(b"app")
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    for node in 0..3 {
        let cell =
            DurableVsrCell::open_existing(state(&root, node).join("pg-1"), PlacementGroupId(1))
                .unwrap();
        assert_eq!(cell.view(), Epoch(final_view));
        assert_eq!(cell.primary(), NodeId(next));
        assert_eq!(cell.committed_tail(), LogIndex(1));
        let kv = rsf_kv::DurableKvEngine::open_replicated(
            state(&root, node).join(format!("application/namespaces/{namespace}/kv")),
            Epoch(1),
            "app",
        )
        .unwrap();
        assert_eq!(
            kv.get(b"automatic/key").unwrap().value,
            b"acknowledged before automatic failover"
        );
        assert_eq!(kv.changes_since(rsf_core::CommitVersion(0)).len(), 1);
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn partial_installation_child() {
    let Some(root) = std::env::var_os("RSF_AUTO_PARTIAL_ROOT") else {
        return;
    };
    let root = PathBuf::from(root);
    let config: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("auto-1.json")).unwrap()).unwrap();
    let peers = config["peers"]
        .as_array()
        .unwrap()
        .iter()
        .map(|peer| {
            let node = peer["node"].as_u64().unwrap();
            (
                NodeId(node),
                PeerEndpoint {
                    address: peer["address"].as_str().unwrap().parse().unwrap(),
                    server_name: "localhost".into(),
                    fingerprint: PeerIdentity::load(&root.join(format!("tls-{node}")))
                        .unwrap()
                        .fingerprint,
                },
            )
        })
        .collect();
    let network = NetworkVsrTransport::new(
        NodeId(1),
        &ClusterId::new("cli-cluster").unwrap(),
        PeerIdentity::load(&root.join("tls-1")).unwrap(),
        peers,
        Duration::from_millis(PEER_TIMEOUT_MS),
    )
    .unwrap();
    let mut local =
        DurableVsrCell::open_existing(state(&root, 1).join("pg-1"), PlacementGroupId(1)).unwrap();
    let request = rsf_vsr::VsrViewChangeRequest {
        pg: PlacementGroupId(1),
        previous_view: Epoch(1),
        target_view: Epoch(2),
        candidate: NodeId(1),
        voters: vec![NodeId(0), NodeId(1), NodeId(2)],
    };
    let (input, snapshots) = network
        .prepare_installation_input(&mut local, &request)
        .unwrap();
    network
        .stage_installation_input(&mut local, &input, &snapshots)
        .unwrap();
    local
        .finish_installation_input(NodeId(1), input.id().unwrap())
        .unwrap();
    println!("AUTO_LOCAL_INSTALLATION_DURABLE");
    use std::io::Write;
    std::io::stdout().flush().unwrap();
    loop {
        std::thread::park_timeout(Duration::from_secs(1));
    }
}

#[test]
fn automatic_daemon_restart_resumes_a_candidate_killed_after_only_local_publication() {
    let root = root();
    let mut peers = provision(&root);
    let mut listeners = reserve(&mut peers);
    configure(&root, &peers, false);
    drop(listeners[0].take());
    let first = Process::start(&root.join("auto-0.json"));
    drop(first);
    drop(listeners[1].take());
    let first = Process::start(&root.join("auto-1.json"));
    drop(first);
    drop(listeners[2].take());
    let follower = Process::start(&root.join("auto-2.json"));
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "automatic::partial_installation_child",
            "--nocapture",
        ])
        .env("RSF_AUTO_PARTIAL_ROOT", &root)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let (send, receive) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in std::io::BufReader::new(stdout).lines() {
            if line.unwrap().contains("AUTO_LOCAL_INSTALLATION_DURABLE") {
                let _ = send.send(());
                return;
            }
        }
    });
    let durable = receive.recv_timeout(Duration::from_secs(15));
    let _ = child.kill();
    child.wait().unwrap();
    reader.join().unwrap();
    durable.unwrap();
    drop(follower);
    {
        let local =
            DurableVsrCell::open_existing(state(&root, 1).join("pg-1"), PlacementGroupId(1))
                .unwrap();
        assert_eq!(local.view(), Epoch(2));
        assert!(local.view_change_promise().is_none());
        let remote =
            DurableVsrCell::open_existing(state(&root, 2).join("pg-1"), PlacementGroupId(1))
                .unwrap();
        assert_eq!(remote.view(), Epoch(1));
        assert_eq!(remote.view_change_promise().unwrap().target_view, Epoch(2));
    }
    let mut listeners = reserve(&mut peers);
    configure(&root, &peers, true);
    let mut processes = (0..3)
        .map(|node| {
            drop(listeners[node].take());
            Some(AutomaticProcess::start(
                &root.join(format!("auto-{node}.json")),
            ))
        })
        .collect::<Vec<_>>();
    let (primary, view) = await_primary(&root, &peers, &mut processes, &[0, 1, 2], 2);
    assert_eq!(
        (primary, view),
        (1, 2),
        "the restarted candidate must confirm its existing installation before a fresh election"
    );
    assert!(
        processes[1]
            .as_ref()
            .unwrap()
            .elections
            .lock()
            .unwrap()
            .iter()
            .any(|event| event["view"] == 2
                && event["quorum_confirmed"] == true
                && event["detail"].as_str().unwrap().contains("resumed"))
    );
    drop(processes);
    std::fs::remove_dir_all(root).unwrap();
}
