//! Real peer control: bootstrap, primary death, stale-primary return, minority
//! fencing, and another election. Client-listener takeover is a separate gate.
use super::*;
use std::sync::{Arc, Mutex};
use std::time::Instant;
const RPC_MS: u64 = 150;
const HEARTBEAT_MS: u64 = 100;
const ELECTION_MS: u64 = 1500;

#[derive(Clone, Default)]
struct ControlEvents {
    events: Vec<serde_json::Value>,
    truncated: bool,
}

pub(super) struct AutomaticProcess {
    pub(super) process: Process,
    elections: Arc<Mutex<Vec<serde_json::Value>>>,
    control_events: Arc<Mutex<ControlEvents>>,
    pub(super) cache: Arc<Mutex<Option<serde_json::Value>>>,
}
impl AutomaticProcess {
    pub(super) fn start(path: &Path) -> Self {
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
        let control_events = Arc::new(Mutex::new(ControlEvents::default()));
        let controls = Arc::clone(&control_events);
        let cache = Arc::new(Mutex::new(None));
        let cache_events = Arc::clone(&cache);
        let (send, receive) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            for line in std::io::BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                let Ok(event) = serde_json::from_str::<serde_json::Value>(&line) else {
                    if line.contains("connection failed:") {
                        eprintln!("{line}");
                    }
                    continue;
                };
                if event["event"] == "peer.control" || event["event"] == "peer.election" {
                    // Preserve initial phase order, and explicitly reject a
                    // truncated trace in tests that use it as an oracle.
                    let mut trace = controls.lock().unwrap();
                    if trace.events.len() < 512 {
                        trace.events.push(event.clone());
                    } else {
                        trace.truncated = true;
                    }
                }
                if event["event"] == "peer.ready" {
                    let _ = send.send(
                        event["listen"]
                            .as_str()
                            .unwrap()
                            .parse::<SocketAddr>()
                            .unwrap(),
                    );
                }
                if event["event"] == "peer.control"
                    || (event["event"] == "peer.rpc"
                        && event["elapsed_ms"].as_u64().is_some_and(|ms| ms >= 100))
                {
                    println!("{event}");
                }
                if event["event"] == "peer.client_cache" {
                    *cache_events.lock().unwrap() = Some(event.clone());
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
            control_events,
            cache,
        }
    }
    fn latest(&self) -> Option<serde_json::Value> {
        self.elections.lock().unwrap().last().cloned()
    }
}
pub(super) fn state(root: &Path, node: u64) -> PathBuf {
    if node == 0 {
        root.join("seed-protocol.vsr")
    } else {
        root.join(format!("store-{node}"))
    }
}
pub(super) fn configure(root: &Path, peers: &BTreeMap<NodeId, PeerEndpoint>, automatic: bool) {
    // Seed writes and frozen-input setup use the normal host-test RPC budget.
    // Short deadlines belong to the election phase, where payloads already exist.
    let rpc_ms = if automatic { RPC_MS } else { PEER_TIMEOUT_MS };
    let mapped=peers.iter().map(|(node,peer)|serde_json::json!({"node":node.0,"address":peer.address.to_string(),"server_name":peer.server_name,"fingerprint":peer.fingerprint.iter().map(|b|format!("{b:02x}")).collect::<String>()})).collect::<Vec<_>>();
    for node in 0..3 {
        let listen = if automatic {
            peers[&NodeId(node)].address.to_string()
        } else {
            "127.0.0.1:0".into()
        };
        let mut config = serde_json::json!({"cluster":"cli-cluster","node":node,"state":state(root,node),"tls":root.join(format!("tls-{node}")),"listen":listen,"timeout_ms":rpc_ms,"peers":mapped,"cells":[{"pg":1,"view":1,"primary":0,"voters":[0,1,2]}]});
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
// Keep child listener reservations outside default macOS/Linux ephemeral
// client-port ranges. Dropping a port-zero reservation before exec can let a
// parallel outbound connection take that port before the child binds it.
pub(super) fn reserve_listener() -> std::net::TcpListener {
    static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    for _ in 0..10_000 {
        let ordinal = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let port = 10_000 + ((std::process::id() as usize * 31 + ordinal) % 10_000) as u16;
        match std::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], port))) {
            Ok(listener) => return listener,
            Err(error) if error.kind() == std::io::ErrorKind::AddrInUse => continue,
            Err(error) => panic!("test listener reservation: {error}"),
        }
    }
    panic!("no fixed test listener port available");
}
pub(super) fn reserve(
    peers: &mut BTreeMap<NodeId, PeerEndpoint>,
) -> Vec<Option<std::net::TcpListener>> {
    (0..3)
        .map(|node| {
            let listener = reserve_listener();
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
pub(super) fn await_primary(
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
pub(super) fn await_minority(
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
    configure(&root, &peers, false);
    let initial = Process::start(&root.join("auto-0.json"));
    drop(initial);
    let first = Process::start(&root.join("auto-1.json"));
    let second = Process::start(&root.join("auto-2.json"));
    peers.get_mut(&NodeId(1)).unwrap().address = first.address;
    peers.get_mut(&NodeId(2)).unwrap().address = second.address;
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
    configure(&root, &peers, false);
    let first = Process::start(&root.join("auto-0.json"));
    drop(first);
    let first = Process::start(&root.join("auto-1.json"));
    drop(first);
    let follower = Process::start(&root.join("auto-2.json"));
    peers.get_mut(&NodeId(2)).unwrap().address = follower.address;
    configure(&root, &peers, false);
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
    let (original_input, original_history) = {
        let mut local =
            DurableVsrCell::open_existing(state(&root, 1).join("pg-1"), PlacementGroupId(1))
                .unwrap();
        assert_eq!(local.view(), Epoch(2));
        assert!(local.view_change_promise().is_none());
        let input = local.installation_input().unwrap().unwrap();
        assert_eq!(input.request().target_view, Epoch(2));
        assert_eq!(input.request().candidate, NodeId(1));
        let snapshots = local.installation_snapshots(input.id().unwrap()).unwrap();
        let history = local.history_snapshot().unwrap();
        assert_eq!(
            rsf_vsr::VsrViewInstallation::select(&snapshots)
                .unwrap()
                .id(),
            history.descriptor.digest.bytes,
        );
        let remote =
            DurableVsrCell::open_existing(state(&root, 2).join("pg-1"), PlacementGroupId(1))
                .unwrap();
        assert_eq!(remote.view(), Epoch(1));
        assert_eq!(remote.view_change_promise(), Some(input.request()));
        (input, history)
    };
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
        primary, 1,
        "the retained candidate must recover this fixture"
    );
    let trace = processes[1]
        .as_ref()
        .unwrap()
        .control_events
        .lock()
        .unwrap()
        .clone();
    assert!(
        !trace.truncated,
        "control evidence was truncated: {}",
        root.display()
    );
    let events = &trace.events;
    let first = events
        .iter()
        .find(|event| event["event"] == "peer.control")
        .expect("retained candidate emitted no control phase");
    assert_eq!(
        first["phase"], "resume",
        "skipped retained installation: {events:?}"
    );
    let collect = events
        .iter()
        .position(|event| event["event"] == "peer.control" && event["phase"] == "collect");
    if let Some(collect) = collect {
        assert!(
            view > 2,
            "fresh election did not advance the installed view: {events:?}"
        );
        let mut attempts = 0;
        let mut last_failed = None;
        for event in &events[..collect] {
            assert_eq!(event["pg"], 1);
            if event["event"] == "peer.control" {
                assert_eq!(event["node"], 1);
                assert_eq!(
                    event["phase"], "resume",
                    "unexpected completion before escalation: {events:?}"
                );
                if attempts != 0 {
                    assert!(
                        last_failed.is_some(),
                        "resume repeated without a failed result: {events:?}"
                    );
                }
                attempts += 1;
            } else {
                assert_eq!(event["event"], "peer.election");
                assert_eq!(
                    event["view"], 2,
                    "unobserved supersession branch: {events:?}"
                );
                assert_eq!(event["primary"], 1);
                assert_eq!(
                    event["quorum_confirmed"], false,
                    "confirmed resume cannot justify failure escalation: {events:?}"
                );
                assert!(!event["detail"].as_str().unwrap().is_empty());
                assert!(
                    attempts != 0,
                    "failed result preceded the first resume: {events:?}"
                );
                last_failed = Some(event);
            }
        }
        // The daemon suppresses identical election-result events. Two resume
        // phases plus the still-unconfirmed emitted result establish ordering;
        // requiring two duplicate result lines would reject legitimate retries.
        assert!(
            attempts >= 2 && last_failed.is_some(),
            "fresh election skipped bounded resume attempts: {events:?}"
        );
    } else {
        assert_eq!(
            view, 2,
            "advanced without a fresh-election phase: {events:?}"
        );
        assert!(
            events.iter().any(|event| {
                event["event"] == "peer.election"
                    && event["view"] == 2
                    && event["primary"] == 1
                    && event["quorum_confirmed"] == true
                    && event["detail"].as_str().unwrap().contains("resumed")
            }),
            "retained installation was not confirmed: {events:?}"
        );
    }
    assert!(
        events.iter().any(|event| {
            event["event"] == "peer.election"
                && event["pg"] == 1
                && event["view"] == view
                && event["primary"] == primary
                && event["quorum_confirmed"] == true
        }),
        "final installed quorum was not confirmed: {events:?}"
    );
    drop(processes);

    // A later safe election can replace the upload pointer, but cannot discard
    // the original installed history. Validate complete selected proof and
    // authoritative replay on every cold survivor, not just the event stream.
    let expected = rsf_vsr::VsrPrimaryProbe {
        view: Epoch(view),
        primary: NodeId(primary),
        ..original_history.descriptor.probe.clone()
    };
    let mut final_input = None;
    let mut final_history = None;
    for node in 0..3 {
        let mut cell =
            DurableVsrCell::open_existing(state(&root, node).join("pg-1"), PlacementGroupId(1))
                .unwrap();
        let status = cell.replica_status().unwrap();
        assert_eq!(
            status.normal, expected,
            "cold voter {node} changed authority"
        );
        assert!(status.promise.is_none());
        assert!(cell.has_installed_view());
        let input = cell.installation_input().unwrap().unwrap();
        assert_eq!(
            *input.request(),
            rsf_vsr::VsrViewChangeRequest {
                target_view: expected.view,
                ..original_input.request().clone()
            }
        );
        let snapshots = cell.installation_snapshots(input.id().unwrap()).unwrap();
        let selected = rsf_vsr::VsrViewInstallation::select(&snapshots).unwrap();
        let history = cell.history_snapshot().unwrap();
        assert_eq!(selected.request(), input.request());
        assert_eq!(
            selected.id(),
            history.descriptor.digest.bytes,
            "voter {node} active history differs from its selected proof"
        );
        assert_eq!(selected.committed_tail(), expected.committed_tail);
        assert_eq!(selected.accepted_tail(), rsf_core::LogIndex(0));
        assert!(
            history.records.starts_with(&original_history.records),
            "voter {node} lost the original installed history"
        );
        if view == 2 {
            assert_eq!(input, original_input);
            assert_eq!(history.records, original_history.records);
        } else {
            assert_ne!(input.id().unwrap(), original_input.id().unwrap());
        }
        if let Some(first) = &final_input {
            assert_eq!(&input, first);
        }
        if let Some((descriptor, records)) = &final_history {
            assert_eq!(&history.descriptor, descriptor);
            assert_eq!(&history.records, records);
        }
        final_input = Some(input);
        final_history = Some((history.descriptor, history.records));
    }
    std::fs::remove_dir_all(root).unwrap();
}
