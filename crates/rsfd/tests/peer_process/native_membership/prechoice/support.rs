use super::*;
use std::io::Write;
use std::sync::mpsc::{Receiver, channel};
use std::thread::JoinHandle;
use std::time::Instant;

pub(super) struct Native {
    child: Option<Child>,
    reader: Option<JoinHandle<()>>,
    ready: Receiver<SocketAddr>,
    expected: SocketAddr,
    log: PathBuf,
}
impl Native {
    /// Spawn returns immediately. The pair helper starts both binaries before
    /// waiting for either ready event, so startup is genuinely concurrent.
    pub(super) fn spawn(r: &Ready, node: u64, boot: &str) -> Self {
        let log = r.root.join(format!("native-{node}-{boot}"));
        let stdout = std::fs::File::create(log.with_extension("stdout")).unwrap();
        let stderr = std::fs::File::create(log.with_extension("stderr")).unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_rsfd"))
            .args(["peer", "serve", "--config"])
            .arg(r.root.join(format!("peer-{node}.json")))
            .stdout(Stdio::piped())
            .stderr(Stdio::from(stderr))
            .spawn()
            .unwrap();
        let (send, ready) = channel();
        let mut native = Self {
            child: Some(child),
            reader: None,
            ready,
            expected: r.peers[&NodeId(node)].address,
            log,
        };
        let pipe = native.child.as_mut().unwrap().stdout.take().unwrap();
        native.reader = Some(std::thread::spawn(move || {
            let mut stdout = stdout;
            for line in std::io::BufReader::new(pipe).lines() {
                let Ok(line) = line else {
                    break;
                };
                writeln!(stdout, "{line}").unwrap();
                stdout.flush().unwrap();
                if let Ok(event) = serde_json::from_str::<serde_json::Value>(&line)
                    && event["event"] == "peer.ready"
                {
                    let address = event["listen"].as_str().unwrap().parse().unwrap();
                    let _ = send.send(address);
                }
            }
        }));
        native
    }
    pub(super) fn ready(&self) {
        assert_eq!(
            self.ready
                .recv_timeout(Duration::from_secs(10))
                .unwrap_or_else(|error| panic!(
                    "native readiness failed: {error}; logs={}",
                    self.log.display()
                )),
            self.expected
        );
    }
    pub(super) fn alive(&mut self) {
        assert!(
            self.child.as_mut().unwrap().try_wait().unwrap().is_none(),
            "native process exited; logs={}",
            self.log.display()
        );
    }
    pub(super) fn stop(mut self) {
        self.reap();
    }
    fn reap(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        if let Some(reader) = self.reader.take() {
            let joined = reader.join();
            if !std::thread::panicking() {
                joined.expect("native stdout reader");
            }
        }
    }
}
impl Drop for Native {
    fn drop(&mut self) {
        self.reap();
    }
}
pub(super) fn pair(r: &Ready, boot: &str) -> (Native, Native) {
    let second = Native::spawn(r, 2, boot);
    let third = Native::spawn(r, 3, boot);
    second.ready();
    third.ready();
    (second, third)
}

pub(super) struct Blackhole {
    pub(super) address: SocketAddr,
    accepted: Receiver<Instant>,
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    worker: Option<JoinHandle<()>>,
}
impl Blackhole {
    pub(super) fn start() -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stopping = std::sync::Arc::clone(&stop);
        let (send, accepted) = channel();
        let worker = std::thread::spawn(move || {
            let mut sockets = Vec::new();
            while !stopping.load(std::sync::atomic::Ordering::Relaxed) {
                match listener.accept() {
                    Ok((socket, _)) => {
                        let at = Instant::now();
                        let _ = send.send(at);
                        sockets.push((at, socket));
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                    Err(error) => panic!("blackhole listener: {error}"),
                }
                // No TLS reply is produced. Both native transport attempts must
                // spend their unchanged deadlines; bound retained fault sockets.
                sockets.retain(|(at, _)| at.elapsed() < Duration::from_millis(3 * RPC_MS));
                assert!(
                    sockets.len() <= 16,
                    "native recovery accumulated outbound workers"
                );
                std::thread::sleep(Duration::from_millis(2));
            }
        });
        Self {
            address,
            accepted,
            stop,
            worker: Some(worker),
        }
    }
    pub(super) fn wait_connection(&self, r: &Ready, budget: &Budget) -> Instant {
        loop {
            if let Ok(at) = self
                .accepted
                .recv_timeout(Duration::from_millis(RECOVERY_MS))
                && at.elapsed() < Duration::from_millis(RPC_MS / 2)
            {
                return at;
            }
            budget.check(
                r,
                "waiting for actual blackholed native outbound TLS flight",
            );
        }
    }
}
impl Drop for Blackhole {
    fn drop(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::Relaxed);
        if let Some(worker) = self.worker.take() {
            let joined = worker.join();
            if !std::thread::panicking() {
                joined.expect("blackhole transport fixture");
            }
        }
    }
}

pub(super) fn redirect_removed_candidate(r: &mut Ready, address: SocketAddr) {
    r.peers.get_mut(&NodeId(1)).unwrap().address = address;
    for node in [2, 3] {
        config(&r.root, node, |value| {
            for peer in value["peers"].as_array_mut().unwrap() {
                if peer["node"] == 1 {
                    peer["address"] = address.to_string().into();
                }
            }
        });
    }
}

pub(super) fn network(r: &Ready, target: u64) -> NetworkVsrTransport {
    let sender = if target == 2 { 3 } else { 2 };
    NetworkVsrTransport::new(
        NodeId(sender),
        &ClusterId::new("cli-cluster").unwrap(),
        PeerIdentity::load(&r.root.join(format!("tls-{sender}"))).unwrap(),
        r.peers.clone(),
        Duration::from_millis(RPC_MS),
    )
    .unwrap()
}
pub(super) fn journal(r: &Ready, node: u64, g: &GroupReady) -> PathBuf {
    r.root
        .join(format!("store-{node}/membership-{}/decision", g.pg.0))
}
pub(super) fn input_path(r: &Ready, node: u64, g: &GroupReady) -> PathBuf {
    journal(r, node, g).join("inputs").join(
        g.input
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>(),
    )
}
pub(super) fn publication_path(r: &Ready, node: u64, g: &GroupReady) -> PathBuf {
    r.root.join(format!(
        "store-{node}/membership-{}/staging/PUBLICATION",
        g.pg.0
    ))
}

pub(super) struct Budget {
    started: Instant,
    limit: Duration,
}
impl Budget {
    pub(super) fn new(r: &Ready) -> Self {
        // Scheduler grace = 2 fair cycles. A cycle includes both unchanged
        // 800ms socket attempts plus 20ms scheduling per remote proposed peer
        // and local action, across every hosted PG. Two proposed peers are
        // remote here; the removed old voter in the TLS map does not add work.
        let groups = r.groups.len() as u64;
        let peers = r
            .groups
            .iter()
            .map(|g| g.proposal.new_voters.len() as u64 - 1)
            .max()
            .unwrap();
        let cycle = (2 * RPC_MS + RECOVERY_MS)
            .checked_mul(groups)
            .unwrap()
            .checked_mul(peers + 1)
            .unwrap();
        let batches = r.groups.iter().map(|g| g.batches as u64 + 1).max().unwrap();
        // Allow one source download and one peer upload per batch, then sixteen
        // bounded discovery/promise/accept/learn/stage/publication phase cycles.
        // Four additional cycles cover initial suspicion and one cold restart.
        // This is a fixed failure budget, not a per-RPC deadline/retry increase.
        let turns = 16u64
            .checked_add(2 * batches)
            .unwrap()
            .checked_add(4)
            .unwrap();
        Self {
            started: Instant::now(),
            limit: Duration::from_millis(cycle.checked_mul(turns).unwrap()),
        }
    }
    pub(super) fn check(&self, r: &Ready, detail: &str) {
        assert!(
            self.started.elapsed() < self.limit,
            "native recovery exceeded fixed {:?} budget after {:?}: {detail}; root={}",
            self.limit,
            self.started.elapsed(),
            r.root.display()
        );
    }
}

pub(super) struct Observer {
    last: BTreeMap<(PlacementGroupId, u64), VsrMembershipDecisionState>,
    pub(super) responses: BTreeMap<(PlacementGroupId, u64), usize>,
    trace: std::fs::File,
}
impl Observer {
    pub(super) fn new(r: &Ready, name: &str) -> Self {
        Self {
            last: BTreeMap::new(),
            responses: BTreeMap::new(),
            trace: std::fs::File::create(r.root.join(format!("observed-{name}.log"))).unwrap(),
        }
    }
    pub(super) fn state(
        &mut self,
        r: &Ready,
        node: u64,
        g: &GroupReady,
    ) -> Option<VsrMembershipDecisionState> {
        let result =
            network(r, node).membership_decision_state(NodeId(node), &g.proposal, g.bootstrap);
        writeln!(self.trace, "node={node} pg={} {result:?}", g.pg.0).unwrap();
        self.trace.flush().unwrap();
        let Ok(state) = result else {
            return None;
        };
        if let Some(old) = self.last.get(&(g.pg, node)) {
            assert!(
                state.promised >= old.promised,
                "promise regressed across native restart"
            );
            if let Some((round, input)) = old.accepted {
                let (new, value) = state.accepted.expect("acceptance disappeared");
                assert!(new >= round);
                if new == round {
                    assert_eq!(value, input);
                }
            }
        }
        if let Some((_, input)) = state.accepted {
            assert_eq!(
                input, g.input,
                "native recovery selected A instead of highest B"
            );
        }
        if let Some((_, input)) = state.chosen {
            assert_eq!(input, g.input);
        }
        *self.responses.entry((g.pg, node)).or_default() += 1;
        self.last.insert((g.pg, node), state.clone());
        Some(state)
    }
}

pub(super) fn wait_own_round(
    r: &Ready,
    node: u64,
    process: &mut Native,
    observer: &mut Observer,
    budget: &Budget,
) -> VsrMembershipRound {
    let g = &r.groups[0];
    loop {
        process.alive();
        if let Some(state) = observer.state(r, node, g)
            && let Some(round) = state.promised
            && round.coordinator == NodeId(node)
            && round > g.initial_round
        {
            assert!(state.chosen.is_none());
            return round;
        }
        budget.check(
            r,
            "waiting for native survivor's own durable recovery round",
        );
        std::thread::sleep(Duration::from_millis(RECOVERY_MS));
    }
}

pub(super) fn wait_all(
    r: &Ready,
    second: &mut Native,
    third: &mut Native,
    observer: &mut Observer,
    budget: &Budget,
) {
    loop {
        second.alive();
        third.alive();
        let mut published = true;
        for g in &r.groups {
            for node in [2, 3] {
                observer.state(r, node, g);
                match network(r, node).query_membership_publication(NodeId(node), g.pg, g.plan) {
                    Ok(report) => {
                        assert_eq!(report.input, g.input);
                        assert_eq!(report.plan, g.plan);
                        assert_eq!(report.replica, NodeId(node));
                    }
                    Err(_) => published = false,
                }
            }
        }
        if published {
            return;
        }
        budget.check(r, "waiting for native publication on all surviving PGs");
        std::thread::sleep(Duration::from_millis(RECOVERY_MS));
    }
}

pub(super) fn preflight(r: &Ready, mode: Mode) {
    for name in ["store-0", "store-1", "command-fixture", "kv-fixture"] {
        assert!(!r.root.join(name).exists());
    }
    for g in &r.groups {
        assert_ne!(g.initial, g.input);
        for node in [2, 3] {
            let store =
                MembershipDecisionStore::open_existing(journal(r, node, g), g.pg, NodeId(node))
                    .unwrap();
            assert!(store.chosen().is_none());
            assert!(!publication_path(r, node, g).parent().unwrap().exists());
            assert_eq!(store.decision_state().unwrap().authority, g.initial);
            if node == 2 || mode == Mode::AcceptedMajority {
                assert_eq!(store.accepted(), Some((g.initial_round, g.input)));
                assert!(matches!(
                    store.input_progress(g.input).unwrap(),
                    VsrMembershipInputProgress::Complete { .. }
                ));
            } else {
                assert!(store.accepted().is_none());
                let progress = store.input_progress(g.input).unwrap();
                if mode == Mode::MinorityWithPartial {
                    assert_eq!(
                        progress,
                        VsrMembershipInputProgress::Partial {
                            input: g.descriptor.clone(),
                            records: g.prefix.len() as u64
                        }
                    );
                } else {
                    assert_eq!(progress, VsrMembershipInputProgress::Missing);
                }
            }
        }
    }
}

pub(super) fn verify_cold(
    r: &Ready,
    direct_majority: bool,
) -> BTreeMap<(PlacementGroupId, u64), Vec<u8>> {
    use sha2::{Digest, Sha256};
    let mut proofs = BTreeMap::new();
    for g in &r.groups {
        for node in [2, 3] {
            let store =
                MembershipDecisionStore::open_existing(journal(r, node, g), g.pg, NodeId(node))
                    .unwrap();
            let chosen = store
                .chosen()
                .expect("native scheduler did not persist a chosen proof");
            assert_eq!(chosen.input_id(), g.input);
            if direct_majority {
                assert_eq!(
                    chosen.round(),
                    g.initial_round,
                    "accepted-majority reconstruction unnecessarily raised its ballot"
                );
            }
            let (_, records) = store.input(g.input).unwrap();
            assert_eq!(records.len() as u64, g.records);
            assert!(records.starts_with(&g.prefix));
            let cell = DurableVsrCell::open_existing(
                r.root.join(format!("store-{node}/pg-{}", g.pg.0)),
                g.pg,
            )
            .unwrap();
            assert!(cell.view() >= Epoch(2));
            assert_eq!(cell.request_commit(&g.request), Some(LogIndex(1)));
            for command in &g.commands[..g.committed] {
                assert_eq!(
                    cell.committed_operation(command.commit.index),
                    Some(&VsrOperation::ReplicatedCommand {
                        operation_id: command.commit.operation_id,
                        payload: command.encode().unwrap(),
                    })
                );
            }
            let accepted = &g.commands[g.committed];
            assert_eq!(
                cell.prepared(accepted.commit.index).unwrap().operation,
                VsrOperation::ReplicatedCommand {
                    operation_id: accepted.commit.operation_id,
                    payload: accepted.encode().unwrap(),
                }
            );
            if node == 3 {
                assert_eq!(cell.promoted_replica().unwrap(), Some(NodeId(3)));
            }
            let namespace = Sha256::digest(g.namespace.as_bytes())
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>();
            let kv_root = r.root.join(format!(
                "store-{node}/application/namespaces/{namespace}/kv"
            ));
            assert!(
                kv_root.is_dir(),
                "native process never materialized application custody"
            );
            let mut kv =
                rsf_kv::DurableKvEngine::open_replicated(&kv_root, Epoch(1), &g.namespace).unwrap();
            assert_eq!(kv.get(&g.key).unwrap().value, g.value);
            // Cold-open replay must find the already durable application receipt;
            // this duplicate never drives native recovery or manufactures a vote.
            assert!(
                kv.apply_application_command(&g.commands[0])
                    .unwrap()
                    .replayed
            );
            assert_eq!(
                kv.changes_since(rsf_core::CommitVersion(0)).len(),
                g.committed
            );
            proofs.insert(
                (g.pg, node),
                std::fs::read(publication_path(r, node, g)).unwrap(),
            );
        }
    }
    for node in [2, 3] {
        let identity: serde_json::Value = serde_json::from_slice(
            &std::fs::read(r.root.join(format!("store-{node}/IDENTITY.json"))).unwrap(),
        )
        .unwrap();
        assert_eq!(identity["version"], 2);
        assert_eq!(
            identity["managed_membership"],
            serde_json::json!(r.groups.iter().map(|g| g.pg.0).collect::<Vec<_>>())
        );
    }
    proofs
}

pub(super) fn deny_clients(r: &Ready) {
    let before = std::fs::read(r.root.join("store-3/IDENTITY.json")).unwrap();
    config(&r.root, 3, |value| {
        // All modes must reach managed admission policy even after the recovery
        // declaration is removed; a missing-election error proves no such gate.
        value["elections"] =
            serde_json::json!({"heartbeat_ms":50,"timeout_ms":6000,"rank_delay_ms":1000});
        value["client"] = serde_json::json!({"state":"forbidden-prechoice-client","listen":"127.0.0.1:1","endpoints":[]});
    });
    let rejected = rejected_start(&r.root.join("peer-3.json"));
    assert!(
        rejected.contains(
            "managed membership recovery cannot admit clients before replicated control admission"
        ),
        "{rejected}"
    );
    config(&r.root, 3, |value| {
        for cell in value["cells"].as_array_mut().unwrap() {
            cell.as_object_mut().unwrap().remove("membership_recovery");
            cell.as_object_mut().unwrap().remove("learner_source");
            cell["voters"] = serde_json::json!([1, 2, 3]);
            cell["view"] = 2.into();
            cell["primary"] = 1.into();
        }
    });
    let rejected = rejected_start(&r.root.join("peer-3.json"));
    assert!(
        rejected
            .contains("durably managed membership groups require explicit recovery configuration"),
        "{rejected}"
    );
    for g in &r.groups {
        std::fs::remove_dir_all(r.root.join(format!("store-3/membership-{}", g.pg.0))).unwrap();
    }
    let rejected = rejected_start(&r.root.join("peer-3.json"));
    assert!(
        rejected
            .contains("durably managed membership groups require explicit recovery configuration"),
        "{rejected}"
    );
    assert_eq!(
        std::fs::read(r.root.join("store-3/IDENTITY.json")).unwrap(),
        before
    );
    assert!(!r.root.join("forbidden-prechoice-client").exists());
    for g in &r.groups {
        assert!(
            !r.root
                .join(format!("store-3/membership-{}", g.pg.0))
                .exists()
        );
    }
}
