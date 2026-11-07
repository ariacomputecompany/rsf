//! Actual native processes, actual TLS and strict cold files; no ACK injection.
use super::bootstrap::*;
use super::*;
use std::process::{Child, Command, Stdio};
use std::time::Instant;

const CHILD_CONFIG: &str = "RSF_TEST_FRESH_CONTROL_CONFIG";
#[test]
fn native_fresh_voter_process_child() {
    let Some(config) = std::env::var_os(CHILD_CONFIG) else {
        return;
    };
    let result = run_cli(&[
        "serve".into(),
        "--config".into(),
        config.to_string_lossy().into_owned(),
    ]);
    panic!("native child returned before parent kill: {result:?}");
}
struct Children(Vec<Child>);
impl Children {
    fn stop(&mut self) {
        for child in &mut self.0 {
            let _ = child.kill();
            child.wait().unwrap();
        }
        self.0.clear();
    }
    fn live(&mut self) {
        for child in &mut self.0 {
            assert!(
                child.try_wait().unwrap().is_none(),
                "native child exited before delivery"
            );
        }
    }
}
impl Drop for Children {
    fn drop(&mut self) {
        self.stop();
    }
}
fn events(path: &Path) -> Vec<serde_json::Value> {
    fs::read_to_string(path)
        .unwrap()
        .lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect()
}

#[test]
fn native_fresh_three_process_tls_genesis_has_actual_reporter_quorum_and_strict_cold_roots() {
    let f = Fixture::new("fresh-three-process");
    let reservations = (0..3)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect::<Vec<_>>();
    let addresses = reservations
        .iter()
        .map(|listener| listener.local_addr().unwrap())
        .collect::<Vec<_>>();
    let template: serde_json::Value =
        serde_json::from_slice(&fs::read(&f.config).unwrap()).unwrap();
    let mut configurations = Vec::new();
    let mut roots = Vec::new();
    let mut logs = Vec::new();
    for node in 0..3 {
        let state = f.root.0.join(format!("native-{node}"));
        let path = f.root.0.join(format!("fresh-{node}.json"));
        let log = f.root.0.join(format!("fresh-{node}.log"));
        let mut config = template.clone();
        config["node"] = node.into();
        config["state"] = state.to_string_lossy().into_owned().into();
        config["tls"] = f
            .root
            .0
            .join(format!("tls-{node}"))
            .to_string_lossy()
            .into_owned()
            .into();
        config["listen"] = addresses[node as usize].to_string().into();
        config["control"]
            .as_object_mut()
            .unwrap()
            .remove("preferred_transport");
        for (peer, address) in addresses.iter().enumerate() {
            config["peers"][peer]["address"] = address.to_string().into();
        }
        fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
        fs::write(&log, b"").unwrap();
        configurations.push(path);
        roots.push(state);
        logs.push(log);
    }
    // Bind failures are fatal; the test never silently selects a different route.
    drop(reservations);
    let mut children = Children(Vec::new());
    for node in 0..3 {
        let output = OpenOptions::new().append(true).open(&logs[node]).unwrap();
        children.0.push(
            Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "peer::control_tests::bootstrap_process::native_fresh_voter_process_child",
                    "--nocapture",
                ])
                .env(CHILD_CONFIG, &configurations[node])
                .stdout(Stdio::from(output.try_clone().unwrap()))
                .stderr(Stdio::from(output))
                .spawn()
                .unwrap(),
        );
    }
    let startup_deadline = Instant::now() + Duration::from_secs(15);
    loop {
        children.live();
        if logs.iter().all(|path| {
            events(path)
                .iter()
                .any(|event| event["event"] == "peer.ready")
        }) {
            break;
        }
        assert!(
            Instant::now() < startup_deadline,
            "native startup failed: {:?}",
            logs.iter()
                .map(|p| fs::read_to_string(p).unwrap())
                .collect::<Vec<_>>()
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    for root in &roots {
        assert_eq!(kind(root), "initialized_voter");
    }
    let delivery_deadline = Instant::now() + Duration::from_secs(15);
    let observation = loop {
        children.live();
        if let Some(event) = events(&logs[0])
            .into_iter()
            .find(|event| event["event"] == "peer.genesis_delivery")
        {
            break event;
        }
        assert!(
            Instant::now() < delivery_deadline,
            "actual TLS delivery failed: {:?}",
            logs.iter()
                .map(|p| fs::read_to_string(p).unwrap())
                .collect::<Vec<_>>()
        );
        std::thread::sleep(Duration::from_millis(10));
    };
    assert_eq!(observation["node"], 0);
    assert_eq!(observation["pg"], 0);
    assert_eq!(observation["primary"], 0);
    let reporters = observation["reporters"]
        .as_array()
        .unwrap()
        .iter()
        .map(|id| id.as_u64().unwrap())
        .collect::<BTreeSet<_>>();
    assert!(reporters.len() >= 2 && reporters.contains(&0));
    assert!(reporters.iter().all(|node| *node < 3));
    assert!(logs[1..].iter().any(|path| {
        events(path)
            .iter()
            .any(|event| event["event"] == "peer.rpc" && event["outcome"] == "accepted")
    }));
    children.stop();
    let mut expected = None;
    let mut committed = BTreeSet::new();
    for node in 0..3 {
        let before = snapshot(&roots[node]);
        drop(PeerService::open(&configurations[node]).unwrap());
        assert_eq!(
            snapshot(&roots[node]),
            before,
            "strict native cold startup changed persisted work"
        );
        assert_eq!(kind(&roots[node]), "initialized_voter");
        let cell = DurableVsrCell::open_existing_bounded(
            roots[node].join("pg-0"),
            PlacementGroupId(0),
            f.policy.limits,
        )
        .unwrap();
        assert_eq!(
            cell.membership_lineage().unwrap().bootstrap,
            f.policy.anchor.control_bootstrap
        );
        let entries = cell.committed_prefix().unwrap();
        if entries.is_empty() {
            assert!(
                !reporters.contains(&(node as u64)),
                "reported delivery has no actual durable Genesis"
            );
            continue;
        }
        assert_eq!(entries.len(), 1);
        let entry = &entries[0];
        assert_eq!(entry.prepare.index, LogIndex(1));
        let rsf_vsr::VsrOperation::ReplicatedCommand { payload, .. } = &entry.prepare.operation
        else {
            panic!("Genesis is not replicated command");
        };
        assert_eq!(
            ControlCommand::decode(payload).unwrap(),
            ControlCommand::Genesis(f.policy.genesis.clone())
        );
        assert!(entry.commit.certificate.signers.len() >= cell.quorum());
        if let Some(expected) = &expected {
            assert_eq!(entry, expected);
        } else {
            expected = Some(entry.clone());
        }
        assert_eq!(
            ControlState::recover(&f.policy.anchor, &cell)
                .unwrap()
                .catalog(),
            &f.policy.genesis
        );
        committed.insert(node as u64);
    }
    assert!(committed.len() >= 2);
    assert!(reporters.is_subset(&committed));
    // Existing delivery completes at quorum. It does not promise all three
    // replicas committed before the stop, nor any sender/admission capability.
}
