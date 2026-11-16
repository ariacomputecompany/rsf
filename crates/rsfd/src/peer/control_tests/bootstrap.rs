//! Fresh native startup uses actual canonical storage and real identity writes.
use super::*;
use state_identity::ControlCustody;

pub(super) fn configure(f: &Fixture, node: u64) {
    let mut config: serde_json::Value =
        serde_json::from_slice(&fs::read(&f.config).unwrap()).unwrap();
    config["node"] = node.into();
    config["tls"] = f
        .root
        .0
        .join(format!("tls-{node}"))
        .to_string_lossy()
        .into_owned()
        .into();
    config["control"]
        .as_object_mut()
        .unwrap()
        .remove("preferred_transport");
    fs::write(&f.config, serde_json::to_vec(&config).unwrap()).unwrap();
}
pub(super) fn kind(state: &Path) -> String {
    let value: serde_json::Value =
        serde_json::from_slice(&fs::read(state.join("IDENTITY.json")).unwrap()).unwrap();
    value["reserved_control"]["custody"]["kind"]
        .as_str()
        .unwrap()
        .into()
}
pub(super) fn snapshot(path: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn walk(root: &Path, at: &Path, result: &mut BTreeMap<PathBuf, Vec<u8>>) {
        for entry in fs::read_dir(at).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                walk(root, &path, result);
            } else {
                result.insert(
                    path.strip_prefix(root).unwrap().to_owned(),
                    fs::read(path).unwrap(),
                );
            }
        }
    }
    let mut result = BTreeMap::new();
    if path.exists() {
        walk(path, path, &mut result);
    }
    result
}
pub(super) fn open_error(path: &Path) -> String {
    match PeerService::open(path) {
        Err(error) => error,
        Ok(_) => panic!("unexpected listening service"),
    }
}
pub(super) fn reserved(f: &Fixture) -> (control::Trusted, StateIdentity) {
    let trusted = load_config(&f.config).unwrap().control.unwrap();
    rsf_storage::create_dir_all_synced(&f.state).unwrap();
    let identity = StateIdentity::open(
        &f.state,
        &f.policy.anchor.cluster,
        NodeId(0),
        Some(trusted.reservation().unwrap()),
    )
    .unwrap();
    assert_eq!(kind(&f.state), "reserved_voter");
    (trusted, identity)
}

#[test]
fn native_fresh_voter_canonical_preflight_rejects_before_reservation() {
    for case in ["view", "primary", "limits", "mixed", "tls", "raw"] {
        let f = Fixture::new(&format!("fresh-preflight-{case}"));
        configure(&f, 0);
        let mut config: serde_json::Value =
            serde_json::from_slice(&fs::read(&f.config).unwrap()).unwrap();
        match case {
            "view" => config["cells"][0]["view"] = 2.into(),
            "primary" => config["cells"][0]["primary"] = 1.into(),
            "limits" => config["control"]["prefix_limits"]["max_encoded_bytes"] = 18.into(),
            "mixed" => config["cells"]
                .as_array_mut()
                .unwrap()
                .push(serde_json::json!({"pg":1,"view":1,"primary":2,"voters":[0,1,2]})),
            "tls" => config["peers"][2]["fingerprint"] = "44".repeat(32).into(),
            "raw" => config["cells"][0]["learner_source"] = 1.into(),
            _ => unreachable!(),
        }
        fs::write(&f.config, serde_json::to_vec(&config).unwrap()).unwrap();
        let error = open_error(&f.config);
        match case {
            "view" | "primary" => assert_eq!(
                error,
                "fresh control bootstrap differs from canonical Genesis lineage"
            ),
            "mixed" => assert_eq!(
                error,
                "fresh native voter requires exactly the dedicated control cell"
            ),
            "raw" => assert!(error.contains("cannot use raw learner"), "{error}"),
            "tls" => assert!(error.contains("certificate"), "{error}"),
            "limits" => assert_eq!(
                error,
                "learner prefix exceeds configured record/byte budget"
            ),
            _ => unreachable!(),
        }
        assert!(!f.state.exists(), "{case} reached reservation effects");
    }
}

#[test]
fn native_fresh_voter_never_adopts_preexisting_control_paths() {
    for name in ["pg-0", "learner-0"] {
        let f = Fixture::new("fresh-no-adoption");
        configure(&f, 0);
        rsf_storage::create_dir_all_synced(&f.state).unwrap();
        drop(StateIdentity::open(&f.state, &f.policy.anchor.cluster, NodeId(0), None).unwrap());
        fs::create_dir(f.state.join(name)).unwrap();
        fs::write(f.state.join(name).join("foreign"), b"do not adopt").unwrap();
        let before = snapshot(&f.state);
        assert_eq!(
            open_error(&f.config),
            "existing control root requires explicit adoption"
        );
        assert_eq!(snapshot(&f.state), before);
    }
}

#[test]
fn native_fresh_voter_initializes_before_listening_and_cold_reopens_same_bootstrap() {
    let f = Fixture::new("fresh-start-cold");
    configure(&f, 0);
    let mut service = PeerService::open(&f.config).unwrap();
    assert_eq!(kind(&f.state), "initialized_voter");
    assert_eq!(
        service
            .state_identity
            .reservation()
            .unwrap()
            .unwrap()
            .custody(),
        &ControlCustody::InitializedVoter {}
    );
    assert_eq!(service.server.control_primary().unwrap(), NodeId(0));
    assert!(service.server.committed_entries().unwrap().is_empty());
    assert!(service.genesis.is_some());
    assert_eq!(service.poll_interval, Some(Duration::from_millis(10)));
    assert!(!f.state.join("pg-1").exists());
    assert!(!f.state.join("learner-0").exists());
    let listen = service.listener.local_addr().unwrap();
    assert_ne!(listen.port(), 0);
    drop(service);
    let before = snapshot(&f.state);
    let service = PeerService::open(&f.config).unwrap();
    assert_eq!(snapshot(&f.state), before);
    drop(service);
    let cell = DurableVsrCell::open_existing_bounded(
        f.state.join("pg-0"),
        PlacementGroupId(0),
        f.policy.limits,
    )
    .unwrap();
    assert_eq!(
        cell.membership_lineage().unwrap().bootstrap,
        f.policy.anchor.control_bootstrap
    );
    assert_eq!(cell.next_index(), LogIndex(1));
    assert_eq!(cell.committed_tail(), LogIndex(0));
}

#[test]
fn native_fresh_voter_initialized_missing_storage_is_strict_and_noncreating() {
    for missing in [
        "whole",
        ".vsr-writer.lock",
        "MANIFEST",
        "MANIFEST.lock",
        "segments/000000000000000001.segment",
        "segments/000000000000000001.lock",
    ] {
        let f = Fixture::new("fresh-initialized-missing");
        configure(&f, 0);
        drop(PeerService::open(&f.config).unwrap());
        let root = f.state.join("pg-0");
        if missing == "whole" {
            fs::remove_dir_all(&root).unwrap();
        } else {
            fs::remove_file(root.join(missing)).unwrap();
        }
        let before = snapshot(&f.state);
        let error = open_error(&f.config);
        if missing == "whole" {
            assert!(
                error.starts_with("bounded voter custody:"),
                "{missing}: {error}"
            );
        } else if missing == ".vsr-writer.lock" {
            assert_eq!(error, "existing voter writer lock is missing");
        } else {
            assert!(
                error.starts_with("storage Recover failed:"),
                "{missing}: {error}"
            );
        }
        assert_eq!(kind(&f.state), "initialized_voter");
        assert_eq!(snapshot(&f.state), before, "factory recreated {missing}");
    }
}

#[test]
fn native_fresh_follower_never_queues_genesis_and_primary_reconciles_same_accepted_tail() {
    let follower = Fixture::new("fresh-follower-quiet");
    configure(&follower, 1);
    let mut service = PeerService::open(&follower.config).unwrap();
    let before = snapshot(&follower.state);
    for _ in 0..3 {
        assert!(
            service
                .genesis
                .as_mut()
                .unwrap()
                .poll(
                    service.local,
                    service.control.as_ref().unwrap(),
                    &mut service.state_identity,
                    &mut service.server
                )
                .unwrap()
                .is_none()
        );
    }
    assert_eq!(snapshot(&follower.state), before);
    drop(service);
    let f = Fixture::new("fresh-primary-accepted");
    configure(&f, 0);
    let mut service = PeerService::open(&f.config).unwrap();
    let progress = service
        .genesis
        .as_mut()
        .unwrap()
        .poll(
            service.local,
            service.control.as_ref().unwrap(),
            &mut service.state_identity,
            &mut service.server,
        )
        .unwrap();
    assert!(matches!(
        progress,
        Some(rsf_rpc::replication::ControlWriterProgress::Accepted { index: LogIndex(1) })
    ));
    drop(service);
    let cell = DurableVsrCell::open_existing_bounded(
        f.state.join("pg-0"),
        PlacementGroupId(0),
        f.policy.limits,
    )
    .unwrap();
    let original = cell.prepared(LogIndex(1)).unwrap().clone();
    assert_eq!(cell.next_index(), LogIndex(2));
    assert_eq!(cell.committed_tail(), LogIndex(0));
    drop(cell);
    let mut service = PeerService::open(&f.config).unwrap();
    service
        .genesis
        .as_mut()
        .unwrap()
        .poll(
            service.local,
            service.control.as_ref().unwrap(),
            &mut service.state_identity,
            &mut service.server,
        )
        .unwrap();
    drop(service);
    let cell = DurableVsrCell::open_existing_bounded(
        f.state.join("pg-0"),
        PlacementGroupId(0),
        f.policy.limits,
    )
    .unwrap();
    assert_eq!(cell.prepared(LogIndex(1)), Some(&original));
    assert_eq!(cell.next_index(), LogIndex(2));
    assert_eq!(cell.committed_tail(), LogIndex(0));
}

#[test]
fn native_fresh_initialized_promise_reopens_for_recovery_without_scheduling_old_primary() {
    let f = Fixture::new("fresh-promised-restart");
    configure(&f, 0);
    drop(PeerService::open(&f.config).unwrap());
    let mut cell = DurableVsrCell::open_existing_bounded(
        f.state.join("pg-0"),
        PlacementGroupId(0),
        f.policy.limits,
    )
    .unwrap();
    let promise = rsf_vsr::VsrViewChangeRequest {
        pg: PlacementGroupId(0),
        previous_view: Epoch(1),
        target_view: Epoch(2),
        candidate: NodeId(1),
        voters: vec![NodeId(0), NodeId(1), NodeId(2)],
    };
    cell.begin_view_change(NodeId(0), NodeId(1), promise.clone())
        .unwrap();
    drop(cell);
    let before = snapshot(&f.state);
    let mut service = PeerService::open(&f.config).unwrap();
    let error = service
        .genesis
        .as_mut()
        .unwrap()
        .poll(
            service.local,
            service.control.as_ref().unwrap(),
            &mut service.state_identity,
            &mut service.server,
        )
        .unwrap_err();
    assert_eq!(
        error,
        "VSR view 1 is fenced by durable promise for view 2 candidate 1; quorum-selected installation is required"
    );
    service.state_identity.validate().unwrap();
    drop(service);
    assert_eq!(snapshot(&f.state), before);
    let cell = DurableVsrCell::open_existing_bounded(
        f.state.join("pg-0"),
        PlacementGroupId(0),
        f.policy.limits,
    )
    .unwrap();
    assert_eq!(cell.view_change_promise(), Some(&promise));
    assert_eq!(cell.next_index(), LogIndex(1));
    assert_eq!(cell.committed_tail(), LogIndex(0));
}
