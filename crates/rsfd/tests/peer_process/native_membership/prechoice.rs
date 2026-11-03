//! Native scheduler coverage: no test invokes poll_membership_recovery or sends
//! promises, chosen proofs, stage/publication requests after native startup.
use super::*;
use rsf_vsr::{
    MembershipDecisionStore, VsrHistoryBatch, VsrMembershipDecisionState,
    VsrMembershipInputProgress, VsrMembershipRound,
};
#[path = "prechoice/support.rs"]
mod support;
use support::*;

#[test]
fn native_prechoice_reconstructs_accepted_majority_and_keeps_stale_learner_clients_closed() {
    let r = fixture_mode(Mode::AcceptedMajority, 1);
    preflight(&r, Mode::AcceptedMajority);
    let mut observed = Observer::new(&r, "majority");
    let budget = Budget::new(&r);
    let (mut second, mut third) = pair(&r, "majority");
    wait_all(&r, &mut second, &mut third, &mut observed, &budget);
    third.stop();
    second.stop();
    let proofs = verify_cold(&r, true);

    // The promotion owner must survive deletion of its obsolete learner copy.
    // Rebuild derived application data through the actual native startup path.
    std::fs::remove_dir_all(r.root.join("store-3/learner-1")).unwrap();
    for node in [2, 3] {
        std::fs::remove_dir_all(r.root.join(format!("store-{node}/application"))).unwrap();
        config(&r.root, node, |value| {
            assert!(value["cells"][0]["membership_recovery"].is_object());
            if node == 3 {
                assert_eq!(value["cells"][0]["learner_source"], 0);
            }
            value["elections"] =
                serde_json::json!({"heartbeat_ms":50,"timeout_ms":6000,"rank_delay_ms":1000});
        });
    }
    let budget = Budget::new(&r);
    let (mut second, mut third) = pair(&r, "stale-learner");
    wait_all(&r, &mut second, &mut third, &mut observed, &budget);
    let election = std::time::Instant::now();
    loop {
        second.alive();
        third.alive();
        if let Ok(status) = network(&r, 3).replica_status(NodeId(3), PG)
            && status.normal.view >= Epoch(3)
            && status.normal.primary == NodeId(2)
        {
            break;
        }
        assert!(
            election.elapsed() < Duration::from_secs(35),
            "promoted stale learner failed ordinary native election; root={}",
            r.root.display()
        );
        std::thread::sleep(Duration::from_millis(30));
    }
    third.stop();
    second.stop();
    assert!(!r.root.join("store-3/learner-1").exists());
    assert_eq!(
        verify_cold(&r, true),
        proofs,
        "ordinary election changed historical publication bytes"
    );
    deny_clients(&r);
}

#[test]
fn native_prechoice_competing_survivors_keep_highest_minority_b_and_original_receipt() {
    // Both uncontrolled concurrent startup and a lower-ranked live coordinator
    // followed by a returning peer must converge through actual native work.
    for returning in [false, true] {
        let r = fixture_mode(Mode::MinorityAccepted, 1);
        preflight(&r, Mode::MinorityAccepted);
        let mut observed = Observer::new(&r, if returning { "returning" } else { "concurrent" });
        let budget = Budget::new(&r);
        let (mut second, mut third) = if returning {
            let mut second = Native::spawn(&r, 2, "existing-coordinator");
            second.ready();
            let floor = wait_own_round(&r, 2, &mut second, &mut observed, &budget);
            assert!(floor > r.groups[0].initial_round);
            let third = Native::spawn(&r, 3, "returning-survivor");
            third.ready();
            (second, third)
        } else {
            pair(&r, "minority")
        };
        wait_all(&r, &mut second, &mut third, &mut observed, &budget);
        third.stop();
        second.stop();
        verify_cold(&r, false);
        for node in [2, 3] {
            let g = &r.groups[0];
            let store =
                MembershipDecisionStore::open_existing(journal(&r, node, g), g.pg, NodeId(node))
                    .unwrap();
            assert!(
                store.chosen().unwrap().round() > g.initial_round,
                "one surviving acceptance was treated as a chosen majority"
            );
            assert!(
                observed.responses[&(g.pg, node)] > 0,
                "native inbound metadata was unresponsive"
            );
        }
        deny_clients(&r);
    }
}

#[test]
fn native_prechoice_coordinator_crash_preserves_an_actively_advancing_partial_input() {
    let r = fixture_mode(Mode::MinorityWithPartial, 12);
    let g = &r.groups[0];
    preflight(&r, Mode::MinorityWithPartial);
    assert!(
        g.batches > 12,
        "partial-copy fixture must require many real bounded exchanges"
    );
    let mut observed = Observer::new(&r, "partial-restart");
    let budget = Budget::new(&r);
    // Node3 has one actual staged B record but no accepted value. Let its native
    // scheduler persist its own round while the only complete survivor is down.
    let mut third = Native::spawn(&r, 3, "partial-coordinator");
    third.ready();
    let own = wait_own_round(&r, 3, &mut third, &mut observed, &budget);
    let mut second = Native::spawn(&r, 2, "partial-source");
    second.ready();
    let seen = loop {
        second.alive();
        third.alive();
        if let Ok(VsrMembershipInputProgress::Partial { input, records }) =
            network(&r, 3).membership_input_progress(NodeId(3), &g.proposal, g.bootstrap, g.input)
        {
            assert_eq!(input, g.descriptor);
            if records > g.prefix.len() as u64 && records < g.records {
                break records;
            }
        }
        budget.check(
            &r,
            "waiting for real native progress beyond the preloaded partial record",
        );
        std::thread::sleep(Duration::from_millis(RECOVERY_MS));
    };
    // SIGKILL/reap happens immediately after the typed cursor response. No
    // fixture write or manual scheduler step creates this advancing prefix.
    third.stop();
    let retained = {
        let store =
            MembershipDecisionStore::open_existing(journal(&r, 3, g), g.pg, NodeId(3)).unwrap();
        let state = store.decision_state().unwrap();
        assert!(state.chosen.is_none());
        assert!(
            state.accepted.is_none(),
            "crash was too late to exercise pre-nomination continuation"
        );
        assert!(state.promised >= Some(own));
        let (_, records) = store.input(g.input).unwrap();
        assert!(
            records.len() as u64 >= seen && (records.len() as u64) < g.records,
            "receiver completed before kill; this run did not exercise partial restart"
        );
        assert!(records.starts_with(&g.prefix));
        records
    };
    let mut third = Native::spawn(&r, 3, "partial-restarted");
    third.ready();
    wait_all(&r, &mut second, &mut third, &mut observed, &budget);
    third.stop();
    second.stop();
    verify_cold(&r, false);
    let store = MembershipDecisionStore::open_existing(journal(&r, 3, g), g.pg, NodeId(3)).unwrap();
    assert!(
        store.input(g.input).unwrap().1.starts_with(&retained),
        "restart changed acknowledged prefix bytes"
    );
    assert!(store.decision_state().unwrap().promised >= Some(own));
    drop(store);
    deny_clients(&r);
}

#[test]
fn native_prechoice_two_groups_progress_with_removed_candidate_and_serve_inbound_queries() {
    let mut r = fixture_groups(&[
        (PG, Mode::MinorityAccepted, 1),
        (PlacementGroupId(2), Mode::MinorityAccepted, 1),
    ]);
    preflight(&r, Mode::MinorityAccepted);
    assert_ne!(r.groups[0].pg, r.groups[1].pg);
    assert_ne!(r.groups[0].namespace, r.groups[1].namespace);
    assert_ne!(r.groups[0].key, r.groups[1].key);
    assert!(
        r.groups[0].value != r.groups[1].value,
        "two-group fixture must detect values materialized into the wrong group"
    );
    let blackhole = Blackhole::start();
    redirect_removed_candidate(&mut r, blackhole.address);
    let versions = r
        .groups
        .iter()
        .flat_map(|g| g.commands.iter().map(|c| c.commit.version))
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        versions.len(),
        r.groups.iter().map(|g| g.commands.len()).sum::<usize>()
    );
    let mut observed = Observer::new(&r, "two-groups");
    let budget = Budget::new(&r);
    // With node3 absent, no publication quorum can form. Repeated direct typed
    // replies from both PGs demonstrate inbound service while its sole worker
    // handles unavailable candidate1/survivor3 using the unchanged RPC budget.
    let mut second = Native::spawn(&r, 2, "two-groups-alone");
    second.ready();
    for _ in 0..3 {
        for g in &r.groups {
            second.alive();
            let entered = blackhole.wait_connection(&r, &budget);
            let state = observed
                .state(&r, 2, g)
                .expect("native inbound query failed during unavailable outbound work");
            assert!(
                entered.elapsed() < Duration::from_millis(RPC_MS),
                "inbound query waited for the blackholed outbound socket deadline"
            );
            assert!(state.chosen.is_none());
            assert!(!publication_path(&r, 2, g).exists());
        }
    }
    let mut third = Native::spawn(&r, 3, "two-groups-returning");
    third.ready();
    wait_all(&r, &mut second, &mut third, &mut observed, &budget);
    third.stop();
    second.stop();
    verify_cold(&r, false);
    for g in &r.groups {
        assert!(observed.responses[&(g.pg, 2)] >= 3);
        assert!(observed.responses[&(g.pg, 3)] > 0);
    }
    deny_clients(&r);
}

fn inventory(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn visit(root: &Path, path: &Path, result: &mut BTreeMap<PathBuf, Vec<u8>>) {
        for entry in std::fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if entry.file_type().unwrap().is_dir() {
                visit(root, &path, result);
            } else {
                assert!(entry.file_type().unwrap().is_file());
                result.insert(
                    path.strip_prefix(root).unwrap().to_path_buf(),
                    std::fs::read(path).unwrap(),
                );
            }
        }
    }
    let mut result = BTreeMap::new();
    visit(root, root, &mut result);
    result
}
fn copy_tree(source: &Path, target: &Path) {
    std::fs::create_dir(target).unwrap();
    for entry in std::fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        if entry.file_type().unwrap().is_dir() {
            copy_tree(&entry.path(), &target.join(entry.file_name()));
        } else {
            assert!(entry.file_type().unwrap().is_file());
            std::fs::copy(entry.path(), target.join(entry.file_name())).unwrap();
        }
    }
}

#[test]
fn native_prechoice_rejects_missing_or_valid_shortened_accepted_custody_without_repair() {
    for fault in ["descriptor", "manifest", "shortened"] {
        let r = fixture_mode(Mode::AcceptedMajority, 1);
        preflight(&r, Mode::AcceptedMajority);
        let g = &r.groups[0];
        let target = input_path(&r, 2, g);
        if fault == "shortened" {
            // Build a real valid partial WAL elsewhere, then deliberately model
            // loss of the acknowledged suffix. Never alter the decision journal.
            let source =
                MembershipDecisionStore::open_existing(journal(&r, 2, g), g.pg, NodeId(2)).unwrap();
            let authority = source.authority().unwrap();
            let first = source.input(g.input).unwrap().1[0].clone();
            drop(source);
            let scratch = r.root.join("short-prefix-fixture");
            let mut partial =
                MembershipDecisionStore::open(&scratch, NodeId(3), &authority).unwrap();
            partial.promise(NodeId(1), g.initial_round).unwrap();
            partial
                .begin_input(NodeId(1), g.initial_round, g.descriptor.clone())
                .unwrap();
            partial
                .append_input(
                    NodeId(1),
                    g.initial_round,
                    g.input,
                    VsrHistoryBatch {
                        ordinal: 0,
                        records: vec![first],
                    },
                )
                .unwrap();
            let prefix = scratch
                .join("inputs")
                .join(
                    g.input
                        .iter()
                        .map(|b| format!("{b:02x}"))
                        .collect::<String>(),
                )
                .join("records");
            drop(partial);
            std::fs::remove_dir_all(target.join("records")).unwrap();
            copy_tree(&prefix, &target.join("records"));
            std::fs::remove_dir_all(scratch).unwrap();
        } else {
            std::fs::remove_file(target.join(if fault == "descriptor" {
                "DESCRIPTOR"
            } else {
                "records/MANIFEST"
            }))
            .unwrap();
        }
        let before = inventory(&target);
        let identity = std::fs::read(r.root.join("store-2/IDENTITY.json")).unwrap();
        rejected_start(&r.root.join("peer-2.json"));
        assert_eq!(
            inventory(&target),
            before,
            "native startup repaired {fault} acknowledged input"
        );
        assert_eq!(
            std::fs::read(r.root.join("store-2/IDENTITY.json")).unwrap(),
            identity
        );
        assert!(!publication_path(&r, 2, g).exists());
        config(
            &r.root,
            2,
            |value| value["client"] = serde_json::json!({"state":"forbidden-corrupt-client","listen":"127.0.0.1:1","endpoints":[]}),
        );
        rejected_start(&r.root.join("peer-2.json"));
        assert!(!r.root.join("forbidden-corrupt-client").exists());
        assert_eq!(inventory(&target), before);
    }
}
