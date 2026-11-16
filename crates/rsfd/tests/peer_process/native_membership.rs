//! Real binary publication, managed role recovery and durable admission denial.
use super::*;
#[path = "native_membership/fixture.rs"]
mod fixture;
use fixture::*;

#[test]
fn native_managed_membership_recovers_without_candidate_and_never_admits_clients_from_local_policy()
{
    let r = fixture();
    let g = &r.groups[0];
    // Missing managed anchors must not be replaced by empty bootstraps/learners
    // during startup, even before monotonic identity enrollment has completed.
    for (node, name) in [(2, "pg-1"), (3, "learner-1")] {
        let original = r.root.join(format!("store-{node}/{name}"));
        let saved = r.root.join(format!("saved-anchor-{node}"));
        std::fs::rename(&original, &saved).unwrap();
        rejected_start(&r.root.join(format!("peer-{node}.json")));
        assert!(
            !original.exists(),
            "managed startup recreated a missing anchor"
        );
        std::fs::rename(saved, original).unwrap();
    }
    // No elections configured: both idle processes must still schedule recovery.
    // Start them back-to-back so their outbound work overlaps in real time.
    let second = Process::start(&r.root.join("peer-2.json"));
    let third = Process::start(&r.root.join("peer-3.json"));
    wait_published(&r);
    let initial_proofs = [2, 3].map(|node| {
        std::fs::read(
            r.root
                .join(format!("store-{node}/membership-1/staging/PUBLICATION")),
        )
        .unwrap()
    });
    drop(third);
    drop(second);
    for node in [2, 3] {
        let cell =
            DurableVsrCell::open_existing(r.root.join(format!("store-{node}/pg-1")), PG).unwrap();
        assert_eq!(cell.view(), Epoch(2));
        assert_eq!(cell.request_commit(&g.request), Some(LogIndex(1)));
        assert_eq!(
            cell.prepared(LogIndex(2)).unwrap().operation,
            VsrOperation::ReplicatedCommand {
                operation_id: g.commands[1].commit.operation_id,
                payload: g.commands[1].encode().unwrap(),
            }
        );
        if node == 3 {
            assert_eq!(cell.promoted_replica().unwrap(), Some(NodeId(3)));
        }
        let identity: serde_json::Value = serde_json::from_slice(
            &std::fs::read(r.root.join(format!("store-{node}/IDENTITY.json"))).unwrap(),
        )
        .unwrap();
        assert_eq!(identity["version"], 2);
        assert_eq!(identity["managed_membership"], serde_json::json!([1]));
        assert_eq!(
            follower_kv(&r.root, node).get(&g.key).unwrap().value.len(),
            128 * 1024
        );
    }
    std::fs::remove_dir_all(r.root.join("store-3/learner-1")).unwrap();
    for node in [2, 3] {
        std::fs::remove_dir_all(r.root.join(format!("store-{node}/application"))).unwrap();
        config(&r.root, node, |value| {
            value["elections"] = serde_json::json!({
                "heartbeat_ms":50,"timeout_ms":6000,"rank_delay_ms":1000,
            })
        });
    }
    // Stale learner_source remains in node3's config. Managed ancestry, journal
    // and owner recovery permit voting without resurrecting its deleted source.
    let second = Process::start(&r.root.join("peer-2.json"));
    let third = Process::start(&r.root.join("peer-3.json"));
    wait_published(&r);
    assert!(!r.root.join("store-3/learner-1").exists());
    let network = transport(&r);
    let deadline = std::time::Instant::now() + Duration::from_secs(35);
    loop {
        if let Ok(status) = network.replica_status(NodeId(3), PG)
            && status.normal.view.0 >= 3
            && status.normal.primary == NodeId(2)
        {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "new native voter quorum did not elect the living candidate"
        );
        std::thread::sleep(Duration::from_millis(30));
    }
    drop(second);
    let mut primary = DurableVsrCell::open_existing(r.root.join("store-2/pg-1"), PG).unwrap();
    assert_eq!(primary.primary(), NodeId(2));
    let mut network = transport(&r);
    if primary.committed_tail() < LogIndex(2) {
        let accepted = primary.reprepare(NodeId(2), LogIndex(2)).unwrap();
        primary
            .replicate_with_transport(NodeId(2), accepted.index, &mut network)
            .unwrap();
    }
    assert_eq!(
        primary.committed_operation(LogIndex(2)),
        Some(&VsrOperation::ReplicatedCommand {
            operation_id: g.commands[1].commit.operation_id,
            payload: g.commands[1].encode().unwrap(),
        })
    );
    let payload = g.commands[2].encode().unwrap();
    let VsrDecision::Prepared(later) = primary
        .prepare(
            NodeId(2),
            VsrOperation::ReplicatedCommand {
                operation_id: g.commands[2].commit.operation_id,
                payload: payload.clone(),
            },
            payload.len() as u32,
        )
        .unwrap()
    else {
        panic!("new voter prepare");
    };
    assert_eq!(later.index, LogIndex(3));
    primary
        .replicate_with_transport(NodeId(2), later.index, &mut network)
        .unwrap();
    drop(primary);
    drop(third);
    let promoted = DurableVsrCell::open_existing(r.root.join("store-3/pg-1"), PG).unwrap();
    assert_eq!(promoted.request_commit(&g.request), Some(LogIndex(1)));
    assert_eq!(
        promoted.committed_operation(LogIndex(3)),
        Some(&later.operation)
    );
    assert_eq!(
        follower_kv(&r.root, 3).get(&g.key).unwrap().value,
        b"committed through the new native voter after election"
    );
    drop(promoted);
    assert_eq!(
        [2, 3].map(|node| std::fs::read(
            r.root
                .join(format!("store-{node}/membership-1/staging/PUBLICATION"))
        )
        .unwrap()),
        initial_proofs
    );
    for name in ["store-0", "store-1"] {
        assert!(!r.root.join(name).exists());
    }

    let identity = std::fs::read(r.root.join("store-3/IDENTITY.json")).unwrap();
    config(&r.root, 3, |value| {
        value["cells"][0]
            .as_object_mut()
            .unwrap()
            .remove("membership_recovery");
        value["cells"][0]
            .as_object_mut()
            .unwrap()
            .remove("learner_source");
        value["cells"][0]["voters"] = serde_json::json!([1, 2, 3]);
        value["cells"][0]["primary"] = 2.into();
        value["cells"][0]["view"] = 3.into();
        value["client"] = serde_json::json!({"state":"forbidden-client-state","listen":"127.0.0.1:1","endpoints":[]});
    });
    let rejected = rejected_start(&r.root.join("peer-3.json"));
    assert!(rejected.contains("managed membership"), "{rejected}");
    std::fs::remove_dir_all(r.root.join("store-3/membership-1")).unwrap();
    let rejected = rejected_start(&r.root.join("peer-3.json"));
    assert!(rejected.contains("managed membership"), "{rejected}");
    assert_eq!(
        std::fs::read(r.root.join("store-3/IDENTITY.json")).unwrap(),
        identity
    );
    assert!(!r.root.join("forbidden-client-state").exists());
    assert!(!r.root.join("store-3/membership-1").exists());
    config(&r.root, 3, |value| {
        value.as_object_mut().unwrap().remove("client");
        value["cells"][0]["membership_recovery"] = serde_json::json!({"interval_ms":RECOVERY_MS});
    });
    let rejected = rejected_start(&r.root.join("peer-3.json"));
    assert!(rejected.contains("custody"), "{rejected}");
    assert!(!r.root.join("store-3/membership-1").exists());
}

#[path = "native_membership/prechoice.rs"]
mod prechoice;
