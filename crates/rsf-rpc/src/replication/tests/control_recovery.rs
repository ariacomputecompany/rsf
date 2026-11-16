//! A locally published view is not a quorum-installed view. A higher durable
//! promise can supersede its retained input even when its uploader restarts.
use super::*;
use crate::replication::PeerElectionTiming;
use rsf_vsr::{VsrClientRequestId, VsrViewChangeRequest};

fn recover_preempted_primary(confirmed_before_preemption: bool) {
    let root = root(if confirmed_before_preemption {
        "confirmed-primary-preempted"
    } else {
        "partial-primary-preempted"
    });
    provision(&root);
    let mut peer = PeerProcess::spawn_staging(&root, 2, false);
    let mut peers = endpoints(&root);
    peers.get_mut(&NodeId(2)).unwrap().address = peer.address;
    let mut old = cell(&root, 0);
    let prepared = prepare(&mut old, b"data must survive the superseded installation");
    old.replicate_with_transport(
        NodeId(0),
        prepared.index,
        &mut transport(&root, peers.clone()),
    )
    .unwrap();
    drop(old);
    drop(peer);
    let request_id = VsrClientRequestId::new("original-request", 7);
    let mut follower = cell(&root, 2);
    follower
        .remember_client_request(request_id.clone(), LogIndex(1))
        .unwrap();
    drop(follower);
    peer = PeerProcess::spawn_staging(&root, 2, false);
    peers.get_mut(&NodeId(2)).unwrap().address = peer.address;
    let network = |peers, timeout| {
        NetworkVsrTransport::new(NodeId(1), &cluster(), identity(&root, 1), peers, timeout).unwrap()
    };
    // Fixture publication uses the suite's ordinary host RPC budget; recovery
    // below retains its shorter election deadline and convergence bound.
    let mut candidate = cell(&root, 1);
    let request = VsrViewChangeRequest {
        pg: PlacementGroupId(1),
        previous_view: Epoch(1),
        target_view: Epoch(2),
        candidate: NodeId(1),
        voters: vec![NodeId(0), NodeId(1), NodeId(2)],
    };
    let (input, snapshots) = network(peers.clone(), Duration::from_millis(800))
        .prepare_installation_input(&mut candidate, &request)
        .unwrap();
    network(peers.clone(), Duration::from_millis(800))
        .stage_installation_input(&mut candidate, &input, &snapshots)
        .unwrap();
    candidate
        .finish_installation_input(NodeId(1), input.id().unwrap())
        .unwrap();
    if confirmed_before_preemption {
        network(peers.clone(), Duration::from_millis(800))
            .resume_view_installation(&mut candidate, &mut |entry| {
                persist_staged_entry(&root, 1, entry)
            })
            .unwrap();
    }
    // Reconstruct controller state after the candidate's local publication.
    drop(candidate);
    let mut server = PeerReplicaServer::new(
        NodeId(1),
        &cluster(),
        identity(&root, 1),
        peers.clone(),
        [(PlacementGroupId(1), cell(&root, 1))].into(),
        Duration::from_millis(200),
    )
    .unwrap();
    let timing = PeerElectionTiming {
        heartbeat: Duration::from_millis(20),
        timeout: Duration::from_millis(1000),
        rank_delay: Duration::from_millis(200),
    };
    if confirmed_before_preemption {
        assert!(
            server
                .poll_elections(timing, &mut |entry| persist_staged_entry(&root, 1, entry))
                .unwrap()
                .unwrap()
                .quorum_confirmed
        );
    }
    drop(peer);
    let mut follower = cell(&root, 2);
    follower
        .begin_view_change(
            NodeId(2),
            NodeId(2),
            VsrViewChangeRequest {
                target_view: Epoch(3),
                candidate: NodeId(2),
                ..request
            },
        )
        .unwrap();
    drop(follower);
    peer = PeerProcess::spawn_staging(&root, 2, false);
    peers.get_mut(&NodeId(2)).unwrap().address = peer.address;
    server.control_transport = network(peers, Duration::from_millis(200));
    if confirmed_before_preemption {
        std::thread::sleep(timing.timeout + timing.rank_delay);
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut updates = Vec::new();
    let mut recovered = false;
    while Instant::now() < deadline {
        if let Some(update) = server
            .poll_elections(timing, &mut |entry| persist_staged_entry(&root, 1, entry))
            .unwrap()
        {
            let ready = update.quorum_confirmed && update.view >= Epoch(4);
            updates.push((
                update.view.0,
                update.primary.0,
                update.quorum_confirmed,
                update.detail,
            ));
            if ready {
                recovered = true;
                break;
            }
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    assert!(
        recovered,
        "superseded primary did not obtain a fresh quorum: {updates:?}"
    );
    let remote = server
        .control_transport
        .replica_status(NodeId(2), PlacementGroupId(1))
        .unwrap();
    assert!(remote.promise.is_none());
    assert!(remote.normal.view >= Epoch(4));
    assert_eq!(remote.normal.primary, NodeId(1));
    drop(server);
    drop(peer);
    for node in [1, 2] {
        let recovered = cell(&root, node);
        assert!(recovered.view() >= Epoch(4));
        assert_eq!(recovered.committed_tail(), LogIndex(1));
        assert_eq!(recovered.request_commit(&request_id), Some(LogIndex(1)));
        assert_eq!(
            recovered.committed_operation(LogIndex(1)),
            Some(&prepared.operation)
        );
        assert_eq!(
            std::fs::read(root.join(format!("materialized-{node}/1"))).unwrap(),
            prepared.encode().unwrap()
        );
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn restarted_partial_primary_recovers_above_another_voters_higher_promise() {
    recover_preempted_primary(false);
}

#[test]
fn previously_confirmed_primary_recovers_quorum_after_higher_promise() {
    recover_preempted_primary(true);
}

#[test]
fn stale_primary_waits_once_for_newer_installed_view_then_recovers_if_repair_never_arrives() {
    let root = root("stale-primary-adoption-window");
    provision(&root);
    let pg = PlacementGroupId(1);
    let mut peer = PeerProcess::spawn_staging(&root, 2, false);
    let mut peers = endpoints(&root);
    peers.get_mut(&NodeId(2)).unwrap().address = peer.address;
    let network = |peers| {
        NetworkVsrTransport::new(
            NodeId(1),
            &cluster(),
            identity(&root, 1),
            peers,
            Duration::from_millis(800),
        )
        .unwrap()
    };
    let mut candidate = cell(&root, 1);
    let request = VsrViewChangeRequest {
        pg,
        previous_view: Epoch(1),
        target_view: Epoch(2),
        candidate: NodeId(1),
        voters: vec![NodeId(0), NodeId(1), NodeId(2)],
    };
    let (input, snapshots) = network(peers.clone())
        .prepare_installation_input(&mut candidate, &request)
        .unwrap();
    network(peers.clone())
        .stage_installation_input(&mut candidate, &input, &snapshots)
        .unwrap();
    network(peers.clone())
        .resume_view_installation(&mut candidate, &mut |_| Ok(()))
        .unwrap();
    drop(peer);
    // Another actual voter obtains and publishes the next view using the two
    // surviving histories. Keep the returning candidate on its older view.
    let mut follower = cell(&root, 2);
    let newer = VsrViewChangeRequest {
        target_view: Epoch(3),
        candidate: NodeId(2),
        ..request
    };
    let mut absent = cell(&root, 0);
    let reports = [
        absent
            .begin_view_change(NodeId(0), NodeId(2), newer.clone())
            .unwrap(),
        follower
            .begin_view_change(NodeId(2), NodeId(2), newer)
            .unwrap(),
    ];
    let plan = rsf_vsr::VsrViewInstallation::select(&reports).unwrap();
    follower.install_view(&plan).unwrap();
    drop(absent);
    drop(follower);
    drop(candidate);
    peer = PeerProcess::spawn_staging(&root, 2, false);
    peers.get_mut(&NodeId(2)).unwrap().address = peer.address;
    let mut server = PeerReplicaServer::new(
        NodeId(1),
        &cluster(),
        identity(&root, 1),
        peers,
        [(pg, cell(&root, 1))].into(),
        Duration::from_millis(200),
    )
    .unwrap();
    let timing = PeerElectionTiming {
        heartbeat: Duration::from_millis(20),
        timeout: Duration::from_millis(1000),
        rank_delay: Duration::from_millis(200),
    };
    let initial = server.cells[&pg].replica_status().unwrap();
    let update = server
        .poll_elections(timing, &mut |_| Ok(()))
        .unwrap()
        .unwrap();
    assert!(!update.quorum_confirmed);
    assert_eq!(update.pg, pg);
    assert_eq!(update.view, Epoch(2));
    assert_eq!(update.primary, NodeId(1));
    assert_eq!(
        update.detail,
        "higher installed view observed; awaiting current primary repair"
    );
    assert!(initial.promise.is_none());
    assert_eq!(server.cells[&pg].replica_status().unwrap(), initial);
    let adoption = server.election_adoption_window(pg).unwrap();
    assert_eq!(adoption.0, Epoch(3));
    let may_elect_at = adoption.1 + timing.timeout;
    let expected_request = |target_view| VsrViewChangeRequest {
        pg,
        previous_view: Epoch(1),
        target_view,
        candidate: NodeId(1),
        voters: initial.normal.voters.clone(),
    };
    // Repeated authenticated inventory must not renew the adoption window.
    // This peer serves RPCs but has no controller to perform primary repair.
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut recovered = false;
    let mut updates = Vec::new();
    let mut checked_installed_input = None;
    let mut last_status = initial.clone();
    while Instant::now() < deadline {
        let update = server
            .poll_elections_with_progress(timing, &mut |_| Ok(()), &mut |phase| {
                assert_eq!(phase.pg, pg);
                assert_eq!(phase.node, NodeId(1));
                assert!(
                    Instant::now() >= may_elect_at,
                    "election started before the original adoption window: {phase:?}"
                );
            })
            .unwrap();
        assert_eq!(server.election_adoption_window(pg), Some(adoption));
        let status = server.cells[&pg].replica_status().unwrap();
        if let Some(update) = update {
            let observed_at = Instant::now();
            let cell = server.cells.get_mut(&pg).unwrap();
            updates.push(format!("{update:?}; local={status:?}"));
            assert_eq!(update.pg, pg);
            assert_eq!(update.primary, NodeId(1));
            assert_eq!(
                status.normal,
                rsf_vsr::VsrPrimaryProbe {
                    view: update.view,
                    ..initial.normal.clone()
                }
            );
            assert!(update.view == Epoch(2) || update.view >= Epoch(4));
            assert!(cell.has_installed_view());
            if observed_at < may_elect_at {
                assert_eq!(status, initial);
                assert!(!update.quorum_confirmed);
            }
            if let Some(promise) = &status.promise {
                assert_eq!(*promise, expected_request(promise.target_view));
                assert!(promise.target_view >= Epoch(4));
                assert!(promise.target_view > status.normal.view);
                assert!(!update.quorum_confirmed);
            } else {
                assert_eq!(cell.primary_probe().unwrap(), status.normal);
            }
            // Local publication precedes remote confirmation. An unconfirmed
            // newer view must still have exact, complete installation custody;
            // a partial next upload must bind the current local promise.
            let input = cell.installation_input().unwrap().unwrap();
            assert_eq!(
                *input.request(),
                expected_request(input.request().target_view)
            );
            if input.request().target_view == status.normal.view {
                let id = input.id().unwrap();
                if checked_installed_input != Some(id) {
                    cell.installation_snapshots(id).unwrap();
                    checked_installed_input = Some(id);
                }
            } else {
                assert_eq!(status.promise.as_ref(), Some(input.request()));
            }
            if update.quorum_confirmed {
                assert!(update.view >= Epoch(4));
                assert!(status.promise.is_none());
                assert!(observed_at <= deadline, "late recovery: {updates:?}");
                recovered = true;
                break;
            }
        } else {
            assert_eq!(status, last_status);
        }
        last_status = status;
        std::thread::sleep(Duration::from_millis(25));
    }
    assert!(
        recovered,
        "read-only inventory prevented bounded recovery: {updates:?}"
    );
    let installed = server.cells[&pg].replica_status().unwrap();
    drop(server);
    drop(peer);
    // Node zero remained absent. Confirmation therefore requires the real
    // surviving voter, whose complete installed identity must survive reopen.
    for node in [1, 2] {
        let mut recovered = cell(&root, node);
        assert!(recovered.has_installed_view());
        assert_eq!(recovered.replica_status().unwrap(), installed);
        let input = recovered.installation_input().unwrap().unwrap();
        assert_eq!(*input.request(), expected_request(installed.normal.view));
        recovered
            .installation_snapshots(input.id().unwrap())
            .unwrap();
    }
    std::fs::remove_dir_all(root).unwrap();
}
