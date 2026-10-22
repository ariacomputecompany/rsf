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
    let network = |peers| {
        NetworkVsrTransport::new(
            NodeId(1),
            &cluster(),
            identity(&root, 1),
            peers,
            Duration::from_millis(200),
        )
        .unwrap()
    };
    let mut candidate = cell(&root, 1);
    let request = VsrViewChangeRequest {
        pg: PlacementGroupId(1),
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
    candidate
        .finish_installation_input(NodeId(1), input.id().unwrap())
        .unwrap();
    if confirmed_before_preemption {
        network(peers.clone())
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
    server.control_transport = network(peers);
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
