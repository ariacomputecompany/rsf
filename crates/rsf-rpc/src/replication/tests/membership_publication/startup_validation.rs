use super::*;
use crate::replication::PeerMembershipRecoveryTiming;

fn strict_server(
    root: &Path,
    node: u64,
    peers: BTreeMap<NodeId, PeerEndpoint>,
) -> Result<PeerReplicaServer, String> {
    let cell = DurableVsrCell::open_existing(root.join(format!("store-{node}")), PG)
        .map_err(|error| error.message)?;
    PeerReplicaServer::new(
        NodeId(node),
        &cluster(),
        identity(root, node),
        peers,
        [(PG, cell)].into(),
        Duration::from_millis(800),
    )?
    .with_existing_membership_paths([(PG, root.join(format!("membership-{node}")))].into())?
    .with_membership_recovery(
        [(
            PG,
            PeerMembershipRecoveryTiming {
                interval: Duration::from_millis(20),
            },
        )]
        .into(),
    )
}

#[test]
fn real_tls_strict_cold_open_reuses_validated_inputs_without_rejecting_published_b_ancestry() {
    let mut ready = fixture();
    let path = ready.root.clone();
    let authority = ready.local().membership_decisions[&PG].authority().unwrap();
    let authority_id = VsrMembershipInput::from_approval(&authority)
        .unwrap()
        .id()
        .unwrap();
    let chosen_id = ready.chosen.input_id();
    assert_ne!(authority_id, chosen_id);
    drop(ready.local.take());

    // A valid transition journal cannot authorize an unrelated newer anchor.
    // The strict callback error must not enter the generic factory-resume path.
    let unrelated = DurableVsrCell::open(
        path.join("foreign-anchor"),
        PG,
        Epoch(9),
        NodeId(0),
        [NodeId(0), NodeId(1), NodeId(2)],
    )
    .unwrap();
    assert!(unrelated.validate_membership_authority(&authority).is_err());
    let rejected = PeerReplicaServer::new(
        NodeId(2),
        &cluster(),
        identity(&path, 2),
        ready.peers.clone(),
        [(PG, unrelated)].into(),
        Duration::from_millis(800),
    )
    .unwrap()
    .with_existing_membership_paths([(PG, path.join("membership-2"))].into());
    assert!(
        rejected.is_err(),
        "strict cold open admitted a foreign newer anchor"
    );
    ready.local = Some(strict_server(&path, 2, ready.peers.clone()).unwrap());
    assert!(ready.local().membership_recovery_enabled());
    assert!(ready.local().membership_admission_required().unwrap());
    ready.publish();
    let retained = [2, 3].map(|node| inventory(&path, node).input);

    // Real authenticated survivors install an ordinary descendant view, commit
    // the accepted suffix, then retain a still newer unfulfilled promise.
    let mut network = ready.network();
    let request = VsrViewChangeRequest {
        pg: PG,
        previous_view: Epoch(2),
        target_view: Epoch(3),
        candidate: NodeId(2),
        voters: proposal().new_voters,
    };
    let local = ready.local().cells.get_mut(&PG).unwrap();
    let (input, snapshots) = network.prepare_installation_input(local, &request).unwrap();
    network
        .stage_installation_input(local, &input, &snapshots)
        .unwrap();
    assert_eq!(
        network
            .resume_view_installation(local, &mut |entry| persist_staged_entry(&path, 2, entry))
            .unwrap(),
        Epoch(3)
    );
    let accepted = local.reprepare(NodeId(2), LogIndex(2)).unwrap();
    local
        .replicate_with_transport(NodeId(2), accepted.index, &mut network)
        .unwrap();
    assert_eq!(accepted.operation, ready.accepted.operation);
    let promise = VsrViewChangeRequest {
        previous_view: Epoch(3),
        target_view: Epoch(4),
        ..request
    };
    network
        .prepare_installation_input(ready.local().cells.get_mut(&PG).unwrap(), &promise)
        .unwrap();
    drop(ready.remote.take());
    drop(ready.local.take());
    let before = [2, 3].map(|node| inventory(&path, node));
    let plan = ready.quorum.plan_id();
    for node in [2, 3] {
        let mut reopened = strict_server(&path, node, ready.peers.clone()).unwrap();
        let cell = &reopened.cells[&PG];
        assert!(
            cell.validate_membership_authority(&authority).is_err(),
            "stale A unexpectedly satisfies first-publication authority"
        );
        assert_eq!(cell.view(), Epoch(3));
        assert_eq!(cell.view_change_promise(), Some(&promise));
        assert!(cell.primary_probe().is_err());
        assert_eq!(cell.request_commit(&ready.request), Some(LogIndex(1)));
        assert_eq!(
            cell.committed_operation(LogIndex(2)),
            Some(&ready.accepted.operation)
        );
        assert_eq!(
            reopened.membership_decisions[&PG]
                .chosen()
                .unwrap()
                .input_id(),
            chosen_id
        );
        assert!(reopened.membership_admission_required().unwrap());
        let token = cell.recovery_state_token().unwrap();
        // Force the historical query to materialize the CURRENT committed
        // suffix, whose second entry did not exist at original publication.
        let materialized = path.join(format!("materialized-{node}"));
        std::fs::remove_dir_all(&materialized).unwrap();
        let report = reopened
            .query_membership_publication(PG, plan, &mut |entry| {
                persist_staged_entry(&path, node, entry)
            })
            .unwrap();
        assert_eq!(report.replica, NodeId(node));
        assert_eq!(report.input, chosen_id);
        assert_eq!(
            std::fs::read(materialized.join("2")).unwrap(),
            accepted.encode().unwrap()
        );
        assert_eq!(reopened.cells[&PG].recovery_state_token().unwrap(), token);
        assert_eq!(reopened.cells[&PG].view_change_promise(), Some(&promise));
        drop(reopened);
    }
    assert_eq!([2, 3].map(|node| inventory(&path, node)), before);
    assert_eq!([2, 3].map(|node| inventory(&path, node).input), retained);

    // Active historical B never excuses corrupt complete journal custody,
    // even when the sealed stage independently retains a valid B proof.
    let name = chosen_id
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let descriptor = path
        .join("membership-2/decision/inputs")
        .join(name)
        .join("DESCRIPTOR");
    let original = std::fs::read(&descriptor).unwrap();
    std::fs::write(&descriptor, b"corrupt chosen descriptor").unwrap();
    assert!(strict_server(&path, 2, ready.peers.clone()).is_err());
    assert_eq!(
        std::fs::read(&descriptor).unwrap(),
        b"corrupt chosen descriptor"
    );
    assert_eq!([2, 3].map(|node| inventory(&path, node)), before);
    std::fs::write(&descriptor, original).unwrap();
    let reopened = strict_server(&path, 2, ready.peers.clone()).unwrap();
    assert_eq!(reopened.cells[&PG].view_change_promise(), Some(&promise));
    drop(reopened);
    for removed in ["store-0", "store-1", "membership-1"] {
        assert!(!path.join(removed).exists());
    }
}
