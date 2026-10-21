//! The replacement uses the installed peer logs, never a new bootstrap or a
//! copy of the lost primary's protocol WAL. Daemon election/routing is separate.
use super::*;
use rsf_core::CommitVersion;
use rsf_protocol::{
    ApplicationSurface, ClientRequestId, ReplicatedFabricCommand, VsrBackedFabricRouter,
};

fn materialize(root: &Path, command: &ReplicatedFabricCommand) -> Result<(), String> {
    let result = match command.application.surface {
        ApplicationSurface::Kv => follower_kv(root, 1)
            .apply_application_command(command)
            .map(|_| ()),
        ApplicationSurface::Sql => follower_sql(root, 1)
            .apply_application_command(command)
            .map(|_| ()),
        ApplicationSurface::Object => follower_objects(root, 1)
            .apply_application_command(command)
            .map(|_| ()),
        ApplicationSurface::Blob => follower_blob(root, 1)
            .apply_application_command(command)
            .map(|_| ()),
    };
    result.map(|_| ()).map_err(|err| err.message)
}
fn transport(root: &Path, peers: BTreeMap<NodeId, PeerEndpoint>) -> NetworkVsrTransport {
    NetworkVsrTransport::new(
        NodeId(1),
        &ClusterId::new("cli-cluster").unwrap(),
        PeerIdentity::load(&root.join("tls-1")).unwrap(),
        peers,
        Duration::from_millis(PEER_TIMEOUT_MS),
    )
    .unwrap()
}

#[test]
fn replacement_router_recovers_all_surfaces_and_original_receipts_from_installed_peer_cells() {
    use rsf_wire::WireProtocol::{Blob, Kv, Object, Sql};
    let root = root();
    let mut peers = provision(&root);
    let state = initialize_network_state(&root, &peers);
    let first = Process::start(&root.join("peer-1.json"));
    let second = Process::start(&root.join("peer-2.json"));
    peers.get_mut(&NodeId(1)).unwrap().address = first.address;
    peers.get_mut(&NodeId(2)).unwrap().address = second.address;
    configure_primary(&root, &state, &peers);
    let primary = Process::primary(&state);
    for (surface, operation, request, value) in [
        (
            Kv,
            "SET customer/1",
            "initial-kv",
            b"original kv".as_slice(),
        ),
        (
            Sql,
            "CREATE TABLE users(id PRIMARY KEY, name)",
            "initial-table",
            b"",
        ),
        (
            Sql,
            "INSERT INTO users (id, name) VALUES (1, ada)",
            "initial-row",
            b"",
        ),
        (Object, "CREATE_BUCKET media", "initial-bucket", b""),
        (
            Object,
            "PUT media/key",
            "initial-object",
            b"original object",
        ),
        (Blob, "PUT initial", "initial-blob", b"original blob"),
    ] {
        let response = surface_request(primary.address, surface, operation, request, value);
        assert!(response.contains("RSF/1 executed"), "{response}");
    }
    drop(primary); // Abruptly lose the original primary and its protocol receipts.
    std::fs::remove_dir_all(&state).unwrap(); // No recovery path may read the old primary.
    drop(first); // Release the candidate's cell for ownership transfer into its router.
    let mut candidate =
        DurableVsrCell::open_existing(root.join("store-1/pg-1"), PlacementGroupId(1)).unwrap();
    let network = transport(&root, peers.clone());
    let target_view = Epoch(candidate.view().0 + 1);
    let request = rsf_vsr::VsrViewChangeRequest {
        pg: PlacementGroupId(1),
        previous_view: candidate.view(),
        target_view,
        candidate: NodeId(1),
        voters: vec![NodeId(0), NodeId(1), NodeId(2)],
    };
    let (input, snapshots) = network
        .prepare_installation_input(&mut candidate, &request)
        .unwrap();
    network
        .stage_installation_input(&mut candidate, &input, &snapshots)
        .unwrap();
    assert_eq!(
        network
            .resume_view_installation(&mut candidate, &mut |entry| {
                let VsrOperation::ReplicatedCommand { payload, .. } = &entry.prepare.operation
                else {
                    return Err("expected complete command".into());
                };
                if payload.starts_with(b"RSFG1") {
                    rsf_protocol::ReplicatedProtocolCommand::decode(payload)
                        .map_err(|err| err.message)?;
                    return Ok(());
                }
                let command =
                    ReplicatedFabricCommand::decode(payload).map_err(|err| err.message)?;
                materialize(&root, &command)
            })
            .unwrap(),
        target_view
    );
    let coordination = root.join("replacement-protocol");
    let mut router = VsrBackedFabricRouter::open_with_installed_cells(
        &coordination,
        Epoch(1),
        NodeId(1),
        [(PlacementGroupId(1), candidate)].into(),
        network,
    )
    .unwrap();
    let recovered = router.reconcile_application_commands().unwrap();
    assert!(recovered >= 6);
    router.confirm_read_quorum().unwrap();
    let originals = router.committed_application_commands();
    let old_version = router.latest_version();
    for command in &originals {
        materialize(&root, command).unwrap();
        let retry = router
            .commit_application(
                command.commit.pg,
                command.request.clone(),
                command.commit.mode,
                command.commit.operation.clone(),
                command.application.clone(),
            )
            .unwrap();
        assert!(retry.replayed);
        assert_eq!(retry.certificate, command.certificate());
    }
    assert_eq!(router.latest_version(), old_version);
    {
        let mut kv = follower_kv(&root, 1);
        assert_eq!(kv.get(b"customer/1").unwrap().value, b"original kv");
        let snapshot = kv.snapshot(&router);
        let result = kv
            .commit_batch_routed(
                &mut router,
                rsf_kv::KvBatch {
                    client_request: Some(ClientRequestId::new("replacement-kv", 1)),
                    snapshot,
                    mutations: vec![rsf_kv::KvMutation::Set {
                        key: b"customer/1".to_vec(),
                        value: b"replacement kv".to_vec(),
                    }],
                },
            )
            .unwrap();
        assert!(result.version > old_version);
    }
    {
        let mut sql = follower_sql(&root, 1);
        assert_eq!(
            sql.read_snapshot("SELECT * FROM users WHERE name=ada")
                .unwrap()
                .rows,
            1
        );
        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "UPDATE users SET name=grace WHERE id=1",
            "replacement-sql",
        )
        .unwrap();
    }
    {
        let mut objects = follower_objects(&root, 1);
        assert_eq!(
            objects.read_verified("media", "key").unwrap(),
            b"original object"
        );
        objects
            .put(
                &mut router,
                PlacementGroupId(1),
                PlacementGroupId(1),
                ClientRequestId::new("replacement-object", 1),
                "media",
                "key",
                b"replacement object".to_vec(),
            )
            .unwrap();
    }
    {
        let mut blobs = follower_blob(&root, 1);
        assert_eq!(
            blobs
                .read_verified(&rsf_core::blob::ContentId::for_bytes(b"original blob"))
                .unwrap(),
            b"original blob"
        );
        blobs
            .put_request(
                &mut router,
                PlacementGroupId(1),
                ClientRequestId::new("replacement-blob", 1),
                b"replacement blob".to_vec(),
            )
            .unwrap();
    }
    let final_version = router.latest_version();
    assert!(final_version > old_version);
    router.confirm_read_quorum().unwrap();
    drop(router);
    drop(second);
    for node in 1..3 {
        let kv = follower_kv(&root, node);
        assert_eq!(kv.get(b"customer/1").unwrap().value, b"replacement kv");
        assert_eq!(kv.changes_since(CommitVersion(0)).len(), 2);
        let mut sql = follower_sql(&root, node);
        assert_eq!(
            sql.read_snapshot("SELECT * FROM users WHERE name=grace")
                .unwrap()
                .rows,
            1
        );
        let mut objects = follower_objects(&root, node);
        assert_eq!(
            objects.read_verified("media", "key").unwrap(),
            b"replacement object"
        );
        assert_eq!(objects.versions("media", "key").unwrap().len(), 2);
        let mut blobs = follower_blob(&root, node);
        for value in [b"original blob".as_slice(), b"replacement blob"] {
            assert_eq!(
                blobs
                    .read_verified(&rsf_core::blob::ContentId::for_bytes(value))
                    .unwrap(),
                value
            );
        }
    }
    let cells = [(
        PlacementGroupId(1),
        DurableVsrCell::open_existing(root.join("store-1/pg-1"), PlacementGroupId(1)).unwrap(),
    )]
    .into();
    let mut router = VsrBackedFabricRouter::open_with_installed_cells(
        &coordination,
        Epoch(1),
        NodeId(1),
        cells,
        transport(&root, peers.clone()),
    )
    .unwrap();
    assert_eq!(router.reconcile_application_commands().unwrap(), 0);
    assert_eq!(router.latest_version(), final_version);
    assert!(router.confirm_read_quorum().is_err());
    let cells = router.into_cells();
    let second = Process::start(&root.join("peer-2.json"));
    peers.get_mut(&NodeId(2)).unwrap().address = second.address;
    let mut router = VsrBackedFabricRouter::open_with_installed_cells(
        &coordination,
        Epoch(1),
        NodeId(1),
        cells,
        transport(&root, peers),
    )
    .unwrap();
    router.confirm_read_quorum().unwrap();
    assert_eq!(router.latest_version(), final_version);
    assert!(!coordination.with_extension("vsr").exists());
    drop(router);
    drop(second);
    std::fs::remove_dir_all(root).unwrap();
}
