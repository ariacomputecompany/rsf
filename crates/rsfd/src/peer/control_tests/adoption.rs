use super::*;

#[test]
fn native_control_legacy_published_learner_requires_explicit_adoption_before_bounded_open() {
    let mut f = Fixture::new("adopt-learner");
    let snapshot = f.genesis_snapshot();
    rsf_storage::create_dir_all_synced(&f.state).unwrap();
    drop(StateIdentity::open(&f.state, &f.policy.anchor.cluster, NodeId(3), None).unwrap());
    let mut learner = rsf_vsr::LearnerHistoryStore::open(
        f.control_root(),
        PlacementGroupId(0),
        NodeId(3),
        NodeId(0),
    )
    .unwrap();
    learner.begin(snapshot.descriptor.clone()).unwrap();
    let id = snapshot.descriptor.id().unwrap();
    for (ordinal, record) in snapshot.records.iter().enumerate() {
        learner.append(id, ordinal as u64, record.clone()).unwrap();
    }
    assert_eq!(learner.finish(id).unwrap(), LogIndex(1));
    drop(learner);
    let original = fs::read(f.state.join("IDENTITY.json")).unwrap();
    let error = match PeerService::open(&f.config) {
        Err(e) => e,
        Ok(_) => panic!("implicit legacy adoption"),
    };
    assert_eq!(error, "existing control root requires explicit adoption");
    assert_eq!(fs::read(f.state.join("IDENTITY.json")).unwrap(), original);
    let mut cfg: serde_json::Value = serde_json::from_slice(&fs::read(&f.config).unwrap()).unwrap();
    cfg["control"]["provisioning"] = serde_json::json!({"kind":"existing_learner","source":0});
    fs::write(&f.config, serde_json::to_vec(&cfg).unwrap()).unwrap();
    let mut service = PeerService::open(&f.config).unwrap();
    let value: serde_json::Value =
        serde_json::from_slice(&fs::read(f.state.join("IDENTITY.json")).unwrap()).unwrap();
    assert_eq!(
        value["reserved_control"]["custody"],
        serde_json::json!({"kind":"existing_learner","source":0})
    );
    wire::exchange(&mut service, &f, 37, wire::initial(&f, 0, &[]))
        .rejected("adopted control learner has no initial transfer origin");
    wire::history(&mut service, &f, 0, &snapshot);
    drop(service);
    let service = PeerService::open(&f.config).unwrap();
    assert_eq!(
        service.server.control_placement_group(),
        Some(PlacementGroupId(0))
    );
}

#[test]
fn native_control_existing_voter_uses_actual_bounded_owner_and_missing_manifest_is_not_initialized()
{
    let mut f = Fixture::new("adopt-voter");
    f.genesis_snapshot();
    rsf_storage::create_dir_all_synced(&f.state).unwrap();
    drop(StateIdentity::open(&f.state, &f.policy.anchor.cluster, NodeId(0), None).unwrap());
    let mut cell = DurableVsrCell::open(
        f.state.join("pg-0"),
        PlacementGroupId(0),
        Epoch(1),
        NodeId(0),
        (0..3).map(NodeId),
    )
    .unwrap();
    cell.catch_up_from(&[f.genesis_entry()]).unwrap();
    drop(cell);
    let mut cfg: serde_json::Value = serde_json::from_slice(&fs::read(&f.config).unwrap()).unwrap();
    cfg["node"] = 0.into();
    cfg["tls"] = f.root.0.join("tls-0").to_string_lossy().into_owned().into();
    cfg["control"]["provisioning"] = serde_json::json!({"kind":"existing_voter"});
    cfg["cells"]
        .as_array_mut()
        .unwrap()
        .push(serde_json::json!({"pg":1,"view":1,"primary":2,"voters":[0,1,2]}));
    fs::write(&f.config, serde_json::to_vec(&cfg).unwrap()).unwrap();
    let service = PeerService::open(&f.config).unwrap();
    assert_eq!(
        service.server.control_placement_group(),
        Some(PlacementGroupId(0))
    );
    drop(service);
    let native = fs::read(f.state.join("IDENTITY.json")).unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&native).unwrap()["reserved_control"]["custody"]
            ["kind"],
        "existing_voter"
    );
    fs::remove_file(f.state.join("pg-0/MANIFEST")).unwrap();
    assert!(PeerService::open(&f.config).is_err());
    assert!(!f.state.join("pg-0/MANIFEST").exists());
    assert_eq!(fs::read(f.state.join("IDENTITY.json")).unwrap(), native);
}
