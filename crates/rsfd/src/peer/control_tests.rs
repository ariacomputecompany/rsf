//! Actual native identity writer + bounded receiver over authenticated TLS.
use super::*;
use rsf_control_state::*;
use rsf_core::{LogIndex, OperationId};
use rsf_rpc::replication::{ControlInitialPolicy, ControlInitialState};
use rsf_vsr::{
    VsrDecision, VsrHistoryBatch, VsrHistorySnapshot, VsrInitialSelection, VsrOperation,
};
use state_identity::tests::{Fixture as Root, with_hook};
use state_identity::{Boundary, StateIdentity};
use std::fs;
mod adoption;
mod fixture;
mod recovery;
mod wire;
use fixture::Fixture;

#[test]
fn native_control_invalid_provisioning_rejects_before_identity_or_reserved_root() {
    let f = Fixture::new("preflight");
    let original = fs::read(&f.config).unwrap();
    for mutation in [0, 1, 2, 3] {
        let mut config: serde_json::Value = serde_json::from_slice(&original).unwrap();
        match mutation {
            0 => config["control"]["prefix_limits"]["max_records"] = u64::MAX.into(),
            1 => config["control"]["prefix_limits"]["max_encoded_bytes"] = 17.into(),
            2 => config["peers"][3]["fingerprint"] = "00".repeat(32).into(),
            3 => config["cells"][0]["learner_source"] = 0.into(),
            _ => unreachable!(),
        }
        fs::write(&f.config, serde_json::to_vec(&config).unwrap()).unwrap();
        assert!(PeerService::open(&f.config).is_err());
        assert!(
            !f.state.exists(),
            "reservation occurred before complete pure validation"
        );
    }
    fs::write(&f.config, &original).unwrap();
    let mut bad = f.policy.genesis.clone();
    bad.groups.pop();
    // Canonical decoder refuses the complete-catalog invariant before opening
    // the native state; corrupting the real provisioning bytes is intentional.
    let genesis = f.root.0.join("genesis");
    let good = fs::read(&genesis).unwrap();
    let mut malformed = good.clone();
    malformed[0] ^= 1;
    fs::write(&genesis, malformed).unwrap();
    assert!(PeerService::open(&f.config).is_err());
    assert!(!f.state.exists());
    assert!(bad.root().is_err());
    fs::write(genesis, good).unwrap();
    let mut bad_policy = f.policy.clone();
    bad_policy.genesis = bad;
    let loaded = load_config(&f.config).unwrap();
    assert!(
        bad_policy
            .validate_configuration(
                &loaded.cluster,
                NodeId(3),
                &loaded.identity,
                &loaded.peers,
                Duration::from_millis(800)
            )
            .is_err()
    );
    assert!(!f.state.exists());
    let service = PeerService::open(&f.config).unwrap();
    assert_eq!(
        service.server.control_placement_group(),
        Some(f.policy.anchor.control_pg)
    );
    assert!(f.state.join("IDENTITY.json").is_file());
}

#[test]
fn real_tls_native_initial_selection_pin_and_publication_follow_real_identity_barriers() {
    let mut f = Fixture::new("complete");
    let snapshot = f.genesis_snapshot();
    let mut service = PeerService::open(&f.config).unwrap();
    let selection = wire::copy(&mut service, &f, &snapshot);
    assert_eq!(f.persisted(), ControlInitialState::Selected(selection));
    assert!(!f.control_root().join("ACTIVE").exists());
    let path = f.control_root();
    let selected = selection;
    let events = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let observed = std::sync::Arc::clone(&events);
    let reply = wire::exchange_with(
        &mut service,
        &f,
        37,
        wire::initial(&f, 5, &wire::selection(selection)),
        move |at, root| {
            if at == Boundary::BeforeRename {
                let next = state_identity::tests::decode_initial(&root.join("IDENTITY.pending"));
                match next {
                    ControlInitialState::Publishing { selection, source } => {
                        assert_eq!(selection, selected);
                        assert_eq!(source, NodeId(0));
                        assert!(!path.join("ACTIVE").exists());
                    }
                    ControlInitialState::Published { selection, source } => {
                        assert_eq!(selection, selected);
                        assert_eq!(source, NodeId(0));
                        assert_eq!(
                            fs::read(path.join("ACTIVE")).unwrap(),
                            selected.history_id()
                        );
                    }
                    other => panic!("unexpected persistence phase {other:?}"),
                }
                let mut events = observed.lock().unwrap();
                events.push(next);
                assert!(events.len() <= 2);
            }
            Ok(())
        },
    );
    let reply = reply.success();
    assert_eq!(
        events.lock().unwrap().as_slice(),
        &[
            ControlInitialState::Publishing {
                selection,
                source: NodeId(0),
            },
            ControlInitialState::Published {
                selection,
                source: NodeId(0),
            },
        ]
    );
    assert_eq!(reply[70], 2);
    assert_eq!(
        f.persisted(),
        ControlInitialState::Published {
            selection,
            source: NodeId(0)
        }
    );
    assert!(
        service
            .server
            .committed_application_entries()
            .unwrap()
            .is_empty()
    );
    let final_bytes = fs::read(f.state.join("IDENTITY.json")).unwrap();
    drop(service);
    let mut cold = PeerService::open(&f.config).unwrap();
    assert_eq!(
        wire::exchange(
            &mut cold,
            &f,
            37,
            wire::initial(&f, 5, &wire::selection(selection))
        )
        .success()[70],
        2
    );
    assert_eq!(
        fs::read(f.state.join("IDENTITY.json")).unwrap(),
        final_bytes,
        "published retry must not create a fake native transition"
    );
}

#[test]
fn real_tls_native_selection_write_uncertainty_is_not_acknowledged_and_cold_resume_is_exact() {
    let mut f = Fixture::new("selected-uncertain");
    let snapshot = f.genesis_snapshot();
    let mut service = PeerService::open(&f.config).unwrap();
    let request = wire::initial(&f, 1, &snapshot.descriptor.encode().unwrap());
    let response = wire::exchange_with(&mut service, &f, 37, request, |at, _| {
        if at == Boundary::AfterRename {
            Err("injected selected publication uncertainty".into())
        } else {
            Ok(())
        }
    });
    response.rejected("native identity durability failed; reopen required");
    let ControlInitialState::Selected(selection) = f.persisted() else {
        panic!("renamed Selected must be actual cold state")
    };
    assert_eq!(selection.history_id(), snapshot.descriptor.id().unwrap());
    assert!(!f.control_root().join("ACTIVE").exists());
    drop(service);
    let mut cold = PeerService::open(&f.config).unwrap();
    let mut payload = wire::selection(selection);
    payload.extend(snapshot.descriptor.encode().unwrap());
    let cursor = wire::exchange(&mut cold, &f, 37, wire::initial(&f, 2, &payload)).cursor();
    assert_eq!(cursor, (selection, 0));
    wire::copy_selected(&mut cold, &f, &snapshot, selection, 0);
    assert_eq!(
        wire::exchange(
            &mut cold,
            &f,
            37,
            wire::initial(&f, 5, &wire::selection(selection))
        )
        .success()[70],
        2
    );
}

#[test]
fn native_control_reservation_blocks_config_omission_raw_sync_and_legacy_binding() {
    let f = Fixture::new("omission");
    drop(PeerService::open(&f.config).unwrap());
    let before = fs::read(f.state.join("IDENTITY.json")).unwrap();
    assert!(
        bind_state(&f.state, &f.policy.anchor.cluster, NodeId(3))
            .unwrap_err()
            .contains("cannot be omitted")
    );
    let raw = fs::read(&f.config).unwrap();
    let mut value: serde_json::Value = serde_json::from_slice(&raw).unwrap();
    value.as_object_mut().unwrap().remove("control");
    value["cells"][0]["learner_source"] = 0.into();
    fs::write(&f.config, serde_json::to_vec(&value).unwrap()).unwrap();
    let error = match PeerService::open(&f.config) {
        Err(e) => e,
        Ok(_) => panic!("omitted restriction opened raw learner"),
    };
    assert!(error.contains("cannot be omitted"), "{error}");
    fs::write(&f.config, raw).unwrap();
    let args = vec![
        "sync-learner".into(),
        "--config".into(),
        f.config.to_string_lossy().into_owned(),
        "--pg".into(),
        "0".into(),
        "--target".into(),
        "1".into(),
    ];
    assert!(
        sync_learner_cli(&args)
            .unwrap_err()
            .contains("reserved control cannot use raw")
    );
    assert_eq!(fs::read(f.state.join("IDENTITY.json")).unwrap(), before);
}

#[test]
fn native_control_v3_schema_and_explicit_legacy_migration_preserve_exact_policy() {
    let f = Fixture::new("migration");
    rsf_storage::create_dir_all_synced(&f.state).unwrap();
    let mut legacy =
        StateIdentity::open(&f.state, &f.policy.anchor.cluster, NodeId(3), None).unwrap();
    legacy.enroll(&[PlacementGroupId(9)].into()).unwrap();
    drop(legacy);
    let loaded = load_config(&f.config).unwrap();
    let trusted = loaded.control.unwrap();
    let mut identity = StateIdentity::open(
        &f.state,
        &f.policy.anchor.cluster,
        NodeId(3),
        Some(trusted.reservation().unwrap()),
    )
    .unwrap();
    assert_eq!(identity.managed(), &[PlacementGroupId(9)].into());
    assert_eq!(
        identity.initial_binding(&f.policy).unwrap().state,
        ControlInitialState::Reserved
    );
    let raw = fs::read(f.state.join("IDENTITY.json")).unwrap();
    let canonical: serde_json::Value = serde_json::from_slice(&raw).unwrap();
    assert_eq!(canonical["version"], 3);
    assert_eq!(canonical["managed_membership"], serde_json::json!([9]));
    drop(identity);
    for change in 0..5 {
        let mut bad = canonical.clone();
        match change {
            0 => bad["reserved_control"]["bootstrap"] = "AB".repeat(32).into(),
            1 => bad["reserved_control"]["max_records"] = u64::MAX.into(),
            2 => {
                bad["reserved_control"]["custody"] = serde_json::json!({"kind":"provisional_learner","generation":0,"history_id":"11".repeat(32)})
            }
            3 => bad["reserved_control"]["custody"]["source"] = 0.into(),
            4 => bad["reserved_control"] = serde_json::Value::Null,
            _ => unreachable!(),
        }
        fs::write(
            f.state.join("IDENTITY.json"),
            serde_json::to_vec(&bad).unwrap(),
        )
        .unwrap();
        assert!(
            StateIdentity::open(
                &f.state,
                &f.policy.anchor.cluster,
                NodeId(3),
                Some(trusted.reservation().unwrap())
            )
            .is_err()
        );
    }
    fs::write(f.state.join("IDENTITY.json"), &raw).unwrap();
    let mut reopened = StateIdentity::open(
        &f.state,
        &f.policy.anchor.cluster,
        NodeId(3),
        Some(trusted.reservation().unwrap()),
    )
    .unwrap();
    reopened.validate().unwrap();
    assert_eq!(fs::read(f.state.join("IDENTITY.json")).unwrap(), raw);
    let mut other = f.policy.clone();
    other.limits.max_records += 1;
    assert!(
        reopened
            .initial_binding(&other)
            .unwrap_err()
            .contains("differs")
    );
    reopened.validate().unwrap();
}

mod bootstrap;
mod bootstrap_faults;
mod bootstrap_process;
