use super::*;

#[test]
fn real_tls_native_changed_pending_selection_is_not_acknowledged_or_promoted_on_reopen() {
    for append in [false, true] {
        let mut f = Fixture::new(if append {
            "tls-pending-append"
        } else {
            "tls-pending-rewrite"
        });
        let snapshot = f.genesis_snapshot();
        let mut service = PeerService::open(&f.config).unwrap();
        let before = fs::read(f.state.join("IDENTITY.json")).unwrap();
        let observed = std::sync::Arc::new(std::sync::Mutex::new(None));
        let copy = std::sync::Arc::clone(&observed);
        let request = wire::initial(&f, 1, &snapshot.descriptor.encode().unwrap());
        wire::exchange_with(&mut service, &f, 37, request, move |at, root| {
            if at == Boundary::BeforeFileSync {
                let mut slot = copy.lock().unwrap();
                assert!(slot.is_none(), "fault must run exactly once");
                *slot = Some(state_identity::tests::mutate_prepared_identity(
                    root, append,
                ));
            }
            Ok(())
        })
        .rejected("native identity durability failed; reopen required");
        let changed = observed.lock().unwrap().clone().expect("fault must occur");
        assert_eq!(fs::read(f.state.join("IDENTITY.pending")).unwrap(), changed);
        assert_eq!(fs::read(f.state.join("IDENTITY.json")).unwrap(), before);
        assert_eq!(f.persisted(), ControlInitialState::Reserved);
        assert!(!f.control_root().join("ACTIVE").exists());
        assert_eq!(
            service.state_identity.validate().unwrap_err(),
            "native identity durability failed; reopen required"
        );
        wire::exchange(&mut service, &f, 37, wire::initial(&f, 0, &[]))
            .rejected("native identity durability failed; reopen required");
        drop(service);
        let mut cold = PeerService::open(&f.config).unwrap();
        assert_eq!(f.persisted(), ControlInitialState::Reserved);
        assert_eq!(fs::read(f.state.join("IDENTITY.json")).unwrap(), before);
        assert_eq!(fs::read(f.state.join("IDENTITY.pending")).unwrap(), changed);
        let (selected, count) = wire::exchange(
            &mut cold,
            &f,
            37,
            wire::initial(&f, 1, &snapshot.descriptor.encode().unwrap()),
        )
        .cursor();
        assert_eq!(count, 0);
        assert_eq!(selected.generation(), 1);
        assert_eq!(selected.history_id(), snapshot.descriptor.id().unwrap());
        assert_eq!(f.persisted(), ControlInitialState::Selected(selected));
        assert!(!f.state.join("IDENTITY.pending").exists());
        wire::copy_selected(&mut cold, &f, &snapshot, selected, count);
        assert_eq!(
            wire::exchange(
                &mut cold,
                &f,
                37,
                wire::initial(&f, 5, &wire::selection(selected)),
            )
            .success()[70],
            2
        );
        assert_eq!(
            f.persisted(),
            ControlInitialState::Published {
                selection: selected,
                source: NodeId(0)
            }
        );
    }
}

#[test]
fn real_tls_native_pin_and_final_publication_uncertainty_resume_without_source_retarget() {
    for pin in [true, false] {
        let mut f = Fixture::new(if pin {
            "pin-uncertain"
        } else {
            "published-uncertain"
        });
        let snapshot = f.genesis_snapshot();
        let mut service = PeerService::open(&f.config).unwrap();
        let selected = wire::copy(&mut service, &f, &snapshot);
        let response = wire::exchange_with(
            &mut service,
            &f,
            37,
            wire::initial(&f, 5, &wire::selection(selected)),
            move |at, root| {
                if at == Boundary::AfterRename {
                    let state = state_identity::tests::decode_initial(&root.join("IDENTITY.json"));
                    if matches!(state, ControlInitialState::Publishing { .. }) == pin {
                        return Err("injected pin/final restriction uncertainty".into());
                    }
                }
                Ok(())
            },
        );
        response.rejected("native identity durability failed; reopen required");
        if pin {
            assert_eq!(
                f.persisted(),
                ControlInitialState::Publishing {
                    selection: selected,
                    source: NodeId(0)
                }
            );
            assert!(!f.control_root().join("ACTIVE").exists());
        } else {
            assert_eq!(
                f.persisted(),
                ControlInitialState::Published {
                    selection: selected,
                    source: NodeId(0)
                }
            );
            assert_eq!(
                fs::read(f.control_root().join("ACTIVE")).unwrap(),
                selected.history_id()
            );
        }
        drop(service);
        let mut cold = PeerService::open(&f.config).unwrap();
        assert_eq!(
            wire::exchange(
                &mut cold,
                &f,
                37,
                wire::initial(&f, 5, &wire::selection(selected))
            )
            .success()[70],
            2
        );
        assert_eq!(
            f.persisted(),
            ControlInitialState::Published {
                selection: selected,
                source: NodeId(0)
            }
        );
    }
}

#[test]
fn real_tls_native_guard_brackets_no_transition_handlers_and_semantic_errors() {
    // Every case reaches a real successful handler (or the deliberate semantic
    // failure) before the second validation observes native file loss.
    for phase in 0..6 {
        let mut f = Fixture::new("response-guard");
        let snapshot = f.genesis_snapshot();
        let mut service = PeerService::open(&f.config).unwrap();
        let (selected, _) = wire::exchange(
            &mut service,
            &f,
            37,
            wire::initial(&f, 1, &snapshot.descriptor.encode().unwrap()),
        )
        .cursor();
        if phase >= 3 {
            wire::copy_selected(&mut service, &f, &snapshot, selected, 0);
            assert_eq!(
                wire::exchange(
                    &mut service,
                    &f,
                    37,
                    wire::initial(&f, 5, &wire::selection(selected))
                )
                .success()[70],
                2
            );
        }
        let (kind, request) = match phase {
            0 => (37, wire::initial(&f, 0, &[])),
            1 => {
                let mut p = wire::selection(selected);
                p.extend(snapshot.descriptor.encode().unwrap());
                (37, wire::initial(&f, 2, &p))
            }
            2 => {
                let mut p = wire::selection(selected);
                p.extend(
                    VsrHistoryBatch::pack(0, &snapshot.records)
                        .unwrap()
                        .encode()
                        .unwrap(),
                );
                (37, wire::initial(&f, 4, &p))
            }
            3 => (37, wire::initial(&f, 5, &wire::selection(selected))),
            4 => (6, snapshot.descriptor.encode().unwrap()), // later ordinary control HISTORY
            5 => (37, wire::initial(&f, 99, &[])), // invalid initial operation; physical failure dominates
            _ => unreachable!(),
        };
        let before = fs::read(f.state.join("IDENTITY.json")).unwrap();
        let saved = f.state.join("saved-identity");
        let mut calls = 0;
        wire::exchange_with(&mut service, &f, kind, request, move |at, root| {
            if at == Boundary::BeforeValidate {
                calls += 1;
                if calls == 2 {
                    fs::rename(root.join("IDENTITY.json"), root.join("saved-identity")).unwrap();
                }
            }
            Ok(())
        })
        .rejected("native identity original file custody changed");
        assert!(saved.exists(), "the post-handler fault must actually occur");
        assert!(!f.state.join("IDENTITY.json").exists());
        fs::rename(saved, f.state.join("IDENTITY.json")).unwrap();
        assert_eq!(fs::read(f.state.join("IDENTITY.json")).unwrap(), before);
        wire::exchange(&mut service, &f, 37, wire::initial(&f, 0, &[]))
            .rejected("native identity durability failed; reopen required");
        drop(service);
        let mut cold = PeerService::open(&f.config).unwrap();
        wire::exchange(&mut cold, &f, 37, wire::initial(&f, 0, &[])).success();
    }
}

#[test]
fn native_control_selected_and_pinned_missing_candidates_never_reenter_factory() {
    for pinned in [false, true] {
        let mut f = Fixture::new("candidate-loss");
        let snapshot = f.genesis_snapshot();
        let mut service = PeerService::open(&f.config).unwrap();
        let selected = wire::copy(&mut service, &f, &snapshot);
        if pinned {
            wire::exchange_with(
                &mut service,
                &f,
                37,
                wire::initial(&f, 5, &wire::selection(selected)),
                |at, root| {
                    if at == Boundary::AfterRename
                        && matches!(
                            state_identity::tests::decode_initial(&root.join("IDENTITY.json")),
                            ControlInitialState::Publishing { .. }
                        )
                    {
                        return Err("injected after durable pin".into());
                    }
                    Ok(())
                },
            )
            .rejected("native identity durability failed; reopen required");
        }
        drop(service);
        let before = fs::read(f.state.join("IDENTITY.json")).unwrap();
        let candidate = f.control_root().join(
            selected
                .history_id()
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>(),
        );
        assert!(candidate.is_dir(), "must remove actual bound candidate");
        fs::remove_dir_all(&candidate).unwrap();
        assert!(PeerService::open(&f.config).is_err());
        assert!(!candidate.exists());
        assert_eq!(fs::read(f.state.join("IDENTITY.json")).unwrap(), before);
    }
}

#[test]
fn native_control_v3_roundtrip_preserves_managed_enrollment_and_refuses_aba_selection() {
    let mut f = Fixture::new("aba");
    let first = f.genesis_snapshot();
    // A second genuine complete history differs by ordinary accepted state;
    // it is valid transfer input but intentionally not a Genesis-only cut.
    let mut source = DurableVsrCell::open(
        f.root.0.join("alternative"),
        PlacementGroupId(0),
        Epoch(1),
        NodeId(0),
        (0..3).map(NodeId),
    )
    .unwrap();
    source.catch_up_from(&[f.genesis_entry()]).unwrap();
    let payload = ControlCommand::Genesis(f.policy.genesis.clone())
        .encode()
        .unwrap();
    assert!(matches!(
        source
            .prepare(
                NodeId(0),
                VsrOperation::ReplicatedCommand {
                    operation_id: OperationId(2),
                    payload: payload.clone()
                },
                payload.len() as u32
            )
            .unwrap(),
        VsrDecision::Prepared(_)
    ));
    let second = source.history_snapshot().unwrap();
    assert_ne!(
        first.descriptor.id().unwrap(),
        second.descriptor.id().unwrap()
    );
    let mut service = PeerService::open(&f.config).unwrap();
    let (a, _) = wire::exchange(
        &mut service,
        &f,
        37,
        wire::initial(&f, 1, &first.descriptor.encode().unwrap()),
    )
    .cursor();
    let replace = |selection, descriptor: &rsf_vsr::VsrHistoryDescriptor| {
        let mut p = wire::selection(selection);
        p.extend(descriptor.encode().unwrap());
        wire::initial(&f, 3, &p)
    };
    let (b, _) = wire::exchange(&mut service, &f, 37, replace(a, &second.descriptor)).cursor();
    assert_eq!(b.generation(), 2);
    wire::copy_selected(&mut service, &f, &second, b, 0);
    wire::exchange(
        &mut service,
        &f,
        37,
        wire::initial(&f, 5, &wire::selection(b)),
    )
    .rejected("initial candidate is not an exact certified original Genesis cut");
    service.state_identity.validate().unwrap();
    assert_eq!(f.persisted(), ControlInitialState::Selected(b));
    let (again, _) = wire::exchange(&mut service, &f, 37, replace(b, &first.descriptor)).cursor();
    assert_eq!(again.generation(), 3);
    assert_eq!(again.history_id(), a.history_id());
    let before = fs::read(f.state.join("IDENTITY.json")).unwrap();
    let reply = wire::exchange(&mut service, &f, 37, replace(a, &second.descriptor)).success();
    assert_eq!(reply[70], 4, "old A generation must be typed conflict");
    assert_eq!(fs::read(f.state.join("IDENTITY.json")).unwrap(), before);
    wire::copy_selected(&mut service, &f, &first, again, 0);
    assert_eq!(
        wire::exchange(
            &mut service,
            &f,
            37,
            wire::initial(&f, 5, &wire::selection(again))
        )
        .success()[70],
        2
    );
    // Enrollment serialization preserves the full existing control restriction.
    // This direct private owner assertion is a schema contract, not an enrolled
    // RPC recovery authority or permission to start this configuration.
    let published = f.persisted();
    service
        .state_identity
        .enroll(&[PlacementGroupId(9)].into())
        .unwrap();
    assert_eq!(f.persisted(), published);
    let stored = fs::read(f.state.join("IDENTITY.json")).unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&stored).unwrap()["managed_membership"],
        serde_json::json!([9])
    );
}

#[test]
fn real_tls_native_published_origin_survives_later_control_history_and_cold_reopen() {
    let mut f = Fixture::new("later-control");
    let initial = f.genesis_snapshot();
    let mut service = PeerService::open(&f.config).unwrap();
    let selection = wire::copy(&mut service, &f, &initial);
    assert_eq!(
        wire::exchange(
            &mut service,
            &f,
            37,
            wire::initial(&f, 5, &wire::selection(selection))
        )
        .success()[70],
        2
    );
    let identity = fs::read(f.state.join("IDENTITY.json")).unwrap();
    let later = f.append_intent();
    assert_eq!(later.descriptor.probe.committed_tail, LogIndex(2));
    assert_ne!(later.descriptor.id().unwrap(), selection.history_id());
    wire::history(&mut service, &f, 0, &later);
    assert_eq!(
        fs::read(f.control_root().join("ACTIVE")).unwrap(),
        later.descriptor.id().unwrap()
    );
    assert_eq!(fs::read(f.state.join("IDENTITY.json")).unwrap(), identity);
    drop(service);
    let mut cold = PeerService::open(&f.config).unwrap();
    let reply = wire::exchange(
        &mut cold,
        &f,
        37,
        wire::initial(&f, 5, &wire::selection(selection)),
    )
    .success();
    assert_eq!(
        reply[70], 3,
        "old origin must observe current initialized history, never rewind"
    );
    assert_eq!(&reply[111..143], later.descriptor.id().unwrap());
    assert_eq!(u64::from_le_bytes(reply[143..151].try_into().unwrap()), 2);
    assert_eq!(fs::read(f.state.join("IDENTITY.json")).unwrap(), identity);
    assert!(
        cold.server
            .committed_application_entries()
            .unwrap()
            .is_empty()
    );
}

#[test]
fn real_tls_native_mixed_application_history_remains_separate_across_control_handoff() {
    let mut f = Fixture::new("mixed-data");
    let initial = f.genesis_snapshot();
    let mut config: serde_json::Value =
        serde_json::from_slice(&fs::read(&f.config).unwrap()).unwrap();
    config["cells"]
        .as_array_mut()
        .unwrap()
        .push(serde_json::json!({"pg":1,"view":1,"primary":2,"voters":[0,1,2],"learner_source":2}));
    fs::write(&f.config, serde_json::to_vec(&config).unwrap()).unwrap();
    let first = f.application_snapshot(1);
    let mut service = PeerService::open(&f.config).unwrap();
    wire::history(&mut service, &f, 2, &first);
    let entries = service.server.committed_application_entries().unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].prepare.pg, PlacementGroupId(1));
    assert_eq!(entries[0].prepare.primary, NodeId(2));
    let selection = wire::copy(&mut service, &f, &initial);
    assert_eq!(
        wire::exchange(
            &mut service,
            &f,
            37,
            wire::initial(&f, 5, &wire::selection(selection))
        )
        .success()[70],
        2
    );
    let second = f.application_snapshot(2);
    wire::history(&mut service, &f, 2, &second);
    let entries = service.server.committed_application_entries().unwrap();
    assert_eq!(entries.len(), 2);
    for (i, e) in entries.iter().enumerate() {
        assert_eq!(e.prepare.pg, PlacementGroupId(1));
        assert_eq!(e.prepare.index, LogIndex(i as u64 + 1));
        assert!(
            matches!(&e.prepare.operation,VsrOperation::ReplicatedCommand{payload,..} if payload==format!("actual-data-{}",i+1).as_bytes())
        );
    }
    drop(service);
    let mut cold = PeerService::open(&f.config).unwrap();
    assert_eq!(
        cold.server.committed_application_entries().unwrap(),
        entries
    );
    assert_eq!(
        f.persisted(),
        ControlInitialState::Published {
            selection,
            source: NodeId(0)
        }
    );
}
