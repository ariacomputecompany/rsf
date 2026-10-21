use super::*;
use rsf_vsr::{VsrViewChangeRequest, VsrViewInstallation};
use std::fs;

fn root(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "rsf-handoff-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}
fn command(index: u64) -> ReplicatedFabricCommand {
    let operation = FabricOperation::KvMutation {
        key: format!("key-{index}").into_bytes(),
    };
    ReplicatedFabricCommand {
        commit: FabricCommit {
            pg: PlacementGroupId(1),
            index: LogIndex(index),
            version: CommitVersion(index),
            mode: ConsistencyMode::Strict,
            operation_id: operation.operation_id(),
            operation,
        },
        request: ClientRequestId::new("handoff", index),
        application: ApplicationPayload {
            namespace: "tenant/db".into(),
            surface: ApplicationSurface::Kv,
            bytes: format!("value-{index}").into_bytes(),
        },
    }
}
fn voters() -> Vec<NodeId> {
    vec![NodeId(0), NodeId(1), NodeId(2)]
}
fn installed(
    path: &Path,
    committed: u64,
    pending: bool,
) -> BTreeMap<PlacementGroupId, DurableVsrCell> {
    let mut source = DurableVsrCell::open(
        path.join("source"),
        PlacementGroupId(1),
        Epoch(1),
        NodeId(0),
        voters(),
    )
    .unwrap();
    let mut candidate = DurableVsrCell::open(
        path.join("candidate"),
        PlacementGroupId(1),
        Epoch(1),
        NodeId(0),
        voters(),
    )
    .unwrap();
    for index in 1..=committed + u64::from(pending) {
        let command = command(index);
        let VsrDecision::Prepared(prepare) = source
            .prepare(
                NodeId(0),
                command.vsr_operation().unwrap(),
                command.encode().unwrap().len() as u32,
            )
            .unwrap()
        else {
            panic!("prepare")
        };
        candidate
            .receive_prepare(NodeId(1), NodeId(0), prepare.clone())
            .unwrap();
        if index <= committed {
            source.acknowledge(NodeId(1), Epoch(1), prepare.index);
            let VsrDecision::Committed(commit) = source.commit(NodeId(0), prepare.index).unwrap()
            else {
                panic!("commit")
            };
            candidate
                .receive_commit(NodeId(1), NodeId(0), commit)
                .unwrap();
        }
    }
    let request = VsrViewChangeRequest {
        pg: PlacementGroupId(1),
        previous_view: Epoch(1),
        target_view: Epoch(2),
        candidate: NodeId(1),
        voters: voters(),
    };
    let snapshots = vec![
        source
            .begin_view_change(NodeId(0), NodeId(1), request.clone())
            .unwrap(),
        candidate
            .begin_view_change(NodeId(1), NodeId(1), request)
            .unwrap(),
    ];
    candidate
        .install_view(&VsrViewInstallation::select(&snapshots).unwrap())
        .unwrap();
    [(PlacementGroupId(1), candidate)].into()
}
fn adopt(
    path: &Path,
    cells: BTreeMap<PlacementGroupId, DurableVsrCell>,
) -> RsfResult<VsrBackedFabricRouter> {
    VsrBackedFabricRouter::open_with_installed_cells(
        path,
        Epoch(1),
        NodeId(1),
        cells,
        LocalVsrTransport::all_reachable(voters()),
    )
}

#[test]
fn installed_commands_rebuild_receipts_finish_pending_and_continue_without_version_reuse() {
    let root = root("recover");
    let cells = installed(&root, 2, true);
    let path = root.join("protocol");
    let mut router = adopt(&path, cells).unwrap();
    assert_eq!(
        router.membership(PlacementGroupId(1)).unwrap(),
        (Epoch(2), NodeId(1), voters())
    );
    assert_eq!(router.latest_version(), CommitVersion(0));
    assert_eq!(router.reconcile_application_commands().unwrap(), 3);
    assert_eq!(router.latest_version(), CommitVersion(3));
    let commands = router.committed_application_commands();
    assert_eq!(commands, (1..=3).map(command).collect::<Vec<_>>());
    let original = command(1);
    let retry = router
        .commit_application(
            original.commit.pg,
            original.request,
            original.commit.mode,
            original.commit.operation,
            original.application,
        )
        .unwrap();
    assert!(retry.replayed);
    assert_eq!(retry.certificate.version, CommitVersion(1));
    let next = command(4);
    let result = router
        .commit_application(
            next.commit.pg,
            next.request,
            next.commit.mode,
            next.commit.operation,
            next.application,
        )
        .unwrap();
    assert_eq!(result.certificate.index, LogIndex(4));
    assert_eq!(result.certificate.version, CommitVersion(4));
    let cells = router.into_cells();
    assert!(DurableVsrCell::open_existing(root.join("candidate"), PlacementGroupId(1)).is_err());
    let mut router = adopt(&path, cells).unwrap();
    assert_eq!(router.reconcile_application_commands().unwrap(), 0);
    assert_eq!(router.latest_version(), CommitVersion(4));
    drop(router);
    let cells = [(
        PlacementGroupId(1),
        DurableVsrCell::open_existing(root.join("candidate"), PlacementGroupId(1)).unwrap(),
    )]
    .into();
    let mut router = adopt(&path, cells).unwrap();
    assert_eq!(router.reconcile_application_commands().unwrap(), 0);
    assert!(router.confirm_read_quorum().is_err()); // The local model cannot authenticate live primary probes.
    assert_eq!(router.committed_application_commands().len(), 4);
    assert!(!path.with_extension("vsr").exists());
    drop(router);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn installed_cell_handoff_rejects_wrong_primary_keys_promises_and_missing_placement() {
    for case in 0..4 {
        let root = root(&format!("reject-{case}"));
        let mut cells = installed(&root, 1, false);
        let local = if case == 0 { NodeId(0) } else { NodeId(1) };
        if case == 1 {
            let cell = cells.remove(&PlacementGroupId(1)).unwrap();
            cells.insert(PlacementGroupId(9), cell);
        }
        if case == 2 {
            cells
                .get_mut(&PlacementGroupId(1))
                .unwrap()
                .begin_view_change(
                    NodeId(1),
                    NodeId(2),
                    VsrViewChangeRequest {
                        pg: PlacementGroupId(1),
                        previous_view: Epoch(2),
                        target_view: Epoch(3),
                        candidate: NodeId(2),
                        voters: voters(),
                    },
                )
                .unwrap();
        }
        let path = root.join("protocol");
        if case == 3 {
            append_placement_map(
                &path,
                Epoch(1),
                &PlacementMap::new(1, [PlacementGroupId(1), PlacementGroupId(2)]).unwrap(),
            )
            .unwrap();
        }
        assert!(
            VsrBackedFabricRouter::open_with_installed_cells(
                &path,
                Epoch(1),
                local,
                cells,
                LocalVsrTransport::all_reachable(voters())
            )
            .is_err()
        );
        assert!(!path.with_extension("vsr").exists());
        fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn adoption_rejects_unrelated_receipts_and_cannot_bootstrap_an_expanded_group() {
    let root = root("foreign-receipt");
    let cells = installed(&root, 1, false);
    let path = root.join("protocol");
    let mut unrelated = DurableFabricRouter::open(&path, [PlacementGroupId(1)], Epoch(1)).unwrap();
    unrelated
        .commit_request(
            PlacementGroupId(1),
            ClientRequestId::new("foreign", 1),
            ConsistencyMode::Strict,
            FabricOperation::KvMutation {
                key: b"other".to_vec(),
            },
        )
        .unwrap();
    drop(unrelated);
    assert!(adopt(&path, cells).is_err());
    let cells = [(
        PlacementGroupId(1),
        DurableVsrCell::open_existing(root.join("candidate"), PlacementGroupId(1)).unwrap(),
    )]
    .into();
    let clean = root.join("clean-protocol");
    let mut router = adopt(&clean, cells).unwrap();
    let before = router.placement_map().clone();
    assert!(
        router
            .update_placement_map([PlacementGroupId(1), PlacementGroupId(2)])
            .is_err()
    );
    assert_eq!(router.placement_map(), &before);
    assert_eq!(recover_placement_map(&clean).unwrap().unwrap(), before);
    assert!(!clean.with_extension("vsr").exists());
    drop(router);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn metadata_receipt_failure_fences_the_router_and_retry_after_reopen_keeps_its_original_version() {
    let root = root("metadata-receipt-failure");
    let cells = installed(&root, 1, false);
    let path = root.join("protocol");
    let mut router = adopt(&path, cells).unwrap();
    router.reconcile_application_commands().unwrap();
    let displaced = root.join("displaced-protocol");
    fs::rename(&path, &displaced).unwrap();
    fs::write(&path, b"unavailable directory").unwrap();
    let operation = FabricOperation::CatalogMutation {
        namespace: "app".into(),
        item: "new-table".into(),
    };
    assert!(
        router
            .commit(PlacementGroupId(1), operation.clone())
            .is_err()
    );
    fs::remove_file(&path).unwrap();
    fs::rename(displaced, &path).unwrap();
    assert!(
        router
            .commit(PlacementGroupId(1), operation.clone())
            .is_err()
    );
    let original = command(1);
    assert!(
        router
            .commit_application(
                original.commit.pg,
                original.request,
                original.commit.mode,
                original.commit.operation,
                original.application
            )
            .is_err()
    );
    let cells = router.into_cells();
    let mut router = adopt(&path, cells).unwrap();
    let receipt = router.commit(PlacementGroupId(1), operation).unwrap();
    assert_eq!(receipt.index, LogIndex(2));
    assert_eq!(receipt.version, CommitVersion(2));
    assert_eq!(router.latest_version(), CommitVersion(2));
    assert_eq!(
        router
            .recovered_certificates(PlacementGroupId(1))
            .unwrap()
            .len(),
        2
    );
    let cells = router.into_cells();
    let mut router = adopt(&path, cells).unwrap();
    assert_eq!(router.reconcile_application_commands().unwrap(), 0);
    assert_eq!(router.latest_version(), CommitVersion(2));
    drop(router);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn a_pending_metadata_request_cannot_be_reused_for_an_application_command() {
    for preview in [false, true] {
        let root = root(if preview {
            "preview-identity"
        } else {
            "commit-identity"
        });
        let cells = installed(&root, 1, false);
        let path = root.join("protocol");
        let mut router = adopt(&path, cells).unwrap();
        router.reconcile_application_commands().unwrap();
        let request = ClientRequestId::new("metadata-request", 1);
        router
            .commit_vsr_request_without_protocol_certificate_for_test(
                PlacementGroupId(1),
                &request,
                ConsistencyMode::Strict,
                FabricOperation::CatalogMutation {
                    namespace: "app".into(),
                    item: "table".into(),
                },
            )
            .unwrap();
        let application = command(3);
        if preview {
            assert!(
                router
                    .preview_application(
                        PlacementGroupId(1),
                        &request,
                        ConsistencyMode::Strict,
                        &application.commit.operation,
                        &application.application
                    )
                    .is_err()
            );
        } else {
            assert!(
                router
                    .commit_application(
                        PlacementGroupId(1),
                        request,
                        ConsistencyMode::Strict,
                        application.commit.operation,
                        application.application
                    )
                    .is_err()
            );
        }
        assert_eq!(router.latest_version(), CommitVersion(2));
        assert_eq!(router.committed_application_commands().len(), 1);
        assert_eq!(router.cells[&PlacementGroupId(1)].next_index(), LogIndex(3));
        let cells = router.into_cells();
        let mut router = adopt(&path, cells).unwrap();
        assert_eq!(router.reconcile_application_commands().unwrap(), 0);
        drop(router);
        fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn installed_surface_operations_without_payloads_require_migration_before_adoption() {
    for (case, operation) in [
        FabricOperation::KvMutation {
            key: b"missing-value".to_vec(),
        },
        FabricOperation::RelationalMutation {
            table: "users".into(),
            key: b"missing-row".to_vec(),
        },
        FabricOperation::ObjectManifest {
            bucket: "media".into(),
            key: "missing-object".into(),
        },
        FabricOperation::BlobPin { digest: [7; 32] },
    ]
    .into_iter()
    .enumerate()
    {
        let root = root(&format!("missing-surface-{case}"));
        let mut cells = installed(&root, 1, false);
        let command = ReplicatedProtocolCommand {
            purpose: ProtocolCommandPurpose::Operation,
            commit: FabricCommit {
                pg: PlacementGroupId(1),
                index: LogIndex(2),
                version: CommitVersion(2),
                mode: ConsistencyMode::Strict,
                operation_id: operation.operation_id(),
                operation: operation.clone(),
            },
            request: None,
        };
        let cell = cells.get_mut(&PlacementGroupId(1)).unwrap();
        let VsrDecision::Prepared(prepare) = cell
            .prepare(
                NodeId(1),
                command.vsr_operation().unwrap(),
                command.encode().unwrap().len() as u32,
            )
            .unwrap()
        else {
            panic!("prepare")
        };
        cell.replicate_with_transport(
            NodeId(1),
            prepare.index,
            &mut LocalVsrTransport::all_reachable(voters()),
        )
        .unwrap();
        let path = root.join("protocol");
        assert!(
            adopt(&path, cells).is_err(),
            "coordination receipts cannot substitute for surface values"
        );
        assert!(
            !path.exists(),
            "reject incomplete surface history before creating a protocol store"
        );
        fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn an_adopted_coordinator_cannot_acknowledge_a_surface_mutation_without_its_payload() {
    let root = root("reject-missing-payload");
    let cells = installed(&root, 1, false);
    let path = root.join("protocol");
    let mut router = adopt(&path, cells).unwrap();
    router.reconcile_application_commands().unwrap();
    let operation = FabricOperation::KvMutation {
        key: b"no-payload".to_vec(),
    };
    assert!(
        router
            .commit(PlacementGroupId(1), operation.clone())
            .is_err()
    );
    assert!(
        router
            .commit_request(
                PlacementGroupId(1),
                ClientRequestId::new("no-payload", 1),
                ConsistencyMode::Strict,
                operation
            )
            .is_err()
    );
    assert_eq!(router.latest_version(), CommitVersion(1));
    assert_eq!(router.cells[&PlacementGroupId(1)].next_index(), LogIndex(2));
    assert!(router.protocol_commands.is_empty());
    let command = command(2);
    let result = router
        .commit_application(
            command.commit.pg,
            command.request,
            command.commit.mode,
            command.commit.operation,
            command.application,
        )
        .unwrap();
    assert_eq!(result.certificate.version, CommitVersion(2));
    drop(router);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn orphaned_internal_pin_recovers_as_coordination_without_becoming_a_user_blob_receipt() {
    let root = root("orphaned-pin");
    let cells = installed(&root, 1, false);
    let path = root.join("protocol");
    let mut router = adopt(&path, cells).unwrap();
    router.reconcile_application_commands().unwrap();
    let request = ClientRequestId::new("object-internal-pin", 1);
    let pin = FabricRouterPort::commit_immutable_dependency(
        &mut router,
        PlacementGroupId(1),
        request.clone(),
        [7; 32],
    )
    .unwrap();
    assert_eq!(pin.certificate.version, CommitVersion(2));
    let cells = router.into_cells();
    let mut router = adopt(&path, cells).unwrap();
    assert_eq!(router.reconcile_application_commands().unwrap(), 0);
    // No containing object command was replicated and no application value is
    // invented during recovery. The pin is only a retryable internal dependency.
    assert_eq!(router.committed_application_commands().len(), 1);
    assert!(
        router
            .commit_request_routed(
                request.clone(),
                ConsistencyMode::AvailabilityFirst,
                FabricOperation::BlobPin { digest: [7; 32] }
            )
            .is_err()
    );
    assert!(
        router
            .validate_immutable_dependency(PlacementGroupId(1), &request, [8; 32])
            .is_err()
    );
    assert!(
        router
            .commit_request(
                PlacementGroupId(1),
                request.clone(),
                ConsistencyMode::AvailabilityFirst,
                FabricOperation::BlobPin { digest: [7; 32] }
            )
            .is_err()
    );
    assert!(
        FabricRouterPort::commit_immutable_dependency(
            &mut router,
            PlacementGroupId(1),
            request.clone(),
            [8; 32]
        )
        .is_err()
    );
    let retry = FabricRouterPort::commit_immutable_dependency(
        &mut router,
        PlacementGroupId(1),
        request,
        [7; 32],
    )
    .unwrap();
    assert!(retry.replayed);
    assert_eq!(retry.certificate, pin.certificate);
    assert_eq!(router.latest_version(), CommitVersion(2));
    assert_eq!(router.cells[&PlacementGroupId(1)].next_index(), LogIndex(3));
    drop(router);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn scoped_custody_restores_live_cells_after_errors_unwind_and_recovery_rejection() {
    let root = root("scoped-custody");
    let mut cells = installed(&root, 2, false);
    let path = root.join("protocol");
    let error: RsfResult<()> = VsrBackedFabricRouter::with_installed_cells(
        &path,
        Epoch(1),
        NodeId(1),
        &mut cells,
        LocalVsrTransport::all_reachable(voters()),
        |router| {
            router.reconcile_application_commands()?;
            assert_eq!(router.latest_version(), CommitVersion(2));
            Err(handoff_invalid("client failed after reconciliation"))
        },
    );
    assert_eq!(
        error.unwrap_err().message,
        "client failed after reconciliation"
    );
    assert_eq!(cells[&PlacementGroupId(1)].committed_tail(), LogIndex(2));
    assert!(DurableVsrCell::open_existing(root.join("candidate"), PlacementGroupId(1)).is_err());
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _: RsfResult<()> = VsrBackedFabricRouter::with_installed_cells(
            &path,
            Epoch(1),
            NodeId(1),
            &mut cells,
            LocalVsrTransport::all_reachable(voters()),
            |_| panic!("injected client unwind"),
        );
    }));
    assert!(panic.is_err());
    assert_eq!(cells.len(), 1);
    // Failure after cells have entered recovery must also restore custody.
    let unrelated = command(3);
    let mut foreign = DurableFabricRouter::open(&path, [PlacementGroupId(1)], Epoch(2)).unwrap();
    foreign
        .commit(PlacementGroupId(1), unrelated.commit.operation)
        .unwrap();
    drop(foreign);
    let result: RsfResult<()> = VsrBackedFabricRouter::with_installed_cells(
        &path,
        Epoch(1),
        NodeId(1),
        &mut cells,
        LocalVsrTransport::all_reachable(voters()),
        |_| Ok(()),
    );
    assert!(result.is_err());
    assert_eq!(cells.len(), 1);
    assert!(DurableVsrCell::open_existing(root.join("candidate"), PlacementGroupId(1)).is_err());
    drop(cells);
    DurableVsrCell::open_existing(root.join("candidate"), PlacementGroupId(1)).unwrap();
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn resident_coordinator_reuses_exact_cells_and_recovers_after_peer_receipts_and_reopen() {
    let root = root("resident-reuse");
    let mut cells = installed(&root, 2, false);
    let path = root.join("protocol");
    let mut cache = InstalledCoordinatorCache::default();
    for index in 3..=66 {
        let command = command(index);
        cache
            .with_cells(
                &path,
                Epoch(1),
                NodeId(1),
                &mut cells,
                LocalVsrTransport::all_reachable(voters()),
                |router, state| {
                    assert_eq!(
                        state,
                        if index == 3 {
                            CoordinatorState::Recovered
                        } else {
                            CoordinatorState::Retained
                        }
                    );
                    router.reconcile_application_commands()?;
                    router.commit_application(
                        PlacementGroupId(1),
                        command.request.clone(),
                        command.commit.mode,
                        command.commit.operation.clone(),
                        command.application.clone(),
                    )?;
                    assert_eq!(
                        router.committed_application_commands_since(CommitVersion(index - 1)),
                        vec![command]
                    );
                    assert_eq!(router.reconcile_application_commands()?, 0);
                    Ok(())
                },
            )
            .unwrap();
    }
    assert_eq!(
        cache.stats(),
        CoordinatorCacheStats {
            full_recoveries: 1,
            reuses: 63
        }
    );
    cells
        .get_mut(&PlacementGroupId(1))
        .unwrap()
        .remember_client_request(VsrClientRequestId::new("peer-receipt", 1), LogIndex(1))
        .unwrap();
    cache
        .with_cells(
            &path,
            Epoch(1),
            NodeId(1),
            &mut cells,
            LocalVsrTransport::all_reachable(voters()),
            |router, state| {
                assert_eq!(state, CoordinatorState::Recovered);
                assert_eq!(router.latest_version(), CommitVersion(66));
                Ok(())
            },
        )
        .unwrap();
    let token = cells[&PlacementGroupId(1)].state_token().unwrap();
    drop(cells);
    cells = [(
        PlacementGroupId(1),
        DurableVsrCell::open_existing(root.join("candidate"), PlacementGroupId(1)).unwrap(),
    )]
    .into();
    assert_ne!(token, cells[&PlacementGroupId(1)].state_token().unwrap());
    cache
        .with_cells(
            &path,
            Epoch(1),
            NodeId(1),
            &mut cells,
            LocalVsrTransport::all_reachable(voters()),
            |router, state| {
                assert_eq!(state, CoordinatorState::Recovered);
                assert_eq!(router.committed_application_commands().len(), 66);
                Ok(())
            },
        )
        .unwrap();
    assert_eq!(cache.stats().full_recoveries, 3);
    drop(cache);
    drop(cells);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn resident_coordinator_drops_stale_policy_errors_unwind_and_promised_authority() {
    let root = root("resident-invalidations");
    let mut cells = installed(&root, 1, false);
    let path = root.join("protocol");
    let mut cache = InstalledCoordinatorCache::default();
    cache
        .with_cells(
            &path,
            Epoch(1),
            NodeId(1),
            &mut cells,
            LocalVsrTransport::all_reachable(voters()),
            |router, _| {
                router.reconcile_application_commands()?;
                Ok(())
            },
        )
        .unwrap();
    let result: RsfResult<()> = cache.with_cells(
        &path,
        Epoch(1),
        NodeId(1),
        &mut cells,
        LocalVsrTransport::all_reachable(voters()),
        |_, state| {
            assert_eq!(state, CoordinatorState::Retained);
            append_protocol_policy(&path, Epoch(1), &ProtocolPolicy::new(2, false, 1)?)?;
            Ok(())
        },
    );
    assert!(result.unwrap_err().message.contains("another writer"));
    cache
        .with_cells(
            &path,
            Epoch(1),
            NodeId(1),
            &mut cells,
            LocalVsrTransport::all_reachable(voters()),
            |router, state| {
                assert_eq!(state, CoordinatorState::Recovered);
                assert!(!router.policy().allow_availability_first_immutable);
                Ok(())
            },
        )
        .unwrap();
    cache
        .with_cells(
            &path,
            Epoch(1),
            NodeId(1),
            &mut cells,
            LocalVsrTransport::all_reachable(voters()),
            |router, state| {
                assert_eq!(state, CoordinatorState::Retained);
                router.update_policy(true, 1)?;
                Ok(())
            },
        )
        .unwrap();
    let result: RsfResult<()> = cache.with_cells(
        &path,
        Epoch(1),
        NodeId(1),
        &mut cells,
        LocalVsrTransport::all_reachable(voters()),
        |_, _| Err(handoff_invalid("injected request error")),
    );
    assert!(result.is_err());
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _: RsfResult<()> = cache.with_cells(
            &path,
            Epoch(1),
            NodeId(1),
            &mut cells,
            LocalVsrTransport::all_reachable(voters()),
            |_, state| {
                assert_eq!(state, CoordinatorState::Recovered);
                panic!("injected unwind")
            },
        );
    }));
    assert!(panic.is_err());
    assert_eq!(cells.len(), 1);
    assert!(DurableVsrCell::open_existing(root.join("candidate"), PlacementGroupId(1)).is_err());
    cache
        .with_cells(
            &path,
            Epoch(1),
            NodeId(1),
            &mut cells,
            LocalVsrTransport::all_reachable(voters()),
            |_, state| {
                assert_eq!(state, CoordinatorState::Recovered);
                Ok(())
            },
        )
        .unwrap();
    cells
        .get_mut(&PlacementGroupId(1))
        .unwrap()
        .begin_view_change(
            NodeId(1),
            NodeId(1),
            VsrViewChangeRequest {
                pg: PlacementGroupId(1),
                previous_view: Epoch(2),
                target_view: Epoch(3),
                candidate: NodeId(1),
                voters: voters(),
            },
        )
        .unwrap();
    let result = cache.with_cells(
        &path,
        Epoch(1),
        NodeId(1),
        &mut cells,
        LocalVsrTransport::all_reachable(voters()),
        |_, _| Ok(()),
    );
    assert!(result.is_err());
    assert_eq!(cells[&PlacementGroupId(1)].view(), Epoch(2));
    let mut source =
        DurableVsrCell::open_existing(root.join("source"), PlacementGroupId(1)).unwrap();
    let request = VsrViewChangeRequest {
        pg: PlacementGroupId(1),
        previous_view: Epoch(1),
        target_view: Epoch(4),
        candidate: NodeId(1),
        voters: voters(),
    };
    let snapshots = [
        source
            .begin_view_change(NodeId(0), NodeId(1), request.clone())
            .unwrap(),
        cells
            .get_mut(&PlacementGroupId(1))
            .unwrap()
            .begin_view_change(NodeId(1), NodeId(1), request)
            .unwrap(),
    ];
    cells
        .get_mut(&PlacementGroupId(1))
        .unwrap()
        .install_view(&VsrViewInstallation::select(&snapshots).unwrap())
        .unwrap();
    cache
        .with_cells(
            &path,
            Epoch(1),
            NodeId(1),
            &mut cells,
            LocalVsrTransport::all_reachable(voters()),
            |router, state| {
                assert_eq!(state, CoordinatorState::Recovered);
                assert_eq!(router.membership(PlacementGroupId(1))?.0, Epoch(4));
                Ok(())
            },
        )
        .unwrap();
    drop(source);
    drop(cache);
    drop(cells);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn protocol_groups_share_a_validated_writer_without_cross_group_cursor_replays() {
    let root = root("shared-protocol-writer");
    let mut router =
        DurableFabricRouter::open(&root, [PlacementGroupId(1), PlacementGroupId(2)], Epoch(1))
            .unwrap();
    assert!(Arc::ptr_eq(
        &router.wals[&PlacementGroupId(1)].store,
        &router.wals[&PlacementGroupId(2)].store
    ));
    let before = router.storage_revision().unwrap();
    for index in 0..32 {
        let pg = PlacementGroupId(index % 2 + 1);
        let commit = router
            .commit(
                pg,
                FabricOperation::CatalogMutation {
                    namespace: "app".into(),
                    item: format!("item-{index}"),
                },
            )
            .unwrap();
        assert_eq!(commit.index, LogIndex(index / 2 + 1));
        assert_eq!(commit.version, CommitVersion(index + 1));
    }
    let after = router.storage_revision().unwrap();
    assert!(after.same_writer_epoch(&before));
    drop(router);
    let router =
        DurableFabricRouter::open(&root, [PlacementGroupId(1), PlacementGroupId(2)], Epoch(1))
            .unwrap();
    assert_eq!(router.latest_version(), CommitVersion(32));
    for pg in [PlacementGroupId(1), PlacementGroupId(2)] {
        assert_eq!(router.recovered_certificates(pg).unwrap().len(), 16);
    }
    drop(router);
    fs::remove_dir_all(root).unwrap();
}

struct PrefixTransport {
    inner: LocalVsrTransport,
    probes: Arc<std::sync::atomic::AtomicUsize>,
    prepares: Arc<std::sync::atomic::AtomicUsize>,
    fail_after: usize,
}
impl VsrTransport for PrefixTransport {
    fn confirm_primary(
        &mut self,
        replica: NodeId,
        probe: &rsf_vsr::VsrPrimaryProbe,
    ) -> Result<(), rsf_vsr::VsrTransportError> {
        let count = self
            .probes
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1;
        if count > self.fail_after {
            return Err(rsf_vsr::VsrTransportError {
                replica,
                reason: "injected quorum loss".into(),
            });
        }
        if probe.view == Epoch(2)
            && probe.primary == NodeId(1)
            && probe.voters == voters()
            && self.inner.committed_tail(replica, probe.pg)? == probe.committed_tail
        {
            Ok(())
        } else {
            Err(rsf_vsr::VsrTransportError {
                replica,
                reason: "simulated peer probe differs from installed prefix".into(),
            })
        }
    }
    fn acknowledge_prepare(
        &mut self,
        message: rsf_vsr::VsrPrepareMessage,
    ) -> Result<NodeId, rsf_vsr::VsrTransportError> {
        self.prepares
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.inner.acknowledge_prepare(message)
    }
    fn acknowledge_commit(
        &mut self,
        replica: NodeId,
        commit: rsf_vsr::VsrCommit,
    ) -> Result<NodeId, rsf_vsr::VsrTransportError> {
        self.inner.acknowledge_commit(replica, commit)
    }
    fn committed_tail(
        &mut self,
        replica: NodeId,
        pg: PlacementGroupId,
    ) -> Result<LogIndex, rsf_vsr::VsrTransportError> {
        self.inner.committed_tail(replica, pg)
    }
    fn transfer_committed(
        &mut self,
        replica: NodeId,
        entry: rsf_vsr::VsrCommittedEntry,
    ) -> Result<(), rsf_vsr::VsrTransportError> {
        self.inner.transfer_committed(replica, entry)
    }
}
#[test]
fn installed_prefix_receipt_recovery_confirms_before_and_after_and_replicates_accepted_entries() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    for fail_after in [0, 2, usize::MAX] {
        let root = root(&format!("prefix-confirmation-{fail_after}"));
        let mut cells = installed(&root, 8, true);
        let path = root.join("protocol");
        let probes = Arc::new(AtomicUsize::new(0));
        let prepares = Arc::new(AtomicUsize::new(0));
        let transport = PrefixTransport {
            inner: LocalVsrTransport::all_reachable(voters()),
            probes: probes.clone(),
            prepares: prepares.clone(),
            fail_after,
        };
        let mut cache = InstalledCoordinatorCache::default();
        let result = cache.with_cells(
            &path,
            Epoch(1),
            NodeId(1),
            &mut cells,
            transport,
            |router, _| router.reconcile_installed_application_commands(),
        );
        if fail_after == usize::MAX {
            assert_eq!(result.unwrap(), 9)
        } else {
            assert_eq!(result.unwrap_err().code, ErrorCode::QuorumUnavailable)
        }
        assert_eq!(
            ProtocolWal::recover(&path, PlacementGroupId(1))
                .unwrap()
                .len(),
            if fail_after == 0 { 0 } else { 9 }
        );
        assert_eq!(
            prepares.load(Ordering::Relaxed),
            if fail_after == 0 { 0 } else { 2 }
        );
        assert_eq!(
            probes.load(Ordering::Relaxed),
            if fail_after == 0 { 2 } else { 4 }
        );
        assert_eq!(
            cells[&PlacementGroupId(1)].committed_tail(),
            LogIndex(if fail_after == 0 { 8 } else { 9 })
        );
        drop(cache);
        drop(cells);
        fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn retained_installed_inventory_cannot_drop_an_unmigrated_group() {
    let root = root("retained-group-inventory");
    let mut cells = installed(&root, 1, false);
    let mut source = DurableVsrCell::open(
        root.join("pg2-source"),
        PlacementGroupId(2),
        Epoch(1),
        NodeId(0),
        voters(),
    )
    .unwrap();
    let mut candidate = DurableVsrCell::open(
        root.join("pg2-candidate"),
        PlacementGroupId(2),
        Epoch(1),
        NodeId(0),
        voters(),
    )
    .unwrap();
    let request = VsrViewChangeRequest {
        pg: PlacementGroupId(2),
        previous_view: Epoch(1),
        target_view: Epoch(2),
        candidate: NodeId(1),
        voters: voters(),
    };
    let reports = [
        source
            .begin_view_change(NodeId(0), NodeId(1), request.clone())
            .unwrap(),
        candidate
            .begin_view_change(NodeId(1), NodeId(1), request)
            .unwrap(),
    ];
    candidate
        .install_view(&VsrViewInstallation::select(&reports).unwrap())
        .unwrap();
    cells.insert(PlacementGroupId(2), candidate);
    let path = root.join("protocol");
    let mut cache = InstalledCoordinatorCache::default();
    cache
        .with_cells(
            &path,
            Epoch(1),
            NodeId(1),
            &mut cells,
            LocalVsrTransport::all_reachable(voters()),
            |router, _| {
                router.reconcile_application_commands()?;
                Ok(())
            },
        )
        .unwrap();
    cache
        .with_cells(
            &path,
            Epoch(1),
            NodeId(1),
            &mut cells,
            LocalVsrTransport::all_reachable(voters()),
            |router, state| {
                assert_eq!(state, CoordinatorState::Retained);
                let before = router.placement_map().clone();
                assert!(router.update_placement_map([PlacementGroupId(1)]).is_err());
                assert_eq!(router.placement_map(), &before);
                Ok(())
            },
        )
        .unwrap();
    cache.invalidate();
    cache
        .with_cells(
            &path,
            Epoch(1),
            NodeId(1),
            &mut cells,
            LocalVsrTransport::all_reachable(voters()),
            |router, state| {
                assert_eq!(state, CoordinatorState::Recovered);
                assert_eq!(
                    router.placement_map().groups,
                    vec![PlacementGroupId(1), PlacementGroupId(2)]
                );
                Ok(())
            },
        )
        .unwrap();
    drop(cache);
    drop(cells);
    drop(source);
    fs::remove_dir_all(root).unwrap();
}
