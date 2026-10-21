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
