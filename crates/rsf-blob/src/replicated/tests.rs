use super::*;
use rsf_core::NodeId;
use rsf_protocol::VsrBackedFabricRouter;

fn root(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "rsf-{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}
fn router(path: &Path) -> VsrBackedFabricRouter {
    VsrBackedFabricRouter::open(
        path,
        [PlacementGroupId(1)],
        Epoch(1),
        NodeId(1),
        [NodeId(1)],
    )
    .unwrap()
}
#[test]
fn complete_blob_commands_recover_bytes_receipts_and_retry_identity() {
    let root = root("complete_blob_commands");
    let mut router = router(&root.join("protocol"));
    let mut primary = DurableBlobEngine::open_replicated(root.join("primary"), "tenant").unwrap();
    let request = ClientRequestId::new("client", 1);
    let original = primary
        .put_request(
            &mut router,
            PlacementGroupId(1),
            request.clone(),
            b"exact payload".to_vec(),
        )
        .unwrap();
    let command = router.committed_application_commands().pop().unwrap();
    assert_eq!(payload_bytes(&command).unwrap(), b"exact payload");
    assert!(
        primary
            .put_request(
                &mut router,
                PlacementGroupId(1),
                request.clone(),
                b"changed".to_vec()
            )
            .is_err()
    );
    let mut follower = DurableBlobEngine::open_replicated(root.join("follower"), "tenant").unwrap();
    let applied = follower.apply_application_command(&command).unwrap();
    assert_eq!(applied.pin_evidence, original.pin_evidence);
    assert_eq!(
        follower.read_verified(&original.id).unwrap(),
        b"exact payload"
    );
    for _ in 0..3 {
        assert_eq!(
            follower.apply_application_command(&command).unwrap().pins,
            1
        );
    }
    let mut wrong = command.clone();
    wrong.application.namespace = "different".into();
    assert!(follower.apply_application_command(&wrong).is_err());
    drop(primary);
    drop(follower);
    drop(router);
    let mut router = self::router(&root.join("protocol"));
    // Plain open preserves the binding and cannot silently return to legacy mode.
    let mut primary = DurableBlobEngine::open(root.join("primary")).unwrap();
    let retry = primary
        .put_request(
            &mut router,
            PlacementGroupId(1),
            request.clone(),
            b"exact payload".to_vec(),
        )
        .unwrap();
    assert_eq!(retry.pin_evidence, original.pin_evidence);
    assert_eq!(retry.pins, 1);
    assert_eq!(router.committed_application_commands().len(), 1);
    let mut empty_router = self::router(&root.join("rolled-back"));
    assert!(
        primary
            .put_request(
                &mut empty_router,
                PlacementGroupId(1),
                request,
                b"exact payload".to_vec()
            )
            .is_err()
    );
    assert!(empty_router.committed_application_commands().is_empty());
    drop(primary);
    assert!(DurableBlobEngine::open_replicated(root.join("primary"), "other").is_err());
    let mut follower = DurableBlobEngine::open(root.join("follower")).unwrap();
    assert_eq!(
        follower.read_verified(&original.id).unwrap(),
        b"exact payload"
    );
}

#[test]
fn interrupted_materialization_rebuilds_from_command_and_rejects_corruption() {
    let root = root("blob_interrupted_materialization");
    let mut router = router(&root.join("protocol"));
    let bytes = b"recover from quorum".to_vec();
    let id = ContentId::for_bytes(&bytes);
    let mut body = PAYLOAD.to_vec();
    body.extend_from_slice(&bytes);
    router
        .commit_application(
            PlacementGroupId(1),
            ClientRequestId::new("interrupted", 1),
            ConsistencyMode::AvailabilityFirst,
            FabricOperation::BlobPin {
                digest: *id.as_bytes(),
            },
            ApplicationPayload {
                namespace: "tenant".into(),
                surface: ApplicationSurface::Blob,
                bytes: body,
            },
        )
        .unwrap();
    // No primary blob receipt or payload exists; its authoritative log is sufficient.
    drop(router);
    let router = self::router(&root.join("protocol"));
    let command = &router.committed_application_commands()[0];
    let mut blobs = DurableBlobEngine::open_replicated(root.join("blobs"), "tenant").unwrap();
    let mut bad = command.clone();
    *bad.application.bytes.last_mut().unwrap() ^= 1;
    assert!(blobs.apply_application_command(&bad).is_err());
    assert_eq!(blobs.verify_all().unwrap().chunks, 0);
    blobs.apply_application_command(command).unwrap();
    assert_eq!(blobs.read_verified(&id).unwrap(), bytes);
}

#[test]
fn oversized_request_and_legacy_binding_fail_before_mutating_store() {
    let root = root("blob_replication_validation");
    let mut router = router(&root.join("protocol"));
    let mut blobs = DurableBlobEngine::open_replicated(root.join("blobs"), "tenant").unwrap();
    assert!(
        blobs
            .put_request(
                &mut router,
                PlacementGroupId(1),
                ClientRequestId::new("oversized", 1),
                vec![1; 1024 * 1024]
            )
            .is_err()
    );
    assert_eq!(blobs.verify_all().unwrap().chunks, 0);
    assert_eq!(router.latest_version(), CommitVersion(0));
    let mut legacy = DurableBlobEngine::open(root.join("legacy")).unwrap();
    let staged = legacy.stage(b"legacy bytes".to_vec()).unwrap();
    drop(legacy);
    assert!(DurableBlobEngine::open_replicated(root.join("legacy"), "tenant").is_err());
    let mut legacy = DurableBlobEngine::open(root.join("legacy")).unwrap();
    assert_eq!(legacy.read_verified(&staged.id).unwrap(), b"legacy bytes");
}

#[test]
fn failed_receipt_fences_reads_and_recovers_original_command_after_reopen() {
    let root = root("blob_receipt_failure");
    let mut router = router(&root.join("protocol"));
    let mut primary = DurableBlobEngine::open_replicated(root.join("primary"), "tenant").unwrap();
    let original = primary
        .put_request(
            &mut router,
            PlacementGroupId(1),
            ClientRequestId::new("client", 1),
            b"durable bytes".to_vec(),
        )
        .unwrap();
    let command = router.committed_application_commands().pop().unwrap();
    let path = root.join("follower");
    let mut follower = DurableBlobEngine::open_replicated(&path, "tenant").unwrap();
    let journal = path.join("replication");
    std::fs::rename(&journal, journal.with_extension("displaced")).unwrap();
    std::fs::write(&journal, b"not a directory").unwrap();
    assert!(follower.apply_application_command(&command).is_err());
    assert!(follower.replication.receipts.is_empty());
    assert!(follower.read_verified(&original.id).is_err());
    assert!(follower.stage(b"new bytes".to_vec()).is_err());
    assert!(follower.apply_application_command(&command).is_err());
    drop(follower);
    std::fs::remove_file(&journal).unwrap();
    std::fs::rename(journal.with_extension("displaced"), &journal).unwrap();
    let mut follower = DurableBlobEngine::open_replicated(&path, "tenant").unwrap();
    let recovered = follower.apply_application_command(&command).unwrap();
    assert_eq!(recovered.pin_evidence, original.pin_evidence);
    assert_eq!(recovered.pins, 1);
    assert_eq!(
        follower.read_verified(&original.id).unwrap(),
        b"durable bytes"
    );
    // A checksummed but invalid application record must not disappear on replay.
    follower.replication.append(b"unknown command").unwrap();
    drop(follower);
    assert!(DurableBlobEngine::open(&path).is_err());
}
