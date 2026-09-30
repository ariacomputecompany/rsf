use super::*;
use rsf_core::NodeId;
use rsf_protocol::VsrBackedFabricRouter;
fn root(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "rsf-object-command-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}
fn router(root: &Path) -> VsrBackedFabricRouter {
    VsrBackedFabricRouter::open(
        root.join("router"),
        [PlacementGroupId(1)],
        Epoch(1),
        NodeId(0),
        [NodeId(0)],
    )
    .unwrap()
}
fn put(
    objects: &mut DurableObjectStore,
    router: &mut impl FabricRouterPort,
    request: u64,
    bytes: &[u8],
) -> RsfResult<DurableObjectPut> {
    objects.put(
        router,
        PlacementGroupId(1),
        PlacementGroupId(1),
        ClientRequestId::new("app", request),
        "media",
        "key",
        bytes.to_vec(),
    )
}
#[test]
fn replay_preserves_bucket_aliases_object_history_and_original_receipts() {
    let root = root("history");
    let mut router = router(&root);
    let mut objects = DurableObjectStore::open_replicated(root.join("objects"), "tenant").unwrap();
    let original_bucket = objects
        .create_bucket_request(
            &mut router,
            PlacementGroupId(1),
            ClientRequestId::new("bucket", 1),
            "media",
        )
        .unwrap();
    let alias = objects
        .create_bucket_request(
            &mut router,
            PlacementGroupId(1),
            ClientRequestId::new("bucket", 2),
            "media",
        )
        .unwrap();
    assert_eq!(alias, original_bucket);
    let first = put(&mut objects, &mut router, 1, b"first").unwrap();
    let second = put(&mut objects, &mut router, 2, b"second").unwrap();
    assert_eq!(put(&mut objects, &mut router, 1, b"first").unwrap(), first);
    assert!(put(&mut objects, &mut router, 1, b"changed").is_err());
    let commands = router.committed_application_commands();
    assert_eq!(commands.len(), 4);
    let mut follower =
        DurableObjectStore::open_replicated(root.join("follower"), "tenant").unwrap();
    for command in &commands {
        follower.apply_application_command(command).unwrap();
    }
    for command in &commands {
        follower.apply_application_command(command).unwrap();
    }
    assert_eq!(follower.bucket("media"), Some(&original_bucket));
    assert_eq!(follower.head("media", "key"), Some(&second.head));
    assert_eq!(follower.versions("media", "key").unwrap().len(), 2);
    assert_eq!(follower.read_verified("media", "key").unwrap(), b"second");
    assert_eq!(
        follower.blobs.read_verified(&first.head.content).unwrap(),
        b"first"
    );
    let version = router.latest_version();
    drop(objects);
    drop(follower);
    drop(router);
    let mut router = self::router(&root);
    let mut objects = DurableObjectStore::open(root.join("objects")).unwrap();
    assert_eq!(put(&mut objects, &mut router, 1, b"first").unwrap(), first);
    assert_eq!(
        objects
            .create_bucket_request(
                &mut router,
                PlacementGroupId(1),
                ClientRequestId::new("bucket", 2),
                "media"
            )
            .unwrap(),
        original_bucket
    );
    assert_eq!(router.latest_version(), version);
    let mut rolled_back = self::router(&root.join("rollback"));
    assert!(put(&mut objects, &mut rolled_back, 1, b"first").is_err());
    assert!(rolled_back.committed_application_commands().is_empty());
    drop(objects);
    assert!(DurableObjectStore::open_replicated(root.join("objects"), "wrong").is_err());
    let mut follower = DurableObjectStore::open(root.join("follower")).unwrap();
    assert_eq!(follower.head("media", "key"), Some(&second.head));
    assert_eq!(follower.read_verified("media", "key").unwrap(), b"second");
}

#[test]
fn rejects_bad_commands_and_oversized_intents_before_mutating_state() {
    let root = root("validation");
    let mut router = router(&root);
    let mut objects = DurableObjectStore::open_replicated(root.join("objects"), "tenant").unwrap();
    objects
        .create_bucket(&mut router, PlacementGroupId(1), "media")
        .unwrap();
    let before = router.latest_version();
    assert!(
        objects
            .put(
                &mut router,
                PlacementGroupId(999),
                PlacementGroupId(1),
                ClientRequestId::new("bad-placement", 1),
                "media",
                "key",
                b"bytes".to_vec()
            )
            .is_err()
    );
    assert!(objects.journal.pending.is_none());
    assert_eq!(objects.blobs.verify_all().unwrap().chunks, 0);
    assert!(put(&mut objects, &mut router, 1, &vec![1; 1024 * 1024]).is_err());
    assert!(objects.journal.pending.is_none());
    assert_eq!(objects.blobs.verify_all().unwrap().chunks, 0);
    assert_eq!(router.latest_version(), before);
    put(&mut objects, &mut router, 2, b"valid bytes").unwrap();
    let commands = router.committed_application_commands();
    let mut follower =
        DurableObjectStore::open_replicated(root.join("follower"), "tenant").unwrap();
    follower.apply_application_command(&commands[0]).unwrap();
    let valid = &commands[1];
    let mut bad = valid.clone();
    *bad.application.bytes.last_mut().unwrap() ^= 1;
    assert!(follower.apply_application_command(&bad).is_err());
    let mut bad = valid.clone();
    bad.application.namespace = "wrong".into();
    assert!(follower.apply_application_command(&bad).is_err());
    let mut bad = valid.clone();
    bad.request.request.0 += 1;
    assert!(follower.apply_application_command(&bad).is_err());
    let mut bad = valid.clone();
    bad.application.bytes.truncate(7);
    assert!(follower.apply_application_command(&bad).is_err());
    assert!(follower.head("media", "key").is_none());
    assert_eq!(follower.blobs.verify_all().unwrap().chunks, 0);
    follower.apply_application_command(valid).unwrap();
    assert_eq!(
        follower.read_verified("media", "key").unwrap(),
        b"valid bytes"
    );
    // Nonempty metadata-only stores are readable but cannot be silently rebound.
    let mut legacy = DurableObjectStore::open(root.join("legacy")).unwrap();
    legacy
        .create_bucket(&mut router, PlacementGroupId(1), "legacy")
        .unwrap();
    drop(legacy);
    assert!(DurableObjectStore::open_replicated(root.join("legacy"), "tenant").is_err());
    assert!(
        DurableObjectStore::open(root.join("legacy"))
            .unwrap()
            .bucket("legacy")
            .is_some()
    );
}
