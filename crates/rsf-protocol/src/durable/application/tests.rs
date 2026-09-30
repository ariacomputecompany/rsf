use super::*;
use std::fs;

fn root(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "rsf-command-{name}-{}-{}",
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
        [PlacementGroupId(1), PlacementGroupId(2)],
        Epoch(1),
        NodeId(0),
        [NodeId(0)],
    )
    .unwrap()
}
fn payload(bytes: &[u8]) -> ApplicationPayload {
    ApplicationPayload {
        namespace: "tenant-a/db".into(),
        surface: ApplicationSurface::Kv,
        bytes: bytes.to_vec(),
    }
}
fn operation() -> FabricOperation {
    FabricOperation::KvMutation {
        key: b"key\0\xff".to_vec(),
    }
}
fn command(router: &VsrBackedFabricRouter, request: u64) -> ReplicatedFabricCommand {
    ReplicatedFabricCommand {
        commit: router
            .router
            .next_commit(PlacementGroupId(1), ConsistencyMode::Strict, operation())
            .unwrap(),
        request: ClientRequestId::new("app", request),
        application: payload(b"full values\0\xff"),
    }
}
#[test]
fn application_codec_preserves_every_operation_and_rejects_malformed_bytes() {
    let root = root("codec");
    let router = router(&root);
    let mut command = command(&router, 1);
    let operations = vec![
        FabricOperation::CatalogMutation {
            namespace: "ns".into(),
            item: "item".into(),
        },
        FabricOperation::TransactionCommit {
            transaction: [7; 16],
        },
        FabricOperation::RelationalCatalog {
            table: "users".into(),
            item: "index".into(),
        },
        FabricOperation::RelationalMutation {
            table: "users".into(),
            key: vec![0, 255],
        },
        operation(),
        FabricOperation::KvBatch {
            keys: vec![vec![], vec![0, 255], b"other".to_vec()],
        },
        FabricOperation::ObjectBucket {
            bucket: "media".into(),
        },
        FabricOperation::ObjectMultipartStart {
            bucket: "media".into(),
            key: "hero".into(),
            upload_id: 123,
        },
        FabricOperation::ObjectMultipartPart {
            bucket: "media".into(),
            key: "hero".into(),
            upload_id: 123,
            part_number: 4,
        },
        FabricOperation::ObjectHead {
            bucket: "media".into(),
            key: "hero".into(),
        },
        FabricOperation::ObjectManifest {
            bucket: "media".into(),
            key: "hero".into(),
        },
        FabricOperation::BlobPin { digest: [8; 32] },
    ];
    for operation in operations {
        command.commit.operation_id = operation.operation_id();
        command.commit.operation = operation;
        let bytes = command.encode().unwrap();
        assert_eq!(ReplicatedFabricCommand::decode(&bytes).unwrap(), command);
        for length in 0..bytes.len() {
            assert!(ReplicatedFabricCommand::decode(&bytes[..length]).is_err());
        }
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert!(ReplicatedFabricCommand::decode(&trailing).is_err());
    }
    command.application.bytes = vec![1; rsf_vsr::MAX_REPLICATED_COMMAND_BYTES];
    assert!(command.encode().is_err());
    command.application.bytes = vec![1];
    command.commit.operation_id = OperationId(0);
    assert!(command.encode().is_err());
}
#[test]
fn command_retry_binds_values_namespace_and_original_version_across_reopen() {
    let root = root("retry");
    let mut router = router(&root);
    let request = ClientRequestId::new("app", 1);
    let first = router
        .commit_application(
            PlacementGroupId(1),
            request.clone(),
            ConsistencyMode::Strict,
            operation(),
            payload(b"first"),
        )
        .unwrap();
    router
        .commit_application(
            PlacementGroupId(2),
            ClientRequestId::new("app", 2),
            ConsistencyMode::Strict,
            operation(),
            payload(b"second"),
        )
        .unwrap();
    drop(router);
    let mut router = super::tests::router(&root);
    assert!(
        router
            .commit_application(
                PlacementGroupId(1),
                request.clone(),
                ConsistencyMode::Strict,
                operation(),
                payload(b"changed")
            )
            .is_err()
    );
    let mut other = payload(b"first");
    other.namespace = "tenant-b/db".into();
    assert!(
        router
            .commit_application(
                PlacementGroupId(1),
                request.clone(),
                ConsistencyMode::Strict,
                operation(),
                other
            )
            .is_err()
    );
    assert!(
        router
            .commit_request(
                PlacementGroupId(1),
                request.clone(),
                ConsistencyMode::Strict,
                operation()
            )
            .is_err()
    );
    let retry = router
        .commit_application(
            PlacementGroupId(1),
            request,
            ConsistencyMode::Strict,
            operation(),
            payload(b"first"),
        )
        .unwrap();
    assert!(retry.replayed);
    assert_eq!(retry.certificate, first.certificate);
    assert_eq!(router.latest_version(), CommitVersion(2));
    assert_eq!(router.committed_application_commands().len(), 2);
}
#[test]
fn pending_command_reserves_global_version_before_another_group_can_commit() {
    let root = root("reservation");
    let mut router = router(&root);
    let original = command(&router, 1);
    router.prepare_application_command(&original).unwrap();
    drop(router);
    let mut router = super::tests::router(&root);
    let other = router.commit(PlacementGroupId(2), operation()).unwrap();
    assert_eq!(other.version, CommitVersion(2));
    assert_eq!(router.committed_application_commands(), vec![original]);
}
#[test]
fn interrupted_application_writer() {
    let Some(root) = std::env::var_os("RSF_COMMAND_KILL_ROOT") else {
        return;
    };
    let root = PathBuf::from(root);
    let mut router = router(&root);
    let original = command(&router, 1);
    router.prepare_application_command(&original).unwrap();
    router.require_application_commit(&original).unwrap();
    use std::io::Write;
    println!("APPLICATION_VSR_DURABLE");
    std::io::stdout().flush().unwrap();
    loop {
        std::thread::park_timeout(std::time::Duration::from_secs(1));
    }
}
#[test]
fn killed_writer_recovers_version_values_and_request_without_client_table_record() {
    use std::io::BufRead;
    use std::process::{Command, Stdio};
    let root = root("kill");
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "durable::application::tests::interrupted_application_writer",
            "--nocapture",
        ])
        .env("RSF_COMMAND_KILL_ROOT", &root)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let (send, receive) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in std::io::BufReader::new(stdout).lines() {
            if line.unwrap().contains("APPLICATION_VSR_DURABLE") {
                let _ = send.send(());
                return;
            }
        }
    });
    let arrived = receive.recv_timeout(std::time::Duration::from_secs(15));
    let _ = child.kill();
    child.wait().unwrap();
    reader.join().unwrap();
    arrived.unwrap();
    let mut router = router(&root);
    assert_eq!(router.latest_version(), CommitVersion(0));
    assert_eq!(router.reconcile_application_commands().unwrap(), 1);
    let commands = router.committed_application_commands();
    assert_eq!(commands.len(), 1);
    assert_eq!(commands[0].application.bytes, b"full values\0\xff");
    assert_eq!(commands[0].request, ClientRequestId::new("app", 1));
    assert_eq!(commands[0].commit.version, CommitVersion(1));
    assert_eq!(router.reconcile_application_commands().unwrap(), 0);
}
#[test]
fn uncertain_receipt_persistence_fences_all_write_paths_until_reopen() {
    let root = root("failure");
    let path = root.join("protocol");
    let mut router = router(&path);
    let original = command(&router, 1);
    router.prepare_application_command(&original).unwrap();
    router
        .application_commands
        .insert(original.request.clone(), original.clone());
    router.require_application_commit(&original).unwrap();
    let displaced = root.join("displaced");
    fs::rename(&path, &displaced).unwrap();
    fs::write(&path, b"not a directory").unwrap();
    assert!(router.finish_application_command(&original).is_err());
    fs::remove_file(&path).unwrap();
    fs::rename(displaced, &path).unwrap();
    assert!(router.commit(PlacementGroupId(2), operation()).is_err());
    drop(router);
    let mut router = super::tests::router(&path);
    assert_eq!(router.reconcile_application_commands().unwrap(), 1);
    assert_eq!(router.committed_application_commands(), vec![original]);
}
#[test]
fn complete_commands_never_rebind_a_legacy_request() {
    let root = root("legacy");
    let mut router = router(&root);
    let request = ClientRequestId::new("legacy", 1);
    router
        .commit_request(
            PlacementGroupId(1),
            request.clone(),
            ConsistencyMode::Strict,
            operation(),
        )
        .unwrap();
    assert!(
        router
            .commit_application(
                PlacementGroupId(1),
                request,
                ConsistencyMode::Strict,
                operation(),
                payload(b"invented values")
            )
            .is_err()
    );
    assert!(router.committed_application_commands().is_empty());
}

#[test]
fn hostile_client_length_and_oversized_payload_fail_without_a_prepare() {
    let root = root("bounds");
    let mut router = router(&root);
    let original = command(&router, 1);
    let mut bytes = original.encode().unwrap();
    // RSFC1 + length-prefixed RSFP1 record; client length is RSFP1 offset 47.
    bytes[9 + 47..9 + 55].copy_from_slice(&u64::MAX.to_le_bytes());
    assert!(ReplicatedFabricCommand::decode(&bytes).is_err());
    let payload = payload(&vec![7; rsf_vsr::MAX_REPLICATED_COMMAND_BYTES]);
    assert!(
        router
            .commit_application(
                PlacementGroupId(1),
                original.request,
                ConsistencyMode::Strict,
                operation(),
                payload
            )
            .is_err()
    );
    assert_eq!(router.cells[&PlacementGroupId(1)].next_index(), LogIndex(1));
    assert_eq!(router.latest_version(), CommitVersion(0));
}

#[test]
fn placement_changes_cannot_hide_application_history_on_restart() {
    let root = root("placement");
    let mut router = router(&root);
    router
        .commit_application(
            PlacementGroupId(1),
            ClientRequestId::new("app", 1),
            ConsistencyMode::Strict,
            operation(),
            payload(b"retained values"),
        )
        .unwrap();
    assert!(router.update_placement_map([PlacementGroupId(2)]).is_err());
    drop(router);
    let router = super::tests::router(&root);
    assert!(router.placement_map().groups.contains(&PlacementGroupId(1)));
    assert_eq!(router.committed_application_commands().len(), 1);
}
