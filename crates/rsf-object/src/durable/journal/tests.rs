use super::*;
use rsf_core::NodeId;
use rsf_protocol::{ConsistencyMode, FabricCommit, RequestCommitEvidence, VsrBackedFabricRouter};

fn root(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "rsf-object-journal-{name}-{}-{}",
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
    id: u64,
    bytes: &[u8],
) -> RsfResult<DurableObjectPut> {
    objects.put(
        router,
        PlacementGroupId(1),
        PlacementGroupId(1),
        ClientRequestId::new("app", id),
        "media",
        "key",
        bytes.to_vec(),
    )
}
#[test]
fn old_retry_preserves_current_head_and_exact_receipt_across_restart() {
    let root = root("retry");
    let mut router = router(&root);
    let mut objects = DurableObjectStore::open(root.join("object")).unwrap();
    objects
        .create_bucket(&mut router, PlacementGroupId(1), "media")
        .unwrap();
    let first = put(&mut objects, &mut router, 1, b"first").unwrap();
    let second = put(&mut objects, &mut router, 2, b"second").unwrap();
    let version = router.latest_version();
    assert_eq!(put(&mut objects, &mut router, 1, b"first").unwrap(), first);
    assert!(put(&mut objects, &mut router, 1, b"different").is_err());
    assert_eq!(objects.head("media", "key"), Some(&second.head));
    assert_eq!(objects.versions("media", "key").unwrap().len(), 2);
    drop(objects);
    let mut objects = DurableObjectStore::open(root.join("object")).unwrap();
    assert_eq!(objects.reconcile(&mut router).unwrap(), 0);
    assert_eq!(put(&mut objects, &mut router, 1, b"first").unwrap(), first);
    assert_eq!(objects.read_verified("media", "key").unwrap(), b"second");
    assert_eq!(router.latest_version(), version);
}

struct InterruptAfterCommit {
    inner: VsrBackedFabricRouter,
    boundary: &'static str,
    fail_path: Option<PathBuf>,
    kill: bool,
}
impl InterruptAfterCommit {
    fn after_commit(&mut self, boundary: &str) {
        if self.boundary == boundary {
            if self.kill {
                use std::io::Write;
                println!("OBJECT_PROTOCOL_DURABLE");
                std::io::stdout().flush().unwrap();
                loop {
                    std::thread::park_timeout(std::time::Duration::from_secs(1));
                }
            }
            if let Some(path) = self.fail_path.take() {
                fs::rename(&path, path.with_extension("displaced")).unwrap();
                fs::write(path, b"not a directory").unwrap();
            }
        }
    }
}
impl FabricRouterPort for InterruptAfterCommit {
    fn validate_immutable_dependency(
        &self,
        pg: PlacementGroupId,
        request: &ClientRequestId,
        digest: [u8; 32],
    ) -> RsfResult<()> {
        self.inner
            .validate_immutable_dependency(pg, request, digest)
    }

    fn commit_immutable_dependency(
        &mut self,
        pg: PlacementGroupId,
        request: ClientRequestId,
        digest: [u8; 32],
    ) -> RsfResult<RequestCommitEvidence> {
        let result = self
            .inner
            .commit_immutable_dependency(pg, request, digest)?;
        self.after_commit("pin");
        Ok(result)
    }

    fn route_operation(&self, operation: &FabricOperation) -> Option<PlacementGroupId> {
        self.inner.route_operation(operation)
    }
    fn commit(
        &mut self,
        pg: PlacementGroupId,
        operation: FabricOperation,
    ) -> RsfResult<FabricCommit> {
        self.inner.commit(pg, operation)
    }
    fn commit_with_mode(
        &mut self,
        pg: PlacementGroupId,
        mode: ConsistencyMode,
        operation: FabricOperation,
    ) -> RsfResult<FabricCommit> {
        self.inner.commit_with_mode(pg, mode, operation)
    }
    fn commit_request(
        &mut self,
        pg: PlacementGroupId,
        request: ClientRequestId,
        operation: FabricOperation,
    ) -> RsfResult<RequestCommitEvidence> {
        self.commit_request_with_mode(pg, request, ConsistencyMode::Strict, operation)
    }
    fn commit_request_with_mode(
        &mut self,
        pg: PlacementGroupId,
        request: ClientRequestId,
        mode: ConsistencyMode,
        operation: FabricOperation,
    ) -> RsfResult<RequestCommitEvidence> {
        let boundary = match operation {
            FabricOperation::BlobPin { .. } => "pin",
            FabricOperation::ObjectManifest { .. } => "head",
            _ => "bucket",
        };
        let result = FabricRouterPort::commit_request_with_mode(
            &mut self.inner,
            pg,
            request,
            mode,
            operation,
        )?;
        self.after_commit(boundary);
        Ok(result)
    }
    fn validate_request(
        &self,
        pg: PlacementGroupId,
        request: &ClientRequestId,
        mode: ConsistencyMode,
        operation: &FabricOperation,
    ) -> RsfResult<()> {
        self.inner.validate_request(pg, request, mode, operation)
    }
    fn validate_application_request(
        &self,
        pg: PlacementGroupId,
        request: &ClientRequestId,
        mode: ConsistencyMode,
        operation: &FabricOperation,
        application: &rsf_protocol::ApplicationPayload,
    ) -> RsfResult<()> {
        self.inner
            .validate_application_request(pg, request, mode, operation, application)
    }
    fn commit_application_request(
        &mut self,
        pg: PlacementGroupId,
        request: ClientRequestId,
        mode: ConsistencyMode,
        operation: FabricOperation,
        application: rsf_protocol::ApplicationPayload,
    ) -> RsfResult<RequestCommitEvidence> {
        let boundary = match &operation {
            FabricOperation::ObjectManifest { .. } => "head",
            _ => "bucket",
        };
        let result =
            self.inner
                .commit_application_request(pg, request, mode, operation, application)?;
        self.after_commit(boundary);
        Ok(result)
    }
    fn request_certificate(&self, request: &ClientRequestId) -> Option<CommitCertificate> {
        self.inner.request_certificate(request)
    }
    fn latest_version(&self) -> CommitVersion {
        self.inner.latest_version()
    }
}
#[test]
fn interrupted_writer_process() {
    let Some(root) = std::env::var_os("RSF_OBJECT_JOURNAL_ROOT") else {
        return;
    };
    let root = PathBuf::from(root);
    let mut router = InterruptAfterCommit {
        inner: router(&root),
        boundary: if std::env::var("RSF_OBJECT_BOUNDARY").unwrap() == "pin" {
            "pin"
        } else {
            "head"
        },
        fail_path: None,
        kill: true,
    };
    let mut objects = if std::env::var("RSF_OBJECT_SCOPED").as_deref() == Ok("1") {
        DurableObjectStore::open_replicated(root.join("object"), "tenant").unwrap()
    } else {
        DurableObjectStore::open(root.join("object")).unwrap()
    };
    objects
        .create_bucket(&mut router, PlacementGroupId(1), "media")
        .unwrap();
    put(&mut objects, &mut router, 1, b"original durable bytes").unwrap();
}
#[test]
fn process_kill_recovers_payload_at_both_commit_boundaries() {
    for scoped in [false, true] {
        killed_object(scoped);
    }
}
fn killed_object(scoped: bool) {
    use std::io::BufRead;
    use std::process::{Command, Stdio};
    for boundary in ["pin", "head"] {
        let root = root(boundary);
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "durable::journal::tests::interrupted_writer_process",
                "--nocapture",
            ])
            .env("RSF_OBJECT_JOURNAL_ROOT", &root)
            .env("RSF_OBJECT_BOUNDARY", boundary)
            .env("RSF_OBJECT_SCOPED", if scoped { "1" } else { "0" })
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let (send, receive) = std::sync::mpsc::channel();
        let reader = std::thread::spawn(move || {
            for line in std::io::BufReader::new(stdout).lines() {
                if line.unwrap().contains("OBJECT_PROTOCOL_DURABLE") {
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
        let mut objects = DurableObjectStore::open(root.join("object")).unwrap();
        assert!(objects.head("media", "key").is_none());
        assert!(put(&mut objects, &mut router, 1, b"wrong bytes").is_err());
        assert_eq!(objects.reconcile(&mut router).unwrap(), 1);
        let receipt = put(&mut objects, &mut router, 1, b"original durable bytes").unwrap();
        assert_eq!(receipt.pins, 1);
        assert_eq!(
            objects.read_verified("media", "key").unwrap(),
            b"original durable bytes"
        );
        assert_eq!(router.latest_version(), CommitVersion(3));
        assert_eq!(objects.versions("media", "key").unwrap().len(), 1);
        assert_eq!(
            router.committed_application_commands().len(),
            if scoped { 2 } else { 0 }
        );
    }
}
#[test]
fn failed_receipt_is_fenced_and_reconciled_without_publishing_head() {
    for scoped in [false, true] {
        failed_receipt(scoped);
    }
}
fn failed_receipt(scoped: bool) {
    let root = root("receipt-failure");
    let journal = root.join("object/journal");
    let mut router = InterruptAfterCommit {
        inner: router(&root),
        boundary: "head",
        fail_path: Some(journal.clone()),
        kill: false,
    };
    let mut objects = if scoped {
        DurableObjectStore::open_replicated(root.join("object"), "tenant").unwrap()
    } else {
        DurableObjectStore::open(root.join("object")).unwrap()
    };
    objects
        .create_bucket(&mut router, PlacementGroupId(1), "media")
        .unwrap();
    assert!(put(&mut objects, &mut router, 1, b"durable bytes").is_err());
    assert!(objects.head("media", "key").is_none());
    assert!(objects.read_verified("media", "key").is_err());
    fs::remove_file(&journal).unwrap();
    fs::rename(journal.with_extension("displaced"), &journal).unwrap();
    assert!(objects.reconcile(&mut router).is_err());
    drop(objects);
    let mut objects = DurableObjectStore::open(root.join("object")).unwrap();
    assert_eq!(objects.reconcile(&mut router).unwrap(), 1);
    assert_eq!(
        objects.read_verified("media", "key").unwrap(),
        b"durable bytes"
    );
}
#[test]
fn single_writer_and_bootstrap_snapshot_are_enforced() {
    let root = root("bootstrap");
    let mut router = router(&root);
    let mut objects = DurableObjectStore::open(root.join("object")).unwrap();
    assert!(DurableObjectStore::open(root.join("object")).is_err());
    objects
        .create_bucket(&mut router, PlacementGroupId(1), "media")
        .unwrap();
    let first = put(&mut objects, &mut router, 1, b"legacy bytes").unwrap();
    let legacy = encode_index(&objects.buckets, &objects.versions);
    drop(objects);
    fs::remove_dir_all(root.join("object/journal")).unwrap();
    fs::write(root.join("object/objects.index"), legacy).unwrap();
    let mut objects = DurableObjectStore::open(root.join("object")).unwrap();
    assert_eq!(
        objects.read_verified("media", "key").unwrap(),
        b"legacy bytes"
    );
    assert_eq!(
        put(&mut objects, &mut router, 1, b"legacy bytes")
            .unwrap()
            .head,
        first.head
    );
    drop(objects);
    fs::write(root.join("object/objects.index"), b"corrupt old index").unwrap();
    let mut objects = DurableObjectStore::open(root.join("object")).unwrap();
    assert_eq!(
        objects.read_verified("media", "key").unwrap(),
        b"legacy bytes"
    );
}

#[test]
fn authoritative_apply_checks_payload_and_replays_without_resurrecting_history() {
    let root = root("apply");
    let mut router = router(&root);
    let mut source = DurableObjectStore::open(root.join("source")).unwrap();
    let bucket_request = ClientRequestId::new("bucket", 0);
    let bucket = source
        .create_bucket_request(
            &mut router,
            PlacementGroupId(1),
            bucket_request.clone(),
            "media",
        )
        .unwrap();
    let first = put(&mut source, &mut router, 1, b"first").unwrap();
    let second = put(&mut source, &mut router, 2, b"second").unwrap();
    let mut follower = DurableObjectStore::open(root.join("follower")).unwrap();
    follower
        .apply_committed_bucket(bucket_request, bucket)
        .unwrap();
    assert!(
        follower
            .apply_committed_put(
                ClientRequestId::new("app", 1),
                first.clone(),
                b"wrong".to_vec()
            )
            .is_err()
    );
    assert!(follower.head("media", "key").is_none());
    follower
        .apply_committed_put(
            ClientRequestId::new("app", 1),
            first.clone(),
            b"first".to_vec(),
        )
        .unwrap();
    follower
        .apply_committed_put(
            ClientRequestId::new("app", 2),
            second.clone(),
            b"second".to_vec(),
        )
        .unwrap();
    drop(follower);
    let mut follower = DurableObjectStore::open(root.join("follower")).unwrap();
    follower
        .apply_committed_put(ClientRequestId::new("app", 1), first, b"first".to_vec())
        .unwrap();
    assert_eq!(follower.head("media", "key"), Some(&second.head));
    assert_eq!(follower.read_verified("media", "key").unwrap(), b"second");
    assert_eq!(follower.versions("media", "key").unwrap().len(), 2);
}

#[test]
fn invalid_receipt_and_truncated_journal_fail_closed() {
    let root = root("corrupt");
    let mut router = router(&root);
    let mut objects = DurableObjectStore::open(root.join("object")).unwrap();
    objects
        .create_bucket(&mut router, PlacementGroupId(1), "media")
        .unwrap();
    let first = put(&mut objects, &mut router, 1, b"first").unwrap();
    let mut forged = first.clone();
    forged.head.evidence.consistency = CommitConsistency::AvailabilityFirst;
    assert!(
        objects
            .apply_committed_put(ClientRequestId::new("bad", 0), forged, b"first".to_vec())
            .is_err()
    );
    let mut forged = first.clone();
    forged.pin_version = forged.head.version;
    forged.pin_evidence.version = forged.head.version;
    assert!(
        objects
            .apply_committed_put(
                ClientRequestId::new("bad-pin", 0),
                forged,
                b"first".to_vec()
            )
            .is_err()
    );
    assert!(
        objects
            .reconcile(&mut rsf_protocol::FabricRouter::with_groups([
                PlacementGroupId(1)
            ]))
            .is_err()
    );
    objects.journal.append("intent\tput\ttruncated").unwrap();
    drop(objects);
    assert!(DurableObjectStore::open(root.join("object")).is_err());
}
