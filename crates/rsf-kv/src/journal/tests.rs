use super::*;
use rsf_core::NodeId;
use rsf_protocol::{RequestCommitEvidence, VsrBackedFabricRouter};
use std::path::PathBuf;

fn root(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "rsf-kv-journal-{name}-{}-{}",
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
fn batch(request: u64, snapshot: ReadSnapshot, value: &[u8]) -> KvBatch {
    KvBatch {
        client_request: Some(ClientRequestId::new("app", request)),
        snapshot,
        mutations: vec![KvMutation::Set {
            key: b"key".to_vec(),
            value: value.to_vec(),
        }],
    }
}

struct InterruptAfterCommit {
    inner: VsrBackedFabricRouter,
    kill_boundary: bool,
    fail_path: Option<PathBuf>,
}
impl InterruptAfterCommit {
    fn after_commit(&mut self) {
        if self.kill_boundary {
            use std::io::Write;
            println!("KV_PROTOCOL_DURABLE");
            std::io::stdout().flush().unwrap();
            loop {
                std::thread::park_timeout(std::time::Duration::from_secs(1));
            }
        }
        if let Some(path) = self.fail_path.take() {
            std::fs::rename(&path, path.with_extension("displaced")).unwrap();
            std::fs::write(path, b"not a directory").unwrap();
        }
    }
}
impl FabricRouterPort for InterruptAfterCommit {
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
        let result = FabricRouterPort::commit_request_with_mode(
            &mut self.inner,
            pg,
            request,
            mode,
            operation,
        )?;
        self.after_commit();
        Ok(result)
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
        let result =
            self.inner
                .commit_application_request(pg, request, mode, operation, application)?;
        self.after_commit();
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
    let Some(root) = std::env::var_os("RSF_KV_JOURNAL_ROOT") else {
        return;
    };
    let root = PathBuf::from(root);
    let mut router = InterruptAfterCommit {
        inner: router(&root),
        kill_boundary: true,
        fail_path: None,
    };
    let mut kv = if std::env::var("RSF_KV_SCOPED").as_deref() == Ok("1") {
        DurableKvEngine::open_replicated(root.join("kv"), Epoch(1), "tenant").unwrap()
    } else {
        DurableKvEngine::open(root.join("kv"), Epoch(1)).unwrap()
    };
    let snapshot = kv.snapshot(&router);
    kv.commit_batch(
        &mut router,
        PlacementGroupId(1),
        batch(7, snapshot, b"full durable payload"),
    )
    .unwrap();
}

#[test]
fn process_kill_between_protocol_commit_and_values_recovers_original_batch() {
    for scoped in [false, true] {
        killed_batch(scoped);
    }
}
fn killed_batch(scoped: bool) {
    use std::io::BufRead;
    use std::process::{Command, Stdio};
    let root = root("kill");
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "journal::tests::interrupted_writer_process",
            "--nocapture",
        ])
        .env("RSF_KV_JOURNAL_ROOT", &root)
        .env("RSF_KV_SCOPED", if scoped { "1" } else { "0" })
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let (send, receive) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in std::io::BufReader::new(stdout).lines() {
            if line.unwrap().contains("KV_PROTOCOL_DURABLE") {
                let _ = send.send(());
                return;
            }
        }
    });
    let boundary = receive.recv_timeout(std::time::Duration::from_secs(15));
    let _ = child.kill();
    child.wait().unwrap();
    reader.join().unwrap();
    boundary.unwrap();
    let mut router = router(&root);
    let mut kv = DurableKvEngine::open(root.join("kv"), Epoch(1)).unwrap();
    assert_eq!(kv.pending_request_count(), 1);
    assert_eq!(
        router.committed_application_commands().len(),
        usize::from(scoped)
    );
    assert_eq!(router.latest_version(), CommitVersion(1));
    assert_eq!(kv.reconcile(&mut router).unwrap(), 1);
    assert_eq!(kv.get(b"key").unwrap().value, b"full durable payload");
    let retry = batch(7, kv.snapshot(&router), b"full durable payload");
    assert!(
        kv.commit_batch(&mut router, PlacementGroupId(1), retry)
            .unwrap()
            .replayed
    );
    assert_eq!(router.latest_version(), CommitVersion(1));
    assert_eq!(kv.changes_since(CommitVersion(0)).len(), 1);
    drop(kv);
    let recovered = DurableKvEngine::open(root.join("kv"), Epoch(1)).unwrap();
    assert_eq!(
        recovered.get(b"key").unwrap().value,
        b"full durable payload"
    );
    drop(recovered);
    drop(router);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn failed_receipt_append_hides_values_fences_writes_and_recovers_from_intent() {
    let root = root("io");
    let path = root.join("kv");
    let mut router = InterruptAfterCommit {
        inner: router(&root),
        kill_boundary: false,
        fail_path: Some(path.clone()),
    };
    let mut kv = DurableKvEngine::open(&path, Epoch(1)).unwrap();
    let first = batch(1, kv.snapshot(&router), b"value");
    assert!(
        kv.commit_batch(&mut router, PlacementGroupId(1), first.clone())
            .is_err()
    );
    assert!(kv.get(b"key").is_none());
    std::fs::remove_file(&path).unwrap();
    std::fs::rename(path.with_extension("displaced"), &path).unwrap();
    assert!(
        kv.commit_batch(&mut router, PlacementGroupId(1), first.clone())
            .unwrap_err()
            .message
            .contains("reopen")
    );
    drop(kv);
    let mut kv = DurableKvEngine::open(&path, Epoch(1)).unwrap();
    kv.reconcile(&mut router).unwrap();
    assert_eq!(kv.get(b"key").unwrap().value, b"value");
    assert_eq!(router.latest_version(), CommitVersion(1));
    drop(kv);
    drop(router);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn receipts_reject_conflicting_retries_and_do_not_resurrect_compacted_history() {
    let root = root("receipts");
    let mut router = router(&root);
    let mut kv = DurableKvEngine::open(root.join("kv"), Epoch(1)).unwrap();
    let first = batch(1, kv.snapshot(&router), b"first");
    let first_result = kv
        .commit_batch(&mut router, PlacementGroupId(1), first.clone())
        .unwrap();
    let second = batch(2, kv.snapshot(&router), b"second");
    kv.commit_batch(&mut router, PlacementGroupId(1), second)
        .unwrap();
    let changed = batch(1, kv.snapshot(&router), b"different");
    assert!(
        kv.commit_batch(&mut router, PlacementGroupId(1), changed)
            .is_err()
    );
    kv.compact(PlacementGroupId(1), CommitVersion(2), &[])
        .unwrap();
    drop(kv);
    let mut kv = DurableKvEngine::open(root.join("kv"), Epoch(1)).unwrap();
    let replay = kv
        .commit_batch(&mut router, PlacementGroupId(1), first)
        .unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.version, first_result.version);
    assert_eq!(kv.get(b"key").unwrap().value, b"second");
    assert_eq!(router.latest_version(), CommitVersion(2));
    drop(kv);
    drop(router);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn replicated_batch_materialization_is_durable_and_exactly_once() {
    let root = root("apply");
    let mut kv = DurableKvEngine::open(&root, Epoch(1)).unwrap();
    let mutations = vec![KvMutation::Set {
        key: b"a".to_vec(),
        value: b"binary\0value\xff".to_vec(),
    }];
    let evidence = CommitEvidence {
        placement_group: PlacementGroupId(1),
        log_index: LogIndex(9),
        version: CommitVersion(12),
        operation_id: operation(&mutations).operation_id(),
        consistency: CommitConsistency::Strict,
    };
    let request = ClientRequestId::new("replicated:namespace", 9);
    assert!(
        !kv.apply_committed_batch(request.clone(), evidence.clone(), mutations.clone())
            .unwrap()
            .replayed
    );
    drop(kv);
    let mut kv = DurableKvEngine::open(&root, Epoch(1)).unwrap();
    assert!(
        kv.apply_committed_batch(request.clone(), evidence.clone(), mutations.clone())
            .unwrap()
            .replayed
    );
    assert_eq!(kv.changes_since(CommitVersion(0)).len(), 1);
    let mut changed = mutations;
    changed[0] = KvMutation::Clear { key: b"a".to_vec() };
    assert!(
        kv.apply_committed_batch(request, evidence, changed)
            .is_err()
    );
    drop(kv);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn duplicate_writers_and_malformed_journal_payloads_are_rejected() {
    let root = root("guards");
    let kv = DurableKvEngine::open(&root, Epoch(1)).unwrap();
    assert!(DurableKvEngine::open(&root, Epoch(1)).is_err());
    drop(kv);
    let intent = Intent {
        request: ClientRequestId::new("app", 1),
        pg: PlacementGroupId(1),
        mutations: vec![KvMutation::Clear { key: b"a".to_vec() }],
    };
    let bytes = encode_intent(&intent).unwrap();
    for end in 0..bytes.len() {
        if end >= INTENT.len() {
            assert!(decode_intent(intent.pg, &bytes[..end]).is_err());
        }
    }
    let mut corrupt_count =
        encode_kv_wal_payload(CommitVersion(1), LogIndex(1), &intent.mutations).unwrap();
    corrupt_count[22..26].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(decode_kv_wal_payload(&corrupt_count).is_err());
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn legacy_certificate_without_values_cannot_authorize_retry_payload() {
    let root = root("legacy-gap");
    let mut router = router(&root);
    let mutations = vec![KvMutation::Set {
        key: b"key".to_vec(),
        value: b"unproven retry bytes".to_vec(),
    }];
    let request = ClientRequestId::new("legacy-client", 1);
    FabricRouterPort::commit_request(
        &mut router,
        PlacementGroupId(1),
        request.clone(),
        operation(&mutations),
    )
    .unwrap();
    let mut kv = DurableKvEngine::open(root.join("kv"), Epoch(1)).unwrap();
    let batch = KvBatch {
        client_request: Some(request),
        snapshot: kv.snapshot(&router),
        mutations,
    };
    let error = kv
        .commit_batch(&mut router, PlacementGroupId(1), batch)
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::CorruptionDetected);
    assert!(kv.get(b"key").is_none());
    assert_eq!(kv.pending_request_count(), 0);
    drop(kv);
    drop(router);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn unresolved_intent_without_protocol_commit_reconciles_before_later_writes() {
    let root = root("prepare-only");
    let mut router = router(&root);
    let mut kv = DurableKvEngine::open(root.join("kv"), Epoch(1)).unwrap();
    let intent = Intent {
        request: ClientRequestId::new("app", 7),
        pg: PlacementGroupId(1),
        mutations: vec![KvMutation::Set {
            key: b"key".to_vec(),
            value: b"original".to_vec(),
        }],
    };
    kv.append_journal(intent.pg, &encode_intent(&intent).unwrap())
        .unwrap();
    drop(kv);
    let mut kv = DurableKvEngine::open(root.join("kv"), Epoch(1)).unwrap();
    assert!(kv.get(b"key").is_none());
    let wrong = batch(7, kv.snapshot(&router), b"wrong");
    assert!(
        kv.commit_batch(&mut router, PlacementGroupId(1), wrong)
            .is_err()
    );
    assert_eq!(router.latest_version(), CommitVersion(0));
    kv.reconcile(&mut router).unwrap();
    assert_eq!(kv.get(b"key").unwrap().value, b"original");
    assert_eq!(router.latest_version(), CommitVersion(1));
    drop(kv);
    drop(router);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn stale_router_cannot_create_an_intent_over_materialized_state() {
    let root = root("stale-router");
    let mut kv = DurableKvEngine::open(root.join("kv"), Epoch(1)).unwrap();
    let mutations = vec![KvMutation::Clear {
        key: b"key".to_vec(),
    }];
    let evidence = CommitEvidence {
        placement_group: PlacementGroupId(1),
        log_index: LogIndex(9),
        version: CommitVersion(12),
        operation_id: operation(&mutations).operation_id(),
        consistency: CommitConsistency::Strict,
    };
    kv.apply_committed_batch(ClientRequestId::new("replica", 9), evidence, mutations)
        .unwrap();
    let mut stale = router(&root);
    let next = batch(1, kv.snapshot(&stale), b"must not commit");
    assert!(
        kv.commit_batch(&mut stale, PlacementGroupId(1), next)
            .unwrap_err()
            .message
            .contains("router is behind")
    );
    assert_eq!(kv.pending_request_count(), 0);
    assert_eq!(stale.latest_version(), CommitVersion(0));
    drop(kv);
    drop(stale);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn bound_kv_replication_materializes_values_clears_and_receipts() {
    let root = root("replicated-kv");
    let mut router = router(&root);
    let mut leader =
        DurableKvEngine::open_replicated(root.join("leader"), Epoch(1), "tenant-a").unwrap();
    let snapshot = leader.snapshot(&router);
    leader
        .commit_batch(
            &mut router,
            PlacementGroupId(1),
            batch(1, snapshot, b"first"),
        )
        .unwrap();
    let snapshot = leader.snapshot(&router);
    leader
        .commit_batch(
            &mut router,
            PlacementGroupId(1),
            batch(2, snapshot, b"second"),
        )
        .unwrap();
    let snapshot = leader.snapshot(&router);
    leader
        .commit_batch(
            &mut router,
            PlacementGroupId(1),
            KvBatch {
                client_request: Some(ClientRequestId::new("app", 3)),
                snapshot,
                mutations: vec![KvMutation::Clear {
                    key: b"key".to_vec(),
                }],
            },
        )
        .unwrap();
    let commands = router.committed_application_commands();
    assert_eq!(commands.len(), 3);
    let mut follower =
        DurableKvEngine::open_replicated(root.join("follower"), Epoch(1), "tenant-a").unwrap();
    for command in &commands {
        follower.apply_application_command(command).unwrap();
    }
    assert!(follower.get(b"key").is_none());
    assert_eq!(
        follower
            .get_at(
                b"key",
                ReadSnapshot {
                    version: CommitVersion(2)
                }
            )
            .unwrap()
            .value,
        b"second"
    );
    assert_eq!(follower.changes_since(CommitVersion(0)).len(), 3);
    drop(follower);
    assert!(DurableKvEngine::open_replicated(root.join("follower"), Epoch(1), "tenant-b").is_err());
    let mut follower =
        DurableKvEngine::open_replicated(root.join("follower"), Epoch(1), "tenant-a").unwrap();
    assert!(
        follower
            .apply_application_command(&commands[0])
            .unwrap()
            .replayed
    );
    assert!(follower.get(b"key").is_none());
    let mut wrong = commands[0].clone();
    wrong.application.namespace = "tenant-b".into();
    assert!(follower.apply_application_command(&wrong).is_err());
    let snapshot = leader.snapshot(&router);
    assert!(
        leader
            .commit_batch(
                &mut router,
                PlacementGroupId(1),
                batch(1, snapshot, b"first")
            )
            .unwrap()
            .replayed
    );
    assert_eq!(router.latest_version(), CommitVersion(3));
}

#[test]
fn invalid_replicated_command_cannot_strand_a_local_intent() {
    let root = root("replicated-bounds");
    let mut router = router(&root);
    let mut kv = DurableKvEngine::open_replicated(root.join("kv"), Epoch(1), "tenant").unwrap();
    let snapshot = kv.snapshot(&router);
    let large = vec![0; 1024 * 1024];
    assert!(
        kv.commit_batch(&mut router, PlacementGroupId(1), batch(1, snapshot, &large))
            .is_err()
    );
    assert_eq!(kv.pending_request_count(), 0);
    assert!(
        kv.commit_batch(
            &mut router,
            PlacementGroupId(1),
            batch(2, snapshot, b"valid")
        )
        .is_ok()
    );
    assert_eq!(router.latest_version(), CommitVersion(1));
}

#[test]
fn legacy_nonempty_kv_requires_explicit_migration_to_replication() {
    let root = root("legacy-binding");
    let mut router = router(&root);
    let mut kv = DurableKvEngine::open(root.join("kv"), Epoch(1)).unwrap();
    let snapshot = kv.snapshot(&router);
    kv.commit_batch(
        &mut router,
        PlacementGroupId(1),
        batch(1, snapshot, b"legacy"),
    )
    .unwrap();
    drop(kv);
    assert!(DurableKvEngine::open_replicated(root.join("kv"), Epoch(1), "tenant").is_err());
    let kv = DurableKvEngine::open(root.join("kv"), Epoch(1)).unwrap();
    assert_eq!(kv.get(b"key").unwrap().value, b"legacy");
}

#[test]
fn replicated_retry_cannot_create_a_command_on_a_rolled_back_router() {
    let root = root("mismatched-router");
    let mut original = router(&root);
    let mut kv = DurableKvEngine::open_replicated(root.join("kv"), Epoch(1), "tenant").unwrap();
    let snapshot = kv.snapshot(&original);
    kv.commit_batch(
        &mut original,
        PlacementGroupId(1),
        batch(1, snapshot, b"original"),
    )
    .unwrap();
    let mut empty = router(&root.join("empty"));
    assert!(
        kv.commit_batch(
            &mut empty,
            PlacementGroupId(1),
            batch(1, snapshot, b"original")
        )
        .is_err()
    );
    assert_eq!(empty.latest_version(), CommitVersion(0));
    assert!(empty.committed_application_commands().is_empty());
}
