//! New-API contract tests; these do not claim an old-API red reproduction.
use super::*;
use std::sync::atomic::AtomicU64;

fn root(label: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    loop {
        let path = std::env::temp_dir().join(format!(
            "rsf-bounded-prefix-{label}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        match fs::create_dir(&path) {
            Ok(()) => return path,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => panic!("cannot reserve fixture root: {error}"),
        }
    }
}
fn limits() -> VsrPrefixLimits {
    VsrPrefixLimits {
        max_records: 128,
        max_encoded_bytes: 1024 * 1024,
    }
}
fn open(path: &Path) -> DurableVsrCell {
    DurableVsrCell::open(
        path,
        PlacementGroupId(1),
        Epoch(1),
        NodeId(0),
        [NodeId(0), NodeId(1), NodeId(2)],
    )
    .unwrap()
}
fn prepare(cell: &mut DurableVsrCell, id: u64, payload: &[u8]) -> VsrPrepare {
    let VsrDecision::Prepared(value) = cell
        .prepare(
            NodeId(0),
            VsrOperation::ReplicatedCommand {
                operation_id: OperationId(id),
                payload: payload.to_vec(),
            },
            payload.len() as u32,
        )
        .unwrap()
    else {
        panic!("prepare rejected");
    };
    value
}
fn commit(cell: &mut DurableVsrCell, index: LogIndex) {
    cell.acknowledge(NodeId(1), cell.view(), index);
    assert!(matches!(
        cell.commit(NodeId(0), index).unwrap(),
        VsrDecision::Committed(_)
    ));
}
fn published(root: &Path, local: u64, snapshot: &VsrHistorySnapshot) -> LearnerHistoryStore {
    let mut owner =
        LearnerHistoryStore::open(root, PlacementGroupId(1), NodeId(local), NodeId(0)).unwrap();
    let id = snapshot.descriptor.id().unwrap();
    owner.begin(snapshot.descriptor.clone()).unwrap();
    for (ordinal, record) in snapshot.records.iter().enumerate() {
        owner.append(id, ordinal as u64, record.clone()).unwrap();
    }
    owner.finish(id).unwrap();
    owner
}
struct Fixture {
    root: PathBuf,
    source: DurableVsrCell,
    three: LearnerHistoryStore,
    four: LearnerHistoryStore,
}
impl Fixture {
    fn new(label: &str) -> Self {
        let root = root(label);
        let mut source = open(&root.join("source"));
        let genesis = prepare(&mut source, 1, b"Genesis");
        commit(&mut source, genesis.index);
        source
            .remember_client_request(VsrClientRequestId::new("original", 1), genesis.index)
            .unwrap();
        let snapshot = source.history_snapshot().unwrap();
        let three = published(&root.join("three"), 3, &snapshot);
        let four = published(&root.join("four"), 4, &snapshot);
        Self {
            root,
            source,
            three,
            four,
        }
    }
    fn advance(&mut self) -> VsrVoterPrefixExport {
        let confirmed = prepare(&mut self.source, 2, b"Begin + certified delivery");
        prepare(&mut self.source, 3, b"accepted-only suffix");
        commit(&mut self.source, confirmed.index);
        self.source
            .export_committed_prefix(LogIndex(2), limits())
            .unwrap()
    }
}
fn assert_semantics(preview: &VsrImportPreview<'_>) -> RsfResult<()> {
    assert_eq!(preview.committed_tail(), LogIndex(2));
    assert_eq!(preview.committed_count(), 2);
    assert_eq!(preview.next_index(), LogIndex(4));
    let mut committed = Vec::new();
    preview.for_each_committed(|prepare, certificate| {
        assert_eq!(prepare.index, certificate.index);
        assert_eq!(
            preview.membership_lineage_at(prepare.view)?,
            Some(preview.membership_lineage()?)
        );
        committed.push(prepare.operation.clone());
        Ok(())
    })?;
    assert_eq!(committed.len(), 2);
    assert!(
        matches!(&committed[1], VsrOperation::ReplicatedCommand { operation_id: OperationId(2), payload } if payload == b"Begin + certified delivery")
    );
    let mut accepted = Vec::new();
    preview.for_each_accepted_tail(|prepare| {
        accepted.push(prepare.clone());
        Ok(())
    })?;
    assert_eq!(accepted.len(), 1);
    assert_eq!(accepted[0].index, LogIndex(3));
    assert!(
        matches!(&accepted[0].operation, VsrOperation::ReplicatedCommand { operation_id: OperationId(3), payload } if payload == b"accepted-only suffix")
    );
    preview.validate_live()
}
fn copy_voter(
    source: &DurableVsrCell,
    export: &VsrVoterPrefixExport,
    receiver: &mut LearnerHistoryStore,
) {
    let mut ordinal = receiver
        .begin_prefix(export.descriptor().clone(), limits())
        .unwrap();
    let id = export.descriptor().id().unwrap();
    while ordinal < export.descriptor().records {
        let batch = source.read_committed_prefix_batch(export, ordinal).unwrap();
        ordinal = receiver.append_prefix_batch(id, &batch).unwrap();
    }
}

#[test]
fn actual_published_learner_relays_after_source_loss_and_cold_partial_resume() {
    let mut fixture = Fixture::new("relay");
    let export = fixture.advance();
    copy_voter(&fixture.source, &export, &mut fixture.three);
    let id = export.descriptor().id().unwrap();
    fixture.three.finish_prefix(id, assert_semantics).unwrap();
    let prefix = fixture
        .source
        .read_committed_prefix_batch(&export, 0)
        .unwrap();
    fixture
        .four
        .begin_prefix(export.descriptor().clone(), limits())
        .unwrap();
    fixture
        .four
        .append_prefix_batch(
            id,
            &VsrHistoryBatch {
                ordinal: 0,
                records: prefix.records[..2].to_vec(),
            },
        )
        .unwrap();
    let Fixture {
        root,
        source,
        three,
        four,
    } = fixture;
    drop(source);
    drop(three);
    drop(four);
    fs::remove_dir_all(root.join("source")).unwrap();
    let three = LearnerHistoryStore::open_published_existing(
        root.join("three"),
        PlacementGroupId(1),
        NodeId(3),
        NodeId(0),
        limits(),
    )
    .unwrap();
    let mut four = LearnerHistoryStore::open_published_existing(
        root.join("four"),
        PlacementGroupId(1),
        NodeId(4),
        NodeId(0),
        limits(),
    )
    .unwrap();
    // Cold state is not a remembered policy/session. Require a fresh bounded
    // BEGIN before validated FINISH; reserved-domain routing must enforce this.
    assert!(four.finish_prefix(id, assert_semantics).is_err());
    let relay = three
        .export_committed_prefix(LogIndex(2), limits())
        .unwrap();
    assert_eq!(relay.descriptor(), export.descriptor());
    assert_eq!(relay.descriptor().probe.primary, NodeId(0));
    let mut ordinal = four
        .begin_prefix(relay.descriptor().clone(), limits())
        .unwrap();
    assert_eq!(ordinal, 2);
    while ordinal < relay.descriptor().records {
        let batch = three.read_committed_prefix_batch(&relay, ordinal).unwrap();
        ordinal = four.append_prefix_batch(id, &batch).unwrap();
    }
    assert_eq!(
        four.finish_prefix(id, assert_semantics).unwrap(),
        LogIndex(2)
    );
    assert_eq!(
        four.published
            .as_ref()
            .unwrap()
            .cell
            .request_commit(&VsrClientRequestId::new("original", 1)),
        Some(LogIndex(1))
    );
    assert_eq!(
        four.committed_entries().unwrap(),
        three.committed_entries().unwrap()
    );
    assert!(
        four.finish(id).is_err(),
        "bounded owner cannot bypass its validator on warm retry"
    );
    four.finish_prefix(id, assert_semantics).unwrap();
    drop(four);
    drop(three);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn promised_source_exports_earlier_prefix_without_releasing_promise_or_accepted_suffix() {
    let mut fixture = Fixture::new("promise");
    let before = fixture.advance();
    let request = VsrViewChangeRequest {
        pg: PlacementGroupId(1),
        previous_view: Epoch(1),
        target_view: Epoch(2),
        candidate: NodeId(1),
        voters: vec![NodeId(0), NodeId(1), NodeId(2)],
    };
    fixture
        .source
        .begin_view_change(NodeId(0), NodeId(1), request.clone())
        .unwrap();
    assert!(fixture.source.primary_probe().is_err());
    assert!(
        fixture
            .source
            .read_committed_prefix_batch(&before, 0)
            .is_err(),
        "changed owner invalidates old export"
    );
    let export = fixture
        .source
        .export_committed_prefix(LogIndex(2), limits())
        .unwrap();
    assert_eq!(export.descriptor(), before.descriptor());
    copy_voter(&fixture.source, &export, &mut fixture.three);
    fixture
        .three
        .finish_prefix(export.descriptor().id().unwrap(), assert_semantics)
        .unwrap();
    assert_eq!(fixture.source.view_change_promise(), Some(&request));
    assert_eq!(fixture.source.next_index(), LogIndex(4));
    assert!(fixture.source.primary_probe().is_err());
}

#[test]
fn clean_semantic_rejection_retains_prior_authority_and_exact_resumable_bytes() {
    let mut fixture = Fixture::new("semantic");
    let export = fixture.advance();
    copy_voter(&fixture.source, &export, &mut fixture.four);
    let old = fs::read(fixture.root.join("four/ACTIVE")).unwrap();
    let id = export.descriptor().id().unwrap();
    let error = fixture
        .four
        .finish_prefix(id, |preview| {
            assert_semantics(preview)?;
            Err(invalid_input("domain rejected candidate"))
        })
        .unwrap_err();
    assert_eq!(error.message, "domain rejected candidate");
    assert_eq!(fs::read(fixture.root.join("four/ACTIVE")).unwrap(), old);
    assert_eq!(fixture.four.committed_entries().unwrap().len(), 1);
    assert!(fixture.four.finish(id).is_err());
    assert!(fixture.four.begin(export.descriptor().clone()).is_err());
    assert_eq!(
        fixture
            .four
            .begin_prefix(export.descriptor().clone(), limits())
            .unwrap(),
        export.descriptor().records
    );
    fixture.four.finish_prefix(id, assert_semantics).unwrap();
}

#[test]
fn callback_success_or_error_cannot_hide_replaced_prior_or_candidate_custody() {
    for candidate in [false, true] {
        for semantic_error in [false, true] {
            let mut fixture = Fixture::new("callback-fault");
            let export = fixture.advance();
            copy_voter(&fixture.source, &export, &mut fixture.four);
            let id = export.descriptor().id().unwrap();
            let active = fixture.root.join("four/ACTIVE");
            let old = fs::read(&active).unwrap();
            let fault = if candidate {
                fixture.four.directory(&id).join("cell/MANIFEST")
            } else {
                active.clone()
            };
            let result = fixture.four.finish_prefix(id, |preview| {
                assert_semantics(preview)?;
                let bytes = fs::read(&fault).unwrap();
                let moved = fault.with_extension("saved");
                fs::rename(&fault, &moved).unwrap();
                fs::write(&fault, &bytes).unwrap(); // same bytes, different file identity
                fs::remove_file(moved).unwrap();
                if semantic_error {
                    Err(invalid_input("semantic callback rejection"))
                } else {
                    Ok(())
                }
            });
            let error = result.unwrap_err();
            assert_ne!(
                error.message, "semantic callback rejection",
                "physical loss must dominate semantic Err"
            );
            assert_eq!(fs::read(&active).unwrap(), old);
            let error = fixture.four.published_state_token().unwrap_err();
            assert!(error.message.contains("learner durability failed"));
            assert!(fixture.four.finish_prefix(id, assert_semantics).is_err());
        }
    }
}

#[test]
fn validator_unwind_leaves_live_owner_fenced_and_cold_prior_publication_intact() {
    let mut fixture = Fixture::new("unwind");
    let export = fixture.advance();
    copy_voter(&fixture.source, &export, &mut fixture.four);
    let id = export.descriptor().id().unwrap();
    let old = fs::read(fixture.root.join("four/ACTIVE")).unwrap();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        fixture.four.finish_prefix(id, |preview| {
            assert_semantics(preview)?;
            panic!("application validator panicked");
        })
    }));
    assert!(result.is_err());
    assert!(
        fixture
            .four
            .committed_entries()
            .unwrap_err()
            .message
            .contains("learner durability failed")
    );
    assert_eq!(fs::read(fixture.root.join("four/ACTIVE")).unwrap(), old);
    let path = fixture.root.join("four");
    drop(fixture.four);
    let mut cold = LearnerHistoryStore::open_published_existing(
        path,
        PlacementGroupId(1),
        NodeId(4),
        NodeId(0),
        limits(),
    )
    .unwrap();
    assert_eq!(cold.committed_entries().unwrap().len(), 1);
    assert_eq!(
        cold.begin_prefix(export.descriptor().clone(), limits())
            .unwrap(),
        export.descriptor().records
    );
    cold.finish_prefix(id, assert_semantics).unwrap();
}

#[test]
fn bounds_fail_before_append_and_exact_duplicate_retries_do_not_reallocate_log_indices() {
    let mut fixture = Fixture::new("limits");
    let export = fixture.advance();
    let id = export.descriptor().id().unwrap();
    let batch = fixture
        .source
        .read_committed_prefix_batch(&export, 0)
        .unwrap();
    let too_small = VsrPrefixLimits {
        max_encoded_bytes: export.encoded_bytes() - 1,
        ..limits()
    };
    fixture
        .four
        .begin_prefix(export.descriptor().clone(), too_small)
        .unwrap();
    assert!(fixture.four.append_prefix_batch(id, &batch).is_err());
    assert_eq!(fixture.four.stage.as_ref().unwrap().count, 0);
    assert_eq!(
        fixture
            .four
            .begin_prefix(export.descriptor().clone(), limits())
            .unwrap(),
        0
    );
    let count = fixture.four.append_prefix_batch(id, &batch).unwrap();
    let bytes = fixture.four.prefix_context.as_ref().unwrap().bytes;
    assert_eq!(fixture.four.append_prefix_batch(id, &batch).unwrap(), count);
    assert_eq!(fixture.four.prefix_context.as_ref().unwrap().bytes, bytes);
    let mut changed = batch.clone();
    changed.records[1].payload.push(0);
    assert!(fixture.four.append_prefix_batch(id, &changed).is_err());
    assert_eq!(fixture.four.stage.as_ref().unwrap().count, count);
    fixture.four.finish_prefix(id, assert_semantics).unwrap();
    assert!(matches!(
        fixture
            .source
            .export_committed_prefix(LogIndex(2), too_small),
        Err(VsrPrefixExportError::LimitExceeded)
    ));
    assert!(
        fixture.source.primary_probe().is_ok(),
        "capacity refusal does not fence valid source"
    );
}

#[test]
fn strict_cold_open_rejects_budget_loss_or_alias_without_recreating_authority() {
    let fixture = Fixture::new("strict-open");
    let path = fixture.root.join("four");
    let active = fs::read(path.join("ACTIVE")).unwrap();
    let id: [u8; 32] = active.clone().try_into().unwrap();
    let directory = fixture.four.directory(&id);
    drop(fixture.four);
    let tiny = VsrPrefixLimits {
        max_records: 1,
        ..limits()
    };
    assert!(
        LearnerHistoryStore::open_published_existing(
            &path,
            PlacementGroupId(1),
            NodeId(4),
            NodeId(0),
            tiny
        )
        .is_err()
    );
    assert_eq!(fs::read(path.join("ACTIVE")).unwrap(), active);
    let manifest = directory.join("cell/MANIFEST");
    let saved = fs::read(&manifest).unwrap();
    fs::remove_file(&manifest).unwrap();
    assert!(
        LearnerHistoryStore::open_published_existing(
            &path,
            PlacementGroupId(1),
            NodeId(4),
            NodeId(0),
            limits()
        )
        .is_err()
    );
    assert!(!manifest.exists());
    fs::write(&manifest, saved).unwrap();
    #[cfg(unix)]
    {
        fs::rename(path.join("ACTIVE"), path.join("ACTIVE-real")).unwrap();
        std::os::unix::fs::symlink("ACTIVE-real", path.join("ACTIVE")).unwrap();
        assert!(
            LearnerHistoryStore::open_published_existing(
                path.join("."),
                PlacementGroupId(1),
                NodeId(4),
                NodeId(0),
                limits()
            )
            .is_err()
        );
        assert!(
            fs::symlink_metadata(path.join("ACTIVE"))
                .unwrap()
                .file_type()
                .is_symlink()
        );
        fs::remove_file(path.join("ACTIVE")).unwrap();
        fs::rename(path.join("ACTIVE-real"), path.join("ACTIVE")).unwrap();
    }
    let cold = LearnerHistoryStore::open_published_existing(
        &path,
        PlacementGroupId(1),
        NodeId(4),
        NodeId(0),
        limits(),
    )
    .unwrap();
    assert_eq!(cold.committed_entries().unwrap().len(), 1);
}

fn copy_records(receiver: &mut LearnerHistoryStore, snapshot: &VsrHistorySnapshot) -> [u8; 32] {
    let id = snapshot.descriptor.id().unwrap();
    let mut ordinal = receiver
        .begin_prefix(snapshot.descriptor.clone(), limits())
        .unwrap();
    while ordinal < snapshot.records.len() as u64 {
        let batch = VsrHistoryBatch::pack(ordinal, &snapshot.records[ordinal as usize..]).unwrap();
        ordinal = receiver.append_prefix_batch(id, &batch).unwrap();
    }
    id
}

#[test]
fn invalid_peer_certificate_rejects_before_owner_handoff_and_a_valid_begin_can_follow() {
    let mut fixture = Fixture::new("invalid-certificate");
    let export = fixture.advance();
    let mut records = fixture
        .source
        .read_committed_prefix_batch(&export, 0)
        .unwrap()
        .records;
    let record = records
        .iter_mut()
        .rev()
        .find(|record| record.kind == WalRecordKind::Commit)
        .unwrap();
    let mut certificate = VsrCommit::decode(&record.payload).unwrap();
    certificate.acknowledgements = 1;
    certificate.certificate.signers = [NodeId(0)].into_iter().collect();
    record.payload = certificate.encode().unwrap();
    let mut descriptor = export.descriptor().clone();
    descriptor.digest = transfer::history_digest(&records).unwrap();
    let bad = VsrHistorySnapshot {
        descriptor,
        records,
    };
    let id = copy_records(&mut fixture.four, &bad);
    let old = fs::read(fixture.root.join("four/ACTIVE")).unwrap();
    let mut invoked = false;
    assert!(
        fixture
            .four
            .finish_prefix(id, |_| {
                invoked = true;
                Ok(())
            })
            .is_err()
    );
    assert!(!invoked, "invalid VSR proof cannot reach domain validation");
    assert_eq!(fs::read(fixture.root.join("four/ACTIVE")).unwrap(), old);
    assert_eq!(fixture.four.committed_entries().unwrap().len(), 1);
    assert!(
        fixture.four.stage.is_some(),
        "semantic replay retained actual stage ownership"
    );
    copy_voter(&fixture.source, &export, &mut fixture.four);
    fixture
        .four
        .finish_prefix(export.descriptor().id().unwrap(), assert_semantics)
        .unwrap();
}

#[test]
fn valid_candidate_cannot_erase_prior_receipt_and_clean_rejection_can_resume_or_switch() {
    let mut fixture = Fixture::new("receipt-conflict");
    let export = fixture.advance();
    let mut records = fixture
        .source
        .read_committed_prefix_batch(&export, 0)
        .unwrap()
        .records;
    let initial = records.len();
    records.retain(|record| !record.payload.starts_with(VSR_CLIENT_REQUEST_MAGIC));
    assert_eq!(
        records.len() + 1,
        initial,
        "fixture must remove the actual prior receipt"
    );
    let mut descriptor = export.descriptor().clone();
    descriptor.records = records.len() as u64;
    descriptor.digest = transfer::history_digest(&records).unwrap();
    let bad = VsrHistorySnapshot {
        descriptor,
        records,
    };
    let id = copy_records(&mut fixture.four, &bad);
    let old = fs::read(fixture.root.join("four/ACTIVE")).unwrap();
    for _ in 0..2 {
        let mut invoked = false;
        let error = fixture
            .four
            .finish_prefix(id, |_| {
                invoked = true;
                Ok(())
            })
            .unwrap_err();
        assert!(error.message.contains("conflicts with published commits"));
        assert!(!invoked);
        assert_eq!(fs::read(fixture.root.join("four/ACTIVE")).unwrap(), old);
        assert_eq!(fixture.four.committed_entries().unwrap().len(), 1);
        assert_eq!(
            fixture
                .four
                .begin_prefix(bad.descriptor.clone(), limits())
                .unwrap(),
            bad.descriptor.records
        );
    }
    copy_voter(&fixture.source, &export, &mut fixture.four);
    fixture
        .four
        .finish_prefix(export.descriptor().id().unwrap(), assert_semantics)
        .unwrap();
}

#[test]
fn cached_prefix_callback_error_and_unwind_obey_same_physical_custody_contract() {
    for fault in [0, 1, 2] {
        let mut fixture = Fixture::new("cached-callback");
        let export = fixture.advance();
        copy_voter(&fixture.source, &export, &mut fixture.four);
        let id = export.descriptor().id().unwrap();
        fixture.four.finish_prefix(id, assert_semantics).unwrap();
        let active = fixture.root.join("four/ACTIVE");
        let before = fs::read(&active).unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            fixture.four.finish_prefix(id, |preview| {
                assert_semantics(preview)?;
                if fault == 2 {
                    panic!("cached validator panicked");
                }
                if fault == 1 {
                    fs::rename(&active, active.with_extension("old")).unwrap();
                    fs::write(&active, &before).unwrap();
                    fs::remove_file(active.with_extension("old")).unwrap();
                }
                Err(invalid_input("cached semantic rejection"))
            })
        }));
        assert_eq!(fs::read(&active).unwrap(), before);
        if fault == 0 {
            assert_eq!(
                result.unwrap().unwrap_err().message,
                "cached semantic rejection"
            );
            assert_eq!(fixture.four.committed_entries().unwrap().len(), 2);
            fixture.four.finish_prefix(id, assert_semantics).unwrap();
        } else {
            if fault == 1 {
                assert_ne!(
                    result.unwrap().unwrap_err().message,
                    "cached semantic rejection"
                );
            } else {
                assert!(result.is_err());
            }
            assert!(
                fixture
                    .four
                    .committed_entries()
                    .unwrap_err()
                    .message
                    .contains("learner durability failed")
            );
        }
    }
}

#[test]
fn learner_publication_guard_rejects_replaced_zero_byte_held_lock() {
    for inner in [false, true] {
        let mut fixture = Fixture::new("held-lock");
        let export = fixture.advance();
        copy_voter(&fixture.source, &export, &mut fixture.four);
        let id = export.descriptor().id().unwrap();
        let active = fs::read(fixture.root.join("four/ACTIVE")).unwrap();
        let prior = fixture.four.published.as_ref().unwrap();
        let lock = if inner {
            fixture
                .four
                .directory(&prior.id)
                .join("cell/.vsr-writer.lock")
        } else {
            fixture.root.join("four/.learner.lock")
        };
        let held = lock.with_extension("held");
        fs::rename(&lock, &held).unwrap();
        fs::write(&lock, []).unwrap();
        assert!(fixture.four.finish_prefix(id, assert_semantics).is_err());
        assert_eq!(fs::read(fixture.root.join("four/ACTIVE")).unwrap(), active);
        fs::remove_file(&lock).unwrap();
        fs::rename(&held, &lock).unwrap();
        // Restoring the exact original inode cannot revive an owner that
        // already observed loss of either its outer or inner held lock.
        assert!(
            fixture
                .four
                .committed_entries()
                .unwrap_err()
                .message
                .contains("learner durability failed")
        );
    }
}

#[test]
fn installed_commit_without_ordinary_boundary_requires_installation_export() {
    let path = root("installation-boundary");
    let mut primary = open(&path.join("zero"));
    let mut first = open(&path.join("one"));
    let mut second = open(&path.join("two"));
    let prepared = prepare(&mut primary, 1, b"committed only at short history");
    first
        .receive_prepare(NodeId(1), NodeId(0), prepared.clone())
        .unwrap();
    second
        .receive_prepare(NodeId(2), NodeId(0), prepared.clone())
        .unwrap();
    primary.acknowledge(NodeId(1), Epoch(1), prepared.index);
    let VsrDecision::Committed(certificate) = primary.commit(NodeId(0), prepared.index).unwrap()
    else {
        panic!("commit");
    };
    first
        .receive_commit(NodeId(1), NodeId(0), certificate)
        .unwrap();
    let next = prepare(&mut primary, 2, b"longer accepted history");
    second.receive_prepare(NodeId(2), NodeId(0), next).unwrap();
    let request = VsrViewChangeRequest {
        pg: PlacementGroupId(1),
        previous_view: Epoch(1),
        target_view: Epoch(2),
        candidate: NodeId(1),
        voters: vec![NodeId(0), NodeId(1), NodeId(2)],
    };
    let snapshots = [
        first
            .begin_view_change(NodeId(1), NodeId(1), request.clone())
            .unwrap(),
        second
            .begin_view_change(NodeId(2), NodeId(1), request)
            .unwrap(),
    ];
    let plan = VsrViewInstallation::select(&snapshots).unwrap();
    // The selected longer history only accepted index1; installation learns its
    // certificate from the shorter reporter, without inventing a Commit record.
    assert!(
        !plan
            .records
            .iter()
            .any(|record| record.kind == WalRecordKind::Commit)
    );
    first.install_view(&plan).unwrap();
    assert_eq!(first.committed_tail(), LogIndex(1));
    assert!(matches!(
        first.export_committed_prefix(LogIndex(1), limits()),
        Err(VsrPrefixExportError::RequiresInstallationBoundary)
    ));
    assert!(
        first.primary_probe().is_ok(),
        "unsupported representation does not fence valid owner"
    );
}

fn active_segment_lock(cell: &Path) -> PathBuf {
    let recovered = SegmentStore::recover_existing(cell).unwrap();
    cell.join("segments")
        .join(&recovered.segments.last().unwrap().file_name)
        .with_extension("lock")
}

#[test]
fn cold_bounded_resume_never_repairs_missing_sidecars_and_explicit_cancel_allows_fresh_copy() {
    for mode in [0, 1, 2, 3] {
        let mut fixture = Fixture::new("sidecar-boundary");
        let export = fixture.advance();
        let id = export.descriptor().id().unwrap();
        assert_eq!(
            fixture
                .four
                .begin_prefix(export.descriptor().clone(), limits())
                .unwrap(),
            0
        );
        if mode == 2 {
            let first = fixture
                .source
                .read_committed_prefix_batch(&export, 0)
                .unwrap();
            fixture
                .four
                .append_prefix_batch(
                    id,
                    &VsrHistoryBatch {
                        ordinal: 0,
                        records: first.records[..1].to_vec(),
                    },
                )
                .unwrap();
        }
        let cell = if mode == 3 {
            fixture
                .four
                .directory(&fixture.four.published.as_ref().unwrap().id)
                .join("cell")
        } else {
            fixture.four.directory(&id).join("cell")
        };
        let sidecar = active_segment_lock(&cell);
        let active = fs::read(fixture.root.join("four/ACTIVE")).unwrap();
        let manifest = fs::read(cell.join("MANIFEST")).unwrap();
        if mode == 1 {
            fs::remove_file(&sidecar).unwrap();
            let error = fixture
                .four
                .begin_prefix(export.descriptor().clone(), limits())
                .unwrap_err();
            assert!(!error.message.is_empty());
            assert!(!sidecar.exists(), "warm loss must not be repaired");
            assert!(
                fixture
                    .four
                    .committed_entries()
                    .unwrap_err()
                    .message
                    .contains("learner durability failed")
            );
            assert_eq!(fs::read(fixture.root.join("four/ACTIVE")).unwrap(), active);
            continue;
        }
        drop(fixture.four);
        fs::remove_file(&sidecar).unwrap();
        let cold = LearnerHistoryStore::open_published_existing(
            fixture.root.join("four"),
            PlacementGroupId(1),
            NodeId(4),
            NodeId(0),
            limits(),
        );
        if mode == 3 {
            assert!(
                cold.is_err(),
                "published lock loss cannot use provisional factory"
            );
            assert!(!sidecar.exists());
        } else {
            let mut cold = cold.unwrap();
            let resume = cold.begin_prefix(export.descriptor().clone(), limits());
            if mode == 0 {
                assert!(
                    resume.is_err(),
                    "ordinary strict existing resume does not complete a missing sidecar"
                );
                assert!(!sidecar.exists());
                assert_eq!(fs::read(cell.join("MANIFEST")).unwrap(), manifest);
                drop(cold);
                let mut cold = LearnerHistoryStore::open_published_existing(
                    fixture.root.join("four"),
                    PlacementGroupId(1),
                    NodeId(4),
                    NodeId(0),
                    limits(),
                )
                .unwrap();
                let active_descriptor = cold.published.as_ref().unwrap().descriptor.clone();
                cold.begin_prefix(active_descriptor, limits()).unwrap();
                assert!(
                    !cold.directory(&id).exists(),
                    "explicit ACTIVE begin retires provisional B before a fresh factory"
                );
                copy_voter(&fixture.source, &export, &mut cold);
                cold.finish_prefix(id, assert_semantics).unwrap();
            } else {
                assert!(
                    resume.is_err(),
                    "nonempty prefix cannot recreate a lost lock"
                );
                assert!(!sidecar.exists());
                assert_eq!(fs::read(cell.join("MANIFEST")).unwrap(), manifest);
            }
        }
        if mode != 0 {
            assert_eq!(fs::read(fixture.root.join("four/ACTIVE")).unwrap(), active);
        }
    }
}

#[test]
fn original_bootstrap_prefix_is_certified_but_legacy_reconfigure_remains_rejected() {
    let path = root("lineage-contract");
    let mut cell = open(&path.join("source"));
    let first = prepare(&mut cell, 1, b"certified bootstrap command");
    commit(&mut cell, first.index);
    let lineage = cell.membership_lineage_at(Epoch(1)).unwrap().unwrap();
    assert_eq!(lineage.plan, None);
    assert_eq!(lineage.voters, vec![NodeId(0), NodeId(1), NodeId(2)]);
    let prefix = cell.export_committed_prefix(LogIndex(1), limits()).unwrap();
    assert_eq!(prefix.through(), LogIndex(1));
    assert_eq!(prefix.descriptor().probe.voters, lineage.voters);
    assert!(matches!(
        cell.reconfigure(Epoch(2), NodeId(0), [NodeId(0), NodeId(1)]),
        VsrDecision::MembershipChanged { .. }
    ));
    let next = prepare(&mut cell, 2, b"legacy membership command");
    commit(&mut cell, next.index);
    assert!(cell.membership_lineage_at(Epoch(2)).is_err());
    let error = cell
        .export_committed_prefix(LogIndex(2), limits())
        .unwrap_err();
    assert!(matches!(
        error,
        VsrPrefixExportError::Failure(RsfError {
            code: ErrorCode::InvalidInput,
            ..
        })
    ));
    assert!(
        cell.primary_probe().is_ok(),
        "legacy API behavior remains usable; rejected export is not custody loss"
    );
    drop(cell);
    fs::remove_dir_all(path).unwrap();
}

mod active_retry;

mod owned_handoff;

mod owned_policy;

#[cfg(unix)]
mod discard;

#[cfg(unix)]
mod discard_contract;

#[cfg(unix)]
mod discard_caller;
