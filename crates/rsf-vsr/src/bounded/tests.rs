use super::*;
use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};
use std::sync::atomic::{AtomicU64, Ordering};

pub(crate) fn limits() -> VsrPrefixLimits {
    VsrPrefixLimits {
        max_records: 4096,
        max_encoded_bytes: 16 * 1024 * 1024,
    }
}
pub(crate) fn root(label: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    loop {
        let root = std::env::temp_dir().join(format!(
            "rsf-bounded-voter-{label}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        match fs::create_dir(&root) {
            Ok(()) => return root,
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => panic!("cannot reserve fixture root: {error}"),
        }
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
fn prepare(cell: &mut DurableVsrCell, id: u64) -> VsrPrepare {
    let payload = format!("bounded voter operation {id}").into_bytes();
    let VsrDecision::Prepared(prepare) = cell
        .prepare(
            NodeId(0),
            VsrOperation::ReplicatedCommand {
                operation_id: OperationId(id),
                payload: payload.clone(),
            },
            payload.len() as u32,
        )
        .unwrap()
    else {
        panic!("prepare rejected")
    };
    prepare
}
fn fixture(path: &Path) -> (DurableVsrCell, DurableVsrCell) {
    let mut source = open(&path.join("source"));
    let mut follower = open(&path.join("follower"));
    let first = prepare(&mut source, 1);
    follower
        .receive_prepare(NodeId(1), NodeId(0), first.clone())
        .unwrap();
    source.acknowledge(NodeId(1), first.view, first.index);
    let VsrDecision::Committed(commit) = source.commit(NodeId(0), first.index).unwrap() else {
        panic!("commit rejected")
    };
    follower
        .receive_commit(NodeId(1), NodeId(0), commit)
        .unwrap();
    source
        .remember_client_request(VsrClientRequestId::new("bounded-receipt", 7), LogIndex(1))
        .unwrap();
    let second = prepare(&mut source, 2);
    follower
        .receive_prepare(NodeId(1), NodeId(0), second)
        .unwrap();
    (source, follower)
}
pub(crate) fn snapshot(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn walk(root: &Path, at: &Path, result: &mut BTreeMap<PathBuf, Vec<u8>>) {
        for entry in fs::read_dir(at).unwrap() {
            let path = entry.unwrap().path();
            let metadata = fs::symlink_metadata(&path).unwrap();
            let key = path.strip_prefix(root).unwrap().to_path_buf();
            if metadata.file_type().is_symlink() {
                result.insert(
                    key,
                    fs::read_link(&path)
                        .unwrap()
                        .as_os_str()
                        .as_encoded_bytes()
                        .to_vec(),
                );
            } else if metadata.is_dir() {
                result.insert(key, b"directory".to_vec());
                walk(root, &path, result);
            } else {
                result.insert(key, fs::read(path).unwrap());
            }
        }
    }
    let mut result = BTreeMap::new();
    walk(root, root, &mut result);
    result
}
pub(crate) fn open_with_hook(
    path: &Path,
    hook: &mut impl FnMut(Boundary) -> RsfResult<()>,
) -> RsfResult<DurableVsrCell> {
    DurableVsrCell::open_existing_bounded_with(path, PlacementGroupId(1), limits(), hook)
}

#[test]
fn bounded_flat_voter_preserves_committed_accepted_receipts_and_later_real_commit() {
    let path = root("flat");
    let (source, mut follower) = fixture(&path);
    let expected = source.committed_prefix().unwrap();
    let accepted = source.prepared(LogIndex(2)).unwrap().clone();
    let lineage = source.membership_lineage().unwrap();
    drop(source);
    let mut source =
        DurableVsrCell::open_existing_bounded(path.join("source"), PlacementGroupId(1), limits())
            .unwrap();
    assert_eq!(source.committed_prefix().unwrap(), expected);
    assert_eq!(source.prepared(LogIndex(2)), Some(&accepted));
    assert_eq!(source.membership_lineage().unwrap(), lineage);
    assert_eq!(
        source.request_commit(&VsrClientRequestId::new("bounded-receipt", 7)),
        Some(LogIndex(1))
    );
    follower
        .receive_prepare(NodeId(1), NodeId(0), accepted.clone())
        .unwrap();
    source.acknowledge(NodeId(1), accepted.view, accepted.index);
    let VsrDecision::Committed(commit) = source.commit(NodeId(0), accepted.index).unwrap() else {
        panic!("commit rejected")
    };
    follower
        .receive_commit(NodeId(1), NodeId(0), commit)
        .unwrap();
    let expected = source.committed_prefix().unwrap();
    drop(source);
    drop(follower);
    let source =
        DurableVsrCell::open_existing_bounded(path.join("source"), PlacementGroupId(1), limits())
            .unwrap();
    assert_eq!(source.committed_prefix().unwrap(), expected);
    assert_eq!(source.next_index(), LogIndex(3));
    drop(source);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn bounded_exact_physical_limits_succeed_and_one_under_stops_before_replay_without_mutation() {
    let path = root("limits");
    let (source, follower) = fixture(&path);
    drop(source);
    drop(follower);
    let target = path.join("source");
    let recovery = SegmentStore::recover_existing(&target).unwrap().recovery;
    let exact = VsrPrefixLimits {
        max_records: recovery.wal_records.len() as u64,
        max_encoded_bytes: recovery
            .wal_records
            .iter()
            .map(|record| 18 + record.payload.len() as u64)
            .sum(),
    };
    let before = snapshot(&target);
    drop(DurableVsrCell::open_existing_bounded(&target, PlacementGroupId(1), exact).unwrap());
    for limits in [
        VsrPrefixLimits {
            max_records: exact.max_records - 1,
            ..exact
        },
        VsrPrefixLimits {
            max_encoded_bytes: exact.max_encoded_bytes - 1,
            ..exact
        },
    ] {
        let mut replayed = false;
        let error = DurableVsrCell::open_existing_bounded_with(
            &target,
            PlacementGroupId(1),
            limits,
            &mut |_| {
                replayed = true;
                Ok(())
            },
        )
        .unwrap_err();
        assert_eq!(error.code, ErrorCode::InvalidInput);
        assert!(
            error.message.contains("physical budget exceeded"),
            "{error:?}"
        );
        assert!(!replayed);
        assert_eq!(snapshot(&target), before);
    }
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn bounded_sparse_frame_and_manifest_reject_at_budget_before_checksum_or_replay() {
    let path = root("sparse");
    let source = open(&path.join("source"));
    let segment = source
        .store
        .root()
        .join("segments")
        .join(&source.store.active_segment().file_name);
    drop(source);
    // Mutate the length field of the actual first physical frame. Its sparse
    // payload is deliberately not checksummed: budget rejection must precede
    // payload/checksum interpretation. The shared storage suite additionally
    // asserts its payload-read counter remains unchanged for this path.
    let length = 32 * 1024 * 1024_u64;
    let mut file = OpenOptions::new().write(true).open(&segment).unwrap();
    file.seek(SeekFrom::Start(8)).unwrap();
    file.write_all(&length.to_le_bytes()).unwrap();
    file.set_len(length + 24).unwrap();
    drop(file);
    let mut replayed = false;
    let error = DurableVsrCell::open_existing_bounded_with(
        &path.join("source"),
        PlacementGroupId(1),
        limits(),
        &mut |_| {
            replayed = true;
            Ok(())
        },
    )
    .unwrap_err();
    assert_eq!(error.code, ErrorCode::InvalidInput);
    assert!(error.message.contains("physical budget exceeded"));
    assert!(!replayed);
    assert_eq!(fs::metadata(&segment).unwrap().len(), length + 24);
    let manifest = path.join("source/MANIFEST");
    let expected_len =
        rsf_storage::append_only_manifest_byte_bound(limits().max_records).unwrap() as u64 + 1;
    OpenOptions::new()
        .write(true)
        .open(&manifest)
        .unwrap()
        .set_len(expected_len)
        .unwrap();
    let error =
        DurableVsrCell::open_existing_bounded(path.join("source"), PlacementGroupId(1), limits())
            .unwrap_err();
    assert_eq!(error.code, ErrorCode::InvalidInput);
    assert!(error.message.contains("ManifestBytes"));
    assert_eq!(fs::metadata(manifest).unwrap().len(), expected_len);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn bounded_missing_owned_files_and_wrong_pg_never_create_or_repair() {
    for name in [
        ".vsr-writer.lock",
        "MANIFEST",
        "MANIFEST.lock",
        "segment",
        "segment-lock",
    ] {
        let path = root("missing");
        let source = open(&path.join("source"));
        let segment = source
            .store
            .root()
            .join("segments")
            .join(&source.store.active_segment().file_name);
        let lost = match name {
            "segment" => segment,
            "segment-lock" => segment.with_extension("lock"),
            _ => path.join("source").join(name),
        };
        drop(source);
        fs::remove_file(&lost).unwrap();
        let before = snapshot(&path);
        assert!(
            DurableVsrCell::open_existing_bounded(
                path.join("source"),
                PlacementGroupId(1),
                limits()
            )
            .is_err()
        );
        assert_eq!(snapshot(&path), before);
        assert!(!lost.exists());
        fs::remove_dir_all(path).unwrap();
    }
    let path = root("wrong-pg");
    let source = open(&path.join("source"));
    drop(source);
    let before = snapshot(&path);
    assert!(
        DurableVsrCell::open_existing_bounded(path.join("source"), PlacementGroupId(9), limits())
            .unwrap_err()
            .message
            .contains("persisted bootstrap")
    );
    assert_eq!(snapshot(&path), before);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn bounded_live_owner_excludes_duplicate_and_latches_observed_inner_loss_after_original_inode_restore()
 {
    let path = root("live");
    let source = open(&path.join("source"));
    drop(source);
    let source =
        DurableVsrCell::open_existing_bounded(path.join("source"), PlacementGroupId(1), limits())
            .unwrap();
    assert_eq!(
        DurableVsrCell::open_existing_bounded(path.join("source"), PlacementGroupId(1), limits())
            .unwrap_err()
            .code,
        ErrorCode::QuorumUnavailable
    );
    let manifest = path.join("source/MANIFEST");
    let saved = path.join("source/MANIFEST.saved");
    fs::rename(&manifest, &saved).unwrap();
    assert!(source.recovery_state_token().is_err());
    fs::rename(saved, &manifest).unwrap();
    assert!(
        source
            .recovery_state_token()
            .unwrap_err()
            .message
            .contains("VSR durability failed")
    );
    drop(source);
    drop(
        DurableVsrCell::open_existing_bounded(path.join("source"), PlacementGroupId(1), limits())
            .unwrap(),
    );
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn bounded_invalid_policy_is_rejected_before_any_namespace_access() {
    let path = root("invalid-policy");
    let missing = path.join("no-such-parent/no-such-owner");
    let error = DurableVsrCell::open_existing_bounded(
        &missing,
        PlacementGroupId(1),
        VsrPrefixLimits {
            max_records: 0,
            max_encoded_bytes: 0,
        },
    )
    .unwrap_err();
    assert_eq!(error.code, ErrorCode::InvalidInput);
    assert!(error.message.contains("limits"));
    assert!(!missing.exists());
    fs::remove_dir_all(path).unwrap();
}

// Fixture-only visible, unacknowledged suffix: derive a legal prepare from the
// actual current VSR state, release that owner, then append its canonical WAL
// with SyncPolicy::None. This is not an injected fsync failure or an ACK claim.
pub(crate) fn visible_prepare(mut cell: DurableVsrCell) -> (PathBuf, VsrPrepare) {
    cell.primary_probe().unwrap();
    let primary = cell.primary();
    let VsrDecision::Prepared(prepare) = cell.cell.prepare(
        primary,
        VsrOperation::ProtocolOperation {
            operation_id: OperationId(999),
        },
        1,
    ) else {
        panic!("visible fixture prepare rejected")
    };
    let root = cell.root.clone();
    let physical = cell.store.root().to_path_buf();
    drop(cell);
    let mut options = SegmentStoreOptions::default();
    options.log_options.sync_policy = rsf_storage::SyncPolicy::None;
    let mut store = SegmentStore::open_with_options(&physical, options).unwrap();
    store
        .append_next_wal(
            prepare.pg,
            prepare.view,
            WalRecordKind::Prepare,
            &prepare.encode().unwrap(),
        )
        .unwrap();
    drop(store);
    (root, prepare)
}

#[test]
fn bounded_flat_visible_suffix_renews_before_owner_and_preserves_wal_bytes() {
    let path = root("visible-flat");
    let source = open(&path.join("source"));
    let (target, prepare) = visible_prepare(source);
    let before = snapshot(&target);
    let mut phases = Vec::new();
    let cell = open_with_hook(&target, &mut |boundary| {
        phases.push(boundary);
        Ok(())
    })
    .unwrap();
    assert_eq!(
        phases,
        vec![
            Boundary::Replayed,
            Boundary::BeforeRenewal,
            Boundary::AfterClosedRenewal,
            Boundary::Renewed
        ]
    );
    assert_eq!(cell.prepared(prepare.index), Some(&prepare));
    assert_eq!(snapshot(&target), before);
    drop(cell);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn bounded_renewal_custody_failure_returns_no_owner_and_same_inode_cold_retry_succeeds() {
    let path = root("renewal-custody");
    let source = open(&path.join("source"));
    let segment = source
        .store
        .root()
        .join("segments")
        .join(&source.store.active_segment().file_name);
    let (target, prepare) = visible_prepare(source);
    let saved = segment.with_extension("saved");
    let mut reached = false;
    let mut renewed = false;
    let error = open_with_hook(&target, &mut |boundary| {
        if boundary == Boundary::BeforeRenewal {
            reached = true;
            fs::rename(&segment, &saved).unwrap();
        }
        if boundary == Boundary::Renewed {
            renewed = true;
        }
        Ok(())
    })
    .unwrap_err();
    assert!(reached);
    assert!(!renewed);
    // A missing named file is an I/O Recover failure. Structural identity
    // mismatches use CorruptionDetected, but this fault is specifically ENOENT.
    assert_eq!(error.code, ErrorCode::InvalidInput);
    assert!(error.message.starts_with("storage Recover failed:"));
    assert!(!segment.exists());
    assert!(!target.join("VSR_PUBLISHED").exists());
    fs::rename(saved, &segment).unwrap();
    let cell =
        DurableVsrCell::open_existing_bounded(&target, PlacementGroupId(1), limits()).unwrap();
    assert_eq!(cell.prepared(prepare.index), Some(&prepare));
    drop(cell);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn bounded_empty_existing_store_never_creates_bootstrap_and_owned_root_alias_rejects() {
    let path = root("empty");
    let target = path.join("empty");
    drop(SegmentStore::open(&target).unwrap());
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(target.join(".vsr-writer.lock"))
        .unwrap();
    let before = snapshot(&target);
    assert!(
        DurableVsrCell::open_existing_bounded(&target, PlacementGroupId(1), limits())
            .unwrap_err()
            .message
            .contains("persisted bootstrap")
    );
    assert_eq!(snapshot(&target), before);
    let actual = path.join("actual");
    drop(open(&actual));
    let alias = path.join("alias");
    std::os::unix::fs::symlink(&actual, &alias).unwrap();
    let before = snapshot(&actual);
    assert!(
        DurableVsrCell::open_existing_bounded(&alias, PlacementGroupId(1), limits())
            .unwrap_err()
            .message
            .contains("physical directory")
    );
    assert_eq!(snapshot(&actual), before);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn bounded_compacted_storage_is_explicitly_unsupported_before_semantic_replay() {
    let path = root("compacted");
    let (source, follower) = fixture(&path);
    drop(source);
    drop(follower);
    let target = path.join("source");
    let mut store = SegmentStore::open(&target).unwrap();
    let commit = store
        .recover_current()
        .unwrap()
        .recovery
        .wal_records
        .into_iter()
        .find(|record| record.kind == WalRecordKind::Commit)
        .unwrap();
    store
        .compact([(PlacementGroupId(1), commit.index)].into_iter().collect())
        .unwrap();
    drop(store);
    assert!(SegmentStore::recover_existing(&target).is_ok());
    let before = snapshot(&target);
    let mut replayed = false;
    let error = open_with_hook(&target, &mut |_| {
        replayed = true;
        Ok(())
    })
    .unwrap_err();
    assert_eq!(error.code, ErrorCode::InvalidInput);
    assert_eq!(
        error.message,
        "bounded voter recovery requires uncompacted history"
    );
    assert!(!replayed);
    assert_eq!(snapshot(&target), before);
    fs::remove_dir_all(path).unwrap();
}
