//! Actual caller compositions over the bounded disposal implementation.
//! These are coverage additions, not claims of an old implementation defect.
use super::discard::{begin_active, cold, image, staged};
use super::*;
use namespace::{Operation, observation as boundary};
use std::cell::Cell;
use std::collections::BTreeSet;
use std::ffi::OsString;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::rc::Rc;

fn candidate_names(root: &Path) -> BTreeSet<OsString> {
    fs::read_dir(root)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .filter(|name| name.to_str().is_some_and(|name| name.len() == 64))
        .collect()
}
fn candidate_set(paths: &[&Path]) -> BTreeSet<OsString> {
    paths
        .iter()
        .map(|path| path.file_name().unwrap().to_os_string())
        .collect()
}
fn identity(path: &Path) -> (u64, u64) {
    let metadata = fs::symlink_metadata(path).unwrap();
    assert!(!metadata.file_type().is_symlink());
    (metadata.dev(), metadata.ino())
}
fn assert_held(path: &Path) {
    let file = OpenOptions::new().write(true).open(path).unwrap();
    assert!(
        matches!(
            rsf_storage::ExclusiveFileLock::acquire(file),
            Err(std::fs::TryLockError::WouldBlock)
        ),
        "actual original owner must still hold {}",
        path.display()
    );
}
fn advance_again(source: &mut DurableVsrCell) -> VsrVoterPrefixExport {
    // Accept index 4 before committing index 3 so the immutable prefix ending
    // at that commit contains the genuine accepted tail as well.
    let accepted = prepare(source, 4, b"accepted-after-replacement");
    assert_eq!(accepted.index, LogIndex(4));
    commit(source, LogIndex(3));
    source
        .export_committed_prefix(LogIndex(3), limits())
        .unwrap()
}
fn assert_c(preview: &VsrImportPreview<'_>) -> RsfResult<()> {
    assert_eq!(preview.committed_tail(), LogIndex(3));
    assert_eq!(preview.committed_count(), 3);
    assert_eq!(preview.next_index(), LogIndex(5));
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
    assert!(matches!(&committed[0], VsrOperation::ReplicatedCommand {
        operation_id: OperationId(1), payload
    } if payload == b"Genesis"));
    assert!(matches!(&committed[1], VsrOperation::ReplicatedCommand {
        operation_id: OperationId(2), payload
    } if payload == b"Begin + certified delivery"));
    assert!(matches!(&committed[2], VsrOperation::ReplicatedCommand {
        operation_id: OperationId(3), payload
    } if payload == b"accepted-only suffix"));
    let mut accepted = Vec::new();
    preview.for_each_accepted_tail(|prepare| {
        accepted.push(prepare.clone());
        Ok(())
    })?;
    assert_eq!(accepted.len(), 1);
    assert_eq!(accepted[0].index, LogIndex(4));
    assert!(
        matches!(&accepted[0].operation, VsrOperation::ReplicatedCommand {
        operation_id: OperationId(4), payload
    } if payload == b"accepted-after-replacement")
    );
    preview.validate_live()
}
fn assert_original_receipt(owner: &LearnerHistoryStore) {
    assert_eq!(
        owner
            .published
            .as_ref()
            .unwrap()
            .cell
            .request_commit(&VsrClientRequestId::new("original", 1)),
        Some(LogIndex(1))
    );
}

#[test]
fn successful_replacement_reaches_three_validated_stores_with_original_owners_still_locked() {
    let mut fixture = Fixture::new("caller-three-held-stores");
    let a = fixture.four.published.as_ref().unwrap().id;
    let a_path = fixture.four.directory(&a);
    let a_bytes = image(&a_path);
    let a_token = fixture.four.published_state_token().unwrap();
    let (b, _) = staged(&mut fixture);
    fixture
        .four
        .finish_prefix(b, |preview| {
            assert_semantics(preview)?;
            Err(invalid_input("keep actual completed B"))
        })
        .unwrap_err();
    let b_path = fixture.four.directory(&b);
    let b_bytes = image(&b_path);
    let c_export = advance_again(&mut fixture.source);
    let c = c_export.descriptor().id().unwrap();
    let c_path = fixture.four.directory(&c);
    let root_path = fixture.root.join("four");
    let root_lock = root_path.join(".learner.lock");
    let original_root_lock = identity(&root_lock);
    let a_lock = a_path.join("cell/.vsr-writer.lock");
    let b_lock = b_path.join("cell/.vsr-writer.lock");
    let original_a_lock = identity(&a_lock);
    let original_b_lock = identity(&b_lock);
    let seen = Rc::new(Cell::new(false));
    let seen_in_callback = seen.clone();
    let inspect_a = a_path.clone();
    let inspect_b = b_path.clone();
    let inspect_c = c_path.clone();
    let inspector = boundary::inspect(move |event| {
        if event.operation != Operation::CandidateReady {
            return;
        }
        assert!(event.completed);
        assert_eq!(event.path, inspect_c);
        assert!(!seen_in_callback.replace(true), "one actual new C");
        assert_eq!(
            candidate_names(&root_path),
            candidate_set(&[&inspect_a, &inspect_b, &inspect_c])
        );
        assert_eq!(image(&inspect_a), a_bytes);
        assert_eq!(image(&inspect_b), b_bytes);
        assert_eq!(identity(&root_lock), original_root_lock);
        assert_held(&root_lock);
        assert_eq!(identity(&a_lock), original_a_lock);
        assert_eq!(identity(&b_lock), original_b_lock);
        assert_held(&a_lock);
        assert_held(&b_lock);
        // This seam runs after actual C strict writer + complete namespace
        // validation, before old B retirement. C is a healthy empty store,
        // not a completed or published history, and grants no authority.
        assert!(inspect_c.join("DESCRIPTOR").is_file());
        assert!(inspect_c.join("cell/MANIFEST").is_file());
        assert_held(&inspect_c.join("cell/MANIFEST.lock"));
        let segments = fs::read_dir(inspect_c.join("cell/segments"))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect::<Vec<_>>();
        let data = segments
            .iter()
            .filter(|path| path.extension().is_some_and(|v| v == "segment"))
            .collect::<Vec<_>>();
        assert_eq!(data.len(), 1);
        assert_eq!(fs::metadata(data[0]).unwrap().len(), 0);
        assert_held(&data[0].with_extension("lock"));
    });
    assert_eq!(
        fixture
            .four
            .begin_prefix(c_export.descriptor().clone(), limits())
            .unwrap(),
        0
    );
    drop(inspector);
    assert!(
        seen.get(),
        "actual maximum-three transition must be reached"
    );
    assert_eq!(fixture.four.published_state_token().unwrap(), a_token);
    assert_eq!(
        candidate_names(&fixture.root.join("four")),
        candidate_set(&[&a_path, &c_path])
    );
    assert!(!b_path.exists());
    assert_original_receipt(&fixture.four);
    copy_voter(&fixture.source, &c_export, &mut fixture.four);
    fixture.four.finish_prefix(c, assert_c).unwrap();
    assert_eq!(
        candidate_names(&fixture.root.join("four")),
        candidate_set(&[&c_path])
    );
    assert_original_receipt(&fixture.four);
    let root = fixture.root.clone();
    drop(fixture);
    let mut owner = cold(&root.join("four"));
    begin_active(&mut owner).unwrap();
    assert_eq!(owner.finish_prefix(c, assert_c).unwrap(), LogIndex(3));
    assert_original_receipt(&owner);
    assert_eq!(
        candidate_names(&root.join("four")),
        candidate_set(&[&c_path])
    );
    drop(owner);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn three_slot_existing_resume_preflights_every_candidate_before_cleanup_and_then_succeeds() {
    // Discover the exact real ready boundary, then interrupt a fresh run at it.
    let mut baseline = Fixture::new("caller-ready-discovery");
    staged(&mut baseline);
    let export = advance_again(&mut baseline.source);
    let watch = boundary::watch(None);
    baseline
        .four
        .begin_prefix(export.descriptor().clone(), limits())
        .unwrap();
    let ready = watch
        .events()
        .iter()
        .position(|event| event.operation == Operation::CandidateReady)
        .unwrap();
    drop(watch);
    let root = baseline.root.clone();
    drop(baseline);
    fs::remove_dir_all(root).unwrap();

    let mut fixture = Fixture::new("caller-three-preflight");
    let a = fixture.four.published.as_ref().unwrap().id;
    let a_path = fixture.four.directory(&a);
    let (b, b_export) = staged(&mut fixture);
    let b_path = fixture.four.directory(&b);
    let c_export = advance_again(&mut fixture.source);
    let c_path = fixture.four.directory(&c_export.descriptor().id().unwrap());
    let watch = boundary::watch(Some(ready));
    let error = fixture
        .four
        .begin_prefix(c_export.descriptor().clone(), limits())
        .unwrap_err();
    assert_eq!(
        error.message,
        "injected bounded namespace boundary failure (test only)"
    );
    assert_eq!(watch.events().len(), ready + 1);
    drop(watch);
    let root = fixture.root.clone();
    drop(fixture);
    let mut owner = cold(&root.join("four"));
    assert_eq!(
        candidate_names(&root.join("four")),
        candidate_set(&[&a_path, &b_path, &c_path])
    );
    let before_valid = image(&root.join("four"));
    let foreign = c_path.join("cell/foreign-before-preflight");
    fs::write(&foreign, b"must not be swept").unwrap();
    let before = image(&root.join("four"));
    let watch = boundary::watch(None);
    let error = owner
        .begin_prefix(b_export.descriptor().clone(), limits())
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::CorruptionDetected);
    assert_eq!(
        error.message,
        "bounded disposable cell has foreign auxiliary"
    );
    assert!(
        watch.events().is_empty(),
        "complete preflight must precede every caller effect"
    );
    drop(watch);
    assert_eq!(image(&root.join("four")), before);
    fs::remove_file(&foreign).unwrap();
    assert_eq!(image(&root.join("four")), before_valid);
    drop(owner);
    let mut owner = cold(&root.join("four"));
    assert_eq!(
        owner
            .begin_prefix(b_export.descriptor().clone(), limits())
            .unwrap(),
        b_export.descriptor().records
    );
    assert_eq!(
        candidate_names(&root.join("four")),
        candidate_set(&[&a_path, &b_path])
    );
    assert!(!c_path.exists());
    owner.finish_prefix(b, assert_semantics).unwrap();
    assert_original_receipt(&owner);
    assert_eq!(
        candidate_names(&root.join("four")),
        candidate_set(&[&b_path])
    );
    drop(owner);
    let mut owner = cold(&root.join("four"));
    begin_active(&mut owner).unwrap();
    owner.finish_prefix(b, assert_semantics).unwrap();
    assert_original_receipt(&owner);
    drop(owner);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn real_owned_rotation_name_failure_is_strictly_refused_then_explicitly_cancelled_by_current_active()
 {
    let mut fixture = Fixture::new("caller-real-rotation-remnant");
    let a = fixture.four.published.as_ref().unwrap().id;
    let a_path = fixture.four.directory(&a);
    let (b, export) = staged(&mut fixture);
    let b_path = fixture.four.directory(&b);
    let batch = fixture
        .source
        .read_committed_prefix_batch(&export, 0)
        .unwrap();
    let genuine_record = batch.records.last().unwrap().clone();
    let Fixture {
        root,
        source,
        three,
        four,
    } = fixture;
    drop(four);
    let mut owner = cold(&root.join("four"));
    let a_token = owner.published_state_token().unwrap();
    let a_bytes = image(&a_path);
    let a_writer = identity(&a_path.join("cell/.vsr-writer.lock"));
    let cell = b_path.join("cell");
    let before = image(&cell);
    let manifest = fs::read(cell.join("MANIFEST")).unwrap();
    let mut options = SegmentStoreOptions {
        max_segment_bytes: 1,
        ..SegmentStoreOptions::default()
    };
    options.log_options.sync_policy = rsf_storage::SyncPolicy::Full;
    let mut producer = SegmentStore::open_with_options(&cell, options).unwrap();
    assert_eq!(producer.segments().len(), 1);
    assert_eq!(
        image(&cell),
        before,
        "producer opening does not manufacture residue"
    );
    let custody = producer.capture_owned_custody().unwrap();
    let permissions = fs::metadata(&cell).unwrap().permissions();
    // Segments remain writable: real rotation creates/syncs its next empty
    // segment and sidecar, then fails specifically creating MANIFEST.tmp.
    fs::set_permissions(&cell, fs::Permissions::from_mode(0o555)).unwrap();
    let result = producer.try_append_owned_wal_group(
        &custody,
        PlacementGroupId(1),
        [Ok(rsf_storage::WalGroupEntry {
            epoch: genuine_record.epoch,
            kind: genuine_record.kind,
            payload: genuine_record.payload.as_slice(),
        })],
    );
    fs::set_permissions(&cell, permissions).unwrap();
    let error = result.expect_err("host must enforce unprivileged MANIFEST.tmp creation denial");
    assert_eq!(error.code, ErrorCode::InvalidInput);
    assert!(error.message.contains("storage Recover failed:"));
    assert!(
        error.message.contains("Permission denied"),
        "actual create failure: {error:?}"
    );
    assert!(!error.message.contains("injected"));
    assert_eq!(fs::read(cell.join("MANIFEST")).unwrap(), manifest);
    assert!(!cell.join("MANIFEST.tmp").exists());
    let after = image(&cell);
    for (name, value) in &before {
        assert_eq!(
            after.get(name),
            Some(value),
            "retained bytes changed at {name:?}"
        );
    }
    let added = after
        .keys()
        .filter(|name| !before.contains_key(*name))
        .collect::<Vec<_>>();
    assert_eq!(
        added.len(),
        2,
        "actual next empty segment and sidecar prove rotation reached publication"
    );
    let segment = added
        .iter()
        .find(|path| path.extension().is_some_and(|value| value == "segment"))
        .unwrap();
    let sidecar = segment.with_extension("lock");
    assert!(added.contains(&&sidecar));
    for name in &added {
        let metadata = fs::symlink_metadata(cell.join(name)).unwrap();
        assert!(metadata.is_file() && !metadata.file_type().is_symlink());
        assert_eq!(metadata.nlink(), 1);
        assert_eq!(metadata.len(), 0);
    }
    let fenced = producer.capture_owned_custody().unwrap_err();
    assert_eq!(fenced.code, ErrorCode::CorruptionDetected);
    assert_eq!(
        fenced.message,
        "segment store durability failed; reopen before further writes"
    );
    drop(producer);
    assert_eq!(owner.published_state_token().unwrap(), a_token);
    assert_eq!(image(&a_path), a_bytes);
    assert_eq!(identity(&a_path.join("cell/.vsr-writer.lock")), a_writer);
    assert_held(&a_path.join("cell/.vsr-writer.lock"));
    let before_resume = image(&root.join("four"));
    let error = owner
        .begin_prefix(export.descriptor().clone(), limits())
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::InvalidInput);
    assert_eq!(error.message, "bounded learner namespace capacity exceeded");
    assert_eq!(
        image(&root.join("four")),
        before_resume,
        "strict current resume must not repair the next files"
    );
    drop(owner);
    let mut owner = cold(&root.join("four"));
    assert_original_receipt(&owner);
    begin_active(&mut owner).unwrap();
    assert!(!b_path.exists());
    assert_eq!(
        candidate_names(&root.join("four")),
        candidate_set(&[&a_path])
    );
    assert_eq!(image(&a_path), a_bytes);
    copy_voter(&source, &export, &mut owner);
    owner.finish_prefix(b, assert_semantics).unwrap();
    assert_original_receipt(&owner);
    assert_eq!(
        candidate_names(&root.join("four")),
        candidate_set(&[&b_path])
    );
    drop(owner);
    let mut owner = cold(&root.join("four"));
    begin_active(&mut owner).unwrap();
    owner.finish_prefix(b, assert_semantics).unwrap();
    assert_original_receipt(&owner);
    drop(owner);
    drop(source);
    drop(three);
    fs::remove_dir_all(root).unwrap();
}
