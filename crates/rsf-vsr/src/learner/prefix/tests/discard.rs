//! Existing public bounded APIs: fail before effects and preserve actual owners.
use super::*;
use std::collections::BTreeMap;

pub(super) fn image(root: &Path) -> BTreeMap<PathBuf, Option<Vec<u8>>> {
    fn visit(base: &Path, path: &Path, out: &mut BTreeMap<PathBuf, Option<Vec<u8>>>) {
        for entry in fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            let kind = entry.file_type().unwrap();
            let relative = path.strip_prefix(base).unwrap().to_path_buf();
            if kind.is_symlink() {
                out.insert(
                    relative,
                    Some(format!("alias:{}", fs::read_link(path).unwrap().display()).into_bytes()),
                );
            } else if kind.is_dir() {
                out.insert(relative, None);
                visit(base, &path, out);
            } else {
                out.insert(relative, Some(fs::read(path).unwrap()));
            }
        }
    }
    let mut out = BTreeMap::new();
    visit(root, root, &mut out);
    out
}
pub(super) fn cold(root: &Path) -> LearnerHistoryStore {
    LearnerHistoryStore::open_published_existing(
        root,
        PlacementGroupId(1),
        NodeId(4),
        NodeId(0),
        limits(),
    )
    .unwrap()
}
pub(super) fn begin_active(owner: &mut LearnerHistoryStore) -> RsfResult<u64> {
    let descriptor = owner.published.as_ref().unwrap().descriptor.clone();
    owner.begin_prefix(descriptor, limits())
}
pub(super) fn staged(fixture: &mut Fixture) -> ([u8; 32], VsrVoterPrefixExport) {
    let export = fixture.advance();
    copy_voter(&fixture.source, &export, &mut fixture.four);
    (export.descriptor().id().unwrap(), export)
}

#[test]
fn complete_namespace_preflight_rejects_unknown_or_aliased_obsolete_content_before_any_deletion() {
    for fault in ["root", "unknown-cell", "descriptor", "alias"] {
        assert_namespace_refusal(fault);
    }
}

// Linux permits these raw filename bytes; APFS rejects their creation before
// the learner can inspect them. Keep the real filesystem case Linux-specific.
#[cfg(target_os = "linux")]
#[test]
fn complete_namespace_preflight_rejects_non_utf8_obsolete_name_without_deletion() {
    assert_namespace_refusal("non-utf8");
}

fn assert_namespace_refusal(fault: &str) {
    let mut fixture = Fixture::new("namespace-refusal");
    let (id, _) = staged(&mut fixture);
    let target = fixture.four.directory(&id);
    // Keep genuine B bytes, but make it cold/unselected for disposal.
    let Fixture {
        root,
        source,
        three,
        four,
    } = fixture;
    drop(four);
    let healthy = cold(&root.join("four"));
    assert_eq!(healthy.committed_entries().unwrap().len(), 1);
    drop(healthy);
    match fault {
        "root" => {
            fs::write(root.join("four/unrecognized"), b"keep").unwrap();
        }
        "unknown-cell" => {
            fs::write(target.join("cell/foreign"), b"keep").unwrap();
        }
        "descriptor" => {
            fs::write(target.join("DESCRIPTOR"), b"foreign").unwrap();
        }
        "alias" => {
            fs::rename(&target, root.join("held-B")).unwrap();
            std::os::unix::fs::symlink(root.join("held-B"), &target).unwrap();
        }
        "non-utf8" => {
            use std::os::unix::ffi::OsStringExt;
            fs::write(
                root.join("four")
                    .join(std::ffi::OsString::from_vec(vec![0xff])),
                b"keep",
            )
            .unwrap();
        }
        _ => unreachable!(),
    }
    let before = image(&root.join("four"));
    let result = LearnerHistoryStore::open_published_existing(
        root.join("four"),
        PlacementGroupId(1),
        NodeId(4),
        NodeId(0),
        limits(),
    );
    assert!(
        result.is_err(),
        "{fault}: cold complete preflight must refuse"
    );
    assert_eq!(image(&root.join("four")), before, "{fault}");
    drop(source);
    drop(three);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn malformed_last_candidate_and_over_capacity_refuse_before_retiring_a_healthy_stage() {
    for capacity in [false, true] {
        let mut fixture = Fixture::new("all-before-effects");
        let (id, _) = staged(&mut fixture);
        // Two bounded empty factories are recognizable obsolete shapes; a
        // fourth total candidate is nevertheless outside the fixed allowance.
        for value in if capacity {
            vec![0xfe, 0xff]
        } else {
            vec![0xff]
        } {
            let other = fixture.four.directory(&[value; 32]);
            fs::create_dir(&other).unwrap();
            if !capacity {
                fs::write(other.join("DESCRIPTOR"), b"malformed final descriptor").unwrap();
            }
        }
        let before = image(&fixture.root.join("four"));
        let error = begin_active(&mut fixture.four).unwrap_err();
        assert!(!error.message.is_empty());
        assert_eq!(image(&fixture.root.join("four")), before);
        assert!(
            fixture.four.directory(&id).is_dir(),
            "valid earlier B cannot be swept before late preflight failure"
        );
        let root = fixture.root.clone();
        drop(fixture);
        fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn bounded_cold_existing_factory_is_not_repaired_into_cursor_and_explicit_active_begin_cancels_it()
{
    let mut fixture = Fixture::new("strict-existing-factory");
    let export = fixture.advance();
    let id = export.descriptor().id().unwrap();
    let directory = fixture.four.directory(&id);
    fs::create_dir(&directory).unwrap();
    let mut descriptor = File::create(directory.join("DESCRIPTOR")).unwrap();
    descriptor
        .write_all(&export.descriptor().encode().unwrap())
        .unwrap();
    descriptor.sync_all().unwrap();
    drop(descriptor);
    // Actual public factory finishes MANIFEST and initial segment but has not
    // produced an active SegmentLog sidecar/writer. No made-up storage codec.
    SegmentStore::resume_empty_initialization(directory.join("cell")).unwrap();
    let before = image(&fixture.root.join("four"));
    let Fixture {
        root,
        source,
        three,
        four,
    } = fixture;
    drop(four);
    let mut owner = cold(&root.join("four"));
    assert!(
        owner
            .begin_prefix(export.descriptor().clone(), limits())
            .is_err()
    );
    assert_eq!(
        image(&root.join("four")),
        before,
        "no cold factory completion or cursor repair"
    );
    drop(owner);
    let mut owner = cold(&root.join("four"));
    begin_active(&mut owner).unwrap();
    assert!(!directory.exists());
    copy_voter(&source, &export, &mut owner);
    owner.finish_prefix(id, assert_semantics).unwrap();
    drop(owner);
    drop(source);
    drop(three);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn healthy_semantic_refusal_keeps_actual_completed_writer_and_exact_duplicate_append() {
    let mut fixture = Fixture::new("rejected-keeps-writer");
    let (id, export) = staged(&mut fixture);
    let active = fs::read(fixture.root.join("four/ACTIVE")).unwrap();
    let error = fixture
        .four
        .finish_prefix(id, |preview| {
            assert_semantics(preview)?;
            Err(invalid_input("domain refuses this time"))
        })
        .unwrap_err();
    assert_eq!(error.message, "domain refuses this time");
    let file = OpenOptions::new()
        .write(true)
        .open(fixture.four.directory(&id).join("cell/.vsr-writer.lock"))
        .unwrap();
    assert!(
        rsf_storage::ExclusiveFileLock::acquire(file).is_err(),
        "semantic refusal must retain the actual completed writer lock"
    );
    assert_eq!(fs::read(fixture.root.join("four/ACTIVE")).unwrap(), active);
    assert_eq!(
        fixture
            .four
            .begin_prefix(export.descriptor().clone(), limits())
            .unwrap(),
        export.descriptor().records
    );
    let batch = fixture
        .source
        .read_committed_prefix_batch(&export, 0)
        .unwrap();
    assert_eq!(
        fixture.four.append_prefix_batch(id, &batch).unwrap(),
        export.descriptor().records
    );
    fixture.four.finish_prefix(id, assert_semantics).unwrap();
    let root = fixture.root.clone();
    drop(fixture);
    let mut owner = cold(&root.join("four"));
    begin_active(&mut owner).unwrap();
    owner.finish_prefix(id, assert_semantics).unwrap();
    assert_eq!(
        owner
            .published
            .as_ref()
            .unwrap()
            .cell
            .request_commit(&VsrClientRequestId::new("original", 1)),
        Some(LogIndex(1))
    );
    drop(owner);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn rejected_owner_original_sidecar_loss_blocks_abandonment_and_restored_inode_does_not_clear_failure()
 {
    let mut fixture = Fixture::new("rejected-custody");
    let (id, _) = staged(&mut fixture);
    fixture
        .four
        .finish_prefix(id, |_| Err(invalid_input("clean semantic refusal")))
        .unwrap_err();
    let sidecar = fixture.four.directory(&id).join("cell/.vsr-writer.lock");
    let held = fixture.root.join("original-writer-lock");
    fs::rename(&sidecar, &held).unwrap();
    fs::write(&sidecar, []).unwrap();
    let before = image(&fixture.root.join("four"));
    assert!(begin_active(&mut fixture.four).is_err());
    assert_eq!(image(&fixture.root.join("four")), before);
    fs::remove_file(&sidecar).unwrap();
    fs::rename(held, &sidecar).unwrap();
    let restored = image(&fixture.root.join("four"));
    let error = begin_active(&mut fixture.four).unwrap_err();
    assert!(error.message.contains("learner durability failed"));
    assert_eq!(image(&fixture.root.join("four")), restored);
    let root = fixture.root.clone();
    drop(fixture);
    let mut owner = cold(&root.join("four"));
    begin_active(&mut owner).unwrap();
    assert!(!owner.directory(&id).exists());
    drop(owner);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn import_callback_namespace_mutation_dominates_ok_or_semantic_error_without_publication() {
    for semantic_error in [false, true] {
        let mut fixture = Fixture::new("callback-namespace");
        let (id, _) = staged(&mut fixture);
        let active = fs::read(fixture.root.join("four/ACTIVE")).unwrap();
        let foreign = fixture.root.join("four/foreign-callback-entry");
        let error = fixture
            .four
            .finish_prefix(id, |preview| {
                assert_semantics(preview)?;
                fs::write(&foreign, b"do not remove").unwrap();
                if semantic_error {
                    Err(invalid_input("semantic callback refusal"))
                } else {
                    Ok(())
                }
            })
            .unwrap_err();
        assert_ne!(error.message, "semantic callback refusal");
        assert_eq!(fs::read(fixture.root.join("four/ACTIVE")).unwrap(), active);
        assert_eq!(fs::read(&foreign).unwrap(), b"do not remove");
        fs::remove_file(&foreign).unwrap();
        assert!(
            fixture
                .four
                .finish_prefix(id, assert_semantics)
                .unwrap_err()
                .message
                .contains("learner durability failed")
        );
        let root = fixture.root.clone();
        drop(fixture);
        fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn cold_lost_finish_reply_recollects_actual_successor_without_republication_or_old_history_loss() {
    let mut fixture = Fixture::new("lost-finish-cleanup");
    let old = fixture.four.published.as_ref().unwrap().id;
    let (id, _) = staged(&mut fixture);
    fixture.four.finish_prefix(id, assert_semantics).unwrap();
    assert!(!fixture.four.directory(&old).exists());
    let root = fixture.root.clone();
    drop(fixture);
    let before = image(&root.join("four"));
    use std::os::unix::fs::MetadataExt;
    let active = fs::metadata(root.join("four/ACTIVE")).unwrap();
    let mut owner = cold(&root.join("four"));
    begin_active(&mut owner).unwrap();
    assert_eq!(
        owner.finish_prefix(id, assert_semantics).unwrap(),
        LogIndex(2)
    );
    let recollected = fs::metadata(root.join("four/ACTIVE")).unwrap();
    assert_eq!(
        (recollected.dev(), recollected.ino()),
        (active.dev(), active.ino())
    );
    assert_eq!(
        image(&root.join("four")),
        before,
        "lost response does not republish or change bytes"
    );
    assert_eq!(
        owner
            .published
            .as_ref()
            .unwrap()
            .cell
            .request_commit(&VsrClientRequestId::new("original", 1)),
        Some(LogIndex(1))
    );
    drop(owner);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn read_only_capacity_refusal_keeps_live_stage_and_allows_exact_policy_retry() {
    let mut fixture = Fixture::new("namespace-capacity-retry");
    let (id, export) = staged(&mut fixture);
    let before = image(&fixture.root.join("four"));
    let too_small = VsrPrefixLimits {
        max_records: export.descriptor().records - 1,
        ..limits()
    };
    assert!(
        fixture
            .four
            .begin_prefix(export.descriptor().clone(), too_small)
            .is_err()
    );
    assert_eq!(image(&fixture.root.join("four")), before);
    assert_eq!(
        fixture
            .four
            .begin_prefix(export.descriptor().clone(), limits())
            .unwrap(),
        export.descriptor().records
    );
    fixture.four.finish_prefix(id, assert_semantics).unwrap();
    let root = fixture.root.clone();
    drop(fixture);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn smaller_byte_policy_cannot_delete_a_live_stage_to_fit_the_active_history() {
    let mut fixture = Fixture::new("protected-byte-budget");
    let (id, export) = staged(&mut fixture);
    let smaller = VsrPrefixLimits {
        max_encoded_bytes: export.encoded_bytes() - 1,
        ..limits()
    };
    // A itself fits; failure must protect the actual larger B before abandon.
    fixture
        .four
        .export_committed_prefix(LogIndex(1), smaller)
        .unwrap();
    let descriptor = fixture.four.published.as_ref().unwrap().descriptor.clone();
    let before = image(&fixture.root.join("four"));
    assert!(fixture.four.begin_prefix(descriptor, smaller).is_err());
    assert_eq!(image(&fixture.root.join("four")), before);
    assert!(fixture.four.directory(&id).is_dir());
    assert_eq!(
        fixture
            .four
            .begin_prefix(export.descriptor().clone(), limits())
            .unwrap(),
        export.descriptor().records
    );
    fixture.four.finish_prefix(id, assert_semantics).unwrap();
    let root = fixture.root.clone();
    drop(fixture);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn actual_caller_unlink_denial_keeps_prior_publication_and_fences_after_permission_restore() {
    use std::os::unix::fs::PermissionsExt;
    let mut fixture = Fixture::new("caller-real-unlink-denial");
    let (id, _) = staged(&mut fixture);
    let segments = fixture.four.directory(&id).join("cell/segments");
    let permissions = fs::metadata(&segments).unwrap().permissions();
    let before = image(&fixture.root.join("four"));
    fs::set_permissions(&segments, fs::Permissions::from_mode(0o555)).unwrap();
    let result = begin_active(&mut fixture.four);
    fs::set_permissions(&segments, permissions).unwrap();
    let error = result
        .expect_err("qualification host must enforce unprivileged directory unlink permissions");
    assert_eq!(error.code, ErrorCode::InvalidInput);
    assert!(!error.message.contains("injected"));
    assert_eq!(image(&fixture.root.join("four")), before);
    assert!(
        begin_active(&mut fixture.four)
            .unwrap_err()
            .message
            .contains("learner durability failed")
    );
    assert_eq!(image(&fixture.root.join("four")), before);
    let root = fixture.root.clone();
    drop(fixture);
    let mut owner = cold(&root.join("four"));
    begin_active(&mut owner).unwrap();
    assert!(!owner.directory(&id).exists());
    assert_eq!(
        owner.published.as_ref().unwrap().cell.committed_tail(),
        LogIndex(1)
    );
    drop(owner);
    fs::remove_dir_all(root).unwrap();
}
