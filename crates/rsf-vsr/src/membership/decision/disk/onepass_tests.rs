//! Physical regression for retaining the opening writer's actual recovery.
use super::*;
use std::cell::RefCell;
type OpenHook = Box<dyn FnOnce(&Path)>;
thread_local! { static HOOK: RefCell<Option<OpenHook>> = const { RefCell::new(None) }; }
pub(super) fn after_open(root: &Path) {
    let hook = HOOK.with(|slot| slot.borrow_mut().take());
    if let Some(hook) = hook {
        hook(root);
    }
}
struct Hook;
impl Drop for Hook {
    fn drop(&mut self) {
        HOOK.with(|slot| {
            slot.borrow_mut().take();
        });
    }
}
fn hook(f: impl FnOnce(&Path) + 'static) -> Hook {
    HOOK.with(|slot| {
        assert!(slot.borrow().is_none());
        *slot.borrow_mut() = Some(Box::new(f));
    });
    Hook
}
fn source(path: &Path) -> (VsrMembershipInput, Vec<VsrHistoryRecord>) {
    let mut cells = crate::membership::tests::fixture(&path.join("voters"));
    let snapshots = cells
        .iter_mut()
        .enumerate()
        .map(|(n, cell)| {
            cell.begin_membership(
                NodeId(n as u64),
                NodeId(0),
                crate::membership::tests::proposal(),
            )
            .unwrap()
        })
        .collect();
    let approval = VsrMembershipApproval::from_snapshots(snapshots).unwrap();
    let input = VsrMembershipInput::from_approval(&approval).unwrap();
    let records = input.flattened_records(&approval).unwrap();
    (input, records)
}
fn root(label: &str) -> std::path::PathBuf {
    let path = crate::membership::tests::root(label);
    fs::create_dir_all(&path).unwrap();
    path
}
fn segment(root: &Path, id: [u8; 32]) -> std::path::PathBuf {
    directory(root, id).join("records/segments/000000000000000001.segment")
}
#[test]
fn stage_copy_uses_original_opened_generation_without_repairing_replacement() {
    let path = root("stage-copy-owned-recovery");
    let (input, records) = source(&path);
    let id = input.id().unwrap();
    let original = path.join("original");
    let foreign = path.join("foreign");
    fs::create_dir(&original).unwrap();
    fs::create_dir(&foreign).unwrap();
    create_input(&original, &input).unwrap();
    create_input(&foreign, &input).unwrap();
    append_records(
        &foreign,
        &mut false,
        &input,
        VsrHistoryBatch::pack(0, &records[..1]).unwrap(),
    )
    .unwrap();
    let foreign_segment = segment(&foreign, id);
    OpenOptions::new()
        .append(true)
        .open(&foreign_segment)
        .unwrap()
        .write_all(b"RSF")
        .unwrap();
    let foreign_bytes = fs::read(&foreign_segment).unwrap();
    let original_bytes = fs::read(segment(&original, id)).unwrap();
    let original_records = directory(&original, id).join("records");
    let foreign_records = directory(&foreign, id).join("records");
    let saved = path.join("original-held-records");
    let move_original = original_records.clone();
    let move_foreign = foreign_records.clone();
    let move_saved = saved.clone();
    let hit = std::rc::Rc::new(std::cell::Cell::new(false));
    let witness = hit.clone();
    let guard = hook(move |_| {
        witness.set(true);
        fs::rename(&move_original, &move_saved).unwrap();
        fs::rename(&move_foreign, &move_original).unwrap();
    });
    let mut failed = false;
    let error = create_and_retain_records(&original, &mut failed, &input, &records).unwrap_err();
    drop(guard);
    assert!(hit.get());
    assert_eq!(error.code, rsf_core::ErrorCode::CorruptionDetected);
    assert!(failed);
    // Restore both original names before checking the regression, so a failed
    // assertion preserves an inspectable fixture with each original root.
    fs::rename(&original_records, &foreign_records).unwrap();
    fs::rename(&saved, &original_records).unwrap();
    assert_eq!(fs::read(segment(&original, id)).unwrap(), original_bytes);
    assert_eq!(
        fs::read(&foreign_segment).unwrap(),
        foreign_bytes,
        "owned stage copy must not repair a substituted foreign generation"
    );
    let retry = create_and_retain_records(&original, &mut failed, &input, &records).unwrap_err();
    assert_eq!(retry.code, rsf_core::ErrorCode::CorruptionDetected);
    assert_eq!(
        retry.message,
        "membership copy durability failed; reopen before further use"
    );
    assert_eq!(fs::read(&foreign_segment).unwrap(), foreign_bytes);
    create_and_retain_records(&original, &mut false, &input, &records).unwrap();
    assert_eq!(read_records(&original, id).unwrap().1, records);
    assert_eq!(fs::read(&foreign_segment).unwrap(), foreign_bytes);
    fs::remove_dir_all(path).unwrap();
}
#[test]
fn stage_copy_reuses_actual_open_recovery_for_empty_partial_and_complete_prefixes() {
    let path = root("stage-copy-owned-prefixes");
    let (input, records) = source(&path);
    let id = input.id().unwrap();
    for count in [0, 1, records.len()] {
        let stage = path.join(format!("copy-{count}"));
        fs::create_dir(&stage).unwrap();
        create_input(&stage, &input).unwrap();
        let mut at = 0;
        while at < count {
            let batch = VsrHistoryBatch::pack(at as u64, &records[at..count]).unwrap();
            at += batch.records.len();
            append_records(&stage, &mut false, &input, batch).unwrap();
        }
        let before = fs::read(segment(&stage, id)).unwrap();
        create_and_retain_records(&stage, &mut false, &input, &records).unwrap();
        let after = fs::read(segment(&stage, id)).unwrap();
        assert!(after.starts_with(&before));
        if count == records.len() {
            assert_eq!(after, before);
        }
        assert_eq!(read_records(&stage, id).unwrap().1, records);
        create_and_retain_records(&stage, &mut false, &input, &records).unwrap();
        assert_eq!(fs::read(segment(&stage, id)).unwrap(), after);
    }
    fs::remove_dir_all(path).unwrap();
}
#[test]
fn stage_copy_open_recovery_keeps_corrupt_prefix_refusal_and_sticky_failure() {
    let path = root("stage-copy-owned-corrupt");
    let (input, records) = source(&path);
    let id = input.id().unwrap();
    let stage = path.join("copy");
    fs::create_dir(&stage).unwrap();
    create_input(&stage, &input).unwrap();
    append_records(
        &stage,
        &mut false,
        &input,
        VsrHistoryBatch::pack(0, &records[..1]).unwrap(),
    )
    .unwrap();
    let segment = segment(&stage, id);
    let mut bytes = fs::read(&segment).unwrap();
    *bytes.last_mut().unwrap() ^= 1;
    fs::write(&segment, &bytes).unwrap();
    let mut failed = false;
    let error = create_and_retain_records(&stage, &mut failed, &input, &records).unwrap_err();
    assert_eq!(error.code, rsf_core::ErrorCode::CorruptionDetected);
    assert!(
        error.message.starts_with("checksum mismatch at offset "),
        "{}",
        error.message
    );
    assert!(failed);
    assert_eq!(fs::read(&segment).unwrap(), bytes);
    assert_eq!(
        create_and_retain_records(&stage, &mut failed, &input, &records)
            .unwrap_err()
            .message,
        "membership copy durability failed; reopen before further use"
    );
    assert_eq!(fs::read(&segment).unwrap(), bytes);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn stage_copy_rejects_same_byte_manifest_replacement_after_actual_open() {
    let path = root("stage-copy-manifest-generation");
    let (input, records) = source(&path);
    let id = input.id().unwrap();
    let stage = path.join("copy");
    fs::create_dir(&stage).unwrap();
    create_input(&stage, &input).unwrap();
    let manifest = directory(&stage, id).join("records/MANIFEST");
    let saved = manifest.with_extension("original");
    let bytes = fs::read(&manifest).unwrap();
    let original_segment = fs::read(segment(&stage, id)).unwrap();
    let target = manifest.clone();
    let original = saved.clone();
    let same_bytes = bytes.clone();
    let hit = std::rc::Rc::new(std::cell::Cell::new(false));
    let witness = hit.clone();
    let guard = hook(move |_| {
        witness.set(true);
        fs::rename(&target, &original).unwrap();
        fs::write(&target, &same_bytes).unwrap();
    });
    let mut failed = false;
    let result = create_and_retain_records(&stage, &mut failed, &input, &records);
    drop(guard);
    assert!(hit.get());
    fs::remove_file(&manifest).unwrap();
    fs::rename(&saved, &manifest).unwrap();
    let error = result.expect_err("owned stage copy must reject a substituted same-byte manifest");
    assert_eq!(error.code, rsf_core::ErrorCode::CorruptionDetected);
    assert_eq!(
        error.message,
        "exclusive store physical custody changed; reopen required"
    );
    assert!(failed);
    assert_eq!(fs::read(&manifest).unwrap(), bytes);
    assert_eq!(fs::read(segment(&stage, id)).unwrap(), original_segment);
    let retry = create_and_retain_records(&stage, &mut failed, &input, &records).unwrap_err();
    assert_eq!(retry.code, rsf_core::ErrorCode::CorruptionDetected);
    assert_eq!(
        retry.message,
        "membership copy durability failed; reopen before further use"
    );
    assert_eq!(fs::read(segment(&stage, id)).unwrap(), original_segment);
    create_and_retain_records(&stage, &mut false, &input, &records).unwrap();
    assert_eq!(read_records(&stage, id).unwrap().1, records);
    fs::remove_dir_all(path).unwrap();
}
