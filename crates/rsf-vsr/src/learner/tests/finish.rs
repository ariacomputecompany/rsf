use super::*;

const FENCED: &str = "learner durability failed; reopen before further operations";
fn active_segment(learner: &LearnerHistoryStore) -> PathBuf {
    let stage = learner.stage.as_ref().unwrap();
    stage
        .store
        .root()
        .join("segments")
        .join(&stage.store.active_segment().file_name)
}
#[derive(Debug, Eq, PartialEq)]
struct Entry {
    path: PathBuf,
    identity: (u64, u64),
    bytes: Option<Vec<u8>>,
}
fn identity(metadata: &fs::Metadata) -> (u64, u64) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        (metadata.dev(), metadata.ino())
    }
    #[cfg(not(unix))]
    {
        let _ = metadata;
        (0, 0)
    }
}
fn inventory(root: &Path) -> Vec<Entry> {
    fn visit(root: &Path, path: &Path, entries: &mut Vec<Entry>) {
        let metadata = fs::symlink_metadata(path).unwrap();
        assert!(!metadata.file_type().is_symlink());
        entries.push(Entry {
            path: path.strip_prefix(root).unwrap().to_path_buf(),
            identity: identity(&metadata),
            bytes: if metadata.is_dir() {
                None
            } else {
                Some(fs::read(path).unwrap())
            },
        });
        if metadata.is_dir() {
            for item in fs::read_dir(path).unwrap() {
                visit(root, &item.unwrap().path(), entries);
            }
        }
    }
    let mut entries = Vec::new();
    visit(root, root, &mut entries);
    entries.sort_by(|a, b| a.path.cmp(&b.path));
    entries
}
fn fenced(receiver: &mut LearnerHistoryStore, snapshot: &VsrHistorySnapshot) {
    assert_eq!(receiver.committed_entries().unwrap_err().message, FENCED);
    assert_eq!(
        receiver
            .begin(snapshot.descriptor.clone())
            .unwrap_err()
            .message,
        FENCED
    );
    assert_eq!(
        receiver
            .finish(snapshot.descriptor.id().unwrap())
            .unwrap_err()
            .message,
        FENCED
    );
}
fn changed_histories(
    root: &Path,
) -> (
    DurableVsrCell,
    DurableVsrCell,
    VsrHistorySnapshot,
    VsrHistorySnapshot,
) {
    let mut original = source(&root.join("source"));
    commit(&mut original, 1);
    let expected = original.history_snapshot().unwrap();
    let mut other = source(&root.join("other-source"));
    commit(&mut other, 99);
    let changed = other.history_snapshot().unwrap();
    assert_eq!(expected.descriptor.records, changed.descriptor.records);
    assert_ne!(expected.descriptor.digest, changed.descriptor.digest);
    (original, other, expected, changed)
}
#[test]
fn finish_recomputes_digest_from_replaced_valid_live_stage_bytes() {
    let root = path("finish-replaced-valid");
    let (original, other, expected, changed) = changed_histories(&root);
    let target = root.join("target");
    let mut receiver = learner(&target);
    let id = stage(&mut receiver, &expected);
    let mut replacement = learner(&root.join("replacement"));
    stage(&mut replacement, &changed);
    let segment = active_segment(&receiver);
    let original_metadata = fs::metadata(&segment).unwrap();
    let original_bytes = fs::read(&segment).unwrap();
    // Complete valid frames replace the bytes through the same destination inode.
    fs::copy(active_segment(&replacement), &segment).unwrap();
    // Make the observed physical change deterministic even on coarse clocks.
    File::open(&segment)
        .unwrap()
        .set_times(fs::FileTimes::new().set_modified(
            original_metadata.modified().unwrap() + std::time::Duration::from_secs(1),
        ))
        .unwrap();
    assert_eq!(
        identity(&fs::metadata(&segment).unwrap()),
        identity(&original_metadata)
    );
    assert_eq!(
        receiver.stage.as_ref().unwrap().count,
        expected.descriptor.records
    );
    assert_eq!(
        receiver.stage.as_ref().unwrap().digest,
        expected.descriptor.digest
    );
    let damaged = inventory(&target);
    let error = receiver.finish(id).unwrap_err();
    assert_eq!(error.code, ErrorCode::CorruptionDetected);
    assert_eq!(
        error.message,
        "owned segment changed after validated WAL state"
    );
    assert!(!target.join("ACTIVE").exists());
    assert_eq!(inventory(&target), damaged);
    fs::write(&segment, &original_bytes).unwrap();
    File::open(&segment)
        .unwrap()
        .set_times(fs::FileTimes::new().set_modified(original_metadata.modified().unwrap()))
        .unwrap();
    assert_eq!(
        identity(&fs::metadata(&segment).unwrap()),
        identity(&original_metadata)
    );
    assert_eq!(fs::read(&segment).unwrap(), original_bytes);
    fenced(&mut receiver, &expected);
    drop(receiver);
    let mut cold = learner(&target);
    assert!(cold.committed_entries().unwrap().is_empty());
    assert_eq!(
        cold.begin(expected.descriptor.clone()).unwrap(),
        expected.descriptor.records
    );
    assert_eq!(cold.finish(id).unwrap(), LogIndex(1));
    assert_eq!(
        cold.committed_entries().unwrap(),
        original.committed_prefix().unwrap()
    );
    drop(cold);
    drop(replacement);
    drop(other);
    drop(original);
    fs::remove_dir_all(root).unwrap();
}
#[test]
fn cold_recovered_valid_changed_stage_preserves_semantic_digest_refusal() {
    let root = path("finish-cold-valid-digest");
    let (original, other, expected, changed) = changed_histories(&root);
    let target = root.join("target");
    let mut receiver = learner(&target);
    let id = stage(&mut receiver, &expected);
    let segment = active_segment(&receiver);
    drop(receiver);
    let mut replacement = learner(&root.join("replacement"));
    stage(&mut replacement, &changed);
    fs::copy(active_segment(&replacement), &segment).unwrap();
    let mut cold = learner(&target);
    assert_eq!(
        cold.begin(expected.descriptor.clone()).unwrap(),
        expected.descriptor.records
    );
    assert_eq!(
        cold.stage.as_ref().unwrap().digest,
        changed.descriptor.digest
    );
    let before = inventory(&target);
    let error = cold.finish(id).unwrap_err();
    assert_eq!(error.code, ErrorCode::CorruptionDetected);
    assert_eq!(error.message, "learner history count or digest mismatch");
    assert!(!target.join("ACTIVE").exists());
    assert_eq!(inventory(&target), before);
    assert!(cold.committed_entries().unwrap().is_empty());
    // The refusal is semantic, not a physical fence; an explicit different
    // descriptor can replace the provisional transfer and publish valid bytes.
    let changed_id = stage(&mut cold, &changed);
    assert_eq!(cold.finish(changed_id).unwrap(), LogIndex(1));
    assert_eq!(
        cold.committed_entries().unwrap(),
        other.committed_prefix().unwrap()
    );
    drop(cold);
    drop(replacement);
    drop(other);
    drop(original);
    fs::remove_dir_all(root).unwrap();
}
#[test]
fn finish_missing_live_stage_custody_does_not_recreate_it() {
    let root = path("finish-missing-custody");
    let mut source = source(&root.join("source"));
    commit(&mut source, 1);
    let snapshot = source.history_snapshot().unwrap();
    for missing in ["manifest", "segment", "directory"] {
        let target = root.join(missing);
        let mut receiver = learner(&target);
        let id = stage(&mut receiver, &snapshot);
        let cell = receiver.stage.as_ref().unwrap().store.root().to_path_buf();
        let removed = match missing {
            "manifest" => cell.join("MANIFEST"),
            "segment" => active_segment(&receiver),
            "directory" => cell,
            _ => unreachable!(),
        };
        let original = identity(&fs::metadata(&removed).unwrap());
        let saved = root.join(format!("original-{missing}"));
        fs::rename(&removed, &saved).unwrap();
        let cause = fs::symlink_metadata(&removed).unwrap_err();
        let before = inventory(&target);
        let error = receiver.finish(id).unwrap_err();
        if missing == "directory" {
            assert_eq!(error.code, ErrorCode::CorruptionDetected);
            assert_eq!(error.message, format!("learner history storage: {cause}"));
        } else {
            assert_eq!(error.code, ErrorCode::InvalidInput);
            assert_eq!(error.message, format!("storage Recover failed: {cause}"));
        }
        assert!(!removed.exists(), "finish recreated missing {missing}");
        assert!(!target.join("ACTIVE").exists());
        assert_eq!(inventory(&target), before);
        fs::rename(saved, &removed).unwrap();
        assert_eq!(identity(&fs::metadata(&removed).unwrap()), original);
        fenced(&mut receiver, &snapshot);
        drop(receiver);
        let mut cold = learner(&target);
        assert!(cold.committed_entries().unwrap().is_empty());
        assert_eq!(
            cold.begin(snapshot.descriptor.clone()).unwrap(),
            snapshot.descriptor.records
        );
        assert_eq!(cold.finish(id).unwrap(), LogIndex(1));
        assert_eq!(
            cold.committed_entries().unwrap(),
            source.committed_prefix().unwrap()
        );
        drop(cold);
    }
    drop(source);
    fs::remove_dir_all(root).unwrap();
}
#[test]
fn finish_revalidates_torn_live_tail_before_resumable_retry() {
    let root = path("finish-torn-live-tail");
    let mut source = source(&root.join("source"));
    commit(&mut source, 1);
    let snapshot = source.history_snapshot().unwrap();
    let target = root.join("target");
    let mut receiver = learner(&target);
    let id = stage(&mut receiver, &snapshot);
    let segment = active_segment(&receiver);
    let length = fs::metadata(&segment).unwrap().len();
    OpenOptions::new()
        .write(true)
        .open(&segment)
        .unwrap()
        .set_len(length - 5)
        .unwrap();
    let before = inventory(&target);
    let error = receiver.finish(id).unwrap_err();
    assert_eq!(error.code, ErrorCode::CorruptionDetected);
    assert_eq!(
        error.message,
        "owned segment changed after validated WAL state"
    );
    assert!(!target.join("ACTIVE").exists());
    assert_eq!(inventory(&target), before);
    assert_eq!(fs::metadata(&segment).unwrap().len(), length - 5);
    fenced(&mut receiver, &snapshot);
    drop(receiver);
    // Only a distinct cold unpublished owner may run ordinary provisional repair.
    let mut receiver = learner(&target);
    assert!(receiver.committed_entries().unwrap().is_empty());
    assert_eq!(
        receiver.begin(snapshot.descriptor.clone()).unwrap(),
        snapshot.descriptor.records - 1
    );
    assert!(fs::metadata(&segment).unwrap().len() < length - 5);
    stage(&mut receiver, &snapshot);
    assert_eq!(receiver.finish(id).unwrap(), LogIndex(1));
    assert_eq!(
        receiver.committed_entries().unwrap(),
        source.committed_prefix().unwrap()
    );
    drop(receiver);
    let receiver = learner(&target);
    assert_eq!(
        receiver.committed_entries().unwrap(),
        source.committed_prefix().unwrap()
    );
    assert_eq!(
        receiver
            .published
            .as_ref()
            .unwrap()
            .cell
            .request_commit(&VsrClientRequestId::new("client", 1)),
        Some(LogIndex(1))
    );
    drop(receiver);
    drop(source);
    fs::remove_dir_all(root).unwrap();
}
