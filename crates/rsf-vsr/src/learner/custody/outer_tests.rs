//! Existing-public-API physical regressions. Root executes old/new implementations.
use super::*;
use std::sync::atomic::AtomicU64;

const FENCED: &str = "learner durability failed; reopen before further operations";
fn limits() -> VsrPrefixLimits {
    VsrPrefixLimits {
        max_records: 128,
        max_encoded_bytes: 1024 * 1024,
    }
}
struct Fixture {
    root: PathBuf,
    source: Option<DurableVsrCell>,
    learner: Option<LearnerHistoryStore>,
}
impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = loop {
            let path = std::env::temp_dir().join(format!(
                "rsf-outer-custody-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            match fs::create_dir(&path) {
                Ok(()) => break path,
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(e) => panic!("reserve fixture: {e}"),
            }
        };
        let mut source = DurableVsrCell::open(
            root.join("source"),
            PlacementGroupId(1),
            Epoch(1),
            NodeId(0),
            [NodeId(0), NodeId(1), NodeId(2)],
        )
        .unwrap();
        commit(&mut source, 1);
        let mut learner = LearnerHistoryStore::open(
            root.join("target"),
            PlacementGroupId(1),
            NodeId(3),
            NodeId(0),
        )
        .unwrap();
        transfer(&mut source, &mut learner);
        Self {
            root,
            source: Some(source),
            learner: Some(learner),
        }
    }
    fn owner(&self) -> &LearnerHistoryStore {
        self.learner.as_ref().unwrap()
    }
    fn reopen(&mut self, strict: bool) {
        drop(self.learner.take());
        self.learner = Some(
            if strict {
                LearnerHistoryStore::open_published_existing(
                    self.root.join("target"),
                    PlacementGroupId(1),
                    NodeId(3),
                    NodeId(0),
                    limits(),
                )
            } else {
                LearnerHistoryStore::open(
                    self.root.join("target"),
                    PlacementGroupId(1),
                    NodeId(3),
                    NodeId(0),
                )
            }
            .unwrap(),
        );
    }
    fn selected(&self) -> PathBuf {
        self.owner()
            .directory(&self.owner().published_history_id().unwrap().unwrap())
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        drop(self.learner.take());
        drop(self.source.take());
        let _ = fs::remove_dir_all(&self.root);
    }
}
fn commit(source: &mut DurableVsrCell, n: u64) {
    let VsrDecision::Prepared(p) = source
        .prepare(
            source.primary(),
            VsrOperation::ReplicatedCommand {
                operation_id: OperationId(n),
                payload: vec![n as u8],
            },
            1,
        )
        .unwrap()
    else {
        panic!("prepare");
    };
    source.acknowledge(NodeId(1), source.view(), p.index);
    assert!(matches!(
        source.commit(source.primary(), p.index).unwrap(),
        VsrDecision::Committed(_)
    ));
}
fn transfer(source: &mut DurableVsrCell, learner: &mut LearnerHistoryStore) {
    let snapshot = source.history_snapshot().unwrap();
    let id = snapshot.descriptor.id().unwrap();
    let start = learner.begin(snapshot.descriptor).unwrap();
    for (i, record) in snapshot
        .records
        .into_iter()
        .enumerate()
        .skip(start as usize)
    {
        learner.append(id, i as u64, record).unwrap();
    }
    learner.finish(id).unwrap();
}
#[derive(Debug, Eq, PartialEq)]
struct Entry {
    directory: bool,
    device: u64,
    inode: u64,
    bytes: Vec<u8>,
}
fn inventory(root: &Path) -> std::collections::BTreeMap<PathBuf, Entry> {
    fn visit(root: &Path, path: &Path, out: &mut std::collections::BTreeMap<PathBuf, Entry>) {
        let metadata = fs::symlink_metadata(path).unwrap();
        assert!(
            !metadata.file_type().is_symlink(),
            "fixture inventory has an alias"
        );
        #[cfg(unix)]
        let (device, inode) = {
            use std::os::unix::fs::MetadataExt;
            (metadata.dev(), metadata.ino())
        };
        #[cfg(not(unix))]
        let (device, inode) = (0, 0);
        let directory = metadata.is_dir();
        out.insert(
            path.strip_prefix(root).unwrap().to_path_buf(),
            Entry {
                directory,
                device,
                inode,
                bytes: if directory {
                    Vec::new()
                } else {
                    fs::read(path).unwrap()
                },
            },
        );
        if directory {
            for entry in fs::read_dir(path).unwrap() {
                visit(root, &entry.unwrap().path(), out);
            }
        }
    }
    let mut out = std::collections::BTreeMap::new();
    visit(root, root, &mut out);
    out
}

fn fenced(owner: &LearnerHistoryStore) {
    let error = owner.published_state_token().unwrap_err();
    assert_eq!(error.code, ErrorCode::CorruptionDetected);
    assert_eq!(error.message, FENCED);
    assert_eq!(owner.committed_entries().unwrap_err().message, FENCED);
    assert_eq!(owner.published_replay().unwrap_err().message, FENCED);
}
fn missing_then_restore(strict: bool, name: &str) {
    let mut f = Fixture::new();
    if strict {
        f.reopen(true);
    }
    let path = if name == ".vsr-writer.lock" {
        f.selected().join("cell").join(name)
    } else {
        f.owner().root.join(name)
    };
    let saved = f.root.join("original");
    let original = fs::metadata(&path).unwrap();
    let before = inventory(&f.owner().root);
    fs::rename(&path, &saved).unwrap();
    let cause = fs::symlink_metadata(&path).unwrap_err();
    let error = f.owner().published_state_token().unwrap_err();
    assert_eq!(error.code, ErrorCode::CorruptionDetected);
    assert_eq!(error.message, format!("learner history storage: {cause}"));
    assert!(!path.exists());
    fs::rename(&saved, &path).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let restored = fs::metadata(&path).unwrap();
        assert_eq!(
            (restored.dev(), restored.ino()),
            (original.dev(), original.ino())
        );
    }
    assert_eq!(inventory(&f.owner().root), before);
    fenced(f.owner());
    f.reopen(strict);
    assert_eq!(f.owner().committed_entries().unwrap().len(), 1);
}
fn replaced_then_restore(strict: bool, name: &str) {
    let mut f = Fixture::new();
    if strict {
        f.reopen(true);
    }
    let path = if name == "DESCRIPTOR" {
        f.selected().join(name)
    } else {
        f.owner().root.join(name)
    };
    let before = inventory(&f.owner().root);
    let saved = f.root.join("original");
    let bytes = fs::read(&path).unwrap();
    fs::rename(&path, &saved).unwrap();
    fs::write(&path, &bytes).unwrap();
    assert_eq!(fs::read(&path).unwrap(), bytes);
    let error = f.owner().published_state_token().unwrap_err();
    assert_eq!(error.code, ErrorCode::CorruptionDetected);
    assert_eq!(error.message, "learner publication metadata was replaced");
    fs::remove_file(&path).unwrap();
    fs::rename(&saved, &path).unwrap();
    assert_eq!(inventory(&f.owner().root), before);
    fenced(f.owner());
    f.reopen(strict);
    f.owner().published_state_token().unwrap();
}
#[test]
fn ordinary_outer_lock_loss_is_observed_and_original_inode_restore_stays_fenced() {
    missing_then_restore(false, ".learner.lock");
}
#[test]
fn strict_outer_lock_loss_is_observed_and_original_inode_restore_stays_fenced() {
    missing_then_restore(true, ".learner.lock");
}
#[test]
fn ordinary_inner_lock_loss_is_observed_and_original_inode_restore_stays_fenced() {
    missing_then_restore(false, ".vsr-writer.lock");
}
#[test]
fn strict_outer_lock_equal_byte_replacement_is_not_recaptured() {
    replaced_then_restore(true, ".learner.lock");
}
#[test]
fn ordinary_identity_equal_byte_replacement_is_not_recaptured() {
    replaced_then_restore(false, "IDENTITY");
}
#[test]
fn strict_active_equal_byte_replacement_is_not_recaptured() {
    replaced_then_restore(true, "ACTIVE");
}
#[test]
fn ordinary_descriptor_equal_byte_replacement_is_not_recaptured() {
    replaced_then_restore(false, "DESCRIPTOR");
}

#[test]
fn unpublished_outer_owner_loss_fences_before_begin_can_create_a_candidate() {
    let mut f = Fixture::new();
    drop(f.learner.take());
    fs::remove_dir_all(f.root.join("target")).unwrap();
    let mut owner = LearnerHistoryStore::open(
        f.root.join("target"),
        PlacementGroupId(1),
        NodeId(3),
        NodeId(0),
    )
    .unwrap();
    let path = owner.root.join(".learner.lock");
    let saved = f.root.join("held");
    fs::rename(&path, &saved).unwrap();
    let before = inventory(&owner.root);
    let cause = fs::symlink_metadata(&path).unwrap_err();
    let descriptor = f
        .source
        .as_mut()
        .unwrap()
        .history_snapshot()
        .unwrap()
        .descriptor;
    assert_eq!(
        owner.begin(descriptor.clone()).unwrap_err().message,
        format!("learner history storage: {cause}")
    );
    assert_eq!(inventory(&owner.root), before);
    fs::rename(saved, path).unwrap();
    assert_eq!(owner.begin(descriptor).unwrap_err().message, FENCED);
}
fn cold_loss(name: &str) {
    let mut f = Fixture::new();
    let cell = f.selected().join("cell");
    drop(f.learner.take());
    let path = cell.join(name);
    let saved = f.root.join("held");
    fs::rename(&path, &saved).unwrap();
    let before = inventory(&f.root.join("target"));
    let error = LearnerHistoryStore::open(
        f.root.join("target"),
        PlacementGroupId(1),
        NodeId(3),
        NodeId(0),
    )
    .unwrap_err();
    assert!(
        !path.exists(),
        "cold publication must not recreate acknowledged missing metadata"
    );
    assert_eq!(
        inventory(&f.root.join("target")),
        before,
        "cold publication refusal must not repair files"
    );
    if name == ".vsr-writer.lock" {
        assert_eq!(error.code, ErrorCode::CorruptionDetected);
        assert_eq!(
            error.message,
            format!(
                "learner history storage: {}",
                fs::symlink_metadata(&path).unwrap_err()
            )
        );
    } else {
        assert_eq!(error.code, ErrorCode::InvalidInput);
        assert!(
            error.message.starts_with("storage Recover failed:"),
            "{error:?}"
        );
    }
    assert!(!path.exists());
    assert_eq!(inventory(&f.root.join("target")), before);
    fs::rename(saved, path).unwrap();
    f.reopen(false);
    f.owner().published_state_token().unwrap();
}
#[test]
fn ordinary_cold_published_missing_manifest_is_not_recreated() {
    cold_loss("MANIFEST");
}
#[test]
fn ordinary_cold_published_missing_writer_lock_is_not_recreated() {
    cold_loss(".vsr-writer.lock");
}
#[test]
fn ordinary_cold_published_torn_tail_is_rejected_without_truncation() {
    let mut f = Fixture::new();
    let segments = f.selected().join("cell/segments");
    drop(f.learner.take());
    let path = fs::read_dir(segments)
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| p.extension().is_some_and(|x| x == "segment"))
        .unwrap();
    let mut file = OpenOptions::new().append(true).open(&path).unwrap();
    file.write_all(b"torn").unwrap();
    file.sync_all().unwrap();
    drop(file);
    let before = inventory(&f.root.join("target"));
    let error = LearnerHistoryStore::open(
        f.root.join("target"),
        PlacementGroupId(1),
        NodeId(3),
        NodeId(0),
    )
    .unwrap_err();
    assert_eq!(error.code, ErrorCode::CorruptionDetected);
    assert_eq!(inventory(&f.root.join("target")), before);
    assert!(fs::read(path).unwrap().ends_with(b"torn"));
}
fn callback_replacement(semantic_error: bool) {
    let mut f = Fixture::new();
    let owner = f.learner.as_mut().unwrap();
    let descriptor = f
        .source
        .as_mut()
        .unwrap()
        .history_snapshot()
        .unwrap()
        .descriptor;
    let id = descriptor.id().unwrap();
    owner.begin_prefix(descriptor, limits()).unwrap();
    let path = owner.root.join("ACTIVE");
    let saved = f.root.join("held");
    let bytes = fs::read(&path).unwrap();
    let error = owner
        .finish_prefix(id, |_| {
            fs::rename(&path, &saved).unwrap();
            fs::write(&path, &bytes).unwrap();
            if semantic_error {
                Err(invalid_input("healthy semantic refusal"))
            } else {
                Ok(())
            }
        })
        .unwrap_err();
    assert_eq!(error.message, "learner publication metadata was replaced");
    fs::remove_file(&path).unwrap();
    fs::rename(saved, path).unwrap();
    fenced(owner);
}
#[test]
fn bounded_callback_success_cannot_adopt_replaced_active() {
    callback_replacement(false);
}
#[test]
fn bounded_callback_semantic_error_cannot_hide_replaced_active() {
    callback_replacement(true);
}
#[test]
fn ordinary_and_bounded_publications_renew_only_their_actual_active_transition() {
    let mut f = Fixture::new();
    let first = f.owner().published_state_token().unwrap();
    commit(f.source.as_mut().unwrap(), 2);
    transfer(f.source.as_mut().unwrap(), f.learner.as_mut().unwrap());
    let second = f.owner().published_state_token().unwrap();
    assert_ne!(first, second);
    f.reopen(true);
    let before = f.owner().published_state_token().unwrap();
    commit(f.source.as_mut().unwrap(), 3);
    let export = f
        .source
        .as_ref()
        .unwrap()
        .export_committed_prefix(LogIndex(3), limits())
        .unwrap();
    let id = export.descriptor().id().unwrap();
    let owner = f.learner.as_mut().unwrap();
    let mut ordinal = owner
        .begin_prefix(export.descriptor().clone(), limits())
        .unwrap();
    while ordinal < export.descriptor().records {
        let batch = f
            .source
            .as_ref()
            .unwrap()
            .read_committed_prefix_batch(&export, ordinal)
            .unwrap();
        ordinal = owner.append_prefix_batch(id, &batch).unwrap();
    }
    assert_eq!(
        owner
            .finish_prefix(id, |preview| {
                assert_eq!(preview.committed_tail(), LogIndex(3));
                Err(invalid_input("policy refusal"))
            })
            .unwrap_err()
            .message,
        "policy refusal"
    );
    assert_eq!(owner.published_state_token().unwrap(), before);
    assert_eq!(
        owner
            .finish_prefix(id, |preview| preview.validate_live())
            .unwrap(),
        LogIndex(3)
    );
    let after = owner.published_state_token().unwrap();
    assert_ne!(after, before);
    assert_eq!(
        owner
            .finish_prefix(id, |preview| preview.validate_live())
            .unwrap(),
        LogIndex(3)
    );
    assert_eq!(owner.published_state_token().unwrap(), after);
    f.reopen(false);
    assert_eq!(f.owner().committed_entries().unwrap().len(), 3);
}

mod transitions;
