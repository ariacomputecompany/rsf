use super::*;

// Include directories as well as files: an error must neither repair missing
// custody nor reclaim an unrelated retained transfer before rejecting the ACK.
fn tree(root: &Path) -> BTreeMap<PathBuf, Option<Vec<u8>>> {
    fn visit(base: &Path, path: &Path, entries: &mut BTreeMap<PathBuf, Option<Vec<u8>>>) {
        for entry in fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            let relative = path.strip_prefix(base).unwrap().to_path_buf();
            if entry.file_type().unwrap().is_dir() {
                entries.insert(relative, None);
                visit(base, &path, entries);
            } else {
                entries.insert(relative, Some(fs::read(path).unwrap()));
            }
        }
    }
    let mut entries = BTreeMap::new();
    visit(root, root, &mut entries);
    entries
}

fn cached_call(
    learner: &mut LearnerHistoryStore,
    operation: &str,
    descriptor: &VsrHistoryDescriptor,
) -> RsfResult<()> {
    let id = descriptor.id()?;
    match operation {
        "begin" => learner.begin(descriptor.clone()).map(|_| ()),
        "finish" => learner.finish(id).map(|_| ()),
        "id" => learner.published_history_id().map(|_| ()),
        "entries" => learner.committed_entries().map(|_| ()),
        _ => unreachable!(),
    }
}

#[test]
fn cached_published_learner_success_requires_outer_and_inner_custody_before_mutation() {
    for fault in ["manifest", "segment", "descriptor", "identity", "active"] {
        // Each endpoint must detect damage itself, rather than merely observe a
        // failure latched by a previous token/getter call in the test.
        for operation in ["begin", "finish", "id", "entries"] {
            let mut f = fixture();
            let descriptor = f
                .source
                .as_mut()
                .unwrap()
                .history_snapshot()
                .unwrap()
                .descriptor;
            let learner = f.learner.as_mut().unwrap();
            let id = descriptor.id().unwrap();
            let directory = learner.directory(&id);
            let cell = directory.join("cell");
            let damaged = match fault {
                "manifest" => cell.join("MANIFEST"),
                "segment" => cell.join("segments").join(
                    &learner
                        .published
                        .as_ref()
                        .unwrap()
                        .cell
                        .store
                        .active_segment()
                        .file_name,
                ),
                "descriptor" => directory.join("DESCRIPTOR"),
                "identity" => learner.root.join("IDENTITY"),
                "active" => learner.root.join("ACTIVE"),
                _ => unreachable!(),
            };
            let saved = fs::read(&damaged).unwrap();
            fs::remove_file(&damaged).unwrap();
            let unrelated = learner.root.join("f".repeat(64));
            assert_ne!(unrelated, directory);
            fs::create_dir(&unrelated).unwrap();
            fs::write(
                unrelated.join("sentinel"),
                b"retain until validated cleanup",
            )
            .unwrap();
            let before = tree(&learner.root);
            assert!(
                cached_call(learner, operation, &descriptor).is_err(),
                "{fault}/{operation}"
            );
            assert!(
                !damaged.exists(),
                "{fault}/{operation}: missing custody recreated"
            );
            assert_eq!(
                tree(&learner.root),
                before,
                "{fault}/{operation}: failed call mutated disk"
            );

            // Restored outer bytes cannot clear the live failure latch. Inner
            // names likewise require a new owner and fresh physical stamp.
            fs::write(&damaged, &saved).unwrap();
            let restored = tree(&learner.root);
            for retry in ["begin", "finish", "id", "entries"] {
                assert!(
                    cached_call(learner, retry, &descriptor).is_err(),
                    "{fault}/{operation}/{retry}"
                );
            }
            assert!(learner.published_state_token().is_err());
            assert_eq!(tree(&learner.root), restored);
            let target = learner.root.clone();
            drop(f.learner.take());
            let mut cold =
                LearnerHistoryStore::open(target, PlacementGroupId(1), NodeId(3), NodeId(0))
                    .unwrap();
            assert_eq!(cold.published_history_id().unwrap(), Some(id));
            assert_eq!(cold.begin(descriptor.clone()).unwrap(), descriptor.records);
            assert_eq!(cold.finish(id).unwrap(), descriptor.probe.committed_tail);
            assert_eq!(
                cold.committed_entries().unwrap(),
                f.source.as_ref().unwrap().committed_prefix().unwrap()
            );
        }
    }
}

#[test]
fn unpublished_token_error_does_not_poison_later_valid_publication_and_retries() {
    let mut f = fixture();
    let target = f.root.join("unpublished");
    let mut learner =
        LearnerHistoryStore::open(&target, PlacementGroupId(1), NodeId(3), NodeId(0)).unwrap();
    assert!(learner.published_state_token().is_err());
    assert_eq!(learner.published_history_id().unwrap(), None);
    assert!(learner.committed_entries().unwrap().is_empty());
    publish(f.source.as_mut().unwrap(), &mut learner);
    let descriptor = f
        .source
        .as_mut()
        .unwrap()
        .history_snapshot()
        .unwrap()
        .descriptor;
    let id = descriptor.id().unwrap();
    let token = learner.published_state_token().unwrap();
    for _ in 0..2 {
        assert_eq!(
            learner.begin(descriptor.clone()).unwrap(),
            descriptor.records
        );
        assert_eq!(learner.finish(id).unwrap(), descriptor.probe.committed_tail);
        assert_eq!(learner.published_history_id().unwrap(), Some(id));
        assert_eq!(learner.published_state_token().unwrap(), token);
        assert_eq!(
            learner.committed_entries().unwrap(),
            f.source.as_ref().unwrap().committed_prefix().unwrap()
        );
    }
    drop(learner);
    let mut cold =
        LearnerHistoryStore::open(target, PlacementGroupId(1), NodeId(3), NodeId(0)).unwrap();
    assert_eq!(cold.begin(descriptor.clone()).unwrap(), descriptor.records);
    assert_eq!(cold.finish(id).unwrap(), descriptor.probe.committed_tail);
    assert_eq!(cold.published_history_id().unwrap(), Some(id));
    assert_ne!(cold.published_state_token().unwrap(), token);
}
