use super::*;

struct Fixture {
    root: PathBuf,
    learner: Option<LearnerHistoryStore>,
    source: Option<DurableVsrCell>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        drop(self.learner.take());
        drop(self.source.take());
        let _ = fs::remove_dir_all(&self.root);
    }
}
fn publish(source: &mut DurableVsrCell, target: &mut LearnerHistoryStore) {
    let snapshot = source.history_snapshot().unwrap();
    let id = snapshot.descriptor.id().unwrap();
    let next = target.begin(snapshot.descriptor).unwrap();
    for (n, record) in snapshot.records.into_iter().enumerate().skip(next as usize) {
        target.append(id, n as u64, record).unwrap();
    }
    target.finish(id).unwrap();
}
fn fixture() -> Fixture {
    let root = std::env::temp_dir().join(format!(
        "rsf-learner-custody-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let mut source = DurableVsrCell::open(
        root.join("source"),
        PlacementGroupId(1),
        Epoch(1),
        NodeId(0),
        [NodeId(0), NodeId(1), NodeId(2)],
    )
    .unwrap();
    source
        .prepare(
            NodeId(0),
            VsrOperation::ReplicatedCommand {
                operation_id: OperationId(1),
                payload: b"first".to_vec(),
            },
            5,
        )
        .unwrap();
    source.acknowledge(NodeId(1), Epoch(1), LogIndex(1));
    source.commit(NodeId(0), LogIndex(1)).unwrap();
    let mut learner = LearnerHistoryStore::open(
        root.join("target"),
        PlacementGroupId(1),
        NodeId(3),
        NodeId(0),
    )
    .unwrap();
    publish(&mut source, &mut learner);
    Fixture {
        root,
        learner: Some(learner),
        source: Some(source),
    }
}
#[test]
fn published_learner_token_rejects_warm_missing_or_corrupt_outer_custody_without_repair() {
    for name in ["IDENTITY", "ACTIVE", "DESCRIPTOR"] {
        for missing in [false, true] {
            let mut f = fixture();
            let learner = f.learner.as_ref().unwrap();
            let token = learner.published_state_token().unwrap();
            let path = if name == "DESCRIPTOR" {
                learner.directory(&token.published).join(name)
            } else {
                learner.root.join(name)
            };
            let before = fs::read(&path).unwrap();
            if missing {
                fs::remove_file(&path).unwrap();
            } else {
                let mut changed = before.clone();
                changed[0] ^= 1;
                fs::write(&path, &changed).unwrap();
            }
            assert!(learner.published_state_token().is_err());
            if missing {
                assert!(!path.exists());
            } else {
                assert_ne!(fs::read(&path).unwrap(), before);
            }
            drop(f.learner.take());
            // Existing low-level learner open may represent absent ACTIVE as
            // unpublished; the recovery-token API must still reject that state.
            if let Ok(cold) = LearnerHistoryStore::open(
                f.root.join("target"),
                PlacementGroupId(1),
                NodeId(3),
                NodeId(0),
            ) {
                assert!(cold.published_state_token().is_err());
            }
        }
    }
}
#[test]
fn published_learner_token_rejects_changed_inner_generation_and_outer_aliases() {
    let f = fixture();
    let learner = f.learner.as_ref().unwrap();
    let token = learner.published_state_token().unwrap();
    let inner = learner.directory(&token.published).join("cell");
    fs::write(inner.join("VSR_ACTIVE"), b"foreign generation pointer").unwrap();
    assert!(learner.published_state_token().is_err());
    assert_eq!(
        fs::read(inner.join("VSR_ACTIVE")).unwrap(),
        b"foreign generation pointer"
    );
    #[cfg(unix)]
    {
        for link in [false, true] {
            let f = fixture();
            let learner = f.learner.as_ref().unwrap();
            let active = learner.root.join("ACTIVE");
            let saved = learner.root.join("saved-active");
            fs::rename(&active, &saved).unwrap();
            if link {
                std::os::unix::fs::symlink(&saved, &active).unwrap();
            } else {
                fs::hard_link(&saved, &active).unwrap();
            }
            assert!(learner.published_state_token().is_err());
            assert!(fs::symlink_metadata(&active).is_ok());
        }
    }
}
#[test]
fn newer_legitimate_learner_publication_changes_opaque_token_without_voting_authority() {
    let mut f = fixture();
    let first = f.learner.as_ref().unwrap().published_state_token().unwrap();
    assert_eq!(
        f.learner.as_ref().unwrap().published_state_token().unwrap(),
        first
    );
    let source = f.source.as_mut().unwrap();
    source
        .prepare(
            NodeId(0),
            VsrOperation::ReplicatedCommand {
                operation_id: OperationId(2),
                payload: b"next".to_vec(),
            },
            4,
        )
        .unwrap();
    source.acknowledge(NodeId(1), Epoch(1), LogIndex(2));
    source.commit(NodeId(0), LogIndex(2)).unwrap();
    publish(source, f.learner.as_mut().unwrap());
    let second = f.learner.as_ref().unwrap().published_state_token().unwrap();
    assert_ne!(second, first);
    assert_eq!(
        f.learner
            .as_ref()
            .unwrap()
            .committed_entries()
            .unwrap()
            .len(),
        2
    );
    assert!(!f.root.join("target/VSR_ACTIVE").exists());
    drop(f.learner.take());
    let cold = LearnerHistoryStore::open(
        f.root.join("target"),
        PlacementGroupId(1),
        NodeId(3),
        NodeId(0),
    )
    .unwrap();
    assert_ne!(
        cold.published_state_token().unwrap(),
        second,
        "opaque token belongs to one owned process handle"
    );
}

#[test]
fn published_learner_token_rejects_missing_inner_directory_and_trailing_dot_root_alias() {
    let f = fixture();
    let learner = f.learner.as_ref().unwrap();
    let token = learner.published_state_token().unwrap();
    let inner = learner.directory(&token.published).join("cell");
    fs::rename(&inner, inner.with_file_name("moved-cell")).unwrap();
    assert!(learner.published_state_token().is_err());
    assert!(!inner.exists());
    #[cfg(unix)]
    {
        let mut f = fixture();
        let learner = f.learner.as_mut().unwrap();
        let alias = f.root.join("aliased-target");
        std::os::unix::fs::symlink(&learner.root, &alias).unwrap();
        learner.root = alias.join(".");
        assert!(learner.published_state_token().is_err());
    }
}

#[cfg(unix)]
#[test]
fn published_default_history_latches_missing_or_changed_physical_store_custody() {
    for fault in ["manifest", "segment", "truncate", "alias", "replace"] {
        let mut f = fixture();
        let learner = f.learner.as_ref().unwrap();
        let token = learner.published_state_token().unwrap();
        let inner = learner.directory(&token.published).join("cell");
        assert!(
            !inner.join("VSR_ACTIVE").exists(),
            "exercise default generation"
        );
        let path = if fault == "manifest" || fault == "replace" {
            inner.join("MANIFEST")
        } else {
            let published = learner.published.as_ref().unwrap();
            inner
                .join("segments")
                .join(&published.cell.store.active_segment().file_name)
        };
        let bytes = fs::read(&path).unwrap();
        match fault {
            "manifest" | "segment" => fs::remove_file(&path).unwrap(),
            "truncate" => OpenOptions::new()
                .write(true)
                .open(&path)
                .unwrap()
                .set_len(0)
                .unwrap(),
            "alias" => {
                let saved = path.with_extension("saved");
                fs::rename(&path, &saved).unwrap();
                std::os::unix::fs::symlink(saved, &path).unwrap();
            }
            "replace" => {
                let replacement = path.with_extension("replacement");
                fs::write(&replacement, &bytes).unwrap();
                fs::rename(replacement, &path).unwrap();
            }
            _ => unreachable!(),
        }
        assert!(learner.published_state_token().is_err(), "{fault}");
        if matches!(fault, "manifest" | "segment") {
            assert!(!path.exists());
            fs::write(&path, &bytes).unwrap();
            assert!(
                learner.published_state_token().is_err(),
                "repairing names cannot clear the warm failure latch"
            );
            drop(f.learner.take());
            let cold = LearnerHistoryStore::open(
                f.root.join("target"),
                PlacementGroupId(1),
                NodeId(3),
                NodeId(0),
            )
            .unwrap();
            assert_ne!(cold.published_state_token().unwrap(), token);
        }
    }
}
