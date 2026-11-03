use super::*;

fn active_segment(learner: &LearnerHistoryStore) -> PathBuf {
    let stage = learner.stage.as_ref().unwrap();
    stage
        .store
        .root()
        .join("segments")
        .join(&stage.store.active_segment().file_name)
}

#[test]
fn finish_recomputes_digest_from_replaced_valid_live_stage_bytes() {
    let root = path("finish-replaced-valid");
    let mut original = source(&root.join("source"));
    commit(&mut original, 1);
    let expected = original.history_snapshot().unwrap();
    let mut other = source(&root.join("other-source"));
    commit(&mut other, 99);
    let changed = other.history_snapshot().unwrap();
    assert_eq!(expected.descriptor.records, changed.descriptor.records);
    assert_ne!(expected.descriptor.digest, changed.descriptor.digest);

    let target = root.join("target");
    let mut receiver = learner(&target);
    let id = stage(&mut receiver, &expected);
    let mut replacement = learner(&root.join("replacement"));
    stage(&mut replacement, &changed);
    // These are complete checksummed WAL records, not a frame-corruption case.
    // The live receiver's cached count/digest still describe the original bytes.
    fs::copy(active_segment(&replacement), active_segment(&receiver)).unwrap();
    assert_eq!(
        receiver.stage.as_ref().unwrap().count,
        expected.descriptor.records
    );
    assert_eq!(
        receiver.stage.as_ref().unwrap().digest,
        expected.descriptor.digest
    );
    let error = receiver.finish(id).unwrap_err();
    assert!(
        error.message.contains("count or digest mismatch"),
        "{error:?}"
    );
    assert!(!target.join("ACTIVE").exists());
    assert!(receiver.committed_entries().unwrap().is_empty());
    drop(receiver);
    assert!(learner(&target).committed_entries().unwrap().is_empty());
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
        if removed.is_dir() {
            fs::remove_dir_all(&removed).unwrap();
        } else {
            fs::remove_file(&removed).unwrap();
        }
        assert!(receiver.finish(id).is_err(), "{missing}");
        assert!(!removed.exists(), "finish recreated missing {missing}");
        assert!(!target.join("ACTIVE").exists());
        assert!(receiver.committed_entries().unwrap().is_empty());
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
    assert!(receiver.finish(id).is_err());
    assert!(!target.join("ACTIVE").exists());
    assert!(receiver.committed_entries().unwrap().is_empty());
    assert_eq!(
        receiver.begin(snapshot.descriptor.clone()).unwrap(),
        snapshot.descriptor.records - 1
    );
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
