use super::*;

#[test]
fn published_replay_borrows_only_committed_history_and_preserves_cold_lineage() {
    let root = path("borrowed-replay");
    let mut source = source(&root.join("source"));
    commit(&mut source, 1);
    source
        .prepare(
            NodeId(0),
            VsrOperation::ReplicatedCommand {
                operation_id: OperationId(2),
                payload: b"accepted only".to_vec(),
            },
            13,
        )
        .unwrap();
    let mut target = learner(&root.join("target"));
    assert!(target.published_replay().is_err());
    let snapshot = source.history_snapshot().unwrap();
    let id = stage(&mut target, &snapshot);
    assert!(target.published_replay().is_err());
    target.finish(id).unwrap();
    for _ in 0..2 {
        let view = target.published_replay().unwrap();
        assert_eq!(view.committed_tail(), LogIndex(1));
        assert_eq!(view.committed_count(), 1);
        assert_eq!(
            view.committed_prepare(LogIndex(1)),
            source.prepared(LogIndex(1))
        );
        assert!(view.committed_prepare(LogIndex(2)).is_none());
        assert_eq!(
            view.membership_lineage().unwrap(),
            source.membership_lineage().unwrap()
        );
        assert_eq!(
            view.membership_lineage_at(Epoch(1)).unwrap(),
            source.membership_lineage_at(Epoch(1)).unwrap()
        );
        let mut observed = Vec::new();
        view.for_each_committed(|prepare, commit| {
            observed.push(VsrCommittedEntry {
                prepare: prepare.clone(),
                commit: commit.clone(),
            });
            Ok(())
        })
        .unwrap();
        assert_eq!(observed, source.committed_prefix().unwrap());
        assert_eq!(view.state_token(), &target.published_state_token().unwrap());
        drop(target);
        target = learner(&root.join("target"));
    }
    drop(target);
    drop(source);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn published_replay_checks_outer_custody_at_completion_and_never_recreates_it() {
    let root = path("borrowed-replay-loss");
    let mut source = source(&root.join("source"));
    commit(&mut source, 1);
    let mut target = learner(&root.join("target"));
    let id = stage(&mut target, &source.history_snapshot().unwrap());
    target.finish(id).unwrap();
    let view = target.published_replay().unwrap();
    let active = root.join("target/ACTIVE");
    let mut count = 0;
    let result = view.for_each_committed(|_, _| {
        count += 1;
        fs::remove_file(&active).unwrap();
        Ok(())
    });
    assert_eq!(count, 1);
    assert!(result.is_err());
    assert!(view.validate_live().is_err());
    assert!(view.membership_lineage().is_err());
    assert!(!active.exists());
    drop(target);
    drop(source);
    fs::remove_dir_all(root).unwrap();
}

const CALLBACK_REFUSAL: &str = "semantic callback refused this candidate";

fn callback_fixture(label: &str) -> (PathBuf, DurableVsrCell, LearnerHistoryStore) {
    let root = path(label);
    let mut source = source(&root.join("source"));
    commit(&mut source, 1);
    let payload = b"accepted callback work";
    assert!(matches!(
        source
            .prepare(
                NodeId(0),
                VsrOperation::ReplicatedCommand {
                    operation_id: OperationId(2),
                    payload: payload.to_vec(),
                },
                payload.len() as u32
            )
            .unwrap(),
        VsrDecision::Prepared(_)
    ));
    let mut target = learner(&root.join("target"));
    let id = stage(&mut target, &source.history_snapshot().unwrap());
    target.finish(id).unwrap();
    (root, source, target)
}

fn visit_callback(
    view: &PublishedVsrReplay<'_>,
    accepted: bool,
    mut callback: impl FnMut() -> RsfResult<()>,
) -> RsfResult<()> {
    if accepted {
        view.for_each_accepted_tail(|prepare| {
            assert_eq!(prepare.index, LogIndex(2));
            callback()
        })
    } else {
        view.for_each_committed(|prepare, commit| {
            assert_eq!(prepare.index, LogIndex(1));
            assert_eq!(commit.index, prepare.index);
            callback()
        })
    }
}

fn callback_faults(accepted: bool) {
    for fault in 0..3 {
        let (root, source, target) = callback_fixture(if accepted {
            "accepted-callback-fault"
        } else {
            "committed-callback-fault"
        });
        let id = target.published_history_id().unwrap().unwrap();
        let affected = match fault {
            0 => root.join("target"),
            1 => root.join("target/ACTIVE"),
            2 => target.directory(&id).join("cell/MANIFEST"),
            _ => unreachable!(),
        };
        // Keep the actual original inode/bytes for restoration. Rewriting its
        // contents would independently invalidate custody and mask this bug.
        let retained = root.join("temporarily-retained-custody");
        let view = target.published_replay().unwrap();
        let mut visited = 0;
        let result = visit_callback(&view, accepted, || {
            visited += 1;
            fs::rename(&affected, &retained).unwrap();
            Err(invalid_input(CALLBACK_REFUSAL))
        });
        assert_eq!(visited, 1);
        // Do not probe owner health before restoration: the traversal itself
        // must have observed and latched the missing path.
        fs::rename(&retained, &affected).unwrap();
        let live = view.validate_live();
        let token = target.published_state_token();
        let error = result.unwrap_err();
        assert_ne!(
            error.message, CALLBACK_REFUSAL,
            "physical custody must dominate callback Err"
        );
        assert!(
            live.is_err(),
            "restoring exact original custody revived the failed observation"
        );
        assert!(
            token
                .unwrap_err()
                .message
                .contains("learner durability failed")
        );
        drop(target);
        // Proper cold reopening of the restored exact bytes is permitted.
        let reopened = learner(&root.join("target"));
        let cold = reopened.published_replay().unwrap();
        let mut count = 0;
        visit_callback(&cold, accepted, || {
            count += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(count, 1);
        cold.validate_live().unwrap();
        drop(reopened);
        drop(source);
        fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn committed_callback_error_still_latches_root_active_and_inner_custody_loss() {
    callback_faults(false);
}

#[test]
fn accepted_callback_error_still_latches_root_active_and_inner_custody_loss() {
    callback_faults(true);
}

#[test]
fn healthy_committed_and_accepted_callback_errors_preserve_the_same_observation() {
    for accepted in [false, true] {
        let (root, source, target) = callback_fixture("healthy-callback-refusal");
        let before = target.published_state_token().unwrap();
        let view = target.published_replay().unwrap();
        let mut visited = 0;
        let error = visit_callback(&view, accepted, || {
            visited += 1;
            Err(invalid_input(CALLBACK_REFUSAL))
        })
        .unwrap_err();
        assert_eq!(visited, 1);
        assert_eq!(error.message, CALLBACK_REFUSAL);
        view.validate_live().unwrap();
        assert_eq!(target.published_state_token().unwrap(), before);
        let mut retry = 0;
        visit_callback(&view, accepted, || {
            retry += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(retry, 1);
        assert_eq!(target.published_state_token().unwrap(), before);
        drop(target);
        drop(source);
        fs::remove_dir_all(root).unwrap();
    }
}
