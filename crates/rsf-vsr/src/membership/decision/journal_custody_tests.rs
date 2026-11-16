use super::*;

fn inventory(root: &Path) -> BTreeMap<std::path::PathBuf, Vec<u8>> {
    fn visit(root: &Path, path: &Path, files: &mut BTreeMap<std::path::PathBuf, Vec<u8>>) {
        for entry in std::fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            let key = entry.path().strip_prefix(root).unwrap().to_owned();
            let kind = entry.file_type().unwrap();
            if kind.is_dir() {
                visit(root, &entry.path(), files);
            } else if kind.is_symlink() {
                files.insert(
                    key,
                    std::fs::read_link(entry.path())
                        .unwrap()
                        .as_os_str()
                        .as_encoded_bytes()
                        .to_vec(),
                );
            } else {
                files.insert(key, std::fs::read(entry.path()).unwrap());
            }
        }
    }
    let mut files = BTreeMap::new();
    visit(root, root, &mut files);
    files
}

#[test]
fn membership_decision_exact_retries_reject_lost_journal_with_complete_input_intact() {
    for fault in [
        "manifest", "segment", "short", "truncate", "replace", "alias",
    ] {
        if fault == "alias" && !cfg!(unix) {
            continue;
        }
        let path = root(&format!("decision-journal-{fault}"));
        let (approval, _) = approvals(&path);
        let input = VsrMembershipInput::from_approval(&approval).unwrap();
        let id = input.id().unwrap();
        let mut state = stores(&path, &approval);
        let journal = path.join("decision-0/journal");
        let segment = journal
            .join("segments")
            .join(&state[0].journal.active_segment().file_name);
        // Keep an actual valid earlier journal prefix, not malformed bytes.
        let initial = std::fs::read(&segment).unwrap();
        let r = round(3, 1);
        let promises = state[..3]
            .iter_mut()
            .map(|s| s.promise(NodeId(1), r).unwrap())
            .collect();
        let decision = VsrMembershipDecision::from_promises(promises, input.clone()).unwrap();
        let nomination = state[1].nominate(&decision).unwrap();
        let accepted = state[0].accept(NodeId(1), &decision, &nomination).unwrap();
        let chosen = VsrMembershipChosen::from_accepted(vec![
            nomination.clone(),
            accepted.clone(),
            state[2].accept(NodeId(1), &decision, &nomination).unwrap(),
        ])
        .unwrap();
        state[0].learn_chosen(chosen.clone()).unwrap();
        // Every tested exact retry genuinely succeeded before the fault.
        let expected = state[0].decision_state().unwrap();
        assert_eq!(
            state[0].promise(NodeId(1), r).unwrap().accepted,
            Some((r, id))
        );
        assert!(matches!(
            state[0].try_promise(NodeId(1), r).unwrap(),
            VsrMembershipPromiseReply::Granted(_)
        ));
        assert_eq!(
            state[0].accept(NodeId(1), &decision, &nomination).unwrap(),
            accepted
        );
        state[0].learn_chosen(chosen.clone()).unwrap();
        state[0]
            .retain_chosen_input(chosen.clone(), &approval)
            .unwrap();
        assert_eq!(
            state[0].finish_chosen_input(NodeId(1), id).unwrap(),
            VsrMembershipImportReply::Chosen { input: id }
        );
        let inputs = inventory(&path.join("decision-0/inputs"));
        match fault {
            "manifest" => std::fs::remove_file(journal.join("MANIFEST")).unwrap(),
            "segment" => std::fs::remove_file(&segment).unwrap(),
            "short" => {
                std::fs::write(&segment, &initial).unwrap();
            }
            "truncate" => {
                std::fs::OpenOptions::new()
                    .write(true)
                    .open(&segment)
                    .unwrap()
                    .set_len(0)
                    .unwrap();
            }
            "replace" => {
                let manifest = journal.join("MANIFEST");
                let replacement = journal.join("replacement");
                std::fs::write(&replacement, std::fs::read(&manifest).unwrap()).unwrap();
                std::fs::rename(replacement, manifest).unwrap();
            }
            "alias" => {
                #[cfg(unix)]
                {
                    let saved = journal.join("saved-segment");
                    std::fs::rename(&segment, &saved).unwrap();
                    std::os::unix::fs::symlink(saved, &segment).unwrap();
                }
            }
            _ => unreachable!(),
        }
        let damaged = inventory(&journal);
        // Distinct paths must detect the fault themselves, before any other
        // failing call has latched the owner. A bool-only healthy() fails here.
        match fault {
            "manifest" => assert!(state[0].promise(NodeId(1), r).is_err()),
            "segment" => assert!(state[0].accept(NodeId(1), &decision, &nomination).is_err()),
            "short" => assert!(state[0].try_promise(NodeId(1), r).is_err()),
            "truncate" => assert!(state[0].learn_chosen(chosen.clone()).is_err()),
            "replace" => assert!(state[0].finish_chosen_input(NodeId(1), id).is_err()),
            "alias" => assert!(state[0].decision_state().is_err()),
            _ => unreachable!(),
        }
        assert!(state[0].decision_state().is_err(), "{fault}");
        assert!(
            state[0].try_promise(NodeId(1), round(2, 1)).is_err(),
            "preempted response: {fault}"
        );
        assert!(state[0].promise(NodeId(1), r).is_err(), "{fault}");
        assert!(
            state[0].promise(NodeId(1), round(4, 1)).is_err(),
            "new journal append: {fault}"
        );
        assert!(
            state[0].accept(NodeId(1), &decision, &nomination).is_err(),
            "{fault}"
        );
        assert!(state[0].learn_chosen(chosen.clone()).is_err(), "{fault}");
        assert!(
            state[0]
                .retain_chosen_input(chosen.clone(), &approval)
                .is_err(),
            "{fault}"
        );
        assert!(
            state[0].finish_chosen_input(NodeId(1), id).is_err(),
            "{fault}"
        );
        assert!(state[0].input_progress(id).is_err(), "{fault}");
        assert_eq!(
            state[0].accepted(),
            expected.accepted,
            "cached facts do not mutate"
        );
        assert_eq!(state[0].chosen(), Some(&chosen));
        assert_eq!(inventory(&journal), damaged, "repaired journal: {fault}");
        assert_eq!(inventory(&path.join("decision-0/inputs")), inputs);
        let (_, retained) = disk::read_records(&path.join("decision-0"), id).unwrap();
        input.validate_records(&retained).unwrap();
        drop(state);
        if matches!(fault, "manifest" | "segment" | "truncate") {
            assert!(
                MembershipDecisionStore::open_existing(
                    path.join("decision-0"),
                    PlacementGroupId(1),
                    NodeId(0)
                )
                .is_err()
            );
        }
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn membership_decision_read_only_failure_latches_even_after_exact_path_restoration() {
    let path = root("decision-journal-latch");
    let (approval, _) = approvals(&path);
    let target = path.join("decision");
    let mut store = MembershipDecisionStore::open(&target, NodeId(0), &approval).unwrap();
    let r = round(1, 1);
    let promise = store.promise(NodeId(1), r).unwrap();
    let manifest = target.join("journal/MANIFEST");
    let saved = target.join("saved-manifest");
    std::fs::rename(&manifest, &saved).unwrap();
    assert!(store.decision_state().is_err());
    // Move the original inode back: stamp comparison alone would now pass.
    std::fs::rename(saved, manifest).unwrap();
    assert!(store.decision_state().is_err());
    assert!(store.promise(NodeId(1), r).is_err());
    drop(store);
    let mut store =
        MembershipDecisionStore::open_existing(&target, PlacementGroupId(1), NodeId(0)).unwrap();
    assert_eq!(store.promise(NodeId(1), r).unwrap(), promise);
    assert_eq!(
        store.promise(NodeId(1), round(2, 1)).unwrap().round,
        round(2, 1)
    );
    drop(store);
    let mut store =
        MembershipDecisionStore::open_existing(&target, PlacementGroupId(1), NodeId(0)).unwrap();
    assert_eq!(store.decision_state().unwrap().promised, Some(round(2, 1)));
    store.promise(NodeId(1), round(3, 1)).unwrap();
    drop(store);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn membership_decision_post_append_custody_failure_returns_no_ack_or_logical_swap() {
    let path = root("decision-journal-post-append");
    let (approval, _) = approvals(&path);
    let target = path.join("decision");
    let mut store = MembershipDecisionStore::open(&target, NodeId(0), &approval).unwrap();
    let before = store.state.clone();
    let mut next = before.clone();
    next.promised = Some(round(7, 1));
    let manifest = target.join("journal/MANIFEST");
    let saved = target.join("saved-manifest");
    let acknowledged = std::cell::Cell::new(false);
    assert!(
        store
            .write_state_with_capture(
                next,
                || acknowledged.set(true),
                |journal| {
                    // Inject actual namespace loss after append's Full sync but before the
                    // wrapper can publish state or invoke its acknowledgement callback.
                    std::fs::rename(&manifest, &saved).unwrap();
                    journal.capture_owned_custody()
                }
            )
            .is_err()
    );
    assert!(!acknowledged.get());
    assert_eq!(store.state, before);
    assert!(store.failed);
    assert!(store.decision_state().is_err());
    std::fs::rename(saved, manifest).unwrap();
    assert!(store.promise(NodeId(1), round(7, 1)).is_err());
    drop(store);
    // Ambiguous durable success is recovered, never discarded or reset.
    let mut store =
        MembershipDecisionStore::open_existing(&target, PlacementGroupId(1), NodeId(0)).unwrap();
    assert_eq!(store.decision_state().unwrap().promised, Some(round(7, 1)));
    assert_eq!(
        store.promise(NodeId(1), round(7, 1)).unwrap().round,
        round(7, 1)
    );
    drop(store);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn membership_decision_metadata_preserves_historical_acceptance_without_scrubbing_input_data() {
    let path = root("decision-journal-not-input-scrub");
    let (approval, other) = approvals(&path);
    let mut state = stores(&path, &approval);
    let chosen = super::recovery::choose_b(&mut state, &other);
    let before = state[0].decision_state().unwrap();
    std::fs::remove_file(state[0].directory(chosen.input_id()).join("DESCRIPTOR")).unwrap();
    assert_eq!(state[0].decision_state().unwrap(), before);
    assert_eq!(
        state[0]
            .promise(NodeId(1), chosen.round())
            .unwrap()
            .accepted,
        before.accepted
    );
    assert!(state[0].input_progress(chosen.input_id()).is_err());
    assert!(state[0].learn_chosen(chosen).is_err());
    drop(state);
    std::fs::remove_dir_all(path).unwrap();
}
