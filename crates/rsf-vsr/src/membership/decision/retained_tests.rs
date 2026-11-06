use super::*;

fn accepted_b(
    path: &Path,
) -> (
    MembershipDecisionStore,
    VsrMembershipApproval,
    VsrMembershipChosen,
) {
    let (a, b) = approvals(path);
    let input = VsrMembershipInput::from_approval(&b).unwrap();
    let records = input.flattened_records(&b).unwrap();
    let id = input.id().unwrap();
    let mut state = stores(path, &a);
    let r = round(1, 1);
    let promises = state
        .iter_mut()
        .take(3)
        .map(|store| store.promise(NodeId(1), r).unwrap())
        .collect();
    let decision = VsrMembershipDecision::from_promises(promises, input.clone()).unwrap();
    for store in state.iter_mut().take(3) {
        assert!(store.retained_approval(id).unwrap().is_none());
        store.begin_input(NodeId(1), r, input.clone()).unwrap();
        let mut at = 0;
        while at < records.len() {
            let batch = VsrHistoryBatch::pack(at as u64, &records[at..]).unwrap();
            at += batch.records.len();
            store.append_input(NodeId(1), r, id, batch).unwrap();
        }
    }
    let nomination = state[1].nominate(&decision).unwrap();
    let chosen = VsrMembershipChosen::from_accepted(vec![
        nomination.clone(),
        state[0].accept(NodeId(1), &decision, &nomination).unwrap(),
        state[2].accept(NodeId(1), &decision, &nomination).unwrap(),
    ])
    .unwrap();
    let survivor = state.swap_remove(2);
    drop(state);
    assert!(survivor.chosen().is_none());
    (survivor, b, chosen)
}

#[test]
fn accepted_input_is_readable_before_choice_and_imports_a_surviving_certificate_cold() {
    let path = root("retained-accepted-input");
    let (store, b, chosen) = accepted_b(&path);
    let id = chosen.input_id();
    let input = VsrMembershipInput::from_approval(&b).unwrap();
    let directory = store.directory(id);
    let descriptor = std::fs::read(directory.join("DESCRIPTOR")).unwrap();
    let records = store.input(id).unwrap().1;
    let before = store
        .journal
        .recover_current()
        .unwrap()
        .recovery
        .wal_records;
    let retained = store.retained_approval(id).unwrap().unwrap();
    assert_eq!(input.flattened_records(&retained).unwrap(), records);
    assert_eq!(
        store
            .journal
            .recover_current()
            .unwrap()
            .recovery
            .wal_records,
        before
    );
    assert!(store.chosen().is_none());
    assert!(store.retained_approval([0x71; 32]).unwrap().is_none());
    assert!(!store.directory([0x71; 32]).exists());
    drop(store);
    for name in ["old", "decision-0", "decision-1", "decision-3"] {
        std::fs::remove_dir_all(path.join(name)).unwrap();
    }
    let mut store = MembershipDecisionStore::open_existing(
        path.join("decision-2"),
        PlacementGroupId(1),
        NodeId(2),
    )
    .unwrap();
    assert!(store.chosen().is_none());
    let retained = store.retained_approval(id).unwrap().unwrap();
    store
        .retain_chosen_input(chosen.clone(), &retained)
        .unwrap();
    assert_eq!(store.chosen(), Some(&chosen));
    assert_eq!(store.input(id).unwrap().1, records);
    assert_eq!(
        std::fs::read(directory.join("DESCRIPTOR")).unwrap(),
        descriptor
    );
    drop(store);
    let store = MembershipDecisionStore::open_existing(
        path.join("decision-2"),
        PlacementGroupId(1),
        NodeId(2),
    )
    .unwrap();
    assert_eq!(store.chosen(), Some(&chosen));
    drop(store);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn retained_input_never_downgrades_missing_or_corrupt_bound_custody_to_absence() {
    for fault in ["directory", "descriptor", "descriptor-bytes", "manifest"] {
        let path = root(fault);
        let (store, _, chosen) = accepted_b(&path);
        let id = chosen.input_id();
        let directory = store.directory(id);
        let before = store
            .journal
            .recover_current()
            .unwrap()
            .recovery
            .wal_records;
        match fault {
            "directory" => std::fs::remove_dir_all(&directory).unwrap(),
            "descriptor" => std::fs::remove_file(directory.join("DESCRIPTOR")).unwrap(),
            "descriptor-bytes" => {
                std::fs::write(directory.join("DESCRIPTOR"), b"damaged input").unwrap()
            }
            "manifest" => std::fs::remove_file(directory.join("records/MANIFEST")).unwrap(),
            _ => unreachable!(),
        }
        for _ in 0..2 {
            assert!(store.retained_approval(id).is_err(), "{fault}");
            assert!(store.chosen().is_none());
            assert_eq!(store.accepted(), Some((chosen.round(), id)));
            assert_eq!(
                store
                    .journal
                    .recover_current()
                    .unwrap()
                    .recovery
                    .wal_records,
                before
            );
        }
        if fault == "directory" {
            assert!(!directory.exists());
        }
        if fault == "descriptor-bytes" {
            assert_eq!(
                std::fs::read(directory.join("DESCRIPTOR")).unwrap(),
                b"damaged input"
            );
        }
        drop(store);
        assert!(
            MembershipDecisionStore::open_existing(
                path.join("decision-2"),
                PlacementGroupId(1),
                NodeId(2),
            )
            .is_err()
        );
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn partial_retained_input_is_an_error_and_a_conflicting_choice_cannot_replace_acceptance() {
    let path = root("retained-partial-conflict");
    let (a, b) = approvals(&path);
    let mut partial = MembershipDecisionStore::open(path.join("partial"), NodeId(2), &a).unwrap();
    let input = VsrMembershipInput::from_approval(&b).unwrap();
    let id = input.id().unwrap();
    assert!(partial.retained_approval(id).unwrap().is_none());
    partial.promise(NodeId(1), round(1, 1)).unwrap();
    partial.begin_input(NodeId(1), round(1, 1), input).unwrap();
    assert!(partial.retained_approval(id).is_err());
    assert!(partial.accepted().is_none());
    drop(partial);
    let accepted_path = path.join("accepted");
    let (mut store, _, chosen) = accepted_b(&accepted_path);
    let authority = store.authority().unwrap();
    let other = VsrMembershipInput::from_approval(&authority)
        .unwrap()
        .id()
        .unwrap();
    assert_ne!(other, chosen.input_id());
    let mut conflicting = chosen.accepted().to_vec();
    for report in &mut conflicting {
        report.input = other;
    }
    let conflicting = VsrMembershipChosen::from_accepted(conflicting).unwrap();
    assert!(store.retain_chosen_input(conflicting, &authority).is_err());
    assert!(store.chosen().is_none());
    assert_eq!(store.accepted(), Some((chosen.round(), chosen.input_id())));
    assert!(
        store
            .retained_approval(chosen.input_id())
            .unwrap()
            .is_some()
    );
    drop(store);
    std::fs::remove_dir_all(path).unwrap();
}

#[path = "chosen_reuse_tests.rs"]
mod chosen_reuse;

#[path = "stage_performance_tests.rs"]
mod stage_performance;

#[path = "stage_source_tests.rs"]
mod stage_source;
