use super::*;

fn replace_records(
    path: &Path,
    input: &VsrMembershipInput,
    records: &[VsrHistoryRecord],
    destination: &Path,
) {
    std::fs::create_dir(path).unwrap();
    disk::create_input(path, input).unwrap();
    let mut ordinal = 0;
    while ordinal < records.len() {
        let batch = VsrHistoryBatch::pack(ordinal as u64, &records[ordinal..]).unwrap();
        ordinal += batch.records.len();
        disk::append_records(path, &mut false, input, batch).unwrap();
    }
    let name = input
        .id()
        .unwrap()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    std::fs::remove_dir_all(destination).unwrap();
    std::fs::rename(path.join("inputs").join(name).join("records"), destination).unwrap();
}

#[test]
fn chosen_input_exact_proof_reuse_preserves_higher_promise_and_first_certificate_cold() {
    let path = root("chosen-proof-reuse");
    let (mut store, approval, chosen) = accepted_b(&path);
    let high = round(91, 3);
    store.promise(NodeId(3), high).unwrap();
    let before = store.accepted();
    store
        .retain_chosen_input(chosen.clone(), &approval)
        .unwrap();
    assert_eq!(store.state.promised, Some(high));
    assert_eq!(store.accepted(), before);
    assert_eq!(store.chosen(), Some(&chosen));
    let journal = store
        .journal
        .recover_current()
        .unwrap()
        .recovery
        .wal_records;
    let mut reports = chosen.accepted().to_vec();
    for report in &mut reports {
        report.round = round(19, 2);
    }
    let alternate = VsrMembershipChosen::from_accepted(reports).unwrap();
    store.retain_chosen_input(alternate, &approval).unwrap();
    assert_eq!(store.chosen(), Some(&chosen));
    assert_eq!(
        store
            .journal
            .recover_current()
            .unwrap()
            .recovery
            .wal_records,
        journal
    );
    drop(store);
    let store = MembershipDecisionStore::open_existing(
        path.join("decision-2"),
        PlacementGroupId(1),
        NodeId(2),
    )
    .unwrap();
    assert_eq!(store.state.promised, Some(high));
    assert_eq!(store.accepted(), before);
    assert_eq!(store.chosen(), Some(&chosen));
    drop(store);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn chosen_input_proof_reuse_rejects_damaged_acknowledged_input_before_copy_boundary() {
    for fault in ["short", "descriptor"] {
        let path = root(&format!("chosen-proof-before-{fault}"));
        let (mut store, approval, chosen) = accepted_b(&path);
        let input = VsrMembershipInput::from_approval(&approval).unwrap();
        let records = input.flattened_records(&approval).unwrap();
        let directory = store.directory(input.id().unwrap());
        if fault == "short" {
            replace_records(
                &path.join("short"),
                &input,
                &records[..1],
                &directory.join("records"),
            );
        } else {
            std::fs::write(directory.join("DESCRIPTOR"), b"corrupt descriptor").unwrap();
        }
        let journal = store
            .journal
            .recover_current()
            .unwrap()
            .recovery
            .wal_records;
        let mut reached = false;
        assert!(
            store
                .retain_chosen_input_with_hook(chosen.clone(), &approval, || {
                    reached = true;
                })
                .is_err()
        );
        assert!(
            !reached,
            "damaged acknowledged input reached copying boundary"
        );
        assert!(store.chosen().is_none());
        assert_eq!(store.accepted(), Some((chosen.round(), chosen.input_id())));
        assert_eq!(
            store
                .journal
                .recover_current()
                .unwrap()
                .recovery
                .wal_records,
            journal
        );
        if fault == "short" {
            assert_eq!(
                store.read_records(input.id().unwrap()).unwrap().1,
                records[..1]
            );
        } else {
            assert_eq!(
                std::fs::read(directory.join("DESCRIPTOR")).unwrap(),
                b"corrupt descriptor"
            );
        }
        drop(store);
        assert!(
            MembershipDecisionStore::open_existing(
                path.join("decision-2"),
                PlacementGroupId(1),
                NodeId(2)
            )
            .is_err()
        );
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn chosen_input_proof_reuse_rereads_after_copy_and_public_learning_still_replays_full_input() {
    for fault in ["short", "full-corrupt", "descriptor"] {
        let path = root(&format!("chosen-proof-after-{fault}"));
        let (source, approval, chosen) = accepted_b(&path);
        let input = VsrMembershipInput::from_approval(&approval).unwrap();
        let records = input.flattened_records(&approval).unwrap();
        let mut store = MembershipDecisionStore::open_existing(
            path.join("decision-3"),
            PlacementGroupId(1),
            NodeId(3),
        )
        .unwrap();
        let high = round(92, 3);
        store.promise(NodeId(3), high).unwrap();
        let directory = store.directory(input.id().unwrap());
        let journal = store
            .journal
            .recover_current()
            .unwrap()
            .recovery
            .wal_records;
        let mut reached = false;
        assert!(
            store
                .retain_chosen_input_with_hook(chosen.clone(), &approval, || {
                    reached = true;
                    match fault {
                        "short" => replace_records(
                            &path.join("short"),
                            &input,
                            &records[..1],
                            &directory.join("records"),
                        ),
                        "full-corrupt" => {
                            let mut damaged = records.clone();
                            damaged.last_mut().unwrap().payload.push(7);
                            replace_records(
                                &path.join("corrupt"),
                                &input,
                                &damaged,
                                &directory.join("records"),
                            );
                        }
                        _ => std::fs::write(directory.join("DESCRIPTOR"), b"corrupt descriptor")
                            .unwrap(),
                    }
                })
                .is_err()
        );
        assert!(reached);
        assert_eq!(store.state.promised, Some(high));
        assert!(store.accepted().is_none());
        assert!(store.chosen().is_none());
        assert_eq!(
            store
                .journal
                .recover_current()
                .unwrap()
                .recovery
                .wal_records,
            journal
        );
        // Even full-count, structurally valid WAL with damaged logical bytes
        // must fail public learning's semantic descriptor/history validation.
        assert!(store.learn_chosen(chosen).is_err());
        assert_eq!(
            store
                .journal
                .recover_current()
                .unwrap()
                .recovery
                .wal_records,
            journal
        );
        drop(store);
        let store = MembershipDecisionStore::open_existing(
            path.join("decision-3"),
            PlacementGroupId(1),
            NodeId(3),
        )
        .unwrap();
        assert_eq!(store.state.promised, Some(high));
        assert!(store.accepted().is_none());
        assert!(store.chosen().is_none());
        drop(store);
        drop(source);
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn chosen_finish_revalidates_missing_and_valid_short_input_before_first_choice() {
    for fault in ["missing-manifest", "valid-short"] {
        let path = root(&format!("chosen-finish-first-{fault}"));
        let (mut store, approval, chosen) = accepted_b(&path);
        let input = VsrMembershipInput::from_approval(&approval).unwrap();
        let id = input.id().unwrap();
        let records = input.flattened_records(&approval).unwrap();
        let high = round(91, 3);
        store.promise(NodeId(3), high).unwrap();
        store
            .begin_chosen_input(NodeId(1), chosen.clone(), input.clone())
            .unwrap();
        assert!(store.chosen().is_none());
        let before = store.decision_state().unwrap();
        let journal = store
            .journal
            .recover_current()
            .unwrap()
            .recovery
            .wal_records;
        let directory = store.directory(id);
        if fault == "missing-manifest" {
            std::fs::remove_file(directory.join("records/MANIFEST")).unwrap();
        } else {
            replace_records(
                &path.join("short"),
                &input,
                &records[..1],
                &directory.join("records"),
            );
            assert_eq!(store.read_records(id).unwrap().1, records[..1]);
        }
        for _ in 0..2 {
            assert!(store.finish_chosen_input(NodeId(1), id).is_err(), "{fault}");
            assert_eq!(store.decision_state().unwrap(), before);
            assert_eq!(
                store
                    .journal
                    .recover_current()
                    .unwrap()
                    .recovery
                    .wal_records,
                journal
            );
        }
        if fault == "missing-manifest" {
            assert!(!directory.join("records/MANIFEST").exists());
        } else {
            assert_eq!(store.read_records(id).unwrap().1, records[..1]);
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
fn chosen_finish_valid_retry_keeps_one_journal_entry_and_checks_custody_again() {
    let path = root("chosen-finish-exact-retry");
    let (mut store, approval, chosen) = accepted_b(&path);
    let input = VsrMembershipInput::from_approval(&approval).unwrap();
    let id = input.id().unwrap();
    let high = round(91, 3);
    store.promise(NodeId(3), high).unwrap();
    let accepted = store.accepted();
    assert_eq!(
        store
            .begin_chosen_input(NodeId(1), chosen.clone(), input.clone())
            .unwrap(),
        VsrMembershipImportReply::Retained {
            input: id,
            records: input.records().unwrap()
        }
    );
    let before = store
        .journal
        .recover_current()
        .unwrap()
        .recovery
        .wal_records;
    assert_eq!(
        store.finish_chosen_input(NodeId(1), id).unwrap(),
        VsrMembershipImportReply::Chosen { input: id }
    );
    assert_eq!(store.chosen(), Some(&chosen));
    assert_eq!(store.accepted(), accepted);
    assert_eq!(store.decision_state().unwrap().promised, Some(high));
    let journal = store
        .journal
        .recover_current()
        .unwrap()
        .recovery
        .wal_records;
    assert_eq!(journal.len(), before.len() + 1);
    assert_eq!(&journal[..before.len()], before.as_slice());
    assert_eq!(
        store.finish_chosen_input(NodeId(3), id).unwrap(),
        VsrMembershipImportReply::Chosen { input: id }
    );
    assert_eq!(
        store
            .journal
            .recover_current()
            .unwrap()
            .recovery
            .wal_records,
        journal
    );
    drop(store);
    let mut store = MembershipDecisionStore::open_existing(
        path.join("decision-2"),
        PlacementGroupId(1),
        NodeId(2),
    )
    .unwrap();
    assert_eq!(store.chosen(), Some(&chosen));
    assert_eq!(store.accepted(), accepted);
    assert_eq!(store.decision_state().unwrap().promised, Some(high));
    assert_eq!(
        store.finish_chosen_input(NodeId(1), id).unwrap(),
        VsrMembershipImportReply::Chosen { input: id }
    );
    assert_eq!(
        store
            .journal
            .recover_current()
            .unwrap()
            .recovery
            .wal_records,
        journal
    );
    let manifest = store.directory(id).join("records/MANIFEST");
    std::fs::remove_file(&manifest).unwrap();
    assert!(store.finish_chosen_input(NodeId(1), id).is_err());
    assert_eq!(store.chosen(), Some(&chosen));
    assert_eq!(
        store
            .journal
            .recover_current()
            .unwrap()
            .recovery
            .wal_records,
        journal
    );
    assert!(!manifest.exists());
    drop(store);
    std::fs::remove_dir_all(path).unwrap();
}
