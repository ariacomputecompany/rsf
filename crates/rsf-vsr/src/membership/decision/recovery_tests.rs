use super::*;

pub(super) fn choose_b(
    state: &mut [MembershipDecisionStore],
    approval: &VsrMembershipApproval,
) -> VsrMembershipChosen {
    let input = VsrMembershipInput::from_approval(approval).unwrap();
    let records = input.flattened_records(approval).unwrap();
    let r = round(4, 1);
    let promises = state[..3]
        .iter_mut()
        .map(|s| s.promise(NodeId(1), r).unwrap())
        .collect();
    let decision = VsrMembershipDecision::from_promises(promises, input.clone()).unwrap();
    for store in &mut state[..3] {
        store.begin_input(NodeId(1), r, input.clone()).unwrap();
        let mut at = 0;
        while at < records.len() {
            let batch = VsrHistoryBatch::pack(at as u64, &records[at..]).unwrap();
            at += batch.records.len();
            store
                .append_input(NodeId(1), r, input.id().unwrap(), batch)
                .unwrap();
        }
    }
    let nomination = state[1].nominate(&decision).unwrap();
    VsrMembershipChosen::from_accepted(vec![
        nomination.clone(),
        state[0].accept(NodeId(1), &decision, &nomination).unwrap(),
        state[2].accept(NodeId(1), &decision, &nomination).unwrap(),
    ])
    .unwrap()
}

fn assert_codec<T: std::fmt::Debug + Eq>(
    value: T,
    bytes: Vec<u8>,
    decode: impl Fn(&[u8]) -> RsfResult<T>,
) {
    assert_eq!(decode(&bytes).unwrap(), value);
    for n in 0..bytes.len() {
        assert!(decode(&bytes[..n]).is_err(), "accepted truncation {n}");
    }
    let mut extra = bytes;
    extra.push(0);
    assert!(decode(&extra).is_err());
}

#[test]
fn membership_recovery_metadata_reconstructs_accepted_majority_without_chosen_survivor() {
    let path = root("prechoice-metadata");
    let (a, b) = approvals(&path);
    let mut state = stores(&path, &a);
    let expected = choose_b(&mut state, &b);
    let reports = state[..3]
        .iter()
        .map(|store| {
            let metadata = store.decision_state().unwrap();
            assert!(metadata.chosen.is_none());
            metadata.validate_for(&proposal()).unwrap();
            assert_codec(
                metadata.clone(),
                metadata.encode().unwrap(),
                VsrMembershipDecisionState::decode,
            );
            let (round, input) = metadata.accepted.unwrap();
            VsrMembershipAccepted {
                proposal: proposal(),
                replica: metadata.replica,
                round,
                input,
            }
        })
        .collect();
    assert_eq!(
        VsrMembershipChosen::from_accepted(reports).unwrap(),
        expected
    );
    let high = round(8, 2);
    let granted = state[0].try_promise(NodeId(2), high).unwrap();
    assert!(
        matches!(&granted,VsrMembershipPromiseReply::Granted(p) if p.accepted == Some((round(4,1),expected.input_id())))
    );
    assert_codec(
        granted.clone(),
        granted.encode().unwrap(),
        VsrMembershipPromiseReply::decode,
    );
    let before = state[0].decision_state().unwrap();
    let lower = state[0].try_promise(NodeId(1), round(7, 1)).unwrap();
    assert_eq!(lower, VsrMembershipPromiseReply::Preempted(before.clone()));
    assert_eq!(state[0].decision_state().unwrap(), before);
    assert_codec(
        lower.clone(),
        lower.encode().unwrap(),
        VsrMembershipPromiseReply::decode,
    );
    assert!(
        state[0].promise(NodeId(1), round(7, 1)).is_err(),
        "legacy promise behavior changed"
    );
    assert!(state[0].try_promise(NodeId(2), round(9, 1)).is_err());
    assert_eq!(state[0].decision_state().unwrap(), before);
    drop(state);
    let store = MembershipDecisionStore::open_existing(
        path.join("decision-0"),
        PlacementGroupId(1),
        NodeId(0),
    )
    .unwrap();
    assert_eq!(store.decision_state().unwrap(), before);
    drop(store);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn membership_recovery_round_floor_uses_lexicographic_order_and_rejects_exhaustion() {
    let p = proposal();
    assert_eq!(
        VsrMembershipRound::after(&p, NodeId(2), None).unwrap(),
        round(1, 2)
    );
    assert_eq!(
        VsrMembershipRound::after(&p, NodeId(2), Some(round(4, 1))).unwrap(),
        round(4, 2)
    );
    assert_eq!(
        VsrMembershipRound::after(&p, NodeId(1), Some(round(4, 2))).unwrap(),
        round(5, 1)
    );
    assert_eq!(
        VsrMembershipRound::after(&p, NodeId(2), Some(round(u64::MAX, 1))).unwrap(),
        round(u64::MAX, 2)
    );
    assert!(VsrMembershipRound::after(&p, NodeId(2), Some(round(u64::MAX, 2))).is_err());
    assert!(VsrMembershipRound::after(&p, NodeId(99), None).is_err());
    assert!(VsrMembershipRound::after(&p, NodeId(1), Some(round(0, 1))).is_err());
}

#[test]
fn membership_chosen_import_resumes_exact_unbound_prefix_across_restart_without_lowering_promise() {
    let path = root("prechoice-chosen-import");
    let (a, b) = approvals(&path);
    let mut state = stores(&path, &a);
    let chosen = choose_b(&mut state, &b);
    let input = VsrMembershipInput::from_approval(&b).unwrap();
    let id = input.id().unwrap();
    let records = input.flattened_records(&b).unwrap();
    let high = round(99, 3);
    state[3].promise(NodeId(3), high).unwrap();
    assert_eq!(
        state[3].input_progress(id).unwrap(),
        VsrMembershipInputProgress::Missing
    );
    let before = state[3].decision_state().unwrap();
    assert_eq!(
        state[3]
            .begin_chosen_input(NodeId(2), chosen.clone(), input.clone())
            .unwrap(),
        VsrMembershipImportReply::Retained {
            input: id,
            records: 0
        }
    );
    let first = VsrHistoryBatch {
        ordinal: 0,
        records: records[..1].to_vec(),
    };
    assert_eq!(
        state[3]
            .append_chosen_input(NodeId(2), id, first.clone())
            .unwrap(),
        VsrMembershipImportReply::Retained {
            input: id,
            records: 1
        }
    );
    assert_eq!(
        state[3]
            .append_chosen_input(NodeId(0), id, first.clone())
            .unwrap(),
        VsrMembershipImportReply::Retained {
            input: id,
            records: 1
        }
    );
    let mut changed = first.clone();
    changed.records[0].payload.push(1);
    assert!(
        state[3]
            .append_chosen_input(NodeId(2), id, changed)
            .is_err()
    );
    assert!(state[3].finish_chosen_input(NodeId(2), id).is_err());
    assert_eq!(
        state[3].decision_state().unwrap(),
        before,
        "copy acknowledged a consensus fact before complete replay"
    );
    drop(state);
    let mut store = MembershipDecisionStore::open_existing(
        path.join("decision-3"),
        PlacementGroupId(1),
        NodeId(3),
    )
    .unwrap();
    assert_eq!(
        store.input_progress(id).unwrap(),
        VsrMembershipInputProgress::Partial {
            input: input.clone(),
            records: 1
        }
    );
    assert_eq!(
        store.append_chosen_input(NodeId(2), id, first).unwrap(),
        VsrMembershipImportReply::BeginRequired
    );
    assert_eq!(
        store.finish_chosen_input(NodeId(2), id).unwrap(),
        VsrMembershipImportReply::BeginRequired
    );
    assert_eq!(
        store
            .begin_chosen_input(NodeId(2), chosen.clone(), input.clone())
            .unwrap(),
        VsrMembershipImportReply::Retained {
            input: id,
            records: 1
        }
    );
    let mut at = 1;
    while at < records.len() {
        let batch = VsrHistoryBatch::pack(at as u64, &records[at..]).unwrap();
        at += batch.records.len();
        assert_eq!(
            store.append_chosen_input(NodeId(2), id, batch).unwrap(),
            VsrMembershipImportReply::Retained {
                input: id,
                records: at as u64
            }
        );
    }
    assert_eq!(
        store.input_progress(id).unwrap(),
        VsrMembershipInputProgress::Complete {
            input: input.clone()
        }
    );
    assert_eq!(
        store.finish_chosen_input(NodeId(2), id).unwrap(),
        VsrMembershipImportReply::Chosen { input: id }
    );
    assert_eq!(store.chosen(), Some(&chosen));
    assert_eq!(store.decision_state().unwrap().promised, Some(high));
    assert_eq!(
        store
            .begin_chosen_input(NodeId(2), chosen.clone(), input)
            .unwrap(),
        VsrMembershipImportReply::Chosen { input: id }
    );
    drop(store);
    let store = MembershipDecisionStore::open_existing(
        path.join("decision-3"),
        PlacementGroupId(1),
        NodeId(3),
    )
    .unwrap();
    assert_eq!(store.chosen(), Some(&chosen));
    assert_eq!(store.decision_state().unwrap().promised, Some(high));
    drop(store);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn membership_recovery_never_repairs_missing_or_corrupt_acknowledged_input() {
    for fault in ["missing-accepted", "corrupt-accepted", "missing-authority"] {
        let path = root(fault);
        let (a, b) = approvals(&path);
        let mut state = stores(&path, &a);
        let chosen = choose_b(&mut state, &b);
        let selected = if fault == "missing-authority" { &a } else { &b };
        let input = VsrMembershipInput::from_approval(selected).unwrap();
        let id = input.id().unwrap();
        let directory = state[0].directory(id);
        if fault == "corrupt-accepted" {
            std::fs::write(directory.join("DESCRIPTOR"), b"broken").unwrap();
        } else {
            std::fs::remove_dir_all(&directory).unwrap();
        }
        assert!(state[0].input_progress(id).is_err());
        let round = round(12, 0);
        state[0].promise(NodeId(0), round).unwrap();
        assert!(state[0].begin_input(NodeId(0), round, input).is_err());
        if fault != "missing-authority" {
            assert!(state[0].retain_chosen_input(chosen, &b).is_err());
        }
        if fault == "corrupt-accepted" {
            assert_eq!(
                std::fs::read(directory.join("DESCRIPTOR")).unwrap(),
                b"broken"
            );
        } else {
            assert!(!directory.exists());
        }
        drop(state);
        assert!(
            MembershipDecisionStore::open_existing(
                path.join("decision-0"),
                PlacementGroupId(1),
                NodeId(0)
            )
            .is_err()
        );
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn membership_recovery_rejects_valid_but_incomplete_acknowledged_prefix_even_on_old_append_path() {
    let path = root("prechoice-truncated-accepted");
    let (a, b) = approvals(&path);
    let mut state = stores(&path, &a);
    let chosen = choose_b(&mut state, &b);
    let input = VsrMembershipInput::from_approval(&b).unwrap();
    let id = input.id().unwrap();
    // Retain a valid empty, unbound B upload to model loss of acknowledged
    // records without introducing a malformed descriptor or WAL envelope.
    state[3]
        .begin_chosen_input(NodeId(2), chosen.clone(), input.clone())
        .unwrap();
    let empty = state[3].directory(id).join("records");
    let acknowledged = state[0].directory(id).join("records");
    std::fs::remove_dir_all(&acknowledged).unwrap();
    fn copy(source: &Path, target: &Path) {
        std::fs::create_dir(target).unwrap();
        for entry in std::fs::read_dir(source).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                copy(&entry.path(), &target.join(entry.file_name()));
            } else {
                std::fs::copy(entry.path(), target.join(entry.file_name())).unwrap();
            }
        }
    }
    copy(&empty, &acknowledged);
    assert!(matches!(
        state[3].input_progress(id).unwrap(),
        VsrMembershipInputProgress::Partial { records: 0, .. }
    ));
    assert!(state[0].input_progress(id).is_err());
    let records = input.flattened_records(&b).unwrap();
    let batch = VsrHistoryBatch::pack(0, &records).unwrap();
    assert!(
        state[0]
            .append_input(NodeId(1), round(4, 1), id, batch.clone())
            .is_err()
    );
    assert!(
        state[0]
            .begin_chosen_input(NodeId(2), chosen, input)
            .is_err()
    );
    assert_eq!(state[0].read_records(id).unwrap().1.len(), 0);
    assert_eq!(
        state[0].append_chosen_input(NodeId(2), id, batch).unwrap(),
        VsrMembershipImportReply::BeginRequired
    );
    assert_eq!(state[0].read_records(id).unwrap().1.len(), 0);
    drop(state);
    assert!(
        MembershipDecisionStore::open_existing(
            path.join("decision-0"),
            PlacementGroupId(1),
            NodeId(0)
        )
        .is_err()
    );
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn membership_recovery_rejects_malformed_unbound_custody_and_failed_handle_metadata() {
    let path = root("prechoice-malformed");
    let (a, b) = approvals(&path);
    let mut state = stores(&path, &a);
    let chosen = choose_b(&mut state, &b);
    let input = VsrMembershipInput::from_approval(&b).unwrap();
    let directory = state[3].directory(input.id().unwrap());
    std::fs::create_dir_all(&directory).unwrap();
    std::fs::write(directory.join("DESCRIPTOR"), b"incomplete foreign bytes").unwrap();
    assert!(state[3].input_progress(input.id().unwrap()).is_err());
    assert!(
        state[3]
            .begin_chosen_input(NodeId(2), chosen, input)
            .is_err()
    );
    assert_eq!(
        std::fs::read(directory.join("DESCRIPTOR")).unwrap(),
        b"incomplete foreign bytes"
    );
    state[3].failed = true;
    assert!(state[3].decision_state().is_err());
    assert!(state[3].try_promise(NodeId(2), round(9, 2)).is_err());
    drop(state);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn membership_recovery_codecs_bound_variants_and_reject_impossible_facts() {
    let path = root("prechoice-codec");
    let (a, b) = approvals(&path);
    let mut state = stores(&path, &a);
    let chosen = choose_b(&mut state, &b);
    let input = VsrMembershipInput::from_approval(&b).unwrap();
    let id = input.id().unwrap();
    let package = VsrMembershipChosenInput::new(chosen.clone(), input.clone()).unwrap();
    assert_codec(
        package.clone(),
        package.encode().unwrap(),
        VsrMembershipChosenInput::decode,
    );
    assert!(
        VsrMembershipChosenInput::new(chosen, VsrMembershipInput::from_approval(&a).unwrap())
            .is_err()
    );
    for value in [
        VsrMembershipInputProgress::Missing,
        VsrMembershipInputProgress::Initializing,
        VsrMembershipInputProgress::Partial {
            input: input.clone(),
            records: 0,
        },
        VsrMembershipInputProgress::Complete {
            input: input.clone(),
        },
    ] {
        assert_codec(
            value.clone(),
            value.encode().unwrap(),
            VsrMembershipInputProgress::decode,
        );
    }
    assert!(
        VsrMembershipInputProgress::Partial {
            input: input.clone(),
            records: input.records().unwrap()
        }
        .encode()
        .is_err()
    );
    for value in [
        VsrMembershipImportReply::BeginRequired,
        VsrMembershipImportReply::Retained {
            input: id,
            records: 7,
        },
        VsrMembershipImportReply::Chosen { input: id },
    ] {
        assert_codec(
            value.clone(),
            value.encode().unwrap(),
            VsrMembershipImportReply::decode,
        );
    }
    let mut metadata = state[0].decision_state().unwrap();
    metadata.promised = None;
    assert!(metadata.encode().is_err());
    let mut metadata = state[0].decision_state().unwrap();
    metadata.chosen = Some((round(5, 1), id));
    assert!(metadata.encode().is_err());
    let mut metadata = state[0].decision_state().unwrap();
    metadata.replica = NodeId(99);
    assert!(metadata.validate_for(&proposal()).is_err());
    assert!(
        VsrMembershipPromiseReply::Preempted(state[3].decision_state().unwrap())
            .encode()
            .is_err()
    );
    let mut oversized = b"VSRMCI1".to_vec();
    oversized.resize(MAX_HISTORY_RECORD_BYTES + 1, 0);
    assert!(VsrMembershipChosenInput::decode(&oversized).is_err());
    let mut oversized = b"VSRMDR1".to_vec();
    oversized.resize(257, 0);
    assert!(VsrMembershipDecisionState::decode(&oversized).is_err());
    drop(state);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn membership_input_factory_prefixes_resume_only_while_unbound_and_never_grant_a_vote() {
    let path = root("prechoice-factory-prefixes");
    let (a, b) = approvals(&path);
    let mut sources = stores(&path, &a);
    let chosen = choose_b(&mut sources, &b);
    let input = VsrMembershipInput::from_approval(&b).unwrap();
    let id = input.id().unwrap();
    let descriptor = input.encode().unwrap();
    for phase in [
        "empty",
        "temp-prefix",
        "ordinary-temp",
        "descriptor",
        "records",
        "segments",
        "segment",
        "manifest-prefix",
        "manifest-complete",
    ] {
        let mut store =
            MembershipDecisionStore::open(path.join(format!("factory-{phase}")), NodeId(3), &a)
                .unwrap();
        let dir = store.directory(id);
        std::fs::create_dir(&dir).unwrap();
        if phase == "temp-prefix" {
            std::fs::write(
                dir.join(".DESCRIPTOR.tmp"),
                &descriptor[..descriptor.len() / 2],
            )
            .unwrap();
        } else if phase == "ordinary-temp" {
            std::fs::write(dir.join(".DESCRIPTOR.tmp"), b"torn descriptor").unwrap();
        } else if phase != "empty" {
            std::fs::write(dir.join("DESCRIPTOR"), &descriptor).unwrap();
            if phase == "records" {
                std::fs::create_dir(dir.join("records")).unwrap();
            }
            if phase == "segments" {
                std::fs::create_dir_all(dir.join("records/segments")).unwrap();
            }
            if ["segment", "manifest-prefix", "manifest-complete"].contains(&phase) {
                drop(SegmentStore::open(dir.join("records")).unwrap());
                let manifest = std::fs::read(dir.join("records/MANIFEST")).unwrap();
                std::fs::remove_file(dir.join("records/MANIFEST")).unwrap();
                // Segment writer locks appear only after manifest publication;
                // remove those empty post-publication fixture artifacts.
                for entry in std::fs::read_dir(dir.join("records/segments")).unwrap() {
                    let entry = entry.unwrap();
                    if entry.path().extension() == Some(std::ffi::OsStr::new("lock")) {
                        std::fs::remove_file(entry.path()).unwrap();
                    }
                }
                if phase == "manifest-prefix" {
                    std::fs::write(
                        dir.join("records/MANIFEST.tmp"),
                        &manifest[..manifest.len() / 2],
                    )
                    .unwrap();
                }
                if phase == "manifest-complete" {
                    std::fs::write(dir.join("records/MANIFEST.tmp"), &manifest).unwrap();
                }
            }
        }
        let before = store.decision_state().unwrap();
        assert_eq!(
            store.input_progress(id).unwrap(),
            VsrMembershipInputProgress::Initializing,
            "{phase}"
        );
        assert_eq!(
            store
                .begin_chosen_input(NodeId(2), chosen.clone(), input.clone())
                .unwrap(),
            VsrMembershipImportReply::Retained {
                input: id,
                records: 0
            },
            "{phase}"
        );
        assert_eq!(store.decision_state().unwrap(), before);
        assert_eq!(
            store.input_progress(id).unwrap(),
            VsrMembershipInputProgress::Partial {
                input: input.clone(),
                records: 0
            }
        );
        assert!(store.finish_chosen_input(NodeId(2), id).is_err());
        assert_eq!(store.decision_state().unwrap(), before);
    }
    let mut bound = MembershipDecisionStore::open(path.join("bound"), NodeId(3), &a).unwrap();
    let r = round(3, 3);
    bound.promise(NodeId(3), r).unwrap();
    bound.begin_input(NodeId(3), r, input.clone()).unwrap();
    let dir = bound.directory(id);
    std::fs::remove_dir_all(&dir).unwrap();
    std::fs::create_dir(&dir).unwrap();
    assert!(bound.input_progress(id).is_err());
    assert!(bound.begin_input(NodeId(3), r, input).is_err());
    assert!(std::fs::read_dir(&dir).unwrap().next().is_none());
    drop(bound);
    drop(sources);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn membership_input_factory_rejects_oversized_temp_foreign_descriptor_and_nonempty_unpublished_wal()
{
    let path = root("prechoice-factory-reject");
    let (a, b) = approvals(&path);
    let input = VsrMembershipInput::from_approval(&b).unwrap();
    let id = input.id().unwrap();
    let records = input.flattened_records(&b).unwrap();
    for fault in [
        "oversized-temp",
        "descriptor",
        "records-first",
        "foreign-entry",
        "nonempty-wal",
    ] {
        let store = MembershipDecisionStore::open(path.join(fault), NodeId(3), &a).unwrap();
        let dir = store.directory(id);
        std::fs::create_dir(&dir).unwrap();
        match fault {
            "oversized-temp" => std::fs::write(
                dir.join(".DESCRIPTOR.tmp"),
                vec![0; MAX_HISTORY_RECORD_BYTES + 1],
            )
            .unwrap(),
            "descriptor" => std::fs::write(
                dir.join("DESCRIPTOR"),
                VsrMembershipInput::from_approval(&a)
                    .unwrap()
                    .encode()
                    .unwrap(),
            )
            .unwrap(),
            "records-first" => std::fs::create_dir(dir.join("records")).unwrap(),
            "foreign-entry" => std::fs::write(dir.join("foreign"), b"preserve").unwrap(),
            _ => {
                disk::create_input(&store.root, &input).unwrap();
                disk::append_records(
                    &store.root,
                    &mut false,
                    &input,
                    VsrHistoryBatch {
                        ordinal: 0,
                        records: records[..1].to_vec(),
                    },
                )
                .unwrap();
                std::fs::remove_file(dir.join("records/MANIFEST")).unwrap();
            }
        }
        let before = store.decision_state().unwrap();
        assert!(store.input_progress(id).is_err(), "{fault}");
        assert!(
            store
                .ensure_input(&input, disk::sync_input_publication)
                .is_err(),
            "{fault}"
        );
        assert_eq!(store.decision_state().unwrap(), before);
        if fault == "oversized-temp" {
            assert_eq!(
                std::fs::metadata(dir.join(".DESCRIPTOR.tmp"))
                    .unwrap()
                    .len(),
                (MAX_HISTORY_RECORD_BYTES + 1) as u64
            );
        }
        if fault == "nonempty-wal" {
            assert!(!dir.join("records/MANIFEST").exists());
        }
    }
    std::fs::remove_dir_all(path).unwrap();
}
