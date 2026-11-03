use super::*;

fn bytes(root: &Path) -> BTreeMap<std::path::PathBuf, (bool, Vec<u8>)> {
    fn visit(base: &Path, path: &Path, out: &mut BTreeMap<std::path::PathBuf, (bool, Vec<u8>)>) {
        for entry in std::fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            let kind = entry.file_type().unwrap();
            if kind.is_dir() {
                visit(base, &entry.path(), out);
            } else {
                let value = if kind.is_symlink() {
                    std::fs::read_link(entry.path())
                        .unwrap()
                        .to_string_lossy()
                        .as_bytes()
                        .to_vec()
                } else {
                    std::fs::read(entry.path()).unwrap()
                };
                out.insert(
                    entry.path().strip_prefix(base).unwrap().to_owned(),
                    (kind.is_symlink(), value),
                );
            }
        }
    }
    let mut out = BTreeMap::new();
    visit(root, root, &mut out);
    out
}

#[test]
fn membership_cold_validator_visits_each_distinct_complete_input_once_with_replayed_identity() {
    for distinct in [false, true] {
        let path = root(if distinct {
            "startup-distinct-inputs"
        } else {
            "startup-shared-input"
        });
        let (a, b) = approvals(&path);
        let selected = if distinct { &b } else { &a };
        let authority = VsrMembershipInput::from_approval(&a).unwrap().id().unwrap();
        let input = VsrMembershipInput::from_approval(selected).unwrap();
        let id = input.id().unwrap();
        let mut state = stores(&path, &a);
        let chosen = super::recovery::choose_b(&mut state, selected);
        state[0].learn_chosen(chosen.clone()).unwrap();
        let before = state[0].decision_state().unwrap();
        assert_eq!(state[0].state.staged, Some((round(4, 1), id)));
        drop(state);
        let decision = path.join("decision-0");
        let retained_bytes = bytes(&decision);
        let mut observed = BTreeMap::new();
        let mut reopened = MembershipDecisionStore::open_existing_with_validation(
            &decision,
            PlacementGroupId(1),
            NodeId(0),
            |owner, approval| {
                let verified = VsrMembershipInput::from_approval(approval)?;
                let id = verified.id()?;
                *observed.entry(id).or_insert(0usize) += 1;
                assert_eq!(owner.authority, authority);
                assert_eq!(owner.proposal(), &proposal());
                assert_eq!(owner.decision_state()?, before);
                assert_eq!(owner.chosen(), Some(&chosen));
                // The opaque proof owns the complete original histories, not
                // just the descriptor or one journal role's cached tuple.
                let expected = if id == authority { &a } else { selected };
                assert_eq!(verified, VsrMembershipInput::from_approval(expected)?);
                assert_eq!(
                    verified.flattened_records(approval)?,
                    verified.flattened_records(expected)?
                );
                Ok(())
            },
        )
        .unwrap();
        let expected = std::collections::BTreeSet::from([authority, id]);
        assert_eq!(
            observed
                .keys()
                .copied()
                .collect::<std::collections::BTreeSet<_>>(),
            expected
        );
        assert!(observed.values().all(|calls| *calls == 1));
        assert_eq!(
            bytes(&decision),
            retained_bytes,
            "validation rewrote retained proof bytes"
        );
        let newer = round(7, 2);
        reopened.promise(NodeId(2), newer).unwrap();
        drop(reopened);
        let reopened =
            MembershipDecisionStore::open_existing(&decision, PlacementGroupId(1), NodeId(0))
                .unwrap();
        assert_eq!(reopened.decision_state().unwrap().promised, Some(newer));
        assert_eq!(reopened.chosen(), Some(&chosen));
        drop(reopened);
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn membership_cold_validator_leaves_staged_only_prefix_resumable_without_approval() {
    let path = root("startup-partial-staged");
    let (a, b) = approvals(&path);
    let input = VsrMembershipInput::from_approval(&b).unwrap();
    let id = input.id().unwrap();
    let records = input.flattened_records(&b).unwrap();
    assert!(records.len() > 1);
    let authority = VsrMembershipInput::from_approval(&a).unwrap().id().unwrap();
    let decision = path.join("decision");
    let r = round(1, 1);
    let mut store = MembershipDecisionStore::open(&decision, NodeId(0), &a).unwrap();
    store.promise(NodeId(1), r).unwrap();
    store.begin_input(NodeId(1), r, input.clone()).unwrap();
    store
        .append_input(
            NodeId(1),
            r,
            id,
            VsrHistoryBatch {
                ordinal: 0,
                records: records[..1].to_vec(),
            },
        )
        .unwrap();
    let before = store.decision_state().unwrap();
    drop(store);
    let mut seen = Vec::new();
    let mut store = MembershipDecisionStore::open_existing_with_validation(
        &decision,
        PlacementGroupId(1),
        NodeId(0),
        |_, approval| {
            seen.push(VsrMembershipInput::from_approval(approval)?.id()?);
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(seen, vec![authority]);
    assert_eq!(store.decision_state().unwrap(), before);
    assert_eq!(
        store.input(id).unwrap(),
        (input.clone(), records[..1].to_vec())
    );
    assert_eq!(store.begin_input(NodeId(1), r, input.clone()).unwrap(), 1);
    let mut at = 1;
    while at < records.len() as u64 {
        at = store
            .append_input(
                NodeId(1),
                r,
                id,
                VsrHistoryBatch::pack(at, &records[at as usize..]).unwrap(),
            )
            .unwrap();
    }
    assert_eq!(
        VsrMembershipInput::from_approval(&store.approval(id).unwrap()).unwrap(),
        input
    );
    assert!(store.accepted().is_none());
    assert!(store.chosen().is_none());
    drop(store);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn membership_cold_host_validator_rejection_preserves_promises_and_all_retained_bytes() {
    let path = root("startup-host-rejection");
    let (a, _) = approvals(&path);
    let decision = path.join("decision");
    let mut store = MembershipDecisionStore::open(&decision, NodeId(0), &a).unwrap();
    let r = round(8, 2);
    store.promise(NodeId(2), r).unwrap();
    let state = store.decision_state().unwrap();
    drop(store);
    let retained = bytes(&decision);
    let mut calls = 0;
    let error = MembershipDecisionStore::open_existing_with_validation(
        &decision,
        PlacementGroupId(1),
        NodeId(0),
        |owner, _| {
            calls += 1;
            assert_eq!(owner.decision_state()?, state);
            Err(invalid_input("injected hosted authority rejection"))
        },
    )
    .unwrap_err();
    assert_eq!(error.message, "injected hosted authority rejection");
    assert_eq!(calls, 1);
    assert_eq!(bytes(&decision), retained);
    let mut reopened =
        MembershipDecisionStore::open_existing(&decision, PlacementGroupId(1), NodeId(0)).unwrap();
    assert_eq!(reopened.decision_state().unwrap(), state);
    assert!(reopened.promise(NodeId(1), round(7, 1)).is_err());
    assert_eq!(reopened.promise(NodeId(2), r).unwrap().round, r);
    drop(reopened);
    assert_eq!(bytes(&decision), retained);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn membership_cold_dedup_never_downgrades_a_complete_bound_input_to_partial() {
    for fault in ["missing", "descriptor", "valid-short-prefix"] {
        let path = root(&format!("startup-bound-{fault}"));
        let (a, _) = approvals(&path);
        let input = VsrMembershipInput::from_approval(&a).unwrap();
        let id = input.id().unwrap();
        let mut state = stores(&path, &a);
        let chosen = super::recovery::choose_b(&mut state, &a);
        state[0].learn_chosen(chosen).unwrap();
        let directory = state[0].directory(id);
        drop(state);
        match fault {
            "missing" => std::fs::remove_file(directory.join("DESCRIPTOR")).unwrap(),
            "descriptor" => {
                std::fs::write(directory.join("DESCRIPTOR"), b"corrupt descriptor").unwrap()
            }
            "valid-short-prefix" => {
                let records = input.flattened_records(&a).unwrap();
                assert!(records.len() > 1);
                let scratch = path.join("short");
                std::fs::create_dir(&scratch).unwrap();
                disk::create_input(&scratch, &input).unwrap();
                disk::append_records(
                    &scratch,
                    &mut false,
                    &input,
                    VsrHistoryBatch {
                        ordinal: 0,
                        records: records[..1].to_vec(),
                    },
                )
                .unwrap();
                let short = scratch.join("inputs").join(hex(id)).join("records");
                std::fs::remove_dir_all(directory.join("records")).unwrap();
                std::fs::rename(short, directory.join("records")).unwrap();
            }
            _ => unreachable!(),
        }
        let decision = path.join("decision-0");
        let retained = bytes(&decision);
        let mut calls = 0;
        assert!(
            MembershipDecisionStore::open_existing_with_validation(
                &decision,
                PlacementGroupId(1),
                NodeId(0),
                |_, _| {
                    calls += 1;
                    Ok(())
                },
            )
            .is_err(),
            "accepted {fault}"
        );
        assert_eq!(calls, 0, "callback received a damaged opaque proof");
        assert_eq!(bytes(&decision), retained, "repaired {fault}");
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
#[cfg(unix)]
fn membership_cold_validator_rejects_input_alias_without_touching_its_target() {
    let path = root("startup-input-alias");
    let (a, _) = approvals(&path);
    let decision = path.join("decision");
    let store = MembershipDecisionStore::open(&decision, NodeId(0), &a).unwrap();
    let id = VsrMembershipInput::from_approval(&a).unwrap().id().unwrap();
    let directory = store.directory(id);
    drop(store);
    let target = path.join("unowned-input");
    std::fs::rename(&directory, &target).unwrap();
    std::os::unix::fs::symlink(&target, &directory).unwrap();
    let owned = bytes(&decision);
    let unowned = bytes(&target);
    let mut calls = 0;
    assert!(
        MembershipDecisionStore::open_existing_with_validation(
            &decision,
            PlacementGroupId(1),
            NodeId(0),
            |_, _| {
                calls += 1;
                Ok(())
            },
        )
        .is_err()
    );
    assert_eq!(calls, 0);
    assert_eq!(bytes(&decision), owned);
    assert_eq!(bytes(&target), unowned);
    std::fs::remove_dir_all(path).unwrap();
}
