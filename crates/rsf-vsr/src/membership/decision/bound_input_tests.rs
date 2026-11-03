use super::*;

fn tree(root: &Path) -> BTreeMap<std::path::PathBuf, Vec<u8>> {
    fn visit(base: &Path, path: &Path, out: &mut BTreeMap<std::path::PathBuf, Vec<u8>>) {
        for entry in std::fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                visit(base, &entry.path(), out);
            } else {
                out.insert(
                    entry.path().strip_prefix(base).unwrap().to_owned(),
                    std::fs::read(entry.path()).unwrap(),
                );
            }
        }
    }
    let mut out = BTreeMap::new();
    visit(root, root, &mut out);
    out
}
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

#[test]
fn membership_bound_input_valid_short_prefix_never_repairs_acknowledged_custody() {
    for binding in ["authority", "accepted", "chosen"] {
        let path = root(&format!("bound-input-{binding}"));
        let (a, b) = approvals(&path);
        let selected = if binding == "authority" { &a } else { &b };
        let input = VsrMembershipInput::from_approval(selected).unwrap();
        let id = input.id().unwrap();
        let records = input.flattened_records(selected).unwrap();
        assert!(records.len() > 1);
        let mut state = stores(&path, &a);
        let r = round(1, 1);
        let promises = state
            .iter_mut()
            .map(|s| s.promise(NodeId(1), r).unwrap())
            .collect();
        let decision = VsrMembershipDecision::from_promises(promises, input.clone()).unwrap();
        for store in &mut state {
            let mut at = store.begin_input(NodeId(1), r, input.clone()).unwrap();
            while at < records.len() as u64 {
                let batch = VsrHistoryBatch::pack(at, &records[at as usize..]).unwrap();
                at = store.append_input(NodeId(1), r, id, batch).unwrap();
            }
        }
        let nomination = state[1].nominate(&decision).unwrap();
        let receiver = if binding == "authority" { 3 } else { 0 };
        let reports = vec![
            nomination.clone(),
            state[receiver]
                .accept(NodeId(1), &decision, &nomination)
                .unwrap(),
            state[2].accept(NodeId(1), &decision, &nomination).unwrap(),
        ];
        if binding == "authority" {
            assert!(state[0].accepted().is_none());
        }
        let chosen = VsrMembershipChosen::from_accepted(reports).unwrap();
        if binding == "chosen" {
            state[0].learn_chosen(chosen.clone()).unwrap();
        }
        // A separate input store builds a structurally valid, fsynced short
        // prefix with the exact descriptor, avoiding a malformed-WAL shortcut.
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
        let name = id.iter().map(|b| format!("{b:02x}")).collect::<String>();
        let input_dir = state[0].directory(id);
        std::fs::remove_dir_all(input_dir.join("records")).unwrap();
        copy(
            &scratch.join("inputs").join(name).join("records"),
            &input_dir.join("records"),
        );
        assert_eq!(state[0].read_records(id).unwrap().1, records[..1]);
        let before = tree(&path.join("decision-0"));
        assert!(
            state[0].begin_input(NodeId(1), r, input.clone()).is_err(),
            "BEGIN resumed damaged {binding}"
        );
        assert_eq!(tree(&path.join("decision-0")), before);
        assert!(
            state[0]
                .append_input(
                    NodeId(1),
                    r,
                    id,
                    VsrHistoryBatch::pack(1, &records[1..]).unwrap()
                )
                .is_err(),
            "APPEND repaired {binding}"
        );
        assert_eq!(tree(&path.join("decision-0")), before);
        assert!(
            state[0].retain_chosen_input(chosen, selected).is_err(),
            "CHOSEN import repaired {binding}"
        );
        assert_eq!(tree(&path.join("decision-0")), before);
        drop(state);
        assert!(
            MembershipDecisionStore::open_existing(
                path.join("decision-0"),
                PlacementGroupId(1),
                NodeId(0)
            )
            .is_err()
        );
        assert_eq!(
            tree(&path.join("decision-0")),
            before,
            "cold rejection changed input or journal bytes"
        );
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn membership_unacknowledged_staged_prefix_still_resumes_after_cold_reopen() {
    let path = root("bound-input-valid-staged");
    let (a, b) = approvals(&path);
    let input = VsrMembershipInput::from_approval(&b).unwrap();
    let records = input.flattened_records(&b).unwrap();
    let id = input.id().unwrap();
    let r = round(1, 1);
    let mut store = MembershipDecisionStore::open(path.join("decision"), NodeId(0), &a).unwrap();
    store.promise(NodeId(1), r).unwrap();
    assert_eq!(store.begin_input(NodeId(1), r, input.clone()).unwrap(), 0);
    assert_eq!(
        store
            .append_input(
                NodeId(1),
                r,
                id,
                VsrHistoryBatch {
                    ordinal: 0,
                    records: records[..1].to_vec()
                }
            )
            .unwrap(),
        1
    );
    drop(store);
    let mut store = MembershipDecisionStore::open_existing(
        path.join("decision"),
        PlacementGroupId(1),
        NodeId(0),
    )
    .unwrap();
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
    store.approval(id).unwrap();
    assert!(store.accepted().is_none());
    assert!(store.chosen().is_none());
    drop(store);
    std::fs::remove_dir_all(path).unwrap();
}
