use super::*;
use std::fs;
fn pending(
    path: &Path,
) -> (
    MembershipDecisionStore,
    VsrMembershipApproval,
    VsrMembershipChosen,
) {
    let (accepted, approval, chosen) = accepted_b(path);
    drop(accepted);
    let owner = MembershipDecisionStore::open_existing(
        path.join("decision-3"),
        PlacementGroupId(1),
        NodeId(3),
    )
    .unwrap();
    (owner, approval, chosen)
}
fn segment(owner: &MembershipDecisionStore, id: [u8; 32]) -> std::path::PathBuf {
    owner
        .directory(id)
        .join("records/segments/000000000000000001.segment")
}
fn fence(error: rsf_core::RsfError) {
    assert_eq!(error.code, rsf_core::ErrorCode::CorruptionDetected);
    assert_eq!(
        error.message,
        "membership decision durability failed; reopen before further use"
    );
}
#[test]
fn decision_copy_retains_opening_manifest_custody_and_fences_after_restoration() {
    let path = root("decision-copy-manifest-owner");
    let (mut owner, approval, chosen) = pending(&path);
    let input = VsrMembershipInput::from_approval(&approval).unwrap();
    let id = input.id().unwrap();
    disk::create_input(&owner.root, &input).unwrap();
    let manifest = owner.directory(id).join("records/MANIFEST");
    let saved = manifest.with_extension("held-original");
    let before = fs::read(&manifest).unwrap();
    let original_records = fs::read(segment(&owner, id)).unwrap();
    let journal = owner
        .journal
        .recover_current()
        .unwrap()
        .recovery
        .wal_records;
    let moved = manifest.clone();
    let original = saved.clone();
    let bytes = before.clone();
    let expected_root = owner.root.clone();
    let hit = std::rc::Rc::new(std::cell::Cell::new(false));
    let witness = hit.clone();
    let guard = disk::decision_open_hook::install(move |root| {
        assert_eq!(root, expected_root);
        witness.set(true);
        fs::rename(&moved, &original).unwrap();
        fs::write(&moved, &bytes).unwrap();
    });
    let result = owner.retain_chosen_input(chosen.clone(), &approval);
    drop(guard);
    assert!(hit.get());
    fs::remove_file(&manifest).unwrap();
    fs::rename(&saved, &manifest).unwrap();
    let error =
        result.expect_err("decision copy must reject a substituted same-byte opening manifest");
    assert_eq!(error.code, rsf_core::ErrorCode::CorruptionDetected);
    assert_eq!(
        error.message,
        "exclusive store physical custody changed; reopen required"
    );
    assert!(owner.chosen().is_none());
    assert_eq!(
        owner
            .journal
            .recover_current()
            .unwrap()
            .recovery
            .wal_records,
        journal
    );
    assert_eq!(fs::read(&manifest).unwrap(), before);
    assert_eq!(fs::read(segment(&owner, id)).unwrap(), original_records);
    fence(
        owner
            .retain_chosen_input(chosen.clone(), &approval)
            .unwrap_err(),
    );
    drop(owner);
    let mut cold = MembershipDecisionStore::open_existing(
        path.join("decision-3"),
        PlacementGroupId(1),
        NodeId(3),
    )
    .unwrap();
    assert!(cold.chosen().is_none());
    cold.retain_chosen_input(chosen.clone(), &approval).unwrap();
    assert_eq!(cold.chosen(), Some(&chosen));
    assert_eq!(
        cold.input(id).unwrap().1,
        input.flattened_records(&approval).unwrap()
    );
    drop(cold);
    let cold = MembershipDecisionStore::open_existing(
        path.join("decision-3"),
        PlacementGroupId(1),
        NodeId(3),
    )
    .unwrap();
    assert_eq!(cold.chosen(), Some(&chosen));
    drop(cold);
    fs::remove_dir_all(path).unwrap();
}
#[test]
fn decision_copy_resumes_staged_prefixes_and_preserves_cold_choice() {
    for count in [0, 1] {
        let path = root("decision-copy-prefix");
        let (mut owner, approval, chosen) = pending(&path);
        let input = VsrMembershipInput::from_approval(&approval).unwrap();
        let id = input.id().unwrap();
        let records = input.flattened_records(&approval).unwrap();
        owner.promise(NodeId(1), round(1, 1)).unwrap();
        owner.begin_input(NodeId(1), round(1, 1), input).unwrap();
        if count > 0 {
            owner
                .append_input(
                    NodeId(1),
                    round(1, 1),
                    id,
                    VsrHistoryBatch::pack(0, &records[..count]).unwrap(),
                )
                .unwrap();
        }
        let before = fs::read(segment(&owner, id)).unwrap();
        owner
            .retain_chosen_input(chosen.clone(), &approval)
            .unwrap();
        let after = fs::read(segment(&owner, id)).unwrap();
        assert!(after.starts_with(&before));
        assert_eq!(owner.input(id).unwrap().1, records);
        owner
            .retain_chosen_input(chosen.clone(), &approval)
            .unwrap();
        assert_eq!(fs::read(segment(&owner, id)).unwrap(), after);
        drop(owner);
        let cold = MembershipDecisionStore::open_existing(
            path.join("decision-3"),
            PlacementGroupId(1),
            NodeId(3),
        )
        .unwrap();
        assert_eq!(cold.chosen(), Some(&chosen));
        assert_eq!(cold.input(id).unwrap().1, records);
        drop(cold);
        fs::remove_dir_all(path).unwrap();
    }
}
#[test]
fn decision_copy_never_repairs_corrupt_acknowledged_input() {
    let path = root("decision-copy-bound-corrupt");
    let (mut owner, approval, chosen) = accepted_b(&path);
    let id = chosen.input_id();
    let target = segment(&owner, id);
    let mut bytes = fs::read(&target).unwrap();
    *bytes.last_mut().unwrap() ^= 1;
    fs::write(&target, &bytes).unwrap();
    let journal = owner
        .journal
        .recover_current()
        .unwrap()
        .recovery
        .wal_records;
    let guard = disk::decision_open_hook::install(|_| {
        panic!("acknowledged corrupt input must not enter create factory")
    });
    let error = owner.retain_chosen_input(chosen, &approval).unwrap_err();
    drop(guard);
    assert_eq!(error.code, rsf_core::ErrorCode::CorruptionDetected);
    assert!(
        error.message.starts_with("checksum mismatch at offset "),
        "{}",
        error.message
    );
    assert_eq!(fs::read(&target).unwrap(), bytes);
    assert_eq!(
        owner
            .journal
            .recover_current()
            .unwrap()
            .recovery
            .wal_records,
        journal
    );
    assert!(owner.chosen().is_none());
    drop(owner);
    fs::remove_dir_all(path).unwrap();
}
