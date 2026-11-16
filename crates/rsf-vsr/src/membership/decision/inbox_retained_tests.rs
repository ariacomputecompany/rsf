use super::*;

fn upload(inbox: &mut MembershipAuthorityInbox, approval: &VsrMembershipApproval) -> [u8; 32] {
    let input = VsrMembershipInput::from_approval(approval).unwrap();
    let id = input.id().unwrap();
    let records = input.flattened_records(approval).unwrap();
    inbox.begin(NodeId(1), input).unwrap();
    let mut at = 0;
    while at < records.len() {
        let batch = VsrHistoryBatch::pack(at as u64, &records[at..]).unwrap();
        at += batch.records.len();
        inbox.append(NodeId(1), id, batch).unwrap();
    }
    inbox.finish(NodeId(1), id).unwrap();
    id
}

#[test]
fn inbox_retains_complete_proof_across_later_selection_without_finishing_that_upload() {
    let path = root("inbox-retained-selection");
    let (a, b) = approvals(&path);
    let mut inbox =
        MembershipAuthorityInbox::open(path.join("inbox"), PlacementGroupId(1), NodeId(3)).unwrap();
    let b_id = upload(&mut inbox, &b);
    let a_id = upload(&mut inbox, &a);
    assert_ne!(a_id, b_id);
    assert!(inbox.finish(NodeId(2), b_id).is_err());
    assert!(inbox.retained_approval(NodeId(99), b_id).is_err());
    for _ in 0..2 {
        let retained = inbox.retained_approval(NodeId(2), b_id).unwrap();
        assert_eq!(
            VsrMembershipInput::from_approval(&retained)
                .unwrap()
                .id()
                .unwrap(),
            b_id
        );
        assert_eq!(inbox.selected_input().unwrap().id().unwrap(), a_id);
    }
    drop(inbox);
    let inbox =
        MembershipAuthorityInbox::open_existing(path.join("inbox"), PlacementGroupId(1), NodeId(3))
            .unwrap();
    assert!(inbox.finish(NodeId(2), b_id).is_err());
    assert_eq!(
        VsrMembershipInput::from_approval(&inbox.retained_approval(NodeId(2), b_id).unwrap())
            .unwrap()
            .id()
            .unwrap(),
        b_id
    );
    assert_eq!(inbox.selected_input().unwrap().id().unwrap(), a_id);
    drop(inbox);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn inbox_retained_lookup_rejects_missing_corrupt_and_incomplete_input() {
    for fault in ["missing", "corrupt", "partial"] {
        let path = root("inbox-retained-invalid");
        let (a, b) = approvals(&path);
        let mut inbox =
            MembershipAuthorityInbox::open(path.join("inbox"), PlacementGroupId(1), NodeId(3))
                .unwrap();
        let b_input = VsrMembershipInput::from_approval(&b).unwrap();
        let b_id = b_input.id().unwrap();
        if fault == "partial" {
            inbox.begin(NodeId(1), b_input).unwrap();
        } else {
            assert_eq!(upload(&mut inbox, &b), b_id);
        }
        let a_id = upload(&mut inbox, &a);
        let directory = path.join("inbox/inputs").join(hex(b_id));
        if fault == "missing" {
            std::fs::remove_dir_all(&directory).unwrap();
        }
        if fault == "corrupt" {
            std::fs::write(directory.join("DESCRIPTOR"), b"damaged retained proof").unwrap();
        }
        for _ in 0..2 {
            assert!(inbox.retained_approval(NodeId(2), b_id).is_err());
            assert_eq!(inbox.selected_input().unwrap().id().unwrap(), a_id);
            if fault == "missing" {
                assert!(!directory.exists());
            }
            if fault == "corrupt" {
                assert_eq!(
                    std::fs::read(directory.join("DESCRIPTOR")).unwrap(),
                    b"damaged retained proof"
                );
            }
        }
        drop(inbox);
        std::fs::remove_dir_all(path).unwrap();
    }
}
