use super::*;
use crate::bounded::tests::{limits, snapshot};
use std::fs;

#[test]
fn bounded_promoted_voter_recovers_actual_role_tail_and_receipts_without_source_or_learner() {
    let path = crate::bounded::tests::root("bounded-promoted");
    let r = ready(&path);
    let target = path.join("promoted");
    let cell =
        r.f.learner
            .promote_membership(&target, &r.stages[2], &r.quorum)
            .unwrap();
    let expected = cell.committed_prefix().unwrap();
    let lineage = cell.membership_lineage().unwrap();
    let accepted = cell.prepared(LogIndex(3)).unwrap().clone();
    drop(cell);
    drop(r);
    fs::remove_dir_all(path.join("old")).unwrap();
    fs::remove_dir_all(path.join("learner")).unwrap();
    let cell =
        DurableVsrCell::open_existing_bounded(&target, PlacementGroupId(1), limits()).unwrap();
    assert_prefix(&cell);
    assert_eq!(cell.promoted_replica().unwrap(), Some(NodeId(3)));
    assert_eq!(cell.membership_lineage().unwrap(), lineage);
    assert_eq!(cell.committed_prefix().unwrap(), expected);
    assert_eq!(cell.prepared(LogIndex(3)), Some(&accepted));
    assert!(!target.join("MANIFEST").exists());
    drop(cell);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn bounded_membership_installed_old_voter_retains_certified_lineage_without_promotion_claim() {
    let path = crate::bounded::tests::root("bounded-member-voter");
    let mut r = ready(&path);
    r.f.cells[1]
        .install_membership(&r.stages[1], &r.quorum)
        .unwrap();
    let target = r.f.cells[1].root.clone();
    let expected = r.f.cells[1].committed_prefix().unwrap();
    let lineage = r.f.cells[1].membership_lineage().unwrap();
    let accepted = r.f.cells[1].prepared(LogIndex(3)).unwrap().clone();
    assert!(lineage.plan.is_some());
    drop(r);
    let cell =
        DurableVsrCell::open_existing_bounded(&target, PlacementGroupId(1), limits()).unwrap();
    assert_eq!(cell.promoted_replica().unwrap(), None);
    assert_eq!(cell.membership_lineage().unwrap(), lineage);
    assert_eq!(cell.committed_prefix().unwrap(), expected);
    assert_eq!(cell.prepared(LogIndex(3)), Some(&accepted));
    assert_eq!(
        cell.request_commit(&VsrClientRequestId::new("original", 1)),
        Some(LogIndex(1))
    );
    assert!(cell.primary_probe().is_ok());
    drop(cell);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn bounded_fenced_voter_replays_exact_existing_membership_fence_without_unfencing() {
    let path = crate::bounded::tests::root("bounded-fence");
    let mut f = fixture(&path);
    let source = f.cells.remove(1);
    let target = source.root.clone();
    let fence = source.membership_fence().unwrap().clone();
    let expected = source.committed_prefix().unwrap();
    let accepted = source.prepared(LogIndex(3)).unwrap().clone();
    drop(source);
    let cell =
        DurableVsrCell::open_existing_bounded(&target, PlacementGroupId(1), limits()).unwrap();
    assert_eq!(cell.membership_fence(), Some(&fence));
    assert_eq!(cell.committed_prefix().unwrap(), expected);
    assert_eq!(cell.prepared(LogIndex(3)), Some(&accepted));
    assert!(cell.primary_probe().is_err());
    assert!(cell.state_token().is_err());
    assert!(cell.recovery_state_token().is_ok());
    drop(cell);
    drop(f);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn bounded_promotion_requires_existing_exact_bounded_owner_and_active_pointer() {
    for fault in [
        "missing-owner",
        "bad-owner",
        "oversized-owner",
        "missing-pointer",
        "aliased-owner",
    ] {
        let path = crate::bounded::tests::root("bounded-promotion-damaged");
        let r = ready(&path);
        let target = path.join("promoted");
        let cell =
            r.f.learner
                .promote_membership(&target, &r.stages[2], &r.quorum)
                .unwrap();
        drop(cell);
        drop(r);
        let owner = target.join("VSR_PROMOTION");
        match fault {
            "missing-owner" => fs::remove_file(&owner).unwrap(),
            "bad-owner" => fs::write(&owner, b"foreign promotion identity").unwrap(),
            "oversized-owner" => fs::OpenOptions::new()
                .write(true)
                .open(&owner)
                .unwrap()
                .set_len(1024 * 1024)
                .unwrap(),
            "missing-pointer" => fs::remove_file(target.join("VSR_ACTIVE")).unwrap(),
            "aliased-owner" => {
                let saved = path.join("saved-owner");
                fs::rename(&owner, &saved).unwrap();
                std::os::unix::fs::symlink(saved, &owner).unwrap();
            }
            _ => unreachable!(),
        }
        let before = snapshot(&target);
        assert!(
            DurableVsrCell::open_existing_bounded(&target, PlacementGroupId(1), limits()).is_err(),
            "{fault}"
        );
        assert_eq!(snapshot(&target), before, "{fault}");
        fs::remove_dir_all(path).unwrap();
    }
}
