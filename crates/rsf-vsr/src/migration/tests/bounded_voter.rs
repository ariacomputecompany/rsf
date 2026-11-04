use super::*;
use crate::bounded::tests::{limits, snapshot};

#[test]
fn bounded_existing_rejects_unmigrated_legacy_then_reuses_actual_persisted_migration() {
    let root = crate::bounded::tests::root("bounded-legacy");
    let target = legacy(&root);
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(target.join(".vsr-writer.lock"))
        .unwrap();
    let before = snapshot(&target);
    assert!(
        DurableVsrCell::open_existing_bounded(&target, PlacementGroupId(1), limits())
            .unwrap_err()
            .message
            .contains("persisted bootstrap")
    );
    assert_eq!(snapshot(&target), before);
    assert!(migrate(&target).unwrap());
    let cell =
        DurableVsrCell::open_existing_bounded(&target, PlacementGroupId(1), limits()).unwrap();
    assert_eq!(cell.view(), Epoch(2));
    assert_eq!(
        cell.replicas(),
        vec![NodeId(0), NodeId(1), NodeId(2), NodeId(3)]
    );
    assert_eq!(
        cell.request_commit(&VsrClientRequestId::new("client", 7)),
        Some(LogIndex(1))
    );
    // Legacy unilateral reconfiguration remains uncertified. Bounded recovery
    // preserves that restriction rather than inventing a membership plan.
    assert!(cell.membership_lineage().is_err());
    let expected = cell.committed_prefix().unwrap();
    drop(cell);
    let cell = DurableVsrCell::open_existing(&target, PlacementGroupId(1)).unwrap();
    assert_eq!(cell.committed_prefix().unwrap(), expected);
    drop(cell);
    std::fs::remove_dir_all(root).unwrap();
}
