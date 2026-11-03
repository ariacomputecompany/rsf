use super::*;

#[test]
fn membership_lineage_is_stable_across_ordinary_views_and_exact_across_cold_history() {
    let path = path("lineage-ordinary");
    let mut cell = open(&path);
    let genesis = cell.membership_lineage().unwrap();
    assert_eq!(genesis.pg, PlacementGroupId(1));
    assert_eq!(genesis.plan, None);
    assert_eq!(genesis.voters, (0..3).map(NodeId).collect::<Vec<_>>());
    assert_eq!(
        genesis.bootstrap,
        rsf_core::Digest::blake3_bytes(&cell.initial_bootstrap).bytes
    );
    commit(&mut cell, &[NodeId(1)]);
    assert!(matches!(
        cell.view_change(Epoch(3), NodeId(2)),
        VsrDecision::ViewChanged { .. }
    ));
    let entry = commit(&mut cell, &[NodeId(0)]);
    assert_eq!(entry.prepare.view, Epoch(3));
    let token = cell.recovery_state_token().unwrap();
    for _ in 0..3 {
        assert_eq!(cell.membership_lineage().unwrap(), genesis);
        assert_eq!(
            cell.membership_lineage_at(entry.prepare.view).unwrap(),
            Some(genesis.clone())
        );
        assert_eq!(cell.membership_lineage_at(Epoch(2)).unwrap(), None);
        assert_eq!(cell.membership_lineage_at(Epoch(4)).unwrap(), None);
    }
    assert_eq!(cell.recovery_state_token().unwrap(), token);
    drop(cell);
    // Caller bootstrap values cannot replace the original persisted identity.
    let cell = DurableVsrCell::open(
        &path,
        PlacementGroupId(1),
        Epoch(99),
        NodeId(9),
        [NodeId(9)],
    )
    .unwrap();
    assert_eq!(cell.membership_lineage().unwrap(), genesis);
    assert_eq!(
        cell.membership_lineage_at(Epoch(1)).unwrap(),
        Some(genesis.clone())
    );
    assert_eq!(cell.membership_lineage_at(Epoch(3)).unwrap(), Some(genesis));
    assert_eq!(cell.membership_lineage_at(Epoch(2)).unwrap(), None);
    drop(cell);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn legacy_membership_round_trip_never_aliases_bootstrap_lineage() {
    let path = path("lineage-legacy-membership");
    let mut cell = open(&path);
    let genesis = cell.membership_lineage().unwrap();
    assert!(matches!(
        cell.reconfigure(Epoch(2), NodeId(0), (0..4).map(NodeId)),
        VsrDecision::MembershipChanged { .. }
    ));
    assert!(matches!(
        cell.reconfigure(Epoch(3), NodeId(0), (0..3).map(NodeId)),
        VsrDecision::MembershipChanged { .. }
    ));
    assert!(matches!(
        cell.view_change(Epoch(5), NodeId(2)),
        VsrDecision::ViewChanged { .. }
    ));
    assert_eq!(cell.replicas(), genesis.voters);
    for cold in [false, true] {
        if cold {
            drop(cell);
            cell = open(&path);
        }
        assert!(cell.membership_lineage().is_err());
        assert_eq!(
            cell.membership_lineage_at(Epoch(1)).unwrap(),
            Some(genesis.clone())
        );
        for view in [2, 3, 5] {
            assert!(cell.membership_lineage_at(Epoch(view)).is_err());
        }
        assert_eq!(cell.membership_lineage_at(Epoch(4)).unwrap(), None);
        assert!(!cell.has_membership_publication().unwrap());
    }
    drop(cell);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn unpersisted_legacy_bootstrap_does_not_supply_lineage_before_explicit_migration() {
    let root = path("lineage-legacy-bootstrap");
    let mut source = open(&root.join("source"));
    let expected = source.membership_lineage().unwrap();
    commit(&mut source, &[NodeId(1)]);
    drop(source);
    let records = SegmentStore::recover_existing(root.join("source"))
        .unwrap()
        .recovery
        .wal_records;
    let target = root.join("legacy");
    let mut store = SegmentStore::open(&target).unwrap();
    for record in records.iter().skip(1) {
        store
            .append_next_wal(record.pg, record.epoch, record.kind, &record.payload)
            .unwrap();
    }
    drop(store);
    let legacy = open(&target);
    assert!(legacy.membership_lineage().is_err());
    assert!(legacy.membership_lineage_at(Epoch(1)).is_err());
    assert!(legacy.membership_lineage_at(Epoch(99)).is_err());
    drop(legacy);
    DurableVsrCell::migrate_legacy_bootstrap(
        &target,
        PlacementGroupId(1),
        Epoch(1),
        NodeId(0),
        (0..3).map(NodeId),
    )
    .unwrap();
    let migrated = DurableVsrCell::open_existing(&target, PlacementGroupId(1)).unwrap();
    assert_eq!(migrated.membership_lineage().unwrap(), expected);
    drop(migrated);
    std::fs::remove_dir_all(root).unwrap();
}
