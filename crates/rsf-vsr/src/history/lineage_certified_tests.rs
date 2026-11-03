use super::*;

#[test]
fn certified_lineage_does_not_retroactively_certify_legacy_membership_epochs() {
    let path = path("lineage-certifies-current-only");
    let mut cells = (0..3)
        .map(|node| open(&path.join(format!("cell-{node}"))))
        .collect::<Vec<_>>();
    let genesis = cells[0].membership_lineage().unwrap();
    for cell in &mut cells {
        assert!(matches!(
            cell.reconfigure(Epoch(2), NodeId(0), (0..4).map(NodeId)),
            VsrDecision::MembershipChanged { .. }
        ));
        assert!(cell.membership_lineage().is_err());
    }
    let proposal = VsrMembershipProposal {
        election: VsrViewChangeRequest {
            pg: PlacementGroupId(1),
            previous_view: Epoch(2),
            target_view: Epoch(3),
            candidate: NodeId(1),
            voters: (0..4).map(NodeId).collect(),
        },
        old_primary: NodeId(0),
        new_voters: (0..3).map(NodeId).collect(),
    };
    let approval = VsrMembershipApproval::from_snapshots(
        cells
            .iter_mut()
            .enumerate()
            .map(|(node, cell)| {
                cell.begin_membership(NodeId(node as u64), NodeId(0), proposal.clone())
                    .unwrap()
            })
            .collect(),
    )
    .unwrap();
    let mut decisions = (0..2)
        .map(|node| {
            MembershipDecisionStore::open(
                path.join(format!("decision-{node}")),
                NodeId(node),
                &approval,
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let round = VsrMembershipRound {
        counter: 1,
        coordinator: NodeId(1),
    };
    let promises = decisions
        .iter_mut()
        .map(|store| store.promise(NodeId(1), round).unwrap())
        .collect();
    let decision = VsrMembershipDecision::from_promises(
        promises,
        VsrMembershipInput::from_approval(&approval).unwrap(),
    )
    .unwrap();
    let nomination = decisions[1].nominate(&decision).unwrap();
    let accepted = decisions[0]
        .accept(NodeId(1), &decision, &nomination)
        .unwrap();
    let chosen = VsrMembershipChosen::from_accepted(vec![nomination, accepted]).unwrap();
    let mut stages = (0..2)
        .map(|node| {
            MembershipStagingStore::stage(
                path.join(format!("stage-{node}")),
                NodeId(node as u64),
                VsrMembershipStagedPlan::select(
                    chosen.clone(),
                    VsrMembershipApproval::from_snapshots(approval.snapshots().to_vec()).unwrap(),
                )
                .unwrap(),
                &cells[node],
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let quorum = VsrMembershipStagedQuorum::from_reports(
        stages
            .iter_mut()
            .map(|stage| stage.materialize(&mut |_| Ok(())).unwrap())
            .collect(),
    )
    .unwrap();
    let id = stages[0].plan().id();
    for node in 0..2 {
        cells[node]
            .install_membership(&stages[node], &quorum)
            .unwrap();
        let current = cells[node].membership_lineage().unwrap();
        assert_eq!(current.plan, Some(id));
        assert_eq!(current.bootstrap, genesis.bootstrap);
        assert_eq!(current.voters, genesis.voters);
        assert_eq!(
            cells[node].membership_lineage_at(Epoch(1)).unwrap(),
            Some(genesis.clone())
        );
        assert!(cells[node].membership_lineage_at(Epoch(2)).is_err());
    }
    drop(stages);
    drop(cells);
    drop(decisions);
    let cell = DurableVsrCell::open_existing(path.join("cell-1"), PlacementGroupId(1)).unwrap();
    assert_eq!(cell.membership_lineage().unwrap().plan, Some(id));
    assert!(cell.membership_lineage_at(Epoch(2)).is_err());
    assert_eq!(cell.membership_lineage_at(Epoch(1)).unwrap(), Some(genesis));
    drop(cell);
    std::fs::remove_dir_all(path).unwrap();
}
