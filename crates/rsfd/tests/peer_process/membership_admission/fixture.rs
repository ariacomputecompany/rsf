//! Build certified pre-crash history with independently owned stores. Node 0
//! never reports or publishes either transition and retains unfenced set A.
use super::*;
use rsf_vsr::{
    LearnerHistoryStore, MembershipDecisionStore, MembershipStagingStore, VsrMembershipApproval,
    VsrMembershipChosen, VsrMembershipDecision, VsrMembershipInput, VsrMembershipProposal,
    VsrMembershipRound, VsrMembershipStagedPlan, VsrMembershipStagedQuorum, VsrViewInstallation,
};

fn chosen(root: &Path, approval: &VsrMembershipApproval, voters: &[u64]) -> VsrMembershipChosen {
    let mut decisions = voters
        .iter()
        .map(|node| {
            MembershipDecisionStore::open(
                root.join(format!("decision-{node}")),
                NodeId(*node),
                approval,
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
        VsrMembershipInput::from_approval(approval).unwrap(),
    )
    .unwrap();
    assert_eq!(voters[0], 1);
    let nomination = decisions[0].nominate(&decision).unwrap();
    let mut accepted = vec![nomination.clone()];
    accepted.extend(
        decisions
            .iter_mut()
            .skip(1)
            .map(|store| store.accept(NodeId(1), &decision, &nomination).unwrap()),
    );
    VsrMembershipChosen::from_accepted(accepted).unwrap()
}

fn plan(approval: &VsrMembershipApproval, chosen: &VsrMembershipChosen) -> VsrMembershipStagedPlan {
    VsrMembershipStagedPlan::select(
        chosen.clone(),
        VsrMembershipApproval::from_snapshots(approval.snapshots().to_vec()).unwrap(),
    )
    .unwrap()
}

pub(super) fn returned_membership(root: &Path) -> DurableVsrCell {
    for node in 0..4 {
        let state = root.join(format!("store-{node}"));
        std::fs::create_dir_all(&state).unwrap();
        std::fs::write(
            state.join("IDENTITY.json"),
            serde_json::to_vec(&serde_json::json!({
                "version":1,"cluster":"cli-cluster","node":node
            }))
            .unwrap(),
        )
        .unwrap();
    }
    let mut cells = (0..3)
        .map(|node| {
            DurableVsrCell::open(
                root.join(format!("store-{node}/pg-1")),
                PG,
                Epoch(1),
                NodeId(0),
                (0..3).map(NodeId),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let ordinary = VsrViewChangeRequest {
        pg: PG,
        previous_view: Epoch(1),
        target_view: Epoch(2),
        candidate: NodeId(1),
        voters: (0..3).map(NodeId).collect(),
    };
    let snapshots = cells
        .iter_mut()
        .enumerate()
        .map(|(node, cell)| {
            cell.begin_view_change(NodeId(node as u64), NodeId(1), ordinary.clone())
                .unwrap()
        })
        .collect::<Vec<_>>();
    let installation = VsrViewInstallation::select(&snapshots).unwrap();
    for cell in &mut cells {
        cell.install_view(&installation).unwrap();
        assert!(!cell.has_membership_publication().unwrap());
    }
    let mut learner =
        LearnerHistoryStore::open(root.join("learner-3"), PG, NodeId(3), NodeId(1)).unwrap();
    let history = cells[1].history_snapshot().unwrap();
    let id = history.descriptor.id().unwrap();
    learner.begin(history.descriptor).unwrap();
    for (index, record) in history.records.into_iter().enumerate() {
        learner.append(id, index as u64, record).unwrap();
    }
    learner.finish(id).unwrap();

    let addition = VsrMembershipProposal {
        election: VsrViewChangeRequest {
            pg: PG,
            previous_view: Epoch(2),
            target_view: Epoch(3),
            candidate: NodeId(1),
            voters: (0..3).map(NodeId).collect(),
        },
        old_primary: NodeId(1),
        new_voters: (0..4).map(NodeId).collect(),
    };
    let approval = VsrMembershipApproval::from_snapshots(
        [1, 2]
            .into_iter()
            .map(|node| {
                cells[node]
                    .begin_membership(NodeId(node as u64), NodeId(1), addition.clone())
                    .unwrap()
            })
            .collect(),
    )
    .unwrap();
    let proof = chosen(&root.join("addition"), &approval, &[1, 2, 3]);
    let mut stages = [1, 2]
        .into_iter()
        .map(|node| {
            MembershipStagingStore::stage(
                root.join(format!("addition/stage-{node}")),
                NodeId(node as u64),
                plan(&approval, &proof),
                &cells[node],
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    stages.push(
        learner
            .stage_membership(root.join("addition/stage-3"), plan(&approval, &proof))
            .unwrap(),
    );
    let reports = stages
        .iter_mut()
        .map(|stage| stage.materialize(&mut |_| Ok(())).unwrap())
        .collect();
    let quorum = VsrMembershipStagedQuorum::from_reports(reports).unwrap();
    for (node, stage) in [1, 2].into_iter().zip(&stages) {
        cells[node].install_membership(stage, &quorum).unwrap();
    }
    let mut third = learner
        .promote_membership(root.join("store-3/pg-1"), &stages[2], &quorum)
        .unwrap();

    let removal = VsrMembershipProposal {
        election: VsrViewChangeRequest {
            pg: PG,
            previous_view: Epoch(3),
            target_view: Epoch(4),
            candidate: NodeId(1),
            voters: (0..4).map(NodeId).collect(),
        },
        old_primary: NodeId(1),
        new_voters: (0..3).map(NodeId).collect(),
    };
    let mut reports = [1, 2]
        .into_iter()
        .map(|node| {
            cells[node]
                .begin_membership(NodeId(node as u64), NodeId(1), removal.clone())
                .unwrap()
        })
        .collect::<Vec<_>>();
    reports.push(
        third
            .begin_membership(NodeId(3), NodeId(1), removal)
            .unwrap(),
    );
    let approval = VsrMembershipApproval::from_snapshots(reports).unwrap();
    let proof = chosen(&root.join("removal"), &approval, &[1, 2]);
    let mut stages = [1, 2]
        .into_iter()
        .map(|node| {
            MembershipStagingStore::stage(
                root.join(format!("removal/stage-{node}")),
                NodeId(node as u64),
                plan(&approval, &proof),
                &cells[node],
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let reports = stages
        .iter_mut()
        .map(|stage| stage.materialize(&mut |_| Ok(())).unwrap())
        .collect();
    let quorum = VsrMembershipStagedQuorum::from_reports(reports).unwrap();
    for (node, stage) in [1, 2].into_iter().zip(&stages) {
        cells[node].install_membership(stage, &quorum).unwrap();
        assert_eq!(cells[node].view(), Epoch(4));
        assert!(cells[node].has_membership_publication().unwrap());
    }
    assert_eq!(cells[0].view(), Epoch(2));
    assert!(cells[0].membership_fence().is_none());
    assert!(!cells[0].has_membership_publication().unwrap());
    cells.swap_remove(1)
}
