//! Real durable histories for replay across uncertain commits and ordinary views.
//! Calls are trusted in-process fixtures; this does not qualify network provenance.
use super::*;
use rsf_vsr::{
    MembershipDecisionStore, MembershipStagingStore, VsrCommit, VsrCommittedEntry,
    VsrMembershipApproval, VsrMembershipChosen, VsrMembershipDecision, VsrMembershipInput,
    VsrMembershipRound, VsrMembershipStagedPlan, VsrMembershipStagedQuorum, VsrPrepare,
    VsrViewInstallation,
};
use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};

type Cells = BTreeMap<NodeId, DurableVsrCell>;

fn fixture(name: &str) -> (PathBuf, ControlCatalog, Cells, Cells) {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let sequence = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let root = std::env::temp_dir().join(format!(
        "rsf-control-{name}-{}-{sequence}",
        std::process::id()
    ));
    std::fs::create_dir(&root).unwrap();
    let mut initial = catalog(2, 4);
    let mut groups = Vec::new();
    for group in &mut initial.groups {
        let cells = (0..3)
            .map(|node| {
                let cell = DurableVsrCell::open(
                    cell_path(&root, group.pg, NodeId(node)),
                    group.pg,
                    Epoch(1),
                    NodeId(0),
                    (0..3).map(NodeId),
                )
                .unwrap();
                (NodeId(node), cell)
            })
            .collect::<Cells>();
        group.lineage.bootstrap = cells[&NodeId(0)].membership_lineage().unwrap().bootstrap;
        groups.push(cells);
    }
    let data = groups.pop().unwrap();
    let mut control = groups.pop().unwrap();
    let prepare = prepare(
        &mut control,
        NodeId(0),
        ControlCommand::Genesis(initial.clone()),
    );
    commit(
        &mut control,
        NodeId(0),
        prepare.index,
        &[NodeId(1), NodeId(2)],
    );
    (root, initial, control, data)
}

fn cell_path(root: &Path, pg: PlacementGroupId, node: NodeId) -> PathBuf {
    root.join(format!("pg-{}/node-{}", pg.0, node.0))
}

fn prepare(cells: &mut Cells, primary: NodeId, command: ControlCommand) -> VsrPrepare {
    let payload = command.encode().unwrap();
    let bytes = payload.len() as u32;
    let index = cells[&primary].next_index();
    let VsrDecision::Prepared(prepare) = cells
        .get_mut(&primary)
        .unwrap()
        .prepare(
            primary,
            VsrOperation::ReplicatedCommand {
                operation_id: OperationId(index.0),
                payload,
            },
            bytes,
        )
        .unwrap()
    else {
        panic!("control prepare rejected");
    };
    acknowledge(cells, &prepare);
    prepare
}

fn acknowledge(cells: &mut Cells, prepare: &VsrPrepare) {
    for node in cells[&prepare.primary].replicas() {
        if node == prepare.primary {
            continue;
        }
        cells
            .get_mut(&node)
            .unwrap()
            .receive_prepare(node, prepare.primary, prepare.clone())
            .unwrap();
        assert!(matches!(
            cells
                .get_mut(&prepare.primary)
                .unwrap()
                .acknowledge(node, prepare.view, prepare.index),
            VsrDecision::Acknowledged { .. }
        ));
    }
}

fn commit(cells: &mut Cells, primary: NodeId, index: LogIndex, followers: &[NodeId]) -> VsrCommit {
    let VsrDecision::Committed(commit) = cells
        .get_mut(&primary)
        .unwrap()
        .commit(primary, index)
        .unwrap()
    else {
        panic!("control commit rejected");
    };
    for node in followers {
        cells
            .get_mut(node)
            .unwrap()
            .receive_commit(*node, primary, commit.clone())
            .unwrap();
    }
    commit
}

fn elect(cells: &mut Cells, previous: u64, target: u64, primary: NodeId, participants: &[NodeId]) {
    let request = VsrViewChangeRequest {
        pg: cells[&primary].membership_lineage().unwrap().pg,
        previous_view: Epoch(previous),
        target_view: Epoch(target),
        candidate: primary,
        voters: cells[&primary].replicas(),
    };
    let snapshots = participants
        .iter()
        .map(|node| {
            cells
                .get_mut(node)
                .unwrap()
                .begin_view_change(*node, primary, request.clone())
                .unwrap()
        })
        .collect::<Vec<_>>();
    let installation = VsrViewInstallation::select(&snapshots).unwrap();
    for node in participants {
        cells
            .get_mut(node)
            .unwrap()
            .install_view(&installation)
            .unwrap();
    }
}

fn reopen(root: &Path, pg: PlacementGroupId, cells: &mut Cells) {
    let nodes = cells.keys().copied().collect::<Vec<_>>();
    drop(std::mem::take(cells));
    for node in nodes {
        cells.insert(
            node,
            DurableVsrCell::open_existing(cell_path(root, pg, node), pg).unwrap(),
        );
    }
}

fn assert_pending(cell: &DurableVsrCell, initial: &ControlCatalog, begin: &MembershipIntent) {
    let state = ControlState::recover(&anchor(initial), cell).unwrap();
    let accepted = ControlOutcome::IntentAccepted {
        intent: begin.id().unwrap(),
    };
    assert_eq!(state.committed_tail(), LogIndex(2));
    assert_eq!(state.catalog(), initial);
    assert_eq!(state.root(), initial.root().unwrap());
    assert_eq!(state.revision(), ControlRevision(1));
    assert_eq!(state.pending(), Some(begin));
    assert_eq!(state.last_outcome(), &accepted);
    assert_eq!(state.request_outcome(begin.request), Some(&accepted));
}

#[test]
fn uncertain_begin_commit_keeps_its_result_after_survivor_reprepare_and_cold_replay() {
    let (root, initial, mut control, data) = fixture("uncertain-begin");
    let begin = intent(&initial, 1, 20, 1, 0, &[1, 2]);
    let original = prepare(
        &mut control,
        NodeId(0),
        ControlCommand::Begin(begin.clone()),
    );
    let old_commit = commit(&mut control, NodeId(0), original.index, &[]);
    assert_eq!(old_commit.view, Epoch(1));
    assert_pending(&control[&NodeId(0)], &initial, &begin);
    for node in [NodeId(1), NodeId(2)] {
        let state = ControlState::recover(&anchor(&initial), &control[&node]).unwrap();
        assert_eq!(state.committed_tail(), LogIndex(1));
        assert!(state.pending().is_none());
        assert_eq!(control[&node].prepared(original.index), Some(&original));
    }

    // The election quorum excludes the sole holder of the old commit record.
    elect(&mut control, 1, 2, NodeId(1), &[NodeId(1), NodeId(2)]);
    let reprepared = control
        .get_mut(&NodeId(1))
        .unwrap()
        .reprepare(NodeId(1), original.index)
        .unwrap();
    assert_eq!(reprepared.index, original.index);
    assert_eq!(reprepared.operation, original.operation);
    assert_eq!(reprepared.view, Epoch(2));
    assert_ne!(reprepared.primary, original.primary);
    // Node 0 is partitioned: only the current survivor quorum receives prepare.
    control
        .get_mut(&NodeId(2))
        .unwrap()
        .receive_prepare(NodeId(2), NodeId(1), reprepared.clone())
        .unwrap();
    control
        .get_mut(&NodeId(1))
        .unwrap()
        .acknowledge(NodeId(2), Epoch(2), original.index);
    let new_commit = commit(&mut control, NodeId(1), original.index, &[NodeId(2)]);
    assert_eq!(new_commit.view, Epoch(2));
    assert_eq!(
        new_commit.certificate.operation,
        old_commit.certificate.operation
    );
    for cell in control.values() {
        assert_pending(cell, &initial, &begin);
    }

    reopen(&root, initial.control_pg, &mut control);
    assert_eq!(
        control[&NodeId(0)].prepared(original.index).unwrap().view,
        Epoch(1)
    );
    assert_eq!(
        control[&NodeId(1)].prepared(original.index).unwrap().view,
        Epoch(2)
    );
    for cell in control.values() {
        assert_pending(cell, &initial, &begin);
    }

    let proposal = &begin.groups[0].proposal;
    assert!(
        control
            .get_mut(&NodeId(1))
            .unwrap()
            .begin_membership(NodeId(1), proposal.old_primary, proposal.clone())
            .is_err(),
        "the stale concrete proposal must not silently rebase"
    );
    assert!(control[&NodeId(1)].membership_fence().is_none());
    assert_pending(&control[&NodeId(1)], &initial, &begin);

    // The old primary can later install the survivor's equal-operation commit.
    elect(
        &mut control,
        1,
        3,
        NodeId(1),
        &[NodeId(0), NodeId(1), NodeId(2)],
    );
    reopen(&root, initial.control_pg, &mut control);
    for cell in control.values() {
        assert_eq!(cell.view(), Epoch(3));
        assert_eq!(cell.prepared(original.index).unwrap().view, Epoch(2));
        assert_pending(cell, &initial, &begin);
    }
    drop(control);
    drop(data);
    std::fs::remove_dir_all(root).unwrap();
}

// A synced payload journal is the fixture's materialization sink. It deliberately
// does not claim to be the daemon projector or an application readiness test.
fn materialize(file: &mut std::fs::File, entry: &VsrCommittedEntry) -> RsfResult<()> {
    let VsrOperation::ReplicatedCommand { payload, .. } = &entry.prepare.operation else {
        return Err(invalid("fixture expected a control command"));
    };
    ControlCommand::decode(payload)?;
    file.write_all(&(payload.len() as u64).to_le_bytes())
        .and_then(|()| file.write_all(payload))
        .and_then(|()| file.sync_all())
        .map_err(|error| invalid(error.to_string()))
}

fn publish(root: &Path, cells: &mut Cells, proposal: &VsrMembershipProposal) -> GroupPublication {
    publish_from(root, cells, proposal, &proposal.election.voters)
}

fn publish_from(
    root: &Path,
    cells: &mut Cells,
    proposal: &VsrMembershipProposal,
    old_reporters: &[NodeId],
) -> GroupPublication {
    let pg = proposal.election.pg;
    let approval = VsrMembershipApproval::from_snapshots(
        old_reporters
            .iter()
            .map(|node| {
                cells
                    .get_mut(node)
                    .unwrap()
                    .begin_membership(*node, proposal.old_primary, proposal.clone())
                    .unwrap()
            })
            .collect(),
    )
    .unwrap();
    let mut decisions = proposal
        .new_voters
        .iter()
        .map(|node| {
            (
                *node,
                MembershipDecisionStore::open(
                    root.join(format!("decision-{}-{}", pg.0, node.0)),
                    *node,
                    &approval,
                )
                .unwrap(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let coordinator = proposal.election.candidate;
    let round = VsrMembershipRound {
        counter: 1,
        coordinator,
    };
    let promises = decisions
        .values_mut()
        .map(|store| store.promise(coordinator, round).unwrap())
        .collect();
    let decision = VsrMembershipDecision::from_promises(
        promises,
        VsrMembershipInput::from_approval(&approval).unwrap(),
    )
    .unwrap();
    let nomination = decisions
        .get_mut(&coordinator)
        .unwrap()
        .nominate(&decision)
        .unwrap();
    let mut accepted = vec![nomination.clone()];
    for (node, store) in &mut decisions {
        if *node != coordinator {
            accepted.push(store.accept(coordinator, &decision, &nomination).unwrap());
        }
    }
    let chosen = VsrMembershipChosen::from_accepted(accepted).unwrap();
    let mut stages = proposal
        .new_voters
        .iter()
        .map(|node| {
            let plan = VsrMembershipStagedPlan::select(
                chosen.clone(),
                VsrMembershipApproval::from_snapshots(approval.snapshots().to_vec()).unwrap(),
            )
            .unwrap();
            (
                *node,
                MembershipStagingStore::stage(
                    root.join(format!("stage-{}-{}", pg.0, node.0)),
                    *node,
                    plan,
                    &cells[node],
                )
                .unwrap(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut reports = Vec::new();
    for (node, stage) in &mut stages {
        let mut file =
            std::fs::File::create(root.join(format!("materialized-{}-{}", pg.0, node.0))).unwrap();
        reports.push(
            stage
                .materialize(&mut |entry| materialize(&mut file, entry))
                .unwrap(),
        );
    }
    let staged = VsrMembershipStagedQuorum::from_reports(reports).unwrap();
    let mut reports = Vec::new();
    for (node, stage) in &stages {
        let cell = cells.get_mut(node).unwrap();
        cell.install_membership(stage, &staged).unwrap();
        let proof = cell
            .membership_publication(stage.plan().id())
            .unwrap()
            .unwrap();
        reports.push(VsrMembershipPublishedReport {
            proposal: proof.proposal().clone(),
            replica: *node,
            input: proof.input_id(),
            plan: proof.plan_id(),
        });
    }
    GroupPublication {
        pg,
        quorum: VsrMembershipPublishedQuorum::from_reports(reports).unwrap(),
    }
}

#[test]
fn finalized_catalog_and_admission_survive_an_ordinary_control_election_and_retry() {
    let (root, initial, mut control, mut data) = fixture("finalized-election");
    let trusted = anchor(&initial);
    let begin = intent(&initial, 1, 30, 1, 0, &[1, 2]);
    let prepared = prepare(
        &mut control,
        NodeId(0),
        ControlCommand::Begin(begin.clone()),
    );
    commit(
        &mut control,
        NodeId(0),
        prepared.index,
        &[NodeId(1), NodeId(2)],
    );
    let data_publication = publish(&root, &mut data, &begin.groups[1].proposal);
    let control_publication = publish(&root, &mut control, &begin.groups[0].proposal);
    let finalization = MembershipFinalization {
        intent: begin.id().unwrap(),
        parent: begin.parent,
        proofs: vec![control_publication, data_publication],
    };
    let pending = ControlState::recover(&trusted, &control[&NodeId(1)]).unwrap();
    assert!(pending.admission(&control[&NodeId(1)]).is_err());
    assert_eq!(pending.pending(), Some(&begin));
    let prepared = prepare(
        &mut control,
        NodeId(1),
        ControlCommand::Finalize(finalization.clone()),
    );
    commit(&mut control, NodeId(1), prepared.index, &[NodeId(2)]);
    let completed = ControlState::recover(&trusted, &control[&NodeId(1)]).unwrap();
    let applied = completed.last_outcome().clone();
    let catalog = completed.catalog().clone();
    let catalog_root = completed.root();
    assert_eq!(completed.revision(), ControlRevision(2));
    assert!(completed.pending().is_none());
    let old_admission = completed.admission(&control[&NodeId(1)]).unwrap();
    let owned_data = BTreeMap::from([(PlacementGroupId(1), data.remove(&NodeId(1)).unwrap())]);
    assert_eq!(
        old_admission
            .validate_authority(
                &control[&NodeId(1)],
                &owned_data,
                NodeId(1),
                &[2; 32],
                initial.routing
            )
            .unwrap(),
        NodeId(1)
    );

    // A different control primary does not alter the common data primary.
    elect(&mut control, 2, 3, NodeId(2), &[NodeId(1), NodeId(2)]);
    assert!(
        old_admission
            .validate_authority(
                &control[&NodeId(1)],
                &owned_data,
                NodeId(1),
                &[2; 32],
                initial.routing
            )
            .is_err(),
        "the old live stamp must expire"
    );
    for node in [NodeId(1), NodeId(2)] {
        let state = ControlState::recover(&trusted, &control[&node]).unwrap();
        assert_eq!(state.catalog(), &catalog);
        assert_eq!(state.root(), catalog_root);
        assert_eq!(state.last_outcome(), &applied);
        assert_eq!(state.revision(), ControlRevision(2));
        assert_eq!(
            state.publication(begin.id().unwrap()),
            Some(finalization.proofs.as_slice())
        );
    }
    let retry = prepare(
        &mut control,
        NodeId(2),
        ControlCommand::Finalize(finalization.clone()),
    );
    commit(&mut control, NodeId(2), retry.index, &[NodeId(1)]);
    reopen(&root, initial.control_pg, &mut control);
    for node in [NodeId(1), NodeId(2)] {
        let state = ControlState::recover(&trusted, &control[&node]).unwrap();
        assert_eq!(state.catalog(), &catalog);
        assert_eq!(state.root(), catalog_root);
        assert_eq!(state.revision(), ControlRevision(2));
        assert_eq!(state.last_outcome(), &applied);
        assert_eq!(
            state.publication(begin.id().unwrap()),
            Some(finalization.proofs.as_slice())
        );
    }
    let local = ControlState::recover(&trusted, &control[&NodeId(1)]).unwrap();
    assert_eq!(
        local
            .admission(&control[&NodeId(1)])
            .unwrap()
            .validate_authority(
                &control[&NodeId(1)],
                &owned_data,
                NodeId(1),
                &[2; 32],
                initial.routing
            )
            .unwrap(),
        NodeId(1)
    );
    drop(control);
    drop(owned_data);
    drop(data);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn old_control_commit_majority_prevents_begin_reprepare_across_membership() {
    // This diagnostic deliberately invokes control-independent kernel fences.
    // The unconfirmed branch must be unreachable through future managed APIs.
    // Passing it documents the missing precondition; it does not implement it.
    for confirmed in [false, true] {
        let (root, initial, mut control, data) = fixture(if confirmed {
            "membership-confirmed-begin"
        } else {
            "membership-unconfirmed-begin"
        });
        let trusted = anchor(&initial);
        let begin = intent(&initial, 1, 40, 1, 0, &[1, 2]);
        let original = prepare(
            &mut control,
            NodeId(0),
            ControlCommand::Begin(begin.clone()),
        );
        // Node 1 wins source selection but has only accepted the intent. In the
        // confirmed case, the old commit quorum {0,2} intersects reports {1,2}
        // at node 2, so staging must learn that commit into source 1's history.
        commit(
            &mut control,
            NodeId(0),
            original.index,
            if confirmed { &[NodeId(2)] } else { &[] },
        );
        assert_pending(&control[&NodeId(0)], &initial, &begin);
        let source = ControlState::recover(&trusted, &control[&NodeId(1)]).unwrap();
        assert!(source.pending().is_none());
        assert_eq!(source.committed_tail(), LogIndex(1));

        // An actual managed control fence receiver must reject node 1 here:
        // it has not durably recovered this exact committed intent. Raw VSR is
        // intentionally control-independent and accepts the old-primary fence.
        let publication = publish_from(
            &root,
            &mut control,
            &begin.groups[0].proposal,
            &[NodeId(1), NodeId(2)],
        );
        for node in [NodeId(1), NodeId(2)] {
            assert_eq!(
                control[&node].membership_lineage().unwrap().plan,
                Some(publication.quorum.plan_id())
            );
        }

        if confirmed {
            for node in [NodeId(1), NodeId(2)] {
                assert_eq!(control[&node].committed_tail(), original.index);
                assert_eq!(control[&node].prepared(original.index), Some(&original));
                assert_pending(&control[&node], &initial, &begin);
            }
            assert!(
                control
                    .get_mut(&NodeId(1))
                    .unwrap()
                    .reprepare(NodeId(1), original.index)
                    .is_err(),
                "the intersecting old commit cannot become an accepted tail"
            );
        } else {
            for node in [NodeId(1), NodeId(2)] {
                assert_eq!(control[&node].committed_tail(), LogIndex(1));
                assert_eq!(control[&node].next_index(), LogIndex(3));
            }
            let reprepared = control
                .get_mut(&NodeId(1))
                .unwrap()
                .reprepare(NodeId(1), original.index)
                .unwrap();
            assert_eq!(reprepared.operation, original.operation);
            assert_eq!(reprepared.index, original.index);
            assert_eq!(reprepared.view, Epoch(2));
            acknowledge(&mut control, &reprepared);
            commit(&mut control, NodeId(1), original.index, &[NodeId(2)]);
        }

        reopen(&root, initial.control_pg, &mut control);
        assert_pending(&control[&NodeId(0)], &initial, &begin);
        for node in [NodeId(1), NodeId(2)] {
            if confirmed {
                assert_pending(&control[&node], &initial, &begin);
            } else {
                let state = ControlState::recover(&trusted, &control[&node]).unwrap();
                let rejected = ControlOutcome::Rejected(Rejection::WrongAuthority);
                assert_eq!(state.last_outcome(), &rejected);
                assert_eq!(state.request_outcome(begin.request), Some(&rejected));
                assert!(state.pending().is_none());
                assert_eq!(state.root(), initial.root().unwrap());
                assert!(state.admission(&control[&node]).is_err());
            }
        }
        drop(control);
        drop(data);
        std::fs::remove_dir_all(root).unwrap();
    }
}
