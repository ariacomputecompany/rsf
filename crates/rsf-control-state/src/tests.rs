use super::*;
use crate::replay::{ReplayState, access};
use rsf_core::{ClusterId, Epoch, LogIndex, NodeId, OperationId, PlacementGroupId};
use rsf_vsr::{
    DurableVsrCell, VsrDecision, VsrMembershipLineage, VsrMembershipProposal,
    VsrMembershipPublishedQuorum, VsrMembershipPublishedReport, VsrOperation, VsrViewChangeRequest,
};

#[path = "recovery_tests.rs"]
mod recovery;

fn node(id: u64, role: AdmissionRole) -> CatalogNode {
    CatalogNode {
        id: NodeId(id),
        incarnation: 1,
        public_key: format!("node-public-key-{id}"),
        nonce: format!("nonce-{id}-000"),
        trust_domain: "rsf.test".into(),
        spiffe_id: format!("spiffe://rsf.test/rsf/node/node-public-key-{id}"),
        certificate: [(id + 1) as u8; 32],
        role,
    }
}
fn catalog(groups: usize, count: usize) -> ControlCatalog {
    let nodes = (0..count)
        .map(|id| {
            node(
                id as u64,
                if id + 1 == count {
                    AdmissionRole::Learner
                } else {
                    AdmissionRole::Voter
                },
            )
        })
        .collect::<Vec<_>>();
    let voters = model::voter_nodes(&nodes);
    ControlCatalog {
        cluster: ClusterId::new("control-test").unwrap(),
        control_pg: PlacementGroupId(0),
        routing: CatalogRouting {
            generation: RoutingGeneration(1),
            placement: [3; 32],
            policy: [4; 32],
        },
        nodes,
        groups: (0..groups)
            .map(|pg| CatalogGroup {
                pg: PlacementGroupId(pg as u64),
                lineage: MembershipLineage {
                    bootstrap: [(pg + 10) as u8; 32],
                    plan: None,
                },
                voters: voters.clone(),
            })
            .collect(),
    }
}
fn anchor(catalog: &ControlCatalog) -> ControlBootstrap {
    ControlBootstrap {
        cluster: catalog.cluster.clone(),
        control_pg: catalog.control_pg,
        control_bootstrap: catalog.group(catalog.control_pg).unwrap().lineage.bootstrap,
        genesis_catalog: catalog.root().unwrap(),
    }
}
fn authority(catalog: &ControlCatalog) -> VsrMembershipLineage {
    let group = catalog.group(catalog.control_pg).unwrap();
    VsrMembershipLineage {
        pg: group.pg,
        bootstrap: group.lineage.bootstrap,
        plan: group.lineage.plan,
        voters: group.voters.clone(),
    }
}
fn start(catalog: &ControlCatalog) -> ReplayState {
    let mut state = ReplayState::new(anchor(catalog));
    state
        .apply(
            LogIndex(1),
            &authority(catalog),
            ControlCommand::Genesis(catalog.clone()),
        )
        .unwrap();
    state
}
fn intent(
    catalog: &ControlCatalog,
    revision: u64,
    request: u8,
    view: u64,
    primary: u64,
    voters: &[u64],
) -> MembershipIntent {
    let mut nodes = catalog.nodes.clone();
    for node in &mut nodes {
        node.role = if voters.contains(&node.id.0) {
            AdmissionRole::Voter
        } else {
            AdmissionRole::Learner
        };
    }
    let candidate = *voters
        .iter()
        .find(|node| catalog.groups[0].voters.contains(&NodeId(**node)))
        .unwrap();
    MembershipIntent {
        request: ControlRequestId([request; 32]),
        parent: catalog.root().unwrap(),
        revision: ControlRevision(revision),
        routing: catalog.routing,
        nodes,
        groups: catalog
            .groups
            .iter()
            .map(|group| GroupIntent {
                pg: group.pg,
                previous: group.lineage,
                proposal: VsrMembershipProposal {
                    election: VsrViewChangeRequest {
                        pg: group.pg,
                        previous_view: Epoch(view),
                        target_view: Epoch(view + 1),
                        candidate: NodeId(candidate),
                        voters: group.voters.clone(),
                    },
                    old_primary: NodeId(primary),
                    new_voters: voters.iter().copied().map(NodeId).collect(),
                },
            })
            .collect(),
    }
}
fn finalize(intent: &MembershipIntent, seed: u8, alternate: bool) -> MembershipFinalization {
    MembershipFinalization {
        intent: intent.id().unwrap(),
        parent: intent.parent,
        proofs: intent
            .groups
            .iter()
            .map(|group| {
                let voters = &group.proposal.new_voters;
                let count = voters.len() / 2 + 1;
                let reporters = if alternate {
                    &voters[voters.len() - count..]
                } else {
                    &voters[..count]
                };
                GroupPublication {
                    pg: group.pg,
                    quorum: VsrMembershipPublishedQuorum::from_reports(
                        reporters
                            .iter()
                            .map(|replica| VsrMembershipPublishedReport {
                                proposal: group.proposal.clone(),
                                replica: *replica,
                                input: [seed + group.pg.0 as u8; 32],
                                plan: [seed + 10 + group.pg.0 as u8; 32],
                            })
                            .collect(),
                    )
                    .unwrap(),
                }
            })
            .collect(),
    }
}
fn next_authority(
    old: &ControlCatalog,
    finalization: &MembershipFinalization,
) -> VsrMembershipLineage {
    let proof = finalization
        .proofs
        .iter()
        .find(|proof| proof.pg == old.control_pg)
        .unwrap();
    VsrMembershipLineage {
        pg: old.control_pg,
        bootstrap: authority(old).bootstrap,
        plan: Some(proof.quorum.plan_id()),
        voters: proof.quorum.proposal().new_voters.clone(),
    }
}

#[test]
fn canonical_commands_reject_every_truncated_prefix_trailing_data_and_unknown_variants() {
    let catalog = catalog(3, 4);
    let intent = intent(&catalog, 1, 20, 1, 0, &[1, 2, 3]);
    for command in [
        ControlCommand::Genesis(catalog),
        ControlCommand::Begin(intent.clone()),
        ControlCommand::Finalize(finalize(&intent, 40, false)),
    ] {
        let bytes = command.encode().unwrap();
        assert_eq!(ControlCommand::decode(&bytes).unwrap(), command);
        for length in 0..bytes.len() {
            assert_eq!(
                ControlCommand::decode(&bytes[..length]).unwrap_err().code,
                ErrorCode::CorruptionDetected
            );
        }
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert_eq!(
            ControlCommand::decode(&trailing).unwrap_err().code,
            ErrorCode::CorruptionDetected
        );
        let mut unknown = bytes;
        unknown[8] = 255;
        assert!(ControlCommand::decode(&unknown).is_err());
    }
}

#[test]
fn malformed_counts_duplicates_and_oversize_plans_cannot_acquire_an_intent() {
    let initial = catalog(3, 4);
    let mut value = intent(&initial, 1, 20, 1, 0, &[1, 2, 3]);
    let mut bytes = ControlCommand::Begin(value.clone()).encode().unwrap();
    // Header/tag, request/parent, revision and routing precede node count.
    let count = 8 + 1 + 32 + 32 + 8 + 8 + 32 + 32;
    bytes[count..count + 4].copy_from_slice(&u32::MAX.to_le_bytes());
    assert_eq!(
        ControlCommand::decode(&bytes).unwrap_err().code,
        ErrorCode::CorruptionDetected
    );
    value.nodes[1] = value.nodes[0].clone();
    assert!(ControlCommand::Begin(value).encode().is_err());

    let large = catalog(MAX_GROUPS, MAX_NODES);
    let desired = (1..MAX_NODES as u64).collect::<Vec<_>>();
    let oversized = intent(&large, 1, 21, 1, 0, &desired);
    assert!(ControlCommand::Begin(oversized.clone()).encode().is_ok());
    assert!(
        oversized.preflight().is_err(),
        "all-reporter finalization must fit before fencing"
    );
    let mut state = start(&large);
    assert_eq!(
        state
            .apply(
                LogIndex(2),
                &authority(&large),
                ControlCommand::Begin(oversized)
            )
            .unwrap(),
        ControlOutcome::Rejected(Rejection::Capacity)
    );
    assert_eq!(access::root(&state), large.root().unwrap());
}

#[test]
fn valid_stale_commands_are_rejected_but_malformed_committed_authority_is_corruption() {
    let initial = catalog(3, 4);
    let mut state = start(&initial);
    let mut stale = intent(&initial, 1, 20, 1, 0, &[1, 2, 3]);
    stale.parent = CatalogRoot([99; 32]);
    assert_eq!(
        state
            .apply(
                LogIndex(2),
                &authority(&initial),
                ControlCommand::Begin(stale)
            )
            .unwrap(),
        ControlOutcome::Rejected(Rejection::StaleParent)
    );
    assert_eq!(access::root(&state), initial.root().unwrap());
    let mut wrong_authority = authority(&initial);
    wrong_authority.bootstrap = [99; 32];
    assert_eq!(
        state
            .apply(
                LogIndex(3),
                &wrong_authority,
                ControlCommand::Genesis(initial.clone())
            )
            .unwrap_err()
            .code,
        ErrorCode::CorruptionDetected
    );
    assert_eq!(
        state
            .apply(
                LogIndex(3),
                &authority(&initial),
                ControlCommand::Genesis(initial.clone())
            )
            .unwrap(),
        ControlOutcome::Rejected(Rejection::AlreadyInitialized)
    );
    let mut empty = ReplayState::new(anchor(&initial));
    let mut wrong_genesis = initial.clone();
    wrong_genesis.routing.generation = RoutingGeneration(2);
    assert_eq!(
        empty
            .apply(
                LogIndex(1),
                &authority(&initial),
                ControlCommand::Genesis(wrong_genesis)
            )
            .unwrap_err()
            .code,
        ErrorCode::CorruptionDetected
    );
}

#[test]
fn all_pg_finalization_is_atomic_and_requires_new_control_authority() {
    let initial = catalog(3, 4);
    let mut state = start(&initial);
    let begin = intent(&initial, 1, 20, 1, 0, &[1, 2, 3]);
    state
        .apply(
            LogIndex(2),
            &authority(&initial),
            ControlCommand::Begin(begin.clone()),
        )
        .unwrap();
    let finalization = finalize(&begin, 40, false);
    let next = next_authority(&initial, &finalization);
    let mut partial = finalization.clone();
    partial.proofs.pop();
    assert_eq!(
        state
            .apply(LogIndex(3), &next, ControlCommand::Finalize(partial))
            .unwrap(),
        ControlOutcome::Rejected(Rejection::IncompleteGroups)
    );
    assert_eq!(access::root(&state), initial.root().unwrap());
    assert_eq!(
        state
            .apply(
                LogIndex(4),
                &authority(&initial),
                ControlCommand::Finalize(finalization.clone())
            )
            .unwrap(),
        ControlOutcome::Rejected(Rejection::WrongAuthority)
    );
    assert_eq!(access::revision(&state), ControlRevision(1));
    let result = state
        .apply(
            LogIndex(5),
            &next,
            ControlCommand::Finalize(finalization.clone()),
        )
        .unwrap();
    assert!(matches!(
        result,
        ControlOutcome::CatalogApplied {
            revision: ControlRevision(2),
            ..
        }
    ));
    for (group, proof) in access::catalog(&state)
        .groups
        .iter()
        .zip(&finalization.proofs)
    {
        assert_eq!(group.lineage.plan, Some(proof.quorum.plan_id()));
        assert_eq!(group.voters, vec![NodeId(1), NodeId(2), NodeId(3)]);
    }
    let root = access::root(&state);
    let retry = finalize(&begin, 40, true);
    assert_eq!(
        state
            .apply(LogIndex(6), &next, ControlCommand::Finalize(retry))
            .unwrap(),
        result
    );
    assert_eq!(access::root(&state), root);
    assert_eq!(
        state
            .apply(
                LogIndex(7),
                &next,
                ControlCommand::Finalize(finalize(&begin, 41, false))
            )
            .unwrap(),
        ControlOutcome::Rejected(Rejection::PublicationConflict)
    );
}

#[test]
fn request_id_reuse_and_identity_rebinding_cannot_mutate_an_existing_plan() {
    let initial = catalog(3, 4);
    let mut state = start(&initial);
    let begin = intent(&initial, 1, 20, 1, 0, &[1, 2, 3]);
    let result = state
        .apply(
            LogIndex(2),
            &authority(&initial),
            ControlCommand::Begin(begin.clone()),
        )
        .unwrap();
    assert_eq!(
        state
            .apply(
                LogIndex(3),
                &authority(&initial),
                ControlCommand::Begin(begin.clone())
            )
            .unwrap(),
        result
    );
    let mut conflict = begin.clone();
    conflict.nodes[3].nonce.push('x');
    assert_eq!(
        state
            .apply(
                LogIndex(4),
                &authority(&initial),
                ControlCommand::Begin(conflict.clone())
            )
            .unwrap(),
        ControlOutcome::Rejected(Rejection::RequestConflict)
    );
    let mut clean = start(&initial);
    conflict.request = ControlRequestId([21; 32]);
    assert_eq!(
        clean
            .apply(
                LogIndex(2),
                &authority(&initial),
                ControlCommand::Begin(conflict)
            )
            .unwrap(),
        ControlOutcome::Rejected(Rejection::IdentityChanged)
    );
}

#[test]
fn a_b_a_membership_keeps_distinct_admission_roots() {
    let initial = catalog(3, 4);
    let mut state = start(&initial);
    let first = intent(&initial, 1, 20, 1, 0, &[1, 2, 3]);
    state
        .apply(
            LogIndex(2),
            &authority(&initial),
            ControlCommand::Begin(first.clone()),
        )
        .unwrap();
    let published = finalize(&first, 40, false);
    state
        .apply(
            LogIndex(3),
            &next_authority(&initial, &published),
            ControlCommand::Finalize(published),
        )
        .unwrap();
    let middle = access::catalog(&state).clone();
    let second = intent(&middle, 2, 21, 7, 2, &[0, 1, 2]);
    state
        .apply(
            LogIndex(4),
            &authority(&middle),
            ControlCommand::Begin(second.clone()),
        )
        .unwrap();
    let published = finalize(&second, 70, false);
    state
        .apply(
            LogIndex(5),
            &next_authority(&middle, &published),
            ControlCommand::Finalize(published),
        )
        .unwrap();
    let current = access::catalog(&state);
    assert_eq!(current.groups[0].voters, initial.groups[0].voters);
    assert_ne!(current.groups[0].lineage, initial.groups[0].lineage);
    assert_ne!(current.root().unwrap(), initial.root().unwrap());
    assert!(!initial.groups[0].matches(&authority(current)));
    assert_eq!(access::revision(&state), ControlRevision(3));
}

fn append(cell: &mut DurableVsrCell, command: ControlCommand, commit: bool) {
    let payload = command.encode().unwrap();
    let bytes = payload.len() as u32;
    let prepared = cell
        .prepare(
            NodeId(0),
            VsrOperation::ReplicatedCommand {
                operation_id: OperationId(1),
                payload,
            },
            bytes,
        )
        .unwrap();
    let VsrDecision::Prepared(prepare) = prepared else {
        panic!("prepare rejected")
    };
    if commit {
        assert!(matches!(
            cell.commit(NodeId(0), prepare.index).unwrap(),
            VsrDecision::Committed(_)
        ));
    }
}

#[test]
fn public_projection_comes_from_committed_live_history_and_survives_cold_replay() {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let sequence = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let root = std::env::temp_dir().join(format!(
        "rsf-control-state-{}-{sequence}",
        std::process::id()
    ));
    std::fs::create_dir(&root).unwrap();
    let mut control = DurableVsrCell::open(
        root.join("control"),
        PlacementGroupId(0),
        Epoch(1),
        NodeId(0),
        [NodeId(0)],
    )
    .unwrap();
    let mut initial = catalog(3, 2);
    initial.groups[0].lineage.bootstrap = control.membership_lineage().unwrap().bootstrap;
    let mut data = std::collections::BTreeMap::new();
    for group in initial.groups.iter_mut().skip(1) {
        let cell = DurableVsrCell::open(
            root.join(format!("data-{}", group.pg.0)),
            group.pg,
            Epoch(1),
            NodeId(0),
            [NodeId(0)],
        )
        .unwrap();
        group.lineage.bootstrap = cell.membership_lineage().unwrap().bootstrap;
        data.insert(group.pg, cell);
    }
    let trusted = anchor(&initial);
    assert!(
        ControlState::recover(&trusted, &control).is_err(),
        "no committed genesis"
    );
    append(&mut control, ControlCommand::Genesis(initial.clone()), true);
    let state = ControlState::recover(&trusted, &control).unwrap();
    let before = state.admission(&control).unwrap();
    assert_eq!(before.root(), trusted.genesis_catalog);
    assert_eq!(
        before
            .validate_authority(&control, &data, NodeId(0), &[1; 32], initial.routing)
            .unwrap(),
        NodeId(0)
    );
    assert!(
        before
            .validate_authority(&control, &data, NodeId(1), &[2; 32], initial.routing)
            .is_err(),
        "learner cannot serve"
    );
    assert!(
        before
            .validate_authority(&control, &data, NodeId(0), &[99; 32], initial.routing)
            .is_err()
    );
    let mut wrong_routing = initial.routing;
    wrong_routing.generation = RoutingGeneration(2);
    assert!(
        before
            .validate_authority(&control, &data, NodeId(0), &[1; 32], wrong_routing)
            .is_err()
    );
    let missing = data.remove(&PlacementGroupId(2)).unwrap();
    assert!(
        before
            .validate_authority(&control, &data, NodeId(0), &[1; 32], initial.routing)
            .is_err()
    );
    data.insert(PlacementGroupId(2), missing);
    let mut wrong_anchor = trusted.clone();
    wrong_anchor.control_bootstrap = [99; 32];
    assert!(ControlState::recover(&wrong_anchor, &control).is_err());
    assert!(matches!(
        control.view_change(Epoch(2), NodeId(0)),
        VsrDecision::ViewChanged { .. }
    ));
    assert!(state.admission(&control).is_err());
    let state = ControlState::recover(&trusted, &control).unwrap();
    assert_eq!(state.root(), before.root());
    assert_eq!(state.revision(), before.revision());
    assert!(
        state
            .admission(&control)
            .unwrap()
            .validate_authority(&control, &data, NodeId(0), &[1; 32], initial.routing)
            .is_ok()
    );
    // A valid accepted intent is not a committed catalog or pending control plan.
    let next = intent(&initial, 1, 20, 2, 0, &[0, 1]);
    assert!(
        state
            .preflight_command(&control, &ControlCommand::Begin(next.clone()))
            .is_ok()
    );
    append(&mut control, ControlCommand::Begin(next), false);
    assert!(
        state.admission(&control).is_err(),
        "old projection stamp is invalidated"
    );
    let accepted = ControlState::recover(&trusted, &control).unwrap();
    assert!(accepted.pending().is_none());
    assert_eq!(accepted.revision(), ControlRevision(1));
    assert!(
        accepted
            .preflight_command(&control, &ControlCommand::Genesis(initial))
            .is_err(),
        "accepted tail must reconcile"
    );
    drop(control);
    let recovered =
        DurableVsrCell::open_existing(root.join("control"), PlacementGroupId(0)).unwrap();
    let cold = ControlState::recover(&trusted, &recovered).unwrap();
    assert_eq!(cold.root(), trusted.genesis_catalog);
    assert!(cold.pending().is_none());
    assert!(cold.admission(&recovered).is_ok());
    drop(recovered);
    drop(data);
    std::fs::remove_dir_all(root).unwrap();
}
