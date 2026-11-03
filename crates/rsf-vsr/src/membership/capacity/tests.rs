use super::*;
use crate::membership::tests::{fixture, proposal, root};

#[test]
fn membership_capacity_rejects_oversized_transition_before_any_durable_fence() {
    let path = root("capacity-no-fence");
    let mut cells = fixture(&path);
    let mut oversized = proposal();
    oversized.new_voters = (0..512).map(NodeId).collect();
    // The individual proposal is valid and remains readable for historical
    // recovery; the combined publication proof is what cannot be admitted.
    let encoded = oversized.encode().unwrap();
    assert_eq!(VsrMembershipProposal::decode(&encoded).unwrap(), oversized);
    let before = cells[0].history_snapshot().unwrap();
    for _ in 0..2 {
        let error = cells[0]
            .begin_membership(NodeId(0), NodeId(0), oversized.clone())
            .unwrap_err();
        assert!(
            error.message.contains("publication record limit"),
            "{}",
            error.message
        );
        assert!(cells[0].membership_fence().is_none());
        assert!(cells[0].view_change_promise().is_none());
        assert_eq!(cells[0].history_snapshot().unwrap().records, before.records);
    }
    drop(cells);
    let mut cell = DurableVsrCell::open_existing(path.join("0"), PlacementGroupId(1)).unwrap();
    assert!(cell.membership_fence().is_none());
    assert!(cell.view_change_promise().is_none());
    assert_eq!(cell.committed_tail(), LogIndex(1));
    assert_eq!(
        cell.request_commit(&VsrClientRequestId::new("original", 1)),
        Some(LogIndex(1))
    );
    assert_eq!(cell.history_snapshot().unwrap().records, before.records);
    // Rejection must leave normal old-view operation available.
    assert!(matches!(
        cell.prepare(
            NodeId(0),
            VsrOperation::ProtocolOperation {
                operation_id: OperationId(3),
            },
            1
        )
        .unwrap(),
        VsrDecision::Prepared(_)
    ));
    drop(cell);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn membership_capacity_matches_complete_encoded_input_and_recovery_decision() {
    let path = root("capacity-codecs");
    let mut cells = fixture(&path);
    let p = proposal();
    p.validate_publication_capacity().unwrap();
    let approval = VsrMembershipApproval::from_snapshots(
        cells
            .iter_mut()
            .enumerate()
            .map(|(n, c)| {
                c.begin_membership(NodeId(n as u64), NodeId(0), p.clone())
                    .unwrap()
            })
            .collect(),
    )
    .unwrap();
    let input = VsrMembershipInput::from_approval(&approval).unwrap();
    let round = VsrMembershipRound {
        counter: u64::MAX,
        coordinator: NodeId(1),
    };
    let promises = p
        .new_voters
        .iter()
        .map(|replica| VsrMembershipPromise {
            proposal: p.clone(),
            replica: *replica,
            round,
            accepted: Some((round, input.id().unwrap())),
        })
        .collect();
    let decision = VsrMembershipDecision::from_promises(promises, input.clone()).unwrap();
    let accepted = p
        .new_voters
        .iter()
        .map(|replica| VsrMembershipAccepted {
            proposal: p.clone(),
            replica: *replica,
            round,
            input: input.id().unwrap(),
        })
        .collect();
    let chosen = VsrMembershipChosen::from_accepted(accepted).unwrap();
    let reports = p
        .new_voters
        .iter()
        .map(|replica| VsrMembershipStagedReport {
            proposal: p.clone(),
            replica: *replica,
            input: input.id().unwrap(),
            plan: [1; 32],
            stage: [2; 32],
            committed_tail: LogIndex(1),
            accepted_tail: LogIndex(2),
        })
        .collect();
    let quorum = VsrMembershipStagedQuorum::from_reports(reports).unwrap();
    let bounds = proof_bounds(&p).unwrap();
    assert_eq!(bounds.input, input.encode().unwrap().len());
    assert_eq!(bounds.decision, decision.encode().unwrap().len());
    assert_eq!(bounds.chosen, chosen.encode().unwrap().len());
    assert_eq!(
        bounds.chosen_input,
        VsrMembershipChosenInput::new(chosen.clone(), input.clone())
            .unwrap()
            .encode()
            .unwrap()
            .len()
    );
    assert_eq!(bounds.quorum, quorum.encode().unwrap().len());
    let published_reports = p
        .new_voters
        .iter()
        .map(|replica| VsrMembershipPublishedReport {
            proposal: p.clone(),
            replica: *replica,
            input: input.id().unwrap(),
            plan: [1; 32],
        })
        .collect::<Vec<_>>();
    let published = VsrMembershipPublishedQuorum::from_reports(published_reports).unwrap();
    assert_eq!(
        bounds.published_report,
        published.reports()[0].encode().unwrap().len()
    );
    assert_eq!(bounds.published_quorum, published.encode().unwrap().len());
    // Exercise the actual activation serializer through sealed staging and
    // local publication, rather than reconstructing its byte layout here.
    let mut stages = (0..3)
        .map(|n| {
            let approval =
                VsrMembershipApproval::from_snapshots(approval.snapshots().to_vec()).unwrap();
            let plan = VsrMembershipStagedPlan::select(chosen.clone(), approval).unwrap();
            MembershipStagingStore::stage(
                path.join(format!("stage-{n}")),
                NodeId(n as u64),
                plan,
                &cells[n],
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let real_quorum = VsrMembershipStagedQuorum::from_reports(
        stages
            .iter_mut()
            .map(|stage| stage.materialize(&mut |_| Ok(())).unwrap())
            .collect(),
    )
    .unwrap();
    stages[0].retain_publication(&real_quorum).unwrap();
    assert_eq!(
        bounds.metadata,
        std::fs::read(path.join("stage-0/META")).unwrap().len()
    );
    let one_more_report = 8 + real_quorum.reports()[0].encode().unwrap().len();
    assert_eq!(
        bounds.publication,
        std::fs::read(path.join("stage-0/PUBLICATION"))
            .unwrap()
            .len()
            + one_more_report
    );
    cells[0]
        .install_membership(&stages[0], &real_quorum)
        .unwrap();
    let published = cells[0].history_snapshot().unwrap();
    let activation = published.records.last().unwrap();
    assert!(activation.payload.starts_with(b"VSRMAI1"));
    // Four configured new voters, but the actual staged majority has three.
    let one_more_report = 8 + real_quorum.reports()[0].encode().unwrap().len();
    assert_eq!(
        bounds.activation,
        activation.payload.len() + one_more_report
    );
    drop(stages);
    drop(cells);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn historical_oversized_membership_fence_remains_readable_and_exact_retry_does_not_rewrite_it() {
    let path = root("capacity-historical-fence");
    let mut cells = fixture(&path);
    let mut p = proposal();
    p.new_voters = (0..512).map(NodeId).collect();
    assert!(p.validate_publication_capacity().is_err());
    // Write the valid pre-preflight on-disk format directly to represent a
    // fence acknowledged by an older binary. Do not relax new initiation.
    cells[0]
        .append_control(WalRecordKind::MembershipPromise, &p.encode().unwrap())
        .unwrap();
    drop(cells);
    let mut cell = DurableVsrCell::open_existing(path.join("0"), PlacementGroupId(1)).unwrap();
    let before = cell.membership_snapshot(NodeId(0), NodeId(0)).unwrap();
    assert_eq!(before.report.proposal, p);
    let after = cell.begin_membership(NodeId(0), NodeId(0), p).unwrap();
    assert_eq!(before.records, after.records);
    assert_eq!(cell.committed_tail(), LogIndex(1));
    assert_eq!(
        cell.request_commit(&VsrClientRequestId::new("original", 1)),
        Some(LogIndex(1))
    );
    assert!(cell.primary_probe().is_err());
    drop(cell);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn membership_capacity_checks_old_and_new_cardinalities_at_the_publication_boundary() {
    let mut p = proposal();
    let first_rejected = (4..=512)
        .find(|count| {
            p.new_voters = (0..*count).map(NodeId).collect();
            p.validate_publication_capacity().is_err()
        })
        .unwrap();
    p.new_voters = (0..first_rejected - 1).map(NodeId).collect();
    p.validate_publication_capacity().unwrap();
    p.new_voters.push(NodeId(first_rejected - 1));
    assert!(p.validate_publication_capacity().is_err());
    p.election.voters = (0..4096).map(NodeId).collect();
    p.new_voters = vec![NodeId(0), NodeId(1), NodeId(2)];
    assert!(p.validate_publication_capacity().is_err());
    assert_eq!(
        VsrMembershipProposal::decode(&p.encode().unwrap()).unwrap(),
        p
    );
    assert!(sum(&[usize::MAX, 1]).is_err());
    assert!(product(usize::MAX, 2).is_err());
}
