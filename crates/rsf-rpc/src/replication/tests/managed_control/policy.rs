use super::*;

#[test]
fn configured_catalog_rejects_first_fence_authority_and_stage_before_effects() {
    let mut f = Fixture::new("control-domain-first-fence");
    let mut approvals = Vec::new();
    for (pg, primary, cells) in [(CONTROL, 0, &mut f.control), (DATA, 2, &mut f.data)] {
        let proposal = proposal(pg, primary);
        let snapshots = [NodeId(0), NodeId(2)]
            .into_iter()
            .map(|node| {
                cells
                    .get_mut(&node)
                    .unwrap()
                    .begin_membership(node, NodeId(primary), proposal.clone())
                    .unwrap()
            })
            .collect::<Vec<_>>();
        approvals.push(VsrMembershipApproval::from_snapshots(snapshots).unwrap());
    }
    let mut server = f.server.take().unwrap();
    let tokens = [CONTROL, DATA].map(|pg| (pg, server.cells[&pg].recovery_state_token().unwrap()));
    for approval in &approvals {
        assert!(
            server
                .initialize_membership_decision(approval)
                .unwrap_err()
                .contains("committed intent")
        );
    }
    let running = Running::start(server);
    let sender = f.transport(0, running.address);
    for approval in &approvals {
        let pg = approval.proposal().election.pg;
        assert!(
            f.transport(approval.proposal().old_primary.0, running.address)
                .exchange_io(
                    NodeId(1),
                    MEMBERSHIP_BEGIN,
                    &approval.proposal().encode().unwrap()
                )
                .unwrap_err()
                .to_string()
                .contains("committed intent")
        );
        let input = VsrMembershipInput::from_approval(approval).unwrap();
        assert!(
            sender
                .exchange_io(NodeId(1), MEMBERSHIP_AUTH_BEGIN, &input.encode().unwrap())
                .unwrap_err()
                .to_string()
                .contains("committed intent")
        );
        // Genuine accepted/chosen evidence, never a fabricated report DTO.
        let mut stores = [0, 3].map(|node| {
            MembershipDecisionStore::open(
                f.root.join(format!("decisions-{}-{node}", pg.0)),
                NodeId(node),
                approval,
            )
            .unwrap()
        });
        let round = VsrMembershipRound {
            counter: 1,
            coordinator: NodeId(0),
        };
        let promises = stores
            .iter_mut()
            .map(|store| store.promise(NodeId(0), round).unwrap())
            .collect::<Vec<_>>();
        let decision = VsrMembershipDecision::from_promises(promises, input).unwrap();
        let nomination = stores[0].nominate(&decision).unwrap();
        let reports = stores
            .iter_mut()
            .map(|store| store.accept(NodeId(0), &decision, &nomination).unwrap())
            .collect::<Vec<_>>();
        let chosen = VsrMembershipChosen::from_accepted(reports).unwrap();
        let error = sender
            .exchange_io(NodeId(1), MEMBERSHIP_STAGE, &chosen.encode().unwrap())
            .unwrap_err()
            .to_string();
        assert!(
            error.contains(if pg == CONTROL {
                "sealed control replay"
            } else {
                "committed intent"
            }),
            "{error}"
        );
        let mut other = DurableVsrCell::open(
            f.root.join(format!("stage-old-{}", pg.0)),
            pg,
            Epoch(1),
            approval.proposal().old_primary,
            (0..3).map(NodeId),
        )
        .unwrap();
        other
            .begin_membership(
                NodeId(1),
                approval.proposal().old_primary,
                approval.proposal().clone(),
            )
            .unwrap();
        let old = if pg == CONTROL {
            &f.control[&NodeId(0)]
        } else {
            &f.data[&NodeId(0)]
        };
        let mut reports = Vec::new();
        for (node, anchor) in [(NodeId(0), old), (NodeId(1), &other)] {
            let approval_copy =
                VsrMembershipApproval::from_snapshots(approval.snapshots().to_vec()).unwrap();
            let plan =
                rsf_vsr::VsrMembershipStagedPlan::select(chosen.clone(), approval_copy).unwrap();
            assert_eq!(plan.committed_tail(), LogIndex(0));
            let mut stage = rsf_vsr::MembershipStagingStore::stage(
                f.root.join(format!("empty-stage-{}-{}", pg.0, node.0)),
                node,
                plan,
                anchor,
            )
            .unwrap();
            // These real VSR fixture stages contain no committed application
            // entries. They cannot supply the missing typed control projection.
            reports.push(
                stage
                    .materialize(&mut |_| panic!("empty fixture stage has a command"))
                    .unwrap(),
            );
        }
        let quorum = rsf_vsr::VsrMembershipStagedQuorum::from_reports(reports).unwrap();
        rejected(
            sender.exchange_io(NodeId(1), MEMBERSHIP_PUBLISH, &quorum.encode().unwrap()),
            if pg == CONTROL {
                "sealed control replay"
            } else {
                "committed intent"
            },
        );
    }
    assert!(running.applied.lock().unwrap().is_empty());
    let server = running.finish();
    for (pg, token) in tokens {
        assert_eq!(server.cells[&pg].recovery_state_token().unwrap(), token);
        assert!(server.cells[&pg].membership_fence().is_none());
    }
    assert!(server.membership_decisions.is_empty());
    assert!(server.membership_staging.is_empty());
    for path in [
        "transition-control/decision",
        "transition-data/decision",
        "transition-control/staging",
        "transition-data/staging",
    ] {
        assert!(!f.root.join(path).exists());
    }
    drop(server);
    f.cleanup();
}

#[test]
fn existing_decision_is_observable_but_only_exact_enrolled_recovery_can_mutate() {
    for enrolled in [false, true] {
        let mut f = Fixture::unbound("control-domain-existing-decision");
        let proposal = proposal(DATA, 2);
        let snapshots = [NodeId(0), NodeId(2)]
            .into_iter()
            .map(|node| {
                f.data
                    .get_mut(&node)
                    .unwrap()
                    .begin_membership(node, NodeId(2), proposal.clone())
                    .unwrap()
            })
            .collect();
        let approval = VsrMembershipApproval::from_snapshots(snapshots).unwrap();
        let mut server = f.server.take().unwrap();
        server.initialize_membership_decision(&approval).unwrap();
        if enrolled {
            server
                .cells
                .get_mut(&DATA)
                .unwrap()
                .begin_membership(NodeId(1), NodeId(2), proposal.clone())
                .unwrap();
            server = server
                .with_membership_recovery(
                    [(
                        DATA,
                        PeerMembershipRecoveryTiming {
                            interval: Duration::from_millis(20),
                        },
                    )]
                    .into_iter()
                    .collect(),
                )
                .unwrap();
        }
        f.server = Some(server);
        f.bind();
        let mut server = f.server.take().unwrap();
        let before = server.membership_decisions[&DATA].decision_state().unwrap();
        if !enrolled {
            assert!(
                server
                    .with_membership_decision(DATA, |_, _| ())
                    .unwrap_err()
                    .contains("committed intent")
            );
        }
        let running = Running::start(server);
        let sender = f.transport(0, running.address);
        assert_eq!(
            sender
                .membership_decision_state(NodeId(1), &proposal, before.bootstrap)
                .unwrap(),
            before
        );
        let round = VsrMembershipRound {
            counter: 1,
            coordinator: NodeId(0),
        };
        let promised =
            sender.promise_membership_recovery(NodeId(1), &proposal, before.bootstrap, round);
        if enrolled {
            assert!(matches!(
                promised.unwrap(),
                rsf_vsr::VsrMembershipPromiseReply::Granted(_)
            ));
        } else {
            assert!(promised.unwrap_err().reason.contains("committed intent"));
        }
        let mut other = proposal.clone();
        other.election.target_view = Epoch(3);
        rejected(
            f.transport(2, running.address).exchange_io(
                NodeId(1),
                MEMBERSHIP_BEGIN,
                &other.encode().unwrap(),
            ),
            if enrolled {
                "cannot initiate another proposal"
            } else {
                "committed intent"
            },
        );
        let server = running.finish();
        let after = server.membership_decisions[&DATA].decision_state().unwrap();
        if enrolled {
            assert_eq!(after.promised, Some(round));
            assert_eq!(server.cells[&DATA].membership_fence(), Some(&proposal));
            assert!(server.membership_admission_required().unwrap());
        } else {
            assert_eq!(after, before);
            assert!(server.cells[&DATA].membership_fence().is_none());
        }
        drop(server);
        f.cleanup();
    }
}

#[test]
fn selected_foreign_authority_upload_cannot_mutate_under_another_enrolled_journal() {
    let mut f = Fixture::unbound("control-domain-foreign-inbox");
    let proposal = proposal(DATA, 2);
    let snapshots = [NodeId(0), NodeId(2)]
        .into_iter()
        .map(|node| {
            f.data
                .get_mut(&node)
                .unwrap()
                .begin_membership(node, NodeId(2), proposal.clone())
                .unwrap()
        })
        .collect();
    let approval = VsrMembershipApproval::from_snapshots(snapshots).unwrap();
    let mut other = proposal.clone();
    other.election.target_view = Epoch(3);
    let mut foreign = [0, 2].map(|node| {
        DurableVsrCell::open(
            f.root.join(format!("foreign-{node}")),
            DATA,
            Epoch(1),
            NodeId(2),
            (0..3).map(NodeId),
        )
        .unwrap()
    });
    let foreign_snapshots = foreign
        .iter_mut()
        .zip([0, 2])
        .map(|(cell, node)| {
            cell.begin_membership(NodeId(node), NodeId(2), other.clone())
                .unwrap()
        })
        .collect();
    let foreign_approval = VsrMembershipApproval::from_snapshots(foreign_snapshots).unwrap();
    let input = VsrMembershipInput::from_approval(&foreign_approval).unwrap();
    let id = input.id().unwrap();
    let mut server = f.server.take().unwrap();
    assert_eq!(
        server
            .membership_inputs
            .get_mut(&DATA)
            .unwrap()
            .begin(NodeId(0), input)
            .unwrap(),
        0
    );
    server.initialize_membership_decision(&approval).unwrap();
    server
        .cells
        .get_mut(&DATA)
        .unwrap()
        .begin_membership(NodeId(1), NodeId(2), proposal.clone())
        .unwrap();
    server = server
        .with_membership_recovery(
            [(
                DATA,
                PeerMembershipRecoveryTiming {
                    interval: Duration::from_millis(20),
                },
            )]
            .into_iter()
            .collect(),
        )
        .unwrap();
    f.server = Some(server);
    f.bind();
    let server = f.server.take().unwrap();
    let before_state = server.membership_decisions[&DATA].decision_state().unwrap();
    let before = tree(&f.root.join("transition-data"));
    let running = Running::start(server);
    let sender = f.transport(0, running.address);
    let records = foreign_approval
        .snapshots()
        .iter()
        .flat_map(|snapshot| snapshot.records.clone())
        .collect::<Vec<_>>();
    let batch = rsf_vsr::VsrHistoryBatch::pack(0, &records).unwrap();
    let mut prefix = DATA.0.to_le_bytes().to_vec();
    prefix.extend_from_slice(&id);
    let mut request = prefix.clone();
    request.extend_from_slice(&batch.encode().unwrap());
    rejected(
        sender.exchange_io(NodeId(1), MEMBERSHIP_AUTH_BATCH, &request),
        "cannot change its retained proposal",
    );
    rejected(
        sender.exchange_io(NodeId(1), MEMBERSHIP_AUTH_FINISH, &prefix),
        "cannot change its retained proposal",
    );
    assert_eq!(tree(&f.root.join("transition-data")), before);
    assert!(running.applied.lock().unwrap().is_empty());
    let server = running.finish();
    assert_eq!(
        server.membership_decisions[&DATA].decision_state().unwrap(),
        before_state
    );
    assert_eq!(server.cells[&DATA].membership_fence(), Some(&proposal));
    assert_eq!(
        server.membership_inputs[&DATA]
            .selected_input()
            .unwrap()
            .proposal(),
        &other
    );
    drop(server);
    drop(foreign);
    f.cleanup();
}
