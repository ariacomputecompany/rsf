//! Actual authenticated receiver tests; the scheduler is deliberately unpolled.
use super::*;

const PG: PlacementGroupId = PlacementGroupId(1);

pub(super) fn publish(source: &mut DurableVsrCell, target: &mut rsf_vsr::LearnerHistoryStore) {
    let snapshot = source.history_snapshot().unwrap();
    let id = snapshot.descriptor.id().unwrap();
    let next = target.begin(snapshot.descriptor).unwrap();
    for (ordinal, record) in snapshot.records.into_iter().enumerate().skip(next as usize) {
        target.append(id, ordinal as u64, record).unwrap();
    }
    target.finish(id).unwrap();
}

fn publish_application(
    source: &mut DurableVsrCell,
    target: &mut crate::replication::learner::LearnerOwner,
) {
    let crate::replication::learner::LearnerOwner::Application(target) = target else {
        panic!("application custody fixture cannot bypass a control owner");
    };
    publish(source, target);
}

pub(super) fn managed_learner(fixture: &Accepted) -> (PeerReplicaServer, DurableVsrCell) {
    let root = &fixture.root;
    let mut source = DurableVsrCell::open(
        root.join("inbound-source"),
        PG,
        Epoch(1),
        NodeId(0),
        [NodeId(0), NodeId(1), NodeId(2)],
    )
    .unwrap();
    let mut learner =
        rsf_vsr::LearnerHistoryStore::open(root.join("inbound-learner"), PG, NodeId(3), NodeId(0))
            .unwrap();
    publish(&mut source, &mut learner);
    let mut server = PeerReplicaServer::with_learners(
        NodeId(3),
        &cluster(),
        identity(root, 3),
        all_endpoints(root),
        BTreeMap::new(),
        [(PG, learner)].into(),
        Duration::from_millis(800),
    )
    .unwrap()
    .with_membership_paths([(PG, root.join("inbound-membership"))].into())
    .unwrap()
    .with_promotion_paths([(PG, root.join("inbound-promoted"))].into())
    .unwrap();
    server
        .initialize_membership_decision(&fixture.authority)
        .unwrap();
    (
        server
            .with_membership_recovery(
                [(
                    PG,
                    crate::replication::PeerMembershipRecoveryTiming {
                        interval: Duration::from_millis(1),
                    },
                )]
                .into(),
            )
            .unwrap(),
        source,
    )
}

fn state(server: &PeerReplicaServer) -> rsf_vsr::VsrMembershipDecisionState {
    server.membership_decisions[&PG].decision_state().unwrap()
}

fn manifest(root: &Path, server: &PeerReplicaServer) -> PathBuf {
    let id = server.learners[&PG]
        .published_history_id()
        .unwrap()
        .unwrap();
    root.join("inbound-learner")
        .join(id_name(id))
        .join("cell/MANIFEST")
}

// One real TLS request through the production authentication, framing, receiver
// dispatch and response encoder. No custom dispatcher can bypass the guard.
pub(super) fn tls(
    mut server: PeerReplicaServer,
    root: &Path,
    sender: u64,
    kind: u8,
    payload: &[u8],
) -> (PeerReplicaServer, io::Result<Vec<u8>>, usize) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let mut peers = all_endpoints(root);
    peers.get_mut(&server.local).unwrap().address = listener.local_addr().unwrap();
    let receiver = server.local;
    let worker = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(3);
        let socket = loop {
            match listener.accept() {
                Ok((socket, _)) => break socket,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    assert!(
                        Instant::now() < deadline,
                        "inbound TLS request never arrived"
                    );
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(error) => panic!("inbound listener: {error}"),
            }
        };
        let mut applied = 0;
        let _ = server.serve_connection_with_apply(socket, &mut |_| {
            applied += 1;
            Ok(())
        });
        (server, applied)
    });
    let network = decision_transport(root, sender, peers);
    let result = network.exchange_once(receiver, kind, payload);
    let (server, applied) = worker.join().unwrap();
    (server, result, applied)
}

// Require a server rejection for the injected missing MANIFEST, never a TLS
// parse error or transport timeout. The first failed custody probe returns the
// storage error; subsequent calls expose the owning layer's failure latch.
#[cfg(unix)]
fn assert_lost_learner_rejection(error: &io::Error, first: bool) {
    assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{error}");
    let message = error.to_string();
    let reason = message
        .strip_prefix("peer rejected request: ")
        .unwrap_or_else(|| panic!("expected structured peer rejection: {error}"));
    if first {
        assert!(reason.starts_with("storage Recover failed: "), "{error}");
    } else {
        // The VSR latch is authoritative before the separate learner ACK fix;
        // that fix also latches the outer learner and returns its exact reason.
        assert!(
            reason.starts_with(
                "VSR durability failed; reopen before further operations: storage Recover failed: "
            ) || reason == "learner durability failed; reopen before further operations",
            "{error}"
        );
    }
}

fn round_payload(round: VsrMembershipRound) -> Vec<u8> {
    let mut bytes = PG.0.to_le_bytes().to_vec();
    bytes.extend_from_slice(&round.counter.to_le_bytes());
    bytes.extend_from_slice(&round.coordinator.0.to_le_bytes());
    bytes.extend_from_slice(&proposed().id().unwrap());
    bytes
}

fn promise_payload(fixture: &Accepted, round: VsrMembershipRound) -> Vec<u8> {
    let mut body = round.counter.to_le_bytes().to_vec();
    body.extend_from_slice(&round.coordinator.0.to_le_bytes());
    wire_request(1, &proposed(), bootstrap(fixture), &body)
}

#[cfg(unix)]
#[test]
fn real_tls_managed_promise_and_accept_reject_lost_learner_before_journal_mutation() {
    for kind in [MEMBERSHIP_RECOVERY, MEMBERSHIP_PROMISE, MEMBERSHIP_ACCEPT] {
        let fixture = accepted_fixture();
        let (mut server, _source) = managed_learner(&fixture);
        let round = VsrMembershipRound {
            counter: 7,
            coordinator: NodeId(1),
        };
        let payload = if kind == MEMBERSHIP_ACCEPT {
            let mut coordinator = MembershipDecisionStore::open(
                fixture.root.join("inbound-coordinator"),
                NodeId(1),
                &fixture.authority,
            )
            .unwrap();
            let own = coordinator.promise(NodeId(1), round).unwrap();
            let remote = server
                .membership_decisions
                .get_mut(&PG)
                .unwrap()
                .promise(NodeId(1), round)
                .unwrap();
            let input = VsrMembershipInput::from_approval(&fixture.authority).unwrap();
            let decision = VsrMembershipDecision::from_promises(vec![own, remote], input).unwrap();
            let nomination = coordinator.nominate(&decision).unwrap();
            let encoded = decision.encode().unwrap();
            let mut bytes = (encoded.len() as u64).to_le_bytes().to_vec();
            bytes.extend_from_slice(&encoded);
            bytes.extend_from_slice(&nomination.encode().unwrap());
            bytes
        } else if kind == MEMBERSHIP_PROMISE {
            round_payload(round)
        } else {
            promise_payload(&fixture, round)
        };
        let before_state = state(&server);
        let missing = manifest(&fixture.root, &server);
        assert!(!missing.parent().unwrap().join("VSR_ACTIVE").exists());
        std::fs::remove_file(&missing).unwrap();
        let before_files = files(&fixture.root.join("inbound-membership"));
        for attempt in 0..2 {
            let (returned, result, applied) = tls(server, &fixture.root, 1, kind, &payload);
            server = returned;
            let error = result.unwrap_err();
            assert_lost_learner_rejection(&error, attempt == 0);
            assert_eq!(applied, 0);
            assert_eq!(state(&server), before_state);
            assert_eq!(
                files(&fixture.root.join("inbound-membership")),
                before_files
            );
            assert!(!missing.exists());
            assert!(!fixture.root.join("inbound-promoted").exists());
        }
    }
}

#[cfg(unix)]
#[test]
fn real_tls_managed_cached_stage_rejects_lost_learner_without_receipt_or_application_work() {
    let fixture = accepted_fixture();
    let (mut server, _source) = managed_learner(&fixture);
    server
        .membership_decisions
        .get_mut(&PG)
        .unwrap()
        .retain_chosen_input(fixture.plan.chosen().clone(), fixture.plan.approval())
        .unwrap();
    let mut applied = 0;
    let report = server
        .stage_membership_locally(fixture.plan.chosen().clone(), &mut |_| {
            applied += 1;
            Ok(())
        })
        .unwrap();
    assert!(applied > 0);
    let payload = fixture.plan.chosen().encode().unwrap();
    let (returned, result, applied) = tls(server, &fixture.root, 1, MEMBERSHIP_STAGE, &payload);
    server = returned;
    assert_eq!(
        rsf_vsr::VsrMembershipStagedReport::decode(&result.unwrap()).unwrap(),
        report
    );
    assert_eq!(
        applied, 0,
        "fixture must exercise the warm receipt shortcut"
    );
    let missing = manifest(&fixture.root, &server);
    std::fs::remove_file(&missing).unwrap();
    let before = files(&fixture.root.join("inbound-membership"));
    let before_state = state(&server);
    let (server, result, applied) = tls(server, &fixture.root, 1, MEMBERSHIP_STAGE, &payload);
    let error = result.unwrap_err();
    assert_lost_learner_rejection(&error, true);
    assert_eq!(applied, 0);
    assert_eq!(state(&server), before_state);
    assert_eq!(files(&fixture.root.join("inbound-membership")), before);
    assert!(!missing.exists());
    assert!(!fixture.root.join("inbound-promoted").exists());
}

#[test]
fn real_tls_managed_changed_learner_revalidates_original_authority_before_caching() {
    let fixture = accepted_fixture();
    let (mut server, mut source) = managed_learner(&fixture);
    let first = server.learners[&PG].published_state_token().unwrap();
    let payload = match &fixture.committed.operation {
        VsrOperation::ReplicatedCommand { payload, .. } => payload,
        _ => panic!("fixture operation"),
    };
    let entry = prepare(&mut source, payload);
    assert_eq!(entry, fixture.committed);
    source.acknowledge(NodeId(1), Epoch(1), entry.index);
    assert!(matches!(
        source.commit(NodeId(0), entry.index).unwrap(),
        VsrDecision::Committed(_)
    ));
    publish_application(&mut source, server.learners.get_mut(&PG).unwrap());
    let second = server.learners[&PG].published_state_token().unwrap();
    assert_ne!(first, second);
    let round = VsrMembershipRound {
        counter: 8,
        coordinator: NodeId(1),
    };
    let (returned, result, applied) = tls(
        server,
        &fixture.root,
        1,
        MEMBERSHIP_RECOVERY,
        &promise_payload(&fixture, round),
    );
    server = returned;
    result.unwrap();
    assert_eq!(applied, 0);
    assert_eq!(state(&server).promised, Some(round));

    // A fully valid hosted snapshot can still be incompatible with retained A.
    // This diagnostic uses the owned source API; it does not claim an old voter
    // can legally commit this divergent operation after a managed quorum fence.
    let entry = prepare(
        &mut source,
        b"committed history absent from original authority A",
    );
    source.acknowledge(NodeId(1), Epoch(1), entry.index);
    assert!(matches!(
        source.commit(NodeId(0), entry.index).unwrap(),
        VsrDecision::Committed(_)
    ));
    publish_application(&mut source, server.learners.get_mut(&PG).unwrap());
    assert_ne!(
        second,
        server.learners[&PG].published_state_token().unwrap()
    );
    let before = state(&server);
    let before_files = files(&fixture.root.join("inbound-membership"));
    let next = VsrMembershipRound {
        counter: 9,
        ..round
    };
    for _ in 0..2 {
        let (returned, result, applied) = tls(
            server,
            &fixture.root,
            1,
            MEMBERSHIP_RECOVERY,
            &promise_payload(&fixture, next),
        );
        server = returned;
        let error = result.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{error}");
        assert!(
            error.to_string().contains("hosted committed operation"),
            "{error}"
        );
        assert_eq!(applied, 0);
        assert_eq!(state(&server), before);
        assert_eq!(
            files(&fixture.root.join("inbound-membership")),
            before_files
        );
    }
}

#[test]
fn managed_anchor_validation_survives_temporarily_taken_recovery() {
    let fixture = accepted_fixture();
    let (mut server, _source) = managed_learner(&fixture);
    let recovery = server.membership_recovery.take().unwrap();
    let round = VsrMembershipRound {
        counter: 10,
        coordinator: NodeId(1),
    };
    let (mut server, result, applied) = tls(
        server,
        &fixture.root,
        1,
        MEMBERSHIP_RECOVERY,
        &promise_payload(&fixture, round),
    );
    result.unwrap();
    assert_eq!(applied, 0);
    assert_eq!(state(&server).promised, Some(round));
    assert!(server.membership_recovery_enabled());
    server.membership_recovery = Some(recovery);
}

#[test]
fn real_tls_managed_chosen_b_accepts_matching_learner_commit_and_original_receipt() {
    let fixture = accepted_fixture();
    let (mut server, mut source) = managed_learner(&fixture);
    let initial = VsrMembershipInput::from_approval(&fixture.authority).unwrap();
    assert_ne!(initial.id().unwrap(), fixture.plan.chosen().input_id());
    // The fixture has actual durable votes from 1+2 for B. B includes the old
    // primary's receipt absent from initial A; A/B share proposal and bootstrap.
    server
        .membership_decisions
        .get_mut(&PG)
        .unwrap()
        .retain_chosen_input(fixture.plan.chosen().clone(), fixture.plan.approval())
        .unwrap();
    let payload = match &fixture.committed.operation {
        VsrOperation::ReplicatedCommand { payload, .. } => payload,
        _ => panic!("fixture operation"),
    };
    let entry = prepare(&mut source, payload);
    assert_eq!(entry, fixture.committed);
    source.acknowledge(NodeId(1), Epoch(1), entry.index);
    assert!(matches!(
        source.commit(NodeId(0), entry.index).unwrap(),
        VsrDecision::Committed(_)
    ));
    source
        .remember_client_request(fixture.request.clone(), entry.index)
        .unwrap();
    let old = server.learners[&PG].published_state_token().unwrap();
    publish_application(&mut source, server.learners.get_mut(&PG).unwrap());
    assert_ne!(old, server.learners[&PG].published_state_token().unwrap());
    let request = wire_request(0, &proposed(), bootstrap(&fixture), &[]);
    let (mut server, result, applied) =
        tls(server, &fixture.root, 1, MEMBERSHIP_RECOVERY, &request);
    let bytes = result.unwrap();
    let metadata = rsf_vsr::VsrMembershipDecisionState::decode(&bytes[7..]).unwrap();
    assert_eq!(metadata.authority, initial.id().unwrap());
    assert_eq!(metadata.chosen.unwrap().1, fixture.plan.chosen().input_id());
    assert_eq!(applied, 0);
    let report = server
        .stage_membership_locally(fixture.plan.chosen().clone(), &mut |_| Ok(()))
        .unwrap();
    assert_eq!(report.input, fixture.plan.chosen().input_id());
    assert_eq!(
        server.membership_staging[&PG]
            .plan()
            .request_commit(&fixture.request),
        Some(entry.index)
    );
    assert!(server.cells.is_empty());
    assert!(server.membership_admission_required().unwrap());
}

#[test]
fn real_tls_managed_new_choice_revalidates_b_even_when_learner_token_is_unchanged() {
    let fixture = accepted_fixture();
    let (mut server, _source) = managed_learner(&fixture);
    let token = server.learners[&PG].published_state_token().unwrap();
    server
        .membership_decisions
        .get_mut(&PG)
        .unwrap()
        .retain_chosen_input(fixture.plan.chosen().clone(), fixture.plan.approval())
        .unwrap();
    assert_eq!(token, server.learners[&PG].published_state_token().unwrap());
    let missing = fixture
        .root
        .join("inbound-membership/decision/inputs")
        .join(id_name(fixture.plan.chosen().input_id()))
        .join("records/MANIFEST");
    std::fs::remove_file(&missing).unwrap();
    let before = files(&fixture.root.join("inbound-membership"));
    let before_state = state(&server);
    let request = wire_request(0, &proposed(), bootstrap(&fixture), &[]);
    for _ in 0..2 {
        let (returned, result, applied) =
            tls(server, &fixture.root, 1, MEMBERSHIP_RECOVERY, &request);
        server = returned;
        let error = result.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{error}");
        assert_eq!(applied, 0);
        assert_eq!(state(&server), before_state);
        assert_eq!(files(&fixture.root.join("inbound-membership")), before);
        assert!(!missing.exists());
    }
}

#[cfg(unix)]
#[test]
fn real_tls_managed_cached_stage_survives_auxiliary_journal_loss_but_not_hosted_loss() {
    let fixture = accepted_fixture();
    let (mut server, _source) = managed_learner(&fixture);
    server
        .membership_decisions
        .get_mut(&PG)
        .unwrap()
        .retain_chosen_input(fixture.plan.chosen().clone(), fixture.plan.approval())
        .unwrap();
    let mut materialized = 0;
    let expected = server
        .stage_membership_locally(fixture.plan.chosen().clone(), &mut |entry| {
            materialized += 1;
            persist_staged_entry(&fixture.root, 3, entry)
        })
        .unwrap();
    assert!(materialized > 0);
    let chosen = fixture.plan.chosen().encode().unwrap();
    let (returned, result, applied) = tls(server, &fixture.root, 1, MEMBERSHIP_STAGE, &chosen);
    server = returned;
    assert_eq!(
        rsf_vsr::VsrMembershipStagedReport::decode(&result.unwrap()).unwrap(),
        expected
    );
    assert_eq!(applied, 0);

    // Locate the hosted file while healthy. Loss below is strictly in the
    // auxiliary acceptor journal; the complete stage/hosted prefix stay intact.
    let hosted = manifest(&fixture.root, &server);
    let journal = fixture
        .root
        .join("inbound-membership/decision/journal/MANIFEST");
    std::fs::remove_file(&journal).unwrap();
    let journal_lost = files(&fixture.root);
    for _ in 0..2 {
        let (returned, result, applied) = tls(server, &fixture.root, 1, MEMBERSHIP_STAGE, &chosen);
        server = returned;
        assert_eq!(
            rsf_vsr::VsrMembershipStagedReport::decode(&result.unwrap()).unwrap(),
            expected
        );
        assert_eq!(applied, 0, "warm stage must not repeat application work");
        assert_eq!(files(&fixture.root), journal_lost);
        assert!(!journal.exists());
        assert!(hosted.is_file());
    }

    // The independent receipt is not a general bypass: its first operation
    // after hosted loss must probe the physical learner token and reject.
    std::fs::remove_file(&hosted).unwrap();
    let hosted_lost = files(&fixture.root);
    for attempt in 0..2 {
        let (returned, result, applied) = tls(server, &fixture.root, 1, MEMBERSHIP_STAGE, &chosen);
        server = returned;
        assert_lost_learner_rejection(&result.unwrap_err(), attempt == 0);
        assert_eq!(applied, 0);
        assert_eq!(files(&fixture.root), hosted_lost);
        assert!(!journal.exists());
        assert!(!hosted.exists());
        assert!(!fixture.root.join("inbound-promoted").exists());
    }
}
