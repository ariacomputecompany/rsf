//! Existing public/wire APIs: these tests can run before the production fix.
use super::*;

fn partial_upload(fixture: &Accepted, peer: &PeerProcess, count: usize) {
    let mut peers = all_endpoints(&fixture.root);
    peers.get_mut(&NodeId(2)).unwrap().address = peer.address;
    let sender = decision_transport(&fixture.root, 1, peers);
    let input = VsrMembershipInput::from_approval(fixture.plan.approval()).unwrap();
    let round = VsrMembershipRound {
        counter: 1,
        coordinator: NodeId(1),
    };
    let mut prefix = 1_u64.to_le_bytes().to_vec();
    prefix.extend_from_slice(&round.counter.to_le_bytes());
    prefix.extend_from_slice(&round.coordinator.0.to_le_bytes());
    prefix.extend_from_slice(&input.proposal().id().unwrap());
    let promise = sender
        .exchange_io(NodeId(2), MEMBERSHIP_PROMISE, &prefix)
        .unwrap();
    let promise = rsf_vsr::VsrMembershipPromise::decode(&promise).unwrap();
    assert_eq!(promise.round, round);
    assert_eq!(
        sender
            .begin_membership_input(NodeId(2), round, &input)
            .unwrap(),
        0
    );
    if count != 0 {
        let records = input.flattened_records(fixture.plan.approval()).unwrap();
        assert!(count < records.len());
        let batch = VsrHistoryBatch::pack(0, &records[..count]).unwrap();
        assert_eq!(batch.records.len(), count);
        assert_eq!(
            sender
                .append_membership_input(NodeId(2), round, &input, batch)
                .unwrap(),
            count as u64
        );
    }
}
fn segment(input: &Path) -> PathBuf {
    let paths = std::fs::read_dir(input.join("records/segments"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension() != Some(std::ffi::OsStr::new("lock")))
        .collect::<Vec<_>>();
    assert_eq!(paths.len(), 1);
    paths[0].clone()
}
fn complete_partial(count: usize) {
    let fixture = fixture_for_receiver(false);
    let peer = PeerProcess::spawn_staging(&fixture.root, 2, false);
    partial_upload(&fixture, &peer, count);
    // Interrupt the transfer by ending the uploading transport, then restart
    // the actual receiver. This is an acknowledged staged prefix, never a vote.
    drop(peer);
    let retained = fixture
        .root
        .join("membership-2/decision/inputs")
        .join(id_name(fixture.plan.chosen().input_id()));
    let path = segment(&retained);
    let prefix = std::fs::read(&path).unwrap();
    assert_eq!(prefix.is_empty(), count == 0);
    let descriptor = std::fs::read(retained.join("DESCRIPTOR")).unwrap();
    #[cfg(unix)]
    let inode = {
        use std::os::unix::fs::MetadataExt;
        std::fs::metadata(&path).unwrap().ino()
    };
    let decision = MembershipDecisionStore::open_existing(
        fixture.root.join("membership-2/decision"),
        PlacementGroupId(1),
        NodeId(2),
    )
    .unwrap();
    assert!(decision.chosen().is_none());
    assert_eq!(
        decision
            .retained_approval(fixture.plan.chosen().input_id())
            .unwrap_err()
            .message,
        "membership input is incomplete"
    );
    drop(decision);
    let peer = PeerProcess::spawn_staging(&fixture.root, 2, false);
    let mut peers = all_endpoints(&fixture.root);
    peers.get_mut(&NodeId(2)).unwrap().address = peer.address;
    let sender = decision_transport(&fixture.root, 3, peers);
    sender
        .provision_membership_authority(NodeId(2), fixture.plan.approval())
        .unwrap();
    let inbox = files(&fixture.root.join("membership-2/inbox"));
    let report = sender
        .stage_membership(NodeId(2), &fixture.plan)
        .unwrap_or_else(|error| {
            panic!(
                "complete inbox must resume staged-only prefix: {}",
                error.reason
            )
        });
    assert_eq!(report.replica, NodeId(2));
    assert_eq!(report.plan, fixture.plan.id());
    assert_eq!(report.committed_tail, LogIndex(1));
    assert_eq!(report.accepted_tail, LogIndex(2));
    assert_eq!(
        std::fs::read(fixture.root.join("materialized-2/1")).unwrap(),
        fixture.committed.encode().unwrap()
    );
    assert_eq!(files(&fixture.root.join("membership-2/inbox")), inbox);
    assert!(!fixture.root.join("store-2/VSR_ACTIVE").exists());
    assert_eq!(
        std::fs::read(retained.join("DESCRIPTOR")).unwrap(),
        descriptor
    );
    let completed = std::fs::read(&path).unwrap();
    assert!(completed.len() > prefix.len());
    assert!(completed.starts_with(&prefix));
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        assert_eq!(std::fs::metadata(&path).unwrap().ino(), inode);
    }
    drop(peer);
    let decision = MembershipDecisionStore::open_existing(
        fixture.root.join("membership-2/decision"),
        PlacementGroupId(1),
        NodeId(2),
    )
    .unwrap();
    assert_eq!(decision.chosen(), Some(fixture.plan.chosen()));
    let approval = decision
        .retained_approval(fixture.plan.chosen().input_id())
        .unwrap()
        .unwrap();
    let input = VsrMembershipInput::from_approval(&approval).unwrap();
    assert_eq!(
        input,
        VsrMembershipInput::from_approval(fixture.plan.approval()).unwrap()
    );
    assert_eq!(
        input.flattened_records(&approval).unwrap(),
        input.flattened_records(fixture.plan.approval()).unwrap()
    );
    drop(decision);
    let stage = rsf_vsr::MembershipStagingStore::open_existing(
        fixture.root.join("membership-2/staging"),
        PlacementGroupId(1),
        NodeId(2),
    )
    .unwrap();
    assert!(stage.report().is_err());
    assert_eq!(
        stage.plan().request_commit(&fixture.request),
        Some(LogIndex(1))
    );
    assert_eq!(
        stage
            .plan()
            .accepted_entries()
            .iter()
            .find(|entry| entry.index == LogIndex(2))
            .unwrap()
            .operation,
        fixture.accepted.operation
    );
}
#[test]
fn real_tls_stage_resumes_zero_staged_prefix_from_complete_inbox() {
    complete_partial(0);
}
#[test]
fn real_tls_stage_resumes_nonempty_staged_prefix_from_complete_inbox() {
    complete_partial(1);
}

fn reject_loss(role: &str, fault: &str) {
    let fixture = if role == "accepted" || role == "chosen" {
        accepted_fixture()
    } else {
        fixture_for_receiver(false)
    };
    if role == "chosen" {
        let mut decision = MembershipDecisionStore::open_existing(
            fixture.root.join("membership-2/decision"),
            PlacementGroupId(1),
            NodeId(2),
        )
        .unwrap();
        decision
            .retain_chosen_input(fixture.plan.chosen().clone(), fixture.plan.approval())
            .unwrap();
    }
    let peer = PeerProcess::spawn_staging(&fixture.root, 2, false);
    if role == "authority" || role == "staged" {
        partial_upload(&fixture, &peer, 1);
    }
    let mut peers = all_endpoints(&fixture.root);
    peers.get_mut(&NodeId(2)).unwrap().address = peer.address;
    let sender = decision_transport(&fixture.root, 3, peers);
    sender
        .provision_membership_authority(NodeId(2), fixture.plan.approval())
        .unwrap();
    let id = if role == "authority" {
        VsrMembershipInput::from_approval(&fixture.authority)
            .unwrap()
            .id()
            .unwrap()
    } else {
        fixture.plan.chosen().input_id()
    };
    let retained = fixture
        .root
        .join("membership-2/decision/inputs")
        .join(id_name(id));
    match fault {
        "missing" => std::fs::remove_dir_all(&retained).unwrap(),
        "short" => {
            std::fs::OpenOptions::new()
                .write(true)
                .open(segment(&retained))
                .unwrap()
                .set_len(0)
                .unwrap();
        }
        "corrupt" => {
            let path = segment(&retained);
            let mut bytes = std::fs::read(&path).unwrap();
            let last = bytes.last_mut().unwrap();
            *last ^= 1;
            std::fs::write(path, bytes).unwrap();
        }
        _ => unreachable!(),
    }
    let before = files(&fixture.root.join("membership-2"));
    for attempt in 0..2 {
        let error = sender
            .stage_membership(NodeId(2), &fixture.plan)
            .unwrap_err();
        let semantic = error
            .reason
            .strip_prefix("peer rejected request: ")
            .unwrap_or_else(|| {
                panic!("expected authenticated STAGE refusal, got {}", error.reason)
            });
        let exact_fault = match fault {
            "missing" => {
                semantic.starts_with("membership decision storage: ")
                    && semantic.contains("No such file or directory")
            }
            "short" => semantic == "membership input is incomplete",
            "corrupt" => semantic.starts_with("checksum mismatch at offset "),
            _ => unreachable!(),
        };
        // Before the fix, staged B's strict incomplete read deliberately stops
        // before authority A is examined. Only that exact old semantic refusal
        // is allowed in the shared RED test; transport failures never qualify.
        let old_strict_prefix = role == "authority" && semantic == "membership input is incomplete";
        let fenced_retry = attempt == 1
            && semantic == "membership decision durability failed; reopen before further use";
        assert!(
            exact_fault || old_strict_prefix || fenced_retry,
            "wrong STAGE refusal for {role}/{fault}: {semantic}"
        );
        assert_eq!(files(&fixture.root.join("membership-2")), before);
        assert!(!fixture.root.join("membership-2/staging").exists());
        assert!(!fixture.root.join("materialized-2").exists());
    }
    drop(peer);
    assert_eq!(files(&fixture.root.join("membership-2")), before);
}
#[test]
fn real_tls_partial_stage_refuses_missing_authority() {
    reject_loss("authority", "missing");
}
#[test]
fn real_tls_partial_stage_refuses_short_authority() {
    reject_loss("authority", "short");
}
#[test]
fn real_tls_partial_stage_refuses_corrupt_authority() {
    reject_loss("authority", "corrupt");
}
#[test]
fn real_tls_partial_stage_refuses_corrupt_staged_prefix() {
    reject_loss("staged", "corrupt");
}
#[test]
fn real_tls_stage_refuses_short_accepted_despite_complete_inbox() {
    reject_loss("accepted", "short");
}
#[test]
fn real_tls_stage_refuses_missing_chosen_despite_complete_inbox() {
    reject_loss("chosen", "missing");
}
