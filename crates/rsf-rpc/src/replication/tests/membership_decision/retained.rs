use super::*;
use rsf_vsr::{
    MembershipAuthorityInbox, VsrClientRequestId, VsrHistoryBatch, VsrMembershipApproval,
    VsrMembershipChosen, VsrMembershipDecision, VsrMembershipStagedPlan,
};

struct Accepted {
    root: PathBuf,
    plan: VsrMembershipStagedPlan,
    authority: VsrMembershipApproval,
    committed: VsrPrepare,
    accepted: VsrPrepare,
    request: VsrClientRequestId,
}
impl Drop for Accepted {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!("preserved membership fixture: {}", self.root.display());
            return;
        }
        let _ = std::fs::remove_dir_all(&self.root);
    }
}
fn id_name(id: [u8; 32]) -> String {
    id.iter().map(|b| format!("{b:02x}")).collect()
}
fn files(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn visit(base: &Path, directory: &Path, files: &mut BTreeMap<PathBuf, Vec<u8>>) {
        for entry in std::fs::read_dir(directory).unwrap() {
            let entry = entry.unwrap();
            let kind = entry.file_type().unwrap();
            if kind.is_dir() {
                visit(base, &entry.path(), files);
            } else {
                assert!(kind.is_file());
                if entry.path().extension() != Some(std::ffi::OsStr::new("lock")) {
                    files.insert(
                        entry.path().strip_prefix(base).unwrap().to_owned(),
                        std::fs::read(entry.path()).unwrap(),
                    );
                }
            }
        }
    }
    let mut result = BTreeMap::new();
    visit(root, root, &mut result);
    result
}
fn accepted_fixture() -> Accepted {
    fixture_for_receiver(true)
}
fn fixture_for_receiver(receiver_accepted: bool) -> Accepted {
    let root = root("accepted-stage-import");
    provision(&root);
    let mut cells = (0..3).map(|node| cell(&root, node)).collect::<Vec<_>>();
    let payload = (0..128 * 1024).map(|n| (n % 251) as u8).collect::<Vec<_>>();
    let committed = prepare(&mut cells[0], &payload);
    for (node, cell) in cells.iter_mut().enumerate().skip(1) {
        cell.receive_prepare(NodeId(node as u64), NodeId(0), committed.clone())
            .unwrap();
    }
    cells[0].acknowledge(NodeId(1), Epoch(1), committed.index);
    let VsrDecision::Committed(commit) = cells[0].commit(NodeId(0), committed.index).unwrap()
    else {
        panic!("commit");
    };
    for (node, cell) in cells.iter_mut().enumerate().skip(1) {
        cell.receive_commit(NodeId(node as u64), NodeId(0), commit.clone())
            .unwrap();
    }
    let request = VsrClientRequestId::new("accepted-input-original", 9);
    cells[0]
        .remember_client_request(request.clone(), committed.index)
        .unwrap();
    let accepted = prepare(
        &mut cells[0],
        b"accepted tail survives choice dissemination loss",
    );
    for (node, cell) in cells.iter_mut().enumerate().skip(1) {
        cell.receive_prepare(NodeId(node as u64), NodeId(0), accepted.clone())
            .unwrap();
    }
    let snapshots = cells
        .iter_mut()
        .enumerate()
        .map(|(node, cell)| {
            cell.begin_membership(NodeId(node as u64), NodeId(0), proposed())
                .unwrap()
        })
        .collect::<Vec<_>>();
    let authority = VsrMembershipApproval::from_snapshots(snapshots[1..].to_vec()).unwrap();
    let approval = VsrMembershipApproval::from_snapshots(snapshots).unwrap();
    let initial = VsrMembershipInput::from_approval(&authority).unwrap();
    let selected = VsrMembershipInput::from_approval(&approval).unwrap();
    assert_ne!(initial.id().unwrap(), selected.id().unwrap());
    // The authority inbox contains A only. B is durably accepted in its own
    // journal, and the old coordinator never disseminates the chosen proof.
    let mut inbox = MembershipAuthorityInbox::open(
        root.join("membership-2/inbox"),
        PlacementGroupId(1),
        NodeId(2),
    )
    .unwrap();
    inbox.begin(NodeId(1), initial.clone()).unwrap();
    let records = initial.flattened_records(&authority).unwrap();
    let mut at = 0;
    while at < records.len() {
        let batch = VsrHistoryBatch::pack(at as u64, &records[at..]).unwrap();
        at += batch.records.len();
        inbox
            .append(NodeId(1), initial.id().unwrap(), batch)
            .unwrap();
    }
    inbox.finish(NodeId(1), initial.id().unwrap()).unwrap();
    drop(inbox);
    let acceptor = if receiver_accepted { 2 } else { 3 };
    let mut stores = [1, acceptor].map(|node| {
        MembershipDecisionStore::open(
            root.join(format!("membership-{node}/decision")),
            NodeId(node),
            &authority,
        )
        .unwrap()
    });
    let round = VsrMembershipRound {
        counter: 1,
        coordinator: NodeId(1),
    };
    let promises = stores
        .iter_mut()
        .map(|store| store.promise(NodeId(1), round).unwrap())
        .collect();
    let decision = VsrMembershipDecision::from_promises(promises, selected.clone()).unwrap();
    let records = selected.flattened_records(&approval).unwrap();
    for store in &mut stores {
        store
            .begin_input(NodeId(1), round, selected.clone())
            .unwrap();
        let mut at = 0;
        while at < records.len() {
            let batch = VsrHistoryBatch::pack(at as u64, &records[at..]).unwrap();
            at += batch.records.len();
            store
                .append_input(NodeId(1), round, selected.id().unwrap(), batch)
                .unwrap();
        }
    }
    let nomination = stores[0].nominate(&decision).unwrap();
    let accepted_vote = stores[1].accept(NodeId(1), &decision, &nomination).unwrap();
    let chosen = VsrMembershipChosen::from_accepted(vec![nomination, accepted_vote]).unwrap();
    assert!(stores[1].chosen().is_none());
    drop(stores);
    if !receiver_accepted {
        // B was chosen by 1+3; the late receiver has only initial authority A.
        drop(
            MembershipDecisionStore::open(
                root.join("membership-2/decision"),
                NodeId(2),
                &authority,
            )
            .unwrap(),
        );
    }
    drop(cells);
    for name in ["store-0", "store-1", "membership-1"] {
        std::fs::remove_dir_all(root.join(name)).unwrap();
    }
    let plan = VsrMembershipStagedPlan::select(chosen, approval).unwrap();
    Accepted {
        root,
        plan,
        authority,
        committed,
        accepted,
        request,
    }
}

#[test]
fn real_tls_stage_imports_chosen_from_accepted_input_without_old_sender_inbox() {
    let fixture = accepted_fixture();
    let root = &fixture.root;
    let inbox = files(&root.join("membership-2/inbox"));
    let retained = files(&root.join("membership-2/decision/inputs"));
    let peer = PeerProcess::spawn_staging(root, 2, false);
    let mut peers = all_endpoints(root);
    peers.get_mut(&NodeId(2)).unwrap().address = peer.address;
    let network = decision_transport(root, 3, peers.clone());
    let old = decision_transport(root, 0, peers);
    assert!(
        old.exchange_io(
            NodeId(2),
            MEMBERSHIP_STAGE,
            &fixture.plan.chosen().encode().unwrap()
        )
        .is_err()
    );
    let mut conflicting = fixture.plan.chosen().accepted().to_vec();
    let authority_id = VsrMembershipInput::from_approval(&fixture.authority)
        .unwrap()
        .id()
        .unwrap();
    for vote in &mut conflicting {
        vote.input = authority_id;
    }
    let conflicting = VsrMembershipChosen::from_accepted(conflicting).unwrap();
    let other = VsrMembershipStagedPlan::select(conflicting, fixture.authority.clone()).unwrap();
    assert!(network.stage_membership(NodeId(2), &other).is_err());
    assert!(!root.join("membership-2/staging").exists());
    let mut foreign = fixture.plan.chosen().accepted().to_vec();
    for vote in &mut foreign {
        vote.proposal.election.target_view = Epoch(3);
    }
    let foreign = VsrMembershipChosen::from_accepted(foreign).unwrap();
    assert!(
        network
            .exchange_io(NodeId(2), MEMBERSHIP_STAGE, &foreign.encode().unwrap())
            .is_err()
    );
    let report = network.stage_membership(NodeId(2), &fixture.plan).unwrap();
    assert_eq!(report.replica, NodeId(2));
    assert_eq!(report.plan, fixture.plan.id());
    assert_eq!(report.committed_tail, LogIndex(1));
    assert_eq!(report.accepted_tail, LogIndex(2));
    assert_eq!(
        std::fs::read(root.join("materialized-2/1")).unwrap(),
        fixture.committed.encode().unwrap()
    );
    assert_eq!(
        files(&root.join("membership-2/inbox")),
        inbox,
        "single-peer staging reprovisioned authority"
    );
    assert_eq!(files(&root.join("membership-2/decision/inputs")), retained);
    assert!(
        !root
            .join("membership-2/inbox/inputs")
            .join(id_name(fixture.plan.chosen().input_id()))
            .exists()
    );
    assert!(!root.join("store-2/VSR_ACTIVE").exists());
    drop(peer);
    let decision = MembershipDecisionStore::open_existing(
        root.join("membership-2/decision"),
        PlacementGroupId(1),
        NodeId(2),
    )
    .unwrap();
    assert_eq!(decision.chosen(), Some(fixture.plan.chosen()));
    drop(decision);
    let stage = rsf_vsr::MembershipStagingStore::open_existing(
        root.join("membership-2/staging"),
        PlacementGroupId(1),
        NodeId(2),
    )
    .unwrap();
    assert!(
        stage.report().is_err(),
        "cold custody granted an application receipt"
    );
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
    drop(stage);
    let peer = PeerProcess::spawn_staging(root, 2, false);
    let mut peers = all_endpoints(root);
    peers.get_mut(&NodeId(2)).unwrap().address = peer.address;
    let retry = decision_transport(root, 3, peers)
        .stage_membership(NodeId(2), &fixture.plan)
        .unwrap();
    assert_eq!(retry, report);
    assert_eq!(files(&root.join("membership-2/inbox")), inbox);
    drop(peer);
}

#[test]
fn real_tls_stage_rejects_corrupt_accepted_custody_without_inbox_fallback_or_repair() {
    for fault in ["descriptor", "missing-input"] {
        let fixture = accepted_fixture();
        let root = &fixture.root;
        let peer = PeerProcess::spawn_staging(root, 2, false);
        let mut peers = all_endpoints(root);
        peers.get_mut(&NodeId(2)).unwrap().address = peer.address;
        let network = decision_transport(root, 3, peers);
        let input = root
            .join("membership-2/decision/inputs")
            .join(id_name(fixture.plan.chosen().input_id()));
        if fault == "descriptor" {
            std::fs::write(input.join("DESCRIPTOR"), b"damaged accepted input").unwrap();
        } else {
            std::fs::remove_dir_all(&input).unwrap();
        }
        let before = files(&root.join("membership-2"));
        for _ in 0..2 {
            assert!(network.stage_membership(NodeId(2), &fixture.plan).is_err());
            assert_eq!(files(&root.join("membership-2")), before);
            assert!(!root.join("membership-2/staging").exists());
            assert!(!root.join("materialized-2").exists());
        }
        drop(peer);
        assert!(
            MembershipDecisionStore::open_existing(
                root.join("membership-2/decision"),
                PlacementGroupId(1),
                NodeId(2)
            )
            .is_err()
        );
        assert_eq!(files(&root.join("membership-2")), before);
    }
}

#[path = "inbox_race.rs"]
mod inbox_race;

#[path = "prechoice.rs"]
mod prechoice;

#[path = "partial_stage.rs"]
mod partial_stage;
