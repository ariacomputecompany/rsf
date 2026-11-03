use super::*;

#[test]
fn membership_publication_ancestry_survives_proof_variants_new_views_and_promises() {
    let path = root("publication-ancestry");
    let mut f = fixture(&path);
    fence_primary(&mut f);
    let (stages, quorum) = staged(&path, &f);
    let id = stages[0].plan().id();
    assert!(!f.cells[0].has_membership_publication().unwrap());
    assert!(f.cells[0].membership_publication(id).unwrap().is_none());
    assert!(stages[0].validate_published_anchor(&f.cells[0]).is_err());

    let mut alternate = MembershipStagingStore::stage(
        path.join("alternate-stage-1"),
        NodeId(1),
        plan(&f, true),
        &f.cells[1],
    )
    .unwrap();
    alternate
        .materialize(&mut |e| write_application(&path.join("alternate-app-1"), e))
        .unwrap();
    let mut learner = f
        .learner
        .stage_membership(path.join("alternate-stage-3"), plan(&f, true))
        .unwrap();
    let third = learner
        .materialize(&mut |e| write_application(&path.join("alternate-app-3"), e))
        .unwrap();
    let other_quorum = VsrMembershipStagedQuorum::from_reports(vec![
        alternate.report().unwrap(),
        stages[2].report().unwrap(),
        third,
    ])
    .unwrap();
    assert_ne!(quorum.encode().unwrap(), other_quorum.encode().unwrap());
    assert_ne!(
        alternate.plan().chosen().encode().unwrap(),
        stages[0].plan().chosen().encode().unwrap()
    );
    assert_eq!(alternate.plan().id(), id);
    alternate.retain_publication(&other_quorum).unwrap();
    let retained = fs::read(path.join("alternate-stage-1/PUBLICATION")).unwrap();
    assert!(
        fs::read(path.join("alternate-stage-1/META"))
            .unwrap()
            .starts_with(b"VSRMST2")
    );

    f.cells[0].install_membership(&stages[0], &quorum).unwrap();
    f.cells[1]
        .install_membership(&alternate, &other_quorum)
        .unwrap();
    f.cells[2].install_membership(&stages[2], &quorum).unwrap();
    assert!(
        f.cells
            .iter()
            .all(|cell| cell.has_membership_publication().unwrap())
    );
    let proof = f.cells[1].membership_publication(id).unwrap().unwrap();
    assert_eq!(proof.proposal(), &proposal());
    assert_eq!(proof.input_id(), alternate.plan().chosen().input_id());
    assert_eq!(proof.plan_id(), id);
    assert_eq!(proof.staged_quorum(), &other_quorum);
    assert_eq!(proof.chosen(), alternate.plan().chosen());
    assert_eq!(proof.input(), alternate.plan().input());
    alternate.validate_published_anchor(&f.cells[1]).unwrap();
    // A shared canonical plan does not prove preservation of a receiver-local
    // suffix. Node 1 has not yet learned node 0's independent durable receipt.
    assert!(stages[0].validate_published_anchor(&f.cells[1]).is_err());
    let mut absent = id;
    absent[0] ^= 1;
    assert!(f.cells[1].membership_publication(absent).unwrap().is_none());

    let request = VsrViewChangeRequest {
        pg: PlacementGroupId(1),
        previous_view: Epoch(2),
        target_view: Epoch(3),
        candidate: NodeId(2),
        voters: (0..4).map(NodeId).collect(),
    };
    let snapshots = f
        .cells
        .iter_mut()
        .enumerate()
        .map(|(n, cell)| {
            cell.begin_view_change(NodeId(n as u64), NodeId(2), request.clone())
                .unwrap()
        })
        .collect::<Vec<_>>();
    // Equal-view, equal-tail selection uses node 0's physical generation. Its
    // valid chosen/staging proof subsets differ from node 1's old publication.
    let next = crate::VsrViewInstallation::select(&snapshots).unwrap();
    f.cells[1].install_view(&next).unwrap();
    assert_eq!(f.cells[1].view(), Epoch(3));
    assert!(f.cells[1].has_membership_publication().unwrap());
    let proof = f.cells[1].membership_publication(id).unwrap().unwrap();
    assert_eq!(proof.staged_quorum(), &quorum);
    assert_eq!(proof.chosen(), stages[0].plan().chosen());
    alternate.validate_published_anchor(&f.cells[1]).unwrap();
    stages[0].validate_published_anchor(&f.cells[1]).unwrap();
    assert_eq!(
        f.cells[1].request_commit(&VsrClientRequestId::new("receiver-extra", 9)),
        Some(LogIndex(1))
    );
    let unchanged = f.cells[1].recovery_state_token().unwrap();
    assert!(
        f.cells[1]
            .install_membership(&alternate, &other_quorum)
            .is_err()
    );
    assert_eq!(f.cells[1].recovery_state_token().unwrap(), unchanged);

    let promise = VsrViewChangeRequest {
        previous_view: Epoch(3),
        target_view: Epoch(4),
        ..request
    };
    f.cells[1]
        .begin_view_change(NodeId(1), NodeId(2), promise.clone())
        .unwrap();
    let unchanged = f.cells[1].recovery_state_token().unwrap();
    for _ in 0..3 {
        assert!(f.cells[1].has_membership_publication().unwrap());
        assert!(f.cells[1].membership_publication(id).unwrap().is_some());
        alternate.validate_published_anchor(&f.cells[1]).unwrap();
    }
    assert_eq!(f.cells[1].recovery_state_token().unwrap(), unchanged);
    assert_eq!(f.cells[1].view_change_promise(), Some(&promise));
    assert!(f.cells[1].primary_probe().is_err());
    drop(f);
    drop(stages);
    drop(alternate);
    drop(learner);

    let recovered = reopen(&path, 1);
    let stage = MembershipStagingStore::open_existing(
        path.join("alternate-stage-1"),
        PlacementGroupId(1),
        NodeId(1),
    )
    .unwrap();
    assert_eq!(recovered.view(), Epoch(3));
    assert!(recovered.has_membership_publication().unwrap());
    assert_eq!(recovered.view_change_promise(), Some(&promise));
    assert_eq!(
        recovered
            .membership_publication(id)
            .unwrap()
            .unwrap()
            .staged_quorum(),
        &quorum
    );
    stage.validate_published_anchor(&recovered).unwrap();
    assert_eq!(stage.publication_quorum().unwrap(), Some(other_quorum));
    assert_eq!(
        fs::read(path.join("alternate-stage-1/PUBLICATION")).unwrap(),
        retained
    );
    assert!(
        stage.report().is_err(),
        "ancestry must not imply boot materialization"
    );
    assert!(recovered.primary_probe().is_err());
    drop(recovered);
    drop(stage);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn membership_publication_ancestry_ignores_inactive_generations_and_matching_voters() {
    let path = root("publication-inactive");
    let mut f = fixture(&path);
    fence_primary(&mut f);
    let (stages, quorum) = staged(&path, &f);
    let id = stages[1].plan().id();
    let result = f.cells[1].install_membership_with_hook(&stages[1], &quorum, &mut |at| {
        if matches!(at, Boundary::Staged) {
            Err(corrupt("stop before authoritative publication"))
        } else {
            Ok(())
        }
    });
    assert!(result.is_err());
    assert!(path.join("old/1/vsr-views").is_dir());
    assert!(!path.join("old/1/VSR_ACTIVE").exists());
    assert!(!f.cells[1].has_membership_publication().unwrap());
    assert!(f.cells[1].membership_publication(id).unwrap().is_none());
    assert!(stages[1].validate_published_anchor(&f.cells[1]).is_err());

    let lookalike = DurableVsrCell::open(
        path.join("same-voters-unrelated-bootstrap"),
        PlacementGroupId(1),
        Epoch(2),
        NodeId(1),
        (0..4).map(NodeId),
    )
    .unwrap();
    assert_eq!(lookalike.replicas(), proposal().new_voters);
    assert!(!lookalike.has_membership_publication().unwrap());
    assert!(lookalike.membership_publication(id).unwrap().is_none());
    assert!(stages[1].validate_published_anchor(&lookalike).is_err());
    drop(lookalike);
    drop(f);

    let mut recovered = reopen(&path, 1);
    assert_eq!(recovered.view(), Epoch(1));
    assert!(!recovered.has_membership_publication().unwrap());
    assert!(recovered.primary_probe().is_err());
    assert!(recovered.membership_publication(id).unwrap().is_none());
    recovered.install_membership(&stages[1], &quorum).unwrap();
    assert!(recovered.has_membership_publication().unwrap());
    assert!(recovered.membership_publication(id).unwrap().is_some());
    drop(recovered);
    drop(stages);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn membership_publication_ancestry_requires_healthy_active_custody() {
    let path = root("publication-active-custody");
    let mut f = fixture(&path);
    let (stages, quorum) = staged(&path, &f);
    f.cells[1].install_membership(&stages[1], &quorum).unwrap();
    let id = stages[1].plan().id();
    assert!(f.cells[1].membership_publication(id).unwrap().is_some());
    assert!(f.cells[1].has_membership_publication().unwrap());
    fs::remove_file(path.join("old/1/VSR_ACTIVE")).unwrap();
    assert!(f.cells[1].has_membership_publication().is_err());
    assert!(f.cells[1].membership_publication(id).is_err());
    assert!(stages[1].validate_published_anchor(&f.cells[1]).is_err());
    drop(f);
    assert!(DurableVsrCell::open_existing(path.join("old/1"), PlacementGroupId(1)).is_err());
    drop(stages);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn published_stage_materialization_applies_current_commits_before_renewing_receipt() {
    let path = root("published-stage-current-materialization");
    let mut f = fixture(&path);
    fence_primary(&mut f);
    let (mut stages, quorum) = staged(&path, &f);
    stages[1].retain_publication(&quorum).unwrap();
    let retained = fs::read(path.join("stage-1/PUBLICATION")).unwrap();
    for (cell, stage) in f.cells.iter_mut().zip(&stages) {
        cell.install_membership(stage, &quorum).unwrap();
    }
    let prepare = f.cells[1].reprepare(NodeId(1), LogIndex(3)).unwrap();
    for n in [0, 2] {
        f.cells[n]
            .receive_prepare(NodeId(n as u64), NodeId(1), prepare.clone())
            .unwrap();
        f.cells[1].acknowledge(NodeId(n as u64), Epoch(2), LogIndex(3));
    }
    let VsrDecision::Committed(commit) = f.cells[1].commit(NodeId(1), LogIndex(3)).unwrap() else {
        panic!("later committed entry");
    };
    for n in [0, 2] {
        f.cells[n]
            .receive_commit(NodeId(n as u64), NodeId(1), commit.clone())
            .unwrap();
    }
    let promise = VsrViewChangeRequest {
        pg: PlacementGroupId(1),
        previous_view: Epoch(2),
        target_view: Epoch(3),
        candidate: NodeId(2),
        voters: (0..4).map(NodeId).collect(),
    };
    f.cells[1]
        .begin_view_change(NodeId(1), NodeId(2), promise.clone())
        .unwrap();
    drop(f);
    drop(stages);
    let cell = reopen(&path, 1);
    let mut stage =
        MembershipStagingStore::open_existing(path.join("stage-1"), PlacementGroupId(1), NodeId(1))
            .unwrap();
    assert!(stage.report().is_err());
    assert_eq!(stage.plan().committed_tail(), LogIndex(2));
    assert_eq!(cell.committed_tail(), LogIndex(3));
    assert!(cell.primary_probe().is_err());
    let before = cell.recovery_state_token().unwrap();
    let mut attempted = Vec::new();
    assert!(
        stage
            .materialize_published(&cell, &mut |entry| {
                attempted.push(entry.prepare.index);
                if entry.prepare.index == LogIndex(3) {
                    Err(corrupt("current application write failed"))
                } else {
                    Ok(())
                }
            })
            .is_err()
    );
    assert_eq!(attempted, vec![LogIndex(1), LogIndex(2), LogIndex(3)]);
    assert!(stage.report().is_err());
    let mut applied = Vec::new();
    let report = stage
        .materialize_published(&cell, &mut |entry| {
            applied.push(entry.prepare.index);
            write_application(&path.join("current-application"), entry)
        })
        .unwrap();
    assert_eq!(applied, vec![LogIndex(1), LogIndex(2), LogIndex(3)]);
    assert_eq!(
        report.committed_tail,
        LogIndex(2),
        "receipt keeps its historical stage identity"
    );
    assert!(path.join("current-application/application/3").is_file());
    assert_eq!(stage.report().unwrap(), report);
    assert_eq!(stage.publication_quorum().unwrap(), Some(quorum));
    assert_eq!(
        fs::read(path.join("stage-1/PUBLICATION")).unwrap(),
        retained
    );
    assert_eq!(cell.recovery_state_token().unwrap(), before);
    assert_eq!(cell.view_change_promise(), Some(&promise));

    let unrelated = DurableVsrCell::open(
        path.join("unrelated"),
        PlacementGroupId(1),
        Epoch(1),
        NodeId(0),
        (0..3).map(NodeId),
    )
    .unwrap();
    let mut invoked = false;
    assert!(
        stage
            .materialize_published(&unrelated, &mut |_| {
                invoked = true;
                Ok(())
            })
            .is_err()
    );
    assert!(!invoked);
    assert!(
        stage.report().is_err(),
        "failed validation invalidates prior boot receipt"
    );
    drop(unrelated);
    drop(stage);
    drop(cell);
    fs::remove_dir_all(path).unwrap();
}
