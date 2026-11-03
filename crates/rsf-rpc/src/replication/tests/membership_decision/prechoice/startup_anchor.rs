//! Same-operation proof handoff; no approval survives in the server cache.
use super::*;
use std::os::unix::fs::PermissionsExt;
const PG: PlacementGroupId = PlacementGroupId(1);

fn initial(server: &mut PeerReplicaServer) -> VsrMembershipApproval {
    let approval = server.membership_decisions[&PG].authority().unwrap();
    server.initialize_membership_decision(&approval).unwrap();
    server.membership_recovery_learner_anchors.clear();
    approval
}

#[test]
fn managed_startup_handoff_reuses_exact_a_without_rereading_its_wal() {
    let fixture = accepted_fixture();
    let (mut server, _source) = super::inbound_custody::managed_learner(&fixture);
    let approval = initial(&mut server);
    let before = server.membership_decisions[&PG].decision_state().unwrap();
    assert!(before.chosen.is_none());
    let id = VsrMembershipInput::from_approval(&approval)
        .unwrap()
        .id()
        .unwrap();
    assert_eq!(id, before.authority);
    let directory = fixture
        .root
        .join("inbound-membership/decision/inputs")
        .join(id_name(id))
        .join("records/segments");
    let segment = std::fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.extension() == Some(std::ffi::OsStr::new("segment")))
        .unwrap();
    let permissions = std::fs::metadata(&segment).unwrap().permissions();
    std::fs::set_permissions(&segment, std::fs::Permissions::from_mode(0o000)).unwrap();
    std::fs::File::open(&segment).expect_err("qualification requires an unprivileged Unix host");
    assert!(
        server.membership_decisions[&PG].approval(id).is_err(),
        "oracle did not deny an actual proof replay"
    );
    let result = server.validate_managed_membership_anchor_with_authority(PG, Some(&approval));
    std::fs::set_permissions(&segment, permissions).unwrap();
    result.unwrap();
    assert!(server.membership_recovery_learner_anchors.contains_key(&PG));
    assert_eq!(
        server.membership_decisions[&PG].decision_state().unwrap(),
        before
    );
    server.validate_managed_membership_anchor(PG).unwrap();
}

#[test]
fn managed_startup_handoff_cannot_substitute_a_for_distinct_chosen_b() {
    let fixture = accepted_fixture();
    let (mut server, _source) = super::inbound_custody::managed_learner(&fixture);
    server
        .membership_decisions
        .get_mut(&PG)
        .unwrap()
        .retain_chosen_input(fixture.plan.chosen().clone(), fixture.plan.approval())
        .unwrap();
    let approval = initial(&mut server);
    let state = server.membership_decisions[&PG].decision_state().unwrap();
    let selected = state.chosen.unwrap().1;
    assert_ne!(state.authority, selected);
    let manifest = fixture
        .root
        .join("inbound-membership/decision/inputs")
        .join(id_name(selected))
        .join("records/MANIFEST");
    let saved = fixture.root.join("selected-manifest");
    std::fs::rename(&manifest, &saved).unwrap();
    assert!(
        server
            .validate_managed_membership_anchor_with_authority(PG, Some(&approval))
            .is_err()
    );
    assert!(!server.membership_recovery_learner_anchors.contains_key(&PG));
    assert!(!manifest.exists(), "handoff repaired missing B");
    assert_eq!(
        server.membership_decisions[&PG].decision_state().unwrap(),
        state
    );
    std::fs::rename(saved, manifest).unwrap();
    server
        .validate_managed_membership_anchor_with_authority(PG, Some(&approval))
        .unwrap();
    assert!(server.membership_recovery_learner_anchors.contains_key(&PG));
    server.validate_managed_membership_anchor(PG).unwrap();
}
