//! The ledger callback is the existing in-process test seam, not native persistence.
use super::*;

fn handoff_fault(name: &str) {
    let f = Fixture::new("outer-custody-handoff");
    let mut owner = f.owner(VsrInitialRestriction::Reserved);
    let mut ledger = Ledger::new();
    let selected = f.copy(&mut owner, &mut ledger);
    let publication = owner
        .finish(&selected, &mut genesis, &mut |event| ledger.record(event))
        .unwrap();
    assert_eq!(ledger.events.len(), 3);
    assert_eq!(publication.source(), NodeId(0));
    let published = publication.into_published();
    published.published_state_token().unwrap();
    assert_eq!(published.committed_entries().unwrap().len(), 1);
    let path = if name == "DESCRIPTOR" {
        published.directory(&selected.history_id()).join(name)
    } else {
        published.root.join(name)
    };
    let original = guard::Exact::capture(&path, &fs::read(&path).unwrap()).unwrap();
    let bytes = fs::read(&path).unwrap();
    let saved = f.root.join("original-metadata");
    fs::rename(&path, &saved).unwrap();
    fs::write(&path, &bytes).unwrap();
    let error = published.published_state_token().unwrap_err();
    assert_eq!(error.code, ErrorCode::CorruptionDetected);
    assert_eq!(error.message, "learner publication metadata was replaced");
    fs::remove_file(&path).unwrap();
    fs::rename(saved, path).unwrap();
    original.validate().unwrap();
    assert_eq!(
        published.committed_entries().unwrap_err().message,
        "learner durability failed; reopen before further operations"
    );
    assert_eq!(
        owner.status().unwrap_err().message,
        "initial learner durability failed or owner consumed; reopen required"
    );
    assert_eq!(ledger.events.len(), 3);
    drop(published);
    drop(owner);
    let cold = LearnerHistoryStore::open_published_existing(
        f.root.join("learner"),
        f.policy.pg,
        f.policy.local,
        NodeId(0),
        f.policy.limits,
    )
    .unwrap();
    assert_eq!(cold.committed_entries().unwrap().len(), 1);
    drop(cold);
    let root = f.root.clone();
    drop(f);
    fs::remove_dir_all(root).unwrap();
}
#[test]
fn initial_consumed_publication_keeps_original_outer_lock_proof() {
    handoff_fault(".learner.lock");
}
#[test]
fn initial_consumed_publication_keeps_original_active_proof() {
    handoff_fault("ACTIVE");
}
#[test]
fn initial_consumed_publication_keeps_original_candidate_proof() {
    handoff_fault("DESCRIPTOR");
}
