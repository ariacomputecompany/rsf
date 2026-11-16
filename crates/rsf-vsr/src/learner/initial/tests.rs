//! New API contracts over actual VSR/segment owners; no old-API red claim.
use super::*;
use std::sync::atomic::AtomicU64;

fn root(label: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    loop {
        let path = std::env::temp_dir().join(format!(
            "rsf-initial-{label}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        match fs::create_dir(&path) {
            Ok(()) => return path,
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => panic!("fixture reservation: {e}"),
        }
    }
}
struct Fixture {
    root: PathBuf,
    source: DurableVsrCell,
    policy: VsrInitialPolicy,
    snapshot: VsrHistorySnapshot,
}
impl Fixture {
    fn new(label: &str) -> Self {
        let root = root(label);
        let mut source = DurableVsrCell::open(
            root.join("source"),
            PlacementGroupId(1),
            Epoch(1),
            NodeId(0),
            [NodeId(0), NodeId(1), NodeId(2)],
        )
        .unwrap();
        let VsrDecision::Prepared(prepare) = source
            .prepare(
                NodeId(0),
                VsrOperation::ReplicatedCommand {
                    operation_id: OperationId(1),
                    payload: b"Genesis".to_vec(),
                },
                7,
            )
            .unwrap()
        else {
            panic!("prepare");
        };
        source.acknowledge(NodeId(1), prepare.view, prepare.index);
        assert!(matches!(
            source.commit(NodeId(0), prepare.index).unwrap(),
            VsrDecision::Committed(_)
        ));
        let policy = VsrInitialPolicy {
            pg: PlacementGroupId(1),
            local: NodeId(3),
            bootstrap: source.membership_lineage().unwrap().bootstrap,
            limits: VsrPrefixLimits {
                max_records: 128,
                max_encoded_bytes: 1024 * 1024,
            },
        };
        let snapshot = source.history_snapshot().unwrap();
        Self {
            root,
            source,
            policy,
            snapshot,
        }
    }
    fn owner(&self, restriction: VsrInitialRestriction) -> InitialLearnerHistoryStore {
        InitialLearnerHistoryStore::open(self.root.join("learner"), self.policy, restriction)
            .unwrap()
    }
    fn copy(
        &self,
        owner: &mut InitialLearnerHistoryStore,
        ledger: &mut Ledger,
    ) -> VsrInitialSelection {
        let selected = owner
            .begin(
                VsrInitialBegin::Start(self.snapshot.descriptor.clone()),
                &mut |event| ledger.record(event),
            )
            .unwrap()
            .selection();
        let mut ordinal = owner.status().unwrap().records();
        while ordinal < self.snapshot.descriptor.records {
            let batch =
                VsrHistoryBatch::pack(ordinal, &self.snapshot.records[ordinal as usize..]).unwrap();
            ordinal = owner.append(&selected, &batch).unwrap().records();
        }
        selected
    }
}
struct Ledger {
    state: VsrInitialState,
    events: Vec<VsrInitialState>,
}
impl Ledger {
    fn new() -> Self {
        Self {
            state: VsrInitialState::Reserved,
            events: Vec::new(),
        }
    }
    fn record(&mut self, event: &VsrInitialTransition<'_>) -> RsfResult<()> {
        assert_eq!(event.expected(), self.state);
        self.state = event.next();
        self.events.push(self.state);
        Ok(())
    }
}
fn genesis(preview: &VsrImportPreview<'_>) -> RsfResult<()> {
    assert_eq!(preview.committed_tail(), LogIndex(1));
    assert_eq!(preview.next_index(), LogIndex(2));
    let mut observed = 0;
    preview.for_each_committed(|prepare, commit| {
        assert_eq!(prepare.index, commit.index);
        assert!(matches!(&prepare.operation, VsrOperation::ReplicatedCommand { payload, .. } if payload == b"Genesis"));
        observed += 1; Ok(())
    })?;
    assert_eq!(observed, 1);
    preview.validate_live()
}
fn sidecar(owner: &InitialLearnerHistoryStore, selected: VsrInitialSelection) -> PathBuf {
    fs::read_dir(owner.directory(&selected.history_id).join("cell/segments"))
        .unwrap()
        .map(Result::unwrap)
        .map(|e| e.path())
        .find(|p| p.extension().is_some_and(|s| s == "lock"))
        .unwrap()
}
#[test]
fn initial_selection_precedes_cursor_and_same_owner_publication_handoff() {
    let f = Fixture::new("publication");
    let mut owner = f.owner(VsrInitialRestriction::Reserved);
    let mut ledger = Ledger::new();
    let selected = f.copy(&mut owner, &mut ledger);
    assert_eq!(ledger.events, [VsrInitialState::Selected(selected)]);
    assert!(!f.root.join("learner/ACTIVE").exists());
    let publication = owner
        .finish(&selected, &mut genesis, &mut |event| {
            if matches!(event.next(), VsrInitialState::Publishing { .. }) {
                assert!(!f.root.join("learner/ACTIVE").exists());
                assert_eq!(
                    fs::read(f.root.join("learner/IDENTITY")).unwrap(),
                    identity::provisional(&f.policy)
                );
            }
            if matches!(event.next(), VsrInitialState::Published { .. }) {
                assert_eq!(
                    fs::read(f.root.join("learner/ACTIVE")).unwrap(),
                    selected.history_id
                );
                assert!(
                    LearnerHistoryStore::open_published_existing(
                        f.root.join("learner"),
                        f.policy.pg,
                        f.policy.local,
                        NodeId(0),
                        f.policy.limits
                    )
                    .is_err(),
                    "actual learner lock remains held during final native barrier"
                );
            }
            ledger.record(event)
        })
        .unwrap();
    assert_eq!(ledger.events.len(), 3);
    assert_eq!(publication.source(), NodeId(0));
    assert!(
        owner.status().is_err(),
        "consumed provisional owner cannot be reused"
    );
    let published = publication.into_published();
    assert_eq!(
        published.published_history_id().unwrap(),
        Some(selected.history_id)
    );
    assert_eq!(published.committed_entries().unwrap().len(), 1);
}
#[test]
fn selected_ack_zero_never_repairs_missing_sidecar_warm_or_cold() {
    for warm in [false, true] {
        let f = Fixture::new(if warm { "warm-zero" } else { "cold-zero" });
        let mut owner = f.owner(VsrInitialRestriction::Reserved);
        let mut ledger = Ledger::new();
        let selected = owner
            .begin(
                VsrInitialBegin::Start(f.snapshot.descriptor.clone()),
                &mut |e| ledger.record(e),
            )
            .unwrap()
            .selection();
        let path = sidecar(&owner, selected);
        if warm {
            fs::remove_file(&path).unwrap();
            assert!(
                owner
                    .begin(
                        VsrInitialBegin::Resume {
                            selection: selected,
                            descriptor: f.snapshot.descriptor.clone()
                        },
                        &mut |e| ledger.record(e)
                    )
                    .is_err()
            );
            assert!(owner.status().is_err());
            drop(owner);
        } else {
            drop(owner);
            fs::remove_file(&path).unwrap();
        }
        assert!(
            InitialLearnerHistoryStore::open(
                f.root.join("learner"),
                f.policy,
                VsrInitialRestriction::Selected(selected)
            )
            .is_err()
        );
        assert!(
            !path.exists(),
            "strict selected open must not recreate ACK0 sidecar"
        );
    }
}
#[test]
fn uncertain_selection_error_is_sticky_after_original_identity_restored() {
    let f = Fixture::new("select-error");
    let mut owner = f.owner(VsrInitialRestriction::Reserved);
    let mut ledger = Ledger::new();
    let identity_path = f.root.join("learner/IDENTITY");
    let saved = f.root.join("saved-identity");
    assert!(
        owner
            .begin(
                VsrInitialBegin::Start(f.snapshot.descriptor.clone()),
                &mut |event| {
                    ledger.record(event)?;
                    fs::rename(&identity_path, &saved).unwrap();
                    Err(invalid_input(
                        "injected native callback error after its state changed",
                    ))
                }
            )
            .is_err()
    );
    fs::rename(saved, identity_path).unwrap();
    assert!(
        owner.status().is_err(),
        "restoring original inode must not clear observed failure"
    );
    drop(owner);
    let VsrInitialState::Selected(selection) = ledger.state else {
        panic!("selection persisted");
    };
    assert_eq!(
        f.owner(VsrInitialRestriction::Selected(selection))
            .status()
            .unwrap()
            .records(),
        0
    );
}
#[test]
fn actual_pin_error_cold_resume_requires_same_complete_candidate() {
    let f = Fixture::new("pin-error");
    let mut owner = f.owner(VsrInitialRestriction::Reserved);
    let mut ledger = Ledger::new();
    let selection = f.copy(&mut owner, &mut ledger);
    assert!(
        owner
            .finish(&selection, &mut genesis, &mut |event| {
                ledger.record(event)?;
                if matches!(event.next(), VsrInitialState::Publishing { .. }) {
                    return Err(invalid_input("pin persisted; reply lost"));
                }
                Ok(())
            })
            .is_err()
    );
    assert!(!f.root.join("learner/ACTIVE").exists());
    drop(owner);
    let mut owner = f.owner(VsrInitialRestriction::Publishing {
        selection,
        source: NodeId(0),
    });
    assert!(matches!(
        owner.begin(
            VsrInitialBegin::Start(f.snapshot.descriptor.clone()),
            &mut |e| ledger.record(e)
        ),
        Err(VsrInitialError::PublicationPinned)
    ));
    let published = owner
        .finish(&selection, &mut genesis, &mut |e| ledger.record(e))
        .unwrap()
        .into_published();
    assert_eq!(
        published.published_history_id().unwrap(),
        Some(selection.history_id)
    );
    assert_eq!(
        ledger
            .events
            .iter()
            .filter(|state| matches!(state, VsrInitialState::Publishing { .. }))
            .count(),
        1
    );
}
#[test]
fn missing_pinned_candidate_and_missing_published_active_never_reinitialize() {
    let f = Fixture::new("missing-pin");
    let mut owner = f.owner(VsrInitialRestriction::Reserved);
    let mut ledger = Ledger::new();
    let selection = f.copy(&mut owner, &mut ledger);
    assert!(
        owner
            .finish(&selection, &mut genesis, &mut |event| {
                ledger.record(event)?;
                Err(invalid_input("stop after pin"))
            })
            .is_err()
    );
    let candidate = owner.directory(&selection.history_id);
    drop(owner);
    fs::remove_dir_all(&candidate).unwrap();
    assert!(
        InitialLearnerHistoryStore::open(
            f.root.join("learner"),
            f.policy,
            VsrInitialRestriction::Publishing {
                selection,
                source: NodeId(0)
            }
        )
        .is_err()
    );
    assert!(!candidate.exists());
    let f = Fixture::new("missing-active");
    let mut owner = f.owner(VsrInitialRestriction::Reserved);
    let mut ledger = Ledger::new();
    let selection = f.copy(&mut owner, &mut ledger);
    let published = owner
        .finish(&selection, &mut genesis, &mut |e| ledger.record(e))
        .unwrap()
        .into_published();
    drop(published);
    fs::remove_file(f.root.join("learner/ACTIVE")).unwrap();
    assert!(
        LearnerHistoryStore::open_published_existing(
            f.root.join("learner"),
            f.policy.pg,
            f.policy.local,
            NodeId(0),
            f.policy.limits
        )
        .is_err()
    );
    assert!(!f.root.join("learner/ACTIVE").exists());
}
#[test]
fn clean_semantic_rejection_then_aba_replacement_rejects_old_generations() {
    let mut f = Fixture::new("replace");
    let mut owner = f.owner(VsrInitialRestriction::Reserved);
    let mut ledger = Ledger::new();
    let a = f.copy(&mut owner, &mut ledger);
    assert!(
        owner
            .finish(
                &a,
                &mut |preview| {
                    genesis(preview)?;
                    Err(invalid_input("clean domain rejection"))
                },
                &mut |e| ledger.record(e)
            )
            .is_err()
    );
    assert!(owner.status().is_ok());
    assert!(!f.root.join("learner/ACTIVE").exists());
    assert!(matches!(
        f.source
            .prepare(
                NodeId(0),
                VsrOperation::ReplicatedCommand {
                    operation_id: OperationId(2),
                    payload: b"accepted tail".to_vec()
                },
                13
            )
            .unwrap(),
        VsrDecision::Prepared(_)
    ));
    let b_descriptor = f.source.history_snapshot().unwrap().descriptor;
    let b = owner
        .begin(
            VsrInitialBegin::Replace {
                expected: a,
                descriptor: b_descriptor.clone(),
            },
            &mut |e| ledger.record(e),
        )
        .unwrap()
        .selection();
    let a3 = owner
        .begin(
            VsrInitialBegin::Replace {
                expected: b,
                descriptor: f.snapshot.descriptor.clone(),
            },
            &mut |e| ledger.record(e),
        )
        .unwrap()
        .selection();
    assert_eq!(a3.history_id, a.history_id);
    assert_eq!(a3.generation, a.generation + 2);
    assert!(matches!(
        owner.begin(
            VsrInitialBegin::Replace {
                expected: a,
                descriptor: b_descriptor
            },
            &mut |e| ledger.record(e)
        ),
        Err(VsrInitialError::SelectionConflict { .. })
    ));
    let first = VsrHistoryBatch::pack(0, &f.snapshot.records).unwrap();
    assert!(matches!(
        owner.append(&a, &first),
        Err(VsrInitialError::SelectionConflict { .. })
    ));
    assert!(matches!(
        owner.finish(&a, &mut genesis, &mut |e| ledger.record(e)),
        Err(VsrInitialError::SelectionConflict { .. })
    ));
    assert!(reclaim::inventory(&owner).unwrap().len() <= 2);
    let mut count = 0;
    while count < f.snapshot.descriptor.records {
        let batch = VsrHistoryBatch::pack(count, &f.snapshot.records[count as usize..]).unwrap();
        count = owner.append(&a3, &batch).unwrap().records();
    }
    assert!(
        owner
            .finish(&a3, &mut genesis, &mut |e| ledger.record(e))
            .is_ok()
    );
}
#[test]
fn callback_panic_keeps_initial_owner_fenced_without_active() {
    let f = Fixture::new("panic");
    let mut owner = f.owner(VsrInitialRestriction::Reserved);
    let mut ledger = Ledger::new();
    let selection = f.copy(&mut owner, &mut ledger);
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        owner.finish(
            &selection,
            &mut |preview| {
                genesis(preview)?;
                panic!("validator panic")
            },
            &mut |e| ledger.record(e),
        )
    }));
    assert!(panic.is_err());
    assert!(owner.status().is_err());
    assert!(!f.root.join("learner/ACTIVE").exists());
}
#[test]
fn selected_cleanup_refuses_unknown_or_alias_and_preserves_current() {
    for alias in [false, true] {
        let f = Fixture::new(if alias { "alias" } else { "unknown" });
        let mut owner = f.owner(VsrInitialRestriction::Reserved);
        let mut ledger = Ledger::new();
        let selection = f.copy(&mut owner, &mut ledger);
        let before = fs::read(owner.directory(&selection.history_id).join("DESCRIPTOR")).unwrap();
        let outside = f.root.join("outside");
        fs::create_dir(&outside).unwrap();
        fs::write(outside.join("untouched"), b"outside").unwrap();
        if alias {
            #[cfg(unix)]
            std::os::unix::fs::symlink(&outside, owner.directory(&[17; 32])).unwrap();
        } else {
            fs::write(f.root.join("learner/unknown"), b"unknown").unwrap();
        }
        assert!(
            owner
                .finish(&selection, &mut genesis, &mut |e| ledger.record(e))
                .is_err()
        );
        assert_eq!(
            fs::read(owner.directory(&selection.history_id).join("DESCRIPTOR")).unwrap(),
            before
        );
        assert_eq!(fs::read(outside.join("untouched")).unwrap(), b"outside");
        assert_eq!(ledger.events, [VsrInitialState::Selected(selection)]);
    }
}

#[test]
fn final_restriction_error_never_returns_owner_and_cold_resume_obeys_actual_native_state() {
    for recorded in [false, true] {
        let f = Fixture::new(if recorded {
            "final-recorded"
        } else {
            "final-not-recorded"
        });
        let mut owner = f.owner(VsrInitialRestriction::Reserved);
        let mut ledger = Ledger::new();
        let selected = f.copy(&mut owner, &mut ledger);
        let error = owner
            .finish(&selected, &mut genesis, &mut |event| {
                if matches!(event.next(), VsrInitialState::Published { .. }) {
                    if recorded {
                        ledger.record(event)?;
                    }
                    return Err(io_error(std::io::Error::other(
                        "uncertain final restriction",
                    )));
                }
                ledger.record(event)
            })
            .unwrap_err();
        assert!(error.to_string().contains("uncertain final restriction"));
        assert_eq!(
            fs::read(f.root.join("learner/ACTIVE")).unwrap(),
            selected.history_id()
        );
        assert!(
            owner
                .status()
                .unwrap_err()
                .message
                .contains("initial learner durability failed")
        );
        drop(owner);
        if recorded {
            assert_eq!(
                ledger.state,
                VsrInitialState::Published {
                    selection: selected,
                    source: NodeId(0)
                }
            );
            let published = LearnerHistoryStore::open_published_existing(
                f.root.join("learner"),
                f.policy.pg,
                f.policy.local,
                NodeId(0),
                f.policy.limits,
            )
            .unwrap();
            assert_eq!(
                published.published_history_id().unwrap(),
                Some(selected.history_id())
            );
            drop(published);
        } else {
            assert_eq!(
                ledger.state,
                VsrInitialState::Publishing {
                    selection: selected,
                    source: NodeId(0)
                }
            );
            let mut owner = f.owner(VsrInitialRestriction::Publishing {
                selection: selected,
                source: NodeId(0),
            });
            let published = owner
                .finish(&selected, &mut genesis, &mut |event| ledger.record(event))
                .unwrap();
            assert_eq!(
                ledger
                    .events
                    .iter()
                    .filter(|state| matches!(state, VsrInitialState::Publishing { .. }))
                    .count(),
                1,
                "cold completion must not create another pin transaction"
            );
            drop(published.into_published());
            drop(owner);
        }
        let root = f.root.clone();
        drop(f);
        fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn initial_replacement_generation_overflow_has_no_effect_and_preserves_exact_selected_owner() {
    let mut f = Fixture::new("generation-overflow");
    let mut owner = f.owner(VsrInitialRestriction::Reserved);
    let mut ledger = Ledger::new();
    let selected = f.copy(&mut owner, &mut ledger);
    drop(owner);
    let maximal = VsrInitialSelection::new(u64::MAX, selected.history_id()).unwrap();
    // Boundary contract: trusted native restriction supplies the checked counter;
    // the fixture does not claim to execute u64::MAX preceding transactions.
    let mut owner = f.owner(VsrInitialRestriction::Selected(maximal));
    let VsrDecision::Prepared(_) = f
        .source
        .prepare(
            NodeId(0),
            VsrOperation::ReplicatedCommand {
                operation_id: OperationId(2),
                payload: b"tail".to_vec(),
            },
            4,
        )
        .unwrap()
    else {
        panic!("accepted suffix");
    };
    let alternate = f.source.history_snapshot().unwrap().descriptor;
    assert_ne!(alternate.id().unwrap(), maximal.history_id());
    let before = owner.status().unwrap();
    let mut called = false;
    let error = owner
        .begin(
            VsrInitialBegin::Replace {
                expected: maximal,
                descriptor: alternate,
            },
            &mut |_| {
                called = true;
                Ok(())
            },
        )
        .unwrap_err();
    assert!(error.to_string().contains("generation overflow"));
    assert!(!called);
    assert_eq!(owner.status().unwrap(), before);
    assert!(!f.root.join("learner/ACTIVE").exists());
    let names = reclaim::inventory(&owner).unwrap();
    assert_eq!(names, vec![selected.history_id()]);
    drop(owner);
    let root = f.root.clone();
    drop(f);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn selected_cleanup_crash_retries_same_requested_id_without_third_candidate_or_aba_alias() {
    for boundary in [
        observation::Boundary::StorageChildren,
        observation::Boundary::Cell,
        observation::Boundary::Descriptor,
    ] {
        let mut f = Fixture::new("cleanup-crash");
        let mut owner = f.owner(VsrInitialRestriction::Reserved);
        let mut ledger = Ledger::new();
        let a = f.copy(&mut owner, &mut ledger);
        let VsrDecision::Prepared(_) = f
            .source
            .prepare(
                NodeId(0),
                VsrOperation::ReplicatedCommand {
                    operation_id: OperationId(2),
                    payload: b"retained suffix".to_vec(),
                },
                15,
            )
            .unwrap()
        else {
            panic!("actual second descriptor");
        };
        let b_descriptor = f.source.history_snapshot().unwrap().descriptor;
        let b_id = b_descriptor.id().unwrap();
        assert_ne!(a.history_id(), b_id);
        let original = f.snapshot.descriptor.clone();
        let watch = observation::fault(boundary, move |old| {
            match boundary {
                observation::Boundary::StorageChildren => {
                    assert!(old.join("cell/MANIFEST").is_file());
                    assert!(old.join("DESCRIPTOR").is_file());
                }
                observation::Boundary::Cell => {
                    assert!(!old.join("cell").exists());
                    assert!(old.join("DESCRIPTOR").is_file());
                }
                observation::Boundary::Descriptor => {
                    assert!(!old.join("cell").exists());
                    assert!(!old.join("DESCRIPTOR").exists());
                }
            }
            Err(io_error(std::io::Error::other(
                "interrupted initial disposal",
            )))
        });
        let error = owner
            .begin(
                VsrInitialBegin::Replace {
                    expected: a,
                    descriptor: b_descriptor,
                },
                &mut |event| ledger.record(event),
            )
            .unwrap_err();
        assert!(error.to_string().contains("interrupted initial disposal"));
        assert_eq!(watch.calls(), 1);
        let VsrInitialState::Selected(b) = ledger.state else {
            panic!("actual restrictive B selection precedes A cleanup");
        };
        assert_eq!(b.generation(), 2);
        assert_eq!(b.history_id(), b_id);
        let b_path = owner.directory(&b_id);
        let b_metadata = fs::read(b_path.join("DESCRIPTOR")).unwrap();
        assert!(owner.status().is_err());
        drop(owner);
        drop(watch);
        let mut owner = f.owner(VsrInitialRestriction::Selected(b));
        assert_eq!(owner.status().unwrap().records(), 0);
        assert_eq!(reclaim::inventory(&owner).unwrap().len(), 2);
        assert_eq!(fs::read(b_path.join("DESCRIPTOR")).unwrap(), b_metadata);
        // Desired A is the SAME id as the partially deleted nonselected A.
        // Complete authorized disposal before preparing A at fresh generation3.
        let cursor = owner
            .begin(
                VsrInitialBegin::Replace {
                    expected: b,
                    descriptor: original.clone(),
                },
                &mut |event| ledger.record(event),
            )
            .unwrap();
        let again = cursor.selection();
        assert_eq!(again.generation(), 3);
        assert_eq!(again.history_id(), a.history_id());
        assert_eq!(cursor.records(), 0);
        assert_eq!(reclaim::inventory(&owner).unwrap(), vec![a.history_id()]);
        let count = ledger.events.len();
        assert!(
            matches!(owner.begin(VsrInitialBegin::Resume { selection: a, descriptor: original.clone() },
            &mut |event| ledger.record(event)), Err(VsrInitialError::SelectionConflict { actual: VsrInitialState::Selected(s) }) if s == again)
        );
        assert!(
            matches!(owner.finish(&a, &mut genesis, &mut |event| ledger.record(event)),
            Err(VsrInitialError::SelectionConflict { actual: VsrInitialState::Selected(s) }) if s == again)
        );
        assert_eq!(ledger.events.len(), count);
        let retry = owner
            .begin(
                VsrInitialBegin::Replace {
                    expected: b,
                    descriptor: original,
                },
                &mut |event| ledger.record(event),
            )
            .unwrap();
        assert_eq!(retry.selection(), again);
        assert_eq!(retry.records(), 0);
        assert_eq!(
            ledger.events.len(),
            count,
            "lost replacement reply is idempotent at its actual generation"
        );
        assert!(!f.root.join("learner/ACTIVE").exists());
        drop(owner);
        let root = f.root.clone();
        drop(f);
        fs::remove_dir_all(root).unwrap();
    }
}

mod composition;

mod outer_custody;
