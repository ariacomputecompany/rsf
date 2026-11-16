//! Initial ownership composition contracts over actual retained owners.
use super::*;

#[test]
fn replayed_initial_refusal_retains_unpublished_owner_and_original_transfer_guards() {
    let f = Fixture::new("replayed-refusal-custody");
    let mut owner = f.owner(VsrInitialRestriction::Reserved);
    let mut ledger = Ledger::new();
    let selection = f.copy(&mut owner, &mut ledger);
    let descriptor_path = owner.directory(&selection.history_id()).join("DESCRIPTOR");
    let descriptor_bytes = fs::read(&descriptor_path).unwrap();
    let mut original_token = None;
    for _ in 0..2 {
        let error = owner
            .finish(
                &selection,
                &mut |preview| {
                    genesis(preview)?;
                    Err(invalid_input("initial domain refuses before publication"))
                },
                &mut |event| ledger.record(event),
            )
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "initial domain refuses before publication"
        );
        assert_eq!(
            owner.status().unwrap().state(),
            VsrInitialState::Selected(selection)
        );
        assert_eq!(
            owner.status().unwrap().records(),
            f.snapshot.descriptor.records
        );
        assert!(!f.root.join("learner/ACTIVE").exists());
        assert_eq!(
            fs::read(f.root.join("learner/IDENTITY")).unwrap(),
            identity::provisional(&f.policy)
        );
        assert_eq!(fs::read(&descriptor_path).unwrap(), descriptor_bytes);
        assert_eq!(ledger.events, vec![VsrInitialState::Selected(selection)]);
        let Body::Replayed {
            published,
            transfer,
            token,
            ..
        } = &owner.candidate.as_ref().unwrap().body
        else {
            panic!("actual replayed owner must survive semantic refusal");
        };
        assert_eq!(transfer.id, selection.history_id());
        transfer.directory.validate().unwrap();
        transfer.flat.validate_cell(&published.cell).unwrap();
        transfer.descriptor.validate().unwrap();
        assert_eq!(published.cell.recovery_state_token().unwrap(), *token);
        if let Some(original) = &original_token {
            assert_eq!(
                token, original,
                "semantic retry must retain the same actual replayed cell"
            );
        } else {
            original_token = Some(token.clone());
        }
        let file = OpenOptions::new()
            .write(true)
            .open(published.cell.root.join(".vsr-writer.lock"))
            .unwrap();
        assert!(matches!(
            rsf_storage::ExclusiveFileLock::acquire(file),
            Err(std::fs::TryLockError::WouldBlock)
        ));
    }
    let before = match &owner.candidate.as_ref().unwrap().body {
        Body::Replayed { token, .. } => token.clone(),
        _ => unreachable!(),
    };
    let publication = owner
        .finish(&selection, &mut genesis, &mut |event| ledger.record(event))
        .unwrap();
    let source = publication.source();
    let published = publication.into_published();
    assert_eq!(
        published
            .published
            .as_ref()
            .unwrap()
            .cell
            .recovery_state_token()
            .unwrap(),
        before
    );
    assert_eq!(published.committed_entries().unwrap().len(), 1);
    assert_eq!(
        ledger.events,
        vec![
            VsrInitialState::Selected(selection),
            VsrInitialState::Publishing { selection, source },
            VsrInitialState::Published { selection, source },
        ]
    );
    drop(published);
    drop(owner);
    let root = f.root.clone();
    drop(f);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn initial_original_candidate_directory_loss_stays_fenced_after_exact_restoration() {
    for replayed in [false, true] {
        let f = Fixture::new("candidate-directory-restoration");
        let mut owner = f.owner(VsrInitialRestriction::Reserved);
        let mut ledger = Ledger::new();
        let selection = f.copy(&mut owner, &mut ledger);
        if replayed {
            let error = owner
                .finish(
                    &selection,
                    &mut |preview| {
                        genesis(preview)?;
                        Err(invalid_input("retain replayed initial owner"))
                    },
                    &mut |event| ledger.record(event),
                )
                .unwrap_err();
            assert_eq!(error.to_string(), "retain replayed initial owner");
            assert!(matches!(
                owner.candidate.as_ref().unwrap().body,
                Body::Replayed { .. }
            ));
        }
        let path = owner.directory(&selection.history_id());
        let original = guard::directory(&path).unwrap();
        let saved = f.root.join("original-candidate");
        fs::rename(&path, &saved).unwrap();
        fs::create_dir(&path).unwrap();
        assert_ne!(guard::directory(&path).unwrap(), original);
        let error = owner.status().unwrap_err();
        assert_eq!(error.message, "bounded learner original directory changed");
        fs::remove_dir(&path).unwrap();
        fs::rename(&saved, &path).unwrap();
        assert_eq!(guard::directory(&path).unwrap(), original);
        assert_eq!(
            owner.status().unwrap_err().message,
            "initial learner durability failed or owner consumed; reopen required"
        );
        assert_eq!(ledger.events, vec![VsrInitialState::Selected(selection)]);
        assert!(!f.root.join("learner/ACTIVE").exists());
        drop(owner);
        let mut cold = f.owner(VsrInitialRestriction::Selected(selection));
        assert_eq!(
            cold.status().unwrap().records(),
            f.snapshot.descriptor.records
        );
        let published = cold
            .finish(&selection, &mut genesis, &mut |event| ledger.record(event))
            .unwrap()
            .into_published();
        assert_eq!(
            published.published_history_id().unwrap(),
            Some(selection.history_id())
        );
        assert_eq!(published.committed_entries().unwrap().len(), 1);
        drop(published);
        drop(cold);
        let root = f.root.clone();
        drop(f);
        fs::remove_dir_all(root).unwrap();
    }
}
