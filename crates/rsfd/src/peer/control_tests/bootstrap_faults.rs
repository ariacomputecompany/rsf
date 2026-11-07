//! Real native mutation boundaries; no callback mock counts as persistence.
use super::bootstrap::*;
use super::*;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

#[test]
fn native_fresh_voter_publication_faults_fence_warm_owner_and_cold_use_actual_final_role() {
    for boundary in [
        Boundary::BeforeWrite,
        Boundary::BeforeFileSync,
        Boundary::BeforeRename,
        Boundary::AfterRename,
        Boundary::BeforeDirectorySync,
        Boundary::AfterDirectorySync,
    ] {
        let f = Fixture::new("fresh-publication-fault");
        configure(&f, 0);
        let (trusted, mut identity) = reserved(&f);
        let hit = Arc::new(AtomicBool::new(false));
        let witness = Arc::clone(&hit);
        let result = with_hook(
            move |at, _| {
                if at == boundary {
                    witness.store(true, Ordering::SeqCst);
                    return Err("injected native Initialized barrier".into());
                }
                Ok(())
            },
            || trusted.open_voter(&f.state, &mut identity),
        );
        assert_eq!(result.unwrap_err(), "injected native Initialized barrier");
        assert!(hit.load(Ordering::SeqCst));
        assert_eq!(
            identity.validate().unwrap_err(),
            "native identity durability failed; reopen required"
        );
        let renamed = matches!(
            boundary,
            Boundary::AfterRename | Boundary::BeforeDirectorySync | Boundary::AfterDirectorySync
        );
        assert_eq!(
            kind(&f.state),
            if renamed {
                "initialized_voter"
            } else {
                "reserved_voter"
            }
        );
        assert!(
            f.state.join("pg-0/MANIFEST").is_file(),
            "actual provisional store must precede native CAS"
        );
        drop(identity);
        let service = PeerService::open(&f.config).unwrap();
        assert_eq!(kind(&f.state), "initialized_voter");
        assert_eq!(service.server.control_primary().unwrap(), NodeId(0));
        assert!(service.server.committed_entries().unwrap().is_empty());
    }
}

#[test]
fn native_fresh_voter_unwind_fences_native_identity_without_exposing_a_voter() {
    let f = Fixture::new("fresh-publication-unwind");
    configure(&f, 0);
    let (trusted, mut identity) = reserved(&f);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        with_hook(
            |at, _| {
                if at == Boundary::BeforeRename {
                    panic!("actual native Initialized unwind");
                }
                Ok(())
            },
            || trusted.open_voter(&f.state, &mut identity),
        )
    }));
    let panic = result.unwrap_err();
    assert_eq!(
        panic.downcast_ref::<&str>(),
        Some(&"actual native Initialized unwind")
    );
    assert_eq!(
        identity.validate().unwrap_err(),
        "native identity durability failed; reopen required"
    );
    assert_eq!(kind(&f.state), "reserved_voter");
    assert!(f.state.join("IDENTITY.pending").is_file());
    drop(identity);
    drop(PeerService::open(&f.config).unwrap());
    assert_eq!(kind(&f.state), "initialized_voter");
}

#[test]
fn native_fresh_voter_retains_original_peer_lock_through_actual_completion() {
    let f = Fixture::new("fresh-completion-lock");
    configure(&f, 0);
    let (trusted, mut identity) = reserved(&f);
    let hit = Arc::new(AtomicBool::new(false));
    let witness = Arc::clone(&hit);
    let failure = with_hook(
        move |at, root| {
            if at == Boundary::BeforeFileSync && !witness.swap(true, Ordering::SeqCst) {
                fs::rename(root.join(".peer.lock"), root.join("original-peer-lock")).unwrap();
                fs::write(root.join(".peer.lock"), b"").unwrap();
            }
            Ok(())
        },
        || trusted.open_voter(&f.state, &mut identity),
    )
    .unwrap_err();
    assert!(hit.load(Ordering::SeqCst));
    assert_eq!(
        failure,
        "native identity held peer lock differs from its name"
    );
    fs::remove_file(f.state.join(".peer.lock")).unwrap();
    fs::rename(
        f.state.join("original-peer-lock"),
        f.state.join(".peer.lock"),
    )
    .unwrap();
    assert_eq!(
        identity.validate().unwrap_err(),
        "native identity durability failed; reopen required"
    );
    assert_eq!(kind(&f.state), "reserved_voter");
    drop(identity);
    drop(PeerService::open(&f.config).unwrap());
    assert_eq!(kind(&f.state), "initialized_voter");
}

#[test]
fn native_fresh_genesis_checks_native_custody_before_any_writer_mutation() {
    let f = Fixture::new("fresh-scheduler-custody");
    configure(&f, 0);
    let mut service = PeerService::open(&f.config).unwrap();
    let storage = snapshot(&f.state.join("pg-0"));
    fs::rename(
        f.state.join(".peer.lock"),
        f.state.join("original-peer-lock"),
    )
    .unwrap();
    fs::write(f.state.join(".peer.lock"), b"").unwrap();
    let failure = service
        .genesis
        .as_mut()
        .unwrap()
        .poll(
            service.local,
            service.control.as_ref().unwrap(),
            &mut service.state_identity,
            &mut service.server,
        )
        .unwrap_err();
    assert_eq!(
        failure,
        "native identity held peer lock differs from its name"
    );
    fs::remove_file(f.state.join(".peer.lock")).unwrap();
    fs::rename(
        f.state.join("original-peer-lock"),
        f.state.join(".peer.lock"),
    )
    .unwrap();
    assert_eq!(
        service.state_identity.validate().unwrap_err(),
        "native identity durability failed; reopen required"
    );
    assert_eq!(snapshot(&f.state.join("pg-0")), storage);
    assert!(service.server.committed_entries().unwrap().is_empty());
}

#[test]
fn native_fresh_completion_rechecks_original_kernel_lock_after_native_publication() {
    let f = Fixture::new("fresh-kernel-postpublication");
    configure(&f, 0);
    let (trusted, mut identity) = reserved(&f);
    let hit = Arc::new(AtomicBool::new(false));
    let witness = Arc::clone(&hit);
    let failure = with_hook(
        move |at, root| {
            if at == Boundary::AfterDirectorySync && !witness.swap(true, Ordering::SeqCst) {
                fs::rename(
                    root.join("pg-0/.vsr-writer.lock"),
                    root.join("original-vsr-lock"),
                )
                .unwrap();
                fs::write(root.join("pg-0/.vsr-writer.lock"), b"").unwrap();
            }
            Ok(())
        },
        || trusted.open_voter(&f.state, &mut identity),
    )
    .unwrap_err();
    assert!(hit.load(Ordering::SeqCst));
    // Native publication's completion check already detected and latched the
    // replaced kernel lock; finish_with must preserve that sticky failure.
    assert_eq!(
        failure,
        "provisional voter custody failed; cold reserved recovery required"
    );
    assert_eq!(kind(&f.state), "initialized_voter");
    fs::remove_file(f.state.join("pg-0/.vsr-writer.lock")).unwrap();
    fs::rename(
        f.state.join("original-vsr-lock"),
        f.state.join("pg-0/.vsr-writer.lock"),
    )
    .unwrap();
    assert_eq!(
        identity.validate().unwrap_err(),
        "native identity durability failed; reopen required"
    );
    drop(identity);
    // Actual durable identity survived; only strict cold recovery may return a voter.
    let service = PeerService::open(&f.config).unwrap();
    assert_eq!(service.server.control_primary().unwrap(), NodeId(0));
    assert!(service.server.committed_entries().unwrap().is_empty());
}

#[test]
fn native_fresh_genesis_rechecks_native_owner_after_actual_local_prepare() {
    let f = Fixture::new("fresh-scheduler-postprepare");
    configure(&f, 0);
    let mut service = PeerService::open(&f.config).unwrap();
    let segment = f.state.join("pg-0/segments/000000000000000001.segment");
    let before = fs::metadata(&segment).unwrap().len();
    let hit = Arc::new(AtomicBool::new(false));
    let witness = Arc::clone(&hit);
    let failure = with_hook(
        move |at, root| {
            if at == Boundary::BeforeValidate
                && fs::metadata(&segment).unwrap().len() > before
                && !witness.swap(true, Ordering::SeqCst)
            {
                fs::rename(root.join(".peer.lock"), root.join("original-peer-lock")).unwrap();
                fs::write(root.join(".peer.lock"), b"").unwrap();
            }
            Ok(())
        },
        || {
            service.genesis.as_mut().unwrap().poll(
                service.local,
                service.control.as_ref().unwrap(),
                &mut service.state_identity,
                &mut service.server,
            )
        },
    )
    .unwrap_err();
    assert!(
        hit.load(Ordering::SeqCst),
        "actual local WAL append must precede the injected custody loss"
    );
    assert_eq!(
        failure,
        "native identity held peer lock differs from its name"
    );
    fs::remove_file(f.state.join(".peer.lock")).unwrap();
    fs::rename(
        f.state.join("original-peer-lock"),
        f.state.join(".peer.lock"),
    )
    .unwrap();
    assert_eq!(
        service.state_identity.validate().unwrap_err(),
        "native identity durability failed; reopen required"
    );
    drop(service);
    let cell = DurableVsrCell::open_existing_bounded(
        f.state.join("pg-0"),
        PlacementGroupId(0),
        f.policy.limits,
    )
    .unwrap();
    assert_eq!(cell.next_index(), LogIndex(2));
    assert_eq!(cell.committed_tail(), LogIndex(0));
    let rsf_vsr::VsrOperation::ReplicatedCommand { payload, .. } =
        &cell.prepared(LogIndex(1)).unwrap().operation
    else {
        panic!("accepted Genesis not a command");
    };
    assert_eq!(
        ControlCommand::decode(payload).unwrap(),
        ControlCommand::Genesis(f.policy.genesis.clone())
    );
}
