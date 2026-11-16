//! Actual delivery/source ownership; fault injections restore original inodes.
use super::*;

fn rotate_source(f: &mut Fixture) {
    // Reopen only this acknowledged fixture owner with smaller legitimate
    // storage rotation policy before any writer/delivery observation is made.
    drop(f.control.remove(&NodeId(0)).unwrap());
    let cell = DurableVsrCell::open_with_store_options(
        f.root.join("pg-0/node-0"),
        CONTROL,
        Epoch(1),
        NodeId(0),
        (0..3).map(NodeId),
        rsf_storage::SegmentStoreOptions {
            log_options: rsf_storage::SegmentLogOptions {
                sync_policy: rsf_storage::SyncPolicy::Full,
            },
            max_segment_bytes: 1,
        },
    )
    .unwrap();
    f.control.insert(NodeId(0), cell);
}
pub(super) fn source(
    f: &mut Fixture,
) -> (PeerReplicaServer, Running, ObservedControlGenesisDelivery) {
    let one = Running::start(voter(f, 1));
    let mut writer = voter(f, 0);
    writer
        .control_transport
        .peers
        .get_mut(&NodeId(1))
        .unwrap()
        .address = one.address;
    writer.request_control_genesis().unwrap();
    let observed = delivery(&mut writer);
    (writer, one, observed)
}
fn physical(error: ControlInitialSourceError) -> rsf_core::RsfError {
    match error {
        ControlInitialSourceError::Failure(error) => error,
        other => panic!("expected actual physical owner failure, got {other:?}"),
    }
}
#[test]
fn actual_initial_source_latches_closed_file_and_held_lock_loss_after_original_inode_restore() {
    for lock in [false, true] {
        let mut f = Fixture::new(if lock {
            "initial-held-lock"
        } else {
            "initial-closed-file"
        });
        rotate_source(&mut f);
        let (writer, one, observed) = source(&mut f);
        let p = policy(&f);
        let export = writer
            .export_initial_control_source(&observed, p.limits)
            .unwrap();
        assert!(
            !writer
                .read_initial_control_batch(&export, 0)
                .unwrap()
                .records
                .is_empty()
        );
        let cell = f.root.join("pg-0/node-0");
        let mut segments = std::fs::read_dir(cell.join("segments"))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.extension().is_some_and(|ext| ext == "segment"))
            .collect::<Vec<_>>();
        segments.sort();
        assert!(
            segments.len() >= 3,
            "actual bootstrap/prepare/commit rotations must exist"
        );
        let path = if lock {
            cell.join("MANIFEST.lock")
        } else {
            segments[0].clone()
        };
        let saved = f.root.join("original-source-file");
        let contents = std::fs::read(&path).unwrap();
        #[cfg(unix)]
        let inode = {
            use std::os::unix::fs::MetadataExt;
            std::fs::metadata(&path).unwrap().ino()
        };
        std::fs::rename(&path, &saved).unwrap();
        let first = if lock {
            writer
                .export_initial_control_source(&observed, p.limits)
                .unwrap_err()
        } else {
            writer.read_initial_control_batch(&export, 0).unwrap_err()
        };
        physical(first);
        assert!(
            !path.exists(),
            "failed source validation must not recreate acknowledged custody"
        );
        std::fs::rename(&saved, &path).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), contents);
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            assert_eq!(std::fs::metadata(&path).unwrap().ino(), inode);
        }
        // Restoring the same inode rules out a second replacement mismatch as
        // the reason for rejection; the first observed loss fenced the owner.
        for error in [
            writer.read_initial_control_batch(&export, 0).unwrap_err(),
            writer
                .export_initial_control_source(&observed, p.limits)
                .unwrap_err(),
        ] {
            let error = physical(error);
            assert!(error.message.contains("VSR durability failed"), "{error:?}");
        }
        assert_eq!(writer.cells[&CONTROL].committed_tail(), LogIndex(1));
        drop(one.finish());
        drop(writer);
        f.cleanup();
    }
}
#[test]
fn actual_initial_delivery_and_export_cannot_be_used_on_another_healthy_owner() {
    let mut first = Fixture::new("initial-owner-a");
    let mut second = Fixture::new("initial-owner-b");
    // Match real provisioning exactly so foreign-owner refusal cannot pass just
    // because the independent fixture generated another certificate/catalog.
    let mut old = second.server.take().unwrap();
    let cells = std::mem::take(&mut old.cells);
    drop(old);
    for node in 0..4 {
        for name in ["certificate.pem", "key.pem", "ca.pem"] {
            std::fs::copy(
                first.root.join(format!("tls-{node}/{name}")),
                second.root.join(format!("tls-{node}/{name}")),
            )
            .unwrap();
        }
    }
    second.peers = first.peers.clone();
    second.catalog = first.catalog.clone();
    second.anchor = first.anchor.clone();
    second.server = Some(
        PeerReplicaServer::new(
            NodeId(1),
            &cluster(),
            identity(&second.root, 1),
            second.peers.clone(),
            cells,
            Duration::from_millis(800),
        )
        .unwrap()
        .with_control_replication(second.anchor.clone(), second.catalog.clone())
        .unwrap(),
    );
    let (a, peer_a, observed_a) = source(&mut first);
    let (b, peer_b, observed_b) = source(&mut second);
    let a_policy = policy(&first);
    let b_policy = policy(&second);
    assert_eq!(a_policy.anchor, b_policy.anchor);
    assert_eq!(a_policy.genesis, b_policy.genesis);
    let a_export = a
        .export_initial_control_source(&observed_a, a_policy.limits)
        .unwrap();
    let b_export = b
        .export_initial_control_source(&observed_b, b_policy.limits)
        .unwrap();
    let token = b.cells[&CONTROL].recovery_state_token().unwrap();
    assert!(matches!(
        b.export_initial_control_source(&observed_a, b_policy.limits),
        Err(ControlInitialSourceError::Ownership(_))
    ));
    assert!(matches!(
        b.read_initial_control_batch(&a_export, 0),
        Err(ControlInitialSourceError::Ownership(_))
    ));
    assert_eq!(b.cells[&CONTROL].recovery_state_token().unwrap(), token);
    assert!(
        !b.read_initial_control_batch(&b_export, 0)
            .unwrap()
            .records
            .is_empty()
    );
    drop(peer_a.finish());
    drop(peer_b.finish());
    drop(a);
    drop(b);
    first.cleanup();
    second.cleanup();
}
#[test]
fn initial_control_only_owner_checks_complete_policy_before_creating_a_root() {
    let f = Fixture::new("initial-control-only");
    let p = policy(&f);
    // Dedicated ownership reserves one PG; trusted Genesis remains complete.
    assert_eq!(p.genesis.groups.len(), 2);
    assert_eq!(p.genesis.root().unwrap(), p.anchor.genesis_catalog);
    let path = f.root.join("control-only");
    let mut invalid = p.clone();
    invalid.limits.max_records = 0;
    let error = PeerReplicaServer::initial_control_learner(
        NodeId(3),
        &cluster(),
        identity(&f.root, 3),
        f.peers.clone(),
        &path,
        ControlInitialBinding {
            policy: invalid,
            state: ControlInitialState::Reserved,
        },
        Duration::from_millis(800),
    )
    .unwrap_err();
    assert!(error.contains("finite limits"), "{error}");
    assert!(!path.exists());
    let mut server = PeerReplicaServer::initial_control_learner(
        NodeId(3),
        &cluster(),
        identity(&f.root, 3),
        f.peers.clone(),
        &path,
        ControlInitialBinding {
            policy: p,
            state: ControlInitialState::Reserved,
        },
        Duration::from_millis(800),
    )
    .unwrap();
    assert!(server.cells.is_empty());
    assert_eq!(
        server.learners.keys().copied().collect::<Vec<_>>(),
        vec![CONTROL]
    );
    assert!(server.committed_application_entries().unwrap().is_empty());
    assert!(server.learners[&CONTROL].control_observation().is_err());
    drop(server);
    f.cleanup();
}
