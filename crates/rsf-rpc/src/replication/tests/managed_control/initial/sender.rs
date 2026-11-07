//! Production sender against actual TLS receiver/kernel owners. Native callback
//! fixtures prove ordering only, not native disk durability or admission.
use super::*;
use crate::replication::{ControlInitialProgress, ObservedControlInitialPublication};
fn point(writer: &mut PeerReplicaServer, address: SocketAddr) {
    writer
        .control_transport
        .peers
        .get_mut(&NodeId(3))
        .unwrap()
        .address = address;
}
fn complete(writer: &mut PeerReplicaServer) -> ObservedControlInitialPublication {
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut last = String::new();
    while Instant::now() < deadline {
        match writer.poll_initial_control_transfer().unwrap() {
            ControlInitialProgress::Observed(observed) => return observed,
            ControlInitialProgress::Conflict { state, .. } => {
                panic!("unexpected conflict: {state:?}")
            }
            ControlInitialProgress::Pinned { .. } => panic!("unexpected pinned publication"),
            other => last = format!("{other:?}"),
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    panic!("initial sender did not finish within unchanged budget: {last}")
}
#[test]
fn real_tls_sender_publication_and_cold_retry_preserve_exact_origin_without_admission() {
    let mut f = Fixture::new("sender-complete");
    let (mut writer, one, delivered) = source::source(&mut f);
    let p = policy(&f);
    let running = InitialRunning::start(
        receiver(&f, ControlInitialState::Reserved),
        ControlInitialState::Reserved,
        f.root.join("initial-control"),
    );
    point(&mut writer, running.address);
    writer
        .request_initial_control_transfer(&delivered, NodeId(3), p.limits, None)
        .unwrap();
    assert!(matches!(
        writer.poll_control_writer().unwrap(),
        ControlWriterProgress::Waiting
    ));
    assert!(
        writer
            .request_initial_control_transfer(&delivered, NodeId(3), p.limits, None)
            .is_err()
    );
    let published = complete(&mut writer);
    assert_eq!(published.target(), NodeId(3));
    assert_eq!(published.origin().generation(), 1);
    assert_eq!(published.current_history(), published.origin().history_id());
    assert_eq!(published.committed_tail(), LogIndex(1));
    assert_eq!(running.events.lock().unwrap().len(), 3);
    assert_eq!(*running.applied.lock().unwrap(), 0);
    assert!(matches!(
        writer.poll_initial_control_transfer().unwrap(),
        ControlInitialProgress::Idle
    ));
    let native = *running.ledger.lock().unwrap();
    let server = running.finish();
    assert!(
        !server.cells.contains_key(&CONTROL),
        "publication did not create a voter"
    );
    assert_eq!(
        server.learners[&CONTROL]
            .control_observation()
            .unwrap()
            .catalog(),
        &f.catalog
    );
    drop(server);
    let before = tree(&f.root.join("initial-control"));
    let running =
        InitialRunning::start(receiver(&f, native), native, f.root.join("initial-control"));
    point(&mut writer, running.address);
    writer
        .request_initial_control_transfer(&delivered, NodeId(3), p.limits, None)
        .unwrap();
    let retry = complete(&mut writer);
    assert_eq!(retry.origin(), published.origin());
    assert_eq!(retry.current_history(), published.current_history());
    assert!(running.events.lock().unwrap().is_empty());
    assert_eq!(tree(&f.root.join("initial-control")), before);
    drop(running.finish());
    drop(one.finish());
    drop(writer);
    f.cleanup();
}
#[test]
fn real_tls_sender_exact_replacement_refuses_stale_selection_aba() {
    let mut f = Fixture::new("sender-aba");
    let (mut writer, one, delivered) = source::source(&mut f);
    let p = policy(&f);
    let export = writer
        .export_initial_control_source(&delivered, p.limits)
        .unwrap();
    // Genuine bootstrap-only B cannot publish Genesis but can be selected.
    let other = f
        .control
        .get_mut(&NodeId(2))
        .unwrap()
        .history_snapshot()
        .unwrap()
        .descriptor;
    assert_ne!(other.id().unwrap(), export.descriptor().id().unwrap());
    let running = InitialRunning::start(
        receiver(&f, ControlInitialState::Reserved),
        ControlInitialState::Reserved,
        f.root.join("initial-control"),
    );
    let network = transport(&f, 0, running.address);
    let cursor = |reply| match reply {
        Reply::Cursor {
            selection,
            records: 0,
        } => selection,
        other => panic!("expected empty selected history: {other:?}"),
    };
    let a1 = cursor(
        network
            .initial_control_exchange(NodeId(3), &p, &Request::Start(export.descriptor().clone()))
            .unwrap(),
    );
    let b2 = cursor(
        network
            .initial_control_exchange(
                NodeId(3),
                &p,
                &Request::Replace {
                    expected: a1,
                    descriptor: other.clone(),
                },
            )
            .unwrap(),
    );
    let a3 = cursor(
        network
            .initial_control_exchange(
                NodeId(3),
                &p,
                &Request::Replace {
                    expected: b2,
                    descriptor: export.descriptor().clone(),
                },
            )
            .unwrap(),
    );
    assert_eq!(a1.history_id(), a3.history_id());
    assert_eq!(a3.generation(), 3);
    let b4 = cursor(
        network
            .initial_control_exchange(
                NodeId(3),
                &p,
                &Request::Replace {
                    expected: a3,
                    descriptor: other.clone(),
                },
            )
            .unwrap(),
    );
    let a5 = cursor(
        network
            .initial_control_exchange(
                NodeId(3),
                &p,
                &Request::Replace {
                    expected: b4,
                    descriptor: export.descriptor().clone(),
                },
            )
            .unwrap(),
    );
    assert_eq!(a5.history_id(), a3.history_id());
    assert_eq!(a5.generation(), 5);
    point(&mut writer, running.address);
    writer
        .request_initial_control_transfer(&delivered, NodeId(3), p.limits, Some(b2))
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        assert!(Instant::now() < deadline);
        match writer.poll_initial_control_transfer().unwrap() {
            ControlInitialProgress::Conflict { target, state } => {
                assert_eq!(target, NodeId(3));
                assert_eq!(state, ControlInitialState::Selected(a5));
                break;
            }
            ControlInitialProgress::Waiting => std::thread::sleep(Duration::from_millis(1)),
            other => panic!("stale replacement did not fail closed: {other:?}"),
        }
    }
    assert_eq!(
        *running.ledger.lock().unwrap(),
        ControlInitialState::Selected(a5)
    );
    assert_eq!(running.events.lock().unwrap().len(), 5);
    let b6 = cursor(
        network
            .initial_control_exchange(
                NodeId(3),
                &p,
                &Request::Replace {
                    expected: a5,
                    descriptor: other,
                },
            )
            .unwrap(),
    );
    writer
        .request_initial_control_transfer(&delivered, NodeId(3), p.limits, Some(b6))
        .unwrap();
    let completed = complete(&mut writer);
    assert_eq!(completed.origin().generation(), 7);
    assert_eq!(completed.origin().history_id(), a3.history_id());
    assert_eq!(running.events.lock().unwrap().len(), 9);
    drop(running.finish());
    drop(one.finish());
    drop(writer);
    f.cleanup();
}
#[test]
fn real_tls_sender_cancel_preserves_remote_selection_and_releases_control_writer() {
    let mut f = Fixture::new("sender-cancel");
    let (mut writer, one, delivered) = source::source(&mut f);
    let running = InitialRunning::start(
        receiver(&f, ControlInitialState::Reserved),
        ControlInitialState::Reserved,
        f.root.join("initial-control"),
    );
    point(&mut writer, running.address);
    writer
        .request_initial_control_transfer(&delivered, NodeId(3), policy(&f).limits, None)
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        assert!(Instant::now() < deadline);
        if let ControlInitialProgress::Advanced { records: 0, .. } =
            writer.poll_initial_control_transfer().unwrap()
        {
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    let selected = *running.ledger.lock().unwrap();
    assert!(matches!(selected, ControlInitialState::Selected(_)));
    let before = tree(&f.root.join("initial-control"));
    assert!(writer.cancel_initial_control_transfer());
    assert!(!writer.cancel_initial_control_transfer());
    assert_eq!(*running.ledger.lock().unwrap(), selected);
    assert_eq!(tree(&f.root.join("initial-control")), before);
    writer.request_membership_intent(f.intent(120)).unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        assert!(Instant::now() < deadline);
        if matches!(
            writer.poll_control_writer().unwrap(),
            ControlWriterProgress::ObservedOldControlDelivery(_)
        ) {
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(writer.cells[&CONTROL].committed_tail(), LogIndex(2));
    assert!(
        writer
            .request_initial_control_transfer(&delivered, NodeId(3), policy(&f).limits, None)
            .is_err(),
        "old delivery cannot be silently refreshed after source mutation"
    );
    drop(running.finish());
    drop(one.finish());
    drop(writer);
    f.cleanup();
}
mod failures;
mod relay;

#[test]
fn sender_rejects_wrong_target_identity_and_old_primary_source_before_remote_mutation() {
    let mut f = Fixture::new("sender-source-identity");
    let (mut writer, one, delivered) = source::source(&mut f);
    let p = policy(&f);
    let running = InitialRunning::start(
        receiver(&f, ControlInitialState::Reserved),
        ControlInitialState::Reserved,
        f.root.join("initial-control"),
    );
    point(&mut writer, running.address);
    let before = tree(&f.root.join("initial-control"));
    assert!(
        writer
            .request_initial_control_transfer(&delivered, NodeId(1), p.limits, None)
            .is_err(),
        "configured voter cannot become initial learner target"
    );
    let fingerprint = writer.control_transport.peers[&NodeId(3)].fingerprint;
    writer
        .control_transport
        .peers
        .get_mut(&NodeId(3))
        .unwrap()
        .fingerprint[0] ^= 1;
    let error = writer
        .request_initial_control_transfer(&delivered, NodeId(3), p.limits, None)
        .unwrap_err();
    assert!(
        error.contains("certificate differs from configured catalog"),
        "{error}"
    );
    writer
        .control_transport
        .peers
        .get_mut(&NodeId(3))
        .unwrap()
        .fingerprint = fingerprint;
    writer
        .request_initial_control_transfer(&delivered, NodeId(3), p.limits, None)
        .unwrap();
    writer
        .cells
        .get_mut(&CONTROL)
        .unwrap()
        .begin_view_change(
            NodeId(0),
            NodeId(2),
            rsf_vsr::VsrViewChangeRequest {
                pg: CONTROL,
                previous_view: Epoch(1),
                target_view: Epoch(2),
                candidate: NodeId(2),
                voters: (0..3).map(NodeId).collect(),
            },
        )
        .unwrap();
    assert!(writer.poll_initial_control_transfer().is_err());
    assert!(!writer.cancel_initial_control_transfer());
    assert_eq!(
        *running.ledger.lock().unwrap(),
        ControlInitialState::Reserved
    );
    assert_eq!(tree(&f.root.join("initial-control")), before);
    assert!(running.events.lock().unwrap().is_empty());
    assert!(
        writer
            .request_initial_control_transfer(&delivered, NodeId(3), p.limits, None)
            .is_err()
    );
    drop(running.finish());
    drop(one.finish());
    drop(writer);
    f.cleanup();
}

#[test]
fn real_tls_sender_never_abandons_foreign_selection_without_exact_replacement() {
    let mut f = Fixture::new("sender-no-implicit-replace");
    let (mut writer, one, delivered) = source::source(&mut f);
    let p = policy(&f);
    let other = f
        .control
        .get_mut(&NodeId(2))
        .unwrap()
        .history_snapshot()
        .unwrap()
        .descriptor;
    let running = InitialRunning::start(
        receiver(&f, ControlInitialState::Reserved),
        ControlInitialState::Reserved,
        f.root.join("initial-control"),
    );
    let network = transport(&f, 0, running.address);
    let Reply::Cursor {
        selection,
        records: 0,
    } = network
        .initial_control_exchange(NodeId(3), &p, &Request::Start(other))
        .unwrap()
    else {
        panic!("selection");
    };
    let before = tree(&f.root.join("initial-control"));
    point(&mut writer, running.address);
    writer
        .request_initial_control_transfer(&delivered, NodeId(3), p.limits, None)
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        assert!(Instant::now() < deadline);
        match writer.poll_initial_control_transfer().unwrap() {
            ControlInitialProgress::Conflict { target, state } => {
                assert_eq!(target, NodeId(3));
                assert_eq!(state, ControlInitialState::Selected(selection));
                break;
            }
            ControlInitialProgress::Waiting => std::thread::sleep(Duration::from_millis(1)),
            other => panic!("unexpected conflict result: {other:?}"),
        }
    }
    assert_eq!(tree(&f.root.join("initial-control")), before);
    assert_eq!(running.events.lock().unwrap().len(), 1);
    writer
        .request_initial_control_transfer(&delivered, NodeId(3), p.limits, Some(selection))
        .unwrap();
    let published = complete(&mut writer);
    assert_eq!(published.origin().generation(), 2);
    assert_ne!(published.origin().history_id(), selection.history_id());
    drop(running.finish());
    drop(one.finish());
    drop(writer);
    f.cleanup();
}
