//! An authentic accepted suffix must not become an initial Genesis publication.
use super::*;
use rsf_vsr::{VsrHistoryBatch, VsrHistoryRecord};

fn upload(
    sender: &NetworkVsrTransport,
    p: &ControlInitialPolicy,
    selection: VsrInitialSelection,
    records: &[VsrHistoryRecord],
) {
    let mut ordinal = 0;
    while ordinal < records.len() as u64 {
        let batch = VsrHistoryBatch::pack(ordinal, &records[ordinal as usize..]).unwrap();
        let expected = ordinal + batch.records.len() as u64;
        let Reply::Cursor {
            selection: actual,
            records: next,
        } = sender
            .initial_control_exchange(NodeId(3), p, &Request::Batch { selection, batch })
            .unwrap()
        else {
            panic!("expected exact retained cursor");
        };
        assert_eq!(actual, selection);
        assert_eq!(next, expected);
        ordinal = next;
    }
    assert_eq!(ordinal, records.len() as u64);
}

#[test]
fn real_tls_initial_accepted_suffix_refuses_pin_and_retains_healthy_replacement() {
    let mut f = Fixture::new("initial-accepted-tail");
    let (mut writer, one, observed) = source::source(&mut f);
    let p = policy(&f);
    let export = writer
        .export_initial_control_source(&observed, p.limits)
        .unwrap();
    let pure = export.descriptor().clone();
    let mut pure_records = Vec::new();
    while pure_records.len() < pure.records as usize {
        let batch = writer
            .read_initial_control_batch(&export, pure_records.len() as u64)
            .unwrap();
        assert_eq!(batch.ordinal, pure_records.len() as u64);
        assert!(!batch.records.is_empty());
        pure_records.extend(batch.records);
    }
    assert_eq!(pure_records.len() as u64, pure.records);
    // Preserve exact bytes exported under actual observed quorum delivery.
    // Mutation invalidates that source token; no later source read uses it.
    let intent = f.intent(141);
    let accepted = prepare_command(
        writer.cells.get_mut(&CONTROL).unwrap(),
        ControlCommand::Begin(intent.clone()),
    );
    assert_eq!(accepted.index, LogIndex(2));
    assert_eq!(writer.cells[&CONTROL].committed_tail(), LogIndex(1));
    assert_eq!(writer.cells[&CONTROL].committed_count(), 1);
    assert_eq!(writer.cells[&CONTROL].next_index(), LogIndex(3));
    assert_eq!(
        writer.cells[&CONTROL].prepared(LogIndex(2)),
        Some(&accepted)
    );
    assert!(
        writer.cells[&CONTROL]
            .committed_operation(LogIndex(2))
            .is_none()
    );
    let VsrOperation::ReplicatedCommand { payload, .. } = &accepted.operation else {
        panic!("actual canonical control command");
    };
    assert_eq!(
        ControlCommand::decode(payload).unwrap(),
        ControlCommand::Begin(intent)
    );
    let snapshot = writer
        .cells
        .get_mut(&CONTROL)
        .unwrap()
        .history_snapshot()
        .unwrap();
    assert_eq!(snapshot.descriptor.probe.committed_tail, LogIndex(1));
    assert_eq!(snapshot.records.len() as u64, snapshot.descriptor.records);
    assert_eq!(snapshot.records.len(), pure_records.len() + 1);
    assert_eq!(
        &snapshot.records[..pure_records.len()],
        pure_records.as_slice()
    );
    assert_eq!(
        snapshot.records.last().unwrap().kind,
        rsf_core::storage::WalRecordKind::Prepare
    );
    assert_ne!(snapshot.descriptor.id().unwrap(), pure.id().unwrap());

    let path = f.root.join("initial-control");
    let running = InitialRunning::start(
        receiver(&f, ControlInitialState::Reserved),
        ControlInitialState::Reserved,
        path.clone(),
    );
    let sender = transport(&f, 0, running.address);
    let Reply::Cursor {
        selection,
        records: 0,
    } = sender
        .initial_control_exchange(NodeId(3), &p, &Request::Start(snapshot.descriptor.clone()))
        .unwrap()
    else {
        panic!("new initial cursor");
    };
    assert_eq!(selection.history_id(), snapshot.descriptor.id().unwrap());
    upload(&sender, &p, selection, &snapshot.records);
    assert!(matches!(
        sender.initial_control_exchange(NodeId(3), &p, &Request::Status).unwrap(),
        Reply::Progress { state: ControlInitialState::Selected(actual), records }
            if actual == selection && records == snapshot.descriptor.records
    ));
    let candidate = path.join(
        selection
            .history_id()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>(),
    );
    assert_eq!(
        std::fs::read(candidate.join("DESCRIPTOR")).unwrap(),
        snapshot.descriptor.encode().unwrap()
    );
    let before = tree(&path);
    reject(
        sender.initial_control_exchange(NodeId(3), &p, &Request::Finish(selection)),
        "initial candidate is not an exact certified original Genesis cut",
    );
    let after = tree(&path);
    for (name, bytes) in &before {
        assert_eq!(
            after.get(name),
            Some(bytes),
            "rejection changed retained {name:?}"
        );
    }
    // Authoritative semantic replay creates its own held VSR lock. It cannot
    // alter the transferred history, native restriction, outer IDENTITY or ACTIVE.
    let added = after
        .iter()
        .filter(|(name, _)| !before.contains_key(*name))
        .collect::<Vec<_>>();
    assert_eq!(added.len(), 1);
    assert_eq!(
        added[0].0,
        &candidate
            .join("cell/.vsr-writer.lock")
            .strip_prefix(&path)
            .unwrap()
            .to_path_buf()
    );
    assert_eq!(added[0].1.as_deref(), Some(&[][..]));
    assert!(!path.join("ACTIVE").exists());
    assert_eq!(
        *running.ledger.lock().unwrap(),
        ControlInitialState::Selected(selection)
    );
    assert_eq!(
        *running.events.lock().unwrap(),
        vec![ControlInitialState::Selected(selection)]
    );
    assert_eq!(*running.applied.lock().unwrap(), 0);

    // Both a repeat FINISH and exact cursor resume operate on the same healthy
    // retained candidate. A transport error cannot satisfy this domain oracle.
    reject(
        sender.initial_control_exchange(NodeId(3), &p, &Request::Finish(selection)),
        "initial candidate is not an exact certified original Genesis cut",
    );
    assert!(matches!(
        sender.initial_control_exchange(NodeId(3), &p, &Request::Resume {
            selection, descriptor: snapshot.descriptor.clone()
        }).unwrap(),
        Reply::Cursor { selection: actual, records }
            if actual == selection && records == snapshot.descriptor.records
    ));
    assert_eq!(tree(&path), after);
    assert_eq!(
        *running.events.lock().unwrap(),
        vec![ControlInitialState::Selected(selection)]
    );

    let Reply::Cursor {
        selection: replacement,
        records: 0,
    } = sender
        .initial_control_exchange(
            NodeId(3),
            &p,
            &Request::Replace {
                expected: selection,
                descriptor: pure.clone(),
            },
        )
        .unwrap()
    else {
        panic!("explicit generation replacement");
    };
    assert_eq!(replacement.generation(), selection.generation() + 1);
    assert_eq!(replacement.history_id(), pure.id().unwrap());
    assert!(matches!(
        sender.initial_control_exchange(NodeId(3), &p, &Request::Finish(selection)).unwrap(),
        Reply::Conflict(ControlInitialState::Selected(actual)) if actual == replacement
    ));
    upload(&sender, &p, replacement, &pure_records);
    assert!(matches!(
        sender.initial_control_exchange(NodeId(3), &p, &Request::Finish(replacement)).unwrap(),
        Reply::Published { origin, current, tail: LogIndex(1) }
            if origin == replacement && current == pure.id().unwrap()
    ));
    assert_eq!(
        *running.events.lock().unwrap(),
        vec![
            ControlInitialState::Selected(selection),
            ControlInitialState::Selected(replacement),
            ControlInitialState::Publishing {
                selection: replacement,
                source: NodeId(0)
            },
            ControlInitialState::Published {
                selection: replacement,
                source: NodeId(0)
            },
        ]
    );
    assert_eq!(
        std::fs::read(path.join("ACTIVE")).unwrap(),
        replacement.history_id()
    );
    assert_eq!(*running.applied.lock().unwrap(), 0);
    let mut server = running.finish();
    let actual = server.learners[&CONTROL].control_observation().unwrap();
    assert_eq!(actual.catalog(), &f.catalog);
    assert_eq!(actual.committed_tail(), LogIndex(1));
    assert!(server.committed_application_entries().unwrap().is_empty());
    drop(server);
    drop(one.finish());
    drop(writer);
    f.cleanup();
}
