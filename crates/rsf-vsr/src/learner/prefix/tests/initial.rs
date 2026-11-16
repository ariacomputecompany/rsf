//! Initial path consumes the same held helper owner and original physical proof.
use super::super::observation::{self, Boundary};
use super::*;
use crate::{
    InitialLearnerHistoryStore, VsrInitialBegin, VsrInitialPolicy, VsrInitialRestriction,
    VsrInitialState,
};

#[test]
fn initial_handoff_rejects_same_bytes_closed_inode_replacement_before_native_pin() {
    let root = root("initial-handoff-custody");
    let mut source = open(&root.join("source"));
    let genesis = prepare(&mut source, 1, b"Genesis");
    commit(&mut source, genesis.index);
    let snapshot = source.history_snapshot().unwrap();
    let policy = VsrInitialPolicy {
        pg: PlacementGroupId(1),
        local: NodeId(3),
        bootstrap: source.membership_lineage().unwrap().bootstrap,
        limits: limits(),
    };
    let path = root.join("initial");
    let mut initial =
        InitialLearnerHistoryStore::open(&path, policy, VsrInitialRestriction::Reserved).unwrap();
    let mut restriction = VsrInitialState::Reserved;
    let selected = initial
        .begin(
            VsrInitialBegin::Start(snapshot.descriptor.clone()),
            &mut |event| {
                assert_eq!(event.expected(), restriction);
                restriction = event.next();
                Ok(())
            },
        )
        .unwrap()
        .selection();
    drop(initial);
    let candidate = path.join(
        selected
            .history_id()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>(),
    );
    let cell = candidate.join("cell");
    // Produce real multi-segment retained bytes under a real writer. The native
    // mock still names only the exact selected descriptor; cold replay must
    // derive progress, never trust a fabricated cursor or certificate.
    let options = SegmentStoreOptions {
        max_segment_bytes: 1,
        log_options: rsf_storage::SegmentLogOptions {
            sync_policy: rsf_storage::SyncPolicy::Full,
        },
    };
    let mut store = SegmentStore::open_with_options(&cell, options).unwrap();
    for record in &snapshot.records {
        store
            .append_next_wal(policy.pg, record.epoch, record.kind, &record.payload)
            .unwrap();
    }
    assert!(store.segments().len() > 1);
    let closed = cell.join("segments").join(&store.segments()[0].file_name);
    drop(store);
    let mut initial =
        InitialLearnerHistoryStore::open(&path, policy, VsrInitialRestriction::Selected(selected))
            .unwrap();
    assert_eq!(
        initial.status().unwrap().records(),
        snapshot.descriptor.records
    );
    let saved = root.join("original-closed");
    let fault_path = closed.clone();
    let fault_saved = saved.clone();
    let watch = observation::fault(Boundary::BeforeHandoff, move |_| {
        let mtime = fs::metadata(&fault_path).unwrap().modified().unwrap();
        fs::rename(&fault_path, &fault_saved).unwrap();
        fs::write(&fault_path, fs::read(&fault_saved).unwrap()).unwrap();
        File::options()
            .write(true)
            .open(&fault_path)
            .unwrap()
            .set_times(std::fs::FileTimes::new().set_modified(mtime))
            .unwrap();
    });
    let mut semantics = 0;
    let mut pins = 0;
    let outcome = initial.finish(
        &selected,
        &mut |_| {
            semantics += 1;
            Ok(())
        },
        &mut |_| {
            pins += 1;
            Ok(())
        },
    );
    assert_eq!(watch.calls(), 1);
    assert!(outcome.is_err());
    assert_eq!(
        semantics, 0,
        "original file custody precedes semantic preview"
    );
    assert_eq!(pins, 0, "no native Publishing or Published event");
    assert!(!path.join("ACTIVE").exists());
    assert_eq!(restriction, VsrInitialState::Selected(selected));
    fs::remove_file(&closed).unwrap();
    fs::rename(&saved, &closed).unwrap();
    assert!(
        initial
            .status()
            .unwrap_err()
            .message
            .contains("initial learner durability failed")
    );
    drop(initial);
    let mut initial =
        InitialLearnerHistoryStore::open(&path, policy, VsrInitialRestriction::Selected(selected))
            .unwrap();
    let complete = initial
        .finish(
            &selected,
            &mut |preview| {
                assert_eq!(preview.committed_tail(), LogIndex(1));
                preview.validate_live()
            },
            &mut |event| {
                assert_eq!(event.expected(), restriction);
                restriction = event.next();
                Ok(())
            },
        )
        .unwrap();
    assert!(
        matches!(restriction, VsrInitialState::Published { selection, source: NodeId(0) } if selection == selected)
    );
    drop(complete.into_published());
    drop(initial);
    drop(source);
    fs::remove_dir_all(root).unwrap();
}
