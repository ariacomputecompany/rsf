use super::*;
use crate::membership::decision::disk::{self, observation};
use std::sync::atomic::{AtomicU64, Ordering};

fn reserved(label: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    loop {
        let path = root(&format!(
            "stage-copy-{label}-{}",
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        match fs::create_dir(&path) {
            Ok(()) => return path,
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => panic!("{}: {e}", path.display()),
        }
    }
}
fn inject_visible_group(path: &Path, records: &[VsrHistoryRecord], epoch: Epoch) {
    let options = SegmentStoreOptions {
        log_options: rsf_storage::SegmentLogOptions {
            sync_policy: rsf_storage::SyncPolicy::Full,
        },
        max_segment_bytes: 1,
    };
    let mut store = SegmentStore::open_with_options(path, options).unwrap();
    let mut ordinal = 0;
    let items = std::iter::from_fn(|| {
        if ordinal == records.len() {
            return None;
        }
        let batch = VsrHistoryBatch::pack(ordinal as u64, &records[ordinal..]).unwrap();
        ordinal += batch.records.len();
        Some(Ok(rsf_storage::WalGroupEntry {
            epoch,
            kind: WalRecordKind::ViewInstallation,
            payload: batch.encode().unwrap(),
        }))
    })
    .chain(std::iter::once(Err(corrupt(
        "complete visible producer failure",
    ))));
    let error = store
        .try_append_next_wal_group(PlacementGroupId(1), items)
        .unwrap_err();
    assert_eq!(error.message, "complete visible producer failure");
    assert!(store.revision().is_err());
    // No final group barrier succeeded; these bytes are only process-visible.
    drop(store);
    let recovered = SegmentStore::recover_existing(path).unwrap();
    let actual = recovered
        .recovery
        .wal_records
        .iter()
        .flat_map(|r| VsrHistoryBatch::decode(&r.payload).unwrap().records)
        .collect::<Vec<_>>();
    assert_eq!(actual, records);
}
fn seed_complete_unsealed(path: &Path, f: &Fixture) -> VsrMembershipStagedPlan {
    fs::create_dir(path).unwrap();
    let mut expected = plan(f, false);
    expected.retain_anchor(&f.cells[0]).unwrap();
    let input = expected.input();
    let source = input.flattened_records(&f.approval).unwrap();
    disk::create_input(path, input).unwrap();
    let id = input
        .id()
        .unwrap()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    inject_visible_group(
        &path.join("inputs").join(id).join("records"),
        &source,
        input.proposal().election.target_view,
    );
    inject_visible_group(
        &path.join("selected"),
        &expected.records,
        input.proposal().election.previous_view,
    );
    assert!(!path.join("META").exists());
    assert!(!path.join("INSTALLED").exists());
    expected
}
fn with_receiver_receipts(path: &Path) -> Fixture {
    let mut f = fixture(path);
    for n in 0..70 {
        f.cells[0]
            .remember_client_request(VsrClientRequestId::new("local-copy-extra", n), LogIndex(1))
            .unwrap();
    }
    f
}
#[test]
fn unsealed_complete_visible_source_and_selected_groups_renew_before_stage_seal() {
    let path = reserved("visible");
    let f = with_receiver_receipts(&path);
    let stage_path = path.join("stage");
    let expected = seed_complete_unsealed(&stage_path, &f);
    let observed = observation::watch(None);
    let mut stage =
        MembershipStagingStore::stage(&stage_path, NodeId(0), plan(&f, false), &f.cells[0])
            .unwrap();
    let events = observed.events();
    assert_eq!(
        events.len(),
        6,
        "both complete local copies must renew instead of early-returning"
    );
    assert!(events.iter().all(|e| e.retained == e.total && e.total != 0));
    assert_eq!(
        events.iter().map(|e| e.boundary).collect::<Vec<_>>(),
        [
            observation::Boundary::Closed,
            observation::Boundary::Active,
            observation::Boundary::Complete,
            observation::Boundary::Closed,
            observation::Boundary::Active,
            observation::Boundary::Complete,
        ]
    );
    drop(observed);
    assert_eq!(stage.plan().id(), expected.id());
    assert_eq!(stage.plan().records, expected.records);
    assert!(stage.report().is_err());
    let mut applied = Vec::new();
    let report = stage
        .materialize(&mut |entry| {
            applied.push(entry.prepare.index);
            Ok(())
        })
        .unwrap();
    assert_eq!(applied, vec![LogIndex(1), LogIndex(2)]);
    assert_eq!(report.accepted_tail, expected.accepted_tail());
    drop(stage);
    let mut cold =
        MembershipStagingStore::open_existing(&stage_path, PlacementGroupId(1), NodeId(0)).unwrap();
    assert_eq!(cold.materialize(&mut |_| Ok(())).unwrap(), report);
    drop(cold);
    drop(f);
    fs::remove_dir_all(path).unwrap();
}
#[test]
fn complete_local_copy_renewal_failure_prevents_meta_for_source_and_selected() {
    for failure in [0, 1, 2, 3, 4, 5] {
        let path = reserved("renewal-error");
        let f = fixture(&path);
        let stage_path = path.join("stage");
        seed_complete_unsealed(&stage_path, &f);
        let observed = observation::watch(Some(failure));
        let error =
            MembershipStagingStore::stage(&stage_path, NodeId(0), plan(&f, false), &f.cells[0])
                .unwrap_err();
        assert_eq!(error.message, "injected local-copy boundary");
        let events = observed.events();
        assert_eq!(events.len(), failure + 1);
        assert!(events.iter().all(|e| e.retained == e.total && e.total != 0));
        assert!(
            !stage_path.join("META").exists(),
            "failed renewal sealed a stage"
        );
        assert!(!stage_path.join("INSTALLED").exists());
        drop(observed);
        let stage =
            MembershipStagingStore::stage(&stage_path, NodeId(0), plan(&f, false), &f.cells[0])
                .unwrap();
        assert!(
            stage.report().is_err(),
            "construction manufactured a materialization ACK"
        );
        drop(stage);
        drop(f);
        fs::remove_dir_all(path).unwrap();
    }
}
