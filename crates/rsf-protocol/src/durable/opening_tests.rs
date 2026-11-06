//! Physical constructor generation and partial-initialization regressions.
use super::*;
use std::cell::{Cell, RefCell};
use std::fs;
use std::rc::Rc;
thread_local! { static HOOK: RefCell<Option<Box<dyn FnOnce()>>> = RefCell::new(None); }
struct Hook;
impl Drop for Hook {
    fn drop(&mut self) {
        HOOK.with(|hook| {
            hook.borrow_mut().take();
        });
    }
}
fn watch(action: impl FnOnce() + 'static) -> Hook {
    HOOK.with(|hook| {
        assert!(hook.borrow().is_none());
        *hook.borrow_mut() = Some(Box::new(action));
    });
    Hook
}
pub(super) fn after_metadata() {
    let action = HOOK.with(|hook| hook.borrow_mut().take());
    if let Some(action) = action {
        action();
    }
}
fn root(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "rsf-protocol-onepass-{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}
fn open(path: &Path) -> RsfResult<VsrBackedFabricRouter> {
    VsrBackedFabricRouter::open(
        path,
        [PlacementGroupId(1)],
        Epoch(1),
        NodeId(0),
        [NodeId(0)],
    )
}
fn cleanup(path: &Path) {
    fs::remove_dir_all(path).unwrap();
    if path.with_extension("vsr").exists() {
        fs::remove_dir_all(path.with_extension("vsr")).unwrap();
    }
}
fn custody_matches(router: &VsrBackedFabricRouter) -> bool {
    router.router.wals[&PlacementGroupId(1)]
        .store
        .lock()
        .unwrap()
        .capture_owned_custody()
        .is_ok()
}
fn records(path: &Path) -> Vec<WalRecord> {
    SegmentStore::recover_existing(path)
        .unwrap()
        .recovery
        .wal_records
}
fn segment_bytes(path: &Path) -> Vec<(std::ffi::OsString, Vec<u8>)> {
    let mut result = fs::read_dir(path.join("segments"))
        .unwrap()
        .map(|entry| {
            let path = entry.unwrap().path();
            (
                path.file_name().unwrap().to_owned(),
                fs::read(path).unwrap(),
            )
        })
        .collect::<Vec<_>>();
    result.sort();
    result
}
fn application() -> ApplicationPayload {
    ApplicationPayload {
        namespace: "onepass/ns".into(),
        surface: ApplicationSurface::Kv,
        bytes: b"original-value".to_vec(),
    }
}
fn operation() -> FabricOperation {
    FabricOperation::KvMutation {
        key: b"onepass-key".to_vec(),
    }
}

#[test]
fn protocol_open_retains_metadata_writer_generation_through_receipt_recovery() {
    let a = root("original");
    let b = root("foreign");
    let saved = a.with_extension("saved");
    {
        let mut original = open(&a).unwrap();
        original.update_policy(false, 1).unwrap();
        let mut foreign = open(&b).unwrap();
        foreign.update_policy(false, 1).unwrap();
        foreign.update_policy(true, 1).unwrap();
    }
    let hit = Rc::new(Cell::new(0));
    let (aa, bb, ss, count) = (a.clone(), b.clone(), saved.clone(), hit.clone());
    let guard = watch(move || {
        count.set(count.get() + 1);
        fs::rename(&aa, &ss).unwrap();
        fs::rename(&bb, &aa).unwrap();
    });
    // VSR stores are siblings at path.with_extension("vsr"), so their original
    // custody is untouched by this protocol-store-only substitution.
    let opened = open(&a);
    let during = opened.as_ref().is_ok_and(|router| !custody_matches(router));
    fs::rename(&a, &b).unwrap();
    fs::rename(&saved, &a).unwrap();
    drop(guard);
    assert_eq!(hit.get(), 1);
    let mut router = opened.unwrap();
    assert_eq!(router.placement_map().groups, vec![PlacementGroupId(1)]);
    assert_eq!(router.policy(), &ProtocolPolicy::new(2, false, 1).unwrap());
    assert_eq!(router.latest_version(), CommitVersion(0));
    assert!(
        during,
        "original writer must not adopt the foreign named protocol store"
    );
    assert!(
        custody_matches(&router),
        "restored original must match the actual held writer"
    );
    let request = ClientRequestId::new("onepass", 1);
    let committed = router
        .commit_application(
            PlacementGroupId(1),
            request.clone(),
            ConsistencyMode::Strict,
            operation(),
            application(),
        )
        .unwrap();
    assert_eq!(committed.certificate.index, LogIndex(1));
    assert_eq!(committed.certificate.version, CommitVersion(1));
    drop(router);
    let original = open(&a).unwrap();
    assert_eq!(original.policy().version, 2);
    assert_eq!(original.latest_version(), CommitVersion(1));
    let commands = original.committed_application_commands();
    assert_eq!(commands.len(), 1);
    assert_eq!(commands[0].request, request);
    assert_eq!(commands[0].application, application());
    assert_eq!(
        CommitCertificate::from(&commands[0].commit),
        committed.certificate
    );
    let foreign = open(&b).unwrap();
    assert_eq!(foreign.policy(), &ProtocolPolicy::new(3, true, 1).unwrap());
    assert_eq!(foreign.latest_version(), CommitVersion(0));
    assert!(foreign.committed_application_commands().is_empty());
    drop(original);
    drop(foreign);
    cleanup(&a);
    cleanup(&b);
}

#[test]
fn protocol_open_preserves_each_missing_metadata_prefix_and_never_reappends_on_reopen() {
    for prefix in 0..3 {
        let path = root("partial");
        if prefix != 0 {
            let mut store = SegmentStore::open(&path).unwrap();
            let payload = if prefix == 1 {
                PlacementMap::new(7, [PlacementGroupId(1)])
                    .unwrap()
                    .encode()
                    .unwrap()
            } else {
                ProtocolPolicy::new(9, false, 1).unwrap().encode().unwrap()
            };
            store
                .append_next_wal(METADATA_PG, Epoch(1), WalRecordKind::Membership, &payload)
                .unwrap();
        }
        let router = open(&path).unwrap();
        assert_eq!(
            router.placement_map().version,
            if prefix == 1 { 7 } else { 1 }
        );
        assert_eq!(router.policy().version, if prefix == 2 { 9 } else { 1 });
        assert_eq!(router.latest_version(), CommitVersion(0));
        drop(router);
        let wal = records(&path);
        assert_eq!(wal.len(), 2);
        assert!(
            wal.iter()
                .all(|record| record.pg == METADATA_PG && record.kind == WalRecordKind::Membership)
        );
        assert_eq!(wal[0].index, LogIndex(1));
        assert_eq!(wal[1].index, LogIndex(2));
        let before = segment_bytes(&path);
        let reopened = open(&path).unwrap();
        drop(reopened);
        assert_eq!(segment_bytes(&path), before);
        assert_eq!(records(&path), wal);
        cleanup(&path);
    }
}

#[test]
fn protocol_open_keeps_placement_before_corrupt_policy_failure_and_refuses_repeatedly() {
    let path = root("invalid-policy");
    let mut store = SegmentStore::open(&path).unwrap();
    store
        .append_next_wal(
            METADATA_PG,
            Epoch(1),
            WalRecordKind::Membership,
            b"RSFY1invalid-policy",
        )
        .unwrap();
    drop(store);
    assert!(open(&path).is_err());
    let wal = records(&path);
    assert_eq!(wal.len(), 2);
    assert_eq!(wal[0].payload.as_slice(), b"RSFY1invalid-policy");
    assert_eq!(
        PlacementMap::decode(&wal[1].payload).unwrap(),
        PlacementMap::new(1, [PlacementGroupId(1)]).unwrap()
    );
    assert!(!path.with_extension("vsr").exists());
    let before = segment_bytes(&path);
    assert!(open(&path).is_err());
    assert_eq!(segment_bytes(&path), before);
    cleanup(&path);
}

#[test]
fn protocol_open_reused_receipts_still_reject_duplicate_physical_commit_records() {
    let path = root("duplicate-receipt");
    let mut router = open(&path).unwrap();
    let request = ClientRequestId::new("duplicate", 1);
    let commit = router
        .commit_application(
            PlacementGroupId(1),
            request.clone(),
            ConsistencyMode::Strict,
            operation(),
            application(),
        )
        .unwrap();
    drop(router);
    let receipt = ProtocolCommitRecord {
        certificate: commit.certificate,
        client_request: Some(request),
    }
    .encode();
    let mut store = SegmentStore::open(&path).unwrap();
    store
        .append_next_wal(
            PlacementGroupId(1),
            Epoch(1),
            WalRecordKind::Commit,
            &receipt,
        )
        .unwrap();
    drop(store);
    let before = segment_bytes(&path);
    let error = open(&path).unwrap_err();
    assert!(
        error.message.contains("duplicate durable protocol receipt"),
        "{error:?}"
    );
    assert_eq!(segment_bytes(&path), before);
    assert!(open(&path).is_err());
    assert_eq!(segment_bytes(&path), before);
    cleanup(&path);
}
