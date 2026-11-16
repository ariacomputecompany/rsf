//! Test-only physical substitution between recovered bytes and domain replay.
use super::*;
use std::cell::{Cell, RefCell};
use std::rc::Rc;
thread_local! {
    static AFTER_OPEN: RefCell<Option<Box<dyn FnOnce()>>> = RefCell::new(None);
}
struct Guard;
impl Drop for Guard {
    fn drop(&mut self) {
        AFTER_OPEN.with(|slot| {
            slot.borrow_mut().take();
        });
    }
}
fn watch(action: impl FnOnce() + 'static) -> Guard {
    AFTER_OPEN.with(|slot| {
        assert!(slot.borrow().is_none());
        *slot.borrow_mut() = Some(Box::new(action));
    });
    Guard
}
pub(crate) fn after_open() {
    let action = AFTER_OPEN.with(|slot| slot.borrow_mut().take());
    if let Some(action) = action {
        action();
    }
}

fn request(zone: &str, token: &str) -> JoinRequest {
    let mut request = super::request(zone);
    request.token = InviteToken(token.into());
    request
}

#[test]
fn recovered_admin_retains_the_original_writer_and_exact_replayed_generation() {
    let a = super::test_path("admin-original-generation");
    let b = super::test_path("admin-foreign-generation");
    let saved = a.with_extension("original");
    {
        let mut original =
            AdminPlane::open_durable(&a, InviteToken("original-token".into())).unwrap();
        super::promote(&mut original, request("original", "original-token"));
        let mut foreign =
            AdminPlane::open_durable(&b, InviteToken("foreign-token".into())).unwrap();
        for zone in ["foreign-a", "foreign-b", "foreign-c"] {
            super::promote(&mut foreign, request(zone, "foreign-token"));
        }
    } // Every fixture writer is dropped before the physical root substitution.
    let hit = Rc::new(Cell::new(0));
    let (aa, bb, ss, count) = (a.clone(), b.clone(), saved.clone(), hit.clone());
    let guard = watch(move || {
        count.set(count.get() + 1);
        std::fs::rename(&aa, &ss).unwrap();
        std::fs::rename(&bb, &aa).unwrap();
    });
    let recovered = AdminPlane::recover(&a);
    let during_swap = recovered.as_ref().is_ok_and(|admin| {
        admin
            .store
            .as_ref()
            .unwrap()
            .capture_owned_custody()
            .is_err()
    });
    // Restore the exact original root before assertions, including intended RED.
    std::fs::rename(&a, &b).unwrap();
    std::fs::rename(&saved, &a).unwrap();
    drop(guard);
    assert_eq!(hit.get(), 1);
    let mut admin = recovered.unwrap();
    assert_eq!(admin.records().count(), 1);
    assert!(
        admin
            .token_verifier
            .matches(&token_verifier(&InviteToken("original-token".into())))
    );
    assert_eq!(admin.membership_view().unwrap().voters(), 1);
    assert!(
        during_swap,
        "writer must retain original physical files while foreign root is named"
    );
    admin
        .store
        .as_ref()
        .unwrap()
        .capture_owned_custody()
        .expect("exact restored original matches retained writer");
    let joined = admin.admit(request("continued", "original-token")).unwrap();
    assert_eq!(joined.node.id, NodeId(1));
    assert_eq!(joined.state, AdmissionState::Learner);
    drop(admin);
    let original = AdminPlane::recover(&a).unwrap();
    assert_eq!(original.records().count(), 2);
    assert_eq!(original.membership_view().unwrap().voters(), 1);
    assert!(
        original
            .records()
            .any(|record| record.node.id == NodeId(1) && record.state == AdmissionState::Learner)
    );
    let foreign = AdminPlane::recover(&b).unwrap();
    assert_eq!(foreign.records().count(), 3);
    assert_eq!(foreign.membership_view().unwrap().voters(), 3);
    assert!(
        foreign
            .token_verifier
            .matches(&token_verifier(&InviteToken("foreign-token".into())))
    );
    drop(original);
    drop(foreign);
    std::fs::remove_dir_all(a).unwrap();
    std::fs::remove_dir_all(b).unwrap();
}

#[test]
fn recovered_admin_refuses_framed_invalid_domain_event_without_rewriting_wal() {
    let path = super::test_path("admin-invalid-domain-event");
    drop(AdminPlane::open_durable(&path, InviteToken("original-token".into())).unwrap());
    let mut store = SegmentStore::open(&path).unwrap();
    store
        .append_next_wal(
            ADMIN_PG,
            Epoch(1),
            WalRecordKind::Membership,
            b"RSFA1invalid-event",
        )
        .unwrap();
    drop(store);
    let files = || {
        let mut files = std::fs::read_dir(path.join("segments"))
            .unwrap()
            .map(|entry| {
                let path = entry.unwrap().path();
                (
                    path.file_name().unwrap().to_owned(),
                    std::fs::read(path).unwrap(),
                )
            })
            .collect::<Vec<_>>();
        files.sort();
        files
    };
    let before = files();
    assert!(AdminPlane::recover(&path).is_err());
    assert_eq!(files(), before);
    assert!(AdminPlane::recover(&path).is_err());
    assert_eq!(files(), before);
    std::fs::remove_dir_all(path).unwrap();
}
