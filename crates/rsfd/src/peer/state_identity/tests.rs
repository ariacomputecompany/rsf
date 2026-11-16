//! Real identity owner tests; fault seams interrupt actual persistence phases.
use super::*;
use std::cell::RefCell;
use std::sync::atomic::{AtomicU64, Ordering};
thread_local! {
    static HOOK:RefCell<Option<Box<Hook>>> = const {RefCell::new(None)};
}
type Hook = dyn FnMut(Boundary, &Path) -> Result<(), String>;
pub(super) fn boundary(at: Boundary, root: &Path) -> Result<(), String> {
    HOOK.with(|slot| match slot.borrow_mut().as_mut() {
        Some(hook) => hook(at, root),
        None => Ok(()),
    })
}
pub(in crate::peer) fn with_hook<T>(
    hook: impl FnMut(Boundary, &Path) -> Result<(), String> + 'static,
    run: impl FnOnce() -> T,
) -> T {
    struct Clear;
    impl Drop for Clear {
        fn drop(&mut self) {
            HOOK.with(|slot| *slot.borrow_mut() = None);
        }
    }
    HOOK.with(|slot| assert!(slot.borrow_mut().replace(Box::new(hook)).is_none()));
    let _clear = Clear;
    run()
}
pub(in crate::peer) struct Fixture(pub PathBuf);
impl Fixture {
    pub(in crate::peer) fn new(name: &str) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        loop {
            let path = std::env::temp_dir().join(format!(
                "rsfd-identity-{name}-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            match std::fs::create_dir(&path) {
                Ok(()) => return Self(path),
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(e) => panic!("fixture: {e}"),
            }
        }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!("native identity fixture retained: {}", self.0.display());
        } else {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}
fn cluster() -> ClusterId {
    ClusterId::new("native-identity-contract").unwrap()
}
fn owner(root: &Path) -> StateIdentity {
    StateIdentity::open(root, &cluster(), NodeId(3), None).unwrap()
}
fn bytes(root: &Path) -> Vec<u8> {
    std::fs::read(root.join("IDENTITY.json")).unwrap()
}
#[test]
fn native_identity_retains_the_actual_exclusive_peer_lock_and_monotonic_managed_set() {
    let f = Fixture::new("held-lock");
    let mut live = owner(&f.0);
    assert!(StateIdentity::open(&f.0, &cluster(), NodeId(3), None).is_err());
    let groups = [PlacementGroupId(1), PlacementGroupId(9)].into();
    live.enroll(&groups).unwrap();
    let before = bytes(&f.0);
    assert!(
        live.enroll(&[PlacementGroupId(1)].into())
            .unwrap_err()
            .contains("cannot be omitted")
    );
    live.validate().unwrap();
    assert_eq!(bytes(&f.0), before);
    drop(live);
    let mut cold = owner(&f.0);
    assert_eq!(cold.managed(), &groups);
    cold.enroll(
        &[
            PlacementGroupId(1),
            PlacementGroupId(9),
            PlacementGroupId(10),
        ]
        .into(),
    )
    .unwrap();
    drop(cold);
    assert!(StateIdentity::open(&f.0, &cluster(), NodeId(4), None).is_err());
    assert_eq!(owner(&f.0).managed().len(), 3);
}
#[test]
fn native_identity_schema_preserves_legacy_and_rejects_duplicate_null_noncanonical_fields() {
    for bytes in [
        r#"{"version":1,"version":2,"cluster":"native","node":3}"#,
        r#"{"version":1,"cluster":"native","node":3,"managed_membership":null}"#,
        r#"{"version":2,"cluster":"native","node":3,"managed_membership":[]}"#,
        r#"{"version":2,"cluster":"native","node":3,"managed_membership":[9,1]}"#,
        r#"{"version":2,"cluster":"native","node":3,"managed_membership":[1,1]}"#,
        r#"{"version":3,"cluster":"native","node":3,"managed_membership":[],"reserved_control":null}"#,
        r#"{"version":1,"cluster":"native","node":3,"unknown":0}"#,
    ] {
        assert!(schema::Value::decode(bytes.as_bytes()).is_err(), "{bytes}");
    }
    for raw in [
        r#"{"version":1,"cluster":"native","node":3}"#,
        r#"{"version":2,"cluster":"native","node":3,"managed_membership":[1,9]}"#,
    ] {
        let v = schema::Value::decode(raw.as_bytes()).unwrap();
        assert_eq!(schema::Value::decode(&v.encode().unwrap()).unwrap(), v);
    }
    assert!(schema::Hex32::try_from("A".repeat(64)).is_err());
    assert!(schema::Hex32::try_from("a".repeat(63)).is_err());
    let mut over = vec![b' '; MAX_IDENTITY_BYTES as usize + 1];
    over[0] = b'{';
    assert!(
        schema::Value::decode(&over)
            .unwrap_err()
            .contains("finite bound")
    );
}
#[test]
fn native_identity_uncertain_publication_requires_cold_selection_and_never_promotes_pending() {
    for at in [
        Boundary::BeforeWrite,
        Boundary::BeforeFileSync,
        Boundary::BeforeRename,
        Boundary::AfterRename,
        Boundary::BeforeDirectorySync,
        Boundary::AfterDirectorySync,
    ] {
        let f = Fixture::new("uncertain");
        let mut live = owner(&f.0);
        let old = bytes(&f.0);
        let error = with_hook(
            move |boundary, _| {
                if boundary == at {
                    Err("injected identity boundary failure".into())
                } else {
                    Ok(())
                }
            },
            || live.enroll(&[PlacementGroupId(7)].into()),
        )
        .unwrap_err();
        assert_eq!(error, "injected identity boundary failure");
        assert_eq!(
            live.validate().unwrap_err(),
            "native identity durability failed; reopen required"
        );
        let visible = bytes(&f.0);
        let renamed = matches!(
            at,
            Boundary::AfterRename | Boundary::BeforeDirectorySync | Boundary::AfterDirectorySync
        );
        assert_eq!(visible == old, !renamed);
        drop(live);
        let mut cold = owner(&f.0);
        assert_eq!(cold.managed().contains(&PlacementGroupId(7)), renamed);
        cold.enroll(&[PlacementGroupId(7)].into()).unwrap();
    }
}
#[test]
fn native_identity_panic_and_observed_file_root_lock_loss_use_distinct_sticky_path() {
    let f = Fixture::new("panic");
    let mut live = owner(&f.0);
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| with_hook(
            |at, _| {
                if at == Boundary::BeforeRename {
                    panic!("injected persistence unwind");
                }
                Ok(())
            },
            || live.enroll(&[PlacementGroupId(7)].into())
        )))
        .is_err()
    );
    assert_eq!(
        live.validate().unwrap_err(),
        "native identity durability failed; reopen required"
    );
    drop(live);
    for name in ["IDENTITY.json", ".peer.lock", ""] {
        let f = Fixture::new("restore");
        let mut live = owner(&f.0);
        let source = if name.is_empty() {
            f.0.clone()
        } else {
            f.0.join(name)
        };
        let saved = f.0.with_extension("saved");
        #[cfg(unix)]
        let inode = {
            use std::os::unix::fs::MetadataExt;
            std::fs::metadata(&source).unwrap().ino()
        };
        std::fs::rename(&source, &saved).unwrap();
        let first = live.validate().unwrap_err();
        assert_ne!(first, "native identity durability failed; reopen required");
        std::fs::rename(&saved, &source).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            assert_eq!(std::fs::metadata(&source).unwrap().ino(), inode);
        }
        // Linux rename can change ctime. This exact reason proves the latch
        // short-circuit, not a claim that all restored version metadata matches.
        assert_eq!(
            live.validate().unwrap_err(),
            "native identity durability failed; reopen required"
        );
    }
}
#[test]
fn native_identity_missing_or_aliased_acknowledged_metadata_is_not_recreated() {
    let f = Fixture::new("missing");
    drop(owner(&f.0));
    std::fs::remove_file(f.0.join(".peer.lock")).unwrap();
    assert!(
        StateIdentity::open(&f.0, &cluster(), NodeId(3), None)
            .unwrap_err()
            .contains("lost peer lock")
    );
    assert!(!f.0.join(".peer.lock").exists());
    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;
        let f = Fixture::new("alias");
        drop(owner(&f.0));
        let original = bytes(&f.0);
        let foreign = f.0.join("foreign");
        std::fs::rename(f.0.join("IDENTITY.json"), &foreign).unwrap();
        symlink(&foreign, f.0.join("IDENTITY.json")).unwrap();
        assert!(StateIdentity::open(&f.0, &cluster(), NodeId(3), None).is_err());
        assert_eq!(std::fs::read(&foreign).unwrap(), original);
        std::fs::remove_file(f.0.join("IDENTITY.json")).unwrap();
        std::fs::hard_link(&foreign, f.0.join("IDENTITY.json")).unwrap();
        assert!(StateIdentity::open(&f.0, &cluster(), NodeId(3), None).is_err());
    }
}
#[test]
fn native_identity_prepared_inode_replacement_cannot_be_adopted_after_rename() {
    let f = Fixture::new("prepared");
    let mut live = owner(&f.0);
    let original = bytes(&f.0);
    let result = with_hook(
        |at, root| {
            if at == Boundary::AfterRename {
                let path = root.join("IDENTITY.json");
                let bytes = std::fs::read(&path).unwrap();
                std::fs::rename(&path, root.join("retained-original")).unwrap();
                std::fs::write(&path, bytes).unwrap();
            }
            Ok(())
        },
        || live.enroll(&[PlacementGroupId(8)].into()),
    );
    assert!(result.is_err());
    assert_ne!(bytes(&f.0), original);
    assert_eq!(
        live.validate().unwrap_err(),
        "native identity durability failed; reopen required"
    );
}

/// Physically alter the already-written pending inode with a valid envelope.
/// This is fault injection only; it constructs no transition or authority.
pub(in crate::peer) fn mutate_prepared_identity(root: &Path, append: bool) -> Vec<u8> {
    use std::io::Write;
    let path = root.join("IDENTITY.pending");
    let intended = std::fs::read(&path).unwrap();
    let original = std::fs::metadata(&path).unwrap();
    let changed = if append {
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        file.write_all(b" ").unwrap();
        let mut changed = intended.clone();
        changed.push(b' ');
        changed
    } else {
        let mut value = schema::Value::decode(&intended).unwrap();
        assert_eq!(value.node, NodeId(3));
        value.node = NodeId(4);
        let changed = value.encode().unwrap();
        assert_eq!(changed.len(), intended.len());
        assert_ne!(changed, intended);
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .write_all(&changed)
            .unwrap();
        changed
    };
    let actual = std::fs::read(&path).unwrap();
    assert_eq!(actual, changed);
    assert_ne!(actual, intended);
    schema::Value::decode(&actual).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let after = std::fs::metadata(&path).unwrap();
        assert_eq!((after.dev(), after.ino()), (original.dev(), original.ino()));
    }
    changed
}

fn rejects_changed_prepared_identity(append: bool) {
    let f = Fixture::new(if append {
        "pending-append"
    } else {
        "pending-rewrite"
    });
    let mut live = owner(&f.0);
    let before = bytes(&f.0);
    let observed = std::sync::Arc::new(std::sync::Mutex::new(None));
    let copy = std::sync::Arc::clone(&observed);
    let renamed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let rename_seen = std::sync::Arc::clone(&renamed);
    let configured = [PlacementGroupId(7)].into();
    let error = with_hook(
        move |at, root| {
            if at == Boundary::BeforeFileSync {
                let mut slot = copy.lock().unwrap();
                assert!(slot.is_none(), "fault must run exactly once");
                *slot = Some(mutate_prepared_identity(root, append));
            }
            if at == Boundary::BeforeRename {
                rename_seen.store(true, Ordering::SeqCst);
            }
            Ok(())
        },
        || live.enroll(&configured),
    )
    .unwrap_err();
    assert_eq!(
        error,
        if append {
            "prepared native identity length differs from intended bytes"
        } else {
            "prepared native identity bytes differ from intended bytes"
        }
    );
    let changed = observed.lock().unwrap().clone().expect("fault must occur");
    assert!(
        !renamed.load(Ordering::SeqCst),
        "invalid bytes reached rename"
    );
    assert_eq!(bytes(&f.0), before);
    assert_eq!(
        std::fs::read(f.0.join("IDENTITY.pending")).unwrap(),
        changed
    );
    assert!(
        live.managed().is_empty(),
        "logical successor must not escape"
    );
    assert_eq!(
        live.validate().unwrap_err(),
        "native identity durability failed; reopen required"
    );
    assert_eq!(
        live.enroll(&configured).unwrap_err(),
        "native identity durability failed; reopen required"
    );
    drop(live);
    let mut cold = owner(&f.0);
    assert!(cold.managed().is_empty());
    assert_eq!(
        bytes(&f.0),
        before,
        "cold open must select the old final file"
    );
    assert_eq!(
        std::fs::read(f.0.join("IDENTITY.pending")).unwrap(),
        changed
    );
    cold.enroll(&configured).unwrap();
    assert_eq!(cold.managed(), &configured);
    assert!(!f.0.join("IDENTITY.pending").exists());
    assert_eq!(schema::Value::decode(&bytes(&f.0)).unwrap().node, NodeId(3));
}

#[test]
fn native_identity_rejects_same_length_valid_pending_rewrite_before_rename() {
    rejects_changed_prepared_identity(false);
}

#[test]
fn native_identity_rejects_appended_pending_bytes_before_rename() {
    rejects_changed_prepared_identity(true);
}

pub(in crate::peer) fn decode_initial(path: &Path) -> ControlInitialState {
    schema::Value::decode(&std::fs::read(path).unwrap())
        .unwrap()
        .control
        .unwrap()
        .custody
        .initial()
        .unwrap()
}

#[test]
fn managed_identity_never_follows_or_replaces_foreign_temporary_paths() {
    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;
        let f = Fixture::new("temporary");
        let mut live = owner(&f.0);
        let before = bytes(&f.0);
        let foreign = f.0.join("foreign");
        std::fs::write(&foreign, b"foreign payload").unwrap();
        symlink(&foreign, f.0.join("IDENTITY.pending")).unwrap();
        assert!(live.enroll(&[PlacementGroupId(1)].into()).is_err());
        assert_eq!(std::fs::read(&foreign).unwrap(), b"foreign payload");
        assert_eq!(bytes(&f.0), before);
        assert_eq!(
            live.validate().unwrap_err(),
            "native identity durability failed; reopen required"
        );
        drop(live);
        std::fs::remove_file(f.0.join("IDENTITY.pending")).unwrap();
        std::fs::create_dir(f.0.join("IDENTITY.pending")).unwrap();
        let mut cold = owner(&f.0);
        assert!(cold.enroll(&[PlacementGroupId(1)].into()).is_err());
        assert!(f.0.join("IDENTITY.pending").is_dir());
        assert_eq!(bytes(&f.0), before);
    }
}

#[test]
fn native_identity_transition_schema_rejects_overflow_retarget_and_skipped_publication() {
    use rsf_vsr::VsrInitialSelection;
    let a = VsrInitialSelection::new(1, [1; 32]).unwrap();
    let b = VsrInitialSelection::new(2, [2; 32]).unwrap();
    let max = VsrInitialSelection::new(u64::MAX, [1; 32]).unwrap();
    assert!(schema::permitted(
        ControlInitialState::Reserved,
        ControlInitialState::Selected(a)
    ));
    assert!(schema::permitted(
        ControlInitialState::Selected(a),
        ControlInitialState::Selected(b)
    ));
    assert!(!schema::permitted(
        ControlInitialState::Selected(max),
        ControlInitialState::Selected(a)
    ));
    assert!(!schema::permitted(
        ControlInitialState::Selected(a),
        ControlInitialState::Published {
            selection: a,
            source: NodeId(0)
        }
    ));
    assert!(!schema::permitted(
        ControlInitialState::Publishing {
            selection: a,
            source: NodeId(0)
        },
        ControlInitialState::Published {
            selection: a,
            source: NodeId(1)
        }
    ));
    assert!(!schema::permitted(
        ControlInitialState::Published {
            selection: a,
            source: NodeId(0)
        },
        ControlInitialState::Reserved
    ));
    // This is pure edge/schema coverage. No opaque kernel/native transition is
    // constructed, and no synthetic state is used to obtain a receiver ACK.
}

#[test]
fn native_identity_rejects_same_inode_rewrite_hidden_by_rename_timestamp_refresh() {
    use std::io::Write;
    use std::os::unix::fs::MetadataExt;
    let f = Fixture::new("postrename-byte-custody");
    let mut live = owner(&f.0);
    let configured = [PlacementGroupId(7)].into();
    let observed = std::sync::Arc::new(std::sync::Mutex::new(None));
    let copy = std::sync::Arc::clone(&observed);
    let result = with_hook(
        move |at, root| {
            if at != Boundary::AfterRename {
                return Ok(());
            }
            let path = root.join("IDENTITY.json");
            assert!(!root.join("IDENTITY.pending").exists());
            let mut held = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();
            let original = held.metadata().unwrap();
            let intended = std::fs::read(&path).unwrap();
            let mut value = schema::Value::decode(&intended).unwrap();
            assert_eq!(value.node, NodeId(3));
            assert_eq!(value.managed, [PlacementGroupId(7)].into());
            value.node = NodeId(4);
            let changed = value.encode().unwrap();
            assert_eq!(changed.len(), intended.len());
            assert_ne!(changed, intended);
            held.write_all(&changed).unwrap();
            held.sync_all().unwrap();
            held.set_times(std::fs::FileTimes::new().set_modified(original.modified().unwrap()))
                .unwrap();
            let after = held.metadata().unwrap();
            assert_eq!((after.dev(), after.ino()), (original.dev(), original.ino()));
            assert_eq!(after.len(), original.len());
            assert_eq!(after.modified().unwrap(), original.modified().unwrap());
            assert_eq!(std::fs::read(&path).unwrap(), changed);
            let mut slot = copy.lock().unwrap();
            assert!(slot.is_none(), "exactly one actual post-rename mutation");
            *slot = Some((held, intended, changed, (original.dev(), original.ino())));
            Ok(())
        },
        || live.enroll(&configured),
    );
    let (held, intended, changed, identity) = observed
        .lock()
        .unwrap()
        .take()
        .expect("physical mutation ran");
    assert_eq!(
        (
            held.metadata().unwrap().dev(),
            held.metadata().unwrap().ino()
        ),
        identity
    );
    assert_eq!(bytes(&f.0), changed);
    let error = result
        .expect_err("post-rename foreign bytes must not become a successful native publication");
    assert_eq!(
        error,
        "prepared native identity bytes differ from intended bytes"
    );
    assert!(
        live.managed().is_empty(),
        "logical successor cannot escape failed publication"
    );
    assert_eq!(
        live.validate().unwrap_err(),
        "native identity durability failed; reopen required"
    );
    assert_eq!(
        live.enroll(&configured).unwrap_err(),
        "native identity durability failed; reopen required"
    );
    drop(live);
    let error = StateIdentity::open(&f.0, &cluster(), NodeId(3), None).unwrap_err();
    assert_eq!(error, "peer state belongs to a different cluster or node");
    assert_eq!(
        bytes(&f.0),
        changed,
        "cold refusal must not repair the foreign identity"
    );
    // Actual rename already happened. Restoring the same inode for this test
    // permits cold recovery of that successor, never revival of the failed owner.
    let path = f.0.join("IDENTITY.json");
    let mut restore = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    restore.write_all(&intended).unwrap();
    restore.sync_all().unwrap();
    assert_eq!(
        (
            restore.metadata().unwrap().dev(),
            restore.metadata().unwrap().ino()
        ),
        identity
    );
    drop(restore);
    let mut cold = owner(&f.0);
    assert_eq!(cold.managed(), &configured);
    cold.validate().unwrap();
    assert_eq!(bytes(&f.0), intended);
}
