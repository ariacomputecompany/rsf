//! Legacy layout fixtures, actual filesystem/lock/barrier calls, no repair mock.
use super::*;
use crate::peer::state_identity::tests::Fixture as Root;
use rsf_core::Epoch;
use rsf_vsr::DurableVsrCell;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fs;
use std::os::unix::fs::{MetadataExt, symlink};
use std::rc::Rc;

type Hook = dyn FnMut(Boundary, &Path) -> Result<(), String>;
thread_local! { static HOOK: RefCell<Option<Box<Hook>>> = const { RefCell::new(None) }; }
pub(super) fn boundary(at: Boundary, root: &Path) -> Result<(), String> {
    HOOK.with(|slot| match slot.borrow_mut().as_mut() {
        Some(hook) => hook(at, root),
        None => Ok(()),
    })
}
fn with_hook<T>(
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
fn cluster() -> ClusterId {
    ClusterId::new("legacy-application-migration").unwrap()
}
struct Fixture {
    root: Root,
    state: PathBuf,
    source: PathBuf,
    binding: PathBuf,
}
impl Fixture {
    fn new() -> Self {
        let root = Root::new("legacy-application-migration");
        let state = root.0.join("daemon");
        let source = state.join("protocol.vsr");
        let binding = state.join("replication-identity");
        rsf_storage::create_dir_all_synced(&binding).unwrap();
        rsf_storage::create_dir_all_synced(&source).unwrap();
        // The old application bind_state wrote this V1 file without a peer
        // lock. This constructs that legacy format, not a new publisher proof.
        fs::write(
            binding.join("IDENTITY.json"),
            br#"{ "version": 1, "cluster": "legacy-application-migration", "node": 0 }"#,
        )
        .unwrap();
        drop(
            DurableVsrCell::open(
                source.join("pg-1"),
                PlacementGroupId(1),
                Epoch(1),
                NodeId(0),
                [NodeId(0)],
            )
            .unwrap(),
        );
        Self {
            root,
            state,
            source,
            binding,
        }
    }
    fn migrate(&self) -> Result<Completed, String> {
        migrate(&self.state, &cluster(), NodeId(0))
    }
    fn owner(&self) -> Result<SourceOwner, String> {
        SourceOwner::open(
            &self.source,
            &cluster(),
            NodeId(0),
            PlacementGroupId(1),
            None,
        )
    }
    fn error(&self) -> String {
        match self.migrate() {
            Err(e) => e,
            Ok(_) => panic!("invalid legacy migration succeeded"),
        }
    }
}
type Inventory = BTreeMap<PathBuf, (u8, u64, u64, Vec<u8>)>;
fn inventory(root: &Path) -> Inventory {
    fn visit(root: &Path, path: &Path, result: &mut Inventory) {
        let metadata = fs::symlink_metadata(path).unwrap();
        let (kind, bytes) = if metadata.is_dir() {
            (0, vec![])
        } else if metadata.is_file() {
            (1, fs::read(path).unwrap())
        } else {
            (
                2,
                fs::read_link(path)
                    .unwrap()
                    .as_os_str()
                    .as_encoded_bytes()
                    .to_vec(),
            )
        };
        result.insert(
            path.strip_prefix(root).unwrap().to_path_buf(),
            (kind, metadata.dev(), metadata.ino(), bytes),
        );
        if metadata.is_dir() {
            for child in fs::read_dir(path).unwrap() {
                visit(root, &child.unwrap().path(), result);
            }
        }
    }
    let mut result = Inventory::new();
    visit(root, root, &mut result);
    result
}
fn lock_inode(f: &Fixture) -> (u64, u64) {
    let m = fs::metadata(f.binding.join(".peer.lock")).unwrap();
    (m.dev(), m.ino())
}
const ALL: [Boundary; 5] = [
    Boundary::BeforeLock,
    Boundary::AfterLock,
    Boundary::AfterIdentitySync,
    Boundary::AfterLockSync,
    Boundary::AfterDirectorySync,
];

#[test]
fn explicit_application_identity_migration_preserves_legacy_bytes_and_repeats_all_barriers() {
    let f = Fixture::new();
    let identity = fs::read(f.binding.join("IDENTITY.json")).unwrap();
    let source = inventory(&f.source);
    assert_eq!(
        f.owner().unwrap_err(),
        "existing native identity lost peer lock"
    );
    let calls = Rc::new(RefCell::new(Vec::new()));
    let observed = Rc::clone(&calls);
    with_hook(
        move |at, _| {
            observed.borrow_mut().push(at);
            Ok(())
        },
        || {
            // Exercise the actual peer CLI dispatch/argument parser; the unchanged
            // process transfer test remains the separate real TLS integration check.
            crate::peer::run_cli(&[
                "migrate-application-identity".into(),
                "--state".into(),
                f.state.display().to_string(),
                "--cluster".into(),
                cluster().as_str().into(),
                "--node".into(),
                "0".into(),
            ])
            .unwrap();
        },
    );
    assert_eq!(calls.borrow().as_slice(), ALL);
    let inode = lock_inode(&f);
    let before = inventory(&f.state);
    calls.borrow_mut().clear();
    let observed = Rc::clone(&calls);
    let completed = with_hook(
        move |at, _| {
            observed.borrow_mut().push(at);
            Ok(())
        },
        || f.migrate(),
    )
    .unwrap();
    assert_eq!(calls.borrow().as_slice(), ALL);
    assert_eq!(f.owner().unwrap_err(), "peer state already has an owner");
    assert_eq!(lock_inode(&f), inode);
    assert_eq!(inventory(&f.state), before);
    drop(completed);
    f.owner().unwrap().validate().unwrap();
    assert_eq!(fs::read(f.binding.join("IDENTITY.json")).unwrap(), identity);
    assert_eq!(inventory(&f.source), source);
}

#[test]
fn explicit_application_identity_migration_rejects_schema_binding_and_namespace_without_effects() {
    for bytes in [
        br#"{"version":2,"cluster":"legacy-application-migration","node":0}"#.as_slice(),
        br#"{"version":2,"cluster":"legacy-application-migration","node":0,"managed_membership":[1]}"#.as_slice(),
        br#"{"version":3,"cluster":"legacy-application-migration","node":0}"#.as_slice(),
        br#"{"version":1,"cluster":"legacy-application-migration","node":0,"managed_membership":[]}"#.as_slice(),
        br#"{"version":1,"cluster":"legacy-application-migration","node":0,"reserved_control":null}"#.as_slice(),
        br#"{"version":1,"version":1,"cluster":"legacy-application-migration","node":0}"#.as_slice(),
        b"corrupt".as_slice(),
    ] {
        let f = Fixture::new(); fs::write(f.binding.join("IDENTITY.json"), bytes).unwrap();
        let before = inventory(&f.state);
        assert_eq!(f.error(), "application migration requires the exact legacy V1 schema");
        assert_eq!(inventory(&f.state), before);
    }
    for bad_node in [false, true] {
        let f = Fixture::new();
        let before = inventory(&f.state);
        let c = if bad_node {
            cluster()
        } else {
            ClusterId::new("wrong-cluster").unwrap()
        };
        let node = if bad_node { NodeId(1) } else { NodeId(0) };
        let error = match migrate(&f.state, &c, node) {
            Err(e) => e,
            Ok(_) => panic!("wrong binding succeeded"),
        };
        assert_eq!(
            error,
            "application migration identity belongs to a different cluster or node"
        );
        assert_eq!(inventory(&f.state), before);
    }
    for name in ["IDENTITY.pending", "foreign"] {
        let f = Fixture::new();
        fs::write(f.binding.join(name), b"retained").unwrap();
        let before = inventory(&f.state);
        assert_eq!(
            f.error(),
            "application migration binding contains foreign entries"
        );
        assert_eq!(inventory(&f.state), before);
    }
}

#[test]
fn explicit_application_identity_migration_refuses_native_and_missing_routes() {
    for marker in ["IDENTITY.json", "IDENTITY.pending", ".peer.lock"] {
        let f = Fixture::new();
        fs::write(f.source.join(marker), b"native").unwrap();
        let before = inventory(&f.state);
        assert_eq!(f.error(), "daemon history source acquired native markers");
        assert_eq!(inventory(&f.state), before);
    }
    for missing in ["source", "binding", "identity"] {
        let f = Fixture::new();
        let expected = match missing {
            "source" => {
                fs::remove_dir_all(&f.source).unwrap();
                "history source directory operation failed"
            }
            "binding" => {
                fs::remove_dir_all(&f.binding).unwrap();
                "history source directory operation failed"
            }
            "identity" => {
                fs::remove_file(f.binding.join("IDENTITY.json")).unwrap();
                "application migration requires an existing V1 identity"
            }
            _ => unreachable!(),
        };
        let before = inventory(&f.state);
        assert_eq!(f.error(), expected);
        assert_eq!(inventory(&f.state), before);
    }
    let root = Root::new("native-not-application-migration");
    drop(StateIdentity::open(&root.0, &cluster(), NodeId(0), None).unwrap());
    fs::remove_file(root.0.join(".peer.lock")).unwrap();
    let before = inventory(&root.0);
    assert_eq!(
        StateIdentity::open_existing(&root.0, &cluster(), NodeId(0), None).unwrap_err(),
        "existing native identity lost peer lock"
    );
    let error = match migrate(&root.0, &cluster(), NodeId(0)) {
        Err(e) => e,
        Ok(_) => panic!("native root migrated as application"),
    };
    assert_eq!(error, "history source directory operation failed");
    assert_eq!(inventory(&root.0), before);
}

#[test]
fn explicit_application_identity_migration_rejects_aliases_and_nonempty_lock() {
    for lock in [false, true] {
        for hard in [false, true] {
            let f = Fixture::new();
            let target = f
                .binding
                .join(if lock { ".peer.lock" } else { "IDENTITY.json" });
            if lock {
                fs::write(&target, b"").unwrap();
            }
            let saved = f.root.0.join("original-file");
            fs::rename(&target, &saved).unwrap();
            if hard {
                fs::hard_link(&saved, &target).unwrap();
            } else {
                symlink(&saved, &target).unwrap();
            }
            let before = inventory(&f.root.0);
            assert_eq!(
                f.error(),
                if hard {
                    "application identity migration file has an alias"
                } else {
                    "application identity migration requires regular files"
                }
            );
            assert_eq!(inventory(&f.root.0), before);
        }
    }
    let f = Fixture::new();
    fs::write(f.binding.join(".peer.lock"), b"occupied").unwrap();
    let before = inventory(&f.state);
    assert_eq!(f.error(), "application migration peer lock is not empty");
    assert_eq!(inventory(&f.state), before);
}

#[test]
fn explicit_application_identity_migration_refuses_actual_held_lock_without_replacement() {
    let f = Fixture::new();
    let file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(f.binding.join(".peer.lock"))
        .unwrap();
    let held = rsf_storage::ExclusiveFileLock::acquire(file).unwrap();
    let before = inventory(&f.state);
    assert_eq!(
        f.error(),
        "application migration binding already has an owner"
    );
    assert_eq!(inventory(&f.state), before);
    drop(held);
    drop(f.migrate().unwrap());
    assert_eq!(inventory(&f.state), before);
}

#[test]
fn explicit_application_identity_migration_original_route_loss_precedes_lock_creation() {
    for which in ["parent", "source", "binding", "identity"] {
        let f = Fixture::new();
        let before = inventory(&f.state);
        let target = match which {
            "parent" => f.state.clone(),
            "source" => f.source.clone(),
            "binding" => f.binding.clone(),
            "identity" => f.binding.join("IDENTITY.json"),
            _ => unreachable!(),
        };
        let saved = f.root.0.join("retained");
        let moved = saved.clone();
        let replaced = target.clone();
        let expected = if which == "identity" {
            "application migration original identity custody changed"
        } else {
            "history source original directory custody changed"
        };
        let error = with_hook(
            move |at, _| {
                if at == Boundary::BeforeLock {
                    fs::rename(&replaced, &moved).unwrap();
                    if which == "identity" {
                        fs::write(&replaced, fs::read(&moved).unwrap()).unwrap();
                    } else {
                        fs::create_dir(&replaced).unwrap();
                    }
                }
                Ok(())
            },
            || f.error(),
        );
        assert_eq!(error, expected);
        if which == "identity" {
            fs::remove_file(&target).unwrap();
        } else {
            fs::remove_dir(&target).unwrap();
        }
        fs::rename(saved, target).unwrap();
        assert!(!f.binding.join(".peer.lock").exists());
        assert_eq!(inventory(&f.state), before);
        drop(f.migrate().unwrap());
    }
}

#[test]
fn explicit_application_identity_migration_postcreation_error_checks_original_custody() {
    let f = Fixture::new();
    let source = inventory(&f.source);
    let identity = f.binding.join("IDENTITY.json");
    let saved = f.root.0.join("original-identity");
    let target = identity.clone();
    let moved = saved.clone();
    let error = with_hook(
        move |at, _| {
            if at == Boundary::AfterLock {
                fs::rename(&target, &moved).unwrap();
                fs::write(&target, fs::read(&moved).unwrap()).unwrap();
                return Err("injected callback error".into());
            }
            Ok(())
        },
        || f.error(),
    );
    assert_eq!(
        error,
        "application migration original identity custody changed"
    );
    let inode = lock_inode(&f);
    fs::remove_file(&identity).unwrap();
    fs::rename(saved, identity).unwrap();
    drop(f.migrate().unwrap());
    assert_eq!(lock_inode(&f), inode);
    assert_eq!(inventory(&f.source), source);
}

#[test]
fn explicit_application_identity_migration_interrupted_boundaries_repeat_without_lock_recreation() {
    for stop in ALL {
        let f = Fixture::new();
        let source = inventory(&f.source);
        let identity = fs::read(f.binding.join("IDENTITY.json")).unwrap();
        let reached = Rc::new(RefCell::new(false));
        let observed = Rc::clone(&reached);
        let error = with_hook(
            move |at, _| {
                if at == stop {
                    *observed.borrow_mut() = true;
                    Err("migration boundary interrupted".into())
                } else {
                    Ok(())
                }
            },
            || f.error(),
        );
        assert_eq!(error, "migration boundary interrupted");
        assert!(*reached.borrow());
        let inode = if stop == Boundary::BeforeLock {
            assert!(!f.binding.join(".peer.lock").exists());
            None
        } else {
            Some(lock_inode(&f))
        };
        let calls = Rc::new(RefCell::new(Vec::new()));
        let observed = Rc::clone(&calls);
        drop(
            with_hook(
                move |at, _| {
                    observed.borrow_mut().push(at);
                    Ok(())
                },
                || f.migrate(),
            )
            .unwrap(),
        );
        assert_eq!(calls.borrow().as_slice(), ALL);
        if let Some(inode) = inode {
            assert_eq!(lock_inode(&f), inode);
        }
        assert_eq!(fs::read(f.binding.join("IDENTITY.json")).unwrap(), identity);
        assert_eq!(inventory(&f.source), source);
    }
}

#[test]
fn explicit_application_identity_migration_unwind_releases_owner_and_preserves_retry_inode() {
    for stop in [Boundary::AfterLock, Boundary::AfterDirectorySync] {
        let f = Fixture::new();
        let source = inventory(&f.source);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            with_hook(
                move |at, _| {
                    if at == stop {
                        std::panic::panic_any("migration-unwind");
                    }
                    Ok(())
                },
                || f.migrate(),
            )
        }));
        let payload = match result {
            Err(payload) => payload,
            Ok(_) => panic!("migration did not reach actual boundary"),
        };
        assert_eq!(payload.downcast_ref::<&str>(), Some(&"migration-unwind"));
        let inode = lock_inode(&f);
        drop(f.migrate().unwrap());
        assert_eq!(lock_inode(&f), inode);
        assert_eq!(inventory(&f.source), source);
    }
}
