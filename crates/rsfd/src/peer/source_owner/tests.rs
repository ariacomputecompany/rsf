//! Real files, actual native identity ownership and existing VSR source stores.
//! These new selector tests do not replace the unchanged actual CLI/TLS test.
use super::*;
use crate::peer::state_identity::tests::Fixture as Root;
use rsf_core::Epoch;
use rsf_vsr::DurableVsrCell;
use std::collections::BTreeMap;
use std::fs;
use std::os::unix::fs::{MetadataExt, symlink};

struct Fixture {
    root: Root,
    parent: PathBuf,
    source: PathBuf,
    binding: PathBuf,
}
fn cluster() -> ClusterId {
    ClusterId::new("application-source-contract").unwrap()
}
impl Fixture {
    fn new(name: &str, native: bool) -> Self {
        let root = Root::new(name);
        let parent = root.0.join("daemon");
        let source = parent.join("protocol.vsr");
        let binding = parent.join("replication-identity");
        rsf_storage::create_dir_all_synced(&source).unwrap();
        rsf_storage::create_dir_all_synced(&binding).unwrap();
        drop(StateIdentity::open(&binding, &cluster(), NodeId(0), None).unwrap());
        if native {
            drop(StateIdentity::open(&source, &cluster(), NodeId(0), None).unwrap());
        }
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
            parent,
            source,
            binding,
        }
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
}
type Inventory = BTreeMap<PathBuf, (u8, u64, u64, Vec<u8>)>;
fn inventory(root: &Path) -> Inventory {
    fn visit(root: &Path, path: &Path, result: &mut Inventory) {
        let metadata = fs::symlink_metadata(path).unwrap();
        let (kind, bytes) = if metadata.is_dir() {
            (0, Vec::new())
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
fn reservation() -> Reservation {
    // This typed provisioning restriction is never used as a source cell or
    // consensus proof; the tests persist it through the actual identity writer.
    serde_json::from_value(serde_json::json!({
        "pg":1, "bootstrap":"01".repeat(32), "genesis_catalog":"02".repeat(32),
        "max_records":4096, "max_encoded_bytes":16777216,
        "custody":{"kind":"existing_voter"}
    }))
    .unwrap()
}

#[test]
fn application_history_source_retains_real_binding_and_existing_cell_without_native_creation() {
    for native in [false, true] {
        let f = Fixture::new("application-source-existing", native);
        let before = inventory(&f.parent);
        let mut owner = f.owner().unwrap();
        assert_eq!(f.owner().unwrap_err(), "peer state already has an owner");
        let cell =
            DurableVsrCell::open_existing(f.source.join("pg-1"), PlacementGroupId(1)).unwrap();
        owner.validate().unwrap();
        assert_eq!(inventory(&f.parent), before);
        assert_eq!(f.source.join("IDENTITY.json").exists(), native);
        assert_eq!(f.source.join(".peer.lock").exists(), native);
        drop(cell);
        drop(owner);
        f.owner().unwrap().validate().unwrap();
        assert_eq!(inventory(&f.parent), before);
    }
}

#[test]
fn application_history_source_refuses_misbound_missing_corrupt_and_lockless_binding() {
    for fault in 0..6 {
        let f = Fixture::new("application-source-binding", false);
        let expected = match fault {
            0 | 1 => "peer state belongs to a different cluster or node",
            2 => {
                fs::remove_file(f.binding.join("IDENTITY.json")).unwrap();
                "native identity is required"
            }
            3 => {
                fs::write(f.binding.join("IDENTITY.json"), b"corrupt").unwrap();
                "peer state identity is corrupt"
            }
            4 => {
                fs::remove_file(f.binding.join(".peer.lock")).unwrap();
                "existing native identity lost peer lock"
            }
            5 => {
                fs::remove_dir_all(&f.binding).unwrap();
                "native identity root is absent"
            }
            _ => unreachable!(),
        };
        let before = inventory(&f.parent);
        let requested_cluster = if fault == 0 {
            ClusterId::new("different-cluster").unwrap()
        } else {
            cluster()
        };
        let node = if fault == 1 { NodeId(1) } else { NodeId(0) };
        assert_eq!(
            SourceOwner::open(
                &f.source,
                &requested_cluster,
                node,
                PlacementGroupId(1),
                None
            )
            .unwrap_err(),
            expected
        );
        assert_eq!(inventory(&f.parent), before, "fault {fault}");
    }
}

#[test]
fn history_source_native_markers_never_fall_back_to_healthy_application_binding() {
    for marker in ["IDENTITY.pending", ".peer.lock"] {
        let f = Fixture::new("application-source-native-marker", false);
        fs::write(f.source.join(marker), b"").unwrap();
        let before = inventory(&f.parent);
        assert_eq!(f.owner().unwrap_err(), "native identity is required");
        assert_eq!(inventory(&f.parent), before);
    }
    let f = Fixture::new("application-source-native-lock-loss", true);
    fs::remove_file(f.source.join(".peer.lock")).unwrap();
    let before = inventory(&f.parent);
    assert_eq!(
        f.owner().unwrap_err(),
        "existing native identity lost peer lock"
    );
    assert_eq!(inventory(&f.parent), before);
    // A malformed final marker is still native classification, including a
    // dangling alias. Its error cannot select the healthy sibling instead.
    fs::remove_file(f.source.join("IDENTITY.json")).unwrap();
    symlink("absent", f.source.join("IDENTITY.json")).unwrap();
    let before = inventory(&f.parent);
    assert_eq!(
        f.owner().unwrap_err(),
        "native identity requires regular files"
    );
    assert_eq!(inventory(&f.parent), before);
}

#[test]
fn history_source_control_configuration_and_reservation_cannot_use_application_route() {
    let f = Fixture::new("application-source-control-config", false);
    let before = inventory(&f.parent);
    assert_eq!(
        SourceOwner::open(
            &f.source,
            &cluster(),
            NodeId(0),
            PlacementGroupId(1),
            Some(reservation())
        )
        .unwrap_err(),
        "native identity is required"
    );
    assert_eq!(inventory(&f.parent), before);
    let f = Fixture::new("application-source-control-native", true);
    drop(StateIdentity::open(&f.source, &cluster(), NodeId(0), Some(reservation())).unwrap());
    let before = inventory(&f.parent);
    assert_eq!(
        f.owner().unwrap_err(),
        "reserved control configuration cannot be omitted"
    );
    assert_eq!(
        SourceOwner::open(
            &f.source,
            &cluster(),
            NodeId(0),
            PlacementGroupId(1),
            Some(reservation())
        )
        .unwrap_err(),
        "reserved control cannot use raw learner synchronization"
    );
    assert_eq!(inventory(&f.parent), before);
    // A reserved sibling binding is not a legacy application binding either.
    let f = Fixture::new("application-source-control-sibling", false);
    drop(StateIdentity::open(&f.binding, &cluster(), NodeId(0), Some(reservation())).unwrap());
    let before = inventory(&f.parent);
    assert_eq!(
        f.owner().unwrap_err(),
        "reserved control configuration cannot be omitted"
    );
    assert_eq!(inventory(&f.parent), before);
}

#[test]
fn application_history_source_route_replacement_is_sticky_after_original_restoration() {
    for parent in [false, true] {
        let f = Fixture::new("application-source-route-swap", false);
        let before = inventory(&f.parent);
        let mut owner = f.owner().unwrap();
        let target = if parent { &f.parent } else { &f.source };
        let saved = f.root.0.join("retained-route");
        fs::rename(target, &saved).unwrap();
        fs::create_dir(target).unwrap();
        assert_eq!(
            owner.validate().unwrap_err(),
            "history source original directory custody changed"
        );
        fs::remove_dir(target).unwrap();
        fs::rename(saved, target).unwrap();
        assert_eq!(
            owner.validate().unwrap_err(),
            "history source custody failed; reopen required"
        );
        assert_eq!(inventory(&f.parent), before);
        drop(owner);
        f.owner().unwrap().validate().unwrap();
    }
}

#[test]
fn application_history_source_identity_replacement_is_sticky_after_original_restoration() {
    let f = Fixture::new("application-source-identity-swap", false);
    let before = inventory(&f.parent);
    let mut owner = f.owner().unwrap();
    let identity = f.binding.join("IDENTITY.json");
    let saved = f.root.0.join("retained-identity");
    let bytes = fs::read(&identity).unwrap();
    fs::rename(&identity, &saved).unwrap();
    fs::write(&identity, bytes).unwrap();
    assert_eq!(
        owner.validate().unwrap_err(),
        "native identity original file custody changed"
    );
    fs::remove_file(&identity).unwrap();
    fs::rename(saved, identity).unwrap();
    assert_eq!(
        owner.validate().unwrap_err(),
        "history source custody failed; reopen required"
    );
    assert_eq!(inventory(&f.parent), before);
    drop(owner);
    f.owner().unwrap().validate().unwrap();
}

#[test]
fn application_history_source_binding_directory_and_lock_substitutions_remain_fenced() {
    for directory in [false, true] {
        let f = Fixture::new("application-source-binding-swap", false);
        let before = inventory(&f.parent);
        let mut owner = f.owner().unwrap();
        let target = if directory {
            f.binding.clone()
        } else {
            f.binding.join(".peer.lock")
        };
        let saved = f.root.0.join("retained-binding");
        fs::rename(&target, &saved).unwrap();
        if directory {
            fs::create_dir(&target).unwrap();
        } else {
            fs::write(&target, b"").unwrap();
        }
        let expected = if directory {
            "native identity root custody changed"
        } else {
            "native identity held peer lock differs from its name"
        };
        assert_eq!(owner.validate().unwrap_err(), expected);
        if directory {
            fs::remove_dir(&target).unwrap();
        } else {
            fs::remove_file(&target).unwrap();
        }
        fs::rename(saved, target).unwrap();
        assert_eq!(
            owner.validate().unwrap_err(),
            "history source custody failed; reopen required"
        );
        assert_eq!(inventory(&f.parent), before);
        drop(owner);
        f.owner().unwrap().validate().unwrap();
    }
}

#[test]
fn application_history_source_marker_appearance_and_identity_unwind_fence_same_owner() {
    let f = Fixture::new("application-source-marker-appears", false);
    let before = inventory(&f.parent);
    let mut owner = f.owner().unwrap();
    let marker = f.source.join("IDENTITY.pending");
    fs::write(&marker, b"not authority").unwrap();
    assert_eq!(
        owner.validate().unwrap_err(),
        "daemon history source acquired native markers"
    );
    fs::remove_file(marker).unwrap();
    assert_eq!(
        owner.validate().unwrap_err(),
        "history source custody failed; reopen required"
    );
    assert_eq!(inventory(&f.parent), before);
    drop(owner);
    let mut owner = f.owner().unwrap();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        crate::peer::state_identity::tests::with_hook(
            |at, _| {
                if at == crate::peer::state_identity::Boundary::BeforeValidate {
                    std::panic::panic_any("source-owner-unwind");
                }
                Ok(())
            },
            || owner.validate(),
        )
    }));
    assert_eq!(
        result.unwrap_err().downcast_ref::<&str>(),
        Some(&"source-owner-unwind")
    );
    assert_eq!(
        owner.validate().unwrap_err(),
        "history source custody failed; reopen required"
    );
    assert_eq!(inventory(&f.parent), before);
    drop(owner);
    f.owner().unwrap().validate().unwrap();
}

#[test]
fn application_history_source_rejects_aliases_and_noncanonical_routes_without_effects() {
    for parent in [false, true] {
        let f = Fixture::new("application-source-alias", false);
        let target = if parent { &f.parent } else { &f.source };
        let saved = f.root.0.join("original-route");
        fs::rename(target, &saved).unwrap();
        symlink(&saved, target).unwrap();
        let before = inventory(&f.root.0);
        assert_eq!(
            f.owner().unwrap_err(),
            "history source route is not an owned directory"
        );
        assert_eq!(inventory(&f.root.0), before);
        fs::remove_file(target).unwrap();
        fs::rename(saved, target).unwrap();
        f.owner().unwrap().validate().unwrap();
    }
    let f = Fixture::new("application-source-wrong-route", false);
    let before = inventory(&f.parent);
    assert_eq!(
        SourceOwner::open(
            &f.parent.join("other"),
            &cluster(),
            NodeId(0),
            PlacementGroupId(1),
            None
        )
        .unwrap_err(),
        "history source requires native identity or the daemon protocol.vsr route"
    );
    assert_eq!(
        SourceOwner::open(
            &f.source.join("../protocol.vsr"),
            &cluster(),
            NodeId(0),
            PlacementGroupId(1),
            None
        )
        .unwrap_err(),
        "history source path contains parent traversal"
    );
    assert_eq!(inventory(&f.parent), before);
}
