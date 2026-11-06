use super::*;
use std::sync::atomic::{AtomicU64, Ordering};
mod boundaries;
mod refusal;

fn root(label: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    loop {
        let path = std::env::temp_dir().join(format!(
            "rsf-provisional-discard-{label}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        match fs::create_dir(&path) {
            Ok(()) => return path,
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => panic!("cannot reserve discard fixture: {e}"),
        }
    }
}
fn limits() -> SegmentReadLimits {
    SegmentReadLimits {
        max_manifest_bytes: 16384,
        max_records: 32,
        max_encoded_bytes: 1024 * 1024,
        max_record_bytes: 1024 * 1024,
    }
}
fn append(store: &mut SegmentStore, bytes: &[u8]) {
    store
        .append_next_wal(
            PlacementGroupId(1),
            rsf_core::Epoch(1),
            WalRecordKind::Prepare,
            bytes,
        )
        .unwrap();
}
fn populated(path: &Path) -> SegmentStore {
    let mut options = SegmentStoreOptions {
        max_segment_bytes: 1,
        ..SegmentStoreOptions::default()
    };
    options.log_options.sync_policy = crate::SyncPolicy::Full;
    let mut store = SegmentStore::open_with_options(path, options).unwrap();
    append(&mut store, b"first");
    append(&mut store, b"second");
    store
}
struct Fixture {
    root: PathBuf,
    a: PathBuf,
    b: SegmentStore,
    proof: SegmentStoreCustody,
    parent: Identity,
}
impl Fixture {
    fn new(label: &str) -> Self {
        let root = root(label);
        let a = root.join("A");
        let b = populated(&root.join("B"));
        let proof = b.capture_owned_custody().unwrap();
        let parent = directory_identity(&root).unwrap();
        Self {
            root,
            a,
            b,
            proof,
            parent,
        }
    }
    fn validate(&self) -> RsfResult<()> {
        if directory_identity(&self.root)? != self.parent {
            return Err(custody_error("actual parent identity changed"));
        }
        self.b.validate_owned_custody(&self.proof)
    }
}
fn image(root: &Path) -> BTreeMap<PathBuf, Option<Vec<u8>>> {
    fn walk(base: &Path, path: &Path, out: &mut BTreeMap<PathBuf, Option<Vec<u8>>>) {
        for entry in fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            let relative = path.strip_prefix(base).unwrap().to_path_buf();
            let kind = entry.file_type().unwrap();
            if kind.is_symlink() {
                out.insert(
                    relative,
                    Some(format!("alias:{}", fs::read_link(path).unwrap().display()).into_bytes()),
                );
            } else if kind.is_dir() {
                out.insert(relative, None);
                walk(base, &path, out);
            } else {
                out.insert(relative, Some(fs::read(path).unwrap()));
            }
        }
    }
    let mut out = BTreeMap::new();
    walk(root, root, &mut out);
    out
}
fn discard(fixture: &Fixture) -> RsfResult<()> {
    let inventory = SegmentStore::inspect_provisional_discard(&fixture.a, limits()).map_err(
        |error| match error {
            SegmentReadError::Failure(error) => error,
            _ => custody_error(&format!("unexpected fixture limit: {error:?}")),
        },
    )?;
    assert_eq!(inventory.other_root_names().len(), 0);
    inventory
        .remove_children(|| fixture.validate())?
        .finish(|| fixture.validate())
}
fn rotation_residue(path: &Path, prefix: usize) {
    let mut store = populated(path);
    let proof = store.capture_owned_custody().unwrap();
    let injected = observation::stop_rotation_after_prefix(prefix);
    let error = store
        .try_append_owned_wal_group(
            &proof,
            PlacementGroupId(1),
            [Ok(crate::WalGroupEntry {
                epoch: rsf_core::Epoch(1),
                kind: WalRecordKind::Prepare,
                payload: b"third",
            })],
        )
        .unwrap_err();
    assert_eq!(
        error.message,
        "injected actual rotation temporary interruption (test only)"
    );
    assert!(store.write_failed);
    assert!(path.join("MANIFEST.tmp").is_file());
    drop(injected);
    drop(store);
}
#[test]
fn discard_is_read_only_bounded_inventory_then_two_phase_without_recovery_or_lock_creation() {
    let fixture = Fixture::new("two-phase");
    drop(populated(&fixture.a));
    let auxiliary = fixture.a.join("caller-auxiliary");
    fs::write(&auxiliary, []).unwrap();
    let auxiliary_stamp = regular_file(&auxiliary).unwrap();
    let before = image(&fixture.root);
    let reads = crate::reader::payload_reads();
    let inventory = SegmentStore::inspect_provisional_discard(&fixture.a, limits()).unwrap();
    assert_eq!(
        inventory.other_root_names().collect::<Vec<_>>(),
        vec![OsStr::new("caller-auxiliary")]
    );
    inventory.validate().unwrap();
    assert_eq!(image(&fixture.root), before);
    assert_eq!(crate::reader::payload_reads(), reads);
    // Exact auxiliary preflight belongs to this test's domain owner.
    assert_eq!(regular_file(&auxiliary).unwrap(), auxiliary_stamp);
    assert_eq!(auxiliary_stamp.bytes, 0);
    let remainder = inventory.remove_children(|| fixture.validate()).unwrap();
    assert_eq!(
        names(&fixture.a, 2).unwrap(),
        BTreeSet::from([
            OsString::from(MANIFEST_FILE),
            OsString::from("caller-auxiliary")
        ])
    );
    assert_eq!(regular_file(&auxiliary).unwrap(), auxiliary_stamp);
    fs::remove_file(&auxiliary).unwrap();
    File::open(&fixture.a).unwrap().sync_all().unwrap();
    remainder.finish(|| fixture.validate()).unwrap();
    assert!(!fixture.a.exists());
    assert_eq!(crate::reader::payload_reads(), reads);
    fixture.validate().unwrap();
    let root = fixture.root.clone();
    drop(fixture);
    fs::remove_dir_all(root).unwrap();
}
#[test]
fn discard_accepts_actual_factory_and_owned_rotation_interruption_prefixes_without_parsing_wal() {
    for prefix in [0, 1, usize::MAX] {
        let fixture = Fixture::new("producer-prefix");
        rotation_residue(&fixture.a, prefix);
        let b = image(&fixture.root.join("B"));
        let reads = crate::reader::payload_reads();
        discard(&fixture).unwrap();
        assert_eq!(crate::reader::payload_reads(), reads);
        assert_eq!(image(&fixture.root.join("B")), b);
        let root = fixture.root.clone();
        drop(fixture);
        fs::remove_dir_all(root).unwrap();
    }
    for boundary in 0..8 {
        let fixture = Fixture::new("factory-prefix");
        let failure =
            crate::store::initialization::test_interrupt_factory(&fixture.a, boundary).unwrap_err();
        assert_eq!(
            failure.message,
            "injected actual initializer boundary interruption (test only)"
        );
        discard(&fixture).unwrap();
        let root = fixture.root.clone();
        drop(fixture);
        fs::remove_dir_all(root).unwrap();
    }
}
#[test]
fn discard_exact_segment_budget_excludes_metadata_and_limits_refuse_without_mutation() {
    let fixture = Fixture::new("limits");
    drop(populated(&fixture.a));
    let before = image(&fixture.root);
    let segment_bytes = fs::read_dir(fixture.a.join(SEGMENT_DIR))
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension() == Some(OsStr::new("segment")))
        .map(|p| fs::metadata(p).unwrap().len())
        .sum::<u64>();
    let manifest_bytes = fs::metadata(fixture.a.join(MANIFEST_FILE)).unwrap().len() as usize;
    let exact = SegmentReadLimits {
        max_manifest_bytes: manifest_bytes,
        max_encoded_bytes: segment_bytes,
        ..limits()
    };
    SegmentStore::inspect_provisional_discard(&fixture.a, exact)
        .unwrap()
        .validate()
        .unwrap();
    for (policy, kind) in [
        (
            SegmentReadLimits {
                max_encoded_bytes: segment_bytes - 1,
                ..exact
            },
            SegmentReadLimit::EncodedBytes,
        ),
        (
            SegmentReadLimits {
                max_manifest_bytes: manifest_bytes - 1,
                ..exact
            },
            SegmentReadLimit::ManifestBytes,
        ),
    ] {
        assert!(
            matches!(SegmentStore::inspect_provisional_discard(&fixture.a,policy),Err(SegmentReadError::LimitExceeded{kind:actual,..}) if actual==kind)
        );
        assert_eq!(image(&fixture.root), before);
    }
    discard(&fixture).unwrap();
    let root = fixture.root.clone();
    drop(fixture);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn discard_declared_name_and_temporary_metadata_caps_remain_independent_of_segment_data() {
    let fixture = Fixture::new("declared-cap");
    let mut store = populated(&fixture.a);
    append(&mut store, b"third");
    drop(store);
    let before = image(&fixture.root);
    assert!(matches!(
        SegmentStore::inspect_provisional_discard(
            &fixture.a,
            SegmentReadLimits {
                max_records: 1,
                ..limits()
            }
        ),
        Err(SegmentReadError::LimitExceeded {
            kind: SegmentReadLimit::Records,
            ..
        })
    ));
    assert_eq!(image(&fixture.root), before);
    discard(&fixture).unwrap();
    rotation_residue(&fixture.a, usize::MAX);
    let manifest = fs::metadata(fixture.a.join(MANIFEST_FILE)).unwrap().len() as usize;
    assert!(fs::metadata(fixture.a.join("MANIFEST.tmp")).unwrap().len() > manifest as u64);
    let before = image(&fixture.root);
    assert!(matches!(
        SegmentStore::inspect_provisional_discard(
            &fixture.a,
            SegmentReadLimits {
                max_manifest_bytes: manifest,
                ..limits()
            }
        ),
        Err(SegmentReadError::LimitExceeded {
            kind: SegmentReadLimit::ManifestBytes,
            ..
        })
    ));
    assert_eq!(image(&fixture.root), before);
    discard(&fixture).unwrap();
    let root = fixture.root.clone();
    drop(fixture);
    fs::remove_dir_all(root).unwrap();
}
