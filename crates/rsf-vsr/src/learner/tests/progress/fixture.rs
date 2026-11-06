use super::*;
use std::sync::atomic::{AtomicU64, Ordering};

pub(super) const FENCED: &str = "learner durability failed; reopen before further operations";
pub(super) struct Fixture {
    pub root: PathBuf,
    pub target: PathBuf,
    pub snapshot: VsrHistorySnapshot,
    pub committed: Vec<VsrCommittedEntry>,
}
impl Fixture {
    pub fn new(label: &str) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = loop {
            let p = std::env::temp_dir().join(format!(
                "rsf-learner-progress-{label}-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            match fs::create_dir(&p) {
                Ok(()) => break p,
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(e) => panic!("reserve fixture: {e}"),
            }
        };
        let mut source = source(&root.join("source"));
        commit(&mut source, 1);
        let snapshot = source.history_snapshot().unwrap();
        let committed = source.committed_prefix().unwrap();
        assert!(snapshot.records.len() > 2);
        drop(source);
        Self {
            target: root.join("target"),
            root,
            snapshot,
            committed,
        }
    }
    pub fn id(&self) -> [u8; 32] {
        self.snapshot.descriptor.id().unwrap()
    }
    pub fn cell(&self) -> PathBuf {
        let id = self
            .id()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        self.target.join(id).join("cell")
    }
    // Actual ordinary BEGIN produces descriptor/factory metadata. A real
    // storage writer copies the actual history into small rotated segments;
    // no manifest or WAL frame is manufactured and no policy is relaxed.
    pub fn retained(&self, count: usize) {
        let mut owner = learner(&self.target);
        assert_eq!(owner.begin(self.snapshot.descriptor.clone()).unwrap(), 0);
        drop(owner);
        let options = SegmentStoreOptions {
            max_segment_bytes: 1,
            log_options: rsf_storage::SegmentLogOptions {
                sync_policy: rsf_storage::SyncPolicy::Data,
            },
        };
        let mut store = SegmentStore::open_with_options(self.cell(), options).unwrap();
        for record in self.snapshot.records.iter().take(count) {
            store
                .append_next_wal(
                    PlacementGroupId(1),
                    record.epoch,
                    record.kind,
                    &record.payload,
                )
                .unwrap();
        }
        drop(store);
        let segments = segment_files(&self.cell());
        assert!(segments.len() >= 2);
        assert!(fs::metadata(segments.last().unwrap()).unwrap().len() > 0);
        assert!(
            segments[..segments.len() - 1]
                .iter()
                .any(|p| fs::metadata(p).unwrap().len() > 0)
        );
    }
    pub fn publish(&self) {
        self.retained(self.snapshot.records.len());
        let mut owner = learner(&self.target);
        assert_eq!(
            owner.begin(self.snapshot.descriptor.clone()).unwrap(),
            self.snapshot.descriptor.records
        );
        assert_eq!(owner.finish(self.id()).unwrap(), LogIndex(1));
        self.assert_published(&owner);
    }
    pub fn assert_published(&self, owner: &LearnerHistoryStore) {
        assert_eq!(owner.committed_entries().unwrap(), self.committed);
        assert_eq!(owner.published_history_id().unwrap(), Some(self.id()));
        assert_eq!(
            owner
                .published
                .as_ref()
                .unwrap()
                .cell
                .request_commit(&VsrClientRequestId::new("client", 1)),
            Some(LogIndex(1))
        );
    }
    pub fn strict_open(&self) -> RsfResult<LearnerHistoryStore> {
        LearnerHistoryStore::open_published_existing(
            &self.target,
            PlacementGroupId(1),
            NodeId(9),
            NodeId(0),
            VsrPrefixLimits {
                max_records: 100,
                max_encoded_bytes: 1024 * 1024,
            },
        )
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.root).unwrap();
    }
}
pub(super) fn segment_files(cell: &Path) -> Vec<PathBuf> {
    let mut paths = fs::read_dir(cell.join("segments"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "segment"))
        .collect::<Vec<_>>();
    paths.sort();
    paths
}
pub(super) fn active(cell: &Path) -> PathBuf {
    segment_files(cell).pop().unwrap()
}
pub(super) fn identity(path: &Path) -> (u64, u64) {
    let m = fs::symlink_metadata(path).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        (m.dev(), m.ino())
    }
    #[cfg(not(unix))]
    {
        let _ = m;
        (0, 0)
    }
}
#[derive(Debug, PartialEq, Eq)]
pub(super) struct Entry(PathBuf, (u64, u64), Option<Vec<u8>>);
pub(super) fn inventory(root: &Path) -> Vec<Entry> {
    fn visit(root: &Path, p: &Path, out: &mut Vec<Entry>) {
        let m = fs::symlink_metadata(p).unwrap();
        assert!(!m.file_type().is_symlink());
        out.push(Entry(
            p.strip_prefix(root).unwrap().to_path_buf(),
            identity(p),
            if m.is_dir() {
                None
            } else {
                Some(fs::read(p).unwrap())
            },
        ));
        if m.is_dir() {
            for e in fs::read_dir(p).unwrap() {
                visit(root, &e.unwrap().path(), out);
            }
        }
    }
    let mut out = Vec::new();
    visit(root, root, &mut out);
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}
pub(super) fn fenced(owner: &mut LearnerHistoryStore, snapshot: &VsrHistorySnapshot) {
    assert_eq!(owner.committed_entries().unwrap_err().message, FENCED);
    assert_eq!(
        owner
            .begin(snapshot.descriptor.clone())
            .unwrap_err()
            .message,
        FENCED
    );
    assert_eq!(
        owner
            .finish(snapshot.descriptor.id().unwrap())
            .unwrap_err()
            .message,
        FENCED
    );
}
pub(super) fn not_found(error: &RsfError, missing: &Path) {
    let cause = fs::symlink_metadata(missing).unwrap_err();
    assert_eq!(cause.kind(), std::io::ErrorKind::NotFound);
    assert_eq!(error.code, ErrorCode::InvalidInput);
    assert_eq!(error.message, format!("storage Recover failed: {cause}"));
}
