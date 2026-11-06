//! Complete held writer names come from that writer, never a disposal proof.
use super::*;

#[derive(Debug)]
pub(super) struct LiveCell {
    directory: Directory,
    segments: Directory,
    root_names: BTreeSet<OsString>,
    segment_names: BTreeSet<OsString>,
    files: Vec<(PathBuf, guard::Stamp)>,
    manifest: guard::Exact,
    locks: Vec<guard::Exact>,
}
impl LiveCell {
    pub(super) fn capture(
        store: &SegmentStore,
        records: u64,
        policy: VsrPrefixLimits,
    ) -> RsfResult<Self> {
        let custody = store.capture_owned_custody()?;
        store.validate_owned_custody(&custody)?;
        let limits = policy.storage()?;
        let count = limits
            .max_records
            .checked_add(1)
            .ok_or_else(|| invalid_input("bounded cell count overflow"))?;
        if store.segments().len() as u64 > count {
            return Err(invalid_input(
                "bounded complete cell exceeds segment capacity",
            ));
        }
        let directory = Directory::capture(store.root())?;
        let segments = Directory::capture(&store.root().join("segments"))?;
        let root_names = names(store.root(), 4)?;
        let required = BTreeSet::from([
            OsString::from("MANIFEST"),
            OsString::from("MANIFEST.lock"),
            OsString::from("segments"),
        ]);
        if !required.is_subset(&root_names)
            || root_names
                .iter()
                .any(|n| !required.contains(n) && n != ".vsr-writer.lock")
        {
            return Err(invalid_input(
                "bounded complete cell has provisional or unknown entries",
            ));
        }
        let (_, manifest) = io::read(&store.root().join("MANIFEST"), limits.max_manifest_bytes)?;
        let mut locks = vec![guard::Exact::capture(
            &store.root().join("MANIFEST.lock"),
            &[],
        )?];
        if root_names.contains(OsStr::new(".vsr-writer.lock")) {
            locks.push(guard::Exact::capture(
                &store.root().join(".vsr-writer.lock"),
                &[],
            )?);
        }
        let maximum = store
            .segments()
            .len()
            .checked_mul(2)
            .ok_or_else(|| invalid_input("bounded segment names overflow"))?;
        let segment_names = names(&segments.path, maximum)?;
        let mut expected = BTreeSet::new();
        let mut files = Vec::new();
        let mut bytes = 0u64;
        for descriptor in store.segments() {
            expected.insert(OsString::from(&descriptor.file_name));
            let path = segments.path.join(&descriptor.file_name);
            let metadata = fs::symlink_metadata(&path).map_err(io_error)?;
            files.push((path.clone(), guard::file_stamp(&metadata)?));
            bytes = bytes
                .checked_add(metadata.len())
                .ok_or_else(|| invalid_input("bounded cell size overflow"))?;
            if bytes > limits.max_encoded_bytes {
                return Err(invalid_input(
                    "bounded complete cell exceeds physical capacity",
                ));
            }
            let lock = path.with_extension("lock");
            if segment_names.contains(lock.file_name().expect("named segment sidecar")) {
                expected.insert(lock.file_name().expect("named sidecar").to_os_string());
                locks.push(guard::Exact::capture(&lock, &[])?);
            }
        }
        if segment_names != expected {
            return Err(corrupt("bounded complete cell has unknown segment names"));
        }
        // These records belong to an actual validated flat learner writer.
        // Normalize physical length with the existing framing contract, without
        // rereading WAL or treating a disposable descriptor count as evidence.
        let overhead = rsf_storage::wal_record_disk_bytes(0)?
            .checked_sub(18)
            .and_then(|framing| framing.checked_mul(records))
            .ok_or_else(|| corrupt("bounded actual history framing overflow"))?;
        let encoded = bytes
            .checked_sub(overhead)
            .ok_or_else(|| corrupt("bounded actual history length differs from record count"))?;
        policy.check(records, encoded)?;
        let result = Self {
            directory,
            segments,
            root_names,
            segment_names,
            files,
            manifest,
            locks,
        };
        result.validate()?;
        store.validate_owned_custody(&custody)?;
        Ok(result)
    }
    pub(super) fn validate(&self) -> RsfResult<()> {
        self.validate_names(false)
    }
    fn validate_names(&self, newly_created_writer: bool) -> RsfResult<()> {
        self.directory.validate()?;
        self.segments.validate()?;
        let mut root_names = self.root_names.clone();
        if newly_created_writer {
            root_names.insert(OsString::from(".vsr-writer.lock"));
        }
        if names(&self.directory.path, 4)? != root_names
            || names(&self.segments.path, self.segment_names.len())? != self.segment_names
        {
            return Err(corrupt("bounded complete cell namespace changed"));
        }
        self.manifest.validate()?;
        for (path, stamp) in &self.files {
            if guard::file_stamp(&fs::symlink_metadata(path).map_err(io_error)?)? != *stamp {
                return Err(corrupt("bounded complete cell original file changed"));
            }
        }
        for lock in &self.locks {
            lock.validate()?;
        }
        self.directory.validate()?;
        self.segments.validate()
    }
    pub(super) fn completed(&mut self, candidate: &Published) -> RsfResult<()> {
        let created = !self.root_names.contains(OsStr::new(".vsr-writer.lock"));
        self.validate_names(created)?;
        let exact = guard::owned_lock(
            &self.directory.path.join(".vsr-writer.lock"),
            &candidate.cell._writer_lock,
        )?;
        if created {
            self.root_names.insert(OsString::from(".vsr-writer.lock"));
            self.locks.push(exact);
        }
        self.validate()
    }
}
