//! Finite exact names, original descriptors and authorized factory transitions.
use super::*;
const AUXILIARY: &str = ".vsr-writer.lock";
#[derive(Debug)]
pub(super) struct Inventory {
    root: Identity,
    directory: Option<Identity>,
    auxiliary: FileStamp,
    pub(super) files: BTreeMap<PathBuf, (File, FileStamp)>,
}
impl Inventory {
    pub(super) fn capture(
        root: &Path,
        auxiliary: &crate::ExclusiveFileLock,
        frame: &[u8],
    ) -> RsfResult<Self> {
        let root_identity = directory_identity(root)?;
        let auxiliary_stamp = file_stamp(&auxiliary.metadata().map_err(custody_io)?)?;
        if auxiliary_stamp.bytes != 0 || regular_file(&root.join(AUXILIARY))? != auxiliary_stamp {
            return Err(custody_error(
                "bootstrap auxiliary differs from original held lock",
            ));
        }
        let mut result = Self {
            root: root_identity,
            directory: None,
            auxiliary: auxiliary_stamp,
            files: BTreeMap::new(),
        };
        // Hard finite inventory (five possible root names, never unbounded
        // collection). The segment directory permits exactly data plus sidecar.
        for (count, entry) in fs::read_dir(root).map_err(custody_io)?.enumerate() {
            let entry = entry.map_err(custody_io)?;
            if count >= 5 {
                return Err(custody_error("bootstrap root inventory exceeds bound"));
            }
            let name = entry.file_name();
            if name == AUXILIARY {
                continue;
            }
            if name == SEGMENT_DIR {
                result.directory = Some(directory_identity(&entry.path())?);
            } else if name == MANIFEST_FILE || name == "MANIFEST.tmp" || name == "MANIFEST.lock" {
                result.capture_file(root, Path::new(&name))?;
            } else {
                return Err(custody_error("foreign bootstrap root child"));
            }
        }
        let manifest = SegmentManifest::initial();
        let segment = PathBuf::from(SEGMENT_DIR).join(&manifest.segments[0].file_name);
        let lock = segment.with_extension("lock");
        if result.directory.is_some() {
            for (count, entry) in fs::read_dir(root.join(SEGMENT_DIR))
                .map_err(custody_io)?
                .enumerate()
            {
                let entry = entry.map_err(custody_io)?;
                let name = PathBuf::from(SEGMENT_DIR).join(entry.file_name());
                if count >= 2 || (name != segment && name != lock) {
                    return Err(custody_error("foreign bootstrap segment child"));
                }
                result.capture_file(root, &name)?;
            }
        }
        let final_present = result.files.contains_key(Path::new(MANIFEST_FILE));
        let temporary_present = result.files.contains_key(Path::new("MANIFEST.tmp"));
        if (final_present && temporary_present)
            || ((final_present || temporary_present) && !result.files.contains_key(&segment))
            || (result.files.contains_key(&lock) && !result.files.contains_key(&segment))
        {
            return Err(custody_error("bootstrap inventory is not a factory prefix"));
        }
        let manifest_bytes = manifest.encode();
        for (name, (file, stamp)) in &mut result.files {
            if name == Path::new("MANIFEST.lock") || *name == lock {
                if stamp.bytes != 0 {
                    return Err(custody_error("bootstrap lock is not empty"));
                }
            } else if name == Path::new(MANIFEST_FILE) || name == Path::new("MANIFEST.tmp") {
                let length = read_prefix(file, stamp, manifest_bytes.as_bytes())?;
                if name == Path::new(MANIFEST_FILE) && length != manifest_bytes.len() {
                    return Err(custody_error("final bootstrap manifest incomplete"));
                }
            } else if *name == segment {
                read_prefix(file, stamp, frame)?;
                if !final_present && stamp.bytes != 0 {
                    return Err(custody_error("bootstrap WAL precedes final manifest"));
                }
            }
        }
        result.validate(root, auxiliary)?;
        Ok(result)
    }
    fn capture_file(&mut self, root: &Path, name: &Path) -> RsfResult<()> {
        let path = root.join(name);
        let expected = regular_file(&path)?;
        let mut options = OpenOptions::new();
        options.read(true).append(true);
        no_follow(&mut options);
        let file = options.open(&path).map_err(custody_io)?;
        if file_stamp(&file.metadata().map_err(custody_io)?)? != expected {
            return Err(custody_error("bootstrap file changed during capture"));
        }
        self.files.insert(name.to_owned(), (file, expected));
        Ok(())
    }
    pub(super) fn validate(
        &self,
        root: &Path,
        auxiliary: &crate::ExclusiveFileLock,
    ) -> RsfResult<()> {
        if directory_identity(root)? != self.root
            || regular_file(&root.join(AUXILIARY))? != self.auxiliary
            || file_stamp(&auxiliary.metadata().map_err(custody_io)?)? != self.auxiliary
        {
            return Err(custody_error(
                "bootstrap original root or auxiliary custody lost",
            ));
        }
        match &self.directory {
            Some(expected) if directory_identity(&root.join(SEGMENT_DIR))? != *expected => {
                return Err(custody_error("bootstrap segment directory changed"));
            }
            None if exists(&root.join(SEGMENT_DIR))? => {
                return Err(custody_error("bootstrap segment directory appeared"));
            }
            _ => {}
        }
        for (name, (file, stamp)) in &self.files {
            if regular_file(&root.join(name))? != *stamp
                || file_stamp(&file.metadata().map_err(custody_io)?)? != *stamp
            {
                return Err(custody_error("bootstrap original file custody lost"));
            }
        }
        let mut seen = 0;
        for (count, entry) in fs::read_dir(root).map_err(custody_io)?.enumerate() {
            let entry = entry.map_err(custody_io)?;
            let name = entry.file_name();
            if count >= 5 {
                return Err(custody_error("bootstrap root inventory overflow"));
            }
            if name == AUXILIARY || (name == SEGMENT_DIR && self.directory.is_some()) {
                continue;
            }
            if !self.files.contains_key(Path::new(&name)) {
                return Err(custody_error("bootstrap root inventory changed"));
            }
            seen += 1;
        }
        if self.directory.is_some() {
            for (count, entry) in fs::read_dir(root.join(SEGMENT_DIR))
                .map_err(custody_io)?
                .enumerate()
            {
                let name = PathBuf::from(SEGMENT_DIR).join(entry.map_err(custody_io)?.file_name());
                if count >= 2 || !self.files.contains_key(&name) {
                    return Err(custody_error("bootstrap segment inventory changed"));
                }
                seen += 1;
            }
        }
        if seen != self.files.len() {
            return Err(custody_error("bootstrap inventory disappeared"));
        }
        Ok(())
    }
    pub(super) fn create_directory(
        &mut self,
        root: &Path,
        auxiliary: &crate::ExclusiveFileLock,
    ) -> RsfResult<()> {
        self.validate(root, auxiliary)?;
        if self.directory.is_none() {
            fs::create_dir(root.join(SEGMENT_DIR)).map_err(custody_io)?;
            self.directory = Some(directory_identity(&root.join(SEGMENT_DIR))?);
        }
        crate::sync_existing_directory(root.join(SEGMENT_DIR)).map_err(custody_io)?;
        self.validate(root, auxiliary)
    }
    pub(super) fn ensure_file(
        &mut self,
        root: &Path,
        name: &Path,
        auxiliary: &crate::ExclusiveFileLock,
    ) -> RsfResult<()> {
        self.validate(root, auxiliary)?;
        if !self.files.contains_key(name) {
            let mut options = OpenOptions::new();
            options.read(true).append(true).create_new(true);
            no_follow(&mut options);
            let file = options.open(root.join(name)).map_err(custody_io)?;
            let stamp = file_stamp(&file.metadata().map_err(custody_io)?)?;
            if stamp.bytes != 0 || regular_file(&root.join(name))? != stamp {
                return Err(custody_error("bootstrap created file changed"));
            }
            self.files.insert(name.to_owned(), (file, stamp));
        }
        self.files[name].0.sync_all().map_err(custody_io)?;
        crate::durable_directory::sync_file_publication(&root.join(name)).map_err(custody_io)?;
        self.validate(root, auxiliary)
    }
    pub(super) fn rename_manifest(
        &mut self,
        root: &Path,
        auxiliary: &crate::ExclusiveFileLock,
    ) -> RsfResult<()> {
        self.validate(root, auxiliary)?;
        if exists(&root.join(MANIFEST_FILE))? {
            return Err(custody_error("bootstrap final manifest appeared"));
        }
        fs::rename(root.join("MANIFEST.tmp"), root.join(MANIFEST_FILE)).map_err(custody_io)?;
        let entry = self
            .files
            .remove(Path::new("MANIFEST.tmp"))
            .ok_or_else(|| custody_error("bootstrap temporary absent"))?;
        // Rename preserves inode/length/mtime; no metadata is silently refreshed.
        self.files.insert(PathBuf::from(MANIFEST_FILE), entry);
        self.validate(root, auxiliary)
    }
}
fn exists(path: &Path) -> RsfResult<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(custody_io(error)),
    }
}
fn no_follow(options: &mut OpenOptions) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    #[cfg(not(unix))]
    let _ = options;
}
pub(super) fn read_prefix(
    file: &mut File,
    expected_stamp: &FileStamp,
    expected: &[u8],
) -> RsfResult<usize> {
    if expected_stamp.bytes > expected.len() as u64 {
        return Err(custody_error("bootstrap prefix exceeds canonical bound"));
    }
    if file_stamp(&file.metadata().map_err(custody_io)?)? != *expected_stamp {
        return Err(custody_error("bootstrap prefix descriptor changed"));
    }
    file.seek(SeekFrom::Start(0)).map_err(custody_io)?;
    let mut bytes = vec![0; expected_stamp.bytes as usize];
    file.read_exact(&mut bytes).map_err(custody_io)?;
    let mut extra = [0];
    if file.read(&mut extra).map_err(custody_io)? != 0
        || !expected.starts_with(&bytes)
        || file_stamp(&file.metadata().map_err(custody_io)?)? != *expected_stamp
    {
        return Err(custody_error(
            "actual bootstrap bytes are not the exact canonical prefix",
        ));
    }
    Ok(bytes.len())
}
