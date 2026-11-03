//! Process-local custody of one sealed stage, independent of the hosted cell.
use super::*;
use std::io::Read;
use std::time::SystemTime;

#[derive(Clone, Debug, Eq, PartialEq)]
struct Identity {
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
}
#[derive(Clone, Debug, Eq, PartialEq)]
struct Stamp {
    identity: Identity,
    bytes: u64,
    modified: SystemTime,
}
#[derive(Clone, Debug)]
pub(super) struct Exact {
    stamp: Stamp,
    bytes: Vec<u8>,
}
impl Exact {
    pub(super) fn optional(path: &Path) -> RsfResult<Option<Self>> {
        let metadata = match fs::symlink_metadata(path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(io_error(error)),
        };
        let stamp = file_stamp(&metadata)?;
        if stamp.bytes > MAX_HISTORY_RECORD_BYTES as u64 {
            return Err(corrupt("stage metadata exceeds bounded custody"));
        }
        let bytes = read_checked(path, &stamp)?;
        Ok(Some(Self { stamp, bytes }))
    }
    pub(super) fn expected(path: &Path, expected: &[u8]) -> RsfResult<Self> {
        let proof = Self::optional(path)?.ok_or_else(|| corrupt("stage metadata is missing"))?;
        if proof.bytes != expected {
            return Err(corrupt("stage metadata differs from expected owned bytes"));
        }
        Ok(proof)
    }
    pub(super) fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    fn validate(&self, path: &Path) -> RsfResult<()> {
        if read_checked(path, &self.stamp)? != self.bytes {
            return Err(corrupt("stage metadata content changed"));
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(super) struct Owner {
    root: PathBuf,
    identity: Identity,
    lock: Stamp,
    _guard: rsf_storage::ExclusiveFileLock,
}
impl Owner {
    pub(super) fn acquire(root: &Path) -> RsfResult<Self> {
        Self::acquire_with(root, |_| {})
    }
    // A private deterministic fault boundary for the check/create race. Public
    // stage callers cannot supply a hook or bypass the opened-file checks.
    fn acquire_with(root: &Path, before_open: impl FnOnce(&Path)) -> RsfResult<Self> {
        let mut owned = PathBuf::new();
        for component in root.components() {
            if component != std::path::Component::CurDir {
                owned.push(component.as_os_str());
            }
        }
        if owned.as_os_str().is_empty() {
            owned.push(".");
        }
        let root = owned.as_path();
        let identity = directory(root)?;
        let path = root.join(".staging.lock");
        match fs::symlink_metadata(&path) {
            Ok(metadata) => {
                file_stamp(&metadata)?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(io_error(error)),
        }
        before_open(&path);
        let mut options = OpenOptions::new();
        options.create(true).truncate(false).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            // Post-open identity checks cannot undo creation through a
            // substituted final symlink to a previously absent foreign target.
            options.custom_flags(libc::O_NOFOLLOW);
        }
        let file = options.open(&path).map_err(io_error)?;
        let lock = file_stamp(&file.metadata().map_err(io_error)?)?;
        if file_stamp(&fs::symlink_metadata(&path).map_err(io_error)?)? != lock {
            return Err(corrupt("stage lock path differs from opened owner"));
        }
        let guard = rsf_storage::ExclusiveFileLock::acquire(file).map_err(|error| {
            invalid_input(format!("membership staging store already owned: {error}"))
        })?;
        let owner = Self {
            root: root.to_path_buf(),
            identity,
            lock,
            _guard: guard,
        };
        owner.validate()?;
        Ok(owner)
    }
    pub(super) fn validate(&self) -> RsfResult<()> {
        if directory(&self.root)? != self.identity
            || file_stamp(
                &fs::symlink_metadata(self.root.join(".staging.lock")).map_err(io_error)?,
            )? != self.lock
        {
            return Err(corrupt("stage root or lock custody changed"));
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(super) struct Source {
    inputs: (PathBuf, Identity),
    input: (PathBuf, Identity),
    descriptor: Exact,
    records: rsf_storage::SegmentReadCustody,
}
impl Source {
    pub(super) fn read(
        root: &Path,
        input: &VsrMembershipInput,
    ) -> RsfResult<(Self, Vec<VsrHistoryRecord>)> {
        let inputs = root.join("inputs");
        let name = input
            .id()?
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let path = inputs.join(name);
        let inputs_identity = directory(&inputs)?;
        let input_identity = directory(&path)?;
        let descriptor = Exact::expected(&path.join("DESCRIPTOR"), &input.encode()?)?;
        let (records, stamp) = super::read_history_at(
            &path.join("records"),
            input.proposal().election.pg,
            input.proposal().election.target_view,
            input.records()?,
        )?;
        let source = Self {
            inputs: (inputs, inputs_identity),
            input: (path, input_identity),
            descriptor,
            records: stamp,
        };
        source.validate()?;
        Ok((source, records))
    }
    pub(super) fn validate(&self) -> RsfResult<()> {
        if directory(&self.inputs.0)? != self.inputs.1 || directory(&self.input.0)? != self.input.1
        {
            return Err(corrupt("stage input directory custody changed"));
        }
        self.descriptor.validate(&self.input.0.join("DESCRIPTOR"))?;
        self.records.validate()
    }
}

#[derive(Debug)]
pub(super) struct StageCustody {
    owner: Owner,
    source: Source,
    selected: rsf_storage::SegmentReadCustody,
    meta: Exact,
    installed: Option<Exact>,
    publication: Option<Exact>,
}
impl StageCustody {
    pub(super) fn new(
        owner: Owner,
        source: Source,
        selected: rsf_storage::SegmentReadCustody,
        meta: Exact,
        installed: Option<Exact>,
        publication: Option<Exact>,
    ) -> RsfResult<Self> {
        let custody = Self {
            owner,
            source,
            selected,
            meta,
            installed,
            publication,
        };
        custody.validate()?;
        Ok(custody)
    }
    fn base(&self) -> RsfResult<()> {
        self.owner.validate()?;
        self.source.validate()?;
        self.selected.validate()
    }
    pub(super) fn validate(&self) -> RsfResult<()> {
        self.base()?;
        self.meta.validate(&self.owner.root.join("META"))?;
        self.validate_installed()?;
        optional(
            &self.owner.root.join("PUBLICATION"),
            self.publication.as_ref(),
        )
    }
    fn validate_installed(&self) -> RsfResult<()> {
        optional(&self.owner.root.join("INSTALLED"), self.installed.as_ref())
    }
    pub(super) fn meta_bytes(&self) -> &[u8] {
        self.meta.bytes()
    }
    pub(super) fn installed_bytes(&self) -> Option<&[u8]> {
        self.installed.as_ref().map(Exact::bytes)
    }
    pub(super) fn publication_bytes(&self) -> Option<&[u8]> {
        self.publication.as_ref().map(Exact::bytes)
    }
    // Only after this owner atomically wrote the exact expected receipt. All
    // unchanged files are still compared with their original observations.
    pub(super) fn renew_installed(&mut self, bytes: &[u8]) -> RsfResult<()> {
        self.base()?;
        self.meta.validate(&self.owner.root.join("META"))?;
        optional(
            &self.owner.root.join("PUBLICATION"),
            self.publication.as_ref(),
        )?;
        let installed = Exact::expected(&self.owner.root.join("INSTALLED"), bytes)?;
        self.installed = Some(installed);
        self.validate()
    }
    // PUBLICATION may be newly written, but META must still be the original.
    // An already retained first proof may never be replaced even by equal bytes.
    pub(super) fn pending_publication(&self, bytes: &[u8]) -> RsfResult<Exact> {
        self.base()?;
        self.meta.validate(&self.owner.root.join("META"))?;
        self.validate_installed()?;
        if let Some(previous) = &self.publication {
            previous.validate(&self.owner.root.join("PUBLICATION"))?;
            if previous.bytes() != bytes {
                return Err(corrupt("stage first publication proof changed"));
            }
            return Ok(previous.clone());
        }
        Exact::expected(&self.owner.root.join("PUBLICATION"), bytes)
    }
    pub(super) fn validate_pending(&self, publication: &Exact) -> RsfResult<()> {
        self.base()?;
        self.meta.validate(&self.owner.root.join("META"))?;
        self.validate_installed()?;
        publication.validate(&self.owner.root.join("PUBLICATION"))
    }
    // Only the owner's completed V1->V2 transaction may replace META.
    pub(super) fn bind_publication(&mut self, bytes: &[u8], publication: Exact) -> RsfResult<()> {
        self.base()?;
        self.validate_installed()?;
        publication.validate(&self.owner.root.join("PUBLICATION"))?;
        let meta = Exact::expected(&self.owner.root.join("META"), bytes)?;
        self.meta = meta;
        self.publication = Some(publication);
        self.validate()
    }
}

pub(super) fn absent(path: &Path) -> RsfResult<()> {
    optional(path, None)
}
fn optional(path: &Path, expected: Option<&Exact>) -> RsfResult<()> {
    if let Some(expected) = expected {
        return expected.validate(path);
    }
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(io_error(error)),
        Ok(_) => Err(corrupt(
            "unexpected stage metadata appeared during ownership",
        )),
    }
}
fn directory(path: &Path) -> RsfResult<Identity> {
    let metadata = fs::symlink_metadata(path).map_err(io_error)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(corrupt("stage directory is missing or aliased"));
    }
    identity(&metadata)
}
fn file_stamp(metadata: &fs::Metadata) -> RsfResult<Stamp> {
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(corrupt("stage file is missing or aliased"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.nlink() != 1 {
            return Err(corrupt("stage file has another hard link"));
        }
    }
    Ok(Stamp {
        identity: identity(metadata)?,
        bytes: metadata.len(),
        modified: metadata.modified().map_err(io_error)?,
    })
}
fn read_checked(path: &Path, expected: &Stamp) -> RsfResult<Vec<u8>> {
    if file_stamp(&fs::symlink_metadata(path).map_err(io_error)?)? != *expected {
        return Err(corrupt("stage file physical custody changed"));
    }
    let mut file = File::open(path).map_err(io_error)?;
    if file_stamp(&file.metadata().map_err(io_error)?)? != *expected {
        return Err(corrupt("stage opened file differs from owned path"));
    }
    let mut bytes = Vec::new();
    Read::by_ref(&mut file)
        .take(expected.bytes + 1)
        .read_to_end(&mut bytes)
        .map_err(io_error)?;
    if bytes.len() as u64 != expected.bytes
        || file_stamp(&file.metadata().map_err(io_error)?)? != *expected
        || file_stamp(&fs::symlink_metadata(path).map_err(io_error)?)? != *expected
    {
        return Err(corrupt("stage file changed during bounded read"));
    }
    Ok(bytes)
}
#[cfg(unix)]
fn identity(metadata: &fs::Metadata) -> RsfResult<Identity> {
    use std::os::unix::fs::MetadataExt;
    Ok(Identity {
        device: metadata.dev(),
        inode: metadata.ino(),
    })
}
#[cfg(not(unix))]
fn identity(_: &fs::Metadata) -> RsfResult<Identity> {
    Err(corrupt(
        "stage custody requires qualified file identity support",
    ))
}

#[cfg(all(test, unix))]
mod tests;
