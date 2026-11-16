use super::*;
use std::fs::{self, File, Metadata};
use std::io::Read;

pub(super) struct Prefix {
    pub input: VsrMembershipInput,
    pub records: Vec<VsrHistoryRecord>,
    custody: rsf_storage::SegmentReadCustody,
    directories: Vec<(PathBuf, Stamp)>,
    files: Vec<Held>,
}
impl Prefix {
    pub fn read(owner: &MembershipDecisionStore, id: [u8; 32]) -> RsfResult<Self> {
        let name: String = id.iter().map(|b| format!("{b:02x}")).collect();
        let dir = owner.root.join("inputs").join(name);
        let directories = [owner.root.clone(), owner.root.join("inputs"), dir.clone()]
            .into_iter()
            .map(|path| Ok((path.clone(), directory(&path)?)))
            .collect::<RsfResult<Vec<_>>>()?;
        let descriptor = Held::read(dir.join("DESCRIPTOR"), MAX_HISTORY_RECORD_BYTES as u64)?;
        let input = VsrMembershipInput::decode(&descriptor.bytes)?;
        if input.id()? != id || input.proposal() != &owner.proposal {
            return Err(corrupt("membership STAGE prefix descriptor differs"));
        }
        let records_root = dir.join("records");
        let mut files = vec![
            descriptor,
            Held::read(records_root.join("MANIFEST.lock"), 0)?,
        ];
        let limit = rsf_storage::append_only_manifest_byte_bound(input.records()?)?;
        let (recovered, custody) = SegmentStore::recover_immutable(&records_root, limit)?;
        if !recovered.recovery.maintenance_records.is_empty()
            || !recovered.recovery.compacted_prefixes.is_empty()
            || recovered.recovery.records.len() != recovered.recovery.wal_records.len()
        {
            return Err(corrupt(
                "membership STAGE input is not append-only history WAL",
            ));
        }
        for segment in &recovered.segments {
            files.push(Held::read(
                records_root
                    .join("segments")
                    .join(&segment.file_name)
                    .with_extension("lock"),
                0,
            )?);
        }
        let records = disk::decode_records(&input, &recovered)?;
        let prefix = Self {
            input,
            records,
            custody,
            directories,
            files,
        };
        prefix.validate()?;
        Ok(prefix)
    }
    pub fn validate(&self) -> RsfResult<()> {
        for (path, stamp) in &self.directories {
            if directory(path)? != *stamp {
                return Err(corrupt("membership STAGE parent custody changed"));
            }
        }
        for file in &self.files {
            file.validate()?;
        }
        self.custody.validate()
    }
}
#[derive(Eq, PartialEq)]
struct Stamp {
    device: u64,
    inode: u64,
    bytes: u64,
    modified: Option<std::time::SystemTime>,
}
fn stamp(metadata: &Metadata, is_directory: bool) -> RsfResult<Stamp> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if (is_directory && !metadata.is_dir())
            || (!is_directory && (!metadata.is_file() || metadata.nlink() != 1))
        {
            return Err(corrupt(
                "membership STAGE custody is not an ordinary owned path",
            ));
        }
        Ok(Stamp {
            device: metadata.dev(),
            inode: metadata.ino(),
            bytes: if is_directory { 0 } else { metadata.len() },
            modified: if is_directory {
                None
            } else {
                Some(metadata.modified().map_err(io_error)?)
            },
        })
    }
    #[cfg(not(unix))]
    {
        let _ = (metadata, is_directory);
        Err(corrupt("membership STAGE requires physical file identity"))
    }
}
fn directory(path: &Path) -> RsfResult<Stamp> {
    stamp(&fs::symlink_metadata(path).map_err(io_error)?, true)
}
struct Held {
    path: PathBuf,
    file: File,
    stamp: Stamp,
    bytes: Vec<u8>,
}
impl Held {
    fn read(path: PathBuf, limit: u64) -> RsfResult<Self> {
        let expected = stamp(&fs::symlink_metadata(&path).map_err(io_error)?, false)?;
        if expected.bytes > limit {
            return Err(corrupt("membership STAGE file exceeds bound"));
        }
        let mut options = OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(libc::O_NOFOLLOW);
        }
        let file = options.open(&path).map_err(io_error)?;
        if stamp(&file.metadata().map_err(io_error)?, false)? != expected {
            return Err(corrupt("membership STAGE opened another file"));
        }
        let mut bytes = Vec::new();
        (&file)
            .take(
                expected
                    .bytes
                    .checked_add(1)
                    .ok_or_else(|| corrupt("membership STAGE file length overflow"))?,
            )
            .read_to_end(&mut bytes)
            .map_err(io_error)?;
        if bytes.len() as u64 != expected.bytes {
            return Err(corrupt("membership STAGE file changed during read"));
        }
        let held = Self {
            path,
            file,
            stamp: expected,
            bytes,
        };
        held.validate()?;
        Ok(held)
    }
    fn validate(&self) -> RsfResult<()> {
        if stamp(&self.file.metadata().map_err(io_error)?, false)? != self.stamp
            || stamp(&fs::symlink_metadata(&self.path).map_err(io_error)?, false)? != self.stamp
        {
            return Err(corrupt("membership STAGE file custody changed"));
        }
        Ok(())
    }
}

// Compare the actual exclusive descriptor, never a freshly opened substitute.
pub(super) struct Namespace {
    path: PathBuf,
    original: Stamp,
}
impl Namespace {
    pub fn capture(owner: &MembershipDecisionStore) -> RsfResult<Self> {
        let original = stamp(&owner._lock.metadata().map_err(io_error)?, false)?;
        if original.bytes != 0 {
            return Err(corrupt("membership STAGE namespace lock is not empty"));
        }
        let result = Self {
            path: owner.root.join(".decision.lock"),
            original,
        };
        result.validate(owner)?;
        Ok(result)
    }
    pub fn validate(&self, owner: &MembershipDecisionStore) -> RsfResult<()> {
        if stamp(&owner._lock.metadata().map_err(io_error)?, false)? != self.original
            || stamp(&fs::symlink_metadata(&self.path).map_err(io_error)?, false)? != self.original
        {
            return Err(corrupt(
                "membership STAGE original namespace lock custody changed",
            ));
        }
        Ok(())
    }
}
