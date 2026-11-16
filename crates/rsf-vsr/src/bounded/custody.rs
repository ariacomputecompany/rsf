//! Finite metadata continuity during one strict voter construction.
use super::*;
use std::fs::{File, Metadata, OpenOptions};
use std::io::Read;
use std::time::SystemTime;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct Identity {
    device: u64,
    inode: u64,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct Stamp {
    identity: Identity,
    bytes: u64,
    modified: SystemTime,
}
#[derive(Debug)]
pub(super) struct Exact {
    pub path: PathBuf,
    pub bytes: Vec<u8>,
    pub stamp: Stamp,
}
impl Exact {
    pub fn read(path: &Path, maximum: usize) -> RsfResult<Self> {
        let expected = regular(&fs::symlink_metadata(path).map_err(io_error)?)?;
        if expected.bytes > maximum as u64 {
            return Err(corrupt("bounded voter metadata exceeds codec limit"));
        }
        let mut options = OpenOptions::new();
        options.read(true);
        nofollow(&mut options);
        let mut file = options.open(path).map_err(io_error)?;
        if regular(&file.metadata().map_err(io_error)?)? != expected {
            return Err(corrupt("bounded voter metadata changed before read"));
        }
        let mut bytes = Vec::with_capacity(expected.bytes as usize);
        (&mut file)
            .take(maximum as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(io_error)?;
        if bytes.len() > maximum
            || regular(&file.metadata().map_err(io_error)?)? != expected
            || regular(&fs::symlink_metadata(path).map_err(io_error)?)? != expected
        {
            return Err(corrupt("bounded voter metadata changed during read"));
        }
        Ok(Self {
            path: path.to_path_buf(),
            bytes,
            stamp: expected,
        })
    }
    pub fn validate(&self) -> RsfResult<()> {
        let current = Self::read(&self.path, self.bytes.len())?;
        if current.stamp != self.stamp || current.bytes != self.bytes {
            return Err(corrupt("bounded voter metadata lost its captured identity"));
        }
        Ok(())
    }
}
#[derive(Debug)]
pub(super) struct Guard {
    directories: BTreeMap<PathBuf, Identity>,
    files: BTreeMap<PathBuf, Option<Exact>>,
}
impl Guard {
    pub fn new(root: &Path) -> RsfResult<Self> {
        Ok(Self {
            directories: [(root.to_path_buf(), directory(root)?)]
                .into_iter()
                .collect(),
            files: BTreeMap::new(),
        })
    }
    pub fn directory(&mut self, path: &Path) -> RsfResult<()> {
        let actual = directory(path)?;
        if let Some(expected) = self.directories.get(path) {
            if actual != *expected {
                return Err(corrupt("bounded voter owned directory changed"));
            }
        } else {
            self.directories.insert(path.to_path_buf(), actual);
        }
        Ok(())
    }
    pub fn optional(&mut self, path: &Path, maximum: usize) -> RsfResult<Option<Vec<u8>>> {
        if let Some(existing) = self.files.get(path) {
            validate_optional(path, existing)?;
            return Ok(existing.as_ref().map(|file| file.bytes.clone()));
        }
        let file = match fs::symlink_metadata(path) {
            Ok(_) => {
                self.directory(
                    path.parent()
                        .ok_or_else(|| corrupt("bounded metadata lacks parent"))?,
                )?;
                Some(Exact::read(path, maximum)?)
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => return Err(io_error(error)),
        };
        let bytes = file.as_ref().map(|file| file.bytes.clone());
        self.files.insert(path.to_path_buf(), file);
        Ok(bytes)
    }
    pub fn read(&mut self, path: &Path, maximum: usize) -> io::Result<Vec<u8>> {
        self.optional(path, maximum)
            .map_err(|error| io::Error::other(error.message))?
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "bounded voter metadata absent"))
    }
    pub fn lock(&mut self, path: &Path) -> RsfResult<rsf_storage::ExclusiveFileLock> {
        self.optional(path, 0)?
            .ok_or_else(|| corrupt("existing voter writer lock is missing"))?;
        let mut options = OpenOptions::new();
        options.write(true);
        nofollow(&mut options);
        let file = options.open(path).map_err(io_error)?;
        let expected = self.files[path].as_ref().expect("present writer lock");
        if regular(&file.metadata().map_err(io_error)?)? != expected.stamp {
            return Err(corrupt("bounded voter opened a different writer lock"));
        }
        let lock = rsf_storage::ExclusiveFileLock::acquire(file).map_err(|error| {
            RsfError::new(
                ErrorCode::QuorumUnavailable,
                format!("VSR store already has a writer: {error}"),
            )
        })?;
        self.held_lock(path, &lock)?;
        Ok(lock)
    }
    pub fn held_lock(&self, path: &Path, lock: &rsf_storage::ExclusiveFileLock) -> RsfResult<()> {
        let expected = self
            .files
            .get(path)
            .and_then(Option::as_ref)
            .ok_or_else(|| corrupt("writer lock was not captured"))?;
        expected.validate()?;
        if regular(&lock.metadata().map_err(io_error)?)? != expected.stamp {
            return Err(corrupt(
                "bounded voter writer lock differs from actual held descriptor",
            ));
        }
        Ok(())
    }
    pub fn validate(&self) -> RsfResult<()> {
        for (path, expected) in &self.directories {
            if directory(path)? != *expected {
                return Err(corrupt("bounded voter owned directory changed"));
            }
        }
        for (path, file) in &self.files {
            validate_optional(path, file)?;
        }
        Ok(())
    }
    pub fn publish_marker(&mut self, root: &Path, file: Exact) -> RsfResult<()> {
        let path = root.join(publication::PUBLISHED);
        if file.path != path || !self.files.get(&path).is_some_and(Option::is_none) {
            return Err(corrupt(
                "bounded recovery cannot replace captured publication metadata",
            ));
        }
        file.validate()?;
        self.files.insert(path, Some(file));
        Ok(())
    }
}
fn validate_optional(path: &Path, expected: &Option<Exact>) -> RsfResult<()> {
    if let Some(file) = expected {
        return file.validate();
    }
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(io_error(error)),
        Ok(_) => Err(corrupt("bounded voter previously absent metadata appeared")),
    }
}
pub(super) fn directory(path: &Path) -> RsfResult<Identity> {
    let metadata = fs::symlink_metadata(path).map_err(io_error)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(corrupt(
            "bounded voter requires an owned physical directory",
        ));
    }
    identity(&metadata)
}
pub(super) fn regular(metadata: &Metadata) -> RsfResult<Stamp> {
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(corrupt(
            "bounded voter metadata must be an owned regular file",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.nlink() != 1 {
            return Err(corrupt("bounded voter metadata is multiply linked"));
        }
    }
    Ok(Stamp {
        identity: identity(metadata)?,
        bytes: metadata.len(),
        modified: metadata.modified().map_err(io_error)?,
    })
}
fn identity(metadata: &Metadata) -> RsfResult<Identity> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        Ok(Identity {
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }
    #[cfg(not(unix))]
    {
        let _ = metadata;
        Err(corrupt(
            "bounded voter custody requires physical file identities",
        ))
    }
}
pub(super) fn nofollow(options: &mut OpenOptions) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    #[cfg(not(unix))]
    let _ = options;
}
pub(super) fn validate_descriptor(path: &Path, file: &File, expected: &Stamp) -> RsfResult<()> {
    if regular(&file.metadata().map_err(io_error)?)? != *expected
        || regular(&fs::symlink_metadata(path).map_err(io_error)?)? != *expected
    {
        return Err(corrupt(
            "bounded publication temporary differs from held file",
        ));
    }
    Ok(())
}
pub(super) fn after_owned_write(path: &Path, file: &File, before: &Stamp) -> RsfResult<Stamp> {
    let current = regular(&file.metadata().map_err(io_error)?)?;
    if current.identity != before.identity {
        return Err(corrupt("bounded publication lost its original file"));
    }
    validate_descriptor(path, file, &current)?;
    Ok(current)
}
