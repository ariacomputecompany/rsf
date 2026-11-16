//! Same-invocation publication of a fresh store, never a cache for reopening.
//! The caller still controls namespace replacement and trusted alias topology.
use super::*;
use std::os::unix::fs::MetadataExt;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Identity(u64, u64);
fn identity(metadata: &fs::Metadata) -> Identity {
    Identity(metadata.dev(), metadata.ino())
}
fn changed() -> io::Error {
    io::Error::other("fresh store publication lost original custody")
}
#[derive(Debug, PartialEq, Eq)]
struct Link {
    identity: Identity,
    changed: (i64, i64),
    target: PathBuf,
}
fn link(path: &Path) -> io::Result<Option<Link>> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() {
        Ok(Some(Link {
            identity: identity(&metadata),
            changed: (metadata.ctime(), metadata.ctime_nsec()),
            target: fs::read_link(path)?,
        }))
    } else {
        Ok(None)
    }
}
#[derive(Debug)]
struct Directory {
    path: PathBuf,
    file: File,
    identity: Identity,
    link: Option<Link>,
}
impl Directory {
    fn capture(path: &Path) -> io::Result<Self> {
        let routing = link(path)?;
        let file = File::open(path)?;
        let metadata = file.metadata()?;
        if !metadata.is_dir() {
            return Err(changed());
        }
        let value = Self {
            path: path.to_path_buf(),
            file,
            identity: identity(&metadata),
            link: routing,
        };
        value.validate()?;
        Ok(value)
    }
    fn validate(&self) -> io::Result<()> {
        // Reopening checks current read/search permission, but does not replace
        // the retained descriptor or adopt a different current namespace.
        let current = File::open(&self.path)?;
        let named = current.metadata()?;
        let held = self.file.metadata()?;
        if !named.is_dir()
            || !held.is_dir()
            || identity(&named) != self.identity
            || identity(&held) != self.identity
            || link(&self.path)? != self.link
        {
            return Err(changed());
        }
        Ok(())
    }
}

/// Minted only after publishing every original directory in both routes. All
/// descriptors survive until final publication; no failed instance is reusable.
#[derive(Debug)]
pub(crate) struct FreshPublication {
    lexical: PathBuf,
    resolved: PathBuf,
    directories: Vec<Directory>,
    segment: Option<(PathBuf, File, Identity)>,
}
impl FreshPublication {
    pub(crate) fn publish_routes(path: &Path) -> io::Result<Self> {
        let lexical = absolute(path)?;
        fs::create_dir_all(&lexical)?;
        let resolved = fs::canonicalize(&lexical)?;
        let mut paths = resolved
            .ancestors()
            .map(Path::to_path_buf)
            .collect::<Vec<_>>();
        if lexical != resolved {
            paths.extend(lexical.ancestors().map(Path::to_path_buf));
        }
        let directories = paths
            .iter()
            .map(|path| Directory::capture(path))
            .collect::<io::Result<Vec<_>>>()?;
        let pending = Self {
            lexical,
            resolved,
            directories,
            segment: None,
        };
        pending.validate_routes()?;
        for directory in &pending.directories {
            directory.validate()?;
            sync_held_directory(&directory.path, &directory.file)?;
            directory.validate()?;
        }
        pending.validate_routes()?;
        #[cfg(test)]
        observation::at(observation::Phase::RoutesPublished)?;
        pending.validate_routes()?;
        // Only successful completion of this full route yields the capability.
        Ok(pending)
    }
    pub(crate) fn validate_routes(&self) -> io::Result<()> {
        if fs::canonicalize(&self.lexical)? != self.resolved {
            return Err(changed());
        }
        for directory in &self.directories {
            directory.validate()?;
        }
        Ok(())
    }
    pub(crate) fn bind_created_segment(&mut self, path: &Path, file: File) -> io::Result<()> {
        self.validate_routes()?;
        if self.segment.is_some() || absolute(path)?.parent() != Some(self.lexical.as_path()) {
            return Err(changed());
        }
        let metadata = file.metadata()?;
        if !metadata.is_file() || metadata.len() != 0 || metadata.nlink() != 1 {
            return Err(changed());
        }
        self.segment = Some((absolute(path)?, file, identity(&metadata)));
        #[cfg(test)]
        observation::at(observation::Phase::SegmentBound)?;
        self.validate_routes()?;
        self.validate_segment(path, None)
    }
    fn validate_segment(&self, path: &Path, writer: Option<&File>) -> io::Result<()> {
        let (original, held, expected) = self.segment.as_ref().ok_or_else(changed)?;
        if absolute(path)? != *original
            || fs::canonicalize(path)?
                != self
                    .resolved
                    .join(original.file_name().ok_or_else(changed)?)
        {
            return Err(changed());
        }
        for metadata in [fs::symlink_metadata(path)?, held.metadata()?] {
            if !metadata.is_file()
                || metadata.file_type().is_symlink()
                || identity(&metadata) != *expected
                || metadata.len() != 0
                || metadata.nlink() != 1
            {
                return Err(changed());
            }
        }
        if let Some(writer) = writer {
            let metadata = writer.metadata()?;
            if identity(&metadata) != *expected || metadata.len() != 0 || metadata.nlink() != 1 {
                return Err(changed());
            }
        }
        Ok(())
    }
    pub(crate) fn finish(self, path: &Path, writer: &File) -> io::Result<()> {
        #[cfg(test)]
        observation::at(observation::Phase::FinalOpened)?;
        self.validate_routes()?;
        self.validate_segment(path, Some(writer))?;
        // Both lexical and resolved parents still name this exact original
        // directory. Its ancestor links were published by this invocation.
        let parent = self.directories.first().ok_or_else(changed)?;
        sync_held_directory(&parent.path, &parent.file)?;
        #[cfg(test)]
        observation::at(observation::Phase::FinalSynced)?;
        self.validate_routes()?;
        self.validate_segment(path, Some(writer))
    }
}

#[cfg(test)]
pub(crate) mod observation;
