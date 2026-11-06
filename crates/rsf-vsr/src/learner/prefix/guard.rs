//! Physical continuity across an application-supplied import validator.
use super::*;
mod flat;
pub(in crate::learner) use flat::Flat;
use std::io::Read;
use std::time::SystemTime;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::learner) struct Identity {
    device: u64,
    inode: u64,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::learner) struct Stamp {
    identity: Identity,
    bytes: u64,
    modified: SystemTime,
}
#[derive(Clone, Debug)]
pub(in crate::learner) struct Exact {
    path: PathBuf,
    stamp: Stamp,
    bytes: Vec<u8>,
}
impl Exact {
    pub(in crate::learner) fn capture(path: &Path, expected: &[u8]) -> RsfResult<Self> {
        let stamp = file_stamp(&fs::symlink_metadata(path).map_err(io_error)?)?;
        if stamp.bytes != expected.len() as u64 {
            return Err(corrupt("learner publication metadata length changed"));
        }
        let exact = Self {
            path: path.to_path_buf(),
            stamp,
            bytes: expected.to_vec(),
        };
        exact.validate()?;
        Ok(exact)
    }
    // A caller-owned rename changes only this original file's name. Never
    // recapture a replacement after publishing an authoritative pointer.
    pub(super) fn relocated(mut self, path: PathBuf) -> RsfResult<Self> {
        self.path = path;
        self.validate()?;
        Ok(self)
    }
    pub(in crate::learner) fn validate(&self) -> RsfResult<()> {
        if file_stamp(&fs::symlink_metadata(&self.path).map_err(io_error)?)? != self.stamp {
            return Err(corrupt("learner publication metadata was replaced"));
        }
        let mut options = OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(libc::O_NOFOLLOW);
        }
        let file = options.open(&self.path).map_err(io_error)?;
        if file_stamp(&file.metadata().map_err(io_error)?)? != self.stamp {
            return Err(corrupt("learner publication opened a different file"));
        }
        let opened = file.try_clone().map_err(io_error)?;
        let limit = self
            .stamp
            .bytes
            .checked_add(1)
            .ok_or_else(|| corrupt("learner publication metadata size overflow"))?;
        let mut bytes = Vec::with_capacity(self.bytes.len());
        file.take(limit).read_to_end(&mut bytes).map_err(io_error)?;
        if bytes != self.bytes
            || file_stamp(&opened.metadata().map_err(io_error)?)? != self.stamp
            || file_stamp(&fs::symlink_metadata(&self.path).map_err(io_error)?)? != self.stamp
        {
            return Err(corrupt(
                "learner publication metadata changed while reading",
            ));
        }
        Ok(())
    }
}
#[derive(Debug)]
pub(in crate::learner) struct PublicationGuard {
    directories: Vec<(PathBuf, Identity)>,
    files: Vec<Exact>,
    previous: VsrLearnerStateToken,
    candidate: VsrStateToken,
}
impl PublicationGuard {
    // Extend an already captured prior publication while the caller's failure
    // sentinel is armed. Prior files/locks are retained, never recaptured.
    pub(super) fn with_candidate(
        mut self,
        owner: &LearnerHistoryStore,
        candidate: &Published,
    ) -> RsfResult<Self> {
        let previous = owner
            .published
            .as_ref()
            .ok_or_else(|| corrupt("bounded prior publication disappeared"))?;
        self.validate(owner, previous)?;
        let path = owner.directory(&candidate.id);
        self.directories.push((path.clone(), directory(&path)?));
        self.directories
            .push((path.join("cell"), directory(&path.join("cell"))?));
        self.files.push(Exact::capture(
            &path.join("DESCRIPTOR"),
            &candidate.descriptor.encode()?,
        )?);
        self.files.push(owned_lock(
            &path.join("cell/.vsr-writer.lock"),
            &candidate.cell._writer_lock,
        )?);
        if let Some(flat) = &candidate.bounded_flat {
            flat.validate_cell(&candidate.cell)?;
        }
        self.candidate = candidate.cell.recovery_state_token()?;
        self.validate(owner, candidate)?;
        Ok(self)
    }
    pub(in crate::learner) fn capture(
        owner: &LearnerHistoryStore,
        candidate: &Published,
    ) -> RsfResult<Self> {
        let previous = owner.published_state_token()?;
        let published = owner
            .published
            .as_ref()
            .ok_or_else(|| invalid_input("bounded import requires an existing publication"))?;
        let prior_directory = owner.directory(&published.id);
        let next_directory = owner.directory(&candidate.id);
        let directories = [
            owner.root.clone(),
            prior_directory.clone(),
            prior_directory.join("cell"),
            next_directory.clone(),
            next_directory.join("cell"),
        ]
        .into_iter()
        .map(|path| directory(&path).map(|identity| (path, identity)))
        .collect::<RsfResult<Vec<_>>>()?;
        let mut identity = b"RSFL1".to_vec();
        for value in [owner.pg.0, owner.local.0, owner.source.0] {
            identity.extend_from_slice(&value.to_le_bytes());
        }
        let files = vec![
            Exact::capture(&owner.root.join("IDENTITY"), &identity)?,
            Exact::capture(&owner.root.join("ACTIVE"), &published.id)?,
            owned_lock(&owner.root.join(".learner.lock"), &owner._lock)?,
            Exact::capture(
                &prior_directory.join("DESCRIPTOR"),
                &published.descriptor.encode()?,
            )?,
            Exact::capture(
                &next_directory.join("DESCRIPTOR"),
                &candidate.descriptor.encode()?,
            )?,
            owned_lock(
                &prior_directory.join("cell/.vsr-writer.lock"),
                &published.cell._writer_lock,
            )?,
            owned_lock(
                &next_directory.join("cell/.vsr-writer.lock"),
                &candidate.cell._writer_lock,
            )?,
        ];
        if let Some(flat) = &candidate.bounded_flat {
            flat.validate_cell(&candidate.cell)?;
        }
        let guard = Self {
            directories,
            files,
            previous,
            candidate: candidate.cell.recovery_state_token()?,
        };
        guard.validate(owner, candidate)?;
        Ok(guard)
    }
    pub(in crate::learner) fn validate(
        &self,
        owner: &LearnerHistoryStore,
        candidate: &Published,
    ) -> RsfResult<()> {
        if let Some(flat) = &candidate.bounded_flat {
            flat.validate_cell(&candidate.cell)?;
        }
        for (path, expected) in &self.directories {
            if directory(path)? != *expected {
                return Err(corrupt("learner publication directory changed"));
            }
        }
        for file in &self.files {
            file.validate()?;
        }
        let previous = owner
            .published
            .as_ref()
            .ok_or_else(|| corrupt("learner publication anchor disappeared"))?;
        // The callback sentinel is armed. Validate captured custody directly;
        // do not bypass a public health check or refresh any evidence.
        if owner.validate_published_custody(previous)? != self.previous
            || candidate.cell.recovery_state_token()? != self.candidate
        {
            return Err(corrupt(
                "learner publication owner changed during validation",
            ));
        }
        if let Some(flat) = &candidate.bounded_flat {
            flat.validate_cell(&candidate.cell)?;
        }
        Ok(())
    }
}
pub(in crate::learner) fn directory(path: &Path) -> RsfResult<Identity> {
    let metadata = fs::symlink_metadata(path).map_err(io_error)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(corrupt("learner prefix directory is missing or aliased"));
    }
    identity(&metadata)
}
pub(in crate::learner) fn file_stamp(metadata: &fs::Metadata) -> RsfResult<Stamp> {
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(corrupt("learner prefix metadata is missing or aliased"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.nlink() != 1 {
            return Err(corrupt("learner prefix metadata is multiply linked"));
        }
    }
    Ok(Stamp {
        identity: identity(metadata)?,
        bytes: metadata.len(),
        modified: metadata.modified().map_err(io_error)?,
    })
}
pub(super) fn same_directory(metadata: &fs::Metadata, expected: &Identity) -> RsfResult<()> {
    if !metadata.is_dir() || metadata.file_type().is_symlink() || identity(metadata)? != *expected {
        return Err(corrupt(
            "bounded learner directory descriptor differs from original",
        ));
    }
    Ok(())
}
fn identity(metadata: &fs::Metadata) -> RsfResult<Identity> {
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
            "learner prefix custody requires physical file identities",
        ))
    }
}

/// Bind the named lock to the actual acquired descriptor, never to a fresh name.
pub(in crate::learner) fn owned_lock(
    path: &Path,
    lock: &rsf_storage::ExclusiveFileLock,
) -> RsfResult<Exact> {
    let held = file_stamp(&lock.metadata().map_err(io_error)?)?;
    let exact = Exact::capture(path, &[])?;
    if exact.stamp != held || file_stamp(&lock.metadata().map_err(io_error)?)? != held {
        return Err(corrupt(
            "learner lock no longer names its actual held descriptor",
        ));
    }
    Ok(exact)
}
