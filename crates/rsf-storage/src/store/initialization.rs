//! Opt-in completion of an unpublished, physically empty SegmentStore factory.
//! This is not recovery, repair, reset, or evidence of caller-level acknowledgement.
use super::*;

impl SegmentStore {
    /// Read-only classification of exact unpublished empty factory shapes.
    /// `false` means a regular final MANIFEST exists, not that it is valid; use
    /// strict existing-store recovery on that path. Errors never mean missing.
    /// The parent must already exist and be held under the caller's exclusive
    /// namespace custody. This predicate grants no acknowledgement or authority.
    pub fn initialization_pending(root: impl AsRef<Path>) -> RsfResult<bool> {
        factory::pending(root.as_ref())
    }

    /// Complete only a physically empty, unpublished factory. The caller must
    /// independently prove that this store is not acknowledged/bound durable
    /// custody. Rechecks under the store gate and exclusive MANIFEST lock.
    /// A final MANIFEST, even an empty initial one, is outside this API.
    /// No WAL parser, truncation, quarantine, data deletion, or generic fallback.
    pub fn resume_empty_initialization(root: impl AsRef<Path>) -> RsfResult<()> {
        factory::resume(root.as_ref())
    }
}

#[cfg(not(unix))]
mod factory {
    use super::*;
    fn unsupported() -> RsfError {
        RsfError::new(
            ErrorCode::InvalidInput,
            "empty initialization requires qualified no-follow filesystem support",
        )
    }
    pub(super) fn pending(_: &Path) -> RsfResult<bool> {
        Err(unsupported())
    }
    pub(super) fn resume(_: &Path) -> RsfResult<()> {
        Err(unsupported())
    }
}

#[cfg(unix)]
mod factory {
    use super::*;
    use std::io::{Read, Seek, SeekFrom};
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub(super) enum Boundary {
        RootSynced,
        LockAcquired,
        SegmentsSynced,
        SegmentSynced,
        TemporaryCreated,
        TemporarySynced,
        ManifestRenamed,
        ManifestSynced,
    }

    /// Private structural proof, consumed only inside a custody-checked factory.
    #[derive(Debug)]
    struct EmptyFactory {
        root_exists: bool,
        segments_exist: bool,
        segment_exists: bool,
    }

    pub(super) fn pending(root: &Path) -> RsfResult<bool> {
        let root = normalized(root)?;
        Ok(inspect(&root)?.is_some())
    }
    pub(super) fn resume(root: &Path) -> RsfResult<()> {
        resume_with_hook(root, |_| Ok(()))
    }
    pub(super) fn resume_with_hook(
        root: &Path,
        mut after: impl FnMut(Boundary) -> RsfResult<()>,
    ) -> RsfResult<()> {
        let root = normalized(root)?;
        let root = root.as_path();
        let proof = require_pending(root)?;
        if !proof.root_exists {
            fs::create_dir(root).map_err(|error| storage_error(StorageIoOperation::Open, error))?;
        }
        sync_directory(parent(root)?)?;
        sync_directory(root)?;
        after(Boundary::RootSynced)?;

        let gate = store_gate(root)?;
        let _gate = lock_gate(&gate)?;
        require_pending(root)?;
        let lock_path = root.join("MANIFEST.lock");
        let lock_file = match regular(&lock_path)? {
            Some(metadata) => {
                require_zero(&metadata, "initialization lock")?;
                open_regular(&lock_path, true)?
            }
            None => create_regular(&lock_path)?,
        };
        require_zero(
            &lock_file.metadata().map_err(recover_error)?,
            "initialization lock",
        )?;
        // Use the same OS lock inode as SegmentLock, without its reentrant
        // registry: an initialization owner must not coexist with any writer.
        let _lock = crate::ExclusiveFileLock::acquire(lock_file).map_err(|error| {
            RsfError::new(
                ErrorCode::InvalidInput,
                format!("empty initialization lock unavailable: {error}"),
            )
        })?;
        after(Boundary::LockAcquired)?;
        let proof = require_pending(root)?;
        let segments = root.join(SEGMENT_DIR);
        if !proof.segments_exist {
            fs::create_dir(&segments)
                .map_err(|error| storage_error(StorageIoOperation::Open, error))?;
        }
        sync_directory(&segments)?;
        sync_directory(root)?;
        after(Boundary::SegmentsSynced)?;

        let initial = SegmentManifest::initial();
        let segment_path = segments.join(&initial.active_segment()?.file_name);
        let segment = if proof.segment_exists {
            open_regular(&segment_path, false)?
        } else {
            create_regular(&segment_path)?
        };
        require_zero(
            &segment.metadata().map_err(recover_error)?,
            "initial segment",
        )?;
        segment.sync_all().map_err(sync_error)?;
        drop(segment);
        sync_directory(&segments)?;
        after(Boundary::SegmentSynced)?;

        let expected = initial.encode();
        let temporary = root.join("MANIFEST.tmp");
        let mut file = if regular(&temporary)?.is_some() {
            open_regular(&temporary, true)?
        } else {
            create_regular(&temporary)?
        };
        after(Boundary::TemporaryCreated)?;
        let prefix = read_prefix(&mut file, expected.as_bytes())?;
        file.seek(SeekFrom::End(0)).map_err(recover_error)?;
        file.write_all(&expected.as_bytes()[prefix.len()..])
            .map_err(|error| storage_error(StorageIoOperation::Append, error))?;
        file.sync_all().map_err(sync_error)?;
        drop(file);
        after(Boundary::TemporarySynced)?;

        // Parent namespace custody plus the gate/lock excludes replacement by
        // cooperating owners. Refuse to overwrite any final entry observed now.
        if regular(&root.join(MANIFEST_FILE))?.is_some() {
            return Err(fail("final manifest appeared during empty initialization"));
        }
        fs::rename(&temporary, root.join(MANIFEST_FILE)).map_err(sync_error)?;
        after(Boundary::ManifestRenamed)?;
        sync_directory(root)?;
        after(Boundary::ManifestSynced)
    }

    fn require_pending(root: &Path) -> RsfResult<EmptyFactory> {
        inspect(root)?.ok_or_else(|| {
            RsfError::new(
                ErrorCode::InvalidInput,
                "published manifest requires strict existing-store recovery",
            )
        })
    }

    fn inspect(root: &Path) -> RsfResult<Option<EmptyFactory>> {
        // Validate the owned parent itself. Ancestors above it are part of the
        // caller's trusted namespace, not reconstructed or canonicalized here.
        require_directory(parent(root)?)?;
        let Some(metadata) = optional_metadata(root)? else {
            return Ok(Some(EmptyFactory {
                root_exists: false,
                segments_exist: false,
                segment_exists: false,
            }));
        };
        if !metadata.is_dir() {
            return Err(fail("initialization root is not an owned directory"));
        }
        if regular(&root.join(MANIFEST_FILE))?.is_some() {
            return Ok(None);
        }
        let mut segments_exist = false;
        let mut temporary = false;
        for (position, entry) in fs::read_dir(root).map_err(recover_error)?.enumerate() {
            let entry = entry.map_err(recover_error)?;
            if position >= 3 {
                return Err(fail("foreign entry in unpublished initialization root"));
            }
            let name = entry.file_name();
            if name == SEGMENT_DIR {
                require_directory(&entry.path())?;
                segments_exist = true;
            } else if name == "MANIFEST.lock" {
                let metadata = regular(&entry.path())?
                    .ok_or_else(|| fail("initialization lock disappeared"))?;
                require_zero(&metadata, "initialization lock")?;
            } else if name == "MANIFEST.tmp" {
                let mut file = open_regular(&entry.path(), false)?;
                read_prefix(&mut file, SegmentManifest::initial().encode().as_bytes())?;
                temporary = true;
            } else {
                return Err(fail("foreign entry in unpublished initialization root"));
            }
        }
        let mut segment_exists = false;
        if segments_exist {
            for entry in fs::read_dir(root.join(SEGMENT_DIR)).map_err(recover_error)? {
                let entry = entry.map_err(recover_error)?;
                if segment_exists || entry.file_name() != segment_file_name(1).as_str() {
                    return Err(fail("unpublished store has a foreign segment or sidecar"));
                }
                let metadata =
                    regular(&entry.path())?.ok_or_else(|| fail("initial segment disappeared"))?;
                require_zero(&metadata, "initial segment")?;
                segment_exists = true;
            }
        }
        if temporary && !segment_exists {
            return Err(fail("manifest temporary lacks its initial empty segment"));
        }
        Ok(Some(EmptyFactory {
            root_exists: true,
            segments_exist,
            segment_exists,
        }))
    }

    fn normalized(root: &Path) -> RsfResult<PathBuf> {
        let mut normalized = PathBuf::new();
        for part in root.components() {
            match part {
                std::path::Component::ParentDir => {
                    return Err(RsfError::new(
                        ErrorCode::InvalidInput,
                        "initialization path must not traverse a parent",
                    ));
                }
                std::path::Component::CurDir => {}
                part => normalized.push(part.as_os_str()),
            }
        }
        if normalized.file_name().is_none() {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                "initialization requires a named child directory",
            ));
        }
        Ok(normalized)
    }
    fn parent(root: &Path) -> RsfResult<&Path> {
        if root.file_name().is_none()
            || root
                .components()
                .any(|part| matches!(part, std::path::Component::ParentDir))
        {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                "initialization requires a child of an owned parent",
            ));
        }
        Ok(root
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or(Path::new(".")))
    }
    fn optional_metadata(path: &Path) -> RsfResult<Option<fs::Metadata>> {
        match fs::symlink_metadata(path) {
            Ok(metadata) => Ok(Some(metadata)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(recover_error(error)),
        }
    }
    fn require_directory(path: &Path) -> RsfResult<()> {
        let metadata = fs::symlink_metadata(path).map_err(recover_error)?;
        if !metadata.is_dir() {
            return Err(fail(
                "initialization directory is absent, aliased or not a directory",
            ));
        }
        Ok(())
    }
    fn regular(path: &Path) -> RsfResult<Option<fs::Metadata>> {
        let metadata = optional_metadata(path)?;
        if let Some(metadata) = &metadata {
            require_regular(metadata)?;
        }
        Ok(metadata)
    }
    fn require_regular(metadata: &fs::Metadata) -> RsfResult<()> {
        if !metadata.is_file() || metadata.nlink() != 1 {
            return Err(fail(
                "initialization file is not a singly owned regular file",
            ));
        }
        Ok(())
    }
    fn require_zero(metadata: &fs::Metadata, label: &str) -> RsfResult<()> {
        require_regular(metadata)?;
        if metadata.len() != 0 {
            return Err(fail(format!(
                "{label} contains bytes; refusing empty initialization"
            )));
        }
        Ok(())
    }
    fn open_regular(path: &Path, write: bool) -> RsfResult<File> {
        regular(path)?.ok_or_else(|| fail("initialization file is missing"))?;
        let file = OpenOptions::new()
            .read(true)
            .write(write)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)
            .map_err(recover_error)?;
        require_regular(&file.metadata().map_err(recover_error)?)?;
        Ok(file)
    }
    fn create_regular(path: &Path) -> RsfResult<File> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)
            .map_err(|error| storage_error(StorageIoOperation::Open, error))?;
        require_regular(&file.metadata().map_err(recover_error)?)?;
        Ok(file)
    }
    fn read_prefix(file: &mut File, expected: &[u8]) -> RsfResult<Vec<u8>> {
        if file.metadata().map_err(recover_error)?.len() > expected.len() as u64 {
            return Err(fail("initial manifest temporary exceeds canonical length"));
        }
        file.seek(SeekFrom::Start(0)).map_err(recover_error)?;
        let mut bytes = Vec::new();
        Read::take(file, expected.len() as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(recover_error)?;
        if !expected.starts_with(&bytes) {
            return Err(fail("initial manifest temporary is not a canonical prefix"));
        }
        Ok(bytes)
    }
    fn sync_directory(path: &Path) -> RsfResult<()> {
        require_directory(path)?;
        let directory = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_DIRECTORY)
            .open(path)
            .map_err(recover_error)?;
        directory.sync_all().map_err(sync_error)
    }
    fn fail(message: impl Into<String>) -> RsfError {
        RsfError::new(ErrorCode::CorruptionDetected, message)
    }
    fn recover_error(error: std::io::Error) -> RsfError {
        storage_error(StorageIoOperation::Recover, error)
    }
    fn sync_error(error: std::io::Error) -> RsfError {
        storage_error(StorageIoOperation::Sync, error)
    }
}

#[cfg(all(test, unix))]
mod tests;
