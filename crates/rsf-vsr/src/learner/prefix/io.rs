//! Small bounded metadata operations within the exclusively owned learner root.
use super::*;
use std::io::Read;

// Existing RSFD1 (45) + RSFV1 (73 + at most 4096 u64 voter IDs).
pub(in crate::learner) const DESCRIPTOR_BYTES: usize = 45 + 73 + 8 * 4096;

pub(in crate::learner) fn normalized(path: &Path) -> RsfResult<PathBuf> {
    let mut root = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                return Err(corrupt("bounded learner path contains parent traversal"));
            }
            component => root.push(component.as_os_str()),
        }
    }
    if root.as_os_str().is_empty() {
        root.push(".");
    }
    Ok(root)
}
fn nofollow(options: &mut OpenOptions) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
}
pub(in crate::learner) fn read(path: &Path, maximum: usize) -> RsfResult<(Vec<u8>, guard::Exact)> {
    let metadata = fs::symlink_metadata(path).map_err(io_error)?;
    let stamp = guard::file_stamp(&metadata)?;
    if metadata.len() > maximum as u64 {
        return Err(corrupt("learner metadata exceeds codec limit"));
    }
    let mut options = OpenOptions::new();
    options.read(true);
    nofollow(&mut options);
    let file = options.open(path).map_err(io_error)?;
    if guard::file_stamp(&file.metadata().map_err(io_error)?)? != stamp {
        return Err(corrupt("learner metadata changed before reading"));
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take(maximum as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(io_error)?;
    if bytes.len() > maximum
        || guard::file_stamp(&fs::symlink_metadata(path).map_err(io_error)?)? != stamp
    {
        return Err(corrupt("learner metadata changed during reading"));
    }
    let exact = guard::Exact::capture(path, &bytes)?;
    if guard::file_stamp(&fs::symlink_metadata(path).map_err(io_error)?)? != stamp {
        return Err(corrupt(
            "learner metadata changed while binding read custody",
        ));
    }
    Ok((bytes, exact))
}
pub(in crate::learner) fn lock(
    path: &Path,
    create: bool,
) -> RsfResult<rsf_storage::ExclusiveFileLock> {
    let original = match fs::symlink_metadata(path) {
        Ok(metadata) => Some((
            guard::Exact::capture(path, &[])?,
            guard::file_stamp(&metadata)?,
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound && create => None,
        Err(error) => return Err(io_error(error)),
    };
    let mut options = OpenOptions::new();
    options.write(true);
    if original.is_none() {
        options.create_new(true);
    }
    nofollow(&mut options);
    let file = options.open(path).map_err(io_error)?;
    let held = guard::file_stamp(&file.metadata().map_err(io_error)?)?;
    let exact = match original {
        Some((exact, expected)) => {
            if held != expected {
                return Err(corrupt("learner lock changed before acquisition"));
            }
            exact.validate()?;
            exact
        }
        None => {
            // Keep the created descriptor through lock acquisition. Reopening
            // its name could adopt a replacement between creation and custody.
            if held != guard::file_stamp(&fs::symlink_metadata(path).map_err(io_error)?)? {
                return Err(corrupt("created learner lock lost its named descriptor"));
            }
            file.sync_all().map_err(io_error)?;
            sync_directory(path.parent().ok_or_else(|| corrupt("lock lacks parent"))?)?;
            guard::Exact::capture(path, &[])?
        }
    };
    if guard::file_stamp(&file.metadata().map_err(io_error)?)? != held
        || guard::file_stamp(&fs::symlink_metadata(path).map_err(io_error)?)? != held
    {
        return Err(corrupt(
            "learner lock changed before acquiring its descriptor",
        ));
    }
    let lock = rsf_storage::ExclusiveFileLock::acquire(file)
        .map_err(|error| invalid_input(format!("bounded learner already has an owner: {error}")))?;
    exact.validate()?;
    if guard::file_stamp(&lock.metadata().map_err(io_error)?)? != held {
        return Err(corrupt("learner lock lost its actual held descriptor"));
    }
    Ok(lock)
}
pub(in crate::learner) fn create_directory(path: &Path) -> RsfResult<()> {
    match fs::create_dir(path) {
        Ok(()) => sync_directory(path.parent().ok_or_else(|| corrupt("stage lacks parent"))?)?,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(io_error(error)),
    }
    guard::directory(path)?;
    Ok(())
}
pub(in crate::learner) fn retain_descriptor(directory: &Path, bytes: &[u8]) -> RsfResult<()> {
    match fs::symlink_metadata(directory.join("DESCRIPTOR")) {
        Ok(_) => guard::Exact::capture(&directory.join("DESCRIPTOR"), bytes).map(|_| ()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            // A descriptor may only be initialized in its empty unbound factory
            // directory, never over an existing records store or old evidence.
            for entry in fs::read_dir(directory).map_err(io_error)? {
                let entry = entry.map_err(io_error)?;
                if entry.file_name() != ".DESCRIPTOR.tmp" {
                    return Err(corrupt("nonempty prefix stage lost its descriptor"));
                }
            }
            publish(directory, "DESCRIPTOR", bytes)
        }
        Err(error) => Err(io_error(error)),
    }
}
pub(in crate::learner) struct PreparedPublication {
    directory: PathBuf,
    temporary: PathBuf,
    target: PathBuf,
    exact: guard::Exact,
}
impl PreparedPublication {
    pub(in crate::learner) fn publish(self) -> RsfResult<()> {
        self.exact.validate()?;
        fs::rename(self.temporary, self.target).map_err(io_error)?;
        sync_directory(&self.directory)
    }
}
pub(in crate::learner) fn publish(directory: &Path, name: &str, bytes: &[u8]) -> RsfResult<()> {
    prepare_publication(directory, name, bytes)?.publish()
}
pub(in crate::learner) fn prepare_publication(
    directory: &Path,
    name: &str,
    bytes: &[u8],
) -> RsfResult<PreparedPublication> {
    guard::directory(directory)?;
    let temporary = directory.join(format!(".{name}.tmp"));
    match fs::symlink_metadata(&temporary) {
        Ok(metadata) => {
            guard::file_stamp(&metadata)?;
            // A regular unpublished temporary is replaceable under this lock.
            // Never truncate through an alias or overwrite a published name.
            fs::remove_file(&temporary).map_err(io_error)?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(io_error(error)),
    }
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
        .map_err(io_error)?;
    file.write_all(bytes).map_err(io_error)?;
    file.sync_all().map_err(io_error)?;
    let created = guard::file_stamp(&file.metadata().map_err(io_error)?)?;
    let exact = guard::Exact::capture(&temporary, bytes)?;
    if guard::file_stamp(&fs::symlink_metadata(&temporary).map_err(io_error)?)? != created {
        return Err(corrupt("learner temporary differs from created descriptor"));
    }
    Ok(PreparedPublication {
        directory: directory.to_path_buf(),
        temporary,
        target: directory.join(name),
        exact,
    })
}
pub(in crate::learner) fn flat(path: &Path) -> RsfResult<()> {
    guard::directory(path)?;
    // This first prerequisite consumes a normalized flat learner store. Do not
    // fabricate a general multi-generation/compacted import from pointer flags.
    for name in [
        "VSR_ACTIVE",
        "VSR_PUBLISHED",
        "vsr-views",
        "VSR_PROMOTION",
        ".VSR_PROMOTION.tmp",
    ] {
        match fs::symlink_metadata(path.join(name)) {
            Ok(_) => {
                return Err(invalid_input(
                    "bounded learner prefix requires a flat unpromoted history",
                ));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(io_error(error)),
        }
    }
    Ok(())
}
