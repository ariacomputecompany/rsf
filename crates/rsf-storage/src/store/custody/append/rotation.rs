//! Exact unpublished rotation completion under the original owned namespace.
use super::*;

struct PreparedFile {
    path: PathBuf,
    file: File,
    stamp: FileStamp,
}
impl PreparedFile {
    fn validate(&self) -> RsfResult<()> {
        if file_stamp(&self.file.metadata().map_err(custody_io)?)? != self.stamp
            || regular_file(&self.path)? != self.stamp
        {
            return Err(custody_error(
                "owned rotation path differs from created descriptor",
            ));
        }
        Ok(())
    }
}

pub(super) fn rotate(
    owner: &mut OwnedAppend,
    store: &mut SegmentStore,
    cursor: &mut WalCursor,
) -> RsfResult<()> {
    let next = store.manifest.next_id()?;
    let descriptor = SegmentDescriptor {
        id: next,
        file_name: segment_file_name(next),
    };
    let mut manifest = store.manifest.clone();
    manifest.segments.push(descriptor.clone());
    manifest.active_id = next;
    let segment_path = store.root.join(SEGMENT_DIR).join(&descriptor.file_name);

    // Before its manifest is published, every qualified rotation writer leaves
    // only an empty next segment. Nonempty/aliased unknown files are not repairable.
    let segment = empty(&segment_path)?;
    owner.validate(store, Some(cursor))?;
    let sidecar = empty(&segment_path.with_extension("lock"))?;
    // Strict cold open requires this sidecar to survive with its exact empty
    // shape. Renew the created/reused file itself before publishing its name.
    sidecar.file.sync_all().map_err(custody_io)?;
    sidecar.validate()?;
    let canonical = fs::canonicalize(&sidecar.path).map_err(custody_io)?;
    let lock = SegmentLock::register_opened(canonical, sidecar.file)?;
    if held_lock_stamp(&lock)? != sidecar.stamp {
        return Err(custody_error(
            "owned rotation lock differs from opened sidecar",
        ));
    }
    validate_lock_name(&lock, &sidecar.path)?;
    segment.validate()?;
    segment.file.sync_all().map_err(custody_io)?;
    crate::durable_directory::sync_file_publication(&segment_path).map_err(custody_io)?;
    owner.validate(store, Some(cursor))?;
    segment.validate()?;
    validate_lock_name(&lock, &sidecar.path)?;

    let encoded = manifest.encode();
    let mut publication = manifest_temporary(&store.root.join("MANIFEST.tmp"), encoded.as_bytes())?;
    owner.validate(store, Some(cursor))?;
    segment.validate()?;
    validate_lock_name(&lock, &sidecar.path)?;
    publication.validate()?;
    let target = store.root.join(MANIFEST_FILE);
    fs::rename(&publication.path, &target).map_err(custody_io)?;
    publication.path = target;
    sync_dir(&store.root)?;
    publication.validate()?;
    if immutable::read_checked(&publication.path, &publication.stamp, encoded.len() as u64)?
        != encoded.as_bytes()
    {
        return Err(custody_error(
            "owned rotation published different manifest bytes",
        ));
    }
    // The manifest was intentionally replaced. Every preexisting data/lock
    // identity is still original, and the new descriptor remains held.
    validate_retained(owner, store)?;
    segment.validate()?;
    validate_lock_name(&lock, &sidecar.path)?;

    owner.expected.manifest = publication.stamp;
    owner.expected.segments.push((next, segment.stamp.clone()));
    cursor.stamp.push(SegmentStamp {
        id: next,
        bytes: segment.stamp.bytes,
        modified: segment.stamp.modified,
    });
    store.active = SegmentLog {
        path: segment.path,
        file: segment.file,
        options: store.options.log_options,
        _lock: lock,
    };
    store.manifest = manifest;
    owner.validate(store, Some(cursor))
}

fn validate_retained(owner: &OwnedAppend, store: &SegmentStore) -> RsfResult<()> {
    if directory_identity(&store.root)? != owner.expected.root
        || directory_identity(&store.root.join(SEGMENT_DIR))? != owner.expected.directory
    {
        return Err(custody_error(
            "owned rotation changed its original directory",
        ));
    }
    validate_lock_name(
        &store._lock,
        &store.root.join(MANIFEST_FILE).with_extension("lock"),
    )?;
    validate_lock_name(
        &store.active._lock,
        &store.active.path.with_extension("lock"),
    )?;
    for ((id, expected), descriptor) in owner.expected.segments.iter().zip(&store.manifest.segments)
    {
        if *id != descriptor.id
            || regular_file(&store.root.join(SEGMENT_DIR).join(&descriptor.file_name))? != *expected
        {
            return Err(custody_error("owned rotation changed an existing segment"));
        }
        if *id == store.manifest.active_id
            && file_stamp(&store.active.file.metadata().map_err(custody_io)?)? != *expected
        {
            return Err(custody_error(
                "owned rotation lost its original active descriptor",
            ));
        }
    }
    Ok(())
}

fn nofollow(options: &mut OpenOptions) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
}

fn empty(path: &Path) -> RsfResult<PreparedFile> {
    let mut options = OpenOptions::new();
    options.read(true).append(true).create_new(true);
    nofollow(&mut options);
    let file = match options.open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let expected = regular_file(path)?;
            if expected.bytes != 0 {
                return Err(custody_error(
                    "owned rotation cannot reuse nonempty unpublished segment or lock",
                ));
            }
            let mut options = OpenOptions::new();
            options.read(true).append(true);
            nofollow(&mut options);
            let file = options.open(path).map_err(custody_io)?;
            if file_stamp(&file.metadata().map_err(custody_io)?)? != expected {
                return Err(custody_error(
                    "owned rotation provisional file was replaced",
                ));
            }
            file
        }
        Err(error) => return Err(custody_io(error)),
    };
    let stamp = file_stamp(&file.metadata().map_err(custody_io)?)?;
    if stamp.bytes != 0 {
        return Err(custody_error(
            "owned rotation provisional descriptor is nonempty",
        ));
    }
    let prepared = PreparedFile {
        path: path.to_path_buf(),
        file,
        stamp,
    };
    prepared.validate()?;
    Ok(prepared)
}

fn manifest_temporary(path: &Path, bytes: &[u8]) -> RsfResult<PreparedFile> {
    let mut options = OpenOptions::new();
    options.append(true).create_new(true);
    nofollow(&mut options);
    let (mut file, previous) = match options.open(path) {
        Ok(file) => (file, Vec::new()),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let stamp = regular_file(path)?;
            if stamp.bytes > bytes.len() as u64 {
                return Err(custody_error(
                    "owned manifest temporary exceeds expected encoding",
                ));
            }
            let previous = immutable::read_checked(path, &stamp, bytes.len() as u64)?;
            if !bytes.starts_with(&previous) {
                return Err(custody_error(
                    "owned manifest temporary is not its canonical prefix",
                ));
            }
            let mut options = OpenOptions::new();
            options.append(true);
            nofollow(&mut options);
            let file = options.open(path).map_err(custody_io)?;
            if file_stamp(&file.metadata().map_err(custody_io)?)? != stamp {
                return Err(custody_error(
                    "owned manifest temporary changed before completion",
                ));
            }
            (file, previous)
        }
        Err(error) => return Err(custody_io(error)),
    };
    let before = file_stamp(&file.metadata().map_err(custody_io)?)?;
    if before.bytes != previous.len() as u64 || regular_file(path)? != before {
        return Err(custody_error(
            "owned manifest temporary differs from its captured prefix",
        ));
    }
    file.write_all(&bytes[previous.len()..])
        .map_err(custody_io)?;
    file.sync_all().map_err(custody_io)?;
    let stamp = file_stamp(&file.metadata().map_err(custody_io)?)?;
    if stamp.identity != before.identity || stamp.bytes != bytes.len() as u64 {
        return Err(custody_error(
            "owned manifest completion differs from exact write",
        ));
    }
    let prepared = PreparedFile {
        path: path.to_path_buf(),
        file,
        stamp,
    };
    prepared.validate()?;
    if immutable::read_checked(path, &prepared.stamp, bytes.len() as u64)? != bytes {
        return Err(custody_error("owned manifest completion bytes differ"));
    }
    Ok(prepared)
}
