use super::*;

pub(super) fn target(path: &Path, learner: &Path) -> RsfResult<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir().map_err(io_error)?.join(path)
    };
    let mut ancestor = absolute.clone();
    let mut missing = Vec::new();
    loop {
        match fs::symlink_metadata(&ancestor) {
            Ok(meta) => {
                if missing.is_empty() && meta.file_type().is_symlink() {
                    return Err(invalid_input("promotion target cannot be a symlink"));
                }
                break;
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                missing.push(
                    ancestor
                        .file_name()
                        .ok_or_else(|| invalid_input("invalid promotion target path"))?
                        .to_owned(),
                );
                if !ancestor.pop() {
                    return Err(invalid_input("promotion target has no existing ancestor"));
                }
            }
            Err(e) => return Err(io_error(e)),
        }
    }
    let mut target = fs::canonicalize(ancestor).map_err(io_error)?;
    for component in missing.into_iter().rev() {
        target.push(component);
    }
    let learner = fs::canonicalize(learner).map_err(io_error)?;
    if target.starts_with(&learner) || learner.starts_with(&target) {
        return Err(invalid_input(
            "promotion target overlaps learner-owned storage",
        ));
    }
    // Another VSR or learner may reclaim its generated subdirectories. A new
    // writer lock does not make a descendant independent of that ownership.
    for ancestor in target.parent().into_iter().flat_map(Path::ancestors) {
        for marker in [".vsr-writer.lock", ".learner.lock"] {
            match fs::symlink_metadata(ancestor.join(marker)) {
                Ok(_) => {
                    return Err(invalid_input(
                        "promotion target is nested in another owned store",
                    ));
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(io_error(e)),
            }
        }
    }
    Ok(target)
}

pub(super) fn lock(root: &Path) -> RsfResult<rsf_storage::ExclusiveFileLock> {
    rsf_storage::create_dir_all_synced(root).map_err(io_error)?;
    let path = root.join(".vsr-writer.lock");
    if fs::symlink_metadata(&path).is_ok_and(|m| m.file_type().is_symlink()) {
        return Err(invalid_input("promotion writer lock cannot be a symlink"));
    }
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(path)
        .map_err(io_error)?;
    rsf_storage::ExclusiveFileLock::acquire(file)
        .map_err(|e| invalid_input(format!("promotion target already has a writer: {e}")))
}
