//! Each phase consumes originals; successful unlinks evolve only expected absence.
use super::*;

pub(super) fn children(
    mut inventory: SegmentDiscardInventory,
    mut parent: impl FnMut() -> RsfResult<()>,
) -> RsfResult<SegmentDiscardRemainder> {
    let gate = inventory.state.gate.clone();
    let _gate = lock_gate(&gate)?;
    let state = &mut inventory.state;
    state.parent_check(&mut parent)?;
    if state.entries.contains_key(Path::new("MANIFEST.tmp")) {
        unlink(state, Path::new("MANIFEST.tmp"), &mut parent)?;
        // Required for the manifestless factory: its temporary may exist only
        // with the initial segment. Do not remove that segment before this sync.
        sync_root(state, &mut parent)?;
    }
    let children = state
        .entries
        .keys()
        .filter(|p| p.parent() == Some(Path::new(SEGMENT_DIR)))
        .cloned()
        .collect::<Vec<_>>();
    for child in children {
        unlink(state, &child, &mut parent)?;
    }
    if state.entries.contains_key(Path::new(SEGMENT_DIR)) {
        let Kind::Directory(identity) = &state.entries[Path::new(SEGMENT_DIR)].kind else {
            unreachable!("captured segments directory")
        };
        sync(state, &state.root.join(SEGMENT_DIR), identity, &mut parent)?;
        rmdir(state, Path::new(SEGMENT_DIR), &mut parent)?;
    }
    if state.entries.contains_key(Path::new("MANIFEST.lock")) {
        unlink(state, Path::new("MANIFEST.lock"), &mut parent)?;
    }
    // Keep original MANIFEST until every storage child and then the caller's
    // own auxiliary names have been durably removed.
    sync_root(state, &mut parent)?;
    state.parent_check(&mut parent)?;
    Ok(SegmentDiscardRemainder {
        state: inventory.state,
        other: inventory.other,
    })
}

pub(super) fn finish(
    mut remainder: SegmentDiscardRemainder,
    mut parent: impl FnMut() -> RsfResult<()>,
) -> RsfResult<()> {
    let state = &mut remainder.state;
    let gate = state.gate.clone();
    let _gate = lock_gate(&gate)?;
    // This is the only external-name transition. The domain owner preflighted
    // the original set and owns its deletions. No name is excluded or adopted.
    for name in &remainder.other {
        require_absent(&state.root.join(name))?;
    }
    for name in &remainder.other {
        state
            .entries
            .get_mut(&PathBuf::from(name))
            .expect("captured auxiliary")
            .present = false;
    }
    state.parent_check(&mut parent)?;
    // Always cover the caller-owned deletion phase before the final anchor.
    sync_root(state, &mut parent)?;
    if state.entries.contains_key(Path::new(MANIFEST_FILE)) {
        unlink(state, Path::new(MANIFEST_FILE), &mut parent)?;
        sync_root(state, &mut parent)?;
    }
    state.parent_check(&mut parent)?;
    before(Operation::RemoveDirectory, &state.root)?;
    fs::remove_dir(&state.root).map_err(custody_io)?;
    state.root_removed = true;
    after(Operation::RemoveDirectory, &state.root)?;
    state.parent_check(&mut parent)?;
    sync(state, &state.parent, &state.parent_identity, &mut parent)?;
    state.parent_check(&mut parent)
}

fn unlink(
    state: &mut State,
    relative: &Path,
    parent: &mut impl FnMut() -> RsfResult<()>,
) -> RsfResult<()> {
    state.parent_check(parent)?;
    let entry = state.entries.get(relative).expect("inventoried unlink");
    if !entry.present || !matches!(entry.kind, Kind::File { .. }) {
        return Err(custody_error(
            "discard unlink requires original surviving file",
        ));
    }
    let path = state.root.join(relative);
    before(Operation::Unlink, &path)?;
    fs::remove_file(&path).map_err(custody_io)?;
    state
        .entries
        .get_mut(relative)
        .expect("inventoried unlink")
        .present = false;
    after(Operation::Unlink, &path)?;
    state.parent_check(parent)
}
fn rmdir(
    state: &mut State,
    relative: &Path,
    parent: &mut impl FnMut() -> RsfResult<()>,
) -> RsfResult<()> {
    state.parent_check(parent)?;
    let entry = state.entries.get(relative).expect("inventoried rmdir");
    if !entry.present || !matches!(entry.kind, Kind::Directory(_)) {
        return Err(custody_error(
            "discard rmdir requires original surviving directory",
        ));
    }
    let path = state.root.join(relative);
    before(Operation::RemoveDirectory, &path)?;
    fs::remove_dir(&path).map_err(custody_io)?;
    state
        .entries
        .get_mut(relative)
        .expect("inventoried rmdir")
        .present = false;
    after(Operation::RemoveDirectory, &path)?;
    state.parent_check(parent)
}
fn sync_root(state: &State, parent: &mut impl FnMut() -> RsfResult<()>) -> RsfResult<()> {
    sync(state, &state.root, &state.root_identity, parent)
}
fn sync(
    state: &State,
    path: &Path,
    expected: &Identity,
    parent: &mut impl FnMut() -> RsfResult<()>,
) -> RsfResult<()> {
    state.parent_check(parent)?;
    if directory_identity(path)? != *expected {
        return Err(custody_error(
            "discard sync directory differs from original",
        ));
    }
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_DIRECTORY);
    }
    let file = options.open(path).map_err(custody_io)?;
    if !file.metadata().map_err(custody_io)?.is_dir()
        || identity(&file.metadata().map_err(custody_io)?)? != *expected
    {
        return Err(custody_error(
            "discard opened sync descriptor differs from original",
        ));
    }
    before(Operation::Sync, path)?;
    file.sync_all()
        .map_err(|error| storage_error(StorageIoOperation::Sync, error))?;
    after(Operation::Sync, path)?;
    if identity(&file.metadata().map_err(custody_io)?)? != *expected
        || directory_identity(path)? != *expected
    {
        return Err(custody_error("discard sync directory changed"));
    }
    state.parent_check(parent)
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Operation {
    Unlink,
    RemoveDirectory,
    Sync,
}
fn before(operation: Operation, path: &Path) -> RsfResult<()> {
    #[cfg(test)]
    {
        super::observation::at(operation, false, path)?;
    }
    #[cfg(not(test))]
    {
        let _ = (operation, path);
    }
    Ok(())
}
fn after(operation: Operation, path: &Path) -> RsfResult<()> {
    #[cfg(test)]
    {
        super::observation::at(operation, true, path)?;
    }
    #[cfg(not(test))]
    {
        let _ = (operation, path);
    }
    Ok(())
}
