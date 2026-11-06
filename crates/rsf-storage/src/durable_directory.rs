use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};

/// Create a directory chain and publish its links through the filesystem root.
/// Existing paths receive the same barrier: visibility after an interrupted
/// mkdir does not establish that the previous parent's sync completed.
///
/// The caller must control namespace replacement throughout this operation.
/// Generic trusted aliases are preserved; their routing and mount topology must
/// already be durably provisioned. We sync both the resolved and lexical routes,
/// but do not claim publication of arbitrary intermediate symlink chains outside
/// those routes. This helper creates directories, never symlinks, and does not
/// replace stricter caller-owned path/no-follow validation.
pub fn create_dir_all_synced(path: impl AsRef<Path>) -> io::Result<()> {
    create_dir_all_synced_with(path.as_ref(), &mut sync_directory)
}

/// Renew publication of an already existing directory route without creating
/// or repairing any entry. The same trusted alias topology as the factory applies.
pub fn sync_existing_directory(path: impl AsRef<Path>) -> io::Result<()> {
    let absolute = absolute(path.as_ref())?;
    if !fs::metadata(&absolute)?.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "publication path is not a directory",
        ));
    }
    let resolved = fs::canonicalize(&absolute)?;
    sync_routes(&resolved, &absolute, &mut sync_directory)
}

fn create_dir_all_synced_with(
    path: &Path,
    sync: &mut impl FnMut(&Path) -> io::Result<()>,
) -> io::Result<()> {
    let absolute = absolute(path)?;
    fs::create_dir_all(&absolute)?;
    let resolved = fs::canonicalize(&absolute)?;
    sync_routes(&resolved, &absolute, sync)
}

/// Publish a segment name after the file has been opened/created. The lock's
/// earlier directory allocation cannot publish a subsequently created file.
pub(crate) fn sync_file_publication(path: &Path) -> io::Result<()> {
    sync_file_publication_with(path, &mut sync_directory)
}

fn sync_file_publication_with(
    path: &Path,
    sync: &mut impl FnMut(&Path) -> io::Result<()>,
) -> io::Result<()> {
    let absolute = absolute(path)?;
    let resolved = fs::canonicalize(&absolute)?;
    // A permitted final file alias can create its target in a different parent.
    // The same provisioned-alias/custody contract as directory creation applies.
    let physical = resolved.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "segment has no physical parent",
        )
    })?;
    let lexical = absolute.parent().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "segment has no lexical parent")
    })?;
    sync_routes(physical, lexical, sync)
}

fn absolute(path: &Path) -> io::Result<PathBuf> {
    // Capture cwd once; do not lexically collapse `..` across a trusted alias.
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(std::env::current_dir()?.join(path))
    }
}

fn sync_routes(
    resolved: &Path,
    lexical: &Path,
    sync: &mut impl FnMut(&Path) -> io::Result<()>,
) -> io::Result<()> {
    for directory in resolved.ancestors() {
        sync(directory)?;
    }
    if lexical != resolved {
        // Keep each route bottom-up. No cached success survives this invocation;
        // a retry must repeat every barrier, even if all paths now exist.
        for directory in lexical.ancestors() {
            sync(directory)?;
        }
    }
    Ok(())
}

fn sync_directory(path: &Path) -> io::Result<()> {
    #[cfg(test)]
    SYNC_ATTEMPTS.with(|attempts| attempts.set(attempts.get() + 1));
    File::open(path)?.sync_all()?;
    #[cfg(test)]
    store_open_observation::after_success(path)?;
    Ok(())
}

// Test-only observation, never a production trust/cache decision.
#[cfg(test)]
thread_local! {
    static SYNC_ATTEMPTS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}
#[cfg(test)]
pub(crate) fn sync_attempts() -> usize {
    SYNC_ATTEMPTS.with(std::cell::Cell::get)
}

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) mod store_open_observation;
