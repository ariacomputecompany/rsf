//! Warm custody checks for an already published non-voting history. A token is
//! only a process-local validation cache; it grants no votes or client access.
use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VsrLearnerStateToken {
    published: [u8; 32],
    cell: VsrStateToken,
}

impl LearnerHistoryStore {
    /// Revalidate actual outer identity/publication/descriptor and inner cell
    /// custody. No files are created, repaired, copied or made authoritative.
    /// A legitimate `finish` of newer history changes this opaque token; callers
    /// must revalidate their transition authority before updating its cache.
    pub fn published_state_token(&self) -> RsfResult<VsrLearnerStateToken> {
        self.check_failure()?;
        let published = self
            .published
            .as_ref()
            .ok_or_else(|| invalid_input("learner has no published recovery anchor"))?;
        let result = self.validate_published_custody(published);
        if result.is_err() {
            // Losing the named outer anchor must fence the entire live learner,
            // just like losing the inner cell's stamped custody. Restoring a
            // file cannot turn this failed owner into a new authority proof.
            self.failed.store(true, Ordering::Relaxed);
        }
        result
    }

    fn validate_published_custody(&self, published: &Published) -> RsfResult<VsrLearnerStateToken> {
        // Normalize only harmless current-directory components. ParentDir
        // would require trusting a different lexical ownership route.
        let mut root = PathBuf::new();
        for component in self.root.components() {
            match component {
                std::path::Component::CurDir => {}
                std::path::Component::ParentDir => {
                    return Err(corrupt("learner custody root contains a parent traversal"));
                }
                component => root.push(component.as_os_str()),
            }
        }
        if root.as_os_str().is_empty() {
            root.push(".");
        }
        regular_directory(&root)?;
        let mut identity = b"RSFL1".to_vec();
        for value in [self.pg.0, self.local.0, self.source.0] {
            identity.extend_from_slice(&value.to_le_bytes());
        }
        exact_file(&root.join("IDENTITY"), &identity)?;
        exact_file(&root.join("ACTIVE"), &published.id)?;
        let directory = self.directory(&published.id);
        regular_directory(&directory)?;
        regular_directory(&directory.join("cell"))?;
        exact_file(
            &directory.join("DESCRIPTOR"),
            &published.descriptor.encode()?,
        )?;
        Ok(VsrLearnerStateToken {
            published: published.id,
            cell: published.cell.recovery_state_token()?,
        })
    }
}

fn regular_directory(path: &Path) -> RsfResult<()> {
    let metadata = fs::symlink_metadata(path).map_err(io_error)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(corrupt(
            "published learner custody is not an owned directory",
        ));
    }
    Ok(())
}
fn exact_file(path: &Path, expected: &[u8]) -> RsfResult<()> {
    let metadata = fs::symlink_metadata(path).map_err(io_error)?;
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || metadata.len() != expected.len() as u64
    {
        return Err(corrupt(
            "published learner custody file is missing, aliased or changed",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.nlink() != 1 {
            return Err(corrupt("published learner custody file is aliased"));
        }
    }
    // The learner owns its namespace and lock. Bound the read even if an
    // external filesystem fault changes length after the regular-file check.
    use std::io::Read;
    let mut bytes = Vec::with_capacity(expected.len());
    File::open(path)
        .map_err(io_error)?
        .take(expected.len() as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(io_error)?;
    if bytes != expected {
        return Err(corrupt(
            "published learner custody differs from the owned snapshot",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
