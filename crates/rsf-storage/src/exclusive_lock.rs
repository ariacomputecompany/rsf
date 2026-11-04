use std::fs::{File, TryLockError};

/// Exclusive ownership for a writer. Explicit unlock prevents a descriptor
/// inherited by a concurrent fork, or a non-owning duplicate, from prolonging
/// ownership after this guard drops. The OS still releases it on process death.
#[derive(Debug)]
pub struct ExclusiveFileLock(File);
impl ExclusiveFileLock {
    /// Inspect the actual held descriptor so an exclusive namespace owner can
    /// compare it with its named sidecar. This grants no replacement/repair API.
    pub fn metadata(&self) -> std::io::Result<std::fs::Metadata> {
        self.0.metadata()
    }

    pub fn acquire(file: File) -> Result<Self, TryLockError> {
        file.try_lock()?;
        Ok(Self(file))
    }
}
impl Drop for ExclusiveFileLock {
    fn drop(&mut self) {
        let _ = self.0.unlock();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::OpenOptions;
    #[test]
    fn dropping_owner_releases_lock_even_while_a_duplicate_descriptor_lives() {
        let path = std::env::temp_dir().join(format!("rsf-exclusive-lock-{}", std::process::id()));
        let open = || {
            OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .open(&path)
                .unwrap()
        };
        let file = open();
        let duplicate = file.try_clone().unwrap();
        let owner = ExclusiveFileLock::acquire(file).unwrap();
        assert!(ExclusiveFileLock::acquire(open()).is_err());
        drop(owner);
        let reopened = ExclusiveFileLock::acquire(open()).unwrap();
        drop(duplicate);
        // Closing a non-owning duplicate cannot release the new owner's lock.
        assert!(ExclusiveFileLock::acquire(open()).is_err());
        drop(reopened);
        std::fs::remove_file(path).unwrap();
    }
}

#[cfg(all(test, unix))]
mod custody_tests {
    use super::*;
    use std::os::unix::fs::MetadataExt;
    #[test]
    fn held_lock_metadata_stays_with_original_inode_after_named_replacement() {
        let path = std::env::temp_dir().join(format!(
            "rsf-held-lock-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .unwrap();
        let owner = ExclusiveFileLock::acquire(file).unwrap();
        let original = owner.metadata().unwrap();
        let saved = path.with_extension("original");
        std::fs::rename(&path, &saved).unwrap();
        std::fs::write(&path, b"").unwrap();
        let held = owner.metadata().unwrap();
        let named = std::fs::metadata(&path).unwrap();
        assert_eq!((held.dev(), held.ino()), (original.dev(), original.ino()));
        assert_ne!((held.dev(), held.ino()), (named.dev(), named.ino()));
        assert_eq!(held.len(), 0);
        assert_eq!(named.len(), 0);
        drop(owner);
        std::fs::remove_file(path).unwrap();
        std::fs::remove_file(saved).unwrap();
    }
}
