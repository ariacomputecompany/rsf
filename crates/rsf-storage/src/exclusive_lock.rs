use std::fs::{File, TryLockError};

/// Exclusive ownership for a writer. Explicit unlock prevents a descriptor
/// inherited by a concurrent fork, or a non-owning duplicate, from prolonging
/// ownership after this guard drops. The OS still releases it on process death.
#[derive(Debug)]
pub struct ExclusiveFileLock(File);
impl ExclusiveFileLock {
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
