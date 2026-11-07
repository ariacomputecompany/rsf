//! Original legacy bytes and same-descriptor lock creation/acquisition.
use super::*;
use serde::Deserialize;
use std::fs::{self, File, Metadata, OpenOptions};
use std::io::Read;
#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

const MAX_IDENTITY_BYTES: u64 = 128 * 1024;
fn io(_: std::io::Error) -> String {
    "application identity migration physical operation failed".into()
}
fn options() -> OpenOptions {
    let mut options = OpenOptions::new();
    #[cfg(unix)]
    options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    options
}
#[derive(Clone, Debug, Eq, PartialEq)]
struct Stamp {
    #[cfg(unix)]
    dev: u64,
    #[cfg(unix)]
    ino: u64,
    len: u64,
    modified: std::time::SystemTime,
    #[cfg(unix)]
    changed: (i64, i64),
}
impl Stamp {
    fn from(metadata: &Metadata) -> Result<Self, String> {
        if !metadata.is_file() {
            return Err("application identity migration requires regular files".into());
        }
        #[cfg(unix)]
        if metadata.nlink() != 1 {
            return Err("application identity migration file has an alias".into());
        }
        Ok(Self {
            #[cfg(unix)]
            dev: metadata.dev(),
            #[cfg(unix)]
            ino: metadata.ino(),
            len: metadata.len(),
            modified: metadata.modified().map_err(io)?,
            #[cfg(unix)]
            changed: (metadata.ctime(), metadata.ctime_nsec()),
        })
    }
    fn named(path: &Path) -> Result<Option<Self>, String> {
        match fs::symlink_metadata(path) {
            Ok(metadata) => Self::from(&metadata).map(Some),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(io(e)),
        }
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct V1 {
    version: u64,
    cluster: String,
    node: u64,
}
struct Identity {
    file: File,
    stamp: Stamp,
}
impl Identity {
    fn read(path: &Path, cluster: &ClusterId, node: NodeId) -> Result<Self, String> {
        let stamp =
            Stamp::named(path)?.ok_or("application migration requires an existing V1 identity")?;
        if stamp.len > MAX_IDENTITY_BYTES {
            return Err("application migration identity exceeds finite bound".into());
        }
        let file = options().read(true).open(path).map_err(io)?;
        let result = Self { file, stamp };
        result.validate(path)?;
        let mut bytes = Vec::with_capacity(
            usize::try_from(result.stamp.len)
                .map_err(|_| "application identity length exceeds platform")?,
        );
        (&result.file)
            .take(MAX_IDENTITY_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(io)?;
        result.validate(path)?;
        if bytes.len() as u64 != result.stamp.len {
            return Err("application migration original identity length changed".into());
        }
        let stored: V1 = serde_json::from_slice(&bytes)
            .map_err(|_| "application migration requires the exact legacy V1 schema")?;
        if stored.version != 1 {
            return Err("application migration requires the exact legacy V1 schema".into());
        }
        if stored.cluster != cluster.as_str() || stored.node != node.0 {
            return Err(
                "application migration identity belongs to a different cluster or node".into(),
            );
        }
        Ok(result)
    }
    fn validate(&self, path: &Path) -> Result<(), String> {
        if Stamp::from(&self.file.metadata().map_err(io)?)? != self.stamp
            || Stamp::named(path)?.as_ref() != Some(&self.stamp)
        {
            return Err("application migration original identity custody changed".into());
        }
        Ok(())
    }
}

pub(super) struct Locked {
    guard: rsf_storage::ExclusiveFileLock,
    // Private duplicate of the same open file description, never a fresh path
    // open. The exclusive guard remains the sole owner through final success.
    sync_file: File,
    stamp: Stamp,
}
impl Locked {
    pub(super) fn sync(&self) -> Result<(), String> {
        self.sync_file.sync_all().map_err(io)
    }
    fn validate(&self, path: &Path) -> Result<(), String> {
        if self.stamp.len != 0
            || Stamp::from(&self.guard.metadata().map_err(io)?)? != self.stamp
            || Stamp::from(&self.sync_file.metadata().map_err(io)?)? != self.stamp
            || Stamp::named(path)?.as_ref() != Some(&self.stamp)
        {
            return Err("application migration original lock custody changed".into());
        }
        Ok(())
    }
}

pub(super) struct Legacy {
    application: Application,
    directory: Directory,
    identity: Identity,
    original_lock: Option<Stamp>,
}
impl Legacy {
    pub(super) fn capture(state: &Path, cluster: &ClusterId, node: NodeId) -> Result<Self, String> {
        let state = normalized(state)?;
        let application = Application::capture(&state.join("protocol.vsr"))?;
        let directory = Directory::capture(&application.binding())?;
        Self::namespace(directory.path())?;
        let identity = Identity::read(&directory.path().join("IDENTITY.json"), cluster, node)?;
        let original_lock = Stamp::named(&directory.path().join(".peer.lock"))?;
        if original_lock.as_ref().is_some_and(|stamp| stamp.len != 0) {
            return Err("application migration peer lock is not empty".into());
        }
        let result = Self {
            application,
            directory,
            identity,
            original_lock,
        };
        result.validate(None)?;
        Ok(result)
    }
    fn namespace(root: &Path) -> Result<(), String> {
        let mut count = 0;
        for entry in fs::read_dir(root).map_err(io)? {
            count += 1;
            let name = entry.map_err(io)?.file_name();
            if count > 2 || (name != "IDENTITY.json" && name != ".peer.lock") {
                return Err("application migration binding contains foreign entries".into());
            }
        }
        Ok(())
    }
    pub(super) fn binding(&self) -> &Path {
        self.directory.path()
    }
    pub(super) fn validate(&self, lock: Option<&Locked>) -> Result<(), String> {
        self.application.validate()?;
        self.directory.validate()?;
        self.identity
            .validate(&self.binding().join("IDENTITY.json"))?;
        Self::namespace(self.binding())?;
        let path = self.binding().join(".peer.lock");
        match lock {
            Some(lock) => lock.validate(&path)?,
            None if Stamp::named(&path)? != self.original_lock => {
                return Err("application migration original lock custody changed".into());
            }
            None => {}
        }
        self.application.validate()?;
        self.directory.validate()
    }
    pub(super) fn checked_boundary(
        &self,
        lock: Option<&Locked>,
        at: Boundary,
    ) -> Result<(), String> {
        self.validate(lock)?;
        let result = boundary(at, self.binding());
        self.validate(lock)?;
        result
    }
    pub(super) fn lock(&self) -> Result<Locked, String> {
        self.validate(None)?;
        let path = self.binding().join(".peer.lock");
        let file = match &self.original_lock {
            Some(stamp) => {
                let file = options().write(true).open(&path).map_err(io)?;
                if Stamp::from(&file.metadata().map_err(io)?)? != *stamp {
                    return Err("application migration original lock custody changed".into());
                }
                file
            }
            None => options()
                .write(true)
                .create_new(true)
                .open(&path)
                .map_err(io)?,
        };
        let stamp = Stamp::from(&file.metadata().map_err(io)?)?;
        if stamp.len != 0 || Stamp::named(&path)?.as_ref() != Some(&stamp) {
            return Err("application migration original lock custody changed".into());
        }
        let sync_file = file.try_clone().map_err(io)?;
        let guard = rsf_storage::ExclusiveFileLock::acquire(file)
            .map_err(|_| "application migration binding already has an owner")?;
        let lock = Locked {
            guard,
            sync_file,
            stamp,
        };
        self.validate(Some(&lock))?;
        Ok(lock)
    }
    pub(super) fn sync_identity(&self, lock: &Locked) -> Result<(), String> {
        self.validate(Some(lock))?;
        let result = self.identity.file.sync_all().map_err(io);
        self.validate(Some(lock))?;
        result
    }
}
