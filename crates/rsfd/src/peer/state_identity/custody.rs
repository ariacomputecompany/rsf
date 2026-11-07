//! Exact native root/held-lock/file custody and prepared-inode publication.
use super::*;
use std::fs::{self, Metadata};
use std::io::{Read, Seek, SeekFrom};
#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

#[derive(Clone, Debug, Eq, PartialEq)]
struct Key {
    #[cfg(unix)]
    dev: u64,
    #[cfg(unix)]
    ino: u64,
}
fn key(m: &Metadata) -> Key {
    Key {
        #[cfg(unix)]
        dev: m.dev(),
        #[cfg(unix)]
        ino: m.ino(),
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
struct Stamp {
    key: Key,
    len: u64,
    modified: std::time::SystemTime,
    #[cfg(unix)]
    changed: (i64, i64),
}
fn stamp(m: &Metadata) -> Result<Stamp, String> {
    if !m.is_file() {
        return Err("native identity requires regular files".into());
    }
    #[cfg(unix)]
    if m.nlink() != 1 {
        return Err("native identity file has an alias".into());
    }
    Ok(Stamp {
        key: key(m),
        len: m.len(),
        modified: m.modified().map_err(io)?,
        #[cfg(unix)]
        changed: (m.ctime(), m.ctime_nsec()),
    })
}
fn io(_: std::io::Error) -> String {
    "native identity physical operation failed".into()
}
fn inspect(path: &Path) -> Result<Option<Metadata>, String> {
    match fs::symlink_metadata(path) {
        Ok(m) => Ok(Some(m)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(io(e)),
    }
}
fn options() -> OpenOptions {
    let mut o = OpenOptions::new();
    #[cfg(unix)]
    o.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    o
}
fn directory(path: &Path) -> Result<(File, Key), String> {
    let named = inspect(path)?.ok_or("native identity root is absent")?;
    if !named.is_dir() {
        return Err("native identity root is not an owned directory".into());
    }
    let mut o = options();
    o.read(true);
    #[cfg(unix)]
    o.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_DIRECTORY);
    let file = o.open(path).map_err(io)?;
    let actual = file.metadata().map_err(io)?;
    if !actual.is_dir() || key(&actual) != key(&named) {
        return Err("native identity root changed during open".into());
    }
    Ok((file, key(&actual)))
}
pub(super) fn normalized(path: &Path) -> Result<PathBuf, String> {
    let input = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir().map_err(io)?.join(path)
    };
    let mut root = PathBuf::new();
    for c in input.components() {
        match c {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                return Err("native identity path contains parent traversal".into());
            }
            c => root.push(c.as_os_str()),
        }
    }
    Ok(root)
}

struct Exact {
    file: File,
    stamp: Stamp,
    bytes: Vec<u8>,
}
impl Exact {
    fn read(path: &Path, maximum: u64) -> Result<Self, String> {
        let named = inspect(path)?.ok_or("native identity file is absent")?;
        let named = stamp(&named)?;
        if named.len > maximum {
            return Err("native identity file exceeds finite bound".into());
        }
        let file = options().read(true).open(path).map_err(io)?;
        if stamp(&file.metadata().map_err(io)?)? != named {
            return Err("native identity file changed during open".into());
        }
        let mut bytes = Vec::with_capacity(
            usize::try_from(named.len).map_err(|_| "native identity length exceeds platform")?,
        );
        (&file)
            .take(
                maximum
                    .checked_add(1)
                    .ok_or("native identity cap overflow")?,
            )
            .read_to_end(&mut bytes)
            .map_err(io)?;
        if bytes.len() as u64 != named.len {
            return Err("native identity file length changed".into());
        }
        let exact = Self {
            file,
            stamp: named,
            bytes,
        };
        exact.validate(path)?;
        Ok(exact)
    }
    fn read_prepared(mut file: File, path: &Path, intended: &[u8]) -> Result<Self, String> {
        let original = stamp(&file.metadata().map_err(io)?)?;
        if intended.len() > MAX_IDENTITY_BYTES as usize || original.len > MAX_IDENTITY_BYTES {
            return Err("prepared native identity exceeds finite bound".into());
        }
        if original.len != intended.len() as u64 {
            return Err("prepared native identity length differs from intended bytes".into());
        }
        if inspect(path)?.as_ref().map(stamp).transpose()?.as_ref() != Some(&original) {
            return Err("prepared native identity original file custody changed".into());
        }
        // Read through the retained create-new descriptor. The pending name
        // never supplies a replacement owner or unverified intended contents.
        file.seek(SeekFrom::Start(0)).map_err(io)?;
        let mut bytes = vec![0; intended.len()];
        file.read_exact(&mut bytes).map_err(io)?;
        let mut extra = [0; 1];
        let trailing = file.read(&mut extra).map_err(io)?;
        let exact = Self {
            file,
            stamp: original,
            bytes,
        };
        exact.validate(path)?;
        if trailing != 0 || exact.bytes.as_slice() != intended {
            return Err("prepared native identity bytes differ from intended bytes".into());
        }
        Ok(exact)
    }
    fn validate(&self, path: &Path) -> Result<(), String> {
        if stamp(&self.file.metadata().map_err(io)?)? != self.stamp
            || inspect(path)?.as_ref().map(stamp).transpose()?.as_ref() != Some(&self.stamp)
        {
            return Err("native identity original file custody changed".into());
        }
        Ok(())
    }
}

pub(super) struct Custody {
    root: PathBuf,
    directory: File,
    root_key: Key,
    lock: rsf_storage::ExclusiveFileLock,
    lock_key: Key,
    identity: Option<Exact>,
}
impl Custody {
    pub(super) fn open(root: &Path, existing: bool) -> Result<Self, String> {
        #[cfg(not(unix))]
        return Err("native control identity requires Unix descriptor custody".into());
        let root = normalized(root)?;
        let (directory, root_key) = directory(&root)?;
        let expected_identity = if inspect(&root.join("IDENTITY.json"))?.is_some() {
            Some(Exact::read(
                &root.join("IDENTITY.json"),
                MAX_IDENTITY_BYTES,
            )?)
        } else {
            None
        };
        let identity_exists = expected_identity.is_some();
        if existing && !identity_exists {
            return Err("native identity is required".into());
        }
        let path = root.join(".peer.lock");
        let named = inspect(&path)?;
        if identity_exists && named.is_none() {
            return Err("existing native identity lost peer lock".into());
        }
        let file = match named {
            Some(m) => {
                let s = stamp(&m)?;
                if s.len != 0 {
                    return Err("peer lock is not empty".into());
                }
                let file = options().write(true).open(&path).map_err(io)?;
                if stamp(&file.metadata().map_err(io)?)? != s {
                    return Err("peer lock changed during open".into());
                }
                file
            }
            None => options()
                .write(true)
                .create_new(true)
                .open(&path)
                .map_err(io)?,
        };
        file.sync_all().map_err(io)?;
        let lock = rsf_storage::ExclusiveFileLock::acquire(file)
            .map_err(|_| "peer state already has an owner")?;
        let lock_key = key(&lock.metadata().map_err(io)?);
        let mut result = Self {
            root,
            directory,
            root_key,
            lock,
            lock_key,
            identity: None,
        };
        result.anchors()?;
        if inspect(&result.root.join("IDENTITY.json"))?.is_some() != identity_exists {
            return Err("peer identity appeared during ownership acquisition".into());
        }
        if let Some(expected) = expected_identity {
            expected.validate(&result.root.join("IDENTITY.json"))?;
            result.identity = Some(expected);
        }
        result.validate()?;
        Ok(result)
    }
    pub(super) fn root(&self) -> &Path {
        &self.root
    }
    pub(super) fn bytes(&self) -> Option<&[u8]> {
        self.identity.as_ref().map(|e| e.bytes.as_slice())
    }
    fn anchors(&self) -> Result<(), String> {
        let named = inspect(&self.root)?.ok_or("native identity root disappeared")?;
        if !named.is_dir()
            || key(&named) != self.root_key
            || key(&self.directory.metadata().map_err(io)?) != self.root_key
        {
            return Err("native identity root custody changed".into());
        }
        let named = inspect(&self.root.join(".peer.lock"))?
            .ok_or("native identity peer lock disappeared")?;
        let named = stamp(&named)?;
        let held = stamp(&self.lock.metadata().map_err(io)?)?;
        if named.key != self.lock_key
            || held.key != self.lock_key
            || named.len != 0
            || held.len != 0
        {
            return Err("native identity held peer lock differs from its name".into());
        }
        Ok(())
    }
    pub(super) fn validate(&self) -> Result<(), String> {
        self.anchors()?;
        match &self.identity {
            Some(e) => e.validate(&self.root.join("IDENTITY.json")),
            None if inspect(&self.root.join("IDENTITY.json"))?.is_none() => Ok(()),
            None => Err("unpublished native identity unexpectedly appeared".into()),
        }
    }
    pub(super) fn empty_factory(&self) -> Result<(), String> {
        self.validate()?;
        let mut count = 0;
        for entry in fs::read_dir(&self.root).map_err(io)? {
            count += 1;
            if count > 2 {
                return Err(
                    "unbound peer state contains existing data; migrate it explicitly".into(),
                );
            }
            let entry = entry.map_err(io)?;
            match entry.file_name().to_str() {
                Some(".peer.lock") => {}
                Some("IDENTITY.pending") => {
                    Exact::read(&entry.path(), MAX_IDENTITY_BYTES)?;
                }
                _ => {
                    return Err(
                        "unbound peer state contains existing data; migrate it explicitly".into(),
                    );
                }
            }
        }
        self.validate()
    }
    pub(super) fn renew(&self) -> Result<(), String> {
        self.validate()?;
        if let Some(identity) = &self.identity {
            identity.file.sync_all().map_err(io)?;
        }
        rsf_storage::sync_existing_directory(&self.root).map_err(io)?;
        self.validate()
    }
    pub(super) fn publish(&mut self, bytes: &[u8]) -> Result<(), String> {
        if bytes.len() > MAX_IDENTITY_BYTES as usize {
            return Err("peer identity exceeds finite bound".into());
        }
        self.validate()?;
        let pending = self.root.join("IDENTITY.pending");
        if inspect(&pending)?.is_some() {
            let prior = Exact::read(&pending, MAX_IDENTITY_BYTES)?;
            self.validate()?;
            prior.validate(&pending)?;
            fs::remove_file(&pending).map_err(io)?;
        }
        boundary(Boundary::BeforeWrite, &self.root)?;
        let mut file = options()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&pending)
            .map_err(io)?;
        file.write_all(bytes).map_err(io)?;
        boundary(Boundary::BeforeFileSync, &self.root)?;
        file.sync_all().map_err(io)?;
        let prepared = Exact::read_prepared(file, &pending, bytes)?;
        self.validate()?;
        prepared.validate(&pending)?;
        boundary(Boundary::BeforeRename, &self.root)?;
        self.validate()?;
        prepared.validate(&pending)?;
        fs::rename(&pending, self.root.join("IDENTITY.json")).map_err(io)?;
        boundary(Boundary::AfterRename, &self.root)?;
        // Rename can change ctime. Renew only the same held prepared inode's
        // expected name transition, never adopt an independently opened file.
        self.anchors()?;
        let after = stamp(&prepared.file.metadata().map_err(io)?)?;
        if after.key != prepared.stamp.key
            || after.len != prepared.stamp.len
            || after.modified != prepared.stamp.modified
        {
            return Err("prepared native identity changed during rename".into());
        }
        // Rename may change ctime, but it cannot authorize different bytes.
        // Bind the renewed stamp to a fresh bounded read of the same retained
        // descriptor and the already verified intended contents.
        let prepared = Exact::read_prepared(
            prepared.file,
            &self.root.join("IDENTITY.json"),
            &prepared.bytes,
        )?;
        boundary(Boundary::BeforeDirectorySync, &self.root)?;
        self.directory.sync_all().map_err(io)?;
        rsf_storage::sync_existing_directory(&self.root).map_err(io)?;
        boundary(Boundary::AfterDirectorySync, &self.root)?;
        self.anchors()?;
        prepared.validate(&self.root.join("IDENTITY.json"))?;
        self.identity = Some(prepared);
        self.validate()
    }
}
