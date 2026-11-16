//! Actual directory descriptors retained across an application-source transfer.
use std::fs::{File, Metadata, OpenOptions};
#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Component, Path, PathBuf};

#[derive(Debug, Eq, PartialEq)]
struct Key {
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
}
fn key(metadata: &Metadata) -> Result<Key, String> {
    if !metadata.is_dir() {
        return Err("history source route is not an owned directory".into());
    }
    Ok(Key {
        #[cfg(unix)]
        device: metadata.dev(),
        #[cfg(unix)]
        inode: metadata.ino(),
    })
}
fn io(_: std::io::Error) -> String {
    "history source directory operation failed".into()
}

pub(super) fn normalized(path: &Path) -> Result<PathBuf, String> {
    let input = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir().map_err(io)?.join(path)
    };
    let mut root = PathBuf::new();
    for component in input.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                return Err("history source path contains parent traversal".into());
            }
            component => root.push(component.as_os_str()),
        }
    }
    Ok(root)
}

#[derive(Debug)]
pub(super) struct Directory {
    path: PathBuf,
    file: File,
    key: Key,
}
impl Directory {
    pub(super) fn capture(path: &Path) -> Result<Self, String> {
        #[cfg(not(unix))]
        return Err("history source requires Unix directory custody".into());
        let named = key(&std::fs::symlink_metadata(path).map_err(io)?)?;
        let mut options = OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_DIRECTORY);
        let file = options.open(path).map_err(io)?;
        if key(&file.metadata().map_err(io)?)? != named {
            return Err("history source original directory custody changed".into());
        }
        let result = Self {
            path: path.to_path_buf(),
            file,
            key: named,
        };
        result.validate()?;
        Ok(result)
    }
    pub(super) fn path(&self) -> &Path {
        &self.path
    }
    pub(super) fn validate(&self) -> Result<(), String> {
        if key(&self.file.metadata().map_err(io)?)? != self.key
            || key(&std::fs::symlink_metadata(&self.path).map_err(io)?)? != self.key
        {
            return Err("history source original directory custody changed".into());
        }
        Ok(())
    }
}
