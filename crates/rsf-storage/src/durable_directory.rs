use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};

/// Create a directory chain and sync each newly created directory and its
/// parent's link before returning. Existing directory contents are unchanged.
/// Call before creating durable files beneath newly allocated store roots.
pub fn create_dir_all_synced(path: impl AsRef<Path>) -> io::Result<()> {
    let path = path.as_ref();
    let path = if path.as_os_str().is_empty() {
        Path::new(".")
    } else {
        path
    };
    let mut missing = Vec::<PathBuf>::new();
    let mut cursor = path;
    loop {
        match fs::metadata(cursor) {
            Ok(metadata) if metadata.is_dir() => break,
            Ok(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "directory path names a non-directory",
                ));
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                missing.push(cursor.to_path_buf())
            }
            Err(error) => return Err(error),
        }
        cursor = cursor
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
    }
    fs::create_dir_all(path)?;
    for directory in missing.iter().rev() {
        File::open(directory)?.sync_all()?;
        let parent = directory
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    fn root(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "rsf-durable-directory-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }
    #[test]
    fn synced_directory_creation_preserves_existing_files_across_nested_retries() {
        let root = root("nested");
        let directory = root.join("one/two/three");
        create_dir_all_synced(&directory).unwrap();
        fs::write(directory.join("retained"), b"original").unwrap();
        create_dir_all_synced(&directory).unwrap();
        create_dir_all_synced(directory.join("four/five")).unwrap();
        assert_eq!(fs::read(directory.join("retained")).unwrap(), b"original");
        assert!(directory.join("four/five").is_dir());
        fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn synced_directory_creation_rejects_file_obstructions_without_replacing_data() {
        let root = root("obstruction");
        create_dir_all_synced(&root).unwrap();
        let file = root.join("existing");
        fs::write(&file, b"do not replace").unwrap();
        assert!(create_dir_all_synced(&file).is_err());
        assert!(create_dir_all_synced(file.join("child")).is_err());
        assert_eq!(fs::read(&file).unwrap(), b"do not replace");
        fs::remove_dir_all(root).unwrap();
    }
}
