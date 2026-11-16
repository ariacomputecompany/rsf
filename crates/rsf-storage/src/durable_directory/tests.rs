use super::*;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT: AtomicU64 = AtomicU64::new(0);

struct Root(PathBuf);
impl Root {
    fn new(label: &str) -> Self {
        Self(std::env::temp_dir().join(unique(label)))
    }
}
impl Drop for Root {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!(
                "directory durability fixture retained: {}",
                self.0.display()
            );
        } else {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
}
fn unique(label: &str) -> String {
    format!(
        "rsf-directory-{label}-{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}
fn injected() -> io::Error {
    io::Error::other("injected directory sync failure")
}
fn capture_create(path: &Path) -> Vec<PathBuf> {
    let mut visited = Vec::new();
    create_dir_all_synced_with(path, &mut |directory| {
        visited.push(directory.to_path_buf());
        sync_directory(directory)
    })
    .unwrap();
    visited
}

#[test]
fn interrupted_mkdir_retries_every_existing_ancestor_and_sync_failure() {
    let root = Root::new("retry");
    let target = root.0.join("container/owner/records/segments");
    // The first barrier fails after recursive mkdir has made every name visible.
    assert!(create_dir_all_synced_with(&target, &mut |_| Err(injected())).is_err());
    assert!(target.is_dir());
    let retained = target.join("retained");
    fs::write(&retained, b"must survive every retry").unwrap();

    let visited = capture_create(&target);
    let resolved = fs::canonicalize(&target).unwrap();
    assert_eq!(visited.first(), Some(&resolved));
    // This is the previously missing link: root itself in its parent, not just
    // the inner records owner. All four intermediate ancestors must be covered.
    for name in [
        root.0.clone(),
        root.0.join("container"),
        root.0.join("container/owner"),
        root.0.join("container/owner/records"),
        root.0.parent().unwrap().to_path_buf(),
    ] {
        assert!(visited.contains(&fs::canonicalize(name).unwrap()));
    }
    let physical_root = resolved.ancestors().last().unwrap();
    assert!(visited.contains(&physical_root.to_path_buf()));
    let first_route = &visited[..resolved.ancestors().count()];
    assert!(
        first_route
            .windows(2)
            .all(|pair| pair[0].parent() == Some(pair[1].as_path()))
    );

    for failure in 0..visited.len() {
        let mut attempted = Vec::new();
        let error = create_dir_all_synced_with(&target, &mut |directory| {
            attempted.push(directory.to_path_buf());
            if attempted.len() == failure + 1 {
                Err(injected())
            } else {
                sync_directory(directory)
            }
        })
        .unwrap_err();
        assert!(error.to_string().contains("injected"));
        assert_eq!(attempted, visited[..=failure]);
        assert_eq!(fs::read(&retained).unwrap(), b"must survive every retry");
        assert_eq!(capture_create(&target), visited);
    }
}

#[test]
fn partial_ancestor_creation_and_existing_bytes_are_preserved() {
    let root = Root::new("partial");
    fs::create_dir_all(root.0.join("one")).unwrap();
    fs::write(root.0.join("one/retained"), b"original").unwrap();
    let target = root.0.join("one/two/three");
    let visited = capture_create(&target);
    assert!(visited.contains(&fs::canonicalize(root.0.parent().unwrap()).unwrap()));
    create_dir_all_synced(&target).unwrap();
    create_dir_all_synced(target.join("four/five")).unwrap();
    assert_eq!(fs::read(root.0.join("one/retained")).unwrap(), b"original");
    assert!(target.join("four/five").is_dir());
}

#[test]
fn file_obstructions_are_errors_without_replacement_or_sync_success() {
    let root = Root::new("obstruction");
    fs::create_dir_all(&root.0).unwrap();
    let file = root.0.join("existing");
    fs::write(&file, b"do not replace").unwrap();
    for blocked in [&file, &file.join("child")] {
        let mut synced = false;
        assert!(
            create_dir_all_synced_with(blocked, &mut |_| {
                synced = true;
                Ok(())
            })
            .is_err()
        );
        assert!(!synced);
    }
    assert_eq!(fs::read(file).unwrap(), b"do not replace");
}

#[test]
fn relative_empty_and_parent_components_keep_filesystem_resolution() {
    let cwd = std::env::current_dir().unwrap();
    let relative = PathBuf::from("target").join(unique("relative"));
    let root = Root(cwd.join(&relative));
    let spelling = relative.join("inner/../leaf");
    let visited = capture_create(&spelling);
    assert_eq!(
        visited.first(),
        Some(&fs::canonicalize(root.0.join("leaf")).unwrap())
    );
    assert!(root.0.join("inner").is_dir());
    assert!(visited.contains(&cwd));
    let mut empty_route = Vec::new();
    create_dir_all_synced_with(Path::new(""), &mut |directory| {
        empty_route.push(directory.to_path_buf());
        Ok(())
    })
    .unwrap();
    assert_eq!(empty_route.first(), Some(&fs::canonicalize(cwd).unwrap()));
}

#[cfg(unix)]
#[test]
fn provisioned_aliases_sync_physical_and_lexical_routes_without_replacement() {
    use std::os::unix::fs::symlink;
    let root = Root::new("alias");
    fs::create_dir_all(root.0.join("physical/deep")).unwrap();
    fs::create_dir_all(root.0.join("logical")).unwrap();
    let alias = root.0.join("logical/link");
    symlink(root.0.join("physical/deep"), &alias).unwrap();
    // Provision the alias before exercising the directory-only helper.
    sync_directory(&root.0.join("logical")).unwrap();
    let visited = capture_create(&alias.join("../created/owner"));
    assert!(root.0.join("physical/created/owner").is_dir());
    assert!(!root.0.join("logical/created").exists());
    assert!(visited.contains(&fs::canonicalize(root.0.join("physical")).unwrap()));
    assert!(visited.contains(&root.0.join("logical")));
    assert!(
        fs::symlink_metadata(alias)
            .unwrap()
            .file_type()
            .is_symlink()
    );
}

#[cfg(unix)]
#[test]
fn final_file_alias_publication_covers_both_parents_and_every_failure() {
    use std::os::unix::fs::symlink;
    let root = Root::new("file-alias");
    fs::create_dir_all(root.0.join("target/subdir")).unwrap();
    fs::create_dir_all(root.0.join("alias")).unwrap();
    let target = root.0.join("target/subdir/log");
    let alias = root.0.join("alias/log");
    symlink(&target, &alias).unwrap();
    sync_directory(&root.0.join("alias")).unwrap();
    fs::write(&alias, b"retained bytes").unwrap();
    let mut visited = Vec::new();
    sync_file_publication_with(&alias, &mut |directory| {
        visited.push(directory.to_path_buf());
        sync_directory(directory)
    })
    .unwrap();
    assert_eq!(
        visited.first(),
        Some(&fs::canonicalize(target.parent().unwrap()).unwrap())
    );
    assert!(visited.contains(&alias.parent().unwrap().to_path_buf()));
    for failure in 0..visited.len() {
        let mut attempted = Vec::new();
        assert!(
            sync_file_publication_with(&alias, &mut |directory| {
                attempted.push(directory.to_path_buf());
                if attempted.len() == failure + 1 {
                    Err(injected())
                } else {
                    sync_directory(directory)
                }
            })
            .is_err()
        );
        assert_eq!(attempted, visited[..=failure]);
        sync_file_publication(&alias).unwrap();
        assert_eq!(fs::read(&target).unwrap(), b"retained bytes");
    }
}

#[cfg(unix)]
#[test]
fn process_kill_after_mkdir_requires_barriers_on_visible_retry() {
    use std::io::BufRead;
    use std::process::{Command, Stdio};
    use std::sync::mpsc;
    let root = Root::new("kill");
    let target = root.0.join("container/decision");
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "durable_directory::tests::directory_creation_process_child",
            "--nocapture",
        ])
        .env("RSF_DIRECTORY_CREATION_CHILD", &target)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let (send, receive) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in io::BufReader::new(stdout).lines() {
            match line {
                Ok(line) if line.contains("RSF_DIRECTORY_CREATED") => {
                    let _ = send.send(());
                    return;
                }
                Ok(_) => {}
                Err(_) => return,
            }
        }
    });
    struct Reap {
        child: std::process::Child,
        reader: Option<std::thread::JoinHandle<()>>,
    }
    impl Drop for Reap {
        fn drop(&mut self) {
            let _ = self.child.kill();
            let _ = self.child.wait();
            if let Some(reader) = self.reader.take() {
                let _ = reader.join();
            }
        }
    }
    let mut process = Reap {
        child,
        reader: Some(reader),
    };
    receive
        .recv_timeout(std::time::Duration::from_secs(10))
        .expect("child did not reach pre-sync mkdir boundary");
    process.child.kill().unwrap();
    assert!(!process.child.wait().unwrap().success());
    assert!(target.is_dir());
    let visited = capture_create(&target);
    assert!(visited.contains(&fs::canonicalize(root.0.parent().unwrap()).unwrap()));
    assert!(visited.contains(&fs::canonicalize(&root.0).unwrap()));
    assert!(visited.contains(&fs::canonicalize(root.0.join("container")).unwrap()));
}

#[cfg(unix)]
#[test]
fn directory_creation_process_child() {
    use std::io::Write;
    let Some(target) = std::env::var_os("RSF_DIRECTORY_CREATION_CHILD") else {
        return;
    };
    create_dir_all_synced_with(Path::new(&target), &mut |_| {
        // No filesystem marker: syncing a marker's parent could hide the gap.
        println!("RSF_DIRECTORY_CREATED");
        io::stdout().flush().unwrap();
        loop {
            std::thread::park();
        }
    })
    .unwrap();
}
