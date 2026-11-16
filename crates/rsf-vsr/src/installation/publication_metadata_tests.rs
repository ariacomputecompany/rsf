use super::*;
use std::io::Read;

fn prefix(path: &Path, maximum: usize) -> Vec<u8> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)
        .unwrap()
        .take(maximum as u64 + 1)
        .read_to_end(&mut bytes)
        .unwrap();
    bytes
}

#[test]
fn fixed_metadata_preserves_default_missing_pointer_and_valid_installed_generation() {
    let path = root("fixed-metadata-default");
    let directory = path.join("default");
    let cell = open(&directory);
    let active = publication::active(&directory, PlacementGroupId(1)).unwrap();
    assert_eq!(active.path, directory);
    assert!(active.plan.is_none());
    assert!(!directory.join("VSR_ACTIVE").exists());
    assert!(!directory.join("VSR_PUBLISHED").exists());
    drop(cell);

    let (mut first, second, snapshots) = fixture(&path);
    let plan = VsrViewInstallation::select(&snapshots).unwrap();
    first.install_view(&plan).unwrap();
    let directory = path.join("1");
    let expected = first.committed_prefix().unwrap();
    drop(first);
    drop(second);
    let active = publication::active(&directory, PlacementGroupId(1)).unwrap();
    assert_eq!(active.plan, Some(plan.id()));
    assert_eq!(
        std::fs::metadata(directory.join("VSR_ACTIVE"))
            .unwrap()
            .len(),
        108
    );
    assert_eq!(
        std::fs::metadata(active.path.join("VSR_SEAL"))
            .unwrap()
            .len(),
        116
    );
    publication::mark_published(&directory, PlacementGroupId(1)).unwrap();
    let marker = std::fs::read(directory.join("VSR_PUBLISHED")).unwrap();
    assert_eq!(marker.len(), 44);
    std::fs::remove_file(directory.join("VSR_PUBLISHED")).unwrap();
    publication::mark_published(&directory, PlacementGroupId(1)).unwrap();
    assert_eq!(
        std::fs::read(directory.join("VSR_PUBLISHED")).unwrap(),
        marker
    );
    let cell = DurableVsrCell::open_existing(&directory, PlacementGroupId(1)).unwrap();
    assert_eq!(cell.committed_prefix().unwrap(), expected);
    drop(cell);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn fixed_pointer_seal_and_marker_reject_sparse_trailing_truncated_and_corrupt_files_without_repair()
{
    let path = root("fixed-metadata-reject");
    let (mut first, second, snapshots) = fixture(&path);
    first
        .install_view(&VsrViewInstallation::select(&snapshots).unwrap())
        .unwrap();
    let directory = path.join("1");
    let generation = publication::active(&directory, PlacementGroupId(1))
        .unwrap()
        .path;
    drop(first);
    drop(second);
    for (file, maximum, marker) in [
        (directory.join("VSR_ACTIVE"), 140, false),
        (generation.join("VSR_SEAL"), 116, false),
        (directory.join("VSR_PUBLISHED"), 44, true),
    ] {
        let valid = std::fs::read(&file).unwrap();
        for fault in ["sparse", "trailing", "truncated", "checksum"] {
            std::fs::write(&file, &valid).unwrap();
            match fault {
                "sparse" => std::fs::OpenOptions::new()
                    .write(true)
                    .open(&file)
                    .unwrap()
                    .set_len(16 * 1024 * 1024)
                    .unwrap(),
                "trailing" => {
                    let mut bytes = valid.clone();
                    bytes.push(0);
                    std::fs::write(&file, bytes).unwrap();
                }
                "truncated" => std::fs::write(&file, &valid[..valid.len() - 1]).unwrap(),
                "checksum" => {
                    let mut bytes = valid.clone();
                    *bytes.last_mut().unwrap() ^= 1;
                    std::fs::write(&file, bytes).unwrap();
                }
                _ => unreachable!(),
            }
            let before = prefix(&file, maximum);
            let length = std::fs::metadata(&file).unwrap().len();
            let result = if marker {
                publication::mark_published(&directory, PlacementGroupId(1))
            } else {
                publication::active(&directory, PlacementGroupId(1)).map(|_| ())
            };
            assert!(result.is_err(), "{} {fault}", file.display());
            assert_eq!(std::fs::metadata(&file).unwrap().len(), length);
            assert_eq!(prefix(&file, maximum), before);
        }
        std::fs::write(&file, &valid).unwrap();
        std::fs::remove_file(&file).unwrap();
        if marker {
            publication::mark_published(&directory, PlacementGroupId(1)).unwrap();
            assert_eq!(std::fs::read(&file).unwrap(), valid);
        } else {
            assert!(publication::active(&directory, PlacementGroupId(1)).is_err());
            assert!(!file.exists());
            std::fs::write(&file, &valid).unwrap();
        }
    }
    let cold = DurableVsrCell::open_existing(&directory, PlacementGroupId(1)).unwrap();
    assert_eq!(cold.view(), Epoch(2));
    drop(cold);
    std::fs::remove_dir_all(path).unwrap();
}
