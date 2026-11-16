use super::*;

#[cfg(unix)]
fn files(path: &Path) -> BTreeMap<std::path::PathBuf, Vec<u8>> {
    if path.is_file() {
        return BTreeMap::from([(path.to_path_buf(), std::fs::read(path).unwrap())]);
    }
    std::fs::read_dir(path)
        .unwrap()
        .flat_map(|entry| files(&entry.unwrap().path()))
        .collect()
}

#[cfg(unix)]
#[test]
fn authority_input_routes_reject_aliases_before_recovery_or_append_warm_and_cold() {
    use std::os::unix::fs::symlink;
    for decision in [false, true] {
        for cold in [false, true] {
            for route in [
                "inputs",
                "input-id",
                "descriptor",
                "records",
                "manifest",
                "segments",
                "segment",
                "manifest-lock",
                "segment-lock",
                "manifest-temporary",
                "quarantine",
            ] {
                let path = root("input-custody-route");
                let (a, _) = approvals(&path);
                let input = VsrMembershipInput::from_approval(&a).unwrap();
                let owned = path.join("owned");
                let mut owner = Owner::create(&owned, decision, &a);
                // Establish the same upload context for both owners. Reads,
                // append retries, and cold recovery must all reject an alias.
                owner.begin(input.clone()).unwrap();
                let directory = input_path(&owned, &input);
                let records = directory.join("records");
                let segment = std::fs::read_dir(records.join("segments"))
                    .unwrap()
                    .map(|entry| entry.unwrap().path())
                    .find(|path| path.extension() == Some(std::ffi::OsStr::new("segment")))
                    .unwrap();
                let selected = match route {
                    "inputs" => owned.join("inputs"),
                    "input-id" => directory.clone(),
                    "descriptor" => directory.join("DESCRIPTOR"),
                    "records" => records.clone(),
                    "manifest" => records.join("MANIFEST"),
                    "segments" => records.join("segments"),
                    "segment" => segment.clone(),
                    "manifest-lock" => records.join("MANIFEST.lock"),
                    "segment-lock" => segment.with_extension("lock"),
                    "manifest-temporary" => records.join("MANIFEST.tmp"),
                    "quarantine" => records.join("quarantine"),
                    _ => unreachable!(),
                };
                let foreign = path.join("foreign");
                if selected.exists() {
                    std::fs::rename(&selected, &foreign).unwrap();
                } else if route == "quarantine" {
                    std::fs::create_dir(&foreign).unwrap();
                    std::fs::write(foreign.join("sentinel"), b"outside directory").unwrap();
                } else {
                    std::fs::write(&foreign, b"outside file").unwrap();
                }
                symlink(&foreign, &selected).unwrap();
                let foreign_before = files(&foreign);
                let journal_before = journal_bytes(&owned, decision);
                if cold {
                    drop(owner);
                    for _ in 0..2 {
                        assert!(
                            Owner::reopen(&owned, decision).is_err(),
                            "{decision}/{route}: cold alias accepted"
                        );
                        assert_eq!(files(&foreign), foreign_before);
                        assert_eq!(journal_bytes(&owned, decision), journal_before);
                    }
                } else {
                    let batch =
                        VsrHistoryBatch::pack(0, &input.flattened_records(&a).unwrap()).unwrap();
                    for _ in 0..2 {
                        assert!(
                            owner.read(input.id().unwrap()).is_err(),
                            "{decision}/{route}: read alias accepted"
                        );
                        assert!(
                            owner.append(&input, batch.clone()).is_err(),
                            "{decision}/{route}: append alias accepted"
                        );
                        assert!(
                            owner.begin(input.clone()).is_err(),
                            "{decision}/{route}: begin alias accepted"
                        );
                        assert_eq!(files(&foreign), foreign_before);
                        assert_eq!(journal_bytes(&owned, decision), journal_before);
                    }
                    drop(owner);
                }
                std::fs::remove_file(selected).unwrap();
                std::fs::remove_dir_all(path).unwrap();
            }
        }
    }
}

#[test]
fn authority_input_missing_bound_files_are_not_recreated() {
    for decision in [false, true] {
        for missing in ["DESCRIPTOR", "records/MANIFEST"] {
            let path = root("input-custody-missing-bound");
            let (a, _) = approvals(&path);
            let input = VsrMembershipInput::from_approval(&a).unwrap();
            let owned = path.join("owned");
            let mut owner = Owner::create(&owned, decision, &a);
            let target = input_path(&owned, &input).join(missing);
            std::fs::remove_file(&target).unwrap();
            let before = journal_bytes(&owned, decision);
            for _ in 0..2 {
                assert!(owner.read(input.id().unwrap()).is_err());
                assert!(owner.begin(input.clone()).is_err());
                assert!(!target.exists());
                assert_eq!(journal_bytes(&owned, decision), before);
            }
            drop(owner);
            for _ in 0..2 {
                assert!(Owner::reopen(&owned, decision).is_err());
                assert!(!target.exists());
                assert_eq!(journal_bytes(&owned, decision), before);
            }
            std::fs::remove_dir_all(path).unwrap();
        }
    }
}
