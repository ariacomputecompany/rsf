use super::*;
use std::fs::{self, File};
use std::io::Write;
use std::path::PathBuf;

fn files(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    let mut result = BTreeMap::new();
    for entry in fs::read_dir(root).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            result.extend(files(&path));
        } else {
            result.insert(path.clone(), fs::read(path).unwrap());
        }
    }
    result
}

fn sync(path: &Path) -> RsfResult<()> {
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|error| corrupt(error.to_string()))
}

// Obtain the real storage factory format, then stop this input's publication
// immediately after its final manifest rename. Host visibility is not a model
// of power-loss durability; the test below checks the missing barrier and ACK
// ordering through the actual BEGIN implementation.
fn renamed_manifest(store: &MembershipDecisionStore, input: &VsrMembershipInput) {
    let scratch = store.root.join("factory-format");
    drop(SegmentStore::open(&scratch).unwrap());
    let manifest = fs::read(scratch.join("MANIFEST")).unwrap();
    let segment = fs::read_dir(scratch.join("segments"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.extension() == Some(std::ffi::OsStr::new("segment")))
        .unwrap();
    assert_eq!(fs::metadata(&segment).unwrap().len(), 0);
    let dir = store.directory(input.id().unwrap());
    fs::create_dir(&dir).unwrap();
    let mut descriptor = File::create(dir.join("DESCRIPTOR")).unwrap();
    descriptor.write_all(&input.encode().unwrap()).unwrap();
    descriptor.sync_all().unwrap();
    fs::create_dir_all(dir.join("records/segments")).unwrap();
    File::create(
        dir.join("records/segments")
            .join(segment.file_name().unwrap()),
    )
    .unwrap()
    .sync_all()
    .unwrap();
    sync(&dir.join("records/segments")).unwrap();
    let mut temporary = File::create(dir.join("records/MANIFEST.tmp")).unwrap();
    temporary.write_all(&manifest).unwrap();
    temporary.sync_all().unwrap();
    drop(temporary);
    fs::rename(
        dir.join("records/MANIFEST.tmp"),
        dir.join("records/MANIFEST"),
    )
    .unwrap();
    // Deliberately no records-directory sync after the rename.
}

#[test]
fn resumed_membership_begin_requires_publication_sync_before_ack_or_journal_binding() {
    for chosen_import in [false, true] {
        for complete in [false, true] {
            let path = root("input-publication-barrier");
            let (a, b) = approvals(&path);
            let mut sources = stores(&path, &a);
            let chosen = super::recovery::choose_b(&mut sources, &b);
            let input = VsrMembershipInput::from_approval(&b).unwrap();
            let id = input.id().unwrap();
            let records = input.flattened_records(&b).unwrap();
            let target = path.join("receiver");
            let mut store = MembershipDecisionStore::open(&target, NodeId(3), &a).unwrap();
            let own_round = round(9, 3);
            store.promise(NodeId(3), own_round).unwrap();
            renamed_manifest(&store, &input);
            assert!(
                !SegmentStore::initialization_pending(store.directory(id).join("records")).unwrap()
            );
            if complete {
                disk::retain_records(&store.root, &mut false, &input, &records).unwrap();
            }
            // Reopen the decision owner without resending a creating request.
            drop(store);
            let mut store =
                MembershipDecisionStore::open_existing(&target, PlacementGroupId(1), NodeId(3))
                    .unwrap();
            let progress = store.input_progress(id).unwrap();
            assert_eq!(
                progress,
                if complete {
                    VsrMembershipInputProgress::Complete {
                        input: input.clone(),
                    }
                } else {
                    VsrMembershipInputProgress::Partial {
                        input: input.clone(),
                        records: 0,
                    }
                }
            );
            let dir = store.directory(id);
            let expected = vec![
                dir.join("records/segments"),
                dir.join("records"),
                dir,
                store.root.join("inputs"),
                store.root.clone(),
            ];
            let before = files(&target);
            let state = store.state.clone();
            for failed_boundary in 0..expected.len() {
                let mut visited = Vec::new();
                let barrier = |root: &Path, id| {
                    disk::sync_input_publication_with(root, id, |path| {
                        visited.push(path.to_path_buf());
                        if visited.len() == failed_boundary + 1 {
                            return Err(corrupt("injected directory publication sync failure"));
                        }
                        sync(path)
                    })
                };
                let result = if chosen_import {
                    store
                        .begin_chosen_input_with_sync(
                            NodeId(2),
                            chosen.clone(),
                            input.clone(),
                            barrier,
                        )
                        .map(|_| ())
                } else {
                    store
                        .begin_input_with_sync(NodeId(3), own_round, input.clone(), barrier)
                        .map(|_| ())
                };
                assert!(
                    result.is_err(),
                    "BEGIN acknowledged without its publication barrier"
                );
                assert_eq!(visited, expected[..=failed_boundary]);
                assert_eq!(store.state, state, "failed sync bound journal custody");
                assert!(
                    store.import.is_none(),
                    "failed sync granted an import permission"
                );
                assert_eq!(files(&target), before, "failed sync rewrote retained bytes");
            }
            let mut visited = Vec::new();
            let barrier = |root: &Path, id| {
                disk::sync_input_publication_with(root, id, |path| {
                    assert_eq!(
                        files(&target),
                        before,
                        "journal changed before directory publication"
                    );
                    visited.push(path.to_path_buf());
                    sync(path)
                })
            };
            if chosen_import {
                store
                    .begin_chosen_input_with_sync(NodeId(2), chosen.clone(), input.clone(), barrier)
                    .unwrap();
                assert_eq!(store.state, state);
                assert!(store.import.is_some());
            } else {
                store
                    .begin_input_with_sync(NodeId(3), own_round, input.clone(), barrier)
                    .unwrap();
                assert_eq!(store.state.staged, Some((own_round, id)));
            }
            assert_eq!(visited, expected);
            // The ordinary public path must remain a valid idempotent retry.
            let mut at = if chosen_import {
                match store
                    .begin_chosen_input(NodeId(2), chosen.clone(), input.clone())
                    .unwrap()
                {
                    VsrMembershipImportReply::Retained { records, .. } => records,
                    value => panic!("unexpected BEGIN result: {value:?}"),
                }
            } else {
                store
                    .begin_input(NodeId(3), own_round, input.clone())
                    .unwrap()
            };
            while at < records.len() as u64 {
                let batch = VsrHistoryBatch::pack(at, &records[at as usize..]).unwrap();
                at += batch.records.len() as u64;
                if chosen_import {
                    store.append_chosen_input(NodeId(2), id, batch).unwrap();
                } else {
                    store.append_input(NodeId(3), own_round, id, batch).unwrap();
                }
            }
            store.learn_chosen(chosen.clone()).unwrap();
            drop(store);
            let store =
                MembershipDecisionStore::open_existing(&target, PlacementGroupId(1), NodeId(3))
                    .unwrap();
            assert_eq!(store.chosen(), Some(&chosen));
            let approval = store.approval(id).unwrap();
            assert_eq!(VsrMembershipInput::from_approval(&approval).unwrap(), input);
            assert_eq!(input.flattened_records(&approval).unwrap(), records);
            assert_eq!(store.decision_state().unwrap().promised, Some(own_round));
            drop(store);
            drop(sources);
            fs::remove_dir_all(path).unwrap();
        }
    }
}
