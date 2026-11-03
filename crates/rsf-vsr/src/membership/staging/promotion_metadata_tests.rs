use super::*;
use std::io::Read;

#[test]
fn actual_promoted_pointer_and_owner_fixed_metadata_reject_damage_and_preserve_cold_replay() {
    let path = root("promotion-fixed-metadata");
    let ready = ready(&path);
    let target = path.join("voter-3");
    let cell = ready
        .f
        .learner
        .promote_membership(&target, &ready.stages[2], &ready.quorum)
        .unwrap();
    assert_prefix(&cell);
    let expected = cell.committed_prefix().unwrap();
    let active = installation::publication::active(&target, PlacementGroupId(1)).unwrap();
    assert!(active.plan.is_some());
    assert_eq!(fs::metadata(target.join("VSR_ACTIVE")).unwrap().len(), 140);
    assert_eq!(
        fs::metadata(target.join("VSR_PROMOTION")).unwrap().len(),
        118
    );
    drop(cell);
    for (file, maximum) in [
        (target.join("VSR_ACTIVE"), 140),
        (target.join("VSR_PROMOTION"), 118),
    ] {
        let valid = fs::read(&file).unwrap();
        for fault in ["sparse", "trailing", "truncated", "checksum"] {
            fs::write(&file, &valid).unwrap();
            match fault {
                "sparse" => fs::OpenOptions::new()
                    .write(true)
                    .open(&file)
                    .unwrap()
                    .set_len(16 * 1024 * 1024)
                    .unwrap(),
                "trailing" => {
                    let mut bytes = valid.clone();
                    bytes.push(0);
                    fs::write(&file, bytes).unwrap();
                }
                "truncated" => fs::write(&file, &valid[..valid.len() - 1]).unwrap(),
                "checksum" => {
                    let mut bytes = valid.clone();
                    *bytes.last_mut().unwrap() ^= 1;
                    fs::write(&file, bytes).unwrap();
                }
                _ => unreachable!(),
            }
            let mut before = Vec::new();
            fs::File::open(&file)
                .unwrap()
                .take(maximum as u64 + 1)
                .read_to_end(&mut before)
                .unwrap();
            let length = fs::metadata(&file).unwrap().len();
            assert!(
                installation::publication::active(&target, PlacementGroupId(1)).is_err(),
                "{} {fault}",
                file.display()
            );
            assert!(DurableVsrCell::open_existing(&target, PlacementGroupId(1)).is_err());
            assert_eq!(fs::metadata(&file).unwrap().len(), length);
            let mut after = Vec::new();
            fs::File::open(&file)
                .unwrap()
                .take(maximum as u64 + 1)
                .read_to_end(&mut after)
                .unwrap();
            assert_eq!(after, before);
        }
        fs::write(&file, valid).unwrap();
    }
    let cold = DurableVsrCell::open_existing(&target, PlacementGroupId(1)).unwrap();
    assert_prefix(&cold);
    assert_eq!(cold.committed_prefix().unwrap(), expected);
    drop(cold);
    drop(ready);
    fs::remove_dir_all(path).unwrap();
}
