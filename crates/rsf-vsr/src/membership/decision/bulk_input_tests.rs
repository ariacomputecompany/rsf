use super::*;

#[test]
fn membership_bulk_input_copy_preserves_prefix_rejects_conflict_and_retries_without_new_wal() {
    let path = root("bulk-input-copy");
    let (_, approval) = approvals(&path);
    let input = VsrMembershipInput::from_approval(&approval).unwrap();
    let id = input.id().unwrap();
    let records = input.flattened_records(&approval).unwrap();
    assert!(records.len() > 2);
    let destination = path.join("destination");
    std::fs::create_dir(&destination).unwrap();
    disk::create_input(&destination, &input).unwrap();
    let mut failed = false;
    disk::append_records(
        &destination,
        &mut failed,
        &input,
        VsrHistoryBatch {
            ordinal: 0,
            records: records[..2].to_vec(),
        },
    )
    .unwrap();
    let mut conflict = records.clone();
    conflict[1].payload.push(1);
    assert!(disk::retain_records(&destination, &mut failed, &input, &conflict).is_err());
    assert_eq!(
        disk::read_records(&destination, id).unwrap().1,
        records[..2]
    );
    assert!(!failed);
    assert!(
        disk::retain_records(
            &destination,
            &mut failed,
            &input,
            &records[..records.len() - 1]
        )
        .is_err()
    );
    assert_eq!(
        disk::read_records(&destination, id).unwrap().1,
        records[..2]
    );
    disk::retain_records(&destination, &mut failed, &input, &records).unwrap();
    assert_eq!(disk::read_records(&destination, id).unwrap().1, records);
    let name = id.iter().map(|b| format!("{b:02x}")).collect::<String>();
    let record_root = destination.join("inputs").join(name).join("records");
    let before = SegmentStore::recover_existing(&record_root)
        .unwrap()
        .recovery
        .wal_records;
    disk::retain_records(&destination, &mut failed, &input, &records).unwrap();
    let after = SegmentStore::recover_existing(&record_root)
        .unwrap()
        .recovery
        .wal_records;
    assert_eq!(
        before, after,
        "exact bulk retry appended another physical WAL record"
    );
    input
        .validate_records(&disk::read_records(&destination, id).unwrap().1)
        .unwrap();
    std::fs::remove_dir_all(path).unwrap();
}
