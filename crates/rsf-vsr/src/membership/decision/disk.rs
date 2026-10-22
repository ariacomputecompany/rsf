use super::*;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::Path;
pub(in crate::membership) fn create_input(
    root: &Path,
    input: &VsrMembershipInput,
) -> RsfResult<()> {
    let id = input.id()?;
    let base = root.join("inputs");
    fs::create_dir_all(&base).map_err(io_error)?;
    sync(root)?;
    let dir = directory(root, id);
    fs::create_dir_all(&dir).map_err(io_error)?;
    sync(&base)?;
    let bytes = input.encode()?;
    match fs::read(dir.join("DESCRIPTOR")) {
        Ok(existing) if existing == bytes => {}
        Ok(_) => return Err(corrupt("membership input descriptor identity collision")),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let temp = dir.join(".DESCRIPTOR.tmp");
            let mut file = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&temp)
                .map_err(io_error)?;
            file.write_all(&bytes)
                .and_then(|()| file.sync_all())
                .map_err(io_error)?;
            fs::rename(temp, dir.join("DESCRIPTOR")).map_err(io_error)?;
            sync(&dir)?;
        }
        Err(error) => return Err(io_error(error)),
    }
    let mut options = SegmentStoreOptions::default();
    options.log_options.sync_policy = rsf_storage::SyncPolicy::Full;
    drop(SegmentStore::open_with_options(
        dir.join("records"),
        options,
    )?);
    sync(&dir)
}
pub(in crate::membership) fn read_records(
    root: &Path,
    id: [u8; 32],
) -> RsfResult<(VsrMembershipInput, Vec<VsrHistoryRecord>)> {
    let dir = directory(root, id);
    if !dir.join("records/MANIFEST").is_file() {
        return Err(corrupt("membership input record store is missing"));
    }
    let input = VsrMembershipInput::decode(&fs::read(dir.join("DESCRIPTOR")).map_err(io_error)?)?;
    if input.id()? != id {
        return Err(corrupt("membership input identity or authority mismatch"));
    }
    let recovered = SegmentStore::recover_existing(dir.join("records"))?;
    let mut records = Vec::new();
    for (position, record) in recovered.recovery.wal_records.iter().enumerate() {
        if record.index != LogIndex(position as u64 + 1)
            || record.pg != input.proposal().election.pg
            || record.epoch != input.proposal().election.target_view
            || record.kind != WalRecordKind::ViewInstallation
        {
            return Err(corrupt("membership input WAL envelope mismatch"));
        }
        let batch = VsrHistoryBatch::decode(&record.payload)?;
        if batch.ordinal != records.len() as u64 {
            return Err(corrupt("membership input batch reorders history"));
        }
        records.extend(batch.records);
        if records.len() as u64 > input.records()? {
            return Err(corrupt("membership input exceeds descriptor"));
        }
    }
    Ok((input, records))
}
pub(in crate::membership) fn append_records(
    root: &Path,
    failed: &mut bool,
    input: &VsrMembershipInput,
    batch: VsrHistoryBatch,
) -> RsfResult<u64> {
    batch.encode()?;
    let id = input.id()?;
    let (_, records) = read_records(root, id)?;
    let count = records.len() as u64;
    let end = batch
        .ordinal
        .checked_add(batch.records.len() as u64)
        .ok_or_else(|| invalid_input("membership input batch overflow"))?;
    if batch.ordinal > count || end > input.records()? {
        return Err(invalid_input(
            "membership input batch is outside durable prefix",
        ));
    }
    let overlap = count
        .saturating_sub(batch.ordinal)
        .min(batch.records.len() as u64) as usize;
    if overlap > 0
        && records.get(batch.ordinal as usize..batch.ordinal as usize + overlap)
            != Some(&batch.records[..overlap])
    {
        return Err(corrupt("membership input retry changes retained records"));
    }
    if end <= count {
        return Ok(count);
    }
    let appended = VsrHistoryBatch {
        ordinal: count,
        records: batch.records[overlap..].to_vec(),
    }
    .encode()?;
    let mut options = SegmentStoreOptions::default();
    options.log_options.sync_policy = rsf_storage::SyncPolicy::Full;
    let mut store = SegmentStore::open_with_options(directory(root, id).join("records"), options)?;
    if let Err(error) = store.append_next_wal(
        input.proposal().election.pg,
        input.proposal().election.target_view,
        WalRecordKind::ViewInstallation,
        &appended,
    ) {
        *failed = true;
        return Err(error);
    }
    Ok(end)
}
fn io_error(error: std::io::Error) -> RsfError {
    corrupt(format!("membership input storage: {error}"))
}
fn sync(path: &Path) -> RsfResult<()> {
    File::open(path)
        .and_then(|f| f.sync_all())
        .map_err(io_error)
}
fn directory(root: &Path, id: [u8; 32]) -> std::path::PathBuf {
    root.join("inputs")
        .join(id.iter().map(|b| format!("{b:02x}")).collect::<String>())
}
