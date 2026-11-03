use super::*;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::Path;
pub(in crate::membership) fn create_input(
    root: &Path,
    input: &VsrMembershipInput,
) -> RsfResult<()> {
    let id = input.id()?;
    let dir = input_directory(root, id, true)?;
    sync(root)?;
    sync(&root.join("inputs"))?;
    regular_file(&dir.join("DESCRIPTOR"), true)?;
    records_route(&dir.join("records"), true)?;
    let bytes = input.encode()?;
    match fs::read(dir.join("DESCRIPTOR")) {
        Ok(existing) if existing == bytes => {}
        Ok(_) => return Err(corrupt("membership input descriptor identity collision")),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let temp = dir.join(".DESCRIPTOR.tmp");
            // The caller owns the input store's lock. Discard only an ordinary
            // unacknowledged temporary, then reject concurrent substitution.
            if regular_file(&temp, true)? {
                fs::remove_file(&temp).map_err(io_error)?;
            }
            let mut file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&temp)
                .map_err(io_error)?;
            file.write_all(&bytes)
                .and_then(|()| file.sync_all())
                .map_err(io_error)?;
            drop(file);
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
    let dir = input_directory(root, id, false)?;
    regular_file(&dir.join("DESCRIPTOR"), false)?;
    records_route(&dir.join("records"), false)?;
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
/// Retain a complete, already validated source using one writer. The caller
/// owns the transition/stage lock and checks acknowledged-input obligations
/// before invoking this copy path. Each appended record keeps Full sync.
pub(in crate::membership) fn retain_records(
    root: &Path,
    failed: &mut bool,
    input: &VsrMembershipInput,
    records: &[VsrHistoryRecord],
) -> RsfResult<()> {
    if records.len() as u64 != input.records()? {
        return Err(invalid_input("membership copy source is not complete"));
    }
    let id = input.id()?;
    let (stored, prefix) = read_records(root, id)?;
    if stored != *input || records.get(..prefix.len()) != Some(prefix.as_slice()) {
        return Err(corrupt(
            "membership copy changes the retained descriptor or prefix",
        ));
    }
    let mut ordinal = prefix.len();
    if ordinal == records.len() {
        return Ok(());
    }
    // read_records checked owned components and the complete existing prefix.
    // Hold one SegmentStore so its validated WAL cursor survives all suffix
    // appends; reopening for every bounded batch replays the growing prefix.
    let mut options = SegmentStoreOptions::default();
    options.log_options.sync_policy = rsf_storage::SyncPolicy::Full;
    let mut store = SegmentStore::open_with_options(directory(root, id).join("records"), options)?;
    while ordinal < records.len() {
        let batch = VsrHistoryBatch::pack(ordinal as u64, &records[ordinal..])?;
        let next = ordinal + batch.records.len();
        let bytes = batch.encode()?;
        if let Err(error) = store.append_next_wal(
            input.proposal().election.pg,
            input.proposal().election.target_view,
            WalRecordKind::ViewInstallation,
            &bytes,
        ) {
            *failed = true;
            return Err(error);
        }
        ordinal = next;
    }
    Ok(())
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

// Do not let recovery truncate a segment or create a lock through an aliased
// route. The owned root is checked too; callers supply its parent custody.
fn input_directory(root: &Path, id: [u8; 32], create: bool) -> RsfResult<std::path::PathBuf> {
    owned_directory(root, false)?;
    let base = root.join("inputs");
    owned_directory(&base, create)?;
    let dir = directory(root, id);
    owned_directory(&dir, create)?;
    Ok(dir)
}

fn owned_directory(path: &Path, create: bool) -> RsfResult<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() => Ok(()),
        Ok(_) => Err(corrupt("membership input route is not an owned directory")),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound && create => {
            fs::create_dir(path).map_err(io_error)
        }
        Err(error) => Err(io_error(error)),
    }
}

fn regular_file(path: &Path, optional: bool) -> RsfResult<bool> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_file() => Ok(true),
        Ok(_) => Err(corrupt("membership input custody is not a regular file")),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound && optional => Ok(false),
        Err(error) => Err(io_error(error)),
    }
}

fn records_route(root: &Path, initializing: bool) -> RsfResult<()> {
    match fs::symlink_metadata(root) {
        Ok(metadata) if metadata.is_dir() => {}
        Ok(_) => {
            return Err(corrupt(
                "membership input records are not an owned directory",
            ));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound && initializing => return Ok(()),
        Err(error) => return Err(io_error(error)),
    }
    regular_file(&root.join("MANIFEST"), initializing)?;
    for name in ["MANIFEST.lock", "MANIFEST.tmp"] {
        regular_file(&root.join(name), true)?;
    }
    for name in ["segments", "quarantine"] {
        let directory = root.join(name);
        match fs::symlink_metadata(&directory) {
            Ok(metadata) if metadata.is_dir() => {
                for entry in fs::read_dir(&directory).map_err(io_error)? {
                    regular_file(&entry.map_err(io_error)?.path(), false)?;
                }
            }
            Ok(_) => {
                return Err(corrupt(
                    "membership input segment route is not an owned directory",
                ));
            }
            Err(error)
                if error.kind() == std::io::ErrorKind::NotFound
                    && (initializing || name == "quarantine") => {}
            Err(error) => return Err(io_error(error)),
        }
    }
    Ok(())
}
