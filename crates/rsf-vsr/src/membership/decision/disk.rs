mod copy;
mod initialization;
use super::*;
pub(in crate::membership) use copy::append_history_suffix;
#[cfg(test)]
pub(in crate::membership) use copy::observation;
pub(in crate::membership) use initialization::{
    initialization_pending, resume_input_initialization,
};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::Path;
pub(in crate::membership) fn create_input(
    root: &Path,
    input: &VsrMembershipInput,
) -> RsfResult<()> {
    create_input_store(root, input).map(drop)
}

// The returned Full-sync writer has crossed the same publication boundary as
// create_input. A stage can keep it for the ensuing copy instead of reopening.
fn create_input_store(root: &Path, input: &VsrMembershipInput) -> RsfResult<SegmentStore> {
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
    let store = SegmentStore::open_with_options(dir.join("records"), options)?;
    sync_input_publication(root, id)?;
    Ok(store)
}
/// Complete directory publication before input custody can be acknowledged.
/// Seeing a final descriptor or manifest after restart does not prove that its
/// rename's directory sync completed. The caller has validated the retained
/// descriptor/history and holds the input parent's exclusive namespace custody.
pub(in crate::membership) fn sync_input_publication(root: &Path, id: [u8; 32]) -> RsfResult<()> {
    sync_input_publication_with(root, id, sync)
}
pub(super) fn sync_input_publication_with(
    root: &Path,
    id: [u8; 32],
    mut sync_directory: impl FnMut(&Path) -> RsfResult<()>,
) -> RsfResult<()> {
    let dir = input_directory(root, id, false)?;
    regular_file(&dir.join("DESCRIPTOR"), false)?;
    records_route(&dir.join("records"), false)?;
    // Flush children before the directory entries that retain them. This also
    // covers a retry after publishing the descriptor or input directory itself.
    for path in [
        dir.join("records/segments"),
        dir.join("records"),
        dir,
        root.join("inputs"),
        root.to_path_buf(),
    ] {
        sync_directory(&path)?;
    }
    Ok(())
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
/// before invoking this copy path. Full-sync groups complete before success.
pub(in crate::membership) fn retain_records(
    root: &Path,
    failed: &mut bool,
    input: &VsrMembershipInput,
    records: &[VsrHistoryRecord],
) -> RsfResult<()> {
    let id = validate_copy_source(*failed, input, records)?;
    retain_records_with_store(root, failed, id, input, records, None)
}

/// Create/resume an unsealed stage input and retain its complete source using
/// the same published writer. Bound decision input must use the stricter caller
/// checks before any factory/copy operation.
pub(in crate::membership) fn create_and_retain_records(
    root: &Path,
    failed: &mut bool,
    input: &VsrMembershipInput,
    records: &[VsrHistoryRecord],
) -> RsfResult<()> {
    let id = validate_copy_source(*failed, input, records)?;
    let store = copy_with_failure(failed, || create_input_store(root, input))?;
    retain_records_with_store(root, failed, id, input, records, Some(store))
}

fn copy_healthy(failed: bool) -> RsfResult<()> {
    if failed {
        return Err(corrupt(
            "membership copy durability failed; reopen before further use",
        ));
    }
    Ok(())
}

fn validate_copy_source(
    failed: bool,
    input: &VsrMembershipInput,
    records: &[VsrHistoryRecord],
) -> RsfResult<[u8; 32]> {
    copy_healthy(failed)?;
    if records.len() as u64 != input.records()? {
        return Err(invalid_input("membership copy source is not complete"));
    }
    input.id()
}

// Each physical scope arms before factory, recovery, renewal or append. Only
// its complete success clears the flag; errors and unwinding stay fenced.
// A pure caller mismatch between successful scopes does not poison custody.
fn copy_with_failure<T>(failed: &mut bool, copy: impl FnOnce() -> RsfResult<T>) -> RsfResult<T> {
    copy_healthy(*failed)?;
    *failed = true;
    let result = copy();
    if result.is_ok() {
        *failed = false;
    }
    result
}

fn retain_records_with_store(
    root: &Path,
    failed: &mut bool,
    id: [u8; 32],
    input: &VsrMembershipInput,
    records: &[VsrHistoryRecord],
    store: Option<SegmentStore>,
) -> RsfResult<()> {
    // Existing recovery can repair an unsealed tail or create lock sidecars;
    // it is a physical operation even when the caller will reject its input.
    let (stored, prefix) = copy_with_failure(failed, || read_records(root, id))?;
    if stored != *input || records.get(..prefix.len()) != Some(prefix.as_slice()) {
        return Err(corrupt(
            "membership copy changes the retained descriptor or prefix",
        ));
    }
    // read_records checked owned components and the complete existing prefix.
    // Hold one SegmentStore so its validated WAL cursor survives all suffix
    // appends; reopening for every bounded batch replays the growing prefix.
    copy_with_failure(failed, || {
        let mut options = SegmentStoreOptions::default();
        options.log_options.sync_policy = rsf_storage::SyncPolicy::Full;
        let mut store = match store {
            Some(store) => store,
            None => SegmentStore::open_with_options(directory(root, id).join("records"), options)?,
        };
        append_history_suffix(
            &mut store,
            input.proposal().election.pg,
            input.proposal().election.target_view,
            records,
            prefix.len(),
        )
    })
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

pub(in crate::membership) fn records_route(root: &Path, initializing: bool) -> RsfResult<()> {
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
