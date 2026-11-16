//! Warm handoff of this invocation's actually published writer, with bounded
//! immutable byte recovery. Existing authoritative targets never enter here.
use super::*;
use crate::learner::prefix::guard;
use rsf_storage::{SegmentReadError, SegmentReadLimits};

pub(super) struct Target {
    root: PathBuf,
    directory: guard::Identity,
    lock: guard::Exact,
    owner: guard::Exact,
}
impl Target {
    pub(super) fn capture(
        root: &Path,
        lock: &rsf_storage::ExclusiveFileLock,
        identity: &Identity,
    ) -> RsfResult<Self> {
        let target = Self {
            root: root.to_path_buf(),
            directory: guard::directory(root)?,
            lock: guard::owned_lock(&root.join(".vsr-writer.lock"), lock)?,
            owner: guard::Exact::capture(&root.join(identity::NAME), &identity.encode())?,
        };
        target.validate()?;
        Ok(target)
    }
    pub(super) fn validate(&self) -> RsfResult<()> {
        if guard::directory(&self.root)? != self.directory {
            return Err(corrupt("promotion target directory custody changed"));
        }
        self.lock.validate()?;
        self.owner.validate()
    }
}

pub(super) fn limits(records: &[VsrHistoryRecord]) -> RsfResult<SegmentReadLimits> {
    let count = u64::try_from(records.len())
        .map_err(|_| corrupt("promotion record count exceeds platform range"))?;
    if count == 0 {
        return Err(corrupt("promotion publication has no bootstrap"));
    }
    let mut bytes = 0u64;
    let mut largest = 0u64;
    for record in records {
        let length = u64::try_from(record.payload.len())
            .map_err(|_| corrupt("promotion payload length exceeds platform range"))?;
        let encoded = rsf_storage::wal_record_disk_bytes(length)?;
        bytes = bytes
            .checked_add(encoded)
            .ok_or_else(|| corrupt("promotion read budget overflow"))?;
        largest = largest.max(encoded);
    }
    Ok(SegmentReadLimits {
        max_manifest_bytes: rsf_storage::append_only_manifest_byte_bound(count)?,
        max_records: count,
        max_encoded_bytes: bytes,
        max_record_bytes: largest,
    })
}

pub(super) fn open(
    root: &Path,
    pg: PlacementGroupId,
    plan: [u8; 32],
    generation: publication::PublishedGeneration,
    limits: SegmentReadLimits,
    writer_lock: rsf_storage::ExclusiveFileLock,
) -> RsfResult<DurableVsrCell> {
    let publication::PublishedGeneration { store, custody } = generation;
    // Read actual pointer, seal and promotion owner after every public hook.
    // A logically valid alternate generation must not redirect the held writer.
    let active = publication::active(root, pg)?;
    if active.path != store.root() || active.plan != Some(plan) {
        return Err(corrupt(
            "published promotion differs from retained generation",
        ));
    }
    let recovered = store
        .recover_owned_immutable_bounded(&custody, limits, |_| Ok(()))
        .map_err(|error| match error {
            SegmentReadError::Failure(error) => error,
            SegmentReadError::LimitExceeded { .. } => {
                corrupt("published promotion exceeds its exact physical read budget")
            }
            SegmentReadError::CompactedPrefixUnsupported => {
                corrupt("published promotion has unexpected compacted history")
            }
        })?;
    let cell = DurableVsrCell::from_opened_store(
        OpenedVsrStore {
            root: root.to_path_buf(),
            active,
            store,
            recovery: recovered.recovery,
            options: SegmentStoreOptions::default(),
            writer_lock,
        },
        pg,
        || Err(corrupt("published promotion lost its original bootstrap")),
    )?;
    // This compares the original observer and actual retained writer/lock FDs.
    // Reopening a lookalike store cannot satisfy the original owner stamp.
    cell.store.validate_owned_custody(&custody)?;
    Ok(cell)
}
