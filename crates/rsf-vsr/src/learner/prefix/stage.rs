//! Shared actual replay/custody for bounded learner staging and strict reopening.
use super::*;
use rsf_storage::{SegmentReadError, SegmentScanControl};

pub(in crate::learner) fn storage_error(error: SegmentReadError) -> RsfError {
    match error {
        SegmentReadError::LimitExceeded { .. } => {
            invalid_input("learner prefix physical budget exceeded")
        }
        SegmentReadError::CompactedPrefixUnsupported => {
            invalid_input("bounded learner prefix requires uncompacted history")
        }
        SegmentReadError::Failure(error) => error,
    }
}
pub(in crate::learner) fn renew(
    store: &mut SegmentStore,
    custody: &rsf_storage::SegmentStoreCustody,
) -> RsfResult<()> {
    store.validate_owned_custody(custody)?;
    store.sync_owned_closed_segments(custody)?;
    #[cfg(test)]
    observation::at(observation::Boundary::AfterClosedRenewal, store.root());
    store.sync_owned_active_segment(custody)?;
    store.validate_owned_custody(custody)
}
pub(in crate::learner) fn validate_record(
    pg: PlacementGroupId,
    ordinal: u64,
    record: &VsrHistoryRecord,
) -> RsfResult<()> {
    record.encode()?;
    if ordinal == 0 {
        let bootstrap = decode_bootstrap(&record.payload)?;
        if record.kind != WalRecordKind::Membership
            || bootstrap.pg != pg
            || bootstrap.view != record.epoch
        {
            return Err(corrupt(
                "bounded learner history must start with original bootstrap",
            ));
        }
    } else if record.payload.starts_with(VSR_BOOTSTRAP_MAGIC)
        || migration::is_record(&record.payload)
    {
        return Err(corrupt(
            "bounded learner history contains duplicate bootstrap authority",
        ));
    }
    Ok(())
}
pub(in crate::learner) fn published_bytes(
    published: &Published,
    limits: VsrPrefixLimits,
) -> RsfResult<u64> {
    let mut bytes = 0u64;
    let mut records = 0u64;
    let result = published.cell.store.scan_owned_prefix(
        &published.cell.store_custody,
        limits.storage()?,
        limits.admission()?,
        |record| {
            records = records
                .checked_add(1)
                .ok_or_else(|| corrupt("prefix count overflow"))?;
            bytes = bytes
                .checked_add(18 + record.payload.len() as u64)
                .ok_or_else(|| corrupt("prefix bytes overflow"))?;
            limits.check(records, bytes)?;
            Ok(SegmentScanControl::Continue)
        },
    );
    if let Err(error) = result {
        if let SegmentReadError::Failure(failure) = &error {
            export::fence(&published.cell, failure)?;
        }
        return Err(storage_error(error));
    }
    if records != published.descriptor.records {
        return Err(corrupt("published prefix count changed"));
    }
    Ok(bytes)
}
pub(super) fn stage_bytes(stage: &Stage, limits: VsrPrefixLimits) -> RsfResult<u64> {
    let custody = stage.store.capture_owned_custody()?;
    let mut count = 0u64;
    let mut bytes = 0u64;
    let mut digest = Digest::blake3_bytes(b"RSF history chain v1");
    let mut previous_hash = [0; 32];
    let result =
        stage
            .store
            .scan_owned_prefix(&custody, limits.storage()?, limits.admission()?, |raw| {
                if raw.pg != stage.descriptor.probe.pg || raw.index.0 != count + 1 {
                    return Err(corrupt(
                        "bounded warm stage has foreign or noncontiguous records",
                    ));
                }
                let record = VsrHistoryRecord {
                    epoch: raw.epoch,
                    kind: raw.kind,
                    payload: raw.payload.clone(),
                };
                validate_record(raw.pg, count, &record)?;
                count = count
                    .checked_add(1)
                    .ok_or_else(|| corrupt("bounded stage count overflow"))?;
                bytes = bytes
                    .checked_add(18 + raw.payload.len() as u64)
                    .ok_or_else(|| corrupt("bounded stage size overflow"))?;
                limits.check(count, bytes)?;
                digest = transfer::extend_digest(digest, &record)?;
                previous_hash = raw.record_hash;
                Ok(SegmentScanControl::Continue)
            });
    stage.store.validate_owned_custody(&custody)?;
    result.map_err(storage_error)?;
    if count != stage.count || digest != stage.digest || previous_hash != stage.previous_hash {
        return Err(corrupt("bounded warm stage differs from retained cursor"));
    }
    Ok(bytes)
}
pub(in crate::learner) fn check_overlap(
    store: &SegmentStore,
    descriptor: &VsrHistoryDescriptor,
    custody: &rsf_storage::SegmentStoreCustody,
    limits: VsrPrefixLimits,
    batch: &VsrHistoryBatch,
    overlap: usize,
) -> RsfResult<bool> {
    if overlap == 0 {
        return Ok(true);
    }
    let mut ordinal = 0u64;
    let mut matched = 0usize;
    let mut changed = false;
    store
        .scan_owned_prefix(custody, limits.storage()?, limits.admission()?, |record| {
            if ordinal >= batch.ordinal {
                let expected = &batch.records[matched];
                changed |= record.pg != descriptor.probe.pg
                    || record.index.0 != ordinal + 1
                    || record.epoch != expected.epoch
                    || record.kind != expected.kind
                    || record.payload != expected.payload;
                matched += 1;
            }
            ordinal += 1;
            Ok(if matched == overlap {
                SegmentScanControl::Stop
            } else {
                SegmentScanControl::Continue
            })
        })
        .map_err(storage_error)?;
    if matched != overlap {
        return Err(corrupt("bounded learner retained overlap disappeared"));
    }
    Ok(!changed)
}
pub(in crate::learner) fn preserves(previous: &Published, candidate: &Published) -> RsfResult<()> {
    let old = &previous.cell;
    let new = &candidate.cell;
    if old.initial_bootstrap != new.initial_bootstrap
        || new.view() < old.view()
        || !new.history.compatible(&old.history)
        || !installation::preserves_committed(&new.cell, &old.cell)
        || old
            .cell
            .client_table
            .iter()
            .any(|(key, value)| new.cell.client_table.get(key) != Some(value))
    {
        return Err(corrupt(
            "bounded learner prefix conflicts with published commits, configurations, receipts or bootstrap",
        ));
    }
    Ok(())
}

#[derive(Debug)]
pub(in crate::learner) struct OpenedPrefix {
    pub stage: Stage,
    pub recovery: rsf_storage::Recovery,
    pub bytes: u64,
    pub custody: rsf_storage::SegmentStoreCustody,
    pub flat: guard::Flat,
}

impl LearnerHistoryStore {
    pub(in crate::learner) fn open_prefix_stage(
        &self,
        descriptor: VsrHistoryDescriptor,
        limits: VsrPrefixLimits,
        initialize: bool,
    ) -> RsfResult<OpenedPrefix> {
        open(
            &self.directory(&descriptor.id()?).join("cell"),
            self.pg,
            descriptor,
            limits,
            initialize,
        )
    }

    pub(in crate::learner) fn complete_prefix(
        &self,
        opened: OpenedPrefix,
        create_lock: bool,
    ) -> RsfResult<Published> {
        let OpenedPrefix {
            stage,
            recovery,
            custody,
            flat,
            ..
        } = opened;
        let replayed = DurableVsrCell::replay_store(&recovery, self.pg, || {
            Err(corrupt("bounded learner requires persisted bootstrap"))
        })?;
        flat.validate()?;
        stage.store.validate_owned_custody(&custody)?;
        validate_replayed(&stage.descriptor, &replayed)?;
        complete_replayed(
            self.pg,
            stage,
            recovery,
            replayed,
            create_lock,
            &custody,
            &flat,
        )
    }
}

pub(in crate::learner) fn validate_replayed(
    descriptor: &VsrHistoryDescriptor,
    replayed: &ReplayedVsrStore,
) -> RsfResult<()> {
    if !replayed.bootstrap_persisted
        || replayed.recovered.2.is_some()
        || replayed.recovered.3.is_some()
        || installation::probe_for_cell(&replayed.recovered.0)? != descriptor.probe
    {
        return Err(corrupt(
            "bounded learner candidate is not its declared normal prefix",
        ));
    }
    Ok(())
}

pub(in crate::learner) fn open(
    directory: &Path,
    pg: PlacementGroupId,
    descriptor: VsrHistoryDescriptor,
    limits: VsrPrefixLimits,
    initialize: bool,
) -> RsfResult<OpenedPrefix> {
    // Policy is checked before even provisional empty metadata may be completed.
    let storage_limits = limits.storage()?;
    let admission = limits.admission()?;
    let candidate = custody::CandidateCustody::capture(
        directory
            .parent()
            .ok_or_else(|| corrupt("learner cell lacks candidate directory"))?,
        &descriptor,
    )?;
    if initialize {
        // Caller-owned provisional empty transfer, including a cold ACK0.
        // Caller validates the exact current authority/selection before this path; it permits
        // only empty metadata/sidecar completion; no WAL progress or
        // publication authority is inferred. Nonempty data stays strict.
        SegmentStore::complete_unbound_empty_initialization(directory)?;
    }
    candidate.validate()?;
    let flat = guard::Flat::capture(directory)?;
    let mut options = SegmentStoreOptions::default();
    options.log_options.sync_policy = rsf_storage::SyncPolicy::Full;
    let (store, recovered) = SegmentStore::open_existing_recovered_bounded(
        directory,
        options,
        storage_limits,
        admission,
    )
    .map_err(storage_error)?;
    let custody = store.capture_owned_custody()?;
    flat.validate()?;
    let recovery = recovered.recovery;
    if recovery.records.len() != recovery.wal_records.len()
        || !recovery.maintenance_records.is_empty()
    {
        return Err(corrupt(
            "bounded learner contains non-history physical records",
        ));
    }
    let mut digest = Digest::blake3_bytes(b"RSF history chain v1");
    let mut bytes = 0u64;
    let mut previous_hash = [0; 32];
    let count = recovery.wal_records.len() as u64;
    if count > descriptor.records {
        return Err(corrupt("bounded learner exceeds declared count"));
    }
    for (position, raw) in recovery.wal_records.iter().enumerate() {
        if raw.pg != pg || raw.index.0 != position as u64 + 1 {
            return Err(corrupt(
                "bounded learner contains foreign or noncontiguous records",
            ));
        }
        bytes = bytes
            .checked_add(18 + raw.payload.len() as u64)
            .ok_or_else(|| corrupt("prefix bytes overflow"))?;
        limits.check(position as u64 + 1, bytes)?;
        let record = VsrHistoryRecord {
            epoch: raw.epoch,
            kind: raw.kind,
            payload: raw.payload.clone(),
        };
        validate_record(pg, position as u64, &record)?;
        digest = transfer::extend_digest(digest, &record)?;
        previous_hash = raw.record_hash;
    }
    flat.validate()?;
    store.validate_owned_custody(&custody)?;
    candidate.validate()?;
    Ok(OpenedPrefix {
        stage: Stage {
            custody: candidate,
            descriptor,
            store,
            count,
            digest,
            previous_hash,
        },
        recovery,
        bytes,
        custody,
        flat,
    })
}
pub(in crate::learner) fn complete_replayed(
    pg: PlacementGroupId,
    stage: Stage,
    recovery: rsf_storage::Recovery,
    replayed: ReplayedVsrStore,
    create_lock: bool,
    custody: &rsf_storage::SegmentStoreCustody,
    flat: &guard::Flat,
) -> RsfResult<Published> {
    if stage.count != stage.descriptor.records || stage.digest != stage.descriptor.digest {
        return Err(corrupt("bounded learner history count or digest mismatch"));
    }
    let Stage {
        descriptor,
        store,
        custody: mut candidate,
        ..
    } = stage;
    let path = store.root().to_path_buf();
    flat.validate()?;
    store.validate_owned_custody(custody)?;
    let writer_lock = candidate.acquire_writer(create_lock)?;
    let lock_guard = guard::owned_lock(&path.join(".vsr-writer.lock"), &writer_lock)?;
    flat.validate()?;
    store.validate_owned_custody(custody)?;
    #[cfg(test)]
    observation::at(observation::Boundary::BeforeHandoff, &path);
    let active = installation::publication::flat_owned(&path);
    let mut options = SegmentStoreOptions::default();
    options.log_options.sync_policy = rsf_storage::SyncPolicy::Full;
    let cell = DurableVsrCell::from_replayed_store_with(
        OpenedVsrStore {
            root: path,
            active,
            store,
            recovery,
            options,
            writer_lock,
        },
        pg,
        replayed,
        |_, _, store| {
            candidate.validate()?;
            flat.validate()?;
            lock_guard.validate()?;
            store.validate_owned_custody(custody)
        },
    )?;
    cell.store.validate_owned_custody(custody)?;
    flat.validate_cell(&cell)?;
    lock_guard.validate()?;
    guard::owned_lock(&cell.root.join(".vsr-writer.lock"), &cell._writer_lock)?;
    if !cell.bootstrap_persisted || cell.primary_probe()? != descriptor.probe {
        return Err(corrupt(
            "bounded learner replay differs from descriptor normal view",
        ));
    }
    flat.validate_cell(&cell)?;
    cell.store.validate_owned_custody(custody)?;
    candidate.validate_cell(&cell)?;
    Ok(Published {
        custody: candidate,
        bounded_flat: Some(flat.clone()),
        id: descriptor.id()?,
        descriptor,
        cell,
    })
}
