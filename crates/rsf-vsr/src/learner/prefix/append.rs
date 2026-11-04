//! One bounded append implementation for published and initial learner owners.
use super::*;
pub(in crate::learner) fn append(
    pg: PlacementGroupId,
    stage: &mut Stage,
    context: &mut PrefixContext,
    failed: &AtomicBool,
    batch: &VsrHistoryBatch,
) -> RsfResult<u64> {
    // Pack/codec limits are independent of the finite whole-history policy.
    let batch_bytes = batch.records.iter().try_fold(0u64, |total, record| {
        total
            .checked_add(18)
            .and_then(|sum| sum.checked_add(record.payload.len() as u64))
            .ok_or_else(|| invalid_input("bounded batch size overflow"))
    })?;
    context
        .limits
        .check(batch.records.len() as u64, batch_bytes)?;
    batch.encode()?;
    let end = batch
        .ordinal
        .checked_add(batch.records.len() as u64)
        .ok_or_else(|| invalid_input("bounded prefix batch overflow"))?;
    if batch.ordinal > stage.count || end > stage.descriptor.records {
        return Err(invalid_input(
            "bounded prefix batch is outside retained history",
        ));
    }
    for (offset, record) in batch.records.iter().enumerate() {
        stage::validate_record(pg, batch.ordinal + offset as u64, record)?;
    }
    let overlap = (stage.count - batch.ordinal).min(batch.records.len() as u64) as usize;
    let unchanged = stage::check_overlap(stage, context, batch, overlap).inspect_err(|_| {
        failed.store(true, Ordering::Relaxed);
    })?;
    if !unchanged {
        return Err(invalid_input(
            "bounded learner retry changes retained records",
        ));
    }
    let mut bytes = context.bytes;
    let mut digest = stage.digest;
    for record in &batch.records[overlap..] {
        bytes = bytes
            .checked_add(18 + record.payload.len() as u64)
            .ok_or_else(|| invalid_input("bounded prefix byte count overflow"))?;
        context.limits.check(stage.count.max(end), bytes)?;
        digest = transfer::extend_digest(digest, record)?;
    }
    context.limits.check(stage.count.max(end), bytes)?;
    let result = (|| {
        context.flat.validate()?;
        context.descriptor.validate()?;
        let original = context
            .custody
            .as_ref()
            .ok_or_else(|| corrupt("bounded append lacks original stage custody"))?;
        // The empty suffix is intentional: duplicate ACKs also renew a
        // recovered prefix's data barrier without changing its bytes.
        #[cfg(test)]
        observation::at(observation::Boundary::BeforeAppend, stage.store.root());
        let appended = stage.store.try_append_owned_wal_group(
            original,
            pg,
            batch.records[overlap..].iter().map(|record| {
                Ok(rsf_storage::WalGroupEntry {
                    epoch: record.epoch,
                    kind: record.kind,
                    payload: &record.payload,
                })
            }),
        )?;
        let tail = appended
            .tail()
            .ok_or_else(|| corrupt("bounded stage lost its WAL tail"))?;
        let custody = appended.into_custody();
        stage.store.validate_owned_custody(&custody)?;
        context.flat.validate()?;
        context.descriptor.validate()?;
        stage.previous_hash = tail.record_hash;
        stage.count = stage.count.max(end);
        stage.digest = digest;
        context.bytes = bytes;
        context.custody = Some(custody);
        Ok(stage.count)
    })();
    if result.is_err() {
        failed.store(true, Ordering::Relaxed);
    }
    result
}
