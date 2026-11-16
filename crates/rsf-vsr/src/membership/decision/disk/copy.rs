//! Complete local copies share the storage group without buffering all batches.
use super::*;

pub(in crate::membership) fn append_history_suffix(
    store: &mut SegmentStore,
    pg: PlacementGroupId,
    epoch: Epoch,
    records: &[VsrHistoryRecord],
    retained: usize,
) -> RsfResult<()> {
    if retained > records.len() {
        return Err(invalid_input(
            "membership copy retained count exceeds source",
        ));
    }
    #[cfg(test)]
    observation::before(observation::Boundary::Closed, retained, records.len())?;
    // The caller compared this exact recovered prefix before entering. A prior
    // failed group may have left visible bytes, including now-closed segments.
    let custody = store.capture_owned_custody()?;
    store.sync_owned_closed_segments(&custody)?;
    #[cfg(test)]
    observation::before(observation::Boundary::Active, retained, records.len())?;
    let mut ordinal = retained;
    let batches = std::iter::from_fn(|| {
        if ordinal == records.len() {
            return None;
        }
        Some((|| {
            let batch = VsrHistoryBatch::pack(ordinal as u64, &records[ordinal..])?;
            let next = ordinal + batch.records.len();
            let payload = batch.encode()?;
            ordinal = next;
            Ok(rsf_storage::WalGroupEntry {
                epoch,
                kind: WalRecordKind::ViewInstallation,
                payload,
            })
        })())
    });
    // Even an empty iterator renews retained active bytes before caller success.
    store.try_append_next_wal_group(pg, batches)?;
    #[cfg(test)]
    observation::before(observation::Boundary::Complete, retained, records.len())?;
    Ok(())
}

#[cfg(test)]
pub(in crate::membership) mod observation;
#[cfg(test)]
mod tests;
