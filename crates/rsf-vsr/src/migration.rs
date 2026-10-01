//! Persist the explicitly supplied original configuration without rewriting a
//! legacy WAL. The migration record binds that configuration to the prior tail.
use super::*;
use rsf_core::storage::WalRecord;

const MAGIC: &[u8; 5] = b"VSRG1";
const MAX_BOOTSTRAP: usize = 37 + 4096 * 8;

pub(super) fn is_record(payload: &[u8]) -> bool {
    payload.starts_with(MAGIC)
}

pub(super) fn recover_bootstrap(
    records: &[&WalRecord],
    pg: PlacementGroupId,
) -> RsfResult<Option<VsrCell>> {
    let mut migrated = None;
    for (position, record) in records.iter().enumerate() {
        if !is_record(&record.payload) {
            continue;
        }
        if position == 0
            || migrated.is_some()
            || record.kind != WalRecordKind::Membership
            || records[0].payload.starts_with(VSR_BOOTSTRAP_MAGIC)
        {
            return Err(corrupt(
                "VSR bootstrap migration requires one legacy prefix and one migration record",
            ));
        }
        let payload = &record.payload;
        if payload.len() < 49 {
            return Err(corrupt("truncated VSR bootstrap migration"));
        }
        let length = u32::from_le_bytes(payload[5..9].try_into().unwrap()) as usize;
        if length > MAX_BOOTSTRAP || payload.len() != 49 + length {
            return Err(corrupt("invalid VSR bootstrap migration length"));
        }
        let end = 9 + length;
        let previous = records[position - 1];
        if read_u64(payload, end)? != previous.index.0
            || payload[end + 8..] != previous.record_hash
            || record.previous_hash != previous.record_hash
        {
            return Err(corrupt(
                "VSR bootstrap migration does not bind its original WAL prefix",
            ));
        }
        let bootstrap = decode_bootstrap(&payload[9..end])?;
        if bootstrap.pg != pg {
            return Err(corrupt("VSR bootstrap migration placement group mismatch"));
        }
        migrated = Some(bootstrap);
    }
    Ok(migrated)
}

impl DurableVsrCell {
    /// Explicit operator migration. The supplied configuration must be the
    /// original bootstrap, not today's membership. Existing WAL is fully replayed
    /// against it before one synced, prefix-bound record is appended. Returns
    /// false for an identical already-persisted bootstrap; conflicting retries fail.
    pub fn migrate_legacy_bootstrap(
        path: impl AsRef<Path>,
        pg: PlacementGroupId,
        original_view: Epoch,
        original_primary: NodeId,
        original_voters: impl IntoIterator<Item = NodeId>,
    ) -> RsfResult<bool> {
        let voters = original_voters.into_iter().collect::<Vec<_>>();
        if original_view.0 == 0
            || voters.is_empty()
            || voters.len() > 4096
            || voters.iter().copied().collect::<BTreeSet<_>>().len() != voters.len()
        {
            return Err(invalid_input(
                "migration requires a positive original view and 1..=4096 unique original voters",
            ));
        }
        let original = VsrCell::new(pg, original_view, original_primary, voters.clone())
            .map_err(invalid_input)?;
        let encoded = encode_bootstrap(&original);
        let path = path.as_ref();
        if !path.is_dir() {
            return Err(invalid_input("migration requires an existing VSR store"));
        }
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(path.join(".vsr-writer.lock"))
            .map_err(|err| invalid_input(err.to_string()))?;
        let writer_lock = rsf_storage::ExclusiveFileLock::acquire(lock).map_err(|err| {
            RsfError::new(
                ErrorCode::QuorumUnavailable,
                format!("VSR migration store already has a writer: {err}"),
            )
        })?;
        let recovered = SegmentStore::recover(path)?.recovery;
        if !recovered.wal_records.iter().any(|record| record.pg == pg) {
            return Err(invalid_input(
                "migration requires an existing placement-group WAL",
            ));
        }
        let mut cell = Self::open_locked(
            path,
            pg,
            original_view,
            original_primary,
            voters,
            SegmentStoreOptions::default(),
            writer_lock,
        )?;
        if cell.initial_bootstrap != encoded {
            return Err(invalid_input(
                "migration configuration conflicts with the persisted original bootstrap",
            ));
        }
        if cell.bootstrap_persisted {
            return Ok(false);
        }
        let recovery = cell.store.recover_current()?.recovery;
        let tail = recovery
            .pg_tail(pg)
            .ok_or_else(|| corrupt("legacy migration prefix disappeared"))?;
        let mut payload = MAGIC.to_vec();
        payload.extend_from_slice(&(encoded.len() as u32).to_le_bytes());
        payload.extend_from_slice(&encoded);
        payload.extend_from_slice(&tail.index.0.to_le_bytes());
        payload.extend_from_slice(&tail.record_hash);
        cell.append(WalRecordKind::Membership, &payload)?;
        cell.bootstrap_persisted = true;
        Ok(true)
    }
}

#[cfg(test)]
mod tests;
