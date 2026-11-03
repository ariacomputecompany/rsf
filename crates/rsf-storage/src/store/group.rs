//! Durable groups hold the ordinary root gate and flush before rotation/return.
use super::*;

/// One borrowed record in a single-PG append group. Physical indices and hashes
/// are derived by the owning store exactly as for `append_next_wal`.
#[derive(Clone, Copy, Debug)]
pub struct WalGroupRecord<'a> {
    pub epoch: rsf_core::Epoch,
    pub kind: WalRecordKind,
    pub payload: &'a [u8],
}

/// One incremental group item whose payload may be borrowed or owned. The
/// group borrows it only for the current WAL frame and never retains it.
#[derive(Clone, Copy, Debug)]
pub struct WalGroupEntry<P> {
    pub epoch: rsf_core::Epoch,
    pub kind: WalRecordKind,
    pub payload: P,
}

impl SegmentStore {
    /// Append a group with one required Data/Full flush per dirty segment.
    /// Rotation flushes the old segment before publishing the next manifest;
    /// the last dirty segment is flushed before success. Existing nonempty active
    /// bytes receive that barrier even for an empty-suffix retry: recovery
    /// alone does not resolve a previous failed data sync. No policy changes.
    ///
    /// Failure can leave a partial or complete prefix, never an acknowledged
    /// group. After mutation begins, any error fences this handle until reopen.
    /// This is not an all-or-nothing transaction. Memory is bounded by one
    /// record's encoding/hash workspace plus existing store metadata; records
    /// are consumed incrementally and their payloads are not retained.
    pub fn append_next_wal_group<'a>(
        &mut self,
        pg: PlacementGroupId,
        records: impl IntoIterator<Item = WalGroupRecord<'a>>,
    ) -> RsfResult<()> {
        self.try_append_next_wal_group(
            pg,
            records.into_iter().map(|record| {
                Ok(WalGroupEntry {
                    epoch: record.epoch,
                    kind: record.kind,
                    payload: record.payload,
                })
            }),
        )
    }

    /// Stream fallible borrowed or owned payloads through the same durable
    /// group. A producer error is not exhaustion: it returns no group success
    /// and fences this handle if mutation has begun. No later item is requested.
    /// Payload memory is released before the next item is requested. Rotation,
    /// empty-suffix renewal and Data/Full rules match `append_next_wal_group`.
    pub fn try_append_next_wal_group<P: AsRef<[u8]>>(
        &mut self,
        pg: PlacementGroupId,
        records: impl IntoIterator<Item = RsfResult<WalGroupEntry<P>>>,
    ) -> RsfResult<()> {
        self.ensure_writable()?;
        if self.options.log_options.sync_policy == crate::SyncPolicy::None {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                "durable WAL groups require Data or Full sync",
            ));
        }
        let gate = self.gate.clone();
        let _guard = lock_gate(&gate)?;
        self.refresh_layout()?;
        let mut tail = self.wal_tail_owned(pg)?;
        let mut cursor = None;
        let mut dirty = self
            .active
            .file
            .metadata()
            .map_err(|err| storage_error(StorageIoOperation::Recover, err))?
            .len()
            != 0;
        for record in records {
            let record = record?;
            let index = tail
                .map_or(Some(1), |tail| tail.index.0.checked_add(1))
                .ok_or_else(|| RsfError::new(ErrorCode::InvalidInput, "WAL index exhausted"))?;
            let input = WalAppend {
                pg,
                epoch: record.epoch,
                index: LogIndex(index),
                kind: record.kind,
                previous_hash: tail.map_or([0; 32], |tail| tail.record_hash),
                payload: record.payload.as_ref(),
            };
            let frame = wal::encode_at_tail(input, tail)?;
            let length = record_disk_len(frame.encoded.len())?;
            if cursor.is_none() {
                cursor = Some(
                    self.cursor
                        .take()
                        .expect("WAL cursor validated before group"),
                );
                // Set before any possible rotation/write. Early returns and
                // unwinding cannot expose an unflushed cursor or clear failure.
                self.write_failed = true;
            }
            if self.rotation_required(length) {
                if dirty {
                    self.active.sync_after_append()?;
                }
                self.rotate()?;
            }
            self.active.append_unsynced(&frame.encoded)?;
            dirty = true;
            tail = Some(WalTail {
                epoch: input.epoch,
                index: input.index,
                record_hash: frame.record_hash,
            });
        }
        if dirty {
            if cursor.is_none() {
                cursor = Some(
                    self.cursor
                        .take()
                        .expect("WAL cursor validated before group"),
                );
                self.write_failed = true;
            }
            self.active.sync_after_append()?;
        }
        if let Some(mut cursor) = cursor {
            cursor.stamp = segment_stamp(&self.root, &self.manifest)?;
            if let Some(tail) = tail {
                cursor.tails.insert(pg, tail);
            }
            self.cursor = Some(cursor);
            self.write_failed = false;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
