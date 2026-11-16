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
        self.append_group(pg, records, Mode::Coordinated)
            .map(|_| ())
    }

    /// Append only against this exact owned physical state. Unlike the generic
    /// multi-handle entry, this never refreshes or recovers changed storage.
    /// The returned tail and custody come from the same successful transition.
    pub fn try_append_owned_wal_group<P: AsRef<[u8]>>(
        &mut self,
        expected: &SegmentStoreCustody,
        pg: PlacementGroupId,
        records: impl IntoIterator<Item = RsfResult<WalGroupEntry<P>>>,
    ) -> RsfResult<OwnedWalGroup> {
        self.append_group(
            pg,
            records,
            Mode::Owned(custody::OwnedAppend::new(expected)),
        )?
        .ok_or_else(|| {
            RsfError::new(
                ErrorCode::CorruptionDetected,
                "owned group did not return custody",
            )
        })
    }

    fn append_group<P: AsRef<[u8]>>(
        &mut self,
        pg: PlacementGroupId,
        records: impl IntoIterator<Item = RsfResult<WalGroupEntry<P>>>,
        mut mode: Mode,
    ) -> RsfResult<Option<OwnedWalGroup>> {
        self.ensure_writable()?;
        if self.options.log_options.sync_policy == crate::SyncPolicy::None {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                "durable WAL groups require Data or Full sync",
            ));
        }
        let gate = self.gate.clone();
        let _guard = lock_gate(&gate)?;
        let mut tail = match &mode {
            Mode::Coordinated => {
                self.refresh_layout()?;
                self.wal_tail_owned(pg)?
            }
            Mode::Owned(owner) => {
                // Retain failure across arbitrary producer code and unwind.
                // A clean early error is cleared only after original custody
                // proves that no owned mutation or renewal has started.
                self.write_failed = true;
                if let Err(error) = owner.validate(self, None) {
                    self.cursor = None;
                    return Err(error);
                }
                self.cursor
                    .as_ref()
                    .expect("owned cursor validated")
                    .tails
                    .get(&pg)
                    .copied()
            }
        };
        let mut cursor: Option<WalCursor> = None;
        let mut changed = false;
        let result: Result<(), GroupFailure> = (|| {
            let mut dirty = match &mode {
                Mode::Coordinated => {
                    self.active
                        .file
                        .metadata()
                        .map_err(|error| storage_error(StorageIoOperation::Recover, error))?
                        .len()
                        != 0
                }
                Mode::Owned(owner) => owner.active_bytes(self)? != 0,
            };
            let mut records = records.into_iter();
            loop {
                mode.validate(self, cursor.as_ref())?;
                let next = records.next();
                // Check after every callback, including Err/None, before its
                // semantic outcome can bypass an observed physical fault.
                mode.validate(self, cursor.as_ref())?;
                let Some(record) = next else {
                    break;
                };
                let record = record.map_err(GroupFailure::Input)?;
                let index = tail
                    .map_or(Some(1), |tail| tail.index.0.checked_add(1))
                    .ok_or_else(|| RsfError::new(ErrorCode::InvalidInput, "WAL index exhausted"))
                    .map_err(GroupFailure::Input)?;
                let input = WalAppend {
                    pg,
                    epoch: record.epoch,
                    index: LogIndex(index),
                    kind: record.kind,
                    previous_hash: tail.map_or([0; 32], |tail| tail.record_hash),
                    payload: record.payload.as_ref(),
                };
                let frame = wal::encode_at_tail(input, tail).map_err(GroupFailure::Input)?;
                let length = record_disk_len(frame.encoded.len()).map_err(GroupFailure::Input)?;
                mode.validate(self, cursor.as_ref())?;
                if cursor.is_none() {
                    cursor = Some(
                        self.cursor
                            .take()
                            .expect("WAL cursor validated before group"),
                    );
                    self.write_failed = true;
                }
                changed = true;
                let rotate = match &mode {
                    Mode::Coordinated => self.rotation_required(length),
                    Mode::Owned(owner) => owner.rotation_required(self, length)?,
                };
                if rotate {
                    if dirty {
                        self.active.sync_after_append()?;
                    }
                    mode.validate(self, cursor.as_ref())?;
                    match &mut mode {
                        Mode::Coordinated => self.rotate()?,
                        Mode::Owned(owner) => {
                            owner.rotate(self, cursor.as_mut().expect("group cursor"))?
                        }
                    }
                }
                mode.validate(self, cursor.as_ref())?;
                let offset = self.active.append_unsynced(&frame.encoded)?;
                if let Mode::Owned(owner) = &mut mode {
                    owner.written(self, cursor.as_mut().expect("group cursor"), offset, length)?;
                }
                dirty = true;
                tail = Some(WalTail {
                    epoch: input.epoch,
                    index: input.index,
                    record_hash: frame.record_hash,
                });
                // Payload/frame are dropped here before asking the producer
                // for another item. No group-sized payload collection exists.
            }
            // Match the original for-loop destructor boundary before the
            // final data barrier; iterator Drop is also external producer code.
            drop(records);
            mode.validate(self, cursor.as_ref())?;
            if dirty {
                if cursor.is_none() {
                    cursor = Some(
                        self.cursor
                            .take()
                            .expect("WAL cursor validated before group"),
                    );
                    self.write_failed = true;
                }
                changed = true;
                mode.validate(self, cursor.as_ref())?;
                self.active.sync_after_append()?;
            }
            mode.validate(self, cursor.as_ref())?;
            if let Some(cursor) = &mut cursor {
                if matches!(mode, Mode::Coordinated) {
                    cursor.stamp = segment_stamp(&self.root, &self.manifest)?;
                }
                if let Some(tail) = tail {
                    cursor.tails.insert(pg, tail);
                }
            }
            Ok(())
        })();
        if let Err(failure) = result {
            if let Mode::Owned(owner) = &mode {
                if matches!(failure, GroupFailure::Input(_)) {
                    // Producer/iterator Drop can alter custody while an input
                    // error unwinds. Check even a late error before returning
                    // its semantic cause; physical loss takes precedence.
                    if let Err(custody) = owner.validate(self, cursor.as_ref()) {
                        self.cursor = None;
                        return Err(custody);
                    }
                    if !changed {
                        self.write_failed = false;
                    } else {
                        self.cursor = None;
                    }
                } else {
                    // A prior physical observation remains sticky even if
                    // producer Drop restored its original inode afterward.
                    self.cursor = None;
                }
            }
            return Err(failure.into_error());
        }
        let outcome = match mode {
            Mode::Coordinated => None,
            Mode::Owned(owner) => match owner.finish(self, cursor.as_ref(), tail) {
                Ok(result) => Some(result),
                Err(error) => {
                    self.cursor = None;
                    return Err(error);
                }
            },
        };
        if let Some(cursor) = cursor {
            self.cursor = Some(cursor);
        }
        self.write_failed = false;
        Ok(outcome)
    }
}

// Internal provenance, not ErrorCode/message classification. Physical calls
// convert by default; only explicit producer/encoding checks can be retryable.
enum GroupFailure {
    Input(RsfError),
    Physical(RsfError),
}
impl From<RsfError> for GroupFailure {
    fn from(error: RsfError) -> Self {
        Self::Physical(error)
    }
}
impl GroupFailure {
    fn into_error(self) -> RsfError {
        match self {
            Self::Input(error) | Self::Physical(error) => error,
        }
    }
}

enum Mode {
    Coordinated,
    Owned(custody::OwnedAppend),
}
impl Mode {
    fn validate(&self, store: &SegmentStore, cursor: Option<&WalCursor>) -> RsfResult<()> {
        match self {
            Self::Coordinated => Ok(()),
            Self::Owned(owner) => owner.validate(store, cursor),
        }
    }
}

#[cfg(test)]
mod tests;
