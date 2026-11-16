//! Renew the retained active descriptor without refreshing or recovering data.
use super::*;

impl SegmentStore {
    /// Renew existing active bytes under this owner's original custody stamp.
    /// Unlike an empty append group, this never refreshes a manifest, recovers
    /// a cursor or repairs a tail. Pair with `sync_owned_closed_segments` when
    /// a recovered unsealed prefix includes closed files needing renewal.
    /// No WAL payload is read and successful renewal retains the same cursor.
    pub fn sync_owned_active_segment(&mut self, expected: &SegmentStoreCustody) -> RsfResult<()> {
        self.ensure_writable()?;
        if self.options.log_options.sync_policy == crate::SyncPolicy::None {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                "durable active renewal requires Data or Full sync",
            ));
        }
        let gate = self.gate.clone();
        let _guard = lock_gate(&gate)?;
        // Arm before validation/sync: error and unwind cannot expose an owner
        // whose existing bytes may have failed a prior durability barrier.
        self.write_failed = true;
        let result = (|| {
            self.validate_owned_custody_locked(expected)?;
            if self.active.file.metadata().map_err(custody_io)?.len() != 0 {
                self.active.sync_after_append()?;
            }
            self.validate_owned_custody_locked(expected)
        })();
        if result.is_err() {
            self.cursor = None;
        } else {
            self.write_failed = false;
        }
        result
    }
}

#[cfg(test)]
mod tests;
