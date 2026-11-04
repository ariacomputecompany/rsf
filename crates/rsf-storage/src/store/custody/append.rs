//! Authorized physical transitions for one exclusively owned WAL group.
use super::*;
mod rotation;

/// Result of one successful owned group, bound to the actual written store.
/// It grants no consensus or admission authority and cannot be decoded from DTOs.
#[derive(Debug)]
pub struct OwnedWalGroup {
    tail: Option<WalTail>,
    custody: SegmentStoreCustody,
}
impl OwnedWalGroup {
    pub fn tail(&self) -> Option<WalTail> {
        self.tail
    }
    pub fn into_custody(self) -> SegmentStoreCustody {
        self.custody
    }
}

pub(in crate::store) struct OwnedAppend {
    expected: SegmentStoreCustody,
}
impl OwnedAppend {
    pub(in crate::store) fn new(expected: &SegmentStoreCustody) -> Self {
        Self {
            expected: expected.clone(),
        }
    }

    pub(in crate::store) fn validate(
        &self,
        store: &SegmentStore,
        cursor: Option<&WalCursor>,
    ) -> RsfResult<()> {
        if !Arc::ptr_eq(&store.observer, &self.expected.observer) {
            return Err(custody_error(
                "owned append stamp belongs to another store handle",
            ));
        }
        let actual = store.capture_owned_custody_for_cursor_locked(cursor)?;
        if actual.root != self.expected.root
            || actual.directory != self.expected.directory
            || actual.manifest != self.expected.manifest
            || actual.segments != self.expected.segments
        {
            return Err(custody_error("owned append physical custody changed"));
        }
        Ok(())
    }

    pub(in crate::store) fn active_bytes(&self, store: &SegmentStore) -> RsfResult<u64> {
        self.expected
            .segments
            .last()
            .filter(|(id, _)| *id == store.manifest.active_id)
            .map(|(_, stamp)| stamp.bytes)
            .ok_or_else(|| custody_error("owned append active segment is not the last descriptor"))
    }

    pub(in crate::store) fn rotation_required(
        &self,
        store: &SegmentStore,
        pending: u64,
    ) -> RsfResult<bool> {
        let bytes = self.active_bytes(store)?;
        let next = bytes
            .checked_add(pending)
            .ok_or_else(|| custody_error("owned segment length overflow"))?;
        Ok(store.options.max_segment_bytes != 0
            && bytes != 0
            && next > store.options.max_segment_bytes)
    }

    pub(in crate::store) fn written(
        &mut self,
        store: &SegmentStore,
        cursor: &mut WalCursor,
        offset: u64,
        disk_bytes: u64,
    ) -> RsfResult<()> {
        let before = self
            .expected
            .segments
            .last_mut()
            .filter(|(id, _)| *id == store.manifest.active_id)
            .ok_or_else(|| custody_error("owned append lost its active stamp"))?;
        let expected_bytes = before
            .1
            .bytes
            .checked_add(disk_bytes)
            .ok_or_else(|| custody_error("owned segment length overflow"))?;
        let after = file_stamp(&store.active.file.metadata().map_err(custody_io)?)?;
        if offset != before.1.bytes
            || after.identity != before.1.identity
            || after.bytes != expected_bytes
            || regular_file(&store.active.path)? != after
        {
            return Err(custody_error(
                "owned append differs from its exact descriptor write",
            ));
        }
        // Only this successful write may advance the active length/mtime. All
        // other identities remain the original or a prior own transition.
        before.1 = after.clone();
        let stamp = cursor
            .stamp
            .last_mut()
            .filter(|stamp| stamp.id == store.manifest.active_id)
            .ok_or_else(|| custody_error("owned append lost its active cursor"))?;
        stamp.bytes = after.bytes;
        stamp.modified = after.modified;
        self.validate(store, Some(cursor))
    }

    pub(in crate::store) fn rotate(
        &mut self,
        store: &mut SegmentStore,
        cursor: &mut WalCursor,
    ) -> RsfResult<()> {
        self.validate(store, Some(cursor))?;
        rotation::rotate(self, store, cursor)
    }

    pub(in crate::store) fn finish(
        self,
        store: &SegmentStore,
        cursor: Option<&WalCursor>,
        tail: Option<WalTail>,
    ) -> RsfResult<OwnedWalGroup> {
        self.validate(store, cursor)?;
        Ok(OwnedWalGroup {
            tail,
            custody: self.expected,
        })
    }
}

#[cfg(all(test, unix))]
mod tests;
