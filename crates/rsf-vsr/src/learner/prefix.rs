//! Opt-in finite prefix transfer. Domain policy is a caller-owned wrapper, not
//! persisted by these generic kernel methods. Cold callers must supply it again.
use super::*;
use crate::transfer::prefix::{self as export, VsrPrefixLimits};
pub(in crate::learner) mod append;
pub(in crate::learner) mod guard;
pub(in crate::learner) mod io;
pub(in crate::learner) mod preview;
pub(in crate::learner) mod stage;
pub use preview::VsrImportPreview;

mod begin;
mod context;
mod finish;
mod namespace;
pub(in crate::learner) use context::{Continuation, PrefixContext, Rejected};

impl LearnerHistoryStore {
    /// Historical data under this actual non-voting owner. No current source
    /// vote, primary ACK, promotion or client permission is implied.
    pub fn export_committed_prefix(
        &self,
        through: LogIndex,
        limits: VsrPrefixLimits,
    ) -> Result<VsrLearnerPrefixExport, VsrPrefixExportError> {
        let token = self.published_state_token()?;
        let published = self.published.as_ref().expect("token requires publication");
        let prefix = export::freeze(&published.cell, through, limits)?;
        if self.published_state_token()? != token {
            return Err(corrupt("learner prefix source changed during export").into());
        }
        Ok(VsrLearnerPrefixExport { prefix, token })
    }

    pub fn read_committed_prefix_batch(
        &self,
        export: &VsrLearnerPrefixExport,
        ordinal: u64,
    ) -> RsfResult<VsrHistoryBatch> {
        if self.published_state_token()? != export.token {
            return Err(invalid_input(
                "prefix belongs to a different or changed learner owner",
            ));
        }
        let batch = export.prefix.batch(ordinal)?;
        if self.published_state_token()? != export.token {
            return Err(corrupt("learner prefix source changed during batch read"));
        }
        Ok(batch)
    }

    /// Open only an existing published non-voter, with limits in force before
    /// reading its manifest or allocating WAL payloads. Never initializes,
    /// repairs, truncates or promotes missing acknowledged custody.
    pub fn open_published_existing(
        path: impl AsRef<Path>,
        pg: PlacementGroupId,
        local: NodeId,
        source: NodeId,
        limits: VsrPrefixLimits,
    ) -> RsfResult<Self> {
        limits.validate()?;
        if local == source {
            return Err(invalid_input("learner and source must differ"));
        }
        let root = io::normalized(path.as_ref())?;
        namespace::Namespace::root_names(&root)?;
        let root_identity = guard::directory(&root)?;
        let lock = io::lock(&root.join(".learner.lock"), false)?;
        let lock_guard = guard::owned_lock(&root.join(".learner.lock"), &lock)?;
        let mut expected = b"RSFL1".to_vec();
        for value in [pg.0, local.0, source.0] {
            expected.extend_from_slice(&value.to_le_bytes());
        }
        let identity = guard::Exact::capture(&root.join("IDENTITY"), &expected)?;
        let (active, active_guard) = io::read(&root.join("ACTIVE"), 32)?;
        let id: [u8; 32] = active
            .try_into()
            .map_err(|_| corrupt("invalid active learner id"))?;
        let owner_custody = custody::OwnerCustody::from_original(
            root.clone(),
            root_identity.clone(),
            identity.clone(),
            lock_guard.clone(),
            Some(active_guard.clone()),
        );
        owner_custody.validate(&root, &lock)?;
        let mut owner = Self {
            owner_custody,
            root,
            pg,
            local,
            source,
            stage: None,
            published: None,
            failed: AtomicBool::new(false),
            prefix_context: None,
            _lock: lock,
        };
        let directory = owner.directory(&id);
        let directory_identity = guard::directory(&directory)?;
        let (bytes, descriptor_guard) =
            io::read(&directory.join("DESCRIPTOR"), io::DESCRIPTOR_BYTES)?;
        let descriptor = VsrHistoryDescriptor::decode(&bytes)?;
        owner.validate_descriptor(&descriptor)?;
        limits.check(descriptor.records, 0)?;
        if descriptor.id()? != id {
            return Err(corrupt("active learner descriptor identity mismatch"));
        }
        let opened = owner.open_prefix_stage(descriptor, limits, false)?;
        opened.stage.custody.require_writer()?;
        let published = owner.complete_prefix(opened, false)?;
        identity.validate()?;
        lock_guard.validate()?;
        guard::owned_lock(&owner.root.join(".learner.lock"), &owner._lock)?;
        active_guard.validate()?;
        descriptor_guard.validate()?;
        if guard::directory(&owner.root)? != root_identity
            || guard::directory(&directory)? != directory_identity
        {
            return Err(corrupt(
                "published learner namespace changed during cold replay",
            ));
        }
        owner.published = Some(published);
        owner.published_state_token()?;
        owner.preflight_namespace(limits)?;
        Ok(owner)
    }

    /// Validate the entire bounded batch, including exact durable overlap,
    /// before writing its suffix. A successful group ACK includes data barriers.
    pub fn append_prefix_batch(&mut self, id: [u8; 32], batch: &VsrHistoryBatch) -> RsfResult<u64> {
        self.healthy()?;
        let context = self
            .prefix_context
            .as_ref()
            .ok_or_else(|| invalid_input("begin bounded prefix before appending"))?;
        if context.id != id {
            return Err(invalid_input("bounded prefix snapshot mismatch"));
        }
        self.validate_prefix_context(context)?;
        if matches!(context.continuation, Continuation::Rejected(_)) {
            let result = append::repeat_rejected(
                self.pg,
                self.prefix_context
                    .as_mut()
                    .expect("validated rejected owner"),
                &self.failed,
                batch,
            );
            self.validate_prefix_context(
                self.prefix_context
                    .as_ref()
                    .expect("retained rejected owner"),
            )?;
            return result;
        }
        let stage = self
            .stage
            .as_mut()
            .ok_or_else(|| invalid_input("published prefix needs no further records"))?;
        append::append(
            self.pg,
            stage,
            self.prefix_context.as_mut().expect("validated context"),
            &self.failed,
            batch,
        )
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod observation;
