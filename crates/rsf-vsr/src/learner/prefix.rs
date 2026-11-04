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

#[derive(Debug)]
pub(in crate::learner) struct PrefixContext {
    pub(in crate::learner) id: [u8; 32],
    pub(in crate::learner) limits: VsrPrefixLimits,
    pub(in crate::learner) bytes: u64,
    pub(in crate::learner) custody: Option<rsf_storage::SegmentStoreCustody>,
    pub(in crate::learner) flat: guard::Flat,
    pub(in crate::learner) descriptor: guard::Exact,
}

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
        let mut owner = Self {
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
        Ok(owner)
    }

    /// Start or resume the exact descriptor under caller-supplied finite limits.
    /// Bounded mode cannot be downgraded by ordinary BEGIN/APPEND/FINISH in this
    /// owner. A reserved-domain wrapper must reestablish policy after reopening.
    pub fn begin_prefix(
        &mut self,
        descriptor: VsrHistoryDescriptor,
        limits: VsrPrefixLimits,
    ) -> RsfResult<u64> {
        self.healthy()?;
        limits.validate()?;
        self.validate_descriptor(&descriptor)?;
        limits.check(descriptor.records, 0)?;
        let id = descriptor.id()?;
        let published = self.published.as_ref().ok_or_else(|| {
            invalid_input("bounded prefix import requires an existing publication")
        })?;
        if descriptor.probe.view < published.descriptor.probe.view
            || descriptor.probe.committed_tail < published.descriptor.probe.committed_tail
        {
            return Err(invalid_input(
                "bounded prefix would regress published view or committed tail",
            ));
        }
        // Both branches can replace/drop a warm stage. Validate its original
        // custody before even an exact current-ACTIVE BEGIN can reclaim it.
        if let Some(context) = &self.prefix_context {
            self.validate_prefix_context(context)?;
        }
        if published.id == id {
            let flat = match &published.bounded_flat {
                Some(flat) => {
                    flat.validate_cell(&published.cell)?;
                    flat.clone()
                }
                None => guard::Flat::capture(&published.cell.root)?,
            };
            let bytes = stage::published_bytes(published, limits)?;
            let descriptor_guard = guard::Exact::capture(
                &self.directory(&id).join("DESCRIPTOR"),
                &descriptor.encode()?,
            )?;
            // Opting an existing generic publication into bounded mode must
            // retain this same original guard in its lifetime/post-callback
            // checks, not only in the transient transfer context.
            flat.validate_cell(&published.cell).inspect_err(|_| {
                self.failed.store(true, Ordering::Relaxed);
            })?;
            self.published
                .as_mut()
                .expect("same-id published owner retained")
                .bounded_flat = Some(flat.clone());
            self.prefix_context = Some(PrefixContext {
                id,
                limits,
                bytes,
                custody: None,
                flat,
                descriptor: descriptor_guard,
            });
            self.stage = None;
            self.reclaim_unreferenced()?;
            return Ok(descriptor.records);
        }
        self.stage = None;
        self.prefix_context = None;
        let directory = self.directory(&id);
        io::create_directory(&directory)?;
        sync_directory(&self.root)?;
        let encoded = descriptor.encode()?;
        io::retain_descriptor(&directory, &encoded)?;
        let descriptor_guard = guard::Exact::capture(&directory.join("DESCRIPTOR"), &encoded)?;
        let result = (|| {
            let stage::OpenedPrefix {
                mut stage,
                bytes,
                custody,
                flat,
                ..
            } = self.open_prefix_stage(descriptor, limits, true)?;
            flat.validate()?;
            stage::renew(&mut stage.store, &custody)?;
            flat.validate()?;
            descriptor_guard.validate()?;
            let count = stage.count;
            self.stage = Some(stage);
            self.prefix_context = Some(PrefixContext {
                id,
                limits,
                bytes,
                custody: Some(custody),
                flat,
                descriptor: descriptor_guard,
            });
            self.reclaim_unreferenced()?;
            Ok(count)
        })();
        if result.is_err() {
            self.failed.store(true, Ordering::Relaxed);
        }
        result
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

    /// Replay and validate a candidate before ACTIVE. The callback sees only a
    /// borrowed read view. Err leaves prior publication authoritative; an unwind
    /// or physical custody fault fences this owner until strict cold reopening.
    pub fn finish_prefix(
        &mut self,
        id: [u8; 32],
        validate: impl FnOnce(&VsrImportPreview<'_>) -> RsfResult<()>,
    ) -> RsfResult<LogIndex> {
        self.healthy()?;
        let context = self
            .prefix_context
            .as_ref()
            .ok_or_else(|| invalid_input("begin bounded prefix before finish"))?;
        if context.id != id {
            return Err(invalid_input("bounded prefix finish snapshot mismatch"));
        }
        self.validate_prefix_context(context)?;
        if self
            .published
            .as_ref()
            .is_some_and(|published| published.id == id)
        {
            let published = self.published.as_ref().expect("published exact prefix");
            let guard = guard::PublicationGuard::capture(self, published).inspect_err(|_| {
                self.failed.store(true, Ordering::Relaxed);
            })?;
            let preview = VsrImportPreview::new(&published.cell).inspect_err(|_| {
                self.failed.store(true, Ordering::Relaxed);
            })?;
            self.failed.store(true, Ordering::Relaxed);
            let semantic = validate(&preview);
            // Must run even on semantic Err; no recapture and no public health
            // bypass. Panic deliberately leaves the failure latch armed.
            guard.validate(self, published)?;
            self.failed.store(false, Ordering::Relaxed);
            semantic?;
            return Ok(published.cell.committed_tail());
        }
        let limits = context.limits;
        let stage = self
            .stage
            .as_ref()
            .ok_or_else(|| invalid_input("no bounded prefix stage"))?;
        if stage.count != stage.descriptor.records {
            return Err(invalid_input("bounded prefix is incomplete"));
        }
        if stage.digest != stage.descriptor.digest {
            return Err(corrupt(
                "bounded prefix digest differs from declared history",
            ));
        }
        let (recovery, _) = SegmentStore::recover_immutable_bounded(
            stage.store.root(),
            limits.storage()?,
            limits.admission()?,
        )
        .map_err(stage::storage_error)
        .inspect_err(|_| {
            self.failed.store(true, Ordering::Relaxed);
        })?;
        self.validate_prefix_context(self.prefix_context.as_ref().expect("context retained"))?;
        // Pure authoritative replay runs while the actual stage and original
        // physical stamps remain owned. Invalid peer certificates are rejected
        // without turning healthy prior publication into a lost-custody error.
        let previous = self.published.as_ref().expect("begin requires publication");
        let prior_guard = guard::PublicationGuard::capture(self, previous).inspect_err(|_| {
            self.failed.store(true, Ordering::Relaxed);
        })?;
        let replayed = DurableVsrCell::replay_store(&recovery.recovery, self.pg, || {
            Err(corrupt("bounded learner requires persisted bootstrap"))
        });
        prior_guard.validate(self, previous).inspect_err(|_| {
            self.failed.store(true, Ordering::Relaxed);
        })?;
        self.validate_prefix_context(self.prefix_context.as_ref().expect("context retained"))?;
        let replayed = replayed?;
        stage::validate_replayed(
            &self.stage.as_ref().expect("stage retained").descriptor,
            &replayed,
        )?;
        let stage = self.stage.take().expect("complete stage");
        let context = self.prefix_context.as_ref().expect("retained context");
        let candidate = stage::complete_replayed(
            self.pg,
            stage,
            recovery.recovery,
            replayed,
            true,
            context
                .custody
                .as_ref()
                .expect("complete stage retains original custody"),
            &context.flat,
        )
        .inspect_err(|_| {
            self.failed.store(true, Ordering::Relaxed);
        })?;
        let guard = guard::PublicationGuard::capture(self, &candidate).inspect_err(|_| {
            self.failed.store(true, Ordering::Relaxed);
        })?;
        let preview = VsrImportPreview::new(&candidate.cell).inspect_err(|_| {
            self.failed.store(true, Ordering::Relaxed);
        })?;
        self.failed.store(true, Ordering::Relaxed);
        let semantic = stage::preserves(
            self.published.as_ref().expect("begin requires publication"),
            &candidate,
        )
        .and_then(|()| validate(&preview));
        guard.validate(self, &candidate)?;
        if let Err(error) = semantic {
            self.failed.store(false, Ordering::Relaxed);
            // Candidate bytes remain resumable, but its writer is dropped.
            // A subsequent bounded BEGIN revalidates before resuming.
            self.prefix_context
                .as_mut()
                .expect("context retained")
                .custody = None;
            return Err(error);
        }
        let publication = io::prepare_publication(&self.root, "ACTIVE", &id)?;
        // The same captured physical evidence is checked after the temporary's
        // data barrier and immediately before the authoritative rename.
        guard.validate(self, &candidate)?;
        publication.publish()?;
        let tail = candidate.cell.committed_tail();
        self.published = Some(candidate);
        // Publication is this owner's transition, not adoption of a changed
        // pointer. Validate the new anchor before clearing the armed latch.
        self.validate_published_custody(self.published.as_ref().expect("just published"))?;
        self.prefix_context
            .as_mut()
            .expect("context retained")
            .custody = None;
        self.failed.store(false, Ordering::Relaxed);
        self.reclaim_unreferenced()?;
        Ok(tail)
    }

    fn validate_prefix_context(&self, context: &PrefixContext) -> RsfResult<()> {
        let result = (|| {
            context.flat.validate()?;
            context.descriptor.validate()?;
            if let Some(expected) = &context.custody {
                let stage = self
                    .stage
                    .as_ref()
                    .ok_or_else(|| corrupt("bounded prefix stage disappeared"))?;
                if stage.descriptor.id()? != context.id {
                    return Err(corrupt("bounded stage identity changed"));
                }
                stage.store.validate_owned_custody(expected)?;
            }
            context.flat.validate()
        })();
        if result.is_err() {
            self.failed.store(true, Ordering::Relaxed);
        }
        result
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod observation;
