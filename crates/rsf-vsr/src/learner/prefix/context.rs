//! The actual candidate survives a healthy semantic refusal. No state here is
//! persisted selection, retention authority, or permission to discard it.
use super::*;

#[derive(Debug)]
pub(in crate::learner) struct PrefixContext {
    pub(in crate::learner) id: [u8; 32],
    pub(in crate::learner) limits: VsrPrefixLimits,
    pub(in crate::learner) bytes: u64,
    pub(in crate::learner) continuation: Continuation,
    pub(in crate::learner) flat: guard::Flat,
    pub(in crate::learner) descriptor: guard::Exact,
    pub(in crate::learner) directory: namespace::Directory,
}

#[derive(Debug)]
pub(in crate::learner) enum Continuation {
    Published,
    Staging(rsf_storage::SegmentStoreCustody),
    Rejected(Box<Rejected>),
}

#[derive(Debug)]
pub(in crate::learner) struct Rejected {
    pub(in crate::learner) candidate: Published,
    pub(in crate::learner) guard: guard::PublicationGuard,
}

impl PrefixContext {
    pub(in crate::learner) fn custody(&self) -> Option<&rsf_storage::SegmentStoreCustody> {
        match &self.continuation {
            Continuation::Published => None,
            Continuation::Staging(custody) => Some(custody),
            Continuation::Rejected(rejected) => Some(&rejected.candidate.cell.store_custody),
        }
    }
}

impl LearnerHistoryStore {
    pub(super) fn validate_prefix_context(&self, context: &PrefixContext) -> RsfResult<()> {
        let result = (|| {
            context.directory.validate()?;
            context.flat.validate()?;
            context.descriptor.validate()?;
            match &context.continuation {
                Continuation::Published => {
                    let published = self
                        .published
                        .as_ref()
                        .ok_or_else(|| corrupt("bounded publication disappeared"))?;
                    if published.id != context.id {
                        return Err(corrupt("bounded publication identity changed"));
                    }
                    context.flat.validate_cell(&published.cell)?;
                }
                Continuation::Staging(expected) => {
                    let stage = self
                        .stage
                        .as_ref()
                        .ok_or_else(|| corrupt("bounded prefix stage disappeared"))?;
                    if stage.descriptor.id()? != context.id {
                        return Err(corrupt("bounded stage identity changed"));
                    }
                    stage.store.validate_owned_custody(expected)?;
                }
                Continuation::Rejected(rejected) => {
                    if rejected.candidate.id != context.id || self.stage.is_some() {
                        return Err(corrupt("bounded rejected owner identity changed"));
                    }
                    rejected.guard.validate(self, &rejected.candidate)?;
                }
            }
            context.directory.validate()?;
            context.flat.validate()
        })();
        if result.is_err() {
            self.failed.store(true, Ordering::Relaxed);
        }
        result
    }

    // Used under an already armed mutation/callback latch. This checks the real
    // held owners directly; it never clears the public failure sentinel.
    pub(super) fn validate_prefix_owners(&self) -> RsfResult<()> {
        let published = self
            .published
            .as_ref()
            .ok_or_else(|| invalid_input("bounded prefix requires publication"))?;
        self.validate_published_custody(published)?;
        guard::owned_lock(&self.root.join(".learner.lock"), &self._lock)?.validate()?;
        // Validate the original inner writer before namespace/capacity checks:
        // a replaced lock must not become a reusable input refusal merely
        // because its displaced name also makes the directory over capacity.
        guard::owned_lock(
            &published.cell.root.join(".vsr-writer.lock"),
            &published.cell._writer_lock,
        )?
        .validate()?;
        if let Some(context) = &self.prefix_context {
            self.validate_prefix_context(context)?;
        }
        if let Some(stage) = &self.stage {
            // A generic warm stage has no historical bounded stamp. Adoption
            // checks its actual retained writer; it never recreates its route.
            let expected = stage.store.capture_owned_custody()?;
            stage.store.validate_owned_custody(&expected)?;
        }
        Ok(())
    }

    pub(super) fn prefix_store(&self, id: &[u8; 32]) -> RsfResult<Option<&SegmentStore>> {
        if let Some(published) = &self.published
            && published.id == *id
        {
            return Ok(Some(&published.cell.store));
        }
        if let Some(stage) = &self.stage
            && stage.descriptor.id()? == *id
        {
            return Ok(Some(&stage.store));
        }
        if let Some(PrefixContext {
            continuation: Continuation::Rejected(rejected),
            ..
        }) = &self.prefix_context
            && rejected.candidate.id == *id
        {
            return Ok(Some(&rejected.candidate.cell.store));
        }
        Ok(None)
    }
    pub(super) fn prefix_count(&self, id: &[u8; 32]) -> RsfResult<u64> {
        if let Some(published) = &self.published
            && published.id == *id
        {
            return Ok(published.descriptor.records);
        }
        if let Some(stage) = &self.stage
            && stage.descriptor.id()? == *id
        {
            return Ok(stage.count);
        }
        if let Some(PrefixContext {
            continuation: Continuation::Rejected(rejected),
            ..
        }) = &self.prefix_context
            && rejected.candidate.id == *id
        {
            return Ok(rejected.candidate.descriptor.records);
        }
        Err(corrupt("bounded namespace lacks actual record count"))
    }
}
