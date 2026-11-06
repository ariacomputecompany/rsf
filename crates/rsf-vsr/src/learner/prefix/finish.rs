//! Replay once, retain rejected ownership, publish before retiring old authority.
use super::*;

impl LearnerHistoryStore {
    /// A healthy semantic refusal retains the actual completed candidate and
    /// its original held locks. Physical failure or unwind remains sticky.
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
        let mut namespace = self.preflight_namespace(context.limits)?;
        if matches!(context.continuation, Continuation::Published) {
            let published = self
                .published
                .as_ref()
                .expect("validated exact publication");
            let guard = guard::PublicationGuard::capture(self, published)
                .inspect_err(|_| self.failed.store(true, Ordering::Relaxed))?;
            let preview = VsrImportPreview::new(&published.cell)?;
            self.failed.store(true, Ordering::Relaxed);
            let semantic = validate(&preview);
            guard.validate(self, published)?;
            namespace.validate(self)?;
            if let Err(error) = semantic {
                self.failed.store(false, Ordering::Relaxed);
                return Err(error);
            }
            let tail = published.cell.committed_tail();
            namespace.clear_temporaries(self)?;
            namespace.discard_obsolete(self, Some(id))?;
            namespace.validate(self)?;
            self.failed.store(false, Ordering::Relaxed);
            return Ok(tail);
        }
        if matches!(context.continuation, Continuation::Staging(_)) {
            self.retain_completed_candidate(&mut namespace)?;
        }
        let context = self.prefix_context.as_ref().expect("retained completion");
        let Continuation::Rejected(rejected) = &context.continuation else {
            return Err(corrupt("bounded finish lost completed ownership"));
        };
        let preview = VsrImportPreview::new(&rejected.candidate.cell)
            .inspect_err(|_| self.failed.store(true, Ordering::Relaxed))?;
        self.failed.store(true, Ordering::Relaxed);
        let semantic = stage::preserves(
            self.published.as_ref().expect("actual prior publication"),
            &rejected.candidate,
        )
        .and_then(|()| validate(&preview));
        // All captured guards run even when the application rejects the data.
        // Panic leaves the already-armed sentinel and actual candidate intact.
        rejected.guard.validate(self, &rejected.candidate)?;
        self.validate_prefix_context(context)?;
        namespace.validate(self)?;
        if let Err(error) = semantic {
            self.failed.store(false, Ordering::Relaxed);
            return Err(error);
        }
        namespace.clear_temporaries(self)?;
        let publication = io::prepare_new_publication(&self.root, "ACTIVE", &id)?;
        namespace.prepared_publication(self, &publication)?;
        let context = self.prefix_context.as_ref().expect("retained completion");
        let Continuation::Rejected(rejected) = &context.continuation else {
            unreachable!("retained actual completed owner")
        };
        rejected.guard.validate(self, &rejected.candidate)?;
        namespace.validate(self)?;
        let active = namespace::effect(namespace::Operation::PublishActive, &self.root, || {
            publication.publish_guarded()
        })?;
        let context = self.prefix_context.as_mut().expect("retained completion");
        let Continuation::Rejected(rejected) =
            std::mem::replace(&mut context.continuation, Continuation::Published)
        else {
            unreachable!("same actual completed owner")
        };
        let tail = rejected.candidate.cell.committed_tail();
        let retired = self
            .published
            .replace(rejected.candidate)
            .expect("old actual publication remains owned until handoff");
        // Only the exact prepared inode/bytes changes ACTIVE expectation.
        namespace.published(self, id, active)?;
        namespace.permit_retirement(self, retired.id)?;
        drop(retired);
        namespace.discard_obsolete(self, Some(id))?;
        namespace.validate(self)?;
        self.failed.store(false, Ordering::Relaxed);
        Ok(tail)
    }

    fn retain_completed_candidate(
        &mut self,
        namespace: &mut namespace::Namespace,
    ) -> RsfResult<()> {
        let context = self.prefix_context.as_ref().expect("bounded stage context");
        let stage = self
            .stage
            .as_ref()
            .ok_or_else(|| corrupt("bounded completion lacks original stage"))?;
        if stage.count != stage.descriptor.records {
            return Err(invalid_input("bounded prefix is incomplete"));
        }
        if stage.digest != stage.descriptor.digest {
            return Err(corrupt(
                "bounded prefix digest differs from declared history",
            ));
        }
        let recovered = SegmentStore::recover_immutable_bounded(
            stage.store.root(),
            context.limits.storage()?,
            context.limits.admission()?,
        );
        self.validate_prefix_context(context)?;
        namespace.validate(self)?;
        let (recovery, _) = recovered
            .map_err(stage::storage_error)
            .inspect_err(|_| self.failed.store(true, Ordering::Relaxed))?;
        let previous = self.published.as_ref().expect("actual prior publication");
        let prior = guard::PublicationGuard::capture(self, previous)
            .inspect_err(|_| self.failed.store(true, Ordering::Relaxed))?;
        let replayed = DurableVsrCell::replay_store(&recovery.recovery, self.pg, || {
            Err(corrupt("bounded learner requires persisted bootstrap"))
        });
        prior
            .validate(self, previous)
            .inspect_err(|_| self.failed.store(true, Ordering::Relaxed))?;
        self.validate_prefix_context(context)?;
        namespace.validate(self)?;
        let replayed = replayed?;
        stage::validate_replayed(&stage.descriptor, &replayed)?;
        self.failed.store(true, Ordering::Relaxed);
        let stage = self.stage.take().expect("complete original stage");
        let candidate = stage::complete_replayed(
            self.pg,
            stage,
            recovery.recovery,
            replayed,
            true,
            context.custody().expect("staged custody"),
            &context.flat,
        )?;
        context.directory.validate()?;
        context.descriptor.validate()?;
        context.flat.validate_cell(&candidate.cell)?;
        candidate
            .cell
            .store
            .validate_owned_custody(context.custody().expect("original staged custody"))?;
        let guard = prior.with_candidate(self, &candidate)?;
        let id = candidate.id;
        self.prefix_context
            .as_mut()
            .expect("same bounded context")
            .continuation = Continuation::Rejected(Box::new(Rejected { candidate, guard }));
        namespace.completed(self, id)?;
        Ok(())
    }
}
