//! Replay, mandatory validation, external restriction, then one owned publication.
use super::*;

impl InitialLearnerHistoryStore {
    pub(super) fn replay_candidate(&mut self, create_lock: bool) -> RsfResult<()> {
        self.healthy()?;
        let candidate = self
            .candidate
            .as_ref()
            .ok_or_else(|| invalid_input("no initial candidate selected"))?;
        let Body::Receiving { stage, .. } = &candidate.body else {
            return self.replayed_source().map(|_| ());
        };
        if stage.count != stage.descriptor.records {
            return Err(invalid_input("initial candidate is incomplete"));
        }
        if stage.digest != stage.descriptor.digest {
            return Err(invalid_input(
                "initial candidate digest differs from descriptor",
            ));
        }
        let limits = self.policy.limits;
        let (recovery, _) = SegmentStore::recover_immutable_bounded(
            stage.store.root(),
            limits.storage()?,
            limits.admission()?,
        )
        .map_err(stage::storage_error)
        .inspect_err(|_| self.failed.store(true, Ordering::Relaxed))?;
        self.healthy()?;
        let replayed = DurableVsrCell::replay_store(&recovery.recovery, self.policy.pg, || {
            Err(corrupt(
                "initial learner requires persisted original bootstrap",
            ))
        });
        // Pure semantic replay may fail; physical postvalidation still runs.
        self.healthy()?;
        let replayed = replayed?;
        stage::validate_replayed(&stage.descriptor, &replayed)?;
        self.failed.store(true, Ordering::Relaxed);
        let candidate = self.candidate.take().expect("validated initial candidate");
        let Body::Receiving { stage, context } = candidate.body else {
            unreachable!("receiving candidate retained")
        };
        let published = stage::complete_replayed(
            self.policy.pg,
            *stage,
            recovery.recovery,
            replayed,
            create_lock,
            context
                .custody()
                .ok_or_else(|| corrupt("initial receiving custody absent at handoff"))?,
            &context.flat,
        )?;
        published.cell.store.validate_owned_custody(
            context
                .custody()
                .ok_or_else(|| corrupt("initial receiving custody absent at handoff"))?,
        )?;
        context.descriptor.validate()?;
        context.directory.validate()?;
        self.custody
            .validate(&self.root, self.lock.as_ref().expect("actual learner lock"))?;
        let token = published.cell.recovery_state_token()?;
        let writer = guard::owned_lock(
            &published.cell.root.join(".vsr-writer.lock"),
            &published.cell._writer_lock,
        )?;
        self.candidate = Some(Candidate {
            body: Body::Replayed {
                published: Box::new(published),
                token,
                writer,
                transfer: CompletedTransfer {
                    id: context.id,
                    flat: context.flat,
                    descriptor: context.descriptor,
                    directory: context.directory,
                },
            },
        });
        self.validate_owned()?;
        self.failed.store(false, Ordering::Relaxed);
        // This is semantic admission of the initial cut, not an owner failure.
        self.replayed_source().map(|_| ())
    }
    pub(super) fn replayed_source(&self) -> RsfResult<NodeId> {
        let candidate = self
            .candidate
            .as_ref()
            .ok_or_else(|| corrupt("initial replay candidate absent"))?;
        let Body::Replayed { published, .. } = &candidate.body else {
            return Err(invalid_input("initial candidate is not replayed"));
        };
        let cell = &published.cell;
        let current = cell.membership_lineage()?;
        let prepare = cell
            .prepared(LogIndex(1))
            .ok_or_else(|| invalid_input("initial candidate lacks Genesis prepare"))?;
        let historical = cell
            .membership_lineage_at(prepare.view)?
            .ok_or_else(|| invalid_input("initial Genesis lacks certified lineage"))?;
        if cell.committed_tail() != LogIndex(1)
            || cell.committed_count() != 1
            || cell.next_index() != LogIndex(2)
            || current.bootstrap != self.policy.bootstrap
            || current.plan.is_some()
            || historical.bootstrap != self.policy.bootstrap
            || historical.plan.is_some()
            || historical.voters != current.voters
            || historical.voters.contains(&self.policy.local)
            || !historical.voters.contains(&prepare.primary)
            || prepare.primary == self.policy.local
            || prepare.primary != published.descriptor.probe.primary
        {
            return Err(invalid_input(
                "initial candidate is not an exact certified original Genesis cut",
            ));
        }
        Ok(prepare.primary)
    }
    fn semantic(
        &mut self,
        validate: &mut impl FnMut(&VsrImportPreview<'_>) -> RsfResult<()>,
    ) -> RsfResult<()> {
        self.healthy()?;
        let candidate = self.candidate.as_ref().expect("replayed candidate");
        let Body::Replayed { published, .. } = &candidate.body else {
            unreachable!("candidate replayed")
        };
        let preview = VsrImportPreview::new(&published.cell)?;
        self.failed.store(true, Ordering::Relaxed);
        let result = validate(&preview);
        // Do not call public healthy while this private callback sentinel is
        // armed. Validate the original physical guards on Ok and Err alike.
        self.validate_owned()?;
        self.failed.store(false, Ordering::Relaxed);
        result
    }
    fn persist(
        &mut self,
        next: VsrInitialState,
        persist: &mut impl FnMut(&VsrInitialTransition<'_>) -> RsfResult<()>,
    ) -> RsfResult<()> {
        self.validate_owned()?;
        self.failed.store(true, Ordering::Relaxed);
        let result = persist(&VsrInitialTransition {
            policy: &self.policy,
            expected: self.state,
            next,
        });
        self.validate_owned()?;
        result?;
        self.state = next;
        // Deliberately keep the sentinel armed through the physical transaction.
        Ok(())
    }
    pub fn finish(
        &mut self,
        selection: &VsrInitialSelection,
        validate: &mut impl FnMut(&VsrImportPreview<'_>) -> RsfResult<()>,
        persist: &mut impl FnMut(&VsrInitialTransition<'_>) -> RsfResult<()>,
    ) -> Result<VsrInitialPublication, VsrInitialError> {
        self.healthy()?;
        let pinned = match self.state {
            VsrInitialState::Selected(actual) if actual == *selection => None,
            VsrInitialState::Publishing {
                selection: actual,
                source,
            } if actual == *selection => Some(source),
            _ => return Err(VsrInitialError::SelectionConflict { actual: self.state }),
        };
        if pinned.is_none() {
            self.failed.store(true, Ordering::Relaxed);
            reclaim::clean_selected(self)?;
            self.failed.store(false, Ordering::Relaxed);
        }
        self.replay_candidate(pinned.is_none())?;
        let source = self.replayed_source()?;
        if pinned.is_some_and(|expected| expected != source) {
            return Err(corrupt("initial pinned source differs from actual Genesis").into());
        }
        self.semantic(validate)?;
        if pinned.is_none() {
            self.persist(
                VsrInitialState::Publishing {
                    selection: *selection,
                    source,
                },
                persist,
            )?;
        } else {
            self.failed.store(true, Ordering::Relaxed);
        }
        if self.custody.source.is_none() {
            let identity = io::prepare_publication(
                &self.root,
                "IDENTITY",
                &identity::published(&self.policy, source),
            )?;
            self.validate_owned()?;
            self.custody.bind_source(source, identity)?;
        }
        self.validate_owned()?;
        if !self.custody.has_active() {
            let active = io::prepare_publication(&self.root, "ACTIVE", &selection.history_id)?;
            self.validate_owned()?;
            self.custody.record_active(active)?;
        }
        self.validate_owned()?;
        // Same actual candidate is inspected after owned publication. Never
        // release/reopen it or substitute a decoded PublishedControlState.
        let candidate = self
            .candidate
            .as_ref()
            .expect("published candidate retained");
        let Body::Replayed { published, .. } = &candidate.body else {
            unreachable!("replayed candidate")
        };
        let preview = VsrImportPreview::new(&published.cell)?;
        let semantic = validate(&preview);
        self.validate_owned()?;
        semantic?; // Any post-publication failure remains fenced.
        self.persist(
            VsrInitialState::Published {
                selection: *selection,
                source,
            },
            persist,
        )?;
        self.validate_owned()?;
        let candidate = self.candidate.take().expect("completed candidate");
        let Body::Replayed { published, .. } = candidate.body else {
            unreachable!("completed replay")
        };
        let owner_custody = self.custody.publication_custody(self.root.clone())?;
        let owner = LearnerHistoryStore {
            owner_custody,
            root: self.root.clone(),
            pg: self.policy.pg,
            local: self.policy.local,
            source,
            stage: None,
            published: Some(*published),
            failed: AtomicBool::new(false),
            prefix_context: None,
            _lock: self.lock.take().expect("actual learner lock"),
        };
        // This can fail only closed. Native Published is already restrictive;
        // no failed completion returns a usable owner or allows fresh factory.
        owner.published_state_token()?;
        self.completed = true;
        Ok(VsrInitialPublication {
            owner,
            selection: *selection,
            source,
        })
    }
}
