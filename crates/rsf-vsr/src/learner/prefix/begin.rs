//! Explicit BEGIN may abandon provisional history, after original owner checks.
use super::*;

impl LearnerHistoryStore {
    /// Start or resume finite history. Existing candidates must be complete
    /// strict stores; only a directory created in this call can run a factory.
    pub fn begin_prefix(
        &mut self,
        descriptor: VsrHistoryDescriptor,
        limits: VsrPrefixLimits,
    ) -> RsfResult<u64> {
        self.healthy()?;
        self.validate_prefix_owners()
            .inspect_err(|_| self.failed.store(true, Ordering::Relaxed))?;
        limits.validate()?;
        self.validate_descriptor(&descriptor)?;
        limits.check(descriptor.records, 0)?;
        let id = descriptor.id()?;
        let published = self.published.as_ref().expect("protected published owner");
        if descriptor.probe.view < published.descriptor.probe.view
            || descriptor.probe.committed_tail < published.descriptor.probe.committed_tail
        {
            return Err(invalid_input(
                "bounded prefix would regress published view or committed tail",
            ));
        }
        // All candidates, including the last malformed/over-limit one, are
        // inspected before the first factory, selection change or deletion.
        let mut namespace = self.preflight_namespace(limits)?;
        if published.id == id {
            return self.begin_published(descriptor, limits, &mut namespace);
        }
        if self
            .prefix_context
            .as_ref()
            .is_some_and(|context| context.id == id)
        {
            let context = self.prefix_context.as_ref().expect("same context");
            let (count, bytes) = match &context.continuation {
                Continuation::Staging(_) => {
                    let stage = self.stage.as_ref().expect("validated stage");
                    limits.check(stage.count, context.bytes)?;
                    (stage.count, context.bytes)
                }
                Continuation::Rejected(rejected) => (
                    rejected.candidate.descriptor.records,
                    stage::published_bytes(&rejected.candidate, limits)?,
                ),
                Continuation::Published => {
                    return Err(corrupt("bounded context differs from ACTIVE"));
                }
            };
            self.failed.store(true, Ordering::Relaxed);
            let context = self.prefix_context.as_mut().expect("validated same owner");
            context.limits = limits;
            context.bytes = bytes;
            namespace.clear_temporaries(self)?;
            namespace.discard_obsolete(self, Some(id))?;
            namespace.validate(self)?;
            self.failed.store(false, Ordering::Relaxed);
            return Ok(count);
        }
        // A generic warm stage is adopted through its actual writer, not a
        // create-capable reopen. The first bounded stamp cannot claim older
        // physical history that the generic handle never captured.
        if self.stage.as_ref().map(|s| s.descriptor.id()).transpose()? == Some(id) {
            let stage = self.stage.as_ref().expect("matching raw stage");
            let bytes = stage::stage_bytes(stage, limits)
                .inspect_err(|_| self.failed.store(true, Ordering::Relaxed))?;
            let context = PrefixContext {
                id,
                limits,
                bytes,
                continuation: Continuation::Staging(stage.store.capture_owned_custody()?),
                flat: guard::Flat::capture(stage.store.root())?,
                descriptor: guard::Exact::capture(
                    &self.directory(&id).join("DESCRIPTOR"),
                    &descriptor.encode()?,
                )?,
                directory: namespace::Directory::capture(&self.directory(&id))?,
            };
            namespace.validate(self)?;
            self.failed.store(true, Ordering::Relaxed);
            self.prefix_context = Some(context);
            namespace.clear_temporaries(self)?;
            namespace.discard_obsolete(self, Some(id))?;
            let count = self.stage.as_ref().expect("same actual stage").count;
            namespace.validate(self)?;
            self.failed.store(false, Ordering::Relaxed);
            return Ok(count);
        }
        self.begin_replacement(descriptor, limits, namespace)
    }

    fn begin_published(
        &mut self,
        descriptor: VsrHistoryDescriptor,
        limits: VsrPrefixLimits,
        namespace: &mut namespace::Namespace,
    ) -> RsfResult<u64> {
        let published = self.published.as_ref().expect("bounded publication");
        let id = published.id;
        let flat = match &published.bounded_flat {
            Some(flat) => {
                flat.validate_cell(&published.cell)?;
                flat.clone()
            }
            None => guard::Flat::capture(&published.cell.root)?,
        };
        let context = PrefixContext {
            id,
            limits,
            bytes: stage::published_bytes(published, limits)?,
            continuation: Continuation::Published,
            flat: flat.clone(),
            descriptor: guard::Exact::capture(
                &self.directory(&id).join("DESCRIPTOR"),
                &descriptor.encode()?,
            )?,
            directory: namespace::Directory::capture(&self.directory(&id))?,
        };
        flat.validate_cell(&published.cell)?;
        namespace.validate(self)?;
        self.failed.store(true, Ordering::Relaxed);
        let retired_id = self.provisional_id()?;
        let retired_stage = self.stage.take();
        let retired_context = self.prefix_context.replace(context);
        self.published
            .as_mut()
            .expect("same publication")
            .bounded_flat = Some(flat);
        if let Some(retired) = retired_id {
            namespace.permit_retirement(self, retired)?;
        }
        // The original metadata was captured for disposal while these actual
        // old writer/lock descriptors remained held.
        drop(retired_stage);
        drop(retired_context);
        namespace.clear_temporaries(self)?;
        namespace.discard_obsolete(self, Some(id))?;
        namespace.validate(self)?;
        self.failed.store(false, Ordering::Relaxed);
        Ok(descriptor.records)
    }

    fn begin_replacement(
        &mut self,
        descriptor: VsrHistoryDescriptor,
        limits: VsrPrefixLimits,
        mut namespace: namespace::Namespace,
    ) -> RsfResult<u64> {
        let id = descriptor.id()?;
        let exists = namespace.contains(&id);
        let encoded = descriptor.encode()?;
        // An existing requested candidate gets no factory/descriptor repair.
        // Check its exact metadata before any obsolete candidate is removed.
        let existing_descriptor = if exists {
            Some(guard::Exact::capture(
                &self.directory(&id).join("DESCRIPTOR"),
                &encoded,
            )?)
        } else {
            None
        };
        let mut created = None;
        let mut created_cell = None;
        self.failed.store(true, Ordering::Relaxed);
        if !exists {
            namespace.clear_temporaries(self)?;
            namespace.discard_obsolete(self, None)?;
            let original = namespace.create_slot(self, id)?;
            namespace::effect(
                namespace::Operation::RetainDescriptor,
                &self.directory(&id),
                || io::retain_descriptor(&self.directory(&id), &encoded),
            )?;
            namespace.validate_preparing(self, id, &original)?;
            let cell = self.directory(&id).join("cell");
            namespace::effect(namespace::Operation::CreateDirectory, &cell, || {
                fs::create_dir(&cell).map_err(io_error)
            })?;
            created_cell = Some(namespace::Directory::capture(&cell)?);
            namespace.validate_preparing(self, id, &original)?;
            created = Some(original);
        }
        let descriptor_guard = match existing_descriptor {
            Some(exact) => exact,
            None => guard::Exact::capture(&self.directory(&id).join("DESCRIPTOR"), &encoded)?,
        };
        let stage::OpenedPrefix {
            mut stage,
            bytes,
            custody,
            flat,
            ..
        } = self.open_prefix_stage(descriptor, limits, !exists)?;
        if let Some(original) = &created_cell {
            original.validate()?;
        }
        if let Some(original) = &created {
            namespace.validate_preparing(self, id, original)?;
        } else {
            namespace.validate(self)?;
        }
        flat.validate()?;
        stage::renew(&mut stage.store, &custody)?;
        flat.validate()?;
        descriptor_guard.validate()?;
        if let Some(original) = &created_cell {
            original.validate()?;
        }
        let context = PrefixContext {
            id,
            limits,
            bytes,
            continuation: Continuation::Staging(custody),
            flat,
            descriptor: descriptor_guard,
            directory: namespace::Directory::capture(&self.directory(&id))?,
        };
        let count = stage.count;
        let retired_id = self.provisional_id()?;
        let retired_stage = self.stage.replace(stage);
        let retired_context = self.prefix_context.replace(context);
        match created {
            Some(original) => namespace.add_created(self, original, id)?,
            None => namespace.adopt_opened(self, id)?,
        }
        if let Some(retired) = retired_id {
            namespace.permit_retirement(self, retired)?;
        }
        drop(retired_stage);
        drop(retired_context);
        namespace.clear_temporaries(self)?;
        namespace.discard_obsolete(self, Some(id))?;
        namespace.validate(self)?;
        self.failed.store(false, Ordering::Relaxed);
        Ok(count)
    }

    fn provisional_id(&self) -> RsfResult<Option<[u8; 32]>> {
        if let Some(stage) = &self.stage {
            return stage.descriptor.id().map(Some);
        }
        Ok(self
            .prefix_context
            .as_ref()
            .and_then(|c| match &c.continuation {
                Continuation::Rejected(rejected) => Some(rejected.candidate.id),
                _ => None,
            }))
    }
    pub(super) fn preflight_namespace(
        &self,
        limits: VsrPrefixLimits,
    ) -> RsfResult<namespace::Namespace> {
        let result = namespace::Namespace::capture(self, limits);
        // A read-only capacity/input refusal is reusable only if actual
        // protected owners remain unchanged; physical failure dominates it.
        self.validate_prefix_owners()
            .inspect_err(|_| self.failed.store(true, Ordering::Relaxed))?;
        if result
            .as_ref()
            .is_err_and(|error| error.code == ErrorCode::CorruptionDetected)
        {
            // In particular, an equal-byte outer inode replacement observed by
            // the original namespace proof must not disappear behind the older
            // byte-oriented published token. Structural faults are conservative
            // sticky refusals; finite capacity errors remain reusable.
            self.failed.store(true, Ordering::Relaxed);
        }
        result
    }
}
