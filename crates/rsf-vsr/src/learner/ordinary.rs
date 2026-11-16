//! Ordinary transfer keeps its original metadata and actual recovered writer.
use super::custody::{CandidateCustody, OwnerCustody};
use super::prefix::{guard, io};
use super::*;
use crate::transfer::MAX_HISTORY_RECORD_BYTES;

impl LearnerHistoryStore {
    pub fn open(
        path: impl AsRef<Path>,
        pg: PlacementGroupId,
        local: NodeId,
        source: NodeId,
    ) -> RsfResult<Self> {
        if local == source {
            return Err(invalid_input("learner and source must differ"));
        }
        let root = io::normalized(path.as_ref())?;
        rsf_storage::create_dir_all_synced(&root).map_err(io_error)?;
        let root_identity = guard::directory(&root)?;
        // Existing ACTIVE is already acknowledged custody. Even this generic
        // constructor must not recreate its missing exclusion sidecar.
        let active = optional(&root.join("ACTIVE"), 32)?;
        let lock = io::lock(&root.join(".learner.lock"), active.is_none())?;
        let lock_guard = guard::owned_lock(&root.join(".learner.lock"), &lock)?;
        let mut expected = b"RSFL1".to_vec();
        for value in [pg.0, local.0, source.0] {
            expected.extend_from_slice(&value.to_le_bytes());
        }
        let identity = match optional(&root.join("IDENTITY"), expected.len())? {
            Some((bytes, exact)) => {
                if bytes != expected {
                    return Err(invalid_input(
                        "learner store identity differs from configuration",
                    ));
                }
                exact
            }
            None => {
                for entry in fs::read_dir(&root).map_err(io_error)? {
                    let entry = entry.map_err(io_error)?;
                    if entry.file_name() != ".learner.lock" && entry.file_name() != ".IDENTITY.tmp"
                    {
                        return Err(corrupt("nonempty learner store has no identity"));
                    }
                }
                lock_guard.validate()?;
                if guard::directory(&root)? != root_identity {
                    return Err(corrupt("learner root changed before identity publication"));
                }
                io::prepare_publication(&root, "IDENTITY", &expected)?.publish_guarded()?
            }
        };
        let owner_custody = OwnerCustody::from_original(
            root.clone(),
            root_identity,
            identity,
            lock_guard,
            active.as_ref().map(|(_, exact)| exact.clone()),
        );
        owner_custody.validate(&root, &lock)?;
        let mut learner = Self {
            root,
            pg,
            local,
            source,
            stage: None,
            published: None,
            failed: AtomicBool::new(false),
            prefix_context: None,
            owner_custody,
            _lock: lock,
        };
        if let Some((bytes, _)) = active {
            let id: [u8; 32] = bytes
                .try_into()
                .map_err(|_| corrupt("invalid active learner history id"))?;
            let directory = learner.directory(&id);
            let (bytes, exact) = io::read(&directory.join("DESCRIPTOR"), io::DESCRIPTOR_BYTES)?;
            let descriptor = VsrHistoryDescriptor::decode(&bytes)?;
            learner.validate_descriptor(&descriptor)?;
            if descriptor.id()? != id {
                return Err(corrupt("active learner descriptor identity mismatch"));
            }
            let custody = CandidateCustody::capture(&directory, &descriptor)?;
            custody.require_writer()?;
            // A published snapshot is immutable. Its existing format limits
            // bound this strict writer open, not a new caller policy or cap.
            let limits = ordinary_limits(descriptor.records)?;
            let (store, recovered) = SegmentStore::open_existing_recovered_bounded(
                directory.join("cell"),
                Default::default(),
                limits,
                |_| Ok(()),
            )
            .map_err(prefix::stage::storage_error)?;
            custody.validate()?;
            exact.validate()?;
            learner
                .owner_custody
                .validate(&learner.root, &learner._lock)?;
            let expected = store.capture_owned_custody()?;
            let mut stage =
                learner.stage_from_recovery(descriptor, store, &recovered.recovery, custody)?;
            let replayed = learner.replay_ordinary(&stage, &recovered.recovery);
            stage.store.validate_owned_custody(&expected)?;
            stage.custody.validate()?;
            learner
                .owner_custody
                .validate(&learner.root, &learner._lock)?;
            let replayed = replayed?;
            learner.renew_recovered_stage(&mut stage, &expected)?;
            let published =
                learner.complete_ordinary(stage, recovered.recovery, replayed, false, &expected)?;
            exact.validate()?;
            learner
                .owner_custody
                .validate(&learner.root, &learner._lock)?;
            learner.published = Some(published);
            learner.published_state_token()?;
        }
        learner.healthy()?;
        Ok(learner)
    }

    /// Publish only the exact recovered store, after semantic extension checks.
    pub fn finish(&mut self, id: [u8; 32]) -> RsfResult<LogIndex> {
        self.healthy()?;
        self.require_ordinary_transfer()?;
        if let Some(published) = &self.published
            && published.id == id
        {
            let tail = published.cell.committed_tail();
            self.reclaim_unreferenced()?;
            self.healthy()?;
            return Ok(tail);
        }
        let stage = self
            .stage
            .as_ref()
            .ok_or_else(|| invalid_input("no staged learner transfer"))?;
        if stage.descriptor.id()? != id {
            return Err(invalid_input("learner finish snapshot mismatch"));
        }
        if stage.count != stage.descriptor.records {
            return Err(invalid_input("learner transfer is incomplete"));
        }
        self.failed.store(true, Ordering::Relaxed);
        let stage = self.stage.take().expect("validated actual stage");
        stage.custody.validate()?;
        let expected = stage.store.capture_owned_custody()?;
        self.validate_ordinary_anchor()?;
        let readback = stage.store.recover_owned_immutable_bounded(
            &expected,
            ordinary_limits(stage.descriptor.records)?,
            |_| Ok(()),
        );
        // Original physical failures dominate scan/limit/semantic errors too.
        stage.store.validate_owned_custody(&expected)?;
        stage.custody.validate()?;
        self.validate_ordinary_anchor()?;
        let recovery = readback.map_err(prefix::stage::storage_error)?.recovery;
        let folded = fold(self.pg, &stage.descriptor, &recovery);
        stage.store.validate_owned_custody(&expected)?;
        stage.custody.validate()?;
        self.validate_ordinary_anchor()?;
        let (count, digest, previous_hash) = match folded {
            Ok(values) => values,
            Err(error) => {
                self.failed.store(false, Ordering::Relaxed);
                return Err(error);
            }
        };
        let mut stage = stage;
        stage.count = count;
        stage.digest = digest;
        stage.previous_hash = previous_hash;
        let replayed = self.replay_ordinary(&stage, &recovery);
        stage.store.validate_owned_custody(&expected)?;
        stage.custody.validate()?;
        self.validate_ordinary_anchor()?;
        let replayed = match replayed {
            Ok(replayed) => replayed,
            Err(error) => {
                // Pure semantic rejection follows successful original custody
                // checks; no publication or writer-lock transition has begun.
                self.failed.store(false, Ordering::Relaxed);
                return Err(error);
            }
        };
        let candidate = self.complete_ordinary(stage, recovery, replayed, true, &expected)?;
        self.validate_ordinary_anchor()?;
        candidate.custody.validate_cell(&candidate.cell)?;
        let semantic = self
            .published
            .as_ref()
            .map_or(Ok(()), |old| prefix::stage::preserves(old, &candidate));
        candidate.custody.validate_cell(&candidate.cell)?;
        self.validate_ordinary_anchor()?;
        if let Err(error) = semantic {
            self.failed.store(false, Ordering::Relaxed);
            return Err(error);
        }
        let publication = io::prepare_publication(&self.root, "ACTIVE", &id)?;
        self.validate_ordinary_anchor()?;
        candidate.custody.validate_cell(&candidate.cell)?;
        let active = publication.publish_guarded()?;
        self.owner_custody.published(active);
        let tail = candidate.cell.committed_tail();
        let retired = self.published.replace(candidate);
        self.validate_ordinary_anchor()?;
        drop(retired);
        self.reclaim_unreferenced()?;
        self.validate_ordinary_anchor()?;
        self.failed.store(false, Ordering::Relaxed);
        Ok(tail)
    }

    pub(super) fn validate_ordinary_anchor(&self) -> RsfResult<()> {
        self.owner_custody.validate(&self.root, &self._lock)?;
        if let Some(published) = &self.published {
            self.validate_published_custody(published)?;
        }
        Ok(())
    }
    pub(super) fn recover_stage(
        &self,
        descriptor: VsrHistoryDescriptor,
        custody: CandidateCustody,
    ) -> RsfResult<Stage> {
        custody.validate()?;
        let (store, recovery) = SegmentStore::open_recovered(
            self.directory(&descriptor.id()?).join("cell"),
            Default::default(),
        )?;
        custody.validate()?;
        let expected = store.capture_owned_custody()?;
        let folded = self.stage_from_recovery(descriptor, store, &recovery.recovery, custody);
        self.validate_ordinary_anchor()?;
        let mut stage = folded?;
        self.renew_recovered_stage(&mut stage, &expected)?;
        Ok(stage)
    }

    // Recovered visible bytes may follow an unsuccessful earlier data sync.
    // Renew the original writer before a progress ACK or cold publication owner
    // escapes. This does not run on healthy warm tokens or duplicate replies.
    pub(in crate::learner) fn renew_recovered_stage(
        &self,
        stage: &mut Stage,
        expected: &rsf_storage::SegmentStoreCustody,
    ) -> RsfResult<()> {
        self.validate_ordinary_anchor()?;
        stage.custody.validate()?;
        stage.store.validate_owned_custody(expected)?;
        let renewed = prefix::stage::renew(&mut stage.store, expected);
        // Attempt the original store check even on failure. A sync error has
        // already fenced that writer; preserve its first error after checking
        // independent original namespace/anchor custody, rather than replacing
        // it with the consequent storage writer fence.
        let physical = stage.store.validate_owned_custody(expected);
        stage.custody.validate()?;
        self.validate_ordinary_anchor()?;
        renewed?;
        physical
    }
    fn stage_from_recovery(
        &self,
        descriptor: VsrHistoryDescriptor,
        store: SegmentStore,
        recovery: &rsf_storage::Recovery,
        custody: CandidateCustody,
    ) -> RsfResult<Stage> {
        let result = fold(self.pg, &descriptor, recovery);
        custody.validate()?;
        let (count, digest, previous_hash) = result?;
        Ok(Stage {
            descriptor,
            store,
            count,
            digest,
            previous_hash,
            custody,
        })
    }
    fn replay_ordinary(
        &self,
        stage: &Stage,
        recovery: &rsf_storage::Recovery,
    ) -> RsfResult<ReplayedVsrStore> {
        if stage.count != stage.descriptor.records || stage.digest != stage.descriptor.digest {
            return Err(corrupt("learner history count or digest mismatch"));
        }
        let replayed = DurableVsrCell::replay_store(recovery, self.pg, || {
            VsrCell::new(
                self.pg,
                stage.descriptor.probe.view,
                stage.descriptor.probe.primary,
                stage.descriptor.probe.voters.clone(),
            )
            .map_err(invalid_input)
        })?;
        if !replayed.bootstrap_persisted
            || installation::probe_for_cell(&replayed.recovered.0)? != stage.descriptor.probe
        {
            return Err(corrupt(
                "recovered learner history differs from source probe",
            ));
        }
        Ok(replayed)
    }
    fn complete_ordinary(
        &self,
        stage: Stage,
        recovery: rsf_storage::Recovery,
        replayed: ReplayedVsrStore,
        create: bool,
        expected: &rsf_storage::SegmentStoreCustody,
    ) -> RsfResult<Published> {
        let Stage {
            descriptor,
            store,
            mut custody,
            ..
        } = stage;
        custody.validate()?;
        store.validate_owned_custody(expected)?;
        let path = store.root().to_path_buf();
        let writer_lock = custody.acquire_writer(create)?;
        let active = installation::publication::active(&path, self.pg)?;
        // This actual learner wrote and verified the root store. Promotion uses
        // a separate target; a pointer cannot substitute another store here.
        if active.path != path {
            return Err(corrupt(
                "learner active generation differs from retained transfer store",
            ));
        }
        store.validate_owned_custody(expected)?;
        custody.validate()?;
        let cell = DurableVsrCell::from_replayed_store_with(
            OpenedVsrStore {
                root: path,
                active,
                store,
                recovery,
                options: Default::default(),
                writer_lock,
            },
            self.pg,
            replayed,
            |root, active, store| {
                custody.validate()?;
                store.validate_owned_custody(expected)?;
                if active.plan.is_some() {
                    installation::publication::mark_published(root, self.pg)?;
                }
                custody.validate()?;
                store.validate_owned_custody(expected)
            },
        )?;
        cell.store.validate_owned_custody(expected)?;
        custody.validate_cell(&cell)?;
        if cell.primary_probe()? != descriptor.probe || !cell.bootstrap_persisted {
            return Err(corrupt(
                "recovered learner history differs from source probe",
            ));
        }
        Ok(Published {
            bounded_flat: None,
            id: descriptor.id()?,
            descriptor,
            cell,
            custody,
        })
    }
}

fn optional(path: &Path, maximum: usize) -> RsfResult<Option<(Vec<u8>, guard::Exact)>> {
    match fs::symlink_metadata(path) {
        Ok(_) => io::read(path, maximum).map(Some),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(io_error(error)),
    }
}
pub(super) fn ordinary_limits(records: u64) -> RsfResult<rsf_storage::SegmentReadLimits> {
    let max_record_bytes = rsf_storage::wal_record_disk_bytes(MAX_HISTORY_RECORD_BYTES as u64)?;
    let max_encoded_bytes = records
        .checked_mul(max_record_bytes)
        .ok_or_else(|| invalid_input("learner physical history bound overflow"))?;
    Ok(rsf_storage::SegmentReadLimits {
        max_manifest_bytes: rsf_storage::append_only_manifest_byte_bound(records)?,
        max_records: records,
        max_encoded_bytes,
        max_record_bytes,
    })
}

fn fold(
    pg: PlacementGroupId,
    descriptor: &VsrHistoryDescriptor,
    recovery: &rsf_storage::Recovery,
) -> RsfResult<(u64, Digest, [u8; 32])> {
    let mut digest = Digest::blake3_bytes(b"RSF history chain v1");
    let mut previous_hash = [0; 32];
    let count = recovery.wal_records.len() as u64;
    if count > descriptor.records {
        return Err(corrupt("learner history exceeds declared record count"));
    }
    for (position, record) in recovery.wal_records.iter().enumerate() {
        if record.pg != pg || record.index.0 != position as u64 + 1 {
            return Err(corrupt(
                "learner history has foreign or noncontiguous records",
            ));
        }
        digest = transfer::extend_digest(
            digest,
            &VsrHistoryRecord {
                epoch: record.epoch,
                kind: record.kind,
                payload: record.payload.to_vec(),
            },
        )?;
        previous_hash = record.record_hash;
    }
    Ok((count, digest, previous_hash))
}
