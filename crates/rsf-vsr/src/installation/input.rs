//! Durable, restartable recovery inputs. Staging never grants voting authority.
use super::*;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;

/// Compact quorum reports plus the identity of the independently replayed plan.
/// Authenticate the candidate before accepting this descriptor or its records.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VsrInstallationInput {
    reports: Vec<VsrViewChangeReport>,
    plan: [u8; 32],
}

impl VsrInstallationInput {
    pub fn from_snapshots(snapshots: &[VsrViewChangeSnapshot]) -> RsfResult<Self> {
        let plan = VsrViewInstallation::select(snapshots)?;
        let mut reports = snapshots
            .iter()
            .map(|s| s.report.clone())
            .collect::<Vec<_>>();
        reports.sort_by_key(|r| r.replica);
        let input = Self {
            reports,
            plan: plan.id(),
        };
        input.encode()?;
        Ok(input)
    }
    pub fn request(&self) -> &VsrViewChangeRequest {
        &self.reports[0].request
    }
    pub fn reports(&self) -> &[VsrViewChangeReport] {
        &self.reports
    }
    pub fn records(&self) -> RsfResult<u64> {
        self.reports.iter().try_fold(0u64, |count, report| {
            count
                .checked_add(report.history.records)
                .ok_or_else(|| invalid_input("installation input record count overflow"))
        })
    }
    pub fn id(&self) -> RsfResult<[u8; 32]> {
        Ok(Digest::blake3_bytes(&self.encode()?).bytes)
    }
    pub fn encode(&self) -> RsfResult<Vec<u8>> {
        let source = self
            .reports
            .iter()
            .max_by_key(|r| {
                (
                    r.history.probe.view,
                    r.accepted_tail,
                    std::cmp::Reverse(r.replica),
                )
            })
            .ok_or_else(|| invalid_input("installation input has no reports"))?
            .replica;
        self.records()?;
        let marker = Installation {
            reports: self.reports.clone(),
            source,
            delta_count: 0,
            delta_digest: transfer::history_digest(&[])?,
        };
        let mut bytes = b"VSIB1".to_vec();
        bytes.extend_from_slice(&self.plan);
        bytes.extend_from_slice(&marker.encode()?);
        Ok(bytes)
    }
    pub fn decode(bytes: &[u8]) -> RsfResult<Self> {
        if bytes.len() < 37
            || !bytes.starts_with(b"VSIB1")
            || bytes.len() > transfer::MAX_HISTORY_RECORD_BYTES
        {
            return Err(corrupt("invalid installation input descriptor"));
        }
        let marker = Installation::decode(&bytes[37..])?;
        let input = Self {
            reports: marker.reports,
            plan: bytes[5..37].try_into().unwrap(),
        };
        if input.encode()? != bytes {
            return Err(corrupt("noncanonical installation input descriptor"));
        }
        Ok(input)
    }
}

#[derive(Debug)]
pub(crate) struct Stage {
    descriptor: VsrInstallationInput,
    store: SegmentStore,
    count: u64,
    failed: bool,
}

impl DurableVsrCell {
    /// Sync the local promise and input identity before acknowledging any upload.
    /// An exact retry after publication is allowed; a newer promise fences it.
    pub fn begin_installation_input(
        &mut self,
        replica: NodeId,
        sender: NodeId,
        descriptor: VsrInstallationInput,
    ) -> RsfResult<u64> {
        self.ensure_healthy()?;
        if self.membership_fence.is_some() {
            return Err(invalid_input(
                "ordinary installation input cannot replace a membership transition",
            ));
        }
        let request = descriptor.request();
        descriptor.encode()?;
        if request.pg != self.cell.pg
            || sender != request.candidate
            || !request.voters.contains(&replica)
            || !self.contains_replica(replica)
        {
            return Err(invalid_input(
                "installation input requires the authenticated candidate and a local voter",
            ));
        }
        let installed = self.installed_plan.is_some()
            && self.cell.view == request.target_view
            && self.cell.primary == sender
            && self.cell.replicas.iter().copied().collect::<Vec<_>>() == request.voters
            && self.view_change.is_none();
        if !installed && self.view_change.as_ref() != Some(request) {
            self.begin_view_change(replica, sender, request.clone())?;
        }
        let id = descriptor.id()?;
        if let Some(stage) = &self.installation_input
            && stage.descriptor == descriptor
            && !stage.failed
        {
            return Ok(stage.count);
        }
        self.installation_input = None;
        let base = self.root.join("vsr-install-input");
        fs::create_dir_all(&base).map_err(io_error)?;
        sync(&self.root)?;
        let directory = base.join(hex(&id));
        fs::create_dir_all(&directory).map_err(io_error)?;
        sync(&base)?;
        let encoded = descriptor.encode()?;
        match fs::read(directory.join("DESCRIPTOR")) {
            Ok(bytes) if bytes == encoded => {}
            Ok(_) => return Err(corrupt("installation input descriptor identity mismatch")),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                atomic(&directory, "DESCRIPTOR", &encoded)?
            }
            Err(err) => return Err(io_error(err)),
        }
        if fs::read(self.root.join("VSR_INSTALL_INPUT")).is_ok_and(|pointer| pointer == id)
            && !directory.join("records/MANIFEST").is_file()
        {
            return Err(corrupt(
                "selected installation input record store is missing",
            ));
        }
        let stage = self.recover_installation_input(descriptor)?;
        sync(&directory)?;
        atomic(&self.root, "VSR_INSTALL_INPUT", &id)?;
        let count = stage.count;
        self.installation_input = Some(stage);
        reclaim_inputs(&base, &directory)?;
        Ok(count)
    }

    /// Recover the last durably selected input after either endpoint restarts.
    pub fn installation_input(&mut self) -> RsfResult<Option<VsrInstallationInput>> {
        self.ensure_healthy()?;
        let id = match fs::read(self.root.join("VSR_INSTALL_INPUT")) {
            Ok(bytes) => <[u8; 32]>::try_from(bytes)
                .map_err(|_| corrupt("invalid installation input pointer"))?,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(io_error(err)),
        };
        self.load_installation_input(id)?;
        Ok(Some(
            self.installation_input.as_ref().unwrap().descriptor.clone(),
        ))
    }

    pub fn append_installation_input(
        &mut self,
        sender: NodeId,
        id: [u8; 32],
        ordinal: u64,
        record: VsrHistoryRecord,
    ) -> RsfResult<u64> {
        self.append_installation_batch(
            sender,
            id,
            crate::VsrHistoryBatch {
                ordinal,
                records: vec![record],
            },
        )
    }

    /// Acknowledge a bounded batch only after its single outer WAL record and
    /// full sync succeed. Recovery exposes its entire logical prefix or none.
    pub fn append_installation_batch(
        &mut self,
        sender: NodeId,
        id: [u8; 32],
        batch: crate::VsrHistoryBatch,
    ) -> RsfResult<u64> {
        self.ensure_healthy()?;
        self.load_installation_input(id)?;
        self.authorize_installation_input(sender)?;
        let mut encoded = batch.encode()?;
        let stage = self.installation_input.as_mut().unwrap();
        if stage.failed {
            return Err(invalid_input(
                "restart installation input after a durability failure",
            ));
        }
        let end = batch
            .ordinal
            .checked_add(batch.records.len() as u64)
            .ok_or_else(|| invalid_input("installation batch range overflow"))?;
        if batch.ordinal > stage.count || end > stage.descriptor.records()? {
            return Err(invalid_input(
                "installation input ordinal is outside durable prefix",
            ));
        }
        let overlap = stage
            .count
            .saturating_sub(batch.ordinal)
            .min(batch.records.len() as u64) as usize;
        if overlap > 0 {
            let durable = decode_staged_records(
                &stage.store.recover_current()?.recovery.wal_records,
                &stage.descriptor,
            )?;
            if durable.get(batch.ordinal as usize..batch.ordinal as usize + overlap)
                != Some(&batch.records[..overlap])
            {
                return Err(corrupt(
                    "installation input duplicate differs from durable record",
                ));
            }
            if end <= stage.count {
                return Ok(stage.count);
            }
            encoded = crate::VsrHistoryBatch {
                ordinal: stage.count,
                records: batch.records[overlap..].to_vec(),
            }
            .encode()?;
        }
        if let Err(err) = stage.store.append_next_wal(
            stage.descriptor.request().pg,
            stage.descriptor.request().target_view,
            WalRecordKind::ViewInstallation,
            &encoded,
        ) {
            stage.failed = true;
            return Err(err);
        }
        stage.count = end;
        Ok(stage.count)
    }

    /// Return full frozen histories for independently validating or resending an
    /// already staged input. This does not publish the selected view.
    pub fn installation_snapshots(
        &mut self,
        id: [u8; 32],
    ) -> RsfResult<Vec<VsrViewChangeSnapshot>> {
        self.ensure_healthy()?;
        self.load_installation_input(id)?;
        let stage = self.installation_input.as_mut().unwrap();
        if stage.failed || stage.count != stage.descriptor.records()? {
            return Err(invalid_input("installation input is incomplete"));
        }
        let records = decode_staged_records(
            &stage.store.recover_current()?.recovery.wal_records,
            &stage.descriptor,
        )?;
        let mut records = records.into_iter();
        let mut snapshots = Vec::new();
        for report in &stage.descriptor.reports {
            let mut history = Vec::new();
            for _ in 0..report.history.records {
                let record = records
                    .next()
                    .ok_or_else(|| corrupt("installation input record disappeared"))?;
                history.push(record);
            }
            snapshots.push(VsrViewChangeSnapshot {
                report: report.clone(),
                records: history,
            });
        }
        let plan = VsrViewInstallation::select(&snapshots)?;
        if plan.id() != stage.descriptor.plan {
            return Err(corrupt(
                "installation input selected plan identity mismatch",
            ));
        }
        Ok(snapshots)
    }

    pub fn finish_installation_input(&mut self, sender: NodeId, id: [u8; 32]) -> RsfResult<Epoch> {
        self.ensure_healthy()?;
        self.load_installation_input(id)?;
        self.authorize_installation_input(sender)?;
        let stage = self.installation_input.as_ref().unwrap();
        if !stage.failed
            && self.view_change.is_none()
            && stage.count == stage.descriptor.records()?
            && self.installed_plan == Some(stage.descriptor.plan)
        {
            // Authorization already checked the exact installed view, candidate,
            // voters and absence of a newer promise. The published generation is
            // authoritative; a lost acknowledgement needs no second selection.
            self.ensure_active_view()?;
            return Ok(self.cell.view);
        }
        let snapshots = self.installation_snapshots(id)?;
        let plan = VsrViewInstallation::select(&snapshots)?;
        self.install_view(&plan)?;
        Ok(self.cell.view)
    }

    fn authorize_installation_input(&self, sender: NodeId) -> RsfResult<()> {
        let request = self
            .installation_input
            .as_ref()
            .unwrap()
            .descriptor
            .request();
        if sender != request.candidate
            || (self.view_change.as_ref() != Some(request)
                && !(self.installed_plan.is_some()
                    && self.cell.view == request.target_view
                    && self.cell.primary == sender
                    && self.view_change.is_none()
                    && self.cell.replicas.iter().copied().collect::<Vec<_>>() == request.voters))
        {
            return Err(invalid_input(
                "installation input no longer matches candidate or active promise",
            ));
        }
        Ok(())
    }
    fn load_installation_input(&mut self, id: [u8; 32]) -> RsfResult<()> {
        if self
            .installation_input
            .as_ref()
            .is_some_and(|s| s.descriptor.id().ok() == Some(id))
        {
            return Ok(());
        }
        let pointer = fs::read(self.root.join("VSR_INSTALL_INPUT")).map_err(io_error)?;
        if pointer != id {
            return Err(invalid_input(
                "installation input is not the selected durable upload",
            ));
        }
        if !self.input_directory(&id).join("records/MANIFEST").is_file() {
            return Err(corrupt(
                "selected installation input record store is missing",
            ));
        }
        let bytes = fs::read(self.input_directory(&id).join("DESCRIPTOR")).map_err(io_error)?;
        let descriptor = VsrInstallationInput::decode(&bytes)?;
        if descriptor.id()? != id {
            return Err(corrupt("installation input identity mismatch"));
        }
        self.installation_input = Some(self.recover_installation_input(descriptor)?);
        Ok(())
    }
    fn input_directory(&self, id: &[u8; 32]) -> PathBuf {
        self.root.join("vsr-install-input").join(hex(id))
    }
    fn recover_installation_input(&self, descriptor: VsrInstallationInput) -> RsfResult<Stage> {
        let mut options = self.options;
        options.log_options.sync_policy = rsf_storage::SyncPolicy::Full;
        let (store, recovered) = SegmentStore::open_recovered(
            self.input_directory(&descriptor.id()?).join("records"),
            options,
        )?;
        let recovered = recovered.recovery;
        let records = decode_staged_records(&recovered.wal_records, &descriptor)?;
        let count = records.len() as u64;
        if count > descriptor.records()? {
            return Err(corrupt("installation input exceeds descriptor"));
        }
        Ok(Stage {
            descriptor,
            store,
            count,
            failed: false,
        })
    }
}
fn decode_staged_records(
    records: &[rsf_core::storage::WalRecord],
    descriptor: &VsrInstallationInput,
) -> RsfResult<Vec<VsrHistoryRecord>> {
    let mut history = Vec::new();
    for (physical, record) in records.iter().enumerate() {
        if record.index != LogIndex(physical as u64 + 1)
            || record.pg != descriptor.request().pg
            || record.epoch != descriptor.request().target_view
            || record.kind != WalRecordKind::ViewInstallation
        {
            return Err(corrupt("invalid installation input WAL envelope"));
        }
        if record.payload.starts_with(b"RSFH1") {
            history.push(VsrHistoryRecord::decode(&record.payload)?);
        } else {
            let batch = crate::VsrHistoryBatch::decode(&record.payload)?;
            if batch.ordinal != history.len() as u64 {
                return Err(corrupt("installation batch reorders durable history"));
            }
            history.extend(batch.records);
        }
        if history.len() as u64 > descriptor.records()? {
            return Err(corrupt("installation input exceeds descriptor"));
        }
    }
    Ok(history)
}

fn hex(id: &[u8; 32]) -> String {
    id.iter().map(|b| format!("{b:02x}")).collect()
}
fn sync(path: &Path) -> RsfResult<()> {
    File::open(path)
        .and_then(|f| f.sync_all())
        .map_err(io_error)
}
fn atomic(directory: &Path, name: &str, bytes: &[u8]) -> RsfResult<()> {
    let temporary = directory.join(format!(".{name}.tmp"));
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&temporary)
        .map_err(io_error)?;
    file.write_all(bytes)
        .and_then(|_| file.sync_all())
        .map_err(io_error)?;
    fs::rename(temporary, directory.join(name)).map_err(io_error)?;
    sync(directory)
}

fn io_error(err: std::io::Error) -> RsfError {
    corrupt(format!("VSR installation input storage: {err}"))
}

fn reclaim_inputs(base: &Path, current: &Path) -> RsfResult<()> {
    for entry in fs::read_dir(base).map_err(io_error)? {
        let entry = entry.map_err(io_error)?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if entry.path() != current
            && name.len() == 64
            && name
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
            && entry.file_type().map_err(io_error)?.is_dir()
        {
            fs::remove_dir_all(entry.path()).map_err(io_error)?;
        }
    }
    sync(base)
}
