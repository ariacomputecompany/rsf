use super::*;
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};

/// Configured, non-voting upload inbox. Complete replay returns old-quorum
/// approval; the caller must anchor it to the hosted voter/learner before opening
/// a decision store. An incomplete descriptor never grants a round promise.
#[derive(Debug)]
pub struct MembershipAuthorityInbox {
    root: PathBuf,
    pg: PlacementGroupId,
    local: NodeId,
    journal: SegmentStore,
    selected: Option<VsrMembershipInput>,
    failed: bool,
    _lock: rsf_storage::ExclusiveFileLock,
}
impl MembershipAuthorityInbox {
    pub fn open(path: impl AsRef<Path>, pg: PlacementGroupId, local: NodeId) -> RsfResult<Self> {
        Self::open_inner(path.as_ref(), pg, local, false)
    }
    /// Reopen enrolled custody without creating a root, lock or identity.
    /// A missing or torn-to-zero identity journal cannot become a new inbox.
    pub fn open_existing(
        path: impl AsRef<Path>,
        pg: PlacementGroupId,
        local: NodeId,
    ) -> RsfResult<Self> {
        Self::open_inner(path.as_ref(), pg, local, true)
    }
    fn open_inner(
        path: &Path,
        pg: PlacementGroupId,
        local: NodeId,
        existing: bool,
    ) -> RsfResult<Self> {
        let root = path.to_path_buf();
        if existing {
            for path in [&root, &root.join("metadata")] {
                if !std::fs::symlink_metadata(path)
                    .map_err(io_error)?
                    .file_type()
                    .is_dir()
                {
                    return Err(corrupt(
                        "membership inbox custody is not an owned directory",
                    ));
                }
            }
            for path in [root.join(".inbox.lock"), root.join("metadata/MANIFEST")] {
                if !std::fs::symlink_metadata(path)
                    .map_err(io_error)?
                    .file_type()
                    .is_file()
                {
                    return Err(corrupt(
                        "membership inbox identity or lock is not a regular file",
                    ));
                }
            }
        } else {
            rsf_storage::create_dir_all_synced(&root).map_err(io_error)?;
        }
        let lock = rsf_storage::ExclusiveFileLock::acquire(
            OpenOptions::new()
                .create(!existing)
                .truncate(false)
                .write(true)
                .open(root.join(".inbox.lock"))
                .map_err(io_error)?,
        )
        .map_err(|e| invalid_input(format!("membership inbox already owned: {e}")))?;
        if root.join("inputs").exists() && !root.join("metadata/MANIFEST").is_file() {
            return Err(corrupt("membership inbox lost its identity journal"));
        }
        if existing
            && SegmentStore::recover_existing(root.join("metadata"))?
                .recovery
                .wal_records
                .is_empty()
        {
            return Err(corrupt("membership inbox identity journal is empty"));
        }
        let mut options = SegmentStoreOptions::default();
        options.log_options.sync_policy = rsf_storage::SyncPolicy::Full;
        let (journal, recovered) = SegmentStore::open_recovered(root.join("metadata"), options)?;
        let mut selected = None;
        for (n, record) in recovered.recovery.wal_records.iter().enumerate() {
            let mut r = Reader::new(&record.payload, b"VSRMBX1")?;
            if r.u64()? != pg.0
                || r.u64()? != local.0
                || record.pg != pg
                || record.epoch != Epoch(0)
                || record.index != LogIndex(n as u64 + 1)
                || record.kind != WalRecordKind::Membership
            {
                return Err(corrupt("membership inbox identity or envelope mismatch"));
            }
            selected = match r.byte()? {
                0 if n == 0 => None,
                1 if n > 0 => Some(VsrMembershipInput::decode(r.bytes()?)?),
                _ => return Err(corrupt("invalid membership inbox selection")),
            };
            r.finish()?;
            if let Some(input) = &selected {
                validate(input, pg, local)?;
            }
        }
        let mut inbox = Self {
            root,
            pg,
            local,
            journal,
            selected,
            failed: false,
            _lock: lock,
        };
        if recovered.recovery.wal_records.is_empty() {
            if existing {
                return Err(corrupt("membership inbox identity journal is empty"));
            }
            inbox.persist(None)?;
        }
        if let Some(input) = &inbox.selected {
            let (retained, _) = disk::read_records(&inbox.root, input.id()?)?;
            if retained != *input {
                return Err(corrupt("membership inbox selected descriptor changed"));
            }
            // Reopen transfers selected custody to this owner before append ACKs.
            disk::sync_input_publication(&inbox.root, input.id()?)?;
        }
        Ok(inbox)
    }
    pub fn selected_input(&self) -> Option<&VsrMembershipInput> {
        self.selected.as_ref()
    }
    pub fn begin(&mut self, sender: NodeId, input: VsrMembershipInput) -> RsfResult<u64> {
        self.healthy()?;
        validate(&input, self.pg, self.local)?;
        authorize(sender, &input)?;
        if let Some(previous) = &self.selected {
            let (retained, records) = disk::read_records(&self.root, previous.id()?)?;
            if retained != *previous {
                return Err(corrupt("membership authority descriptor changed"));
            }
            if previous == &input {
                // An existing selected upload needs fresh byte validation and
                // the complete input publication barrier, not a new writer.
                disk::sync_input_publication(&self.root, input.id()?)?;
                return Ok(records.len() as u64);
            }
        }
        disk::create_input(&self.root, &input)?;
        let (_, records) = disk::read_records(&self.root, input.id()?)?;
        if self.selected.as_ref() != Some(&input) {
            self.persist(Some(input))?;
        }
        Ok(records.len() as u64)
    }
    pub fn append(
        &mut self,
        sender: NodeId,
        id: [u8; 32],
        batch: VsrHistoryBatch,
    ) -> RsfResult<u64> {
        self.healthy()?;
        let input = self
            .selected
            .as_ref()
            .ok_or_else(|| invalid_input("begin membership authority upload first"))?;
        authorize(sender, input)?;
        if input.id()? != id {
            return Err(invalid_input(
                "membership authority upload identity mismatch",
            ));
        }
        disk::append_records(&self.root, &mut self.failed, input, batch)
    }
    pub fn finish(&self, sender: NodeId, id: [u8; 32]) -> RsfResult<VsrMembershipApproval> {
        self.healthy()?;
        let input = self
            .selected
            .as_ref()
            .ok_or_else(|| invalid_input("membership authority upload is absent"))?;
        authorize(sender, input)?;
        if input.id()? != id {
            return Err(invalid_input(
                "membership authority upload identity mismatch",
            ));
        }
        let (retained, records) = disk::read_records(&self.root, id)?;
        if retained != *input {
            return Err(corrupt("membership authority descriptor changed"));
        }
        let approval = input.validate_records(&records)?;
        // Finished nonselected input is retained too: renew it at the usable
        // proof boundary, not on every metadata/record read or by scanning IDs.
        disk::sync_input_publication(&self.root, id)?;
        Ok(approval)
    }
    /// Read a complete retained input independently of the current upload
    /// selection. A different proposed voter may continue a chosen transition
    /// after another upload selected A over already-finished B. This only
    /// validates and renews retained proof bytes; the caller must still anchor the approval to its
    /// hosted history and import the matching chosen certificate.
    pub fn retained_approval(
        &self,
        sender: NodeId,
        id: [u8; 32],
    ) -> RsfResult<VsrMembershipApproval> {
        self.healthy()?;
        let (input, records) = disk::read_records(&self.root, id)?;
        validate(&input, self.pg, self.local)?;
        authorize(sender, &input)?;
        let approval = input.validate_records(&records)?;
        // Finished nonselected input is retained too: renew it at the usable
        // proof boundary, not on every metadata/record read or by scanning IDs.
        disk::sync_input_publication(&self.root, id)?;
        Ok(approval)
    }
    fn persist(&mut self, selected: Option<VsrMembershipInput>) -> RsfResult<()> {
        let mut bytes = b"VSRMBX1".to_vec();
        put_u64(&mut bytes, self.pg.0);
        put_u64(&mut bytes, self.local.0);
        bytes.push(u8::from(selected.is_some()));
        if let Some(input) = &selected {
            put_bytes(&mut bytes, &input.encode()?)?;
        }
        if bytes.len() > MAX_HISTORY_RECORD_BYTES {
            return Err(invalid_input("membership inbox state exceeds limit"));
        }
        match self
            .journal
            .append_next_wal(self.pg, Epoch(0), WalRecordKind::Membership, &bytes)
        {
            Ok(_) => {
                self.selected = selected;
                Ok(())
            }
            Err(error) => {
                self.failed = true;
                Err(error)
            }
        }
    }
    fn healthy(&self) -> RsfResult<()> {
        if self.failed {
            return Err(corrupt(
                "membership inbox durability failed; reopen before further use",
            ));
        }
        Ok(())
    }
}
fn validate(input: &VsrMembershipInput, pg: PlacementGroupId, local: NodeId) -> RsfResult<()> {
    input.encode()?;
    if input.proposal().election.pg != pg || !input.proposal().new_voters.contains(&local) {
        return Err(invalid_input(
            "membership authority input differs from configured group or proposed local voter",
        ));
    }
    Ok(())
}
fn authorize(sender: NodeId, input: &VsrMembershipInput) -> RsfResult<()> {
    if !input.proposal().new_voters.contains(&sender) {
        return Err(invalid_input(
            "membership authority uploader is not a proposed voter",
        ));
    }
    Ok(())
}
fn io_error(error: std::io::Error) -> RsfError {
    corrupt(format!("membership authority inbox: {error}"))
}
