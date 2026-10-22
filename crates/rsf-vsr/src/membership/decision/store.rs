use super::state::{DecodedState, State, decode_state, encode_state, validate_transition};
use super::*;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

/// A separate transition acceptor. No method exposes a writable application
/// cell, a primary probe, a commit acknowledgement or a promoted learner.
#[derive(Debug)]
pub struct MembershipDecisionStore {
    root: PathBuf,
    local: NodeId,
    proposal: VsrMembershipProposal,
    authority: [u8; 32],
    bootstrap: [u8; 32],
    journal: SegmentStore,
    state: State,
    failed: bool,
    _lock: rsf_storage::ExclusiveFileLock,
}
impl MembershipDecisionStore {
    pub fn open(
        path: impl AsRef<Path>,
        local: NodeId,
        approval: &VsrMembershipApproval,
    ) -> RsfResult<Self> {
        Self::open_inner(path.as_ref(), local, Some(approval), None)
    }
    /// Reopen retained authority and promises without contacting the original
    /// proposer or any old voter. Every retained proof is independently replayed.
    pub fn open_existing(
        path: impl AsRef<Path>,
        pg: PlacementGroupId,
        local: NodeId,
    ) -> RsfResult<Self> {
        Self::open_inner(path.as_ref(), local, None, Some(pg))
    }
    fn open_inner(
        root: &Path,
        local: NodeId,
        approval: Option<&VsrMembershipApproval>,
        pg: Option<PlacementGroupId>,
    ) -> RsfResult<Self> {
        if approval.is_none() && !root.join("journal/MANIFEST").is_file() {
            return Err(corrupt("membership decision journal is missing"));
        }
        fs::create_dir_all(root).map_err(io_error)?;
        let lock = rsf_storage::ExclusiveFileLock::acquire(
            OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .open(root.join(".decision.lock"))
                .map_err(io_error)?,
        )
        .map_err(|e| invalid_input(format!("membership decision store already owned: {e}")))?;
        if !root.join("journal/MANIFEST").is_file() && root.join("inputs").exists() {
            return Err(corrupt(
                "membership inputs exist without their authority journal",
            ));
        }
        let mut options = SegmentStoreOptions::default();
        options.log_options.sync_policy = rsf_storage::SyncPolicy::Full;
        let (journal, recovered) = SegmentStore::open_recovered(root.join("journal"), options)?;
        let records = &recovered.recovery.wal_records;
        if records.is_empty() {
            let approval =
                approval.ok_or_else(|| corrupt("membership decision journal has no authority"))?;
            let input = VsrMembershipInput::from_approval(approval)?;
            let proposal = input.proposal().clone();
            proposal.encode()?;
            if !proposal.new_voters.contains(&local) {
                return Err(invalid_input(
                    "membership decision acceptor is not a proposed voter",
                ));
            }
            let bootstrap = bootstrap_id(approval)?;
            let mut store = Self {
                root: root.to_path_buf(),
                local,
                proposal,
                authority: input.id()?,
                bootstrap,
                journal,
                state: State::default(),
                failed: false,
                _lock: lock,
            };
            store.create_input(&input)?;
            let records = input.flattened_records(approval)?;
            let mut ordinal = 0;
            while ordinal < records.len() {
                let batch = VsrHistoryBatch::pack(ordinal as u64, &records[ordinal..])?;
                ordinal += batch.records.len();
                store.append_records(&input, batch)?;
            }
            store.read_approval(store.authority)?;
            store.write_state(State::default(), || {})?;
            return Ok(store);
        }
        let mut identity = None;
        let mut previous = None;
        for (position, record) in records.iter().enumerate() {
            let DecodedState {
                local: stored_local,
                proposal,
                authority,
                bootstrap,
                state,
            } = decode_state(&record.payload)?;
            if stored_local != local
                || pg.is_some_and(|pg| pg != proposal.election.pg)
                || record.index != LogIndex(position as u64 + 1)
                || record.pg != proposal.election.pg
                || record.epoch != proposal.election.target_view
                || record.kind != WalRecordKind::Membership
                || !proposal.new_voters.contains(&local)
            {
                return Err(corrupt(
                    "membership decision journal identity or envelope mismatch",
                ));
            }
            let current = (proposal.clone(), authority, bootstrap);
            if identity.as_ref().is_some_and(|old| *old != current) {
                return Err(corrupt("membership decision journal changes authority"));
            }
            if position == 0 && state != State::default() {
                return Err(corrupt(
                    "membership decision journal must begin before any promise",
                ));
            }
            validate_transition(previous.as_ref(), &state, &proposal)?;
            identity = Some(current);
            previous = Some(state);
        }
        let (proposal, authority, bootstrap) = identity.unwrap();
        if let Some(approval) = approval
            && (approval.proposal() != &proposal || bootstrap_id(approval)? != bootstrap)
        {
            return Err(invalid_input(
                "membership decision store authority differs from supplied proof",
            ));
        }
        let store = Self {
            root: root.to_path_buf(),
            local,
            proposal,
            authority,
            bootstrap,
            journal,
            state: previous.unwrap(),
            failed: false,
            _lock: lock,
        };
        store.read_approval(authority)?;
        if let Some((_, id)) = store.state.accepted {
            store.read_approval(id)?;
        }
        if let Some(chosen) = &store.state.chosen {
            store.read_approval(chosen.input_id())?;
        }
        if let Some((_, id)) = store.state.staged {
            store.read_records(id)?;
        }
        Ok(store)
    }
    pub fn proposal(&self) -> &VsrMembershipProposal {
        &self.proposal
    }
    pub fn accepted(&self) -> Option<(VsrMembershipRound, [u8; 32])> {
        self.state.accepted
    }
    pub fn chosen(&self) -> Option<&VsrMembershipChosen> {
        self.state.chosen.as_ref()
    }
    pub fn authority(&self) -> RsfResult<VsrMembershipApproval> {
        self.healthy()?;
        self.read_approval(self.authority)
    }
    pub fn input(&self, id: [u8; 32]) -> RsfResult<(VsrMembershipInput, Vec<VsrHistoryRecord>)> {
        self.healthy()?;
        self.read_records(id)
    }
    pub fn approval(&self, id: [u8; 32]) -> RsfResult<VsrMembershipApproval> {
        self.healthy()?;
        self.read_approval(id)
    }
    /// Authenticated proposed-voter coordinators may raise the recovery round.
    /// Exact retries preserve the highest accepted plan; lower rounds reject.
    pub fn promise(
        &mut self,
        sender: NodeId,
        round: VsrMembershipRound,
    ) -> RsfResult<VsrMembershipPromise> {
        self.healthy()?;
        self.authorize(sender, round)?;
        if self.state.promised.is_some_and(|old| old > round) {
            return Err(invalid_input(
                "membership round is below the durable promise",
            ));
        }
        if self.state.promised != Some(round) {
            let mut next = self.state.clone();
            next.promised = Some(round);
            self.write_state(next, || {})?;
        }
        Ok(VsrMembershipPromise {
            proposal: self.proposal.clone(),
            replica: self.local,
            round,
            accepted: self.state.accepted,
        })
    }
    pub fn begin_input(
        &mut self,
        sender: NodeId,
        round: VsrMembershipRound,
        input: VsrMembershipInput,
    ) -> RsfResult<u64> {
        self.healthy()?;
        self.require_round(sender, round)?;
        if input.proposal() != &self.proposal {
            return Err(invalid_input("membership upload has a different proposal"));
        }
        let id = input.id()?;
        self.check_value(round, id)?;
        if id == self.authority
            || self.state.accepted.is_some_and(|(_, old)| old == id)
            || self.state.staged.is_some_and(|(_, old)| old == id)
            || self
                .state
                .chosen
                .as_ref()
                .is_some_and(|chosen| chosen.input_id() == id)
        {
            self.read_records(id)?;
        }
        self.create_input(&input)?;
        let (_, records) = self.read_records(id)?;
        let mut next = self.state.clone();
        next.staged = Some((round, id));
        if next != self.state {
            self.write_state(next, || {})?;
        }
        Ok(records.len() as u64)
    }
    pub fn append_input(
        &mut self,
        sender: NodeId,
        round: VsrMembershipRound,
        id: [u8; 32],
        batch: VsrHistoryBatch,
    ) -> RsfResult<u64> {
        self.healthy()?;
        self.require_round(sender, round)?;
        if self.state.staged != Some((round, id)) {
            return Err(invalid_input(
                "membership upload is not the selected round input",
            ));
        }
        let (input, _) = self.read_records(id)?;
        self.append_records(&input, batch)
    }
    /// Bind the coordinator's own plan durably before relaying it. This prevents
    /// a restarted coordinator from reusing one round for different inputs.
    pub fn nominate(
        &mut self,
        decision: &VsrMembershipDecision,
    ) -> RsfResult<VsrMembershipAccepted> {
        self.nominate_with_hook(decision, || {})
    }
    pub(super) fn nominate_with_hook(
        &mut self,
        decision: &VsrMembershipDecision,
        after_synced: impl FnOnce(),
    ) -> RsfResult<VsrMembershipAccepted> {
        if self.local != decision.round().coordinator {
            return Err(invalid_input(
                "only the round coordinator may nominate membership input",
            ));
        }
        self.accept_plan(self.local, decision, after_synced)
    }
    /// Authenticate the coordinator before accepting its durable nomination.
    pub fn accept(
        &mut self,
        sender: NodeId,
        decision: &VsrMembershipDecision,
        nomination: &VsrMembershipAccepted,
    ) -> RsfResult<VsrMembershipAccepted> {
        nomination.encode()?;
        if nomination.replica != sender
            || nomination.round != decision.round()
            || nomination.proposal != self.proposal
            || nomination.input != decision.input().id()?
        {
            return Err(invalid_input(
                "membership acceptance differs from the authenticated coordinator nomination",
            ));
        }
        self.accept_plan(sender, decision, || {})
    }
    fn accept_plan(
        &mut self,
        sender: NodeId,
        decision: &VsrMembershipDecision,
        after_synced: impl FnOnce(),
    ) -> RsfResult<VsrMembershipAccepted> {
        self.healthy()?;
        let round = decision.round();
        self.require_round(sender, round)?;
        if decision.input().proposal() != &self.proposal {
            return Err(invalid_input("membership decision has different authority"));
        }
        let id = decision.input().id()?;
        self.check_value(round, id)?;
        let (input, _) = self.read_records(id)?;
        if input != *decision.input() {
            return Err(corrupt(
                "membership decision input differs from retained descriptor",
            ));
        }
        self.read_approval(id)?;
        if self.state.accepted != Some((round, id)) {
            let mut next = self.state.clone();
            next.accepted = Some((round, id));
            self.write_state(next, after_synced)?;
        }
        Ok(VsrMembershipAccepted {
            proposal: self.proposal.clone(),
            replica: self.local,
            round,
            input: id,
        })
    }
    /// Record a new-quorum plan decision. This is not an application installation
    /// acknowledgement and does not publish a VSR configuration.
    pub fn learn_chosen(&mut self, chosen: VsrMembershipChosen) -> RsfResult<()> {
        self.healthy()?;
        chosen.encode()?;
        if chosen.proposal() != &self.proposal
            || self
                .state
                .chosen
                .as_ref()
                .is_some_and(|old| old.input_id() != chosen.input_id())
            || self
                .state
                .accepted
                .is_some_and(|(round, id)| round >= chosen.round() && id != chosen.input_id())
        {
            return Err(invalid_input(
                "membership choice conflicts with retained authority or accepted input",
            ));
        }
        self.read_approval(chosen.input_id())?;
        if self
            .state
            .chosen
            .as_ref()
            .is_some_and(|old| old.input_id() == chosen.input_id())
        {
            return Ok(());
        }
        let mut next = self.state.clone();
        next.promised = Some(
            next.promised
                .map_or(chosen.round(), |r| r.max(chosen.round())),
        );
        if next
            .accepted
            .is_none_or(|(round, _)| round < chosen.round())
        {
            next.accepted = Some((chosen.round(), chosen.input_id()));
        }
        next.chosen = Some(chosen);
        self.write_state(next, || {})
    }
    fn authorize(&self, sender: NodeId, round: VsrMembershipRound) -> RsfResult<()> {
        round.validate(&self.proposal)?;
        if sender != round.coordinator {
            return Err(invalid_input(
                "membership round sender differs from coordinator",
            ));
        }
        Ok(())
    }
    fn require_round(&self, sender: NodeId, round: VsrMembershipRound) -> RsfResult<()> {
        self.authorize(sender, round)?;
        if self.state.promised != Some(round) {
            return Err(invalid_input(
                "membership action does not match durable round promise",
            ));
        }
        Ok(())
    }
    fn check_value(&self, round: VsrMembershipRound, id: [u8; 32]) -> RsfResult<()> {
        if self
            .state
            .accepted
            .is_some_and(|(old, value)| old == round && value != id)
            || self
                .state
                .chosen
                .as_ref()
                .is_some_and(|chosen| chosen.input_id() != id)
        {
            return Err(invalid_input(
                "membership round or chosen plan is already bound to another input",
            ));
        }
        Ok(())
    }
    fn healthy(&self) -> RsfResult<()> {
        if self.failed {
            return Err(corrupt(
                "membership decision durability failed; reopen before further use",
            ));
        }
        Ok(())
    }
    fn write_state(&mut self, next: State, after_synced: impl FnOnce()) -> RsfResult<()> {
        validate_transition(Some(&self.state), &next, &self.proposal)?;
        let payload = encode_state(
            self.local,
            &self.proposal,
            self.authority,
            self.bootstrap,
            &next,
        )?;
        match self.journal.append_next_wal(
            self.proposal.election.pg,
            self.proposal.election.target_view,
            WalRecordKind::Membership,
            &payload,
        ) {
            Ok(_) => {
                self.state = next;
                after_synced();
                Ok(())
            }
            Err(error) => {
                self.failed = true;
                Err(error)
            }
        }
    }
    fn directory(&self, id: [u8; 32]) -> PathBuf {
        self.root.join("inputs").join(hex(id))
    }
    fn create_input(&self, input: &VsrMembershipInput) -> RsfResult<()> {
        let id = input.id()?;
        let base = self.root.join("inputs");
        fs::create_dir_all(&base).map_err(io_error)?;
        sync(&self.root)?;
        let dir = self.directory(id);
        fs::create_dir_all(&dir).map_err(io_error)?;
        sync(&base)?;
        let bytes = input.encode()?;
        match fs::read(dir.join("DESCRIPTOR")) {
            Ok(existing) if existing == bytes => {}
            Ok(_) => return Err(corrupt("membership input descriptor identity collision")),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let temp = dir.join(".DESCRIPTOR.tmp");
                let mut file = OpenOptions::new()
                    .create(true)
                    .truncate(true)
                    .write(true)
                    .open(&temp)
                    .map_err(io_error)?;
                file.write_all(&bytes)
                    .and_then(|()| file.sync_all())
                    .map_err(io_error)?;
                fs::rename(temp, dir.join("DESCRIPTOR")).map_err(io_error)?;
                sync(&dir)?;
            }
            Err(error) => return Err(io_error(error)),
        }
        let mut options = SegmentStoreOptions::default();
        options.log_options.sync_policy = rsf_storage::SyncPolicy::Full;
        drop(SegmentStore::open_with_options(
            dir.join("records"),
            options,
        )?);
        sync(&dir)
    }
    fn read_records(&self, id: [u8; 32]) -> RsfResult<(VsrMembershipInput, Vec<VsrHistoryRecord>)> {
        let dir = self.directory(id);
        if !dir.join("records/MANIFEST").is_file() {
            return Err(corrupt("membership input record store is missing"));
        }
        let input =
            VsrMembershipInput::decode(&fs::read(dir.join("DESCRIPTOR")).map_err(io_error)?)?;
        if input.id()? != id || input.proposal() != &self.proposal {
            return Err(corrupt("membership input identity or authority mismatch"));
        }
        let recovered = SegmentStore::recover_existing(dir.join("records"))?;
        let mut records = Vec::new();
        for (position, record) in recovered.recovery.wal_records.iter().enumerate() {
            if record.index != LogIndex(position as u64 + 1)
                || record.pg != self.proposal.election.pg
                || record.epoch != self.proposal.election.target_view
                || record.kind != WalRecordKind::ViewInstallation
            {
                return Err(corrupt("membership input WAL envelope mismatch"));
            }
            let batch = VsrHistoryBatch::decode(&record.payload)?;
            if batch.ordinal != records.len() as u64 {
                return Err(corrupt("membership input batch reorders history"));
            }
            records.extend(batch.records);
            if records.len() as u64 > input.records()? {
                return Err(corrupt("membership input exceeds descriptor"));
            }
        }
        Ok((input, records))
    }
    fn read_approval(&self, id: [u8; 32]) -> RsfResult<VsrMembershipApproval> {
        let (input, records) = self.read_records(id)?;
        let approval = input.validate_records(&records)?;
        if bootstrap_id(&approval)? != self.bootstrap {
            return Err(corrupt("membership input changes historical bootstrap"));
        }
        Ok(approval)
    }
    fn append_records(
        &mut self,
        input: &VsrMembershipInput,
        batch: VsrHistoryBatch,
    ) -> RsfResult<u64> {
        batch.encode()?;
        let id = input.id()?;
        let (_, records) = self.read_records(id)?;
        let count = records.len() as u64;
        let end = batch
            .ordinal
            .checked_add(batch.records.len() as u64)
            .ok_or_else(|| invalid_input("membership input batch overflow"))?;
        if batch.ordinal > count || end > input.records()? {
            return Err(invalid_input(
                "membership input batch is outside durable prefix",
            ));
        }
        let overlap = count
            .saturating_sub(batch.ordinal)
            .min(batch.records.len() as u64) as usize;
        if overlap > 0
            && records.get(batch.ordinal as usize..batch.ordinal as usize + overlap)
                != Some(&batch.records[..overlap])
        {
            return Err(corrupt("membership input retry changes retained records"));
        }
        if end <= count {
            return Ok(count);
        }
        let appended = VsrHistoryBatch {
            ordinal: count,
            records: batch.records[overlap..].to_vec(),
        }
        .encode()?;
        let mut options = SegmentStoreOptions::default();
        options.log_options.sync_policy = rsf_storage::SyncPolicy::Full;
        let mut store =
            SegmentStore::open_with_options(self.directory(id).join("records"), options)?;
        if let Err(error) = store.append_next_wal(
            self.proposal.election.pg,
            self.proposal.election.target_view,
            WalRecordKind::ViewInstallation,
            &appended,
        ) {
            self.failed = true;
            return Err(error);
        }
        Ok(end)
    }
}
fn bootstrap_id(approval: &VsrMembershipApproval) -> RsfResult<[u8; 32]> {
    let record = approval.snapshots()[0]
        .records
        .first()
        .ok_or_else(|| corrupt("membership authority has no bootstrap"))?;
    Ok(Digest::blake3_bytes(&record.encode()?).bytes)
}
fn io_error(error: std::io::Error) -> RsfError {
    corrupt(format!("membership decision storage: {error}"))
}
fn sync(path: &Path) -> RsfResult<()> {
    File::open(path)
        .and_then(|f| f.sync_all())
        .map_err(io_error)
}
fn hex(id: [u8; 32]) -> String {
    id.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
