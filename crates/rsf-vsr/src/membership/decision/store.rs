mod recovery;
use super::disk;
use super::state::{DecodedState, State, decode_state, encode_state, validate_transition};
use super::*;
use std::fs::OpenOptions;
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
    import: Option<recovery::Import>,
    _lock: rsf_storage::ExclusiveFileLock,
}
impl MembershipDecisionStore {
    pub fn open(
        path: impl AsRef<Path>,
        local: NodeId,
        approval: &VsrMembershipApproval,
    ) -> RsfResult<Self> {
        Self::open_inner(path.as_ref(), local, Some(approval), None, |_, _| Ok(()))
    }
    /// Reopen retained authority and promises without contacting the original
    /// proposer or any old voter. Every retained proof is independently replayed.
    pub fn open_existing(
        path: impl AsRef<Path>,
        pg: PlacementGroupId,
        local: NodeId,
    ) -> RsfResult<Self> {
        Self::open_existing_with_validation(path, pg, local, |_, _| Ok(()))
    }
    /// Reopen and validate hosted authority from each distinct complete input
    /// while its opaque replay proof is already owned by this cold operation.
    /// The callback cannot replace journal, bootstrap or input validation; all
    /// bound inputs and namespace barriers complete before ownership escapes.
    /// It receives no mutable acceptor and no approval is cached after return.
    pub fn open_existing_with_validation(
        path: impl AsRef<Path>,
        pg: PlacementGroupId,
        local: NodeId,
        validate: impl FnMut(&Self, &VsrMembershipApproval) -> RsfResult<()>,
    ) -> RsfResult<Self> {
        Self::open_inner(path.as_ref(), local, None, Some(pg), validate)
    }

    fn open_inner(
        root: &Path,
        local: NodeId,
        approval: Option<&VsrMembershipApproval>,
        pg: Option<PlacementGroupId>,
        mut validate: impl FnMut(&Self, &VsrMembershipApproval) -> RsfResult<()>,
    ) -> RsfResult<Self> {
        if approval.is_none() && !root.join("journal/MANIFEST").is_file() {
            return Err(corrupt("membership decision journal is missing"));
        }
        rsf_storage::create_dir_all_synced(root).map_err(io_error)?;
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
                import: None,
                _lock: lock,
            };
            store.create_input(&input)?;
            let records = input.flattened_records(approval)?;
            disk::retain_records(&store.root, &mut store.failed, &input, &records)?;
            let retained = store.read_approval(store.authority)?;
            store.write_state(State::default(), || {})?;
            validate(&store, &retained)?;
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
            import: None,
            _lock: lock,
        };
        // One strongest proof per exact input ID, irrespective of how many
        // journal roles refer to it. A staged-only upload may remain partial.
        let mut inputs = std::collections::BTreeSet::from([authority]);
        inputs.extend(store.state.accepted.map(|(_, id)| id));
        inputs.extend(store.state.chosen.as_ref().map(|chosen| chosen.input_id()));
        for id in &inputs {
            let approval = store.read_approval(*id)?;
            validate(&store, &approval)?;
        }
        if let Some((_, id)) = store.state.staged
            && inputs.insert(id)
        {
            store.read_records(id)?;
        }
        // Replay validates authority; cold ownership also renews the namespace
        // of every journal-bound input before this acceptor can acknowledge.
        for id in inputs {
            disk::sync_input_publication(&store.root, id)?;
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
    /// Return complete retained input without requiring a learned chosen proof.
    /// Only an absent, unreferenced directory returns None. Missing journal-bound
    /// input, malformed custody, and incomplete uploads remain errors; callers
    /// must not replace those with an inbox copy. This does not learn a choice
    /// or acknowledge application effects.
    pub fn retained_approval(&self, id: [u8; 32]) -> RsfResult<Option<VsrMembershipApproval>> {
        self.healthy()?;
        let bound = self.input_is_bound(id);
        match std::fs::symlink_metadata(self.directory(id)) {
            Ok(metadata) if metadata.file_type().is_dir() => self.read_approval(id).map(Some),
            Ok(_) => Err(corrupt(
                "retained membership input is not an owned directory",
            )),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound && !bound => Ok(None),
            Err(error) => Err(io_error(error)),
        }
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
        self.begin_input_with_sync(sender, round, input, disk::sync_input_publication)
    }

    fn begin_input_with_sync(
        &mut self,
        sender: NodeId,
        round: VsrMembershipRound,
        input: VsrMembershipInput,
        sync_publication: impl FnOnce(&Path, [u8; 32]) -> RsfResult<()>,
    ) -> RsfResult<u64> {
        self.healthy()?;
        self.require_round(sender, round)?;
        if input.proposal() != &self.proposal {
            return Err(invalid_input("membership upload has a different proposal"));
        }
        let id = input.id()?;
        self.check_value(round, id)?;
        let count = self.ensure_input(&input, sync_publication)?;
        let mut next = self.state.clone();
        next.staged = Some((round, id));
        if next != self.state {
            self.write_state(next, || {})?;
        }
        Ok(count)
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
        let input = match self.input_progress(id)? {
            VsrMembershipInputProgress::Partial { input, .. }
            | VsrMembershipInputProgress::Complete { input } => input,
            VsrMembershipInputProgress::Missing | VsrMembershipInputProgress::Initializing => {
                return Err(corrupt(
                    "selected membership upload lost its retained prefix",
                ));
            }
        };
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
        self.accept_plan_with_publication(
            sender,
            decision,
            after_synced,
            disk::sync_input_publication,
        )
    }

    fn accept_plan_with_publication(
        &mut self,
        sender: NodeId,
        decision: &VsrMembershipDecision,
        after_synced: impl FnOnce(),
        publish: impl FnOnce(&Path, [u8; 32]) -> RsfResult<()>,
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
        if !self.input_is_bound(id) {
            // A complete unbound retained input is readable data, not yet local
            // durable custody. Bind it only after its publication barrier.
            publish(&self.root, id)?;
        }
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
    /// Import the exact input already chosen by a new quorum, without creating
    /// a new ballot or replacing its value. This supports publication to a voter
    /// which missed the original acceptance/upload round.
    pub fn retain_chosen_input(
        &mut self,
        chosen: VsrMembershipChosen,
        approval: &VsrMembershipApproval,
    ) -> RsfResult<()> {
        self.retain_chosen_input_with_hook(chosen, approval, || {})
    }

    fn retain_chosen_input_with_hook(
        &mut self,
        chosen: VsrMembershipChosen,
        approval: &VsrMembershipApproval,
        after_copied: impl FnOnce(),
    ) -> RsfResult<()> {
        self.validate_choice(&chosen)?;
        let input = VsrMembershipInput::from_approval(approval)?;
        let id = input.id()?;
        if input.proposal() != &self.proposal
            || chosen.input_id() != id
            || bootstrap_id(approval)? != self.bootstrap
        {
            return Err(invalid_input(
                "retained membership choice/input differs from anchored authority",
            ));
        }
        // VsrMembershipApproval is opaque replay-validated custody. Comparing
        // complete retained bytes against it carries that same proof, without
        // replaying all old-voter application histories a second time.
        let records = input.flattened_records(approval)?;
        if self
            .state
            .chosen
            .as_ref()
            .is_some_and(|old| old.input_id() == id)
        {
            return self.compare_retained_input(&input, &records);
        }
        if id == self.authority || self.state.accepted.is_some_and(|(_, stored)| stored == id) {
            // This is before create/copy: acknowledged input may never use a
            // trusted source to repair a shorter or damaged retained prefix.
            self.compare_retained_input(&input, &records)?;
            // This exact complete input already crossed its first binding's
            // publication barrier, or was renewed on cold recovery. Re-copying
            // it would reopen a factory without adding proof or durable bytes.
            return self.learn_verified_chosen(chosen);
        } else if self.state.staged.is_some_and(|(_, stored)| stored == id) {
            self.read_records(id)?;
        }
        self.create_input(&input)?;
        disk::retain_records(&self.root, &mut self.failed, &input, &records)?;
        after_copied();
        self.compare_retained_input(&input, &records)?;
        self.learn_verified_chosen(chosen)
    }

    fn compare_retained_input(
        &self,
        input: &VsrMembershipInput,
        records: &[VsrHistoryRecord],
    ) -> RsfResult<()> {
        let (retained, previous) = self.read_records(input.id()?)?;
        if retained != *input || previous != records {
            return Err(corrupt("chosen membership input changed or lost records"));
        }
        Ok(())
    }

    /// Record a new-quorum plan decision. This is not an application installation
    /// acknowledgement and does not publish a VSR configuration.
    pub fn learn_chosen(&mut self, chosen: VsrMembershipChosen) -> RsfResult<()> {
        self.validate_choice(&chosen)?;
        self.read_approval(chosen.input_id())?;
        self.learn_verified_chosen(chosen)
    }

    fn validate_choice(&self, chosen: &VsrMembershipChosen) -> RsfResult<()> {
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
        Ok(())
    }

    /// Private proof boundary: callers have either replayed read_approval or
    /// compared every persisted byte against an opaque validated approval.
    /// This method still rechecks consensus state and never installs a cell.
    fn learn_verified_chosen(&mut self, chosen: VsrMembershipChosen) -> RsfResult<()> {
        self.learn_verified_chosen_with_publication(chosen, disk::sync_input_publication)
    }

    fn learn_verified_chosen_with_publication(
        &mut self,
        chosen: VsrMembershipChosen,
        publish: impl FnOnce(&Path, [u8; 32]) -> RsfResult<()>,
    ) -> RsfResult<()> {
        self.validate_choice(&chosen)?;
        if self
            .state
            .chosen
            .as_ref()
            .is_some_and(|old| old.input_id() == chosen.input_id())
        {
            return Ok(());
        }
        if !self.input_is_bound(chosen.input_id()) {
            publish(&self.root, chosen.input_id())?;
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

    // Each durable reference is renewed on cold open; live factories and BEGIN
    // publish before adding one. This is derived custody, never a cached bit.
    fn input_is_bound(&self, id: [u8; 32]) -> bool {
        id == self.authority
            || self.state.accepted.is_some_and(|(_, stored)| stored == id)
            || self.state.staged.is_some_and(|(_, stored)| stored == id)
            || self
                .state
                .chosen
                .as_ref()
                .is_some_and(|chosen| chosen.input_id() == id)
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
        disk::create_input(&self.root, input)
    }
    fn read_records(&self, id: [u8; 32]) -> RsfResult<(VsrMembershipInput, Vec<VsrHistoryRecord>)> {
        let value = disk::read_records(&self.root, id)?;
        if value.0.proposal() != &self.proposal {
            return Err(corrupt("membership input authority mismatch"));
        }
        Ok(value)
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
        disk::append_records(&self.root, &mut self.failed, input, batch)
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
fn hex(id: [u8; 32]) -> String {
    id.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
