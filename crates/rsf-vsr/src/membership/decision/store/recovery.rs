use super::*;

#[derive(Debug)]
pub(super) struct Import {
    chosen: VsrMembershipChosen,
    input: VsrMembershipInput,
}

impl MembershipDecisionStore {
    pub fn decision_state(&self) -> RsfResult<VsrMembershipDecisionState> {
        self.healthy()?;
        // Replay and write_state are the only ways this private state changes;
        // both validate the complete transition proof. A metadata poll checks
        // its compact projection without reserializing the chosen quorum.
        let value = VsrMembershipDecisionState {
            pg: self.proposal.election.pg,
            proposal: self.proposal.id()?,
            replica: self.local,
            bootstrap: self.bootstrap,
            authority: self.authority,
            promised: self.state.promised,
            accepted: self.state.accepted,
            chosen: self
                .state
                .chosen
                .as_ref()
                .map(|c| (c.round(), c.input_id())),
        };
        value.validate_for(&self.proposal)?;
        Ok(value)
    }

    pub fn try_promise(
        &mut self,
        sender: NodeId,
        round: VsrMembershipRound,
    ) -> RsfResult<VsrMembershipPromiseReply> {
        self.healthy()?;
        self.authorize(sender, round)?;
        if self.state.promised.is_some_and(|old| old > round) {
            return self
                .decision_state()
                .map(VsrMembershipPromiseReply::Preempted);
        }
        self.promise(sender, round)
            .map(VsrMembershipPromiseReply::Granted)
    }

    /// Missing means genuinely absent and unreferenced. A staged upload may be
    /// partial; authority/accepted/chosen inputs must always replay completely.
    pub fn input_progress(&self, id: [u8; 32]) -> RsfResult<VsrMembershipInputProgress> {
        self.healthy()?;
        let complete = self.complete_input(id);
        let bound = complete || self.state.staged.is_some_and(|(_, value)| value == id);
        let path = self.directory(id);
        match std::fs::symlink_metadata(&path) {
            Ok(meta) if meta.is_dir() && !meta.file_type().is_symlink() => {}
            Ok(_) => return Err(corrupt("membership input is not an owned directory")),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound && !bound => {
                return Ok(VsrMembershipInputProgress::Missing);
            }
            Err(error) => return Err(io_error(error)),
        }
        if !bound && disk::initialization_pending(&self.root, id, &self.proposal)? {
            return Ok(VsrMembershipInputProgress::Initializing);
        }
        let descriptor = std::fs::symlink_metadata(path.join("DESCRIPTOR")).map_err(io_error)?;
        let directory = std::fs::symlink_metadata(path.join("records")).map_err(io_error)?;
        let manifest =
            std::fs::symlink_metadata(path.join("records/MANIFEST")).map_err(io_error)?;
        if !descriptor.is_file()
            || descriptor.file_type().is_symlink()
            || descriptor.len() > MAX_HISTORY_RECORD_BYTES as u64
            || !directory.is_dir()
            || directory.file_type().is_symlink()
            || !manifest.is_file()
            || manifest.file_type().is_symlink()
        {
            return Err(corrupt(
                "membership input custody is not regular bounded storage",
            ));
        }
        let (input, records) = self.read_records(id)?;
        let count = records.len() as u64;
        let total = input.records()?;
        if count < total {
            if complete {
                return Err(corrupt(
                    "acknowledged membership input lost its complete prefix",
                ));
            }
            return Ok(VsrMembershipInputProgress::Partial {
                input,
                records: count,
            });
        }
        let approval = input.validate_records(&records)?;
        if bootstrap_id(&approval)? != self.bootstrap {
            return Err(corrupt("membership input changes historical bootstrap"));
        }
        Ok(VsrMembershipInputProgress::Complete { input })
    }

    fn complete_input(&self, id: [u8; 32]) -> bool {
        id == self.authority
            || self.state.accepted.is_some_and(|(_, value)| value == id)
            || self
                .state
                .chosen
                .as_ref()
                .is_some_and(|chosen| chosen.input_id() == id)
    }

    /// Only a genuinely missing value may use the creating input factory.
    pub(super) fn ensure_input(
        &self,
        input: &VsrMembershipInput,
        sync_publication: impl FnOnce(&Path, [u8; 32]) -> RsfResult<()>,
    ) -> RsfResult<u64> {
        if input.proposal() != &self.proposal {
            return Err(invalid_input("membership input has another proposal"));
        }
        let id = input.id()?;
        let count = match self.input_progress(id)? {
            VsrMembershipInputProgress::Missing => {
                self.create_input(input)?;
                0
            }
            VsrMembershipInputProgress::Initializing => {
                disk::resume_input_initialization(&self.root, input)?;
                0
            }
            VsrMembershipInputProgress::Partial {
                input: stored,
                records,
            } => {
                if stored != *input {
                    return Err(corrupt("membership input descriptor changed"));
                }
                records
            }
            VsrMembershipInputProgress::Complete { input: stored } => {
                if stored != *input {
                    return Err(corrupt("membership input descriptor changed"));
                }
                input.records()?
            }
        };
        sync_publication(&self.root, id)?;
        Ok(count)
    }

    fn import_sender(&self, sender: NodeId) -> RsfResult<()> {
        self.healthy()?;
        if !self.proposal.new_voters.contains(&sender)
            || !self.proposal.new_voters.contains(&self.local)
        {
            return Err(invalid_input(
                "chosen input import requires proposed voters",
            ));
        }
        Ok(())
    }
    fn validate_import(
        &self,
        chosen: &VsrMembershipChosen,
        input: &VsrMembershipInput,
    ) -> RsfResult<()> {
        chosen.encode()?;
        if chosen.proposal() != &self.proposal
            || input.proposal() != &self.proposal
            || chosen.input_id() != input.id()?
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
                "chosen import conflicts with retained authority or accepted value",
            ));
        }
        Ok(())
    }

    /// Retain only a boot-scoped copy permission. No promise, acceptance, choice,
    /// staged round, or application state changes before full replay at finish.
    pub fn begin_chosen_input(
        &mut self,
        sender: NodeId,
        chosen: VsrMembershipChosen,
        input: VsrMembershipInput,
    ) -> RsfResult<VsrMembershipImportReply> {
        self.begin_chosen_input_with_sync(sender, chosen, input, disk::sync_input_publication)
    }

    pub(super) fn begin_chosen_input_with_sync(
        &mut self,
        sender: NodeId,
        chosen: VsrMembershipChosen,
        input: VsrMembershipInput,
        sync_publication: impl FnOnce(&Path, [u8; 32]) -> RsfResult<()>,
    ) -> RsfResult<VsrMembershipImportReply> {
        self.import_sender(sender)?;
        let package = VsrMembershipChosenInput::new(chosen, input)?;
        let chosen = package.chosen;
        let input = package.input;
        self.validate_import(&chosen, &input)?;
        let id = input.id()?;
        if self.import.as_ref().is_some_and(|old| old.input != input) {
            return Err(invalid_input(
                "another chosen input import is already bound this boot",
            ));
        }
        let count = self.ensure_input(&input, sync_publication)?;
        if self
            .state
            .chosen
            .as_ref()
            .is_some_and(|old| old.input_id() == id)
        {
            return Ok(VsrMembershipImportReply::Chosen { input: id });
        }
        if self.import.is_none() {
            self.import = Some(Import { chosen, input });
        }
        Ok(VsrMembershipImportReply::Retained {
            input: id,
            records: count,
        })
    }

    pub fn append_chosen_input(
        &mut self,
        sender: NodeId,
        id: [u8; 32],
        batch: VsrHistoryBatch,
    ) -> RsfResult<VsrMembershipImportReply> {
        self.import_sender(sender)?;
        batch.encode()?;
        // A late batch may follow successful finish; validate exact overlap but
        // never append or reconstruct the already complete chosen value.
        if self
            .state
            .chosen
            .as_ref()
            .is_some_and(|old| old.input_id() == id)
        {
            let VsrMembershipInputProgress::Complete { input } = self.input_progress(id)? else {
                return Err(corrupt("chosen membership input is incomplete"));
            };
            self.append_records(&input, batch)?;
            return Ok(VsrMembershipImportReply::Chosen { input: id });
        }
        let Some(context) = &self.import else {
            return Ok(VsrMembershipImportReply::BeginRequired);
        };
        if context.input.id()? != id {
            return Err(invalid_input("chosen input import context differs"));
        }
        self.validate_import(&context.chosen, &context.input)?;
        let input = context.input.clone();
        match self.input_progress(id)? {
            VsrMembershipInputProgress::Missing | VsrMembershipInputProgress::Initializing => {
                return Err(corrupt(
                    "active chosen input import lost its retained prefix",
                ));
            }
            VsrMembershipInputProgress::Partial { .. }
            | VsrMembershipInputProgress::Complete { .. } => {}
        }
        let records = self.append_records(&input, batch)?;
        Ok(VsrMembershipImportReply::Retained { input: id, records })
    }

    pub fn finish_chosen_input(
        &mut self,
        sender: NodeId,
        id: [u8; 32],
    ) -> RsfResult<VsrMembershipImportReply> {
        self.import_sender(sender)?;
        if self
            .state
            .chosen
            .as_ref()
            .is_some_and(|old| old.input_id() == id)
        {
            self.read_approval(id)?;
            return Ok(VsrMembershipImportReply::Chosen { input: id });
        }
        let Some(context) = &self.import else {
            return Ok(VsrMembershipImportReply::BeginRequired);
        };
        if context.input.id()? != id {
            return Err(invalid_input("chosen input finish context differs"));
        }
        self.validate_import(&context.chosen, &context.input)?;
        self.read_approval(id)?;
        let chosen = context.chosen.clone();
        self.learn_chosen(chosen)?;
        self.import = None;
        Ok(VsrMembershipImportReply::Chosen { input: id })
    }
}
