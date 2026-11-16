//! Complete application commands on the VSR spine. The protocol receipt is a
//! derived index; the prepared command is authoritative across an uncertain commit.
use super::command::PendingCommand;
use super::*;

pub(super) mod codec;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplicationSurface {
    Kv,
    Sql,
    Object,
    Blob,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationPayload {
    pub namespace: String,
    pub surface: ApplicationSurface,
    /// Versioned, surface-owned command encoding, including values or durable
    /// immutable-content references. Callers must arrange referenced payload durability.
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicatedFabricCommand {
    pub commit: FabricCommit,
    pub request: ClientRequestId,
    pub application: ApplicationPayload,
}

impl ReplicatedFabricCommand {
    pub fn encode(&self) -> RsfResult<Vec<u8>> {
        codec::encode(self)
    }
    pub fn decode(bytes: &[u8]) -> RsfResult<Self> {
        codec::decode(bytes)
    }
    pub fn certificate(&self) -> CommitCertificate {
        CommitCertificate::from(&self.commit)
    }
    pub(super) fn vsr_operation(&self) -> RsfResult<VsrOperation> {
        Ok(VsrOperation::ReplicatedCommand {
            operation_id: self.commit.operation_id,
            payload: self.encode()?,
        })
    }
}
fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message)
}
fn corrupt(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::CorruptionDetected, message)
}

impl<T: VsrTransport> VsrBackedFabricRouter<T> {
    pub(super) fn recover_application_commands(&mut self) -> RsfResult<()> {
        self.recover_application_commands_from(&SegmentStore::recover(&self.path)?)
    }

    pub(super) fn recover_application_commands_from(
        &mut self,
        recovery: &SegmentStoreRecovery,
    ) -> RsfResult<()> {
        let mut receipts = BTreeMap::new();
        for wal in &recovery.recovery.wal_records {
            if wal.kind == WalRecordKind::Commit
                && ProtocolCommitRecord::is_protocol_payload(&wal.payload)
            {
                let receipt = ProtocolCommitRecord::decode(&wal.payload)?;
                if receipts
                    .insert((wal.pg, receipt.certificate.index), receipt)
                    .is_some()
                {
                    return Err(corrupt("duplicate durable protocol receipt"));
                }
            }
        }
        let mut versions = std::collections::BTreeSet::new();
        let mut requests = std::collections::BTreeSet::new();
        for (pg, cell) in &self.cells {
            for index in 1..cell.next_index().0 {
                let Some(prepare) = cell.prepared(LogIndex(index)) else {
                    continue;
                };
                let VsrOperation::ReplicatedCommand {
                    operation_id,
                    payload,
                } = &prepare.operation
                else {
                    continue;
                };
                let (commit, request) = if ReplicatedProtocolCommand::is_payload(payload) {
                    let command = ReplicatedProtocolCommand::decode(payload)?;
                    let commit = command.commit.clone();
                    let request = command.request.clone();
                    if let Some(request) = &command.request {
                        self.protocol_requests
                            .insert(request.clone(), commit.version);
                    }
                    if self
                        .router
                        .next_indexes
                        .get(pg)
                        .is_none_or(|next| *next <= index)
                    {
                        self.pending_commands
                            .insert(commit.version, PendingCommand::Protocol(command.clone()));
                    }
                    self.protocol_commands.insert(commit.version, command);
                    (commit, request)
                } else {
                    let command = ReplicatedFabricCommand::decode(payload)?;
                    let commit = command.commit.clone();
                    let request = Some(command.request.clone());
                    if self.router.request_certificate(&command.request).is_some() {
                        self.committed_applications
                            .insert(commit.version, command.request.clone());
                    } else {
                        self.pending_commands
                            .insert(commit.version, PendingCommand::Application(command.clone()));
                    }
                    self.application_commands
                        .insert(command.request.clone(), command);
                    (commit, request)
                };
                if commit.pg != *pg
                    || commit.index != prepare.index
                    || commit.operation_id != *operation_id
                {
                    return Err(corrupt("replicated command differs from its VSR prepare"));
                }
                if !versions.insert(commit.version)
                    || request
                        .as_ref()
                        .is_some_and(|r| !requests.insert(r.clone()))
                {
                    return Err(corrupt(
                        "replicated commands reuse a commit version or client request",
                    ));
                }
                if let Some(receipt) = receipts.get(&(*pg, prepare.index)) {
                    if receipt.certificate != CommitCertificate::from(&commit)
                        || receipt.client_request != request
                        || cell.committed_operation(prepare.index) != Some(&prepare.operation)
                    {
                        return Err(corrupt(
                            "protocol receipt lacks matching committed command evidence",
                        ));
                    }
                } else if self
                    .router
                    .next_indexes
                    .get(pg)
                    .is_some_and(|next| *next > index)
                {
                    return Err(corrupt("protocol receipt prefix has a gap"));
                }
                if let Some(request) = request
                    && let Some(certificate) = self.router.request_certificate(&request)
                    && certificate != CommitCertificate::from(&commit)
                {
                    return Err(corrupt(
                        "protocol request receipt remaps a replicated command",
                    ));
                }
            }
        }
        Ok(())
    }

    /// Complete durable application and metadata commands in their original
    /// global version order before allocating another version.
    pub fn reconcile_application_commands(&mut self) -> RsfResult<usize> {
        self.ensure_application_healthy()?;
        let pending = self.pending_commands.values().cloned().collect::<Vec<_>>();
        let count = pending.len();
        for command in pending {
            match command {
                PendingCommand::Application(command) => {
                    self.finish_application_command(&command)?
                }
                PendingCommand::Protocol(command) => self.finish_protocol_command(&command)?,
            }
        }
        Ok(count)
    }

    /// Preview metadata after resolving earlier uncertain commands. No new
    /// prepare or acknowledgement is created by this planning operation.
    pub(super) fn preview_application(
        &mut self,
        pg: PlacementGroupId,
        request: &ClientRequestId,
        mode: ConsistencyMode,
        operation: &FabricOperation,
        application: &ApplicationPayload,
    ) -> RsfResult<FabricCommit> {
        self.ensure_application_healthy()?;
        self.policy.validate_commit(mode, operation)?;
        let original = self.application_commands.get(request).cloned();
        let commit = if let Some(original) = &original {
            if original.commit.pg != pg
                || original.commit.mode != mode
                || &original.commit.operation != operation
                || &original.application != application
            {
                return Err(invalid(
                    "application retry differs from its original complete command",
                ));
            }
            original.commit.clone()
        } else {
            if self.router.request_certificate(request).is_some() {
                return Err(invalid(
                    "legacy request has no complete application command",
                ));
            }
            self.router.next_commit(pg, mode, operation.clone())?
        };
        ReplicatedFabricCommand {
            commit,
            request: request.clone(),
            application: application.clone(),
        }
        .encode()?;
        self.reconcile_application_commands()?;
        if original.is_none() && self.router.request_certificate(request).is_some() {
            return Err(invalid(
                "request identity belongs to a replicated protocol command",
            ));
        }
        let commit = match original {
            Some(original) => original.commit,
            None => self.router.next_commit(pg, mode, operation.clone())?,
        };
        ReplicatedFabricCommand {
            commit: commit.clone(),
            request: request.clone(),
            application: application.clone(),
        }
        .encode()?;
        Ok(commit)
    }

    /// Replicate the exact namespace, operation, values and request identity as
    /// one command. No receipt is returned until durable quorum acknowledgement.
    pub fn commit_application(
        &mut self,
        pg: PlacementGroupId,
        request: ClientRequestId,
        mode: ConsistencyMode,
        operation: FabricOperation,
        application: ApplicationPayload,
    ) -> RsfResult<DurableRequestCommit> {
        self.ensure_application_healthy()?;
        if let Some(original) = self.application_commands.get(&request).cloned() {
            if original.commit.pg != pg
                || original.commit.mode != mode
                || original.commit.operation != operation
                || original.application != application
            {
                return Err(invalid(
                    "application retry differs from its original complete command",
                ));
            }
            self.reconcile_application_commands()?;
            // Reconcile the exact retry with peers even when its local protocol
            // receipt exists; a lost remote response must not turn into local-only success.
            self.require_application_commit(&original)?;
            return Ok(DurableRequestCommit {
                certificate: original.certificate(),
                replayed: true,
            });
        }
        if self.router.request_certificate(&request).is_some() {
            return Err(invalid(
                "legacy request has no complete application command; explicit migration is required",
            ));
        }
        self.policy.validate_commit(mode, &operation)?;
        // Validate before resolving other work or writing a prepare.
        let preview = ReplicatedFabricCommand {
            commit: self.router.next_commit(pg, mode, operation.clone())?,
            request: request.clone(),
            application: application.clone(),
        };
        preview.encode()?;
        self.reconcile_application_commands()?;
        if self.router.request_certificate(&request).is_some() {
            return Err(invalid(
                "request identity belongs to a replicated protocol command",
            ));
        }
        let command = ReplicatedFabricCommand {
            commit: self.router.next_commit(pg, mode, operation)?,
            request: request.clone(),
            application,
        };
        self.prepare_application_command(&command)?;
        self.pending_commands.insert(
            command.commit.version,
            PendingCommand::Application(command.clone()),
        );
        self.application_commands.insert(request, command.clone());
        self.finish_application_command(&command)?;
        Ok(DurableRequestCommit {
            certificate: command.certificate(),
            replayed: false,
        })
    }

    /// Rebuild derived receipts from an installed committed prefix after a fresh
    /// quorum check. Accepted-only entries still use ordinary replication. A
    /// second prefix/quorum confirmation is required before reporting success.
    pub fn reconcile_installed_application_commands(&mut self) -> RsfResult<usize> {
        self.ensure_application_healthy()?;
        if self.provision_local_cells || self.cells.values().any(|cell| !cell.has_installed_view())
        {
            return Err(invalid(
                "prefix receipt recovery requires installed peer cells",
            ));
        }
        self.repair_and_confirm_read_quorum()?;
        let pending = self.pending_commands.values().cloned().collect::<Vec<_>>();
        let count = pending.len();
        for command in pending {
            match command {
                PendingCommand::Application(command) => {
                    if self.cells[&command.commit.pg].committed_operation(command.commit.index)
                        == Some(&command.vsr_operation()?)
                    {
                        self.append_application_receipt(&command)?;
                    } else {
                        self.finish_application_command(&command)?;
                    }
                }
                PendingCommand::Protocol(command) => {
                    if self.cells[&command.commit.pg].committed_operation(command.commit.index)
                        == Some(&command.vsr_operation()?)
                    {
                        self.append_protocol_receipt(&command)?;
                    } else {
                        self.finish_protocol_command(&command)?;
                    }
                }
            }
        }
        self.repair_and_confirm_read_quorum()?;
        Ok(count)
    }

    /// Confirm the configured primary against a current quorum before serving
    /// local application reads. The caller must serialize the read with writes.
    pub fn confirm_read_quorum(&mut self) -> RsfResult<()> {
        self.reconcile_application_commands()?;
        self.repair_and_confirm_read_quorum()
    }

    fn repair_and_confirm_read_quorum(&mut self) -> RsfResult<()> {
        for cell in self.cells.values_mut() {
            if cell.primary() != self.primary {
                return Err(invalid("local node is not the current read primary"));
            }
            let tail = cell.committed_tail();
            if tail.0 != 0 {
                cell.replicate_with_transport(self.primary, tail, &mut self.transport)?;
            }
        }
        self.confirm_primary_quorum()
    }

    /// Observe a quorum in every group at the current view, membership and
    /// committed tip. Unlike read reconciliation, this does not repair or append.
    pub fn confirm_primary_quorum(&mut self) -> RsfResult<()> {
        self.ensure_application_healthy()?;
        for cell in self.cells.values() {
            let probe = cell.primary_probe()?;
            if probe.primary != self.primary {
                return Err(invalid(
                    "local node is not the current placement-group primary",
                ));
            }
            let mut acknowledgements = 1;
            let mut failures = Vec::new();
            for replica in probe
                .voters
                .iter()
                .copied()
                .filter(|node| *node != self.primary)
            {
                match self.transport.confirm_primary(replica, &probe) {
                    Ok(()) => acknowledgements += 1,
                    Err(err) => failures.push(format!("node {}: {}", replica.0, err.reason)),
                }
            }
            if acknowledgements < cell.quorum() {
                return Err(RsfError::new(
                    ErrorCode::QuorumUnavailable,
                    format!(
                        "placement group {} primary quorum unavailable: {}/{}; {}",
                        probe.pg.0,
                        acknowledgements,
                        cell.quorum(),
                        failures.join("; ")
                    ),
                ));
            }
        }
        Ok(())
    }

    /// The ordered, locally certified commands available to an application
    /// materializer. Reconcile first to include an interrupted commit.
    pub fn committed_application_commands(&self) -> Vec<ReplicatedFabricCommand> {
        self.committed_application_commands_since(CommitVersion(0))
    }

    /// Return only certified application payloads newer than a materializer cursor.
    pub fn committed_application_commands_since(
        &self,
        after: CommitVersion,
    ) -> Vec<ReplicatedFabricCommand> {
        self.committed_applications
            .range((std::ops::Bound::Excluded(after), std::ops::Bound::Unbounded))
            .map(|(_, request)| self.application_commands[request].clone())
            .collect()
    }

    pub(super) fn ensure_application_healthy(&self) -> RsfResult<()> {
        if let Some(reason) = &self.application_failure {
            return Err(corrupt(format!(
                "application protocol persistence failed; reopen before continuing: {reason}"
            )));
        }
        Ok(())
    }
    fn prepare_application_command(&mut self, command: &ReplicatedFabricCommand) -> RsfResult<()> {
        let expected = command.vsr_operation()?;
        let cell = self
            .cells
            .get_mut(&command.commit.pg)
            .ok_or_else(|| invalid("application placement group is not mapped"))?;
        if cell.primary() != self.primary {
            return Err(invalid("local node is not the application primary"));
        }
        if let Some(prepared) = cell.prepared(command.commit.index) {
            if prepared.operation != expected {
                return Err(corrupt(
                    "application command conflicts with prepared VSR history",
                ));
            }
            return Ok(());
        }
        let bytes = command.encode()?.len() as u32;
        match cell.prepare(self.primary, expected, bytes) {
            Ok(VsrDecision::Prepared(prepare)) if prepare.index == command.commit.index => Ok(()),
            Ok(VsrDecision::Rejected { reason, .. }) => Err(invalid(reason)),
            Ok(_) => Err(corrupt(
                "application prepare returned an unexpected VSR index or decision",
            )),
            Err(err) => {
                self.application_failure = Some(err.message.clone());
                Err(err)
            }
        }
    }
    fn require_application_commit(&mut self, command: &ReplicatedFabricCommand) -> RsfResult<()> {
        self.prepare_application_command(command)?;
        let cell = self
            .cells
            .get_mut(&command.commit.pg)
            .expect("validated group");
        let prepared = cell
            .prepared(command.commit.index)
            .expect("validated prepare");
        if prepared.view != cell.view() && cell.committed_operation(command.commit.index).is_none()
        {
            cell.reprepare(self.primary, command.commit.index)?;
        }
        cell.replicate_with_transport(self.primary, command.commit.index, &mut self.transport)
    }
    fn finish_application_command(&mut self, command: &ReplicatedFabricCommand) -> RsfResult<()> {
        let next = self.router.next_commit(
            command.commit.pg,
            command.commit.mode,
            command.commit.operation.clone(),
        )?;
        if next != command.commit {
            return Err(corrupt(
                "application command would change its original commit version or index",
            ));
        }
        self.require_application_commit(command)?;
        self.append_application_receipt(command)
    }

    fn append_application_receipt(&mut self, command: &ReplicatedFabricCommand) -> RsfResult<()> {
        let next = self.router.next_commit(
            command.commit.pg,
            command.commit.mode,
            command.commit.operation.clone(),
        )?;
        if next != command.commit {
            return Err(corrupt(
                "application receipt would change its original commit version or index",
            ));
        }
        self.sync_protocol_epoch(command.commit.pg)?;
        match self
            .router
            .append_existing_request(command.commit.clone(), command.request.clone())
        {
            Ok(_) => {
                self.pending_commands.remove(&command.commit.version);
                self.committed_applications
                    .insert(command.commit.version, command.request.clone());
                Ok(())
            }
            Err(err) => {
                self.application_failure = Some(err.message.clone());
                Err(err)
            }
        }
    }
}

#[cfg(test)]
mod tests;
