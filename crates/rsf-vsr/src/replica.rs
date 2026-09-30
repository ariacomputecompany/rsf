use super::*;

const MAX_MESSAGE_BYTES: usize = MAX_REPLICATED_COMMAND_BYTES + 64 * 1024;

pub(super) fn validate_payload(operation: &VsrOperation, bytes: u32) -> Result<(), String> {
    if let VsrOperation::ReplicatedCommand { payload, .. } = operation {
        if payload.is_empty() || payload.len() > MAX_REPLICATED_COMMAND_BYTES {
            return Err("replicated command must contain 1..=1048576 bytes".into());
        }
        if payload.len() != bytes as usize {
            return Err("replicated command byte count does not match payload".into());
        }
    }
    Ok(())
}

impl VsrPrepare {
    pub fn encode(&self) -> RsfResult<Vec<u8>> {
        validate_prepare(self)?;
        let bytes = encode_prepare(self)?;
        if bytes.len() > MAX_MESSAGE_BYTES {
            return Err(invalid_input("VSR prepare exceeds message limit"));
        }
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> RsfResult<Self> {
        if bytes.len() > MAX_MESSAGE_BYTES {
            return Err(corrupt("VSR prepare exceeds message limit"));
        }
        let prepare = decode_prepare(bytes)?;
        validate_prepare(&prepare).map_err(|err| corrupt(err.message))?;
        Ok(prepare)
    }
}

impl VsrCommit {
    pub fn encode(&self) -> RsfResult<Vec<u8>> {
        // Public callers may construct a certificate, so do not depend on the
        // internal encoder's assumption that a valid prepare already exists.
        if self.certificate.pg != self.pg
            || self.certificate.view != self.view
            || self.certificate.index != self.index
            || self.certificate.primary != self.primary
        {
            return Err(invalid_input("VSR certificate header differs from commit"));
        }
        let mut operation = Vec::new();
        encode_operation(&self.certificate.operation, &mut operation)?;
        if operation.len() > MAX_MESSAGE_BYTES || self.certificate.signers.len() > 4096 {
            return Err(invalid_input("VSR commit exceeds message limit"));
        }
        let bytes = encode_commit(self);
        Self::decode(&bytes)?;
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> RsfResult<Self> {
        if bytes.len() > MAX_MESSAGE_BYTES {
            return Err(corrupt("VSR commit exceeds message limit"));
        }
        let commit = decode_commit(bytes)?;
        if commit.index.0 == 0
            || commit.certificate.signers.len() > 4096
            || !commit.certificate.signers.contains(&commit.primary)
        {
            return Err(corrupt(
                "VSR commit requires a nonzero index and primary signer",
            ));
        }
        Ok(commit)
    }
}

fn validate_prepare(prepare: &VsrPrepare) -> RsfResult<()> {
    if prepare.index.0 == 0 || prepare.bytes == 0 {
        return Err(invalid_input(
            "VSR prepare requires a nonzero index and byte count",
        ));
    }
    validate_payload(&prepare.operation, prepare.bytes).map_err(invalid_input)
}

impl DurableVsrCell {
    /// Persist a leader's complete prepare before acknowledging it. `sender`
    /// must come from the authenticated transport, never the message body.
    pub fn receive_prepare(
        &mut self,
        replica: NodeId,
        sender: NodeId,
        prepare: VsrPrepare,
    ) -> RsfResult<NodeId> {
        self.ensure_healthy()?;
        self.validate_peer(replica, sender, prepare.pg, prepare.view, prepare.primary)?;
        validate_prepare(&prepare)?;
        let next = prepare
            .index
            .0
            .checked_add(1)
            .ok_or_else(|| invalid_input("VSR log index exhausted"))?;
        if let Some(existing) = self.cell.pending.get(&prepare.index) {
            if existing.prepare == prepare {
                return Ok(replica);
            }
            if existing.committed || prepare.view <= existing.prepare.view {
                return Err(invalid_input(
                    "conflicting prepare at an existing VSR index",
                ));
            }
        } else if prepare.index.0 != self.cell.next_index {
            return Err(invalid_input(
                "VSR prepare would leave a gap; transfer the missing prefix first",
            ));
        }
        self.append(WalRecordKind::Prepare, &prepare.encode()?)?;
        self.cell.next_index = self.cell.next_index.max(next);
        self.cell.pending.insert(
            prepare.index,
            PendingPrepare {
                prepare,
                acknowledgements: [sender, replica].into_iter().collect(),
                committed: false,
            },
        );
        Ok(replica)
    }

    /// Persist a quorum certificate before reporting the new committed prefix.
    pub fn receive_commit(
        &mut self,
        replica: NodeId,
        sender: NodeId,
        commit: VsrCommit,
    ) -> RsfResult<LogIndex> {
        self.ensure_healthy()?;
        self.validate_peer(replica, sender, commit.pg, commit.view, commit.primary)?;
        let pending = self
            .cell
            .pending
            .get(&commit.index)
            .ok_or_else(|| invalid_input("commit has no durable prepare"))?;
        validate_commit_certificate(
            &commit,
            &pending.prepare,
            &self.cell.replicas,
            self.cell.quorum(),
        )?;
        if pending.committed {
            return Ok(self.cell.committed_tail());
        }
        if self.cell.committed_tail().0.checked_add(1) != Some(commit.index.0) {
            return Err(invalid_input("commit must extend the contiguous prefix"));
        }
        self.append(WalRecordKind::Commit, &commit.encode()?)?;
        self.cell
            .pending
            .get_mut(&commit.index)
            .expect("validated prepare")
            .committed = true;
        self.cell.committed.push(commit);
        Ok(self.cell.committed_tail())
    }

    /// Re-propose the same uncommitted operation after installing a new view.
    /// Votes from the prior view are discarded and must be collected again.
    pub fn reprepare(&mut self, primary: NodeId, index: LogIndex) -> RsfResult<VsrPrepare> {
        self.ensure_healthy()?;
        if primary != self.cell.primary {
            return Err(invalid_input("reprepare rejected from non-primary"));
        }
        let pending = self
            .cell
            .pending
            .get(&index)
            .ok_or_else(|| invalid_input("reprepare index is unknown"))?;
        if pending.committed {
            return Err(invalid_input("committed prepare cannot change view"));
        }
        if pending.prepare.view == self.cell.view {
            return Ok(pending.prepare.clone());
        }
        let prepare = VsrPrepare {
            view: self.cell.view,
            primary,
            ..pending.prepare.clone()
        };
        self.append(WalRecordKind::Prepare, &prepare.encode()?)?;
        self.cell.pending.insert(
            index,
            PendingPrepare {
                prepare: prepare.clone(),
                acknowledgements: [primary].into_iter().collect(),
                committed: false,
            },
        );
        Ok(prepare)
    }

    fn validate_peer(
        &self,
        replica: NodeId,
        sender: NodeId,
        pg: PlacementGroupId,
        view: Epoch,
        primary: NodeId,
    ) -> RsfResult<()> {
        if pg != self.cell.pg
            || view != self.cell.view
            || primary != self.cell.primary
            || sender != primary
        {
            return Err(invalid_input(
                "replication message does not match the authenticated primary and active view",
            ));
        }
        if replica == primary || !self.cell.replicas.contains(&replica) {
            return Err(invalid_input(
                "replication target is not a follower in the active voter set",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
