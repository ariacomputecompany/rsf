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
        self.ensure_active_view()?;
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
        self.ensure_active_view()?;
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
        self.ensure_active_view()?;
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

impl DurableVsrCell {
    /// Commit an existing prepare and confirm durable commit evidence on a
    /// quorum. Retrying a locally committed index also repairs lagging peers.
    pub fn replicate_with_transport<T: VsrTransport>(
        &mut self,
        primary: NodeId,
        index: LogIndex,
        transport: &mut T,
    ) -> RsfResult<()> {
        self.ensure_active_view()?;
        if primary != self.primary() {
            return Err(invalid_input("replication requires the active primary"));
        }
        let prepare = self
            .prepared(index)
            .cloned()
            .ok_or_else(|| invalid_input("replication index has no prepare"))?;
        let already_committed = self.committed_operation(index).is_some();
        let replicas = self.replicas();
        let tail = self.committed_tail();
        let mut synchronized = BTreeSet::new();
        let mut failures = Vec::new();
        for replica in replicas.iter().copied().filter(|node| *node != primary) {
            let result = (|| {
                let remote_tail = transport.committed_tail(replica, prepare.pg)?;
                if remote_tail > tail {
                    return Err(VsrTransportError {
                        replica,
                        reason: "follower committed prefix is ahead of primary".into(),
                    });
                }
                // Validate the common tip as well as the missing suffix. A
                // matching height alone is not evidence of matching history.
                for next in remote_tail.0.max(1)..=tail.0 {
                    let commit = self.cell.committed[(next - 1) as usize].clone();
                    let entry = VsrCommittedEntry {
                        prepare: self.cell.pending[&LogIndex(next)].prepare.clone(),
                        commit,
                    };
                    transport.transfer_committed(replica, entry)?;
                }
                if already_committed && index != tail {
                    transport.transfer_committed(
                        replica,
                        VsrCommittedEntry {
                            prepare: prepare.clone(),
                            commit: self.cell.committed[(index.0 - 1) as usize].clone(),
                        },
                    )?;
                }
                if !already_committed {
                    let acknowledged = transport.acknowledge_prepare(VsrPrepareMessage {
                        prepare: prepare.clone(),
                        replica,
                    })?;
                    if acknowledged != replica {
                        return Err(VsrTransportError {
                            replica,
                            reason: "transport acknowledged a different replica".into(),
                        });
                    }
                    self.acknowledge(replica, prepare.view, index);
                }
                Ok(())
            })();
            match result {
                Ok(()) => {
                    synchronized.insert(replica);
                }
                Err(err) => failures.push(format!("node {}: {}", replica.0, err.reason)),
            }
        }
        if already_committed {
            return quorum_result(1 + synchronized.len(), self.quorum(), &failures);
        }
        let commit = match self.commit(primary, index)? {
            VsrDecision::Committed(commit) => commit,
            VsrDecision::Rejected { reason, .. } => {
                return Err(RsfError::new(
                    ErrorCode::QuorumUnavailable,
                    format!("{reason}; {}", failures.join("; ")),
                ));
            }
            _ => return Err(corrupt("unexpected replication commit decision")),
        };
        let mut durable = 1;
        for replica in synchronized {
            match transport.acknowledge_commit(replica, commit.clone()) {
                Ok(node) if node == replica => durable += 1,
                Ok(_) => failures.push(format!(
                    "node {}: transport acknowledged a different replica",
                    replica.0
                )),
                Err(err) => failures.push(format!("node {}: {}", replica.0, err.reason)),
            }
        }
        quorum_result(durable, self.quorum(), &failures)
    }
}

fn quorum_result(durable: usize, quorum: usize, failures: &[String]) -> RsfResult<()> {
    if durable >= quorum {
        Ok(())
    } else {
        Err(RsfError::new(
            ErrorCode::QuorumUnavailable,
            format!(
                "durable commit evidence reached {durable} replicas, requires {quorum}; {}",
                failures.join("; ")
            ),
        ))
    }
}
