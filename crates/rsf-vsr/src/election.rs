//! Durable promises fence the old view before exporting immutable recovery input.
//! A promise is not an installed view or permission for a candidate to serve.
use super::*;

pub(super) const PROMISE_MAGIC: &[u8; 5] = b"VSRF1";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VsrViewChangeRequest {
    pub pg: PlacementGroupId,
    /// Minimum installed view admitted to this election; reports retain their actual normal view.
    pub previous_view: Epoch,
    pub target_view: Epoch,
    pub candidate: NodeId,
    pub voters: Vec<NodeId>,
}

impl VsrViewChangeRequest {
    pub fn encode(&self) -> RsfResult<Vec<u8>> {
        if self.target_view <= self.previous_view
            || self.voters.is_empty()
            || self.voters.len() > 4096
            || self.voters.windows(2).any(|pair| pair[0] >= pair[1])
            || !self.voters.contains(&self.candidate)
        {
            return Err(invalid_input("invalid view-change request"));
        }
        let mut out = PROMISE_MAGIC.to_vec();
        for value in [
            self.pg.0,
            self.previous_view.0,
            self.target_view.0,
            self.candidate.0,
        ] {
            out.extend_from_slice(&value.to_le_bytes());
        }
        out.extend_from_slice(&(self.voters.len() as u32).to_le_bytes());
        for node in &self.voters {
            out.extend_from_slice(&node.0.to_le_bytes());
        }
        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> RsfResult<Self> {
        if bytes.len() < 41 || !bytes.starts_with(PROMISE_MAGIC) {
            return Err(corrupt("invalid view-change promise encoding"));
        }
        let count = u32::from_le_bytes(bytes[37..41].try_into().unwrap()) as usize;
        if count > 4096 || bytes.len() != 41 + count * 8 {
            return Err(corrupt("invalid view-change promise voter count"));
        }
        let request = Self {
            pg: PlacementGroupId(read_u64(bytes, 5)?),
            previous_view: Epoch(read_u64(bytes, 13)?),
            target_view: Epoch(read_u64(bytes, 21)?),
            candidate: NodeId(read_u64(bytes, 29)?),
            voters: (0..count)
                .map(|i| read_u64(bytes, 41 + i * 8).map(NodeId))
                .collect::<RsfResult<_>>()?,
        };
        if request.encode()? != bytes {
            return Err(corrupt("noncanonical view-change promise"));
        }
        Ok(request)
    }

    pub(super) fn validate_cell(&self, cell: &VsrCell) -> RsfResult<()> {
        self.encode()?;
        if self.pg != cell.pg
            || self.previous_view > cell.view
            || self.target_view <= cell.view
            || self.voters != cell.replicas.iter().copied().collect::<Vec<_>>()
        {
            return Err(invalid_input(
                "view-change request differs from installed configuration",
            ));
        }
        Ok(())
    }

    pub(super) fn extends(&self, previous: &Self) -> bool {
        self == previous
            || (self.target_view > previous.target_view
                && self.pg == previous.pg
                && self.voters == previous.voters)
    }
}

/// Bound to a durable promise and a complete immutable WAL history. The log
/// includes accepted but uncommitted operations; committed height alone cannot
/// establish the log to carry into a new view. Authenticate the reporting node
/// at the transport boundary before using this as quorum evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VsrViewChangeReport {
    pub request: VsrViewChangeRequest,
    pub replica: NodeId,
    pub accepted_tail: LogIndex,
    pub history: VsrHistoryDescriptor,
}

impl VsrViewChangeReport {
    pub fn encode(&self) -> RsfResult<Vec<u8>> {
        if !self.request.voters.contains(&self.replica)
            || self.history.probe.pg != self.request.pg
            || self.history.probe.view < self.request.previous_view
            || self.history.probe.view >= self.request.target_view
            || self.history.probe.voters != self.request.voters
            || self.accepted_tail < self.history.probe.committed_tail
        {
            return Err(invalid_input("invalid view-change report"));
        }
        let request = self.request.encode()?;
        let mut out = b"VSRR1".to_vec();
        out.extend_from_slice(&(request.len() as u32).to_le_bytes());
        out.extend_from_slice(&request);
        out.extend_from_slice(&self.replica.0.to_le_bytes());
        out.extend_from_slice(&self.accepted_tail.0.to_le_bytes());
        out.extend_from_slice(&self.history.encode()?);
        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> RsfResult<Self> {
        if bytes.len() < 9 || !bytes.starts_with(b"VSRR1") {
            return Err(corrupt("invalid view-change report encoding"));
        }
        let length = u32::from_le_bytes(bytes[5..9].try_into().unwrap()) as usize;
        let end = 9usize
            .checked_add(length)
            .ok_or_else(|| corrupt("view-change report length overflow"))?;
        let request = VsrViewChangeRequest::decode(
            bytes
                .get(9..end)
                .ok_or_else(|| corrupt("truncated view-change report"))?,
        )?;
        let report = Self {
            request,
            replica: NodeId(read_u64(bytes, end)?),
            accepted_tail: LogIndex(read_u64(bytes, end + 8)?),
            history: VsrHistoryDescriptor::decode(
                bytes
                    .get(end + 16..)
                    .ok_or_else(|| corrupt("missing view-change history descriptor"))?,
            )?,
        };
        if report.encode()? != bytes {
            return Err(corrupt("noncanonical view-change report"));
        }
        Ok(report)
    }
}

#[derive(Debug)]
pub struct VsrViewChangeSnapshot {
    pub report: VsrViewChangeReport,
    pub records: Vec<VsrHistoryRecord>,
}

impl VsrViewChangeSnapshot {
    pub fn validate_integrity(&self) -> RsfResult<()> {
        self.report.encode()?;
        if self.records.len() as u64 != self.report.history.records
            || transfer::history_digest(&self.records)? != self.report.history.digest
        {
            return Err(corrupt("view-change history count or digest mismatch"));
        }
        let promise = self.report.request.encode()?;
        if !self.records.last().is_some_and(|record| {
            record.kind == WalRecordKind::ViewPromise
                && record.epoch == self.report.history.probe.view
                && record.payload == promise
        }) {
            return Err(corrupt(
                "view-change history does not end at its durable promise",
            ));
        }
        // The descriptor's identity binds the entire frozen history, including
        // prepares, certificates, prior configurations and client receipts.
        Ok(())
    }
}

impl DurableVsrCell {
    pub fn view_change_promise(&self) -> Option<&VsrViewChangeRequest> {
        self.view_change.as_ref()
    }

    pub fn begin_view_change(
        &mut self,
        replica: NodeId,
        sender: NodeId,
        request: VsrViewChangeRequest,
    ) -> RsfResult<VsrViewChangeSnapshot> {
        self.ensure_healthy()?;
        if self.membership_fence.is_some() {
            return Err(invalid_input(
                "complete the durable membership transition before an ordinary election",
            ));
        }
        request.validate_cell(&self.cell)?;
        if sender != request.candidate || !self.cell.replicas.contains(&replica) {
            return Err(invalid_input(
                "view-change requester or reporting replica is not an authenticated voter",
            ));
        }
        if !self.bootstrap_persisted {
            return Err(invalid_input(
                "migrate legacy bootstrap before promising a new view",
            ));
        }
        if self
            .view_change
            .as_ref()
            .is_some_and(|previous| !request.extends(previous))
        {
            return Err(invalid_input(
                "view-change promise would regress or change the candidate in the same view",
            ));
        }
        if self.view_change.as_ref() != Some(&request) {
            self.append_control(WalRecordKind::ViewPromise, &request.encode()?)?;
            self.view_change = Some(request.clone());
        }
        self.frozen_view_change_snapshot(replica, &request)
    }

    pub fn frozen_view_change_snapshot(
        &mut self,
        replica: NodeId,
        request: &VsrViewChangeRequest,
    ) -> RsfResult<VsrViewChangeSnapshot> {
        self.ensure_healthy()?;
        if self.membership_fence.is_some() {
            return Err(invalid_input(
                "membership transitions require membership recovery reports",
            ));
        }

        if self.view_change.as_ref() != Some(request) || !self.cell.replicas.contains(&replica) {
            return Err(invalid_input("requested view-change promise is not active"));
        }
        let snapshot = self.history_snapshot_for_recovery()?;
        let value = VsrViewChangeSnapshot {
            report: VsrViewChangeReport {
                request: request.clone(),
                replica,
                accepted_tail: LogIndex(self.cell.next_index - 1),
                history: snapshot.descriptor,
            },
            records: snapshot.records,
        };
        value.validate_integrity()?;
        Ok(value)
    }

    pub(super) fn ensure_active_view(&self) -> RsfResult<()> {
        self.ensure_healthy()?;
        if let Some(proposal) = &self.membership_fence {
            return Err(RsfError::new(
                ErrorCode::QuorumUnavailable,
                format!(
                    "VSR membership transition from view {} to {} is fenced; old-quorum approval does not activate proposed voters",
                    proposal.election.previous_view.0, proposal.election.target_view.0
                ),
            ));
        }

        if let Some(promise) = &self.view_change {
            return Err(RsfError::new(
                ErrorCode::QuorumUnavailable,
                format!(
                    "VSR view {} is fenced by durable promise for view {} candidate {}; quorum-selected installation is required",
                    self.cell.view.0, promise.target_view.0, promise.candidate.0
                ),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
