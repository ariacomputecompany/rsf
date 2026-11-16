use super::*;
use rsf_vsr::VsrPrepare;
mod receive;

pub(super) struct AcceptedReplay {
    state: ReplayState,
    bytes: usize,
    next: LogIndex,
}
impl ControlState {
    pub(super) fn validate_projection(&self, control: &DurableVsrCell) -> RsfResult<()> {
        if self.stamp != control.recovery_state_token()? {
            return Err(invalid(
                "control projection is stale; replay the current committed cell",
            ));
        }
        Ok(())
    }

    /// Validate the entire bounded retained accepted suffix in original order,
    /// returning its first unresolved index (if any). Reconcile that exact
    /// operation through VSR before proposing a new command. This does not
    /// commit, discard, reprepare, or expose simulated pending work as authority.
    ///
    /// Canonical commands with deterministic Rejected outcomes are valid log
    /// work and retain their receipt/capacity effects in the simulation. Ordinary
    /// older-view prepares remain valid only within the same certified membership
    /// lineage. A foreign/malformed tail or a membership-boundary reprepare
    /// fails closed. The caller must own the current primary; this helper checks
    /// active view custody but does not authenticate a caller or grant a lease.
    pub fn preflight_accepted_tail(&self, control: &DurableVsrCell) -> RsfResult<Option<LogIndex>> {
        control.primary_probe()?;
        let replay = self.replay_accepted(control)?;
        self.validate_projection(control)?;
        let first = self
            .state
            .tail
            .0
            .checked_add(1)
            .ok_or_else(|| invalid("control log index exhausted"))?;
        Ok((replay.next.0 > first).then_some(LogIndex(first)))
    }

    /// Check a prospective received PREPARE against current owned control
    /// custody and the complete accepted suffix. Exact retries/reprepare retain
    /// the full operation and byte count at the original index; only a new next
    /// index may append. Authentication, follower eligibility, VSR certificate
    /// checks and durable receive_prepare remain mandatory caller/kernel duties.
    ///
    /// This validates command domain, deterministic replay and reserved capacity;
    /// it grants no first-fence authority and does not require a successful
    /// business outcome. Rejected canonical commands still reconcile normally.
    pub fn preflight_prepare(
        &self,
        control: &DurableVsrCell,
        prepare: &VsrPrepare,
    ) -> RsfResult<usize> {
        self.validate_projection(control)?;
        let active = control.primary_probe()?;
        if prepare.pg != self.state.anchor.control_pg
            || prepare.pg != active.pg
            || prepare.view != active.view
            || prepare.primary != active.primary
        {
            return Err(invalid(
                "control prepare differs from current PG/view/primary",
            ));
        }
        let command = checked_command(prepare)?;
        let mut replay = self.replay_accepted(control)?;
        if let Some(retained) = control.prepared(prepare.index) {
            if retained.operation != prepare.operation
                || retained.bytes != prepare.bytes
                || (prepare.index <= self.state.tail && retained != prepare)
                || prepare.view < retained.view
            {
                return Err(invalid(
                    "control prepare would replace an existing stable operation",
                ));
            }
        } else {
            if prepare.index != replay.next {
                return Err(invalid(
                    "control prepare would leave a gap or reuse an absent index",
                ));
            }
            let authority = control.membership_lineage()?;
            replay.append(prepare.index, &authority, command, prepare.bytes as usize)?;
        }
        self.validate_projection(control)?;
        Ok(prepare.bytes as usize)
    }

    fn replay_accepted(&self, control: &DurableVsrCell) -> RsfResult<AcceptedReplay> {
        self.validate_projection(control)?;
        let authority = control.membership_lineage()?;
        let next = control.next_index();
        let mut replay =
            AcceptedReplay::new(self.state.clone(), self.replay_bytes, &authority, next)?;
        for index in replay.next.0..next.0 {
            let prepare = control
                .prepared(LogIndex(index))
                .ok_or_else(|| corrupt("accepted control history has a gap"))?;
            let historical = control
                .membership_lineage_at(prepare.view)?
                .ok_or_else(|| corrupt("accepted control prepare has no historical lineage"))?;
            replay.push(prepare, &historical, &authority)?;
        }
        replay.finish(next)
    }

    /// Call before proposing, under exclusive control-cell custody, after
    /// reconciling any accepted tail. Reserve one maximum finalization before
    /// allowing an intent to fence data. No checkpoint exists in this format.
    /// This is a capacity check, not a proposal-freshness authorization. The
    /// writer and each fence receiver must validate the exact concrete proposal
    /// against their live cell immediately before effects. An ordinary election
    /// leaves a stale pending proposal intact; this API never rebases or cancels it.
    pub fn preflight_command(
        &self,
        control: &DurableVsrCell,
        command: &ControlCommand,
    ) -> RsfResult<usize> {
        self.validate_projection(control)?;
        if self.state.tail.0.checked_add(1) != Some(control.next_index().0) {
            return Err(invalid(
                "control preflight requires current replay and reconciled accepted tail",
            ));
        }
        self.state.reserve_begin_receipt(command)?;
        let bytes = command.encode()?.len();
        let pending = self.pending();
        control.primary_probe()?;
        let completes_pending =
            if let (ControlCommand::Finalize(finalization), Some(_)) = (command, pending) {
                let mut simulated = self.state.clone();
                simulated.finalize(finalization.clone(), &control.membership_lineage()?)?;
                simulated.pending.is_none()
            } else {
                false
            };
        let reservation = if completes_pending {
            0
        } else if let Some(intent) = pending {
            intent.preflight()?.maximum_encoded_bytes
        } else if let ControlCommand::Begin(intent) = command {
            intent.preflight()?.maximum_encoded_bytes
        } else {
            0
        };
        let total = self
            .replay_bytes
            .checked_add(bytes)
            .ok_or_else(|| invalid("control history budget overflow"))?;
        check_budget(total, control.next_index(), reservation)?;
        Ok(bytes)
    }
}
impl AcceptedReplay {
    pub(super) fn new(
        state: ReplayState,
        bytes: usize,
        authority: &VsrMembershipLineage,
        next: LogIndex,
    ) -> RsfResult<Self> {
        if authority.pg != state.anchor.control_pg
            || authority.bootstrap != state.anchor.control_bootstrap
            || authority.voters.len() > MAX_NODES
        {
            return Err(invalid(
                "accepted control history has foreign or oversized authority",
            ));
        }
        let first = state
            .tail
            .0
            .checked_add(1)
            .ok_or_else(|| invalid("control log index exhausted"))?;
        if next.0 < first || next.0 > MAX_CONTROL_RECORDS as u64 + 1 {
            return Err(invalid(
                "accepted control history exceeds qualified record bound",
            ));
        }
        Ok(Self {
            state,
            bytes,
            next: LogIndex(first),
        })
    }
    pub(super) fn push(
        &mut self,
        prepare: &VsrPrepare,
        historical: &VsrMembershipLineage,
        current: &VsrMembershipLineage,
    ) -> RsfResult<()> {
        if prepare.pg != current.pg || prepare.index != self.next {
            return Err(corrupt("accepted control prepare has a foreign PG/index"));
        }
        if historical != current {
            return Err(invalid(
                "accepted control operation cannot reprepare across membership lineage",
            ));
        }
        self.append(
            prepare.index,
            historical,
            checked_command(prepare)?,
            prepare.bytes as usize,
        )
    }
    pub(super) fn finish(self, expected: LogIndex) -> RsfResult<Self> {
        if self.next != expected {
            return Err(corrupt("accepted control history has a gap"));
        }
        self.reserve_finalization()?;
        Ok(self)
    }
    fn append(
        &mut self,
        index: LogIndex,
        authority: &VsrMembershipLineage,
        command: ControlCommand,
        bytes: usize,
    ) -> RsfResult<()> {
        if index != self.next {
            return Err(corrupt("accepted control simulation is not contiguous"));
        }
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .ok_or_else(|| invalid("accepted control replay byte overflow"))?;
        check_budget(self.bytes, index, 0)?;
        // Business rejection is a deterministic committed result, not malformed
        // storage. Never replace a retained operation to force successful Begin.
        self.state.apply(index, authority, command)?;
        self.next = LogIndex(
            index
                .0
                .checked_add(1)
                .ok_or_else(|| invalid("control log index exhausted"))?,
        );
        self.reserve_finalization()
    }
    fn reserve_finalization(&self) -> RsfResult<()> {
        let reservation = self
            .state
            .pending
            .as_ref()
            .map(|pending| {
                pending
                    .intent
                    .preflight()
                    .map(|budget| budget.maximum_encoded_bytes)
            })
            .transpose()?
            .unwrap_or(0);
        check_budget(self.bytes, self.state.tail, reservation)
    }
}
fn check_budget(bytes: usize, last: LogIndex, reservation: usize) -> RsfResult<()> {
    let total = bytes
        .checked_add(reservation)
        .ok_or_else(|| invalid("control history budget overflow"))?;
    let records = last
        .0
        .checked_add(u64::from(reservation != 0))
        .ok_or_else(|| invalid("control history record budget overflow"))?;
    if total > MAX_REPLAY_BYTES || records > MAX_CONTROL_RECORDS as u64 {
        return Err(invalid(
            "control history has no qualified capacity for command and finalization",
        ));
    }
    Ok(())
}
pub(super) fn checked_command(prepare: &VsrPrepare) -> RsfResult<ControlCommand> {
    let VsrOperation::ReplicatedCommand { payload, .. } = &prepare.operation else {
        return Err(corrupt("non-control operation in accepted control history"));
    };
    if prepare.index.0 == 0
        || payload.is_empty()
        || payload.len() > MAX_COMMAND_BYTES
        || prepare.bytes as usize != payload.len()
    {
        return Err(corrupt(
            "accepted control operation has invalid index/byte bounds",
        ));
    }
    ControlCommand::decode(payload).map_err(|error| corrupt(error.message))
}

impl ReplayState {
    /// A fresh writer must be able to retain its request's retry identity.
    /// Accepted replay still executes deterministic Capacity outcomes unchanged.
    pub(crate) fn reserve_begin_receipt(&self, command: &ControlCommand) -> RsfResult<()> {
        if let ControlCommand::Begin(intent) = command
            && !self.receipts.contains_key(&intent.request)
            && self.receipts.len() == MAX_RECEIPTS
        {
            return Err(invalid("control request receipt capacity exhausted"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finalization_reservation_cannot_exhaust_or_wrap_qualified_history_capacity() {
        let finalization = 4096;
        let occupied = MAX_REPLAY_BYTES - finalization;
        let penultimate = LogIndex(MAX_CONTROL_RECORDS as u64 - 1);
        assert!(check_budget(occupied, penultimate, finalization).is_ok());
        assert!(check_budget(occupied + 1, penultimate, finalization).is_err());
        assert!(
            check_budget(occupied, LogIndex(MAX_CONTROL_RECORDS as u64), finalization).is_err()
        );
        // Once a successful finalization consumes the reservation, the exact
        // qualified boundary can be retained; no additional record is reserved.
        assert!(check_budget(MAX_REPLAY_BYTES, LogIndex(MAX_CONTROL_RECORDS as u64), 0).is_ok());
        assert!(check_budget(usize::MAX, penultimate, finalization).is_err());
        assert!(check_budget(occupied, LogIndex(u64::MAX), finalization).is_err());
    }
}
