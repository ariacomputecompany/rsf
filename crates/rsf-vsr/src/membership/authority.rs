use super::*;
impl DurableVsrCell {
    /// Validate an old-quorum transition against this existing cell's original
    /// bootstrap, historical authority and committed operations. No role changes.
    pub fn validate_membership_authority(&self, approval: &VsrMembershipApproval) -> RsfResult<()> {
        self.ensure_healthy()?;
        if !self.bootstrap_persisted {
            return Err(invalid_input(
                "membership authority requires a durable bootstrap anchor",
            ));
        }
        if approval.proposal().election.pg != self.cell.pg
            || approval.proposal().election.previous_view < self.cell.view
        {
            return Err(invalid_input(
                "membership authority regresses or differs from hosted group",
            ));
        }
        for snapshot in approval.snapshots() {
            if snapshot.records.first().map(|r| r.payload.as_slice())
                != Some(self.initial_bootstrap.as_slice())
            {
                return Err(corrupt(
                    "membership authority differs from hosted bootstrap",
                ));
            }
            let recovered = snapshot.recover_state()?;
            if !recovered.1.contains_authority(&self.history) {
                return Err(corrupt(
                    "membership authority omits or changes hosted historical configurations",
                ));
            }
            for (request, index) in &self.cell.client_table {
                if recovered
                    .0
                    .client_table
                    .get(request)
                    .is_some_and(|other| other != index)
                {
                    return Err(corrupt(
                        "membership authority remaps a hosted client receipt",
                    ));
                }
            }
        }
        let source = approval
            .snapshots()
            .iter()
            .max_by_key(|s| {
                (
                    s.report.voter.accepted_tail,
                    std::cmp::Reverse(s.report.voter.replica),
                )
            })
            .ok_or_else(|| corrupt("membership authority has no snapshots"))?;
        let selected = source.recover_state()?;
        for pending in self.cell.pending.values().filter(|p| p.committed) {
            if !selected
                .0
                .pending
                .get(&pending.prepare.index)
                .is_some_and(|entry| installation::same_operation(&entry.prepare, &pending.prepare))
            {
                return Err(corrupt(
                    "membership authority drops or changes a hosted committed operation",
                ));
            }
        }
        Ok(())
    }
}
