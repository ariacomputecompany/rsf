//! Explicit STAGE source selection; never a generic retained-read error fallback.
use super::*;
mod prefix;
use prefix::Prefix;

impl MembershipDecisionStore {
    /// Resolve an exact STAGE input. Complete retained authority wins. A lazy
    /// replay-validated source is consulted only for genuine unbound absence or
    /// a positively inspected staged-only prefix. Authority/accepted/chosen loss
    /// remains a strict error. No file or journal is created or repaired here.
    ///
    /// This returns only an opaque complete approval, not permission to bypass
    /// hosted-role validation or the original mutable owner's retain_chosen_input.
    /// The RPC caller must preserve that existing validation-before-effect order.
    pub fn stage_approval(
        &self,
        chosen: &VsrMembershipChosen,
        supply: impl FnOnce() -> RsfResult<VsrMembershipApproval>,
    ) -> RsfResult<VsrMembershipApproval> {
        self.validate_choice(chosen)?;
        let mut guard = SupplierGuard::new(self);
        let namespace = prefix::Namespace::capture(self)?;
        let id = chosen.input_id();
        let staged_only = self.state.staged.is_some_and(|(_, value)| value == id)
            && id != self.authority
            && self.state.accepted.is_none_or(|(_, value)| value != id)
            && self
                .state
                .chosen
                .as_ref()
                .is_none_or(|value| value.input_id() != id);
        if !staged_only {
            // Deliberately preserve generic strict lookup and every error.
            if let Some(approval) = self.retained_approval(id)? {
                namespace.validate(self)?;
                self.validate_choice(chosen)?;
                guard.disarm();
                return Ok(approval);
            }
        }
        // Journal custody alone does not prove the acknowledged input files
        // still exist. Pin and replay every distinct acknowledged dependency.
        let mut bound_ids = vec![self.authority];
        if let Some((_, accepted)) = self.state.accepted {
            bound_ids.push(accepted);
        }
        if let Some(chosen) = &self.state.chosen {
            bound_ids.push(chosen.input_id());
        }
        bound_ids.sort_unstable();
        bound_ids.dedup();
        let mut bound = Vec::new();
        for bound_id in bound_ids {
            let snapshot = Prefix::read(self, bound_id)?;
            let approval = snapshot.input.validate_records(&snapshot.records)?;
            if bootstrap_id(&approval)? != self.bootstrap {
                return Err(corrupt(
                    "membership STAGE acknowledged input lost its anchor",
                ));
            }
            bound.push(snapshot);
        }
        let prefix = if staged_only {
            Some(Prefix::read(self, id)?)
        } else {
            None
        };
        if let Some(prefix) = &prefix
            && prefix.records.len() as u64 == prefix.input.records()?
        {
            let approval = prefix.input.validate_records(&prefix.records)?;
            self.validate_stage_source(chosen, &approval)?;
            prefix.validate()?;
            for snapshot in &bound {
                snapshot.validate()?;
            }
            self.validate_choice(chosen)?;
            namespace.validate(self)?;
            guard.disarm();
            return Ok(approval);
        }
        // A staged reference is resumable data, not an acceptance. Retain its
        // actual physical owner and bytes while obtaining the complete source.
        let state = self.state.clone();
        let supplied = supply();
        self.validate_choice(chosen)?;
        if let Some(prefix) = &prefix {
            prefix.validate()?;
        }
        namespace.validate(self)?;
        for snapshot in &bound {
            snapshot.validate()?;
        }
        if self.state != state {
            return Err(corrupt("membership STAGE state changed during supply"));
        }
        if !staged_only && self.retained_approval(id)?.is_some() {
            return Err(corrupt("membership STAGE source changed during supply"));
        }
        guard.disarm();
        let approval = supplied?;
        let input = self.validate_stage_source(chosen, &approval)?;
        if let Some(prefix) = &prefix {
            let records = input.flattened_records(&approval)?;
            if input != prefix.input
                || records.get(..prefix.records.len()) != Some(prefix.records.as_slice())
            {
                return Err(corrupt(
                    "membership STAGE source changes retained descriptor or prefix",
                ));
            }
        }
        // The normal stage path next validates the hosted anchor and invokes
        // retain_chosen_input on this same owner, which rechecks current state,
        // descriptor, complete acknowledged inputs and every copied prefix byte.
        Ok(approval)
    }

    fn validate_stage_source(
        &self,
        chosen: &VsrMembershipChosen,
        approval: &VsrMembershipApproval,
    ) -> RsfResult<VsrMembershipInput> {
        let input = VsrMembershipInput::from_approval(approval)?;
        if input.proposal() != &self.proposal
            || input.id()? != chosen.input_id()
            || bootstrap_id(approval)? != self.bootstrap
        {
            return Err(invalid_input(
                "membership STAGE source differs from chosen anchored input",
            ));
        }
        Ok(input)
    }
}

// A supplier error alone does not poison unchanged custody. Physical failure
// after a supplier, or unwinding through that boundary, fences the actual owner.
struct SupplierGuard<'a> {
    owner: &'a MembershipDecisionStore,
    armed: bool,
}
impl<'a> SupplierGuard<'a> {
    fn new(owner: &'a MembershipDecisionStore) -> Self {
        Self { owner, armed: true }
    }
    fn disarm(&mut self) {
        self.armed = false;
    }
}
impl Drop for SupplierGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.owner.custody_failed.store(true, Ordering::Relaxed);
        }
    }
}
