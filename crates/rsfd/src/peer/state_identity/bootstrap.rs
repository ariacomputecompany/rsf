//! Native durable CAS consumes actual provisional completion, never a DTO.
use super::*;
use rsf_vsr::{VsrBootstrapCompletion, VsrBootstrapSpec};

impl StateIdentity {
    pub(in crate::peer) fn fail_bootstrap(&mut self) {
        self.failed = true;
    }

    pub(in crate::peer) fn persist_bootstrap(
        &mut self,
        policy: &ControlInitialPolicy,
        expected: &VsrBootstrapSpec,
        completion: &VsrBootstrapCompletion<'_>,
    ) -> Result<(), String> {
        self.validate()?;
        completion.validate().map_err(|e| e.message)?;
        let reservation = self
            .value
            .control
            .as_ref()
            .ok_or("control reservation is absent")?;
        let group = policy
            .genesis
            .group(policy.anchor.control_pg)
            .ok_or("control group absent")?;
        if reservation.custody != (ControlCustody::ReservedVoter {})
            || !reservation.matches(policy)?
            || completion.bootstrap() != expected
            || completion.limits() != policy.limits
            || expected.pg() != policy.anchor.control_pg
            || expected.bootstrap_digest() != policy.anchor.control_bootstrap
            || expected.voters() != group.voters
            || !group.voters.contains(&self.value.node)
            || !policy.genesis.nodes.iter().any(|node| {
                node.id == self.value.node && node.role == rsf_control_state::AdmissionRole::Voter
            })
            || completion.path() != self.custody.root().join(format!("pg-{}", expected.pg().0))
        {
            return Err("bootstrap completion differs from exact native voter reservation".into());
        }
        let mut next = self.value.clone();
        next.control.as_mut().expect("reservation checked").custody =
            ControlCustody::InitializedVoter {};
        self.replace(next)?;
        completion.validate().map_err(|e| e.message)?;
        self.validate()
    }
}
