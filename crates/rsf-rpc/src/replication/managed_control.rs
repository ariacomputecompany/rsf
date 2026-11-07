//! Configuration-owned control routing. This module grants no client admission.
use super::*;
use rsf_control_state::{
    ControlBootstrap, ControlCatalog, ControlCommand, ControlState, PublishedControlState,
};
mod application;
pub(super) mod initial;
pub(super) mod learner;
pub(super) mod native;
mod voter;
pub(super) mod writer;

#[derive(Debug)]
pub(super) struct Binding {
    anchor: ControlBootstrap,
    genesis: ControlCatalog,
    voter: Option<ControlState>,
    learner: Option<PublishedControlState>,
    writer: writer::Writer,
    initial_sender: Option<initial::sender::Sender>,
}
impl Binding {
    fn for_learner(
        anchor: ControlBootstrap,
        genesis: ControlCatalog,
        state: PublishedControlState,
    ) -> Self {
        Self {
            anchor,
            genesis,
            voter: None,
            learner: Some(state),
            writer: writer::Writer::default(),
            initial_sender: None,
        }
    }
    pub(super) fn pg(&self) -> PlacementGroupId {
        self.anchor.control_pg
    }
    pub(super) fn refresh_voter(&mut self, cell: &DurableVsrCell) -> Result<(), String> {
        if cell.committed_tail().0 == 0 {
            ControlState::validate_initial_replica(&self.anchor, &self.genesis, cell)
                .map_err(|e| e.message)?;
            self.voter = None;
        } else {
            if let Some(state) = &self.voter
                && state.validate_retained_history(cell).is_ok()
            {
                return Ok(());
            }
            let state = ControlState::recover(&self.anchor, cell).map_err(|e| e.message)?;
            state
                .validate_retained_history(cell)
                .map_err(|e| e.message)?;
            self.voter = Some(state);
        }
        self.learner = None;
        Ok(())
    }
    pub(super) fn record_learner_observation(&mut self, state: PublishedControlState) {
        self.learner = Some(state);
        self.voter = None;
    }
    fn refresh_learner(&mut self, learner: &super::learner::LearnerOwner) -> Result<(), String> {
        self.record_learner_observation(learner.control_observation().map_err(|e| e.message)?);
        Ok(())
    }
}
impl PeerReplicaServer {
    /// Bind an already hosted reserved control group to trusted provisioning.
    /// Neither network frames nor this configuration initiate managed work or
    /// authorize application clients. The ordinary data-primary may differ.
    pub fn with_control_replication(
        mut self,
        anchor: ControlBootstrap,
        genesis: ControlCatalog,
    ) -> Result<Self, String> {
        let pg = anchor.control_pg;
        if !self.cells.contains_key(&pg) {
            return Err("control learner requires policy before bounded cold open; raw learner enrollment is forbidden".into());
        }
        let actual = self
            .cells
            .keys()
            .chain(self.learners.keys())
            .copied()
            .collect();
        self.validate_control_configuration(&anchor, &genesis, actual)?;
        let mut binding = Binding {
            anchor,
            genesis,
            voter: None,
            learner: None,
            writer: writer::Writer::default(),
            initial_sender: None,
        };
        binding.refresh_voter(&self.cells[&pg])?;
        self.managed_control = Some(binding);
        Ok(self)
    }
    fn validate_control_configuration(
        &self,
        anchor: &ControlBootstrap,
        genesis: &ControlCatalog,
        actual: BTreeSet<PlacementGroupId>,
    ) -> Result<(), String> {
        if self.managed_control.is_some() {
            return Err("control replication is already configured".into());
        }
        if self
            .membership_recovery_bindings
            .contains_key(&anchor.control_pg)
        {
            return Err(
                "control membership recovery requires sealed control replay custody".into(),
            );
        }
        validate_policy(
            self.cluster,
            self.local,
            &self.identity,
            &self.nodes,
            anchor,
            genesis,
        )?;
        if actual
            != genesis
                .groups
                .iter()
                .map(|group| group.pg)
                .collect::<BTreeSet<_>>()
        {
            return Err("control provisioning requires the exact hosted group inventory".into());
        }
        Ok(())
    }
    pub fn control_placement_group(&self) -> Option<PlacementGroupId> {
        self.managed_control.as_ref().map(Binding::pg)
    }
    pub(super) fn is_control_group(&self, pg: PlacementGroupId) -> bool {
        self.control_placement_group() == Some(pg)
    }
    pub(super) fn control_stage_supported(&self, pg: PlacementGroupId) -> Result<(), String> {
        if self.is_control_group(pg) {
            Err("configured control membership staging/publication requires sealed control replay custody".into())
        } else {
            Ok(())
        }
    }
    /// Configuration grants no first-fence permission. Only the exact legacy
    /// transition independently enrolled by with_membership_recovery may keep
    /// mutating. This is migration custody, not authorization for a new intent.
    pub(super) fn control_membership_mutation(
        &mut self,
        pg: PlacementGroupId,
        proposal: Option<&rsf_vsr::VsrMembershipProposal>,
    ) -> Result<(), String> {
        if self.managed_control.is_none() {
            return self.validate_managed_membership_anchor(pg);
        }
        let bound = self.membership_recovery_bindings.get(&pg).ok_or_else(|| {
            "configured catalog membership requires committed intent and durable delivery authority".to_string()
        })?;
        if proposal.is_some_and(|proposal| proposal != bound) {
            return Err("configured catalog recovery cannot change its retained proposal".into());
        }
        // The shared owned-custody boundary selects actual chosen B (never a
        // minority accepted input), binds learner tokens and validates published
        // ancestry. Do not duplicate its A/B or physical journal rules here.
        self.validate_managed_membership_anchor(pg)
    }
    pub(super) fn validate_application_domain(
        &self,
        prepare: &rsf_vsr::VsrPrepare,
    ) -> Result<(), String> {
        if self.managed_control.is_some()
            && !self.is_control_group(prepare.pg)
            && let rsf_vsr::VsrOperation::ReplicatedCommand { payload, .. } = &prepare.operation
            && ControlCommand::has_domain_prefix(payload)
        {
            return Err("control command cannot enter a configured application group".into());
        }
        Ok(())
    }
    pub(super) fn preflight_received_control(
        &self,
        prepare: &rsf_vsr::VsrPrepare,
    ) -> Result<(), String> {
        self.validate_application_domain(prepare)?;
        if let Some(binding) = &self.managed_control
            && binding.pg() == prepare.pg
        {
            let cell = self
                .cells
                .get(&prepare.pg)
                .ok_or_else(|| "control group is not a voter".to_string())?;
            ControlState::preflight_received_prepare(
                &binding.anchor,
                &binding.genesis,
                cell,
                prepare,
            )
            .map_err(|e| e.message)?;
        }
        Ok(())
    }
    pub(super) fn preflight_committed_control(
        &self,
        entry: &VsrCommittedEntry,
    ) -> Result<(), String> {
        self.validate_application_domain(&entry.prepare)?;
        if let Some(binding) = &self.managed_control
            && binding.pg() == entry.prepare.pg
        {
            let cell = self
                .cells
                .get(&entry.prepare.pg)
                .ok_or_else(|| "control group is not a voter".to_string())?;
            ControlState::preflight_committed_extension(
                &binding.anchor,
                &binding.genesis,
                cell,
                entry,
            )
            .map_err(|e| e.message)?;
        }
        Ok(())
    }
    pub(super) fn materialize_owned_prefix(
        &mut self,
        pg: PlacementGroupId,
        apply: &mut impl FnMut(&VsrCommittedEntry) -> Result<(), String>,
    ) -> Result<(), String> {
        let cell = self
            .cells
            .get(&pg)
            .ok_or_else(|| "materialization group is not hosted as a voter".to_string())?;
        materialize_voter(self.managed_control.as_mut(), cell, apply)
    }
    pub(super) fn materialize_owned_learner(
        &mut self,
        pg: PlacementGroupId,
        apply: &mut impl FnMut(&VsrCommittedEntry) -> Result<(), String>,
    ) -> Result<(), String> {
        let learner = self
            .learners
            .get(&pg)
            .ok_or_else(|| "materialization group is not hosted as a learner".to_string())?;
        if let Some(binding) = &mut self.managed_control
            && binding.pg() == pg
        {
            return binding.refresh_learner(learner);
        }
        for entry in learner.committed_entries().map_err(|e| e.message)? {
            self.validate_application_domain(&entry.prepare)?;
            apply(&entry)?;
        }
        Ok(())
    }
}

pub(super) fn materialize_voter(
    binding: Option<&mut Binding>,
    cell: &DurableVsrCell,
    apply: &mut impl FnMut(&VsrCommittedEntry) -> Result<(), String>,
) -> Result<(), String> {
    if let Some(binding) = binding {
        if cell.membership_lineage().map_err(|e| e.message)?.pg == binding.pg() {
            return binding.refresh_voter(cell);
        }
        for entry in cell.committed_prefix().map_err(|e| e.message)? {
            if let rsf_vsr::VsrOperation::ReplicatedCommand { payload, .. } =
                &entry.prepare.operation
                && ControlCommand::has_domain_prefix(payload)
            {
                return Err("control command retained in configured application group".into());
            }
            apply(&entry)?;
        }
    } else {
        for entry in cell.committed_prefix().map_err(|e| e.message)? {
            apply(&entry)?;
        }
    }
    Ok(())
}

// Pure trusted-configuration checks precede both mixed and control-only opens.
fn validate_policy(
    cluster: [u8; 32],
    local_id: NodeId,
    identity: &PeerIdentity,
    nodes: &BTreeMap<[u8; 32], NodeId>,
    anchor: &ControlBootstrap,
    genesis: &ControlCatalog,
) -> Result<(), String> {
    let pg = anchor.control_pg;
    if cluster_digest(&anchor.cluster) != cluster
        || genesis.cluster != anchor.cluster
        || genesis.control_pg != pg
        || genesis.root().map_err(|e| e.message)? != anchor.genesis_catalog
        || genesis
            .group(pg)
            .is_none_or(|group| group.lineage.bootstrap != anchor.control_bootstrap)
    {
        return Err("control provisioning differs from server cluster/Genesis".into());
    }
    let local = genesis
        .nodes
        .iter()
        .find(|node| node.id == local_id)
        .ok_or_else(|| "local identity is absent from provisioned control catalog".to_string())?;
    if local.certificate != identity.fingerprint
        || genesis
            .nodes
            .iter()
            .any(|node| nodes.get(&node.certificate) != Some(&node.id))
    {
        return Err("peer certificate mapping differs from provisioned control identities".into());
    }
    Ok(())
}
