use crate::*;
use rsf_core::{LogIndex, NodeId, PlacementGroupId};
use rsf_vsr::{DurableVsrCell, VsrMembershipLineage, VsrOperation, VsrStateToken};
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Rejection {
    AlreadyInitialized,
    StaleParent,
    RequestConflict,
    Busy,
    Capacity,
    InvalidPlan,
    IncompleteGroups,
    IdentityChanged,
    UnsupportedRoutingChange,
    WrongAuthority,
    MissingIntent,
    PublicationConflict,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ControlOutcome {
    CatalogApplied {
        revision: ControlRevision,
        root: CatalogRoot,
    },
    IntentAccepted {
        intent: IntentId,
    },
    Rejected(Rejection),
}

/// No public entry-slice constructor: only the healthy authoritative cell creates
/// this projection. A caller-provided certificate or voter list is insufficient.
#[derive(Debug)]
pub struct ControlState {
    state: ReplayState,
    stamp: VsrStateToken,
    replay_bytes: usize,
}
impl ControlState {
    pub fn recover(anchor: &ControlBootstrap, cell: &DurableVsrCell) -> RsfResult<Self> {
        anchor.validate()?;
        let stamp = cell.recovery_state_token()?;
        let current = cell.membership_lineage()?;
        if current.pg != anchor.control_pg || current.bootstrap != anchor.control_bootstrap {
            return Err(corrupt(
                "control cell does not extend provisioned bootstrap",
            ));
        }
        // Check aggregate allocation before committed_prefix clones payloads.
        let tail = cell.committed_tail().0;
        if tail > MAX_CONTROL_RECORDS as u64 || cell.committed_count() > MAX_CONTROL_RECORDS {
            return Err(invalid(
                "control history exceeds qualified replay record bound",
            ));
        }
        let mut bytes = 0usize;
        for index in 1..=tail {
            if let Some(operation) = cell.committed_operation(LogIndex(index)) {
                let VsrOperation::ReplicatedCommand { payload, .. } = operation else {
                    return Err(corrupt("non-control operation in control history"));
                };
                bytes = bytes
                    .checked_add(payload.len())
                    .ok_or_else(|| corrupt("control replay byte overflow"))?;
                if bytes > MAX_REPLAY_BYTES {
                    return Err(invalid(
                        "control history exceeds qualified replay byte bound",
                    ));
                }
                let prepare = cell
                    .prepared(LogIndex(index))
                    .ok_or_else(|| corrupt("committed control operation has no prepare"))?;
                let historical = cell
                    .membership_lineage_at(prepare.view)?
                    .ok_or_else(|| corrupt("control operation has no historical lineage"))?;
                if historical.voters.len() > MAX_NODES {
                    return Err(invalid("control history exceeds qualified voter bound"));
                }
            }
        }
        let entries = cell.committed_prefix()?;
        let mut state = ReplayState::new(anchor.clone());
        for entry in entries {
            if entry.prepare.pg != anchor.control_pg
                || entry.commit.pg != anchor.control_pg
                || entry.prepare.index != entry.commit.index
                || entry.prepare.operation != entry.commit.certificate.operation
            {
                return Err(corrupt("control committed entry identity mismatch"));
            }
            let VsrOperation::ReplicatedCommand { payload, .. } = &entry.prepare.operation else {
                return Err(corrupt("non-control operation in control history"));
            };
            let command =
                ControlCommand::decode(payload).map_err(|error| corrupt(error.message))?;
            let authority = cell
                .membership_lineage_at(entry.prepare.view)?
                .ok_or_else(|| corrupt("control entry lacks historical membership lineage"))?;
            // Reprepare may replace an ordinary view/primary envelope around the
            // same operation. Replay consumes its stable membership authority;
            // concrete proposal freshness must not change a committed outcome.
            state.apply(entry.prepare.index, &authority, command)?;
        }
        if state.catalog.is_none() || state.tail.0 != tail {
            return Err(corrupt(
                "control history lacks complete committed genesis/prefix",
            ));
        }
        if stamp != cell.recovery_state_token()? {
            return Err(corrupt("control authority changed during replay"));
        }
        Ok(Self {
            state,
            stamp,
            replay_bytes: bytes,
        })
    }
    pub fn catalog(&self) -> &ControlCatalog {
        self.state.catalog.as_ref().expect("recovered genesis")
    }
    pub fn root(&self) -> CatalogRoot {
        self.state.root.expect("recovered genesis")
    }
    pub fn revision(&self) -> ControlRevision {
        self.state.revision
    }
    pub fn committed_tail(&self) -> LogIndex {
        self.state.tail
    }
    pub fn last_outcome(&self) -> &ControlOutcome {
        self.state.last.as_ref().expect("recovered genesis")
    }
    pub fn pending(&self) -> Option<&MembershipIntent> {
        self.state.pending.as_ref()
    }
    pub fn request_outcome(&self, request: ControlRequestId) -> Option<&ControlOutcome> {
        self.state
            .receipts
            .get(&request)
            .map(|receipt| &receipt.outcome)
    }
    pub fn publication(&self, intent: IntentId) -> Option<&[GroupPublication]> {
        self.state
            .completed
            .get(&intent)
            .map(|complete| complete.proofs.as_slice())
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
        if self.stamp != control.recovery_state_token()?
            || control.next_index().0 != self.state.tail.0 + 1
        {
            return Err(invalid(
                "control preflight requires current replay and reconciled accepted tail",
            ));
        }
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
            .and_then(|total| total.checked_add(reservation))
            .ok_or_else(|| invalid("control history budget overflow"))?;
        let records = control.next_index().0 + if reservation != 0 { 1 } else { 0 };
        if total > MAX_REPLAY_BYTES || records > MAX_CONTROL_RECORDS as u64 {
            return Err(invalid(
                "control history has no qualified capacity for command and finalization",
            ));
        }
        Ok(bytes)
    }
    pub fn admission(&self, control: &DurableVsrCell) -> RsfResult<CommittedAdmission> {
        if self.stamp != control.recovery_state_token()? {
            return Err(invalid(
                "control projection is stale; replay the current committed cell",
            ));
        }
        let group = self
            .catalog()
            .group(self.catalog().control_pg)
            .expect("validated catalog");
        if !group.matches(&control.membership_lineage()?) {
            return Err(invalid("current control membership is not admitted"));
        }
        control.primary_probe()?;
        Ok(CommittedAdmission {
            catalog: self.catalog().clone(),
            root: self.root(),
            revision: self.revision(),
            stamp: self.stamp.clone(),
        })
    }
}

/// A replay-derived binding, not a lease or application readiness assertion.
/// Call validate_authority at each coordination boundary, then reconcile/apply
/// and obtain fresh data quorum. This type has no public constructor or decoder.
#[derive(Debug)]
pub struct CommittedAdmission {
    catalog: ControlCatalog,
    root: CatalogRoot,
    revision: ControlRevision,
    stamp: VsrStateToken,
}
impl CommittedAdmission {
    pub fn root(&self) -> CatalogRoot {
        self.root
    }
    pub fn revision(&self) -> ControlRevision {
        self.revision
    }
    pub fn routing(&self) -> CatalogRouting {
        self.catalog.routing
    }
    pub fn catalog(&self) -> &ControlCatalog {
        &self.catalog
    }
    /// The data map excludes the control PG and must be complete. Returns the
    /// common current data primary, which may differ from the control primary.
    pub fn validate_authority(
        &self,
        control: &DurableVsrCell,
        data: &BTreeMap<PlacementGroupId, DurableVsrCell>,
        local: NodeId,
        certificate: &[u8; 32],
        routing: CatalogRouting,
    ) -> RsfResult<NodeId> {
        if self.stamp != control.recovery_state_token()? || routing != self.catalog.routing {
            return Err(invalid("admission projection or routing binding changed"));
        }
        let control_group = self
            .catalog
            .group(self.catalog.control_pg)
            .expect("validated catalog");
        if !control_group.matches(&control.membership_lineage()?) {
            return Err(invalid("control lineage differs from admitted catalog"));
        }
        control.primary_probe()?;
        let node = self
            .catalog
            .nodes
            .iter()
            .find(|node| node.id == local)
            .ok_or_else(|| invalid("local node absent from committed catalog"))?;
        if node.role != AdmissionRole::Voter || &node.certificate != certificate {
            return Err(invalid("local identity/role cannot serve this catalog"));
        }
        if data.len() + 1 != self.catalog.groups.len()
            || data.contains_key(&self.catalog.control_pg)
        {
            return Err(invalid("data inventory differs from committed catalog"));
        }
        let mut primary = None;
        for group in self
            .catalog
            .groups
            .iter()
            .filter(|group| group.pg != self.catalog.control_pg)
        {
            let cell = data
                .get(&group.pg)
                .ok_or_else(|| invalid("admitted data group is missing"))?;
            if !group.matches(&cell.membership_lineage()?) {
                return Err(invalid(
                    "data membership lineage differs from committed admission",
                ));
            }
            let probe = cell.primary_probe()?;
            if !probe.voters.contains(&local) || primary.is_some_and(|old| old != probe.primary) {
                return Err(invalid("data groups require a common admitted primary"));
            }
            primary = Some(probe.primary);
        }
        primary.ok_or_else(|| invalid("catalog has no data groups"))
    }
}

#[derive(Clone, Debug)]
struct Receipt {
    command: [u8; 32],
    outcome: ControlOutcome,
}
#[derive(Clone, Debug)]
struct Completion {
    parent: CatalogRoot,
    proofs: Vec<GroupPublication>,
    outcome: ControlOutcome,
}
#[derive(Clone, Debug)]
pub(crate) struct ReplayState {
    anchor: ControlBootstrap,
    catalog: Option<ControlCatalog>,
    root: Option<CatalogRoot>,
    revision: ControlRevision,
    tail: LogIndex,
    pending: Option<MembershipIntent>,
    receipts: BTreeMap<ControlRequestId, Receipt>,
    completed: BTreeMap<IntentId, Completion>,
    last: Option<ControlOutcome>,
}
impl ReplayState {
    pub(crate) fn new(anchor: ControlBootstrap) -> Self {
        Self {
            anchor,
            catalog: None,
            root: None,
            revision: ControlRevision(0),
            tail: LogIndex(0),
            pending: None,
            receipts: BTreeMap::new(),
            completed: BTreeMap::new(),
            last: None,
        }
    }
    pub(crate) fn apply(
        &mut self,
        index: LogIndex,
        authority: &VsrMembershipLineage,
        command: ControlCommand,
    ) -> RsfResult<ControlOutcome> {
        // Internal replay accepts only a strictly ordered, shape-validated input.
        command.encode().map_err(|error| corrupt(error.message))?;
        if index <= self.tail
            || authority.pg != self.anchor.control_pg
            || authority.bootstrap != self.anchor.control_bootstrap
        {
            return Err(corrupt(
                "control replay order or bootstrap authority mismatch",
            ));
        }
        let outcome = match command {
            ControlCommand::Genesis(catalog) => self.genesis(catalog, authority)?,
            ControlCommand::Begin(intent) => {
                if self.catalog.is_none() {
                    return Err(corrupt("membership intent precedes genesis"));
                }
                self.begin(intent, authority)?
            }
            ControlCommand::Finalize(finalization) => {
                if self.catalog.is_none() {
                    return Err(corrupt("finalization precedes genesis"));
                }
                self.finalize(finalization, authority)?
            }
        };
        self.tail = index;
        self.last = Some(outcome.clone());
        Ok(outcome)
    }
    fn genesis(
        &mut self,
        catalog: ControlCatalog,
        authority: &VsrMembershipLineage,
    ) -> RsfResult<ControlOutcome> {
        if self.catalog.is_some() {
            return Ok(rejected(Rejection::AlreadyInitialized));
        }
        let root = catalog.root()?;
        if root != self.anchor.genesis_catalog
            || catalog.cluster != self.anchor.cluster
            || catalog.control_pg != self.anchor.control_pg
            || catalog
                .groups
                .iter()
                .any(|group| group.lineage.plan.is_some())
            || !catalog
                .group(catalog.control_pg)
                .expect("validated catalog")
                .matches(authority)
        {
            return Err(corrupt(
                "committed genesis does not match provisioned authority",
            ));
        }
        self.catalog = Some(catalog);
        self.root = Some(root);
        self.revision = ControlRevision(1);
        Ok(ControlOutcome::CatalogApplied {
            revision: self.revision,
            root,
        })
    }
    fn begin(
        &mut self,
        intent: MembershipIntent,
        authority: &VsrMembershipLineage,
    ) -> RsfResult<ControlOutcome> {
        let id = intent.id()?;
        if let Some(receipt) = self.receipts.get(&intent.request) {
            return Ok(if receipt.command == id.0 {
                receipt.outcome.clone()
            } else {
                rejected(Rejection::RequestConflict)
            });
        }
        if self.receipts.len() == MAX_RECEIPTS {
            return Ok(rejected(Rejection::Capacity));
        }
        let outcome = match self.check_intent(&intent, authority) {
            Err(reason) => rejected(reason),
            Ok(()) => {
                self.pending = Some(intent.clone());
                ControlOutcome::IntentAccepted { intent: id }
            }
        };
        self.receipts.insert(
            intent.request,
            Receipt {
                command: id.0,
                outcome: outcome.clone(),
            },
        );
        Ok(outcome)
    }
    fn check_intent(
        &self,
        intent: &MembershipIntent,
        authority: &VsrMembershipLineage,
    ) -> Result<(), Rejection> {
        let catalog = self.catalog.as_ref().expect("genesis checked");
        if Some(intent.parent) != self.root || intent.revision != self.revision {
            return Err(Rejection::StaleParent);
        }
        if self.pending.is_some() {
            return Err(Rejection::Busy);
        }
        if self.completed.len() == MAX_TRANSITIONS {
            return Err(Rejection::Capacity);
        }
        if !catalog
            .group(catalog.control_pg)
            .expect("validated catalog")
            .matches(authority)
        {
            return Err(Rejection::WrongAuthority);
        }
        if intent.routing != catalog.routing {
            return Err(Rejection::UnsupportedRoutingChange);
        }
        if intent.groups.len() != catalog.groups.len() {
            return Err(Rejection::IncompleteGroups);
        }
        let voters = model::voter_nodes(&intent.nodes);
        for (old, next) in catalog.groups.iter().zip(&intent.groups) {
            if old.pg != next.pg {
                return Err(Rejection::IncompleteGroups);
            }
            if old.lineage != next.previous
                || next.proposal.election.pg != old.pg
                || next.proposal.election.voters != old.voters
                || next.proposal.new_voters != voters
            {
                return Err(Rejection::InvalidPlan);
            }
        }
        // An uncertain commit can be re-prepared with a later ordinary view and
        // primary. Its unchanged command must retain the same semantic result.
        // Only membership lineage authorizes replay here; live proposal freshness
        // belongs to the writer and every receiver immediately before fencing.
        for old in &catalog.nodes {
            if !intent
                .nodes
                .iter()
                .any(|next| next.id == old.id && next.preserves(old))
            {
                return Err(Rejection::IdentityChanged);
            }
        }
        let largest = catalog.nodes.last().expect("nonempty catalog").id;
        if intent
            .nodes
            .iter()
            .any(|next| next.id <= largest && !catalog.nodes.iter().any(|old| old.id == next.id))
        {
            return Err(Rejection::IdentityChanged);
        }
        intent.preflight().map_err(|_| Rejection::Capacity)?;
        Ok(())
    }
    fn finalize(
        &mut self,
        finalization: MembershipFinalization,
        authority: &VsrMembershipLineage,
    ) -> RsfResult<ControlOutcome> {
        if let Some(completion) = self.completed.get(&finalization.intent) {
            return Ok(
                if completion.parent == finalization.parent
                    && same_publications(&completion.proofs, &finalization.proofs)
                {
                    completion.outcome.clone()
                } else {
                    rejected(Rejection::PublicationConflict)
                },
            );
        }
        let Some(intent) = self.pending.as_ref() else {
            return Ok(rejected(Rejection::MissingIntent));
        };
        if intent.id()? != finalization.intent {
            return Ok(rejected(Rejection::MissingIntent));
        }
        if finalization.parent != intent.parent || Some(finalization.parent) != self.root {
            return Ok(rejected(Rejection::StaleParent));
        }
        if finalization.proofs.len() != intent.groups.len() {
            return Ok(rejected(Rejection::IncompleteGroups));
        }
        let mut next = self.catalog.as_ref().expect("genesis checked").clone();
        for ((group, bound), proof) in next
            .groups
            .iter_mut()
            .zip(&intent.groups)
            .zip(&finalization.proofs)
        {
            if proof.pg != bound.pg {
                return Ok(rejected(Rejection::IncompleteGroups));
            }
            if proof.quorum.proposal() != &bound.proposal
                || Some(proof.quorum.plan_id()) == bound.previous.plan
                || self.completed.values().any(|old| {
                    old.proofs.iter().any(|old| {
                        old.pg == proof.pg && old.quorum.plan_id() == proof.quorum.plan_id()
                    })
                })
            {
                return Ok(rejected(Rejection::PublicationConflict));
            }
            group.voters = proof.quorum.proposal().new_voters.clone();
            group.lineage = MembershipLineage {
                bootstrap: bound.previous.bootstrap,
                plan: Some(proof.quorum.plan_id()),
            };
        }
        next.nodes = intent.nodes.clone();
        next.validate().map_err(|error| corrupt(error.message))?;
        if !next
            .group(next.control_pg)
            .expect("validated catalog")
            .matches(authority)
        {
            return Ok(rejected(Rejection::WrongAuthority));
        }
        let revision = ControlRevision(
            self.revision
                .0
                .checked_add(1)
                .ok_or_else(|| corrupt("catalog revision exhausted"))?,
        );
        let root = next.root()?;
        let outcome = ControlOutcome::CatalogApplied { revision, root };
        self.catalog = Some(next);
        self.root = Some(root);
        self.revision = revision;
        self.pending = None;
        self.completed.insert(
            finalization.intent,
            Completion {
                parent: finalization.parent,
                proofs: finalization.proofs,
                outcome: outcome.clone(),
            },
        );
        Ok(outcome)
    }
}
fn rejected(reason: Rejection) -> ControlOutcome {
    ControlOutcome::Rejected(reason)
}
fn same_publications(left: &[GroupPublication], right: &[GroupPublication]) -> bool {
    left.len() == right.len()
        && left.iter().zip(right).all(|(left, right)| {
            left.pg == right.pg
                && left.quorum.proposal() == right.quorum.proposal()
                && left.quorum.input_id() == right.quorum.input_id()
                && left.quorum.plan_id() == right.quorum.plan_id()
        })
}
#[cfg(test)]
pub(crate) mod access {
    use super::*;
    pub(crate) fn catalog(state: &ReplayState) -> &ControlCatalog {
        state.catalog.as_ref().expect("genesis")
    }
    pub(crate) fn root(state: &ReplayState) -> CatalogRoot {
        state.root.expect("genesis")
    }
    pub(crate) fn revision(state: &ReplayState) -> ControlRevision {
        state.revision
    }
}
