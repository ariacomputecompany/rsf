//! Volatile driver for the existing durable transition acceptor. A frozen
//! phase-one quorum selects one input; transport metadata never replaces it.
use super::*;
mod detector;
pub(super) mod exchange;
mod finish;
mod step;
use detector::Detector;
pub(super) use detector::grace;
use exchange::{Binding, Context, Kind, Operation, Request, Response, Scope};

#[derive(Debug, Default)]
pub(super) struct Choice {
    observations: BTreeMap<NodeId, VsrMembershipDecisionState>,
    inspected: std::collections::BTreeSet<NodeId>,
    ballot: Option<Ballot>,
    chosen: Option<Arc<VsrMembershipChosen>>,
    download: Option<Download>,
    detector: Detector,
}
#[derive(Debug)]
struct Ballot {
    round: VsrMembershipRound,
    promises: BTreeMap<NodeId, VsrMembershipPromise>,
    frozen: Option<Vec<VsrMembershipPromise>>,
    selected: Option<[u8; 32]>,
    decision: Option<Arc<VsrMembershipDecision>>,
    nomination: Option<VsrMembershipAccepted>,
    accepted: BTreeMap<NodeId, VsrMembershipAccepted>,
    uploads: BTreeMap<NodeId, Option<u64>>,
}
#[derive(Debug)]
struct Download {
    id: [u8; 32],
    input: Option<Arc<VsrMembershipInput>>,
    begun: bool,
    records: u64,
    source: Option<NodeId>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Repair {
    Inspect,
    Begin,
    Records(u64),
    Finish,
}

fn context(store: &MembershipDecisionStore) -> Result<Context, String> {
    let state = store.decision_state().map_err(|e| e.message)?;
    Ok(Context {
        proposal: Arc::new(store.proposal().clone()),
        bootstrap: state.bootstrap,
    })
}
fn request(
    store: &MembershipDecisionStore,
    scope: Scope,
    operation: Operation,
) -> Result<Action, String> {
    Ok(Action::Choice(Request {
        context: context(store)?,
        scope,
        operation,
    }))
}
fn floor(state: &VsrMembershipDecisionState) -> Option<VsrMembershipRound> {
    state
        .promised
        .into_iter()
        .chain(state.accepted.map(|v| v.0))
        .chain(state.chosen.map(|v| v.0))
        .max()
}
fn highest(promises: &[VsrMembershipPromise], authority: [u8; 32]) -> Result<[u8; 32], String> {
    let mut values = BTreeMap::new();
    for p in promises {
        if let Some((round, id)) = p.accepted
            && values.insert(round, id).is_some_and(|old| old != id)
        {
            return Err("one durable membership round accepted conflicting values".into());
        }
    }
    Ok(values.last_key_value().map_or(authority, |(_, id)| *id))
}
fn validate_state(
    state: &VsrMembershipDecisionState,
    replica: NodeId,
    context: &Context,
) -> Result<(), String> {
    state
        .validate_for(&context.proposal)
        .map_err(|e| e.message)?;
    if state.replica != replica || state.bootstrap != context.bootstrap {
        return Err("membership recovery state changed authenticated reporter or bootstrap".into());
    }
    Ok(())
}
impl Choice {
    fn scope(&self) -> Scope {
        if let Some(chosen) = &self.chosen {
            Scope::Chosen {
                input: chosen.input_id(),
            }
        } else if let Some(ballot) = &self.ballot {
            Scope::Round {
                round: ballot.round,
                input: ballot.selected,
            }
        } else {
            Scope::Discovery
        }
    }
    fn observe(&mut self, state: VsrMembershipDecisionState, now: Instant) {
        // Only the reporting coordinator's own changing durable facts are
        // activity. A follower relaying its promise never renews that lease.
        self.detector.observe(&state, now);
        self.observations.insert(state.replica, state);
    }
    fn accepted_choice(
        &self,
        proposal: &VsrMembershipProposal,
    ) -> Result<Option<VsrMembershipChosen>, String> {
        let mut votes: BTreeMap<(VsrMembershipRound, [u8; 32]), Vec<VsrMembershipAccepted>> =
            BTreeMap::new();
        for state in self.observations.values() {
            if let Some((round, input)) = state.accepted {
                votes
                    .entry((round, input))
                    .or_default()
                    .push(VsrMembershipAccepted {
                        proposal: proposal.clone(),
                        replica: state.replica,
                        round,
                        input,
                    });
            }
        }
        for (_, reports) in votes.into_iter().rev() {
            if reports.len() > proposal.new_voters.len() / 2 {
                return VsrMembershipChosen::from_accepted(reports)
                    .map(Some)
                    .map_err(|e| e.message);
            }
        }
        Ok(None)
    }
    fn select_chosen(&mut self, chosen: VsrMembershipChosen) -> Result<(), String> {
        if self
            .chosen
            .as_ref()
            .is_some_and(|old| old.input_id() != chosen.input_id())
        {
            return Err("authenticated membership certificates choose conflicting inputs".into());
        }
        let id = chosen.input_id();
        self.chosen = Some(Arc::new(chosen));
        self.ballot = None;
        if self.download.as_ref().is_none_or(|d| d.id != id) {
            self.download = Some(Download {
                id,
                input: None,
                begun: false,
                records: 0,
                source: None,
            });
        } else {
            // Ordinary round-copy permission is not a chosen import context,
            // even when all bytes are already retained. A lost ACCEPT reply
            // followed by a metadata majority must bind chosen BEGIN first.
            self.download.as_mut().unwrap().begun = false;
        }
        Ok(())
    }
    fn current_floor(&self) -> Option<VsrMembershipRound> {
        self.observations.values().filter_map(floor).max()
    }
    fn discard_preempted(&mut self, current: &VsrMembershipDecisionState) -> Result<(), String> {
        if let Some(ballot) = &self.ballot {
            let superseded = self
                .current_floor()
                .into_iter()
                .chain(floor(current))
                .any(|r| r > ballot.round);
            if superseded || current.promised != Some(ballot.round) {
                self.ballot = None;
                self.download = None;
                self.detector.contended()?;
            }
        }
        Ok(())
    }
}

impl Group {
    pub(super) fn validate_prechoice_anchor(
        &mut self,
        server: &PeerReplicaServer,
        pg: PlacementGroupId,
    ) -> Result<(), String> {
        if let Some(cell) = server.cells.get(&pg) {
            cell.recovery_state_token().map_err(|e| e.message)?;
            if cell.membership_fence() != server.membership_recovery_bindings.get(&pg) {
                return Err("unchosen managed voter lost its exact membership fence".into());
            }
        } else {
            let learner = server
                .learners
                .get(&pg)
                .ok_or_else(|| "managed membership lost hosted anchor".to_string())?;
            let token = learner.published_state_token().map_err(|e| e.message)?;
            if self.learner_token.as_ref() != Some(&token) {
                let authority = server
                    .membership_decisions
                    .get(&pg)
                    .ok_or("managed membership lost decision custody")?
                    .authority()
                    .map_err(|e| e.message)?;
                learner
                    .validate_membership_authority(&authority)
                    .map_err(|e| e.message)?;
                self.learner_token = Some(token);
            }
        }
        Ok(())
    }
    pub(super) fn ensure_peers(
        &mut self,
        proposal: &VsrMembershipProposal,
        local: NodeId,
        now: Instant,
    ) {
        for replica in &proposal.new_voters {
            if *replica != local {
                self.peers.entry(*replica).or_insert_with(|| Peer::new(now));
            }
        }
    }
    pub(super) fn binding(
        &self,
        server: &PeerReplicaServer,
        pg: PlacementGroupId,
        action: &Action,
    ) -> Result<Binding, String> {
        if let Action::Choice(request) = action {
            return request.binding();
        }
        let store = &server.membership_decisions[&pg];
        let state = store.decision_state().map_err(|e| e.message)?;
        Ok(Binding {
            proposal: state.proposal,
            bootstrap: state.bootstrap,
            scope: Scope::Plan {
                input: self
                    .input
                    .ok_or_else(|| "recovery action has no chosen input".to_string())?,
                plan: self
                    .plan
                    .ok_or_else(|| "recovery action has no canonical plan".to_string())?,
            },
        })
    }
    pub(super) fn matches_binding(
        &mut self,
        server: &PeerReplicaServer,
        pg: PlacementGroupId,
        binding: Binding,
    ) -> Result<bool, String> {
        let store = server
            .membership_decisions
            .get(&pg)
            .ok_or_else(|| "managed decision disappeared".to_string())?;
        if Some(store.proposal()) != server.membership_recovery_bindings.get(&pg) {
            return Ok(false);
        }
        let state = store.decision_state().map_err(|e| e.message)?;
        if state.proposal != binding.proposal || state.bootstrap != binding.bootstrap {
            return Ok(false);
        }
        match binding.scope {
            Scope::Plan { input, plan } => {
                if self.input != Some(input)
                    || self.plan != Some(plan)
                    || store.chosen().is_none_or(|c| c.input_id() != input)
                {
                    return Ok(false);
                }
                self.validate_local_identity(server, pg)?;
            }
            scope => {
                if store.chosen().is_some() || self.choice.scope() != scope {
                    return Ok(false);
                }
                if let Scope::Round { round, .. } = scope
                    && state.promised != Some(round)
                {
                    return Ok(false);
                }
                self.validate_prechoice_anchor(server, pg)?;
            }
        }
        Ok(true)
    }
}

impl PeerReplicaServer {
    /// Called only after a successful authenticated durable operation. Merely
    /// relaying another node's promise or repeating an unchanged cursor is not
    /// coordinator progress. No journal handle enters the scheduler worker.
    pub(in crate::replication) fn note_membership_recovery_progress(
        &mut self,
        pg: PlacementGroupId,
        sender: NodeId,
        round: VsrMembershipRound,
        copy: Option<([u8; 32], u64)>,
    ) {
        if let Some(recovery) = &mut self.membership_recovery
            && let Some(group) = recovery.groups.get_mut(&pg)
        {
            let now = Instant::now();
            group.choice.detector.coordinator(sender, round, now);
            if let Some((input, records)) = copy {
                group
                    .choice
                    .detector
                    .copied(sender, round, input, records, now);
            }
        }
    }
}

#[cfg(test)]
mod tests;
