use super::*;
use worker::{Action, Reply};

#[derive(Clone, Copy, Debug)]
enum Phase {
    Tail,
    Transfer(LogIndex),
    Prepare,
    Prepared,
    Commit,
    Observe,
    Done,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Purpose {
    Reconcile,
    Genesis,
    Observe,
}
#[derive(Debug)]
pub(super) struct Cycle {
    stamp: VsrStateToken,
    prepare: VsrPrepare,
    entries: Vec<VsrCommittedEntry>,
    peers: BTreeMap<NodeId, Phase>,
    last: Option<NodeId>,
    acknowledgements: BTreeSet<NodeId>,
    delivered: BTreeSet<NodeId>,
    purpose: Purpose,
}
impl Cycle {
    fn new(
        cell: &DurableVsrCell,
        prepare: VsrPrepare,
        local: NodeId,
        purpose: Purpose,
    ) -> Result<Self, String> {
        Ok(Self {
            stamp: cell.recovery_state_token().map_err(|e| e.message)?,
            prepare,
            entries: cell.committed_prefix().map_err(|e| e.message)?,
            peers: cell
                .replicas()
                .into_iter()
                .filter(|node| *node != local)
                .map(|node| (node, Phase::Tail))
                .collect(),
            last: None,
            acknowledgements: [local].into_iter().collect(),
            delivered: [local].into_iter().collect(),
            purpose,
        })
    }
    fn committed(&self) -> bool {
        self.prepare.index <= self.tail()
    }
    fn tail(&self) -> LogIndex {
        self.entries
            .last()
            .map_or(LogIndex(0), |entry| entry.prepare.index)
    }
    fn entry(&self, index: LogIndex) -> Result<VsrCommittedEntry, String> {
        let offset = index
            .0
            .checked_sub(1)
            .and_then(|value| usize::try_from(value).ok());
        offset
            .and_then(|offset| self.entries.get(offset))
            .filter(|entry| entry.prepare.index == index)
            .cloned()
            .ok_or_else(|| "control writer source prefix has a gap".into())
    }
    fn validate(&self, cell: &DurableVsrCell, local: NodeId) -> Result<(), String> {
        require_primary(cell, local)?;
        if self.stamp != cell.recovery_state_token().map_err(|e| e.message)?
            || cell.prepared(self.prepare.index) != Some(&self.prepare)
        {
            return Err("control writer source changed; discard in-flight progress".into());
        }
        Ok(())
    }
    fn next(
        &mut self,
        context: Option<&identity::Context>,
    ) -> Result<Option<(NodeId, Action)>, String> {
        let peer = self
            .peers
            .iter()
            .filter(|(_, phase)| !matches!(phase, Phase::Done | Phase::Prepared))
            .find(|(node, _)| self.last.is_none_or(|last| **node > last))
            .or_else(|| {
                self.peers
                    .iter()
                    .find(|(_, phase)| !matches!(phase, Phase::Done | Phase::Prepared))
            })
            .map(|(node, _)| *node);
        let Some(peer) = peer else {
            return Ok(None);
        };
        self.last = Some(peer);
        let action = match self.peers[&peer] {
            Phase::Tail => Action::Tail(self.prepare.pg),
            Phase::Transfer(index) => Action::Transfer(self.entry(index)?),
            Phase::Prepare => Action::Prepare(self.prepare.clone()),
            Phase::Commit => Action::Commit(self.entry(self.prepare.index)?.commit),
            Phase::Observe => Action::Observe(
                context
                    .ok_or("control observation context disappeared")?
                    .clone(),
            ),
            Phase::Done | Phase::Prepared => unreachable!("only actionable peer selected"),
        };
        Ok(Some((peer, action)))
    }
    fn synchronized(&mut self, replica: NodeId) {
        let phase = if self.committed() {
            self.delivered.insert(replica);
            if self.purpose == Purpose::Observe {
                Phase::Observe
            } else {
                Phase::Done
            }
        } else {
            Phase::Prepare
        };
        self.peers.insert(replica, phase);
    }
    fn finish(
        &mut self,
        replica: NodeId,
        action: &Action,
        reply: Reply,
        cell: &mut DurableVsrCell,
        collector: &mut Option<observation::Collector>,
    ) -> Result<(), String> {
        match (action, reply) {
            (Action::Tail(_), Reply::Tail(tail)) => {
                if tail > self.tail() {
                    return Err("control follower is ahead of writer prefix".into());
                }
                // Authenticate the common committed tip even when heights match.
                if self.tail().0 == 0 {
                    self.synchronized(replica);
                } else {
                    self.peers
                        .insert(replica, Phase::Transfer(LogIndex(tail.0.max(1))));
                }
            }
            (Action::Transfer(entry), Reply::Stored) => {
                if entry.prepare.index < self.tail() {
                    self.peers.insert(
                        replica,
                        Phase::Transfer(LogIndex(entry.prepare.index.0 + 1)),
                    );
                } else {
                    self.synchronized(replica);
                }
            }
            (Action::Prepare(prepare), Reply::Stored) => {
                match cell.acknowledge(replica, prepare.view, prepare.index) {
                    VsrDecision::Acknowledged { .. } => {}
                    other => {
                        return Err(format!(
                            "control prepare acknowledgement rejected: {other:?}"
                        ));
                    }
                }
                self.acknowledgements.insert(replica);
                self.peers.insert(
                    replica,
                    if self.committed() {
                        Phase::Commit
                    } else {
                        Phase::Prepared
                    },
                );
            }
            (Action::Commit(_), Reply::Stored) => {
                self.synchronized(replica);
            }
            (Action::Observe(context), Reply::Observation(report)) => {
                let collector = collector.as_mut().ok_or("control collector disappeared")?;
                if collector.key() != &context.key {
                    return Err("control collector key changed".into());
                }
                collector.insert(*report)?;
                self.peers.insert(replica, Phase::Done);
            }
            _ => return Err("control writer exchange returned the wrong response type".into()),
        }
        Ok(())
    }
}
impl Writer {
    pub(super) fn poll(
        &mut self,
        local: NodeId,
        certificate: [u8; 32],
        transport: &NetworkVsrTransport,
        provisioning: (&ControlBootstrap, &ControlCatalog),
        projection: &mut Option<ControlState>,
        cells: &mut BTreeMap<PlacementGroupId, DurableVsrCell>,
    ) -> Result<ControlWriterProgress, String> {
        let (anchor, genesis) = provisioning;
        let cell = cells
            .get_mut(&anchor.control_pg)
            .ok_or("control writer owner disappeared")?;
        // Refresh/health was checked by the server before entering this method.
        // A changed source cannot consume a late reply, even if its DTO is valid.
        if let Some(flight) = &mut self.flight {
            let Some(reply) = flight.take_reply() else {
                return Ok(ControlWriterProgress::Waiting);
            };
            let flight = self.flight.take().expect("completed flight");
            let validity = self
                .cycle
                .as_ref()
                .ok_or("control writer cycle disappeared")?
                .validate(cell, local)
                .and_then(|()| {
                    if let Action::Observe(context) = flight.action.as_ref() {
                        if identity::Context::recover(
                            projection
                                .as_ref()
                                .ok_or("control observation lost Genesis")?,
                            cell,
                        )?
                        .key != context.key
                        {
                            Err("control pending intent changed during observation".into())
                        } else {
                            Ok(())
                        }
                    } else {
                        Ok(())
                    }
                });
            if let Err(error) = validity {
                self.cycle = None;
                self.collector = None;
                return Err(error);
            }
            let cycle = self.cycle.as_mut().expect("validated cycle");
            match reply {
                Ok(reply) => {
                    if let Err(error) = cycle.finish(
                        flight.replica,
                        &flight.action,
                        reply,
                        cell,
                        &mut self.collector,
                    ) {
                        cycle.peers.insert(flight.replica, Phase::Tail);
                        return Err(error);
                    }
                }
                Err(reason) => {
                    cycle.peers.insert(flight.replica, Phase::Tail);
                    return Ok(ControlWriterProgress::Retrying {
                        replica: flight.replica,
                        reason,
                    });
                }
            }
        }
        require_primary(cell, local)?;
        if let Some(cycle) = &self.cycle
            && let Err(error) = cycle.validate(cell, local)
        {
            self.cycle = None;
            self.collector = None;
            return Err(error);
        }
        // An automatic pending-intent observation may have started before an
        // explicit goal was queued. Drain and validate its existing flight, then
        // discard only that read-only collection if the new goal differs. An
        // accepted-tail cycle must always finish its durable work first.
        if self
            .cycle
            .as_ref()
            .is_some_and(|cycle| cycle.purpose == Purpose::Observe)
            && self.requested.as_ref().is_some_and(|request| {
                request.membership() != projection.as_ref().and_then(ControlState::pending)
            })
        {
            self.cycle = None;
            self.collector = None;
        }
        if let Some(cycle) = &mut self.cycle {
            if !cycle.committed() && cycle.acknowledgements.len() >= cell.quorum() {
                let commit = match cell
                    .commit(local, cycle.prepare.index)
                    .map_err(|e| e.message)?
                {
                    VsrDecision::Committed(commit) => commit,
                    other => return Err(format!("control commit rejected: {other:?}")),
                };
                cycle.entries.push(VsrCommittedEntry {
                    prepare: cycle.prepare.clone(),
                    commit,
                });
                cycle.stamp = cell.recovery_state_token().map_err(|e| e.message)?;
                for phase in cycle.peers.values_mut() {
                    if matches!(phase, Phase::Prepared) {
                        *phase = Phase::Commit;
                    }
                }
                *projection = Some(ControlState::recover(anchor, cell).map_err(|e| e.message)?);
                return Ok(if cycle.prepare.index == LogIndex(1) {
                    ControlWriterProgress::GenesisLocallyCommitted
                } else {
                    ControlWriterProgress::Accepted {
                        index: cycle.prepare.index,
                    }
                });
            }
            if cycle.committed()
                && cycle.purpose != Purpose::Observe
                && cycle.delivered.len() >= cell.quorum()
            {
                if cycle.purpose == Purpose::Genesis {
                    let observed = genesis::observe(
                        anchor,
                        genesis,
                        cell,
                        &cycle.entry(LogIndex(1))?,
                        &cycle.stamp,
                        &cycle.delivered,
                    )?;
                    self.cycle = None;
                    self.requested = None;
                    return Ok(ControlWriterProgress::ObservedGenesisDelivery(observed));
                }
                self.cycle = None;
            }
        }
        if self.cycle.is_none() && projection.is_none() {
            ControlState::validate_initial_replica(anchor, genesis, cell).map_err(|e| e.message)?;
            if cell.prepared(LogIndex(1)).is_none()
                && !matches!(self.requested, Some(Request::Genesis))
            {
                return Ok(ControlWriterProgress::GenesisRequired);
            }
            let prepare = genesis::prepare(anchor, genesis, cell, local)?;
            let purpose = if matches!(self.requested, Some(Request::Genesis)) {
                Purpose::Genesis
            } else {
                Purpose::Reconcile
            };
            self.cycle = Some(Cycle::new(cell, prepare, local, purpose)?);
            return Ok(ControlWriterProgress::Accepted { index: LogIndex(1) });
        }
        if self.cycle.is_none() {
            let state = projection
                .as_ref()
                .ok_or("control projection disappeared")?;
            if let Some(index) = state.preflight_accepted_tail(cell).map_err(|e| e.message)? {
                let prepare = cell.reprepare(local, index).map_err(|e| e.message)?;
                self.cycle = Some(Cycle::new(cell, prepare, local, Purpose::Reconcile)?);
                *projection = Some(ControlState::recover(anchor, cell).map_err(|e| e.message)?);
                return Ok(ControlWriterProgress::Accepted { index });
            }
            if matches!(self.requested, Some(Request::Genesis)) {
                genesis::original_membership(anchor, genesis, cell)?;
                let prepare = cell
                    .prepared(LogIndex(1))
                    .ok_or("committed Genesis prepare missing")?
                    .clone();
                self.cycle = Some(Cycle::new(cell, prepare, local, Purpose::Genesis)?);
            } else {
                let requested = self
                    .requested
                    .as_ref()
                    .and_then(Request::membership)
                    .or_else(|| state.pending());
                let Some(requested) = requested else {
                    return Ok(ControlWriterProgress::Idle);
                };
                if let Some(outcome) = state.intent_outcome(requested).map_err(|e| e.message)? {
                    let id = requested.id().map_err(|e| e.message)?;
                    if outcome != (ControlOutcome::IntentAccepted { intent: id })
                        || state.pending().is_none_or(|pending| pending != requested)
                    {
                        self.collector = None;
                        return Ok(ControlWriterProgress::LocallyCommitted {
                            intent: id,
                            begin: state.request_begin_index(requested.request),
                            outcome,
                        });
                    }
                    let context = identity::Context::recover(state, cell)?;
                    if self
                        .collector
                        .as_ref()
                        .is_none_or(|collector| collector.key() != &context.key)
                    {
                        self.collector = Some(observation::Collector::new(context.clone()));
                    }
                    self.collector
                        .as_mut()
                        .expect("collector")
                        .insert(observation::local(state, cell, local, certificate)?)?;
                    let prepare = cell
                        .prepared(context.key.begin)
                        .ok_or("committed intent prepare missing")?
                        .clone();
                    self.cycle = Some(Cycle::new(cell, prepare, local, Purpose::Observe)?);
                } else {
                    let command = ControlCommand::Begin(requested.clone());
                    let bytes = match state.preflight_command(cell, &command) {
                        Ok(bytes) => bytes,
                        Err(error) => {
                            self.requested = None;
                            return Err(error.message);
                        }
                    };
                    // Validate every target only at a new append, after receipt and
                    // complete accepted-tail resolution. The control PG aliases its
                    // existing owner; no second opener or borrowed writable map.
                    if let Err(error) = validate_targets(requested, cells) {
                        // No append or accepted operation belongs to this queued
                        // request yet. Discard only the failed volatile plan, never
                        // a retained log operation or committed pending intent.
                        self.requested = None;
                        return Err(error);
                    }
                    let cell = cells.get_mut(&anchor.control_pg).expect("control retained");
                    let operation_id = next_operation_id(cell)?;
                    let prepare = match cell
                        .prepare(
                            local,
                            VsrOperation::ReplicatedCommand {
                                operation_id,
                                payload: command.encode().map_err(|e| e.message)?,
                            },
                            bytes as u32,
                        )
                        .map_err(|e| e.message)?
                    {
                        VsrDecision::Prepared(prepare) => prepare,
                        other => return Err(format!("control Begin prepare rejected: {other:?}")),
                    };
                    let index = prepare.index;
                    self.cycle = Some(Cycle::new(cell, prepare, local, Purpose::Reconcile)?);
                    *projection = Some(ControlState::recover(anchor, cell).map_err(|e| e.message)?);
                    return Ok(ControlWriterProgress::Accepted { index });
                }
            }
        }
        let cell = cells.get(&anchor.control_pg).expect("control retained");
        let cycle = self.cycle.as_mut().expect("active writer cycle");
        let context = if cycle.purpose == Purpose::Observe {
            let state = projection
                .as_ref()
                .ok_or("control observation lost Genesis")?;
            let context = identity::Context::recover(state, cell)?;
            if self
                .collector
                .as_ref()
                .is_none_or(|collector| collector.key() != &context.key)
            {
                self.cycle = None;
                self.collector = None;
                return Err("control pending intent superseded or finalized".into());
            }
            if let Some(observed) = self
                .collector
                .as_ref()
                .and_then(observation::Collector::observed)
            {
                return Ok(ControlWriterProgress::ObservedOldControlDelivery(observed));
            }
            Some(context)
        } else {
            None
        };
        if let Some((replica, action)) = cycle.next(context.as_ref())? {
            self.flight = Some(worker::Flight::start(transport.clone(), replica, action)?);
        }
        if cycle.committed() && matches!(self.requested, Some(Request::Genesis)) {
            return Ok(ControlWriterProgress::GenesisLocallyCommitted);
        }
        if let Some(state) = projection.as_ref()
            && let Some(requested) = self
                .requested
                .as_ref()
                .and_then(Request::membership)
                .or_else(|| state.pending())
            && let Some(outcome) = state.intent_outcome(requested).map_err(|e| e.message)?
        {
            return Ok(ControlWriterProgress::LocallyCommitted {
                intent: requested.id().map_err(|e| e.message)?,
                begin: state.request_begin_index(requested.request),
                outcome,
            });
        }
        Ok(ControlWriterProgress::Waiting)
    }
}
fn validate_targets(
    intent: &MembershipIntent,
    cells: &BTreeMap<PlacementGroupId, DurableVsrCell>,
) -> Result<(), String> {
    for group in &intent.groups {
        let cell = cells
            .get(&group.pg)
            .ok_or("new Begin requires every target's owned voter custody")?;
        let active = cell.primary_probe().map_err(|e| e.message)?;
        let lineage = cell.membership_lineage().map_err(|e| e.message)?;
        let proposal = &group.proposal;
        if lineage.bootstrap != group.previous.bootstrap
            || lineage.plan != group.previous.plan
            || active.pg != group.pg
            || proposal.election.pg != group.pg
            || active.view != proposal.election.previous_view
            || active.primary != proposal.old_primary
            || lineage.voters != proposal.election.voters
        {
            return Err(
                "new Begin target proposal differs from current owned lineage/view/primary/voters"
                    .into(),
            );
        }
    }
    Ok(())
}

pub(super) fn next_operation_id(cell: &DurableVsrCell) -> Result<rsf_core::OperationId, String> {
    let mut maximum = 0;
    for index in 1..cell.next_index().0 {
        match &cell
            .prepared(LogIndex(index))
            .ok_or("control history operation gap")?
            .operation
        {
            VsrOperation::ReplicatedCommand { operation_id, .. } => {
                maximum = maximum.max(operation_id.0)
            }
            _ => return Err("non-control operation in retained history".into()),
        }
    }
    maximum
        .checked_add(1)
        .map(rsf_core::OperationId)
        .ok_or_else(|| "control operation identifier exhausted".into())
}
