//! Bounded control-log replication and volatile authenticated old delivery.
//! This module cannot create D, Ready, a first-fence permit or client admission.
use super::*;
use rsf_control_state::{ControlOutcome, IntentId, MembershipIntent};
use rsf_vsr::{VsrDecision, VsrOperation, VsrStateToken};

pub(in crate::replication) mod genesis;
pub(in crate::replication) mod identity;
pub(in crate::replication) mod observation;
mod progress;
mod worker;

/// Historical process-local observation, not a durable delivery proof or gate.
/// There is deliberately no decoder, public constructor or conversion to an
/// admission/fence capability. Recollect after restart; validate live work before
/// any future durable confirmation protocol consumes such observations.
#[derive(Debug)]
pub struct ObservedOldControlDelivery {
    key: Box<identity::Key>,
    reporters: Vec<NodeId>,
}
impl ObservedOldControlDelivery {
    pub fn intent(&self) -> IntentId {
        self.key.intent
    }
    pub fn begin_index(&self) -> LogIndex {
        self.key.begin
    }
    pub fn reporters(&self) -> &[NodeId] {
        &self.reporters
    }
}
#[derive(Debug)]
pub enum ControlWriterProgress {
    Idle,
    /// No committed or accepted Genesis and no explicit bootstrap request.
    GenesisRequired,
    /// Local durable Genesis only; this does not establish remote delivery.
    GenesisLocallyCommitted,
    ObservedGenesisDelivery(ObservedControlGenesisDelivery),
    Waiting,
    Accepted {
        index: LogIndex,
    },
    /// Only the local owned control prefix is committed. Rejected commands are
    /// committed receipts too, and never count as old-delivery observations.
    LocallyCommitted {
        intent: IntentId,
        begin: Option<LogIndex>,
        outcome: ControlOutcome,
    },
    ObservedOldControlDelivery(ObservedOldControlDelivery),
    Retrying {
        replica: NodeId,
        reason: String,
    },
}
pub use genesis::ObservedControlGenesisDelivery;

#[derive(Debug)]
enum Request {
    Genesis,
    Membership(Box<MembershipIntent>),
}
impl Request {
    fn membership(&self) -> Option<&MembershipIntent> {
        match self {
            Self::Genesis => None,
            Self::Membership(intent) => Some(intent),
        }
    }
}
#[derive(Debug, Default)]
pub(super) struct Writer {
    requested: Option<Request>,
    cycle: Option<progress::Cycle>,
    flight: Option<worker::Flight>,
    collector: Option<observation::Collector>,
}
impl PeerReplicaServer {
    /// Queue one canonical Begin. This requires current control-primary custody
    /// but does not append, commit, promise delivery or authorize any PG fence.
    /// A cold restart resumes accepted work and can recollect the pending Begin;
    /// unaccepted in-memory requests must be submitted again by their caller.
    pub fn request_membership_intent(
        &mut self,
        intent: MembershipIntent,
    ) -> Result<IntentId, String> {
        let id = intent.id().map_err(|e| e.message)?;
        let binding = self
            .managed_control
            .as_mut()
            .ok_or("control replication is not configured")?;
        let cell = self
            .cells
            .get(&binding.pg())
            .ok_or("control writer requires a hosted voter")?;
        binding.refresh_voter(cell)?;
        require_primary(cell, self.local)?;
        let state = binding
            .voter
            .as_ref()
            .ok_or("control writer requires committed Genesis")?;
        state.preflight_accepted_tail(cell).map_err(|e| e.message)?;
        state.intent_outcome(&intent).map_err(|e| e.message)?;
        if let Some(previous) = &binding.writer.requested {
            if previous.membership() == Some(&intent) {
                return Ok(id);
            }
            if binding.writer.flight.is_some()
                || match previous.membership() {
                    Some(previous) => state
                        .intent_outcome(previous)
                        .map_err(|e| e.message)?
                        .is_none(),
                    None => true,
                }
            {
                return Err("control writer already owns an unresolved request".into());
            }
        }
        // Poll rechecks the receipt under fresh custody before new-append
        // planning; queueing and canonical conflict checks do not append.
        let continuing = binding.writer.requested.is_none() && binding.writer.cycle.is_some();
        binding.writer.requested = Some(Request::Membership(Box::new(intent)));
        if !continuing {
            binding.writer.cycle = None;
            binding.writer.collector = None;
        }
        Ok(id)
    }
    /// Queue configured Genesis on the actual current control primary. No
    /// caller payload or commit vector can authorize bootstrap. This does not
    /// append or establish delivery; native factory restrictions are separate.
    pub fn request_control_genesis(&mut self) -> Result<(), String> {
        let binding = self
            .managed_control
            .as_mut()
            .ok_or("control replication is not configured")?;
        let cell = self
            .cells
            .get(&binding.pg())
            .ok_or("control writer requires a hosted voter")?;
        binding.refresh_voter(cell)?;
        require_primary(cell, self.local)?;
        genesis::original_membership(&binding.anchor, &binding.genesis, cell)?;
        if let Some(state) = &binding.voter {
            state.preflight_accepted_tail(cell).map_err(|e| e.message)?;
        }
        if let Some(previous) = &binding.writer.requested {
            match previous {
                Request::Genesis => return Ok(()),
                Request::Membership(intent) => {
                    if binding.writer.flight.is_some()
                        || binding
                            .voter
                            .as_ref()
                            .ok_or("committed Genesis disappeared")?
                            .intent_outcome(intent)
                            .map_err(|e| e.message)?
                            .is_none()
                    {
                        return Err("control writer already owns an unresolved request".into());
                    }
                }
            }
        }
        // A cold accepted-tail reconciliation can already own a flight without
        // an explicit request. Attach the goal without discarding that work.
        let continuing = binding.writer.requested.is_none() && binding.writer.cycle.is_some();
        binding.writer.requested = Some(Request::Genesis);
        if !continuing {
            binding.writer.cycle = None;
            binding.writer.collector = None;
        }
        Ok(())
    }
    /// Advance local durable work or at most one bounded TLS exchange. A worker
    /// holds only transport/DTOs; this server retains every cell. The independent
    /// membership scheduler may also have one flight (two total per server).
    pub fn poll_control_writer(&mut self) -> Result<ControlWriterProgress, String> {
        let binding = self
            .managed_control
            .as_mut()
            .ok_or("control replication is not configured")?;
        let pg = binding.pg();
        let cell = self
            .cells
            .get(&pg)
            .ok_or("control writer requires a hosted voter")?;
        binding.refresh_voter(cell)?;
        let Binding {
            anchor,
            genesis,
            voter,
            writer,
            ..
        } = binding;
        writer.poll(
            self.local,
            self.identity.fingerprint,
            &self.control_transport,
            (anchor, genesis),
            voter,
            &mut self.cells,
        )
    }
}
fn require_primary(cell: &DurableVsrCell, local: NodeId) -> Result<(), String> {
    if cell.primary_probe().map_err(|e| e.message)?.primary != local {
        return Err("control writer requires the actual current control primary".into());
    }
    Ok(())
}
