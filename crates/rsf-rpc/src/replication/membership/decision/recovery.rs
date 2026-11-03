//! Bounded recovery of an already chosen transition. Workers own transport only;
//! durable authority and application materialization stay on the server thread.
use super::*;
use rsf_vsr::{
    VsrMembershipPublishedQuorum, VsrMembershipPublishedReport, VsrMembershipStagedQuorum,
    VsrMembershipStagedReport,
};
use std::sync::mpsc::TryRecvError;

mod progress;
mod worker;
use worker::{Action, Reply};

#[derive(Clone, Copy, Debug)]
pub struct PeerMembershipRecoveryTiming {
    pub interval: Duration,
}
impl PeerMembershipRecoveryTiming {
    fn validate(self) -> Result<(), String> {
        if self.interval < Duration::from_millis(1) || self.interval > Duration::from_secs(60) {
            return Err("membership recovery interval must be between 1 ms and 60 s".into());
        }
        Ok(())
    }
}

/// Progress and historical evidence only. No event admits clients or releases
/// the native managed-group policy. Ordinary elections do not depend on events.
#[derive(Debug)]
pub struct PeerMembershipRecoveryUpdate {
    pub pg: PlacementGroupId,
    pub plan: Option<[u8; 32]>,
    pub phase: &'static str,
    pub replica: Option<NodeId>,
    pub publication_quorum: Option<VsrMembershipPublishedQuorum>,
    pub detail: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Upload {
    Begin,
    Records(u64),
    Finish,
}

#[derive(Debug)]
struct Peer {
    next: Instant,
    upload: Option<Upload>,
    require_stage: bool,
    query_next: bool,
    failures: u32,
    provisioned: bool,
}

impl Peer {
    fn failed(&mut self, action: worker::Kind, interval: Duration, now: Instant) {
        self.failures = self.failures.saturating_add(1).min(6);
        self.next = now
            + interval
                .saturating_mul(1 << self.failures)
                .min(Duration::from_secs(60));
        match action {
            worker::Kind::Stage if !self.provisioned => self.upload = Some(Upload::Begin),
            worker::Kind::Stage => {}
            worker::Kind::Publish => self.require_stage = true,
            worker::Kind::Query => self.query_next = false,
            // Ask the durable inbox for its offset after a failed upload. No
            // error text is interpreted as missing or repairable sealed input.
            _ => self.upload = Some(Upload::Begin),
        }
    }
}

#[derive(Debug)]
struct Group {
    timing: PeerMembershipRecoveryTiming,
    next: Instant,
    input: Option<[u8; 32]>,
    plan: Option<[u8; 32]>,
    local_stage: Option<VsrMembershipStagedReport>,
    stages: BTreeMap<NodeId, VsrMembershipStagedReport>,
    quorum: Option<Arc<VsrMembershipStagedQuorum>>,
    local_publication: Option<VsrMembershipPublishedReport>,
    publications: BTreeMap<NodeId, VsrMembershipPublishedReport>,
    reported_majority: bool,
    peers: BTreeMap<NodeId, Peer>,
    last_peer: Option<NodeId>,
}

impl Group {
    fn new(timing: PeerMembershipRecoveryTiming, now: Instant) -> Self {
        Self {
            timing,
            next: now,
            input: None,
            plan: None,
            local_stage: None,
            stages: BTreeMap::new(),
            quorum: None,
            local_publication: None,
            publications: BTreeMap::new(),
            reported_majority: false,
            peers: BTreeMap::new(),
            last_peer: None,
        }
    }

    fn update(
        &self,
        pg: PlacementGroupId,
        phase: &'static str,
        detail: String,
    ) -> PeerMembershipRecoveryUpdate {
        PeerMembershipRecoveryUpdate {
            pg,
            plan: self.plan,
            phase,
            replica: None,
            publication_quorum: None,
            detail,
        }
    }

    fn due_peer(&mut self, now: Instant) -> Option<NodeId> {
        let keys = rotated(self.peers.keys().copied().collect(), self.last_peer);
        let peer = keys.into_iter().find(|peer| self.peers[peer].next <= now)?;
        self.last_peer = Some(peer);
        Some(peer)
    }
}

#[derive(Clone, Copy, Debug)]
struct PendingAction {
    pg: PlacementGroupId,
    input: [u8; 32],
    plan: [u8; 32],
    replica: NodeId,
    phase: &'static str,
    action: worker::Kind,
}

#[derive(Debug)]
struct Flight {
    request: PendingAction,
    worker: worker::Worker,
}

#[derive(Debug)]
pub(in crate::replication) struct Recovery {
    groups: BTreeMap<PlacementGroupId, Group>,
    last: Option<PlacementGroupId>,
    flight: Option<Flight>,
    transport: Arc<NetworkVsrTransport>,
}

fn rotated<T: Copy + Ord>(mut keys: Vec<T>, last: Option<T>) -> Vec<T> {
    if let Some(last) = last {
        let split = keys.partition_point(|key| *key <= last);
        keys.rotate_left(split);
    }
    keys
}

impl PeerReplicaServer {
    /// Bind native recovery to journals already opened through the strict
    /// existing-custody path. No fresh proposal is authorized by this policy.
    pub fn with_membership_recovery(
        mut self,
        timing: BTreeMap<PlacementGroupId, PeerMembershipRecoveryTiming>,
    ) -> Result<Self, String> {
        if self.membership_recovery.is_some() || !self.membership_recovery_bindings.is_empty() {
            return Err("membership recovery is already configured".into());
        }
        if timing.is_empty() {
            return Err("membership recovery requires at least one managed group".into());
        }
        let now = Instant::now();
        let mut groups = BTreeMap::new();
        let mut bindings = BTreeMap::new();
        for (pg, timing) in timing {
            timing.validate()?;
            let store = self.membership_decisions.get(&pg).ok_or_else(|| {
                "membership recovery requires an existing decision journal".to_string()
            })?;
            let proposal = store.proposal().clone();
            let authority = store.authority().map_err(|e| e.message)?;
            // Reuse the same published-B versus initial-A invariant as cold
            // configured recovery. This also checks healthy stage/anchor custody.
            self.initialize_membership_decision(&authority)?;
            if let Some(cell) = self.cells.get(&pg)
                && cell.membership_fence() != Some(&proposal)
            {
                let stage = self.membership_staging.get(&pg).ok_or_else(|| {
                    "unfenced managed voter requires its retained publication stage".to_string()
                })?;
                stage
                    .validate_published_anchor(cell)
                    .map_err(|e| e.message)?;
            }
            if self.learners.contains_key(&pg) && !self.membership_promotion_paths.contains_key(&pg)
            {
                return Err("managed learner requires a configured promotion target".into());
            }
            bindings.insert(pg, proposal);
            groups.insert(pg, Group::new(timing, now));
        }
        self.membership_recovery_bindings = bindings;
        self.membership_recovery = Some(Recovery {
            groups,
            last: None,
            flight: None,
            transport: Arc::new(self.control_transport.clone()),
        });
        Ok(self)
    }

    /// Durable native policy is owned by the caller; these immutable bindings
    /// enforce its runtime guard even if a polling callback unwinds.
    pub fn membership_recovery_enabled(&self) -> bool {
        !self.membership_recovery_bindings.is_empty()
    }

    pub fn poll_membership_recovery(
        &mut self,
        apply: &mut impl FnMut(&VsrCommittedEntry) -> Result<(), String>,
    ) -> Result<Option<PeerMembershipRecoveryUpdate>, String> {
        let Some(mut recovery) = self.membership_recovery.take() else {
            return if self.membership_recovery_enabled() {
                Err("membership recovery scheduling lost custody; reopen the peer".into())
            } else {
                Ok(None)
            };
        };
        let result = recovery.poll(self, apply);
        self.membership_recovery = Some(recovery);
        result
    }

    pub(in crate::replication) fn guard_membership_recovery(
        &self,
        kind: u8,
        payload: &[u8],
    ) -> io::Result<()> {
        if self.membership_recovery_bindings.is_empty() {
            return Ok(());
        }
        let proposal = match kind {
            MEMBERSHIP_BEGIN => {
                Some(VsrMembershipProposal::decode(payload).map_err(|e| invalid(e.message))?)
            }
            MEMBERSHIP_RESUME => Some(
                VsrMembershipReport::decode(payload)
                    .map_err(|e| invalid(e.message))?
                    .proposal,
            ),
            MEMBERSHIP_AUTH_BEGIN => Some(
                VsrMembershipInput::decode(payload)
                    .map_err(|e| invalid(e.message))?
                    .proposal()
                    .clone(),
            ),
            _ => None,
        };
        if let Some(proposal) = proposal
            && let Some(bound) = self.membership_recovery_bindings.get(&proposal.election.pg)
            && bound != &proposal
        {
            return Err(invalid(
                "managed membership recovery cannot initiate another proposal",
            ));
        }
        Ok(())
    }
}

impl Recovery {
    fn poll(
        &mut self,
        server: &mut PeerReplicaServer,
        apply: &mut impl FnMut(&VsrCommittedEntry) -> Result<(), String>,
    ) -> Result<Option<PeerMembershipRecoveryUpdate>, String> {
        if let Some(flight) = &self.flight {
            let result = match flight.worker.receiver.try_recv() {
                Ok(result) => result,
                Err(TryRecvError::Empty) => return Ok(None),
                Err(TryRecvError::Disconnected) => {
                    Err("membership transport worker stopped without a reply".into())
                }
            };
            let Flight { request, worker } = self.flight.take().unwrap();
            // The worker sends as its final operation; joining never waits for
            // additional network or storage work after a result is available.
            let joined = worker.handle.join();
            let result = if joined.is_err() {
                Err("membership transport worker panicked".into())
            } else {
                result
            };
            return self.finish(server, request, result).map(Some);
        }
        let now = Instant::now();
        let pg = rotated(self.groups.keys().copied().collect(), self.last)
            .into_iter()
            .find(|pg| self.groups[pg].next <= now);
        let Some(pg) = pg else {
            return Ok(None);
        };
        self.last = Some(pg);
        let group = self.groups.get_mut(&pg).unwrap();
        group.next = now + group.timing.interval;
        let (mut update, action) = match group.step(server, pg, apply) {
            Ok(value) => value,
            Err(error) => return Ok(Some(group.update(pg, "local-error", error))),
        };
        if let Some((replica, action)) = action {
            let input = group
                .input
                .ok_or_else(|| "recovery action has no chosen input".to_string())?;
            let plan = group
                .plan
                .ok_or_else(|| "recovery action has no canonical plan".to_string())?;
            let phase = action.phase();
            let kind = action.kind();
            match worker::spawn(Arc::clone(&self.transport), replica, action) {
                Ok(worker) => {
                    self.flight = Some(Flight {
                        request: PendingAction {
                            pg,
                            input,
                            plan,
                            replica,
                            phase,
                            action: kind,
                        },
                        worker,
                    });
                    update.replica = Some(replica);
                }
                Err(error) => {
                    update.phase = "worker-error";
                    update.detail = error;
                }
            }
        }
        Ok(Some(update))
    }
}

impl Drop for Recovery {
    fn drop(&mut self) {
        // A reopen or caught unwind cannot accumulate detached transport work.
        // All I/O carries existing RPC deadlines and at most one lost-reply retry.
        if let Some(flight) = self.flight.take() {
            let _ = flight.worker.handle.join();
        }
    }
}

#[cfg(test)]
mod tests;
