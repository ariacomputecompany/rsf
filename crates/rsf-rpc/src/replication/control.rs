//! Serialized peer control. Timeouts can suspect a primary; only persisted
//! promises, quorum selection and complete installation establish a new view.
use super::*;
mod primary;
mod status;
use rsf_core::Epoch;
use rsf_vsr::{VsrReplicaStatus, VsrViewChangeRequest};

#[derive(Clone, Copy, Debug)]
pub struct PeerElectionTiming {
    pub heartbeat: Duration,
    pub timeout: Duration,
    pub rank_delay: Duration,
}
impl PeerElectionTiming {
    pub fn validate(&self, rpc_timeout: Duration, voters: usize) -> Result<(), String> {
        let budget = rpc_timeout
            .checked_mul((voters.saturating_sub(1) as u32).saturating_mul(2))
            .ok_or("election RPC budget overflow")?;
        if voters == 0
            || voters > 4096
            || rpc_timeout.is_zero()
            || self.heartbeat < Duration::from_millis(1)
            || self.heartbeat > Duration::from_secs(60)
            || self.timeout > Duration::from_secs(3600)
            || self.rank_delay > self.timeout
            || self.rank_delay < self.heartbeat
            || self.timeout < budget.saturating_add(self.heartbeat.saturating_mul(3))
        {
            return Err("election timeout must cover all peer RPC deadlines plus three heartbeats; rank delay must cover a heartbeat".into());
        }
        Ok(())
    }
}
/// Diagnostic progress only. A phase event never establishes voting authority
/// or client readiness; only the completed update can report quorum confirmation.
#[derive(Clone, Copy, Debug)]
pub struct PeerElectionProgress {
    pub node: NodeId,
    pub pg: PlacementGroupId,
    pub phase: &'static str,
    pub elapsed: Duration,
    pub records: Option<u64>,
}
#[derive(Debug)]
pub struct PeerElectionUpdate {
    pub pg: PlacementGroupId,
    pub view: Epoch,
    pub primary: NodeId,
    pub quorum_confirmed: bool,
    pub detail: String,
}
#[derive(Debug)]
pub(super) struct Clock {
    last_contact: Instant,
    next_action: Instant,
    last_repair: Instant,
    own_failures: u32,
    confirmed_view: Option<Epoch>,
    // Authenticated inventory is a lower bound for the next proposal, not
    // evidence that a view is installed or application service is permitted.
    highest_observed: Epoch,
    // Inventory grants one bounded adoption window for a newer installed view;
    // repeated status reads cannot renew it or follower liveness.
    newer_installed: Option<(Epoch, Instant)>,
}
impl Clock {
    pub(super) fn new(now: Instant) -> Self {
        Self {
            last_contact: now,
            next_action: now,
            last_repair: now,
            own_failures: 0,
            confirmed_view: None,
            highest_observed: Epoch(0),
            newer_installed: None,
        }
    }
}
impl PeerReplicaServer {
    /// At most one group's outbound control action per call. Run between bounded
    /// incoming-RPC batches; the caller retains exclusive ownership of all cells.
    pub fn poll_elections(
        &mut self,
        timing: PeerElectionTiming,
        apply: &mut impl FnMut(&VsrCommittedEntry) -> Result<(), String>,
    ) -> Result<Option<PeerElectionUpdate>, String> {
        self.poll_elections_with_progress(timing, apply, &mut |_| {})
    }

    /// Observe phase boundaries even when a slow durable operation has not yet
    /// returned an election update. The observer must not block or change state.
    pub fn poll_elections_with_progress(
        &mut self,
        timing: PeerElectionTiming,
        apply: &mut impl FnMut(&VsrCommittedEntry) -> Result<(), String>,
        progress: &mut impl FnMut(PeerElectionProgress),
    ) -> Result<Option<PeerElectionUpdate>, String> {
        for cell in self.cells.values() {
            timing.validate(self.timeout, cell.replicas().len())?;
        }
        let now = Instant::now();
        let mut groups = self.cells.keys().copied().collect::<Vec<_>>();
        if let Some(last) = self.last_control_group {
            let split = groups.partition_point(|pg| *pg <= last);
            groups.rotate_left(split);
        }
        for pg in groups {
            let status = self.cells[&pg]
                .replica_status()
                .map_err(|err| err.message)?;
            let clock = &self.election_clock[&pg];
            if now < clock.next_action {
                continue;
            }
            let local_primary = status.normal.primary == self.local && status.promise.is_none();
            let authority = status
                .promise
                .as_ref()
                .map_or(status.normal.primary, |request| request.candidate);
            let rank = successor_rank(&status.normal.voters, authority, self.local)?;
            let deadline = timing.timeout.saturating_add(
                timing
                    .rank_delay
                    .saturating_mul(rank.saturating_sub(1) as u32),
            );
            let own_promise = status
                .promise
                .as_ref()
                .is_some_and(|p| p.candidate == self.local);
            if !local_primary
                && !own_promise
                && now.saturating_duration_since(clock.last_contact) < deadline
            {
                continue;
            }
            self.last_control_group = Some(pg);
            let result = if local_primary {
                self.primary_control(pg, &status, timing, now, apply, progress)
            } else {
                self.elect(pg, &status, timing, apply, progress)
            };
            let clock = self.election_clock.get_mut(&pg).unwrap();
            let delay = if result.is_err() {
                timing.rank_delay
            } else {
                timing.heartbeat
            };
            clock.next_action = clock.next_action.max(Instant::now() + delay);
            let cell = &self.cells[&pg];
            return Ok(Some(PeerElectionUpdate {
                pg,
                view: cell.view(),
                primary: cell.primary(),
                quorum_confirmed: result.is_ok(),
                detail: result.unwrap_or_else(|err| err),
            }));
        }
        Ok(None)
    }

    fn elect(
        &mut self,
        pg: PlacementGroupId,
        status: &VsrReplicaStatus,
        timing: PeerElectionTiming,
        apply: &mut impl FnMut(&VsrCommittedEntry) -> Result<(), String>,
        progress: &mut impl FnMut(PeerElectionProgress),
    ) -> Result<String, String> {
        let started = Instant::now();
        let local = self.local;
        let mut phase = |phase, records| {
            progress(PeerElectionProgress {
                node: local,
                pg,
                phase,
                elapsed: started.elapsed(),
                records,
            })
        };
        self.installation_pending.insert(pg);
        self.election_clock.get_mut(&pg).unwrap().confirmed_view = None;
        let same = status
            .promise
            .as_ref()
            .filter(|p| {
                p.candidate == self.local
                    && p.target_view >= self.election_clock[&pg].highest_observed
            })
            .cloned();
        let failures = self.election_clock[&pg].own_failures;
        if let Some(request) = &same
            && failures < 2
        {
            let cell = self.cells.get_mut(&pg).unwrap();
            if let Some(input) = cell.installation_input().map_err(|e| e.message)?
                && input.request() == request
                && cell
                    .begin_installation_input(self.local, self.local, input.clone())
                    .map_err(|e| e.message)?
                    == input.records().map_err(|e| e.message)?
            {
                phase("resume", Some(input.records().map_err(|e| e.message)?));
                let result = self
                    .control_transport
                    .resume_view_installation(cell, apply)
                    .map_err(|e| e.reason);
                return self.election_result(pg, result.map(|_| ()), timing);
            }
        }
        let request = if let Some(request) = same.filter(|_| failures < 2) {
            request
        } else {
            let mut highest = status.promise.as_ref().map_or(status.normal.view.0, |p| {
                p.target_view.0.max(status.normal.view.0)
            });
            highest = highest.max(self.election_clock[&pg].highest_observed.0);
            for replica in status
                .normal
                .voters
                .iter()
                .copied()
                .filter(|node| *node != self.local)
            {
                if let Ok(remote) = self.control_transport.replica_status(replica, pg)
                    && remote.normal.voters == status.normal.voters
                {
                    highest = highest
                        .max(remote.normal.view.0)
                        .max(remote.promise.map_or(0, |p| p.target_view.0));
                }
            }
            self.election_clock.get_mut(&pg).unwrap().highest_observed = Epoch(highest);
            VsrViewChangeRequest {
                pg,
                previous_view: Epoch(1),
                target_view: Epoch(highest.checked_add(1).ok_or("election view exhausted")?),
                candidate: self.local,
                voters: status.normal.voters.clone(),
            }
        };
        let cell = self.cells.get_mut(&pg).unwrap();
        let result = (|| {
            phase("collect", None);
            let (input, snapshots) = self
                .control_transport
                .prepare_installation_input(cell, &request)
                .map_err(|e| e.reason)?;
            let records = input.records().map_err(|e| e.message)?;
            phase("stage", Some(records));
            self.control_transport
                .stage_installation_input(cell, &input, &snapshots)
                .map_err(|e| e.reason)?;
            phase("install", Some(records));
            self.control_transport
                .resume_view_installation(cell, apply)
                .map_err(|e| e.reason)?;
            Ok(())
        })();
        phase(if result.is_ok() { "complete" } else { "failed" }, None);
        self.election_result(pg, result, timing)
    }

    fn observe_view(&mut self, pg: PlacementGroupId, status: &VsrReplicaStatus) -> Epoch {
        let view = status
            .normal
            .view
            .max(status.promise.as_ref().map_or(Epoch(0), |p| p.target_view));
        let local_view = self.cells[&pg].view();
        let clock = self.election_clock.get_mut(&pg).unwrap();
        clock.highest_observed = clock.highest_observed.max(view);
        if status.promise.is_none()
            && status.normal.view > local_view
            && status.normal.primary != self.local
            && clock
                .newer_installed
                .is_none_or(|(seen, _)| status.normal.view > seen)
        {
            clock.newer_installed = Some((status.normal.view, Instant::now()));
        }
        view
    }

    fn awaiting_newer_view_repair(&self, pg: PlacementGroupId, timeout: Duration) -> bool {
        self.election_clock[&pg]
            .newer_installed
            .is_some_and(|(view, first_seen)| {
                view > self.cells[&pg].view() && first_seen.elapsed() < timeout
            })
    }
    fn election_result(
        &mut self,
        pg: PlacementGroupId,
        result: Result<(), String>,
        timing: PeerElectionTiming,
    ) -> Result<String, String> {
        let clock = self.election_clock.get_mut(&pg).unwrap();
        match result {
            Ok(()) => {
                clock.own_failures = 0;
                clock.confirmed_view = Some(self.cells[&pg].view());
                clock.last_contact = Instant::now();
                clock.last_repair = Instant::now();
                self.installation_pending.remove(&pg);
                Ok("new view installed and materialized on voting quorum".into())
            }
            Err(err) => {
                clock.own_failures = clock.own_failures.saturating_add(1);
                clock.next_action = Instant::now() + timing.rank_delay;
                Err(err)
            }
        }
    }
}
fn successor_rank(voters: &[NodeId], authority: NodeId, local: NodeId) -> Result<usize, String> {
    let from = voters
        .iter()
        .position(|n| *n == authority)
        .ok_or("authority is not a voter")?;
    let to = voters
        .iter()
        .position(|n| *n == local)
        .ok_or("local node is not a voter")?;
    Ok((to + voters.len() - from) % voters.len())
}

#[cfg(test)]
mod tests;
