use super::*;

impl PeerReplicaServer {
    pub(super) fn primary_control(
        &mut self,
        pg: PlacementGroupId,
        status: &VsrReplicaStatus,
        timing: PeerElectionTiming,
        now: Instant,
        apply: &mut impl FnMut(&VsrCommittedEntry) -> Result<(), String>,
        progress: &mut impl FnMut(PeerElectionProgress),
    ) -> Result<String, String> {
        let local = self.local;
        if self.installation_pending.contains(&pg) {
            for entry in self.cells[&pg].committed_prefix().map_err(|e| e.message)? {
                apply(&entry)?;
            }
            self.installation_pending.remove(&pg);
        }
        if self.cells[&pg].has_installed_view()
            && self.election_clock[&pg].confirmed_view != Some(status.normal.view)
        {
            let input = self
                .cells
                .get_mut(&pg)
                .unwrap()
                .installation_input()
                .map_err(|e| e.message)?;
            if input.is_some_and(|i| {
                i.request().candidate == self.local && i.request().target_view == status.normal.view
            }) {
                let mut superseded = false;
                for replica in status.normal.voters.iter().copied().filter(|n| *n != local) {
                    if let Ok(remote) = self.control_transport.replica_status(replica, pg)
                        && remote.normal.voters == status.normal.voters
                    {
                        superseded |= self.observe_view(pg, &remote) > status.normal.view;
                    }
                }
                if self.awaiting_newer_view_repair(pg, timing.timeout) {
                    return Err(
                        "higher installed view observed; awaiting current primary repair".into(),
                    );
                }
                if superseded || self.election_clock[&pg].own_failures >= 2 {
                    return self.elect(pg, status, timing, apply, progress);
                }
                let started = Instant::now();
                progress(PeerElectionProgress {
                    node: self.local,
                    pg,
                    phase: "resume",
                    elapsed: Duration::ZERO,
                    records: None,
                });
                if let Err(error) = self.resume_installed(pg, apply) {
                    let clock = self.election_clock.get_mut(&pg).unwrap();
                    clock.own_failures = clock.own_failures.saturating_add(1);
                    return Err(error);
                }
                progress(PeerElectionProgress {
                    node: self.local,
                    pg,
                    phase: "complete",
                    elapsed: started.elapsed(),
                    records: None,
                });
                return Ok(
                    "durable local installation resumed and confirmed on voting quorum".into(),
                );
            }
            // A locally installed view without retained upload input must obtain
            // a new reporting quorum; it cannot infer network installation.
            return self.elect(pg, status, timing, apply, progress);
        }
        let mut confirmed = 1;
        let mut newer = false;
        let mut reachable_lag = false;
        for replica in status
            .normal
            .voters
            .iter()
            .copied()
            .filter(|node| *node != local)
        {
            // A status query is deliberately read-only. Only a normal-view
            // heartbeat or accepted protocol progress refreshes follower timers.
            let heartbeat = self
                .control_transport
                .exchange_io(
                    replica,
                    HEARTBEAT,
                    &status.normal.encode().map_err(|e| e.message)?,
                )
                .map_err(|e| e.to_string())
                .and_then(|bytes| VsrReplicaStatus::decode(&bytes).map_err(|e| e.message));
            match heartbeat {
                Ok(remote) => {
                    reachable_lag |= remote.normal != status.normal || remote.promise.is_some();
                    newer |= self.observe_view(pg, &remote) > status.normal.view;
                    if remote.normal == status.normal && remote.promise.is_none() {
                        confirmed += 1;
                    }
                }
                Err(_) => {
                    // Inventory can reveal a higher fence, but cannot replace
                    // an active heartbeat/materialization acknowledgement.
                    if let Ok(remote) = self.control_transport.replica_status(replica, pg) {
                        reachable_lag = true;
                        newer |= self.observe_view(pg, &remote) > status.normal.view;
                    }
                }
            }
        }
        if !self.cells[&pg].has_installed_view()
            && !newer
            && confirmed > status.normal.voters.len() / 2
        {
            return self.elect(pg, status, timing, apply, progress);
        }
        // A selected input remains available after publication so restarted or
        // previously unreachable voters can adopt it without stopping peers.
        if self.cells[&pg].has_installed_view()
            && reachable_lag
            && !newer
            && now.saturating_duration_since(self.election_clock[&pg].last_repair)
                >= (timing.timeout / 2).max(timing.heartbeat)
        {
            self.election_clock.get_mut(&pg).unwrap().last_repair = now;
            let input = self
                .cells
                .get_mut(&pg)
                .unwrap()
                .installation_input()
                .map_err(|e| e.message)?;
            if input.is_some_and(|i| {
                i.request().candidate == self.local && i.request().target_view == status.normal.view
            }) {
                let started = Instant::now();
                progress(PeerElectionProgress {
                    node: self.local,
                    pg,
                    phase: "resume",
                    elapsed: Duration::ZERO,
                    records: None,
                });
                self.resume_installed(pg, apply)?;
                progress(PeerElectionProgress {
                    node: self.local,
                    pg,
                    phase: "complete",
                    elapsed: started.elapsed(),
                    records: None,
                });
                return Ok("installed quorum confirmed; lagging-voter installation retried".into());
            }
        }
        if confirmed <= status.normal.voters.len() / 2 {
            if self.awaiting_newer_view_repair(pg, timing.timeout) {
                return Err(
                    "higher installed view observed; awaiting current primary repair".into(),
                );
            }
            if now.saturating_duration_since(self.election_clock[&pg].last_contact)
                >= timing.timeout
            {
                return self.elect(pg, status, timing, apply, progress);
            }
            return Err(format!(
                "primary status lacks matching installed quorum ({confirmed}/{})",
                status.normal.voters.len()
            ));
        }
        let clock = self.election_clock.get_mut(&pg).unwrap();
        clock.last_contact = Instant::now();
        clock.own_failures = 0;
        Ok("current view and committed tip confirmed by voting quorum".into())
    }

    fn resume_installed(
        &mut self,
        pg: PlacementGroupId,
        apply: &mut impl FnMut(&VsrCommittedEntry) -> Result<(), String>,
    ) -> Result<(), String> {
        let cell = self.cells.get_mut(&pg).unwrap();
        self.control_transport
            .resume_view_installation(cell, apply)
            .map_err(|e| e.reason)?;
        let tail = cell.committed_tail();
        if tail.0 != 0 {
            cell.replicate_with_transport(self.local, tail, &mut self.control_transport)
                .map_err(|e| e.message)?;
        }
        let clock = self.election_clock.get_mut(&pg).unwrap();
        clock.confirmed_view = Some(cell.view());
        clock.last_repair = Instant::now();
        clock.own_failures = 0;
        self.installation_pending.remove(&pg);
        Ok(())
    }
}
