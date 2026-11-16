use super::*;

impl NetworkVsrTransport {
    pub fn replica_status(
        &self,
        replica: NodeId,
        pg: PlacementGroupId,
    ) -> Result<VsrReplicaStatus, VsrTransportError> {
        let fail = |reason: String| VsrTransportError { replica, reason };
        let bytes = self
            .exchange_io(replica, REPLICA_STATUS, &pg.0.to_le_bytes())
            .map_err(|err| fail(err.to_string()))?;
        let status = VsrReplicaStatus::decode(&bytes).map_err(|err| fail(err.message))?;
        if status.normal.pg != pg
            || !status.normal.voters.contains(&replica)
            || !status.normal.voters.contains(&self.local)
        {
            return Err(fail(
                "replica status differs from authenticated voting group".into(),
            ));
        }
        Ok(status)
    }
}
impl PeerReplicaServer {
    pub(in crate::replication) fn dispatch_status(
        &mut self,
        sender: NodeId,
        payload: &[u8],
    ) -> io::Result<Vec<u8>> {
        if payload.len() != 8 || sender == self.local {
            return Err(invalid("invalid replica status request"));
        }
        let pg = PlacementGroupId(u64_at(payload, 0)?);
        let cell = self
            .cells
            .get(&pg)
            .ok_or_else(|| invalid("status group is not hosted as a voter"))?;
        if !cell.contains_replica(sender) {
            return Err(invalid("status requester is not a configured voter"));
        }
        let bytes = cell
            .replica_status()
            .and_then(|s| s.encode())
            .map_err(|err| invalid(err.message))?;
        Ok(bytes)
    }
    pub(in crate::replication) fn dispatch_heartbeat(
        &mut self,
        sender: NodeId,
        payload: &[u8],
        apply: &mut impl FnMut(&VsrCommittedEntry) -> Result<(), String>,
    ) -> io::Result<Vec<u8>> {
        let probe = rsf_vsr::VsrPrimaryProbe::decode(payload).map_err(|e| invalid(e.message))?;
        let cell = self.cell(sender, probe.pg, apply)?;
        let local = cell.primary_probe().map_err(|e| invalid(e.message))?;
        if probe.view != local.view
            || probe.primary != local.primary
            || probe.voters != local.voters
        {
            return Err(invalid(
                "heartbeat differs from the installed primary and voting configuration",
            ));
        }
        cell.replica_status()
            .and_then(|s| s.encode())
            .map_err(|e| invalid(e.message))
    }
    pub(in crate::replication) fn record_primary_contact(
        &mut self,
        pg: PlacementGroupId,
        sender: NodeId,
        now: Instant,
    ) {
        let Some(cell) = self.cells.get(&pg) else {
            return;
        };
        let authority = cell
            .view_change_promise()
            .map_or(cell.primary(), |request| request.candidate);
        if sender == authority
            && let Some(clock) = self.election_clock.get_mut(&pg)
        {
            clock.last_contact = now;
            clock.own_failures = 0;
        }
    }
}
