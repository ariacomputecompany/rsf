//! Election preparation authenticates candidates and reporting voters. It
//! collects frozen recovery inputs; it does not install a view or grant service.
use super::*;
use rsf_vsr::{VsrViewChangeReport, VsrViewChangeRequest, VsrViewChangeSnapshot};

impl NetworkVsrTransport {
    pub fn prepare_view_installation(
        &self,
        local: &mut DurableVsrCell,
        request: &VsrViewChangeRequest,
    ) -> Result<rsf_vsr::VsrViewInstallation, VsrTransportError> {
        let snapshots = self.collect_view_change(local, request)?;
        rsf_vsr::VsrViewInstallation::select(&snapshots).map_err(|err| VsrTransportError {
            replica: self.local,
            reason: err.message,
        })
    }

    pub fn fetch_view_change(
        &self,
        replica: NodeId,
        request: &VsrViewChangeRequest,
    ) -> Result<VsrViewChangeSnapshot, VsrTransportError> {
        let fail = |reason: String| VsrTransportError { replica, reason };
        if request.candidate != self.local || !request.voters.contains(&replica) {
            return Err(fail(
                "view-change collection requires the local candidate and a configured voter".into(),
            ));
        }
        let response = self
            .exchange_io(
                replica,
                VIEW_CHANGE_BEGIN,
                &request.encode().map_err(|err| fail(err.message))?,
            )
            .map_err(|err| fail(err.to_string()))?;
        let report = VsrViewChangeReport::decode(&response).map_err(|err| fail(err.message))?;
        if report.request != *request || report.replica != replica {
            return Err(fail(
                "view-change report differs from authenticated request or target".into(),
            ));
        }
        let mut query = report.encode().map_err(|err| fail(err.message))?;
        let prefix = query.len();
        let mut records = Vec::new();
        while (records.len() as u64) < report.history.records {
            let ordinal = records.len() as u64;
            query.truncate(prefix);
            query.extend_from_slice(&ordinal.to_le_bytes());
            let response = self
                .exchange_io(replica, VIEW_CHANGE_BATCH, &query)
                .map_err(|err| fail(err.to_string()))?;
            let batch =
                rsf_vsr::VsrHistoryBatch::decode(&response).map_err(|err| fail(err.message))?;
            if batch.ordinal != ordinal
                || ordinal + batch.records.len() as u64 > report.history.records
            {
                return Err(fail(
                    "view-change history batch range differs from frozen report".into(),
                ));
            }
            records.extend(batch.records);
        }
        let snapshot = VsrViewChangeSnapshot { report, records };
        snapshot
            .validate_integrity()
            .map_err(|err| fail(err.message))?;
        Ok(snapshot)
    }

    /// Collect one independently persisted promise per voter. Partial failure
    /// leaves promises fenced and can be retried after restart. A returned quorum
    /// is recovery input, not an election certificate or permission to write.
    pub fn collect_view_change(
        &self,
        local: &mut DurableVsrCell,
        request: &VsrViewChangeRequest,
    ) -> Result<Vec<VsrViewChangeSnapshot>, VsrTransportError> {
        let fail = |reason: String| VsrTransportError {
            replica: self.local,
            reason,
        };
        if request.candidate != self.local {
            return Err(fail(
                "only the local candidate may collect a view change".into(),
            ));
        }
        let snapshot = local
            .begin_view_change(self.local, self.local, request.clone())
            .map_err(|err| fail(err.message))?;
        let mut snapshots = vec![snapshot];
        let mut failures = Vec::new();
        for replica in request
            .voters
            .iter()
            .copied()
            .filter(|replica| *replica != self.local)
        {
            match self.fetch_view_change(replica, request) {
                Ok(snapshot) => snapshots.push(snapshot),
                Err(err) => failures.push(format!("{}: {}", replica.0, err.reason)),
            }
        }
        if snapshots.len() <= request.voters.len() / 2 {
            return Err(fail(format!(
                "view-change reports lack quorum ({}/{}): {}",
                snapshots.len(),
                request.voters.len(),
                failures.join("; ")
            )));
        }
        Ok(snapshots)
    }
}

impl PeerReplicaServer {
    pub(super) fn dispatch_election(
        &mut self,
        sender: NodeId,
        kind: u8,
        payload: &[u8],
    ) -> io::Result<Vec<u8>> {
        let rsf = |err: rsf_core::RsfError| invalid(err.message);
        let (request, expected) = if kind == VIEW_CHANGE_BEGIN {
            (VsrViewChangeRequest::decode(payload).map_err(rsf)?, None)
        } else {
            let end = payload
                .len()
                .checked_sub(8)
                .ok_or_else(|| invalid("truncated election record query"))?;
            let report = VsrViewChangeReport::decode(&payload[..end]).map_err(rsf)?;
            let ordinal = u64_at(payload, end)?;
            if report.replica != self.local {
                return Err(invalid("election record query names a different replica"));
            }
            (report.request.clone(), Some((report, ordinal)))
        };
        if sender != request.candidate || sender == self.local {
            return Err(invalid(
                "authenticated sender is not the remote view-change candidate",
            ));
        }
        self.pending_contact = Some(request.pg);
        let cell = self
            .cells
            .get_mut(&request.pg)
            .ok_or_else(|| invalid("view-change group is not hosted as a voter"))?;
        if kind == VIEW_CHANGE_BEGIN {
            let snapshot = cell
                .begin_view_change(self.local, sender, request)
                .map_err(rsf)?;
            let response = snapshot.report.encode().map_err(rsf)?;
            self.election_snapshots
                .insert(snapshot.report.request.pg, snapshot);
            return Ok(response);
        }
        if cell.view_change_promise() != Some(&request) {
            return Err(invalid(
                "view-change record query no longer matches the durable promise",
            ));
        }
        if !self.election_snapshots.contains_key(&request.pg) {
            let snapshot = cell
                .frozen_view_change_snapshot(self.local, &request)
                .map_err(rsf)?;
            self.election_snapshots.insert(request.pg, snapshot);
        }
        let snapshot = &self.election_snapshots[&request.pg];
        let (report, ordinal) = expected.ok_or_else(|| invalid("missing election record query"))?;
        if snapshot.report != report {
            return Err(invalid(
                "election snapshot identity differs from frozen history",
            ));
        }
        let ordinal = usize::try_from(ordinal)
            .map_err(|_| invalid("election record ordinal exceeds platform range"))?;
        if kind == VIEW_CHANGE_BATCH {
            return rsf_vsr::VsrHistoryBatch::pack(
                ordinal as u64,
                snapshot
                    .records
                    .get(ordinal..)
                    .ok_or_else(|| invalid("election batch ordinal is outside snapshot"))?,
            )
            .and_then(|batch| batch.encode())
            .map_err(rsf);
        }
        snapshot
            .records
            .get(ordinal)
            .ok_or_else(|| invalid("election record ordinal is outside snapshot"))?
            .encode()
            .map_err(rsf)
    }
}
