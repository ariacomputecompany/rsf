//! Authenticated, restartable view installation. Success requires an installed
//! majority and application of the committed prefix before acknowledging it.
use super::*;
use rsf_vsr::{
    VsrHistoryRecord, VsrInstallationInput, VsrViewChangeRequest, VsrViewChangeSnapshot,
};

impl NetworkVsrTransport {
    pub fn prepare_installation_input(
        &self,
        local: &mut DurableVsrCell,
        request: &VsrViewChangeRequest,
    ) -> Result<(VsrInstallationInput, Vec<VsrViewChangeSnapshot>), VsrTransportError> {
        let snapshots = self.collect_view_change(local, request)?;
        let input =
            VsrInstallationInput::from_snapshots(&snapshots).map_err(|err| VsrTransportError {
                replica: self.local,
                reason: err.message,
            })?;
        Ok((input, snapshots))
    }

    /// Persist the complete input locally before any publication. The caller
    /// may resume from this durable copy even if the candidate already activated.
    pub fn stage_installation_input(
        &self,
        local: &mut DurableVsrCell,
        input: &VsrInstallationInput,
        snapshots: &[VsrViewChangeSnapshot],
    ) -> Result<(), VsrTransportError> {
        let fail = |err: rsf_core::RsfError| VsrTransportError {
            replica: self.local,
            reason: err.message,
        };
        let checked = VsrInstallationInput::from_snapshots(snapshots).map_err(fail)?;
        if checked != *input || input.request().candidate != self.local {
            return Err(VsrTransportError {
                replica: self.local,
                reason: "installation snapshots or local candidate differ".into(),
            });
        }
        let mut count = local
            .begin_installation_input(self.local, self.local, input.clone())
            .map_err(fail)?;
        let id = input.id().map_err(fail)?;
        let records = ordered_records(input, snapshots).map_err(|reason| VsrTransportError {
            replica: self.local,
            reason,
        })?;
        while count < records.len() as u64 {
            let batch =
                rsf_vsr::VsrHistoryBatch::pack(count, &records[count as usize..]).map_err(fail)?;
            count = local
                .append_installation_batch(self.local, id, batch)
                .map_err(fail)?;
        }
        // Verify an existing prefix as well as newly appended records.
        local.installation_snapshots(id).map_err(fail)?;
        Ok(())
    }

    /// Resume publication from durable input; do not report takeover until a
    /// majority, including this candidate, has installed and materialized it.
    pub fn resume_view_installation(
        &self,
        local: &mut DurableVsrCell,
        apply: &mut impl FnMut(&VsrCommittedEntry) -> Result<(), String>,
    ) -> Result<rsf_core::Epoch, VsrTransportError> {
        self.resume_view_installation_owned(local, &mut |cell| {
            for entry in cell.committed_prefix().map_err(|e| e.message)? {
                apply(&entry)?;
            }
            Ok(())
        })
    }

    // Internal owner-aware completion permits configured projections to inspect
    // the installed lineage without ever exposing a learner or synthetic cell.
    pub(in crate::replication) fn resume_view_installation_owned(
        &self,
        local: &mut DurableVsrCell,
        materialize: &mut impl FnMut(&DurableVsrCell) -> Result<(), String>,
    ) -> Result<rsf_core::Epoch, VsrTransportError> {
        let fail = |reason: String| VsrTransportError {
            replica: self.local,
            reason,
        };
        let input = local
            .installation_input()
            .map_err(|err| fail(err.message))?
            .ok_or_else(|| fail("no durable installation input".into()))?;
        if input.request().candidate != self.local {
            return Err(fail(
                "only the local candidate can resume installation".into(),
            ));
        }
        let id = input.id().map_err(|err| fail(err.message))?;
        let snapshots = local
            .installation_snapshots(id)
            .map_err(|err| fail(err.message))?;
        let view = local
            .finish_installation_input(self.local, id)
            .map_err(|err| fail(err.message))?;
        materialize(local).map_err(fail)?;
        let mut installed = 1;
        let mut failures = Vec::new();
        for replica in input
            .request()
            .voters
            .iter()
            .copied()
            .filter(|r| *r != self.local)
        {
            match self.send_installation_input(replica, &input, &snapshots) {
                Ok(()) => installed += 1,
                Err(err) => failures.push(format!("{}: {}", replica.0, err.reason)),
            }
        }
        if installed <= input.request().voters.len() / 2 {
            return Err(fail(format!(
                "installed view lacks quorum ({installed}/{}): {}",
                input.request().voters.len(),
                failures.join("; ")
            )));
        }
        Ok(view)
    }

    fn send_installation_input(
        &self,
        replica: NodeId,
        input: &VsrInstallationInput,
        snapshots: &[VsrViewChangeSnapshot],
    ) -> Result<(), VsrTransportError> {
        let fail = |reason: String| VsrTransportError { replica, reason };
        let descriptor = input.encode().map_err(|err| fail(err.message))?;
        let mut count = self
            .request_io(replica, INSTALL_BEGIN, &descriptor)
            .map_err(|err| fail(err.to_string()))?;
        if count > input.records().map_err(|err| fail(err.message))? {
            return Err(fail("remote installation count exceeds descriptor".into()));
        }
        let mut prefix = input.request().pg.0.to_le_bytes().to_vec();
        prefix.extend_from_slice(&input.id().map_err(|err| fail(err.message))?);
        let records = ordered_records(input, snapshots).map_err(fail)?;
        while count < records.len() as u64 {
            let batch = rsf_vsr::VsrHistoryBatch::pack(count, &records[count as usize..])
                .map_err(|err| fail(err.message))?;
            let expected = count + batch.records.len() as u64;
            let mut payload = prefix.clone();
            payload.extend_from_slice(&batch.encode().map_err(|err| fail(err.message))?);
            let next = self
                .request_io(replica, INSTALL_BATCH, &payload)
                .map_err(|err| fail(err.to_string()))?;
            if next != expected {
                return Err(fail(
                    "remote installation batch acknowledgement differs".into(),
                ));
            }
            count = next;
        }
        let view = self
            .request_io(replica, INSTALL_FINISH, &prefix)
            .map_err(|err| fail(err.to_string()))?;
        if view != input.request().target_view.0 {
            return Err(fail(
                "remote installation acknowledged a different view".into(),
            ));
        }
        Ok(())
    }
}

impl PeerReplicaServer {
    pub(super) fn dispatch_installation(
        &mut self,
        sender: NodeId,
        kind: u8,
        payload: &[u8],
        apply: &mut impl FnMut(&VsrCommittedEntry) -> Result<(), String>,
    ) -> io::Result<Vec<u8>> {
        let rsf = |err: rsf_core::RsfError| invalid(err.message);
        if sender == self.local {
            return Err(invalid("installation requires a remote candidate"));
        }
        let descriptor = if kind == INSTALL_BEGIN {
            Some(VsrInstallationInput::decode(payload).map_err(rsf)?)
        } else {
            None
        };
        let pg = match &descriptor {
            Some(d) => d.request().pg,
            None => PlacementGroupId(u64_at(payload, 0)?),
        };
        self.pending_contact = Some(pg);
        let control = self.is_control_group(pg);
        let cell = self
            .cells
            .get_mut(&pg)
            .ok_or_else(|| invalid("installation group is not hosted as a voter"))?;
        let count = if let Some(descriptor) = descriptor {
            cell.begin_installation_input(self.local, sender, descriptor)
                .map_err(rsf)?
        } else {
            let id: [u8; 32] = payload
                .get(8..40)
                .ok_or_else(|| invalid("truncated installation identity"))?
                .try_into()
                .unwrap();
            match kind {
                INSTALL_BATCH => cell
                    .append_installation_batch(
                        sender,
                        id,
                        rsf_vsr::VsrHistoryBatch::decode(
                            payload
                                .get(40..)
                                .ok_or_else(|| invalid("missing installation batch"))?,
                        )
                        .map_err(rsf)?,
                    )
                    .map_err(rsf)?,
                INSTALL_RECORD => cell
                    .append_installation_input(
                        sender,
                        id,
                        u64_at(payload, 40)?,
                        VsrHistoryRecord::decode(
                            payload
                                .get(48..)
                                .ok_or_else(|| invalid("missing installation record"))?,
                        )
                        .map_err(rsf)?,
                    )
                    .map_err(rsf)?,
                INSTALL_FINISH if payload.len() == 40 => {
                    let materialized = !self.installation_pending.contains(&pg);
                    let before = if cell.view_change_promise().is_none() {
                        Some(cell.state_token().map_err(rsf)?)
                    } else {
                        None
                    };
                    self.installation_pending.insert(pg);
                    let view = cell.finish_installation_input(sender, id).map_err(rsf)?;
                    if control || !materialized || before != Some(cell.state_token().map_err(rsf)?)
                    {
                        managed_control::materialize_voter(
                            self.managed_control.as_mut(),
                            cell,
                            apply,
                        )
                        .map_err(invalid)?;
                    }
                    self.installation_pending.remove(&pg);
                    view.0
                }
                _ => return Err(invalid("invalid installation request size or kind")),
            }
        };
        Ok(count.to_le_bytes().to_vec())
    }
}

// Descriptor reports define the canonical concatenation, irrespective of the
// order in which network reports arrived or which voter became candidate.
fn ordered_records(
    input: &VsrInstallationInput,
    snapshots: &[VsrViewChangeSnapshot],
) -> Result<Vec<VsrHistoryRecord>, String> {
    let mut records = Vec::new();
    for report in input.reports() {
        let snapshot = snapshots
            .iter()
            .find(|snapshot| snapshot.report == *report)
            .ok_or("installation descriptor has no matching snapshot")?;
        if snapshot.records.len() as u64 != report.history.records {
            return Err("installation snapshot record count differs".into());
        }
        records.extend(snapshot.records.iter().cloned());
    }
    Ok(records)
}
