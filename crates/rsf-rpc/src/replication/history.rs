//! Non-voting history transfer uses authenticated peer RPCs but cannot satisfy
//! any voting RPC. Source ownership and current-voter probes bracket transfer.
use super::*;
use rsf_vsr::{VsrHistoryDescriptor, VsrHistoryRecord, VsrPrimaryProbe};

impl NetworkVsrTransport {
    pub fn sync_learner(
        &mut self,
        target: NodeId,
        source: &mut DurableVsrCell,
    ) -> Result<LogIndex, VsrTransportError> {
        let fail = |reason: String| VsrTransportError {
            replica: target,
            reason,
        };
        if source.primary() != self.local || source.contains_replica(target) || target == self.local
        {
            return Err(fail(
                "learner transfer requires the local primary and a non-voting target".into(),
            ));
        }
        let snapshot = source.history_snapshot().map_err(|err| fail(err.message))?;
        self.confirm_history_quorum(&snapshot.descriptor.probe, target)?;
        let id = snapshot.descriptor.id().map_err(|err| fail(err.message))?;
        let descriptor = snapshot
            .descriptor
            .encode()
            .map_err(|err| fail(err.message))?;
        let count = self.request(target, HISTORY_BEGIN, &descriptor)?.0;
        if count > snapshot.descriptor.records {
            return Err(fail("learner resume position exceeds snapshot".into()));
        }
        for (ordinal, record) in snapshot.records.iter().enumerate().skip(count as usize) {
            let mut bytes = snapshot.descriptor.probe.pg.0.to_le_bytes().to_vec();
            bytes.extend_from_slice(&id);
            bytes.extend_from_slice(&(ordinal as u64).to_le_bytes());
            bytes.extend_from_slice(&record.encode().map_err(|err| fail(err.message))?);
            self.expect_index(target, HISTORY_RECORD, &bytes, LogIndex(ordinal as u64 + 1))?;
        }
        // Re-probe before publication: a stale primary must not report successful
        // catch-up after it has lost current configuration authority.
        self.confirm_history_quorum(&snapshot.descriptor.probe, target)?;
        let mut bytes = snapshot.descriptor.probe.pg.0.to_le_bytes().to_vec();
        bytes.extend_from_slice(&id);
        self.expect_index(
            target,
            HISTORY_FINISH,
            &bytes,
            snapshot.descriptor.probe.committed_tail,
        )?;
        Ok(snapshot.descriptor.probe.committed_tail)
    }

    fn confirm_history_quorum(
        &mut self,
        probe: &VsrPrimaryProbe,
        target: NodeId,
    ) -> Result<(), VsrTransportError> {
        let mut confirmed = 1;
        let mut failures = Vec::new();
        for voter in &probe.voters {
            if *voter == self.local {
                continue;
            }
            match self.confirm_primary(*voter, probe) {
                Ok(()) => confirmed += 1,
                Err(err) => failures.push(format!("{}: {}", voter.0, err.reason)),
            }
        }
        if confirmed <= probe.voters.len() / 2 {
            return Err(VsrTransportError {
                replica: target,
                reason: format!(
                    "learner source lacks current primary quorum ({confirmed}/{}): {}",
                    probe.voters.len(),
                    failures.join("; ")
                ),
            });
        }
        Ok(())
    }
}

impl PeerReplicaServer {
    pub(super) fn dispatch_history(
        &mut self,
        sender: NodeId,
        kind: u8,
        payload: &[u8],
        apply: &mut impl FnMut(&VsrCommittedEntry) -> Result<(), String>,
    ) -> io::Result<LogIndex> {
        let rsf = |err: rsf_core::RsfError| invalid(err.message);
        let descriptor = if kind == HISTORY_BEGIN {
            Some(VsrHistoryDescriptor::decode(payload).map_err(rsf)?)
        } else {
            None
        };
        let pg = match &descriptor {
            Some(descriptor) => descriptor.probe.pg,
            None => PlacementGroupId(u64_at(payload, 0)?),
        };
        let learner = self
            .learners
            .get_mut(&pg)
            .ok_or_else(|| invalid("placement group is not configured as a learner"))?;
        if sender != learner.source() {
            return Err(invalid(
                "sender is not this learner's configured history source",
            ));
        }
        if let Some(descriptor) = descriptor {
            return learner.begin(descriptor).map(LogIndex).map_err(rsf);
        }
        let id = payload
            .get(8..40)
            .ok_or_else(|| invalid("truncated learner snapshot id"))?
            .try_into()
            .unwrap();
        match kind {
            HISTORY_RECORD => {
                let ordinal = u64_at(payload, 40)?;
                let record = VsrHistoryRecord::decode(
                    payload
                        .get(48..)
                        .ok_or_else(|| invalid("missing history record"))?,
                )
                .map_err(rsf)?;
                learner
                    .append(id, ordinal, record)
                    .map(LogIndex)
                    .map_err(rsf)
            }
            HISTORY_FINISH if payload.len() == 40 => {
                let tail = learner.finish(id).map_err(rsf)?;
                // Publishing the WAL precedes materialization, so crashes or lost
                // replies can retry the idempotent materializer from ACTIVE.
                if self.learner_materialized.get(&pg) != Some(&id) {
                    for entry in learner.committed_entries().map_err(rsf)? {
                        apply(&entry).map_err(invalid)?;
                    }
                    self.learner_materialized.insert(pg, id);
                }
                Ok(tail)
            }
            _ => Err(invalid("invalid learner history request")),
        }
    }
}
