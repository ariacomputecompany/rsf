//! Non-voting history transfer uses authenticated peer RPCs but cannot satisfy
//! any voting RPC. Source ownership and current-voter probes bracket transfer.
use super::*;
pub(super) mod status;
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
        let count = self
            .request(target, HISTORY_BEGIN, &descriptor)
            .map_err(|err| fail(format!("learner BEGIN: {}", err.reason)))?
            .0;
        if count > snapshot.descriptor.records {
            return Err(fail("learner resume position exceeds snapshot".into()));
        }
        for (ordinal, record) in snapshot.records.iter().enumerate().skip(count as usize) {
            let mut bytes = snapshot.descriptor.probe.pg.0.to_le_bytes().to_vec();
            bytes.extend_from_slice(&id);
            bytes.extend_from_slice(&(ordinal as u64).to_le_bytes());
            bytes.extend_from_slice(&record.encode().map_err(|err| fail(err.message))?);
            self.expect_index(target, HISTORY_RECORD, &bytes, LogIndex(ordinal as u64 + 1))
                .map_err(|err| fail(format!("learner RECORD {ordinal}: {}", err.reason)))?;
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
        )
        .map_err(|err| fail(format!("learner FINISH: {}", err.reason)))?;
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
        let reserved = self.is_control_group(pg);
        if self.learners.get(&pg).is_some_and(|owner| {
            matches!(
                owner,
                learner::LearnerOwner::Control(_) | learner::LearnerOwner::InitialControl(_)
            )
        }) != reserved
        {
            return Err(invalid(
                "control learner owner differs from immutable binding",
            ));
        }
        let learner = self
            .learners
            .get_mut(&pg)
            .ok_or_else(|| invalid("placement group is not configured as a learner"))?;
        if matches!(learner, learner::LearnerOwner::InitialControl(_)) {
            return Err(invalid(
                "initial control requires its distinct bounded native-persisted protocol",
            ));
        }
        if Some(sender) != learner.configured_source() {
            return Err(invalid(
                "sender is not this learner's configured history source",
            ));
        }
        if let Some(descriptor) = descriptor {
            return match learner {
                learner::LearnerOwner::Application(owner) => {
                    owner.begin(descriptor).map(LogIndex).map_err(rsf)
                }
                learner::LearnerOwner::Control(owner) => {
                    owner.begin(sender, descriptor).map(LogIndex)
                }
                learner::LearnerOwner::InitialControl(_) => {
                    unreachable!("initial history rejected")
                }
            };
        }
        let id = payload
            .get(8..40)
            .ok_or_else(|| invalid("truncated learner snapshot id"))?
            .try_into()
            .unwrap();
        match kind {
            HISTORY_RECORD => {
                let ordinal = u64_at(payload, 40)?;
                let encoded = payload
                    .get(48..)
                    .ok_or_else(|| invalid("missing history record"))?;
                match learner {
                    learner::LearnerOwner::Application(owner) => owner
                        .append(id, ordinal, VsrHistoryRecord::decode(encoded).map_err(rsf)?)
                        .map(LogIndex)
                        .map_err(rsf),
                    learner::LearnerOwner::Control(owner) => {
                        owner.append(sender, id, ordinal, encoded).map(LogIndex)
                    }
                    learner::LearnerOwner::InitialControl(_) => {
                        unreachable!("initial history rejected")
                    }
                }
            }
            HISTORY_FINISH if payload.len() == 40 => {
                let tail = match learner {
                    learner::LearnerOwner::Application(owner) => owner.finish(id).map_err(rsf)?,
                    learner::LearnerOwner::InitialControl(_) => {
                        unreachable!("initial history rejected")
                    }
                    learner::LearnerOwner::Control(owner) => {
                        let (tail, state) = owner.finish(sender, id)?;
                        let binding = self
                            .managed_control
                            .as_mut()
                            .ok_or_else(|| invalid("control learner lost immutable binding"))?;
                        if binding.pg() != pg {
                            return Err(invalid("control learner binding PG mismatch"));
                        }
                        binding.record_learner_observation(state);
                        self.learner_materialized.insert(pg, id);
                        return Ok(tail);
                    }
                };
                if self.learner_materialized.get(&pg) != Some(&id) {
                    self.materialize_owned_learner(pg, apply).map_err(invalid)?;
                    self.learner_materialized.insert(pg, id);
                }
                Ok(tail)
            }
            _ => Err(invalid("invalid learner history request")),
        }
    }
}
