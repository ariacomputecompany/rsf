use std::collections::BTreeMap;
use std::path::Path;

use rsf_core::{Epoch, LogIndex, PlacementGroupId, RsfResult, storage::WalRecordKind};
use rsf_storage::{SegmentScanFilter, SegmentStore, WalAppend};

use crate::codec::{UpgradeWalRecord, decode_upgrade_wal_payload, encode_upgrade_wal_payload};
use crate::{SoftwareVersion, UpgradePlan, UpgradeStep, corrupt, invalid, validate_plan};

const UPGRADE_PG: PlacementGroupId = PlacementGroupId(46);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UpgradeRollout {
    pub id: String,
    pub plan: UpgradePlan,
    pub next_step: usize,
    pub completed_steps: Vec<UpgradeStep>,
    pub failed: Option<UpgradeFailure>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UpgradeFailure {
    pub step: usize,
    pub reason: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum UpgradeRolloutDecision {
    Started(UpgradeRolloutStatus),
    Duplicate(UpgradeRolloutStatus),
    StepCompleted(UpgradeRolloutStatus),
    Completed(UpgradeRolloutStatus),
    Failed(UpgradeRolloutStatus),
    Rejected { id: String, reason: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UpgradeRolloutStatus {
    pub id: String,
    pub from: SoftwareVersion,
    pub to: SoftwareVersion,
    pub next_step: usize,
    pub total_steps: usize,
    pub completed_steps: usize,
    pub failed: Option<UpgradeFailure>,
}

impl UpgradeRollout {
    fn status(&self) -> UpgradeRolloutStatus {
        UpgradeRolloutStatus {
            id: self.id.clone(),
            from: self.plan.from.clone(),
            to: self.plan.to.clone(),
            next_step: self.next_step,
            total_steps: self.plan.steps.len(),
            completed_steps: self.completed_steps.len(),
            failed: self.failed.clone(),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct UpgradeRolloutTracker {
    rollouts: BTreeMap<String, UpgradeRollout>,
}

impl UpgradeRolloutTracker {
    pub fn start(
        &mut self,
        id: impl Into<String>,
        plan: UpgradePlan,
    ) -> RsfResult<UpgradeRolloutDecision> {
        let id = id.into();
        validate_rollout_id(&id)?;
        validate_plan(&plan)?;
        if let Some(existing) = self.rollouts.get(&id) {
            return Ok(UpgradeRolloutDecision::Duplicate(existing.status()));
        }
        let rollout = UpgradeRollout {
            id: id.clone(),
            plan,
            next_step: 0,
            completed_steps: Vec::new(),
            failed: None,
        };
        let status = rollout.status();
        self.rollouts.insert(id, rollout);
        Ok(UpgradeRolloutDecision::Started(status))
    }

    pub fn complete_next(
        &mut self,
        id: &str,
        expected: &UpgradeStep,
    ) -> RsfResult<UpgradeRolloutDecision> {
        let rollout = self
            .rollouts
            .get_mut(id)
            .ok_or_else(|| invalid(format!("upgrade rollout {id} is not active")))?;
        if rollout.failed.is_some() {
            return Ok(UpgradeRolloutDecision::Rejected {
                id: id.to_string(),
                reason: "failed rollout cannot complete more steps".to_string(),
            });
        }
        if rollout.next_step >= rollout.plan.steps.len() {
            return Ok(UpgradeRolloutDecision::Completed(rollout.status()));
        }
        let planned = rollout
            .plan
            .steps
            .get(rollout.next_step)
            .expect("next step checked");
        if planned != expected {
            return Err(invalid(format!(
                "upgrade rollout {id} expected step {:?} but got {:?}",
                planned, expected
            )));
        }
        rollout.completed_steps.push(expected.clone());
        rollout.next_step += 1;
        if rollout.next_step == rollout.plan.steps.len() {
            Ok(UpgradeRolloutDecision::Completed(rollout.status()))
        } else {
            Ok(UpgradeRolloutDecision::StepCompleted(rollout.status()))
        }
    }

    pub fn fail(
        &mut self,
        id: &str,
        reason: impl Into<String>,
    ) -> RsfResult<UpgradeRolloutDecision> {
        let reason = reason.into();
        if reason.trim().is_empty() {
            return Err(invalid("upgrade failure reason must be non-empty"));
        }
        let rollout = self
            .rollouts
            .get_mut(id)
            .ok_or_else(|| invalid(format!("upgrade rollout {id} is not active")))?;
        if rollout.next_step >= rollout.plan.steps.len() {
            return Ok(UpgradeRolloutDecision::Rejected {
                id: id.to_string(),
                reason: "completed rollout cannot fail".to_string(),
            });
        }
        rollout.failed = Some(UpgradeFailure {
            step: rollout.next_step,
            reason,
        });
        Ok(UpgradeRolloutDecision::Failed(rollout.status()))
    }

    pub fn status(&self, id: &str) -> RsfResult<UpgradeRolloutStatus> {
        self.rollouts
            .get(id)
            .map(UpgradeRollout::status)
            .ok_or_else(|| invalid(format!("upgrade rollout {id} is not active")))
    }
}

#[derive(Debug)]
pub struct DurableUpgradeRolloutTracker {
    tracker: UpgradeRolloutTracker,
    store: SegmentStore,
    epoch: Epoch,
    next_index: u64,
}

impl DurableUpgradeRolloutTracker {
    pub fn open(path: impl AsRef<Path>, epoch: Epoch) -> RsfResult<Self> {
        let store = SegmentStore::open(path)?;
        let scan = store.scan(SegmentScanFilter {
            pg: Some(UPGRADE_PG),
        })?;
        let mut tracker = UpgradeRolloutTracker::default();
        let mut next_index = 1;

        for record in scan
            .wal_records
            .iter()
            .filter(|record| record.kind == WalRecordKind::Upgrade)
        {
            apply_upgrade_record(&mut tracker, decode_upgrade_wal_payload(&record.payload)?)?;
            next_index = next_index.max(record.index.0.saturating_add(1));
        }

        Ok(Self {
            tracker,
            store,
            epoch,
            next_index,
        })
    }

    pub fn start(
        &mut self,
        id: impl Into<String>,
        plan: UpgradePlan,
    ) -> RsfResult<UpgradeRolloutDecision> {
        let id = id.into();
        let mut tracker = self.tracker.clone();
        let decision = tracker.start(id.clone(), plan.clone())?;
        if matches!(decision, UpgradeRolloutDecision::Started(_)) {
            self.append_record(&UpgradeWalRecord::Started { id, plan })?;
            self.tracker = tracker;
        }
        Ok(decision)
    }

    pub fn complete_next(
        &mut self,
        id: &str,
        expected: &UpgradeStep,
    ) -> RsfResult<UpgradeRolloutDecision> {
        let mut tracker = self.tracker.clone();
        let decision = tracker.complete_next(id, expected)?;
        if matches!(
            decision,
            UpgradeRolloutDecision::StepCompleted(_) | UpgradeRolloutDecision::Completed(_)
        ) {
            self.append_record(&UpgradeWalRecord::StepCompleted {
                id: id.to_string(),
                step: expected.clone(),
            })?;
            self.tracker = tracker;
        }
        Ok(decision)
    }

    pub fn fail(
        &mut self,
        id: &str,
        reason: impl Into<String>,
    ) -> RsfResult<UpgradeRolloutDecision> {
        let reason = reason.into();
        let mut tracker = self.tracker.clone();
        let decision = tracker.fail(id, reason.clone())?;
        if matches!(decision, UpgradeRolloutDecision::Failed(_)) {
            self.append_record(&UpgradeWalRecord::Failed {
                id: id.to_string(),
                reason,
            })?;
            self.tracker = tracker;
        }
        Ok(decision)
    }

    pub fn status(&self, id: &str) -> RsfResult<UpgradeRolloutStatus> {
        self.tracker.status(id)
    }

    pub fn tracker(&self) -> &UpgradeRolloutTracker {
        &self.tracker
    }

    fn append_record(&mut self, record: &UpgradeWalRecord) -> RsfResult<()> {
        let payload = encode_upgrade_wal_payload(record)?;
        let recovery = self.store.recover_current()?.recovery;
        let tail = recovery.pg_tail(UPGRADE_PG);
        let previous_hash = tail.map_or([0_u8; 32], |tail| tail.record_hash);
        let index = LogIndex(self.next_index);
        let (_, _, record) = self.store.append_wal(WalAppend {
            pg: UPGRADE_PG,
            epoch: self.epoch,
            index,
            kind: WalRecordKind::Upgrade,
            previous_hash,
            payload: &payload,
        })?;
        self.next_index = record.index.0.saturating_add(1);
        Ok(())
    }
}

fn apply_upgrade_record(
    tracker: &mut UpgradeRolloutTracker,
    record: UpgradeWalRecord,
) -> RsfResult<()> {
    match record {
        UpgradeWalRecord::Started { id, plan } => match tracker.start(id, plan)? {
            UpgradeRolloutDecision::Started(_) => Ok(()),
            other => Err(corrupt(format!(
                "upgrade WAL start replay produced unexpected decision {other:?}"
            ))),
        },
        UpgradeWalRecord::StepCompleted { id, step } => match tracker.complete_next(&id, &step)? {
            UpgradeRolloutDecision::StepCompleted(_) | UpgradeRolloutDecision::Completed(_) => {
                Ok(())
            }
            other => Err(corrupt(format!(
                "upgrade WAL step replay produced unexpected decision {other:?}"
            ))),
        },
        UpgradeWalRecord::Failed { id, reason } => match tracker.fail(&id, reason)? {
            UpgradeRolloutDecision::Failed(_) => Ok(()),
            other => Err(corrupt(format!(
                "upgrade WAL failure replay produced unexpected decision {other:?}"
            ))),
        },
    }
}

fn validate_rollout_id(id: &str) -> RsfResult<()> {
    if id.is_empty()
        || !id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        return Err(invalid(
            "upgrade rollout id must be non-empty ASCII alphanumeric, '_' or '-'",
        ));
    }
    Ok(())
}
