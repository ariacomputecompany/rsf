#![forbid(unsafe_code)]

mod ledger;

pub use ledger::{
    DurableRepairLedger, RepairCompletion, RepairOperation, RepairOperationId, RepairOperationState,
};

use rsf_core::{
    Digest, ErrorCode, NodeId, PlacementGroupId, RsfError, RsfResult,
    placement::{PlacementGroup, PlacementPolicy},
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaState {
    pub node: NodeId,
    pub present: bool,
    pub digest_ok: bool,
    pub lag_bytes: u64,
    pub durable_bytes: u64,
    pub checksum: Digest,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaScrub {
    pub pg: PlacementGroupId,
    pub expected_replicas: usize,
    pub scanned_replicas: usize,
    pub present_replicas: usize,
    pub verified_replicas: usize,
    pub missing_replicas: usize,
    pub corrupt_replicas: usize,
    pub slow_replicas: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RepairFindingKind {
    MissingReplica,
    CorruptReplica,
    SlowReplica,
    UnderReplicated,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RepairFinding {
    pub pg: PlacementGroupId,
    pub node: Option<NodeId>,
    pub kind: RepairFindingKind,
    pub detail: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RepairAction {
    CopyReplica {
        pg: PlacementGroupId,
        from: NodeId,
        to: NodeId,
    },
    QuarantineReplica {
        pg: PlacementGroupId,
        node: NodeId,
    },
    RecheckDigest {
        pg: PlacementGroupId,
        node: NodeId,
    },
    DemoteSlowReplica {
        pg: PlacementGroupId,
        node: NodeId,
    },
    RestoreReplication {
        pg: PlacementGroupId,
        healthy: usize,
        expected: usize,
        deficit: usize,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaCopyResult {
    pub pg: PlacementGroupId,
    pub from: NodeId,
    pub to: NodeId,
    pub bytes: u64,
    pub checksum: Digest,
    pub restored: ReplicaState,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaQuarantineResult {
    pub pg: PlacementGroupId,
    pub node: NodeId,
    pub bytes: u64,
    pub checksum: Digest,
    pub quarantined: ReplicaState,
    pub reason: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UnderReplicationRepairResult {
    pub pg: PlacementGroupId,
    pub healthy_before: usize,
    pub expected: usize,
    pub deficit: usize,
    pub copy_repairs: usize,
    pub quarantine_repairs: usize,
    pub demote_repairs: usize,
    pub projected_healthy_replicas: usize,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RepairReport {
    pub findings: Vec<RepairFinding>,
    pub actions: Vec<RepairAction>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RepairPlanner {
    slow_lag_bytes: u64,
}

impl Default for RepairPlanner {
    fn default() -> Self {
        Self {
            slow_lag_bytes: 1024 * 1024,
        }
    }
}

impl RepairPlanner {
    pub fn new(slow_lag_bytes: u64) -> Self {
        Self { slow_lag_bytes }
    }

    pub fn scrub(
        &self,
        group: &PlacementGroup,
        states: &[ReplicaState],
    ) -> RsfResult<ReplicaScrub> {
        group.acting_set.validate()?;
        validate_scrub_inventory(group, states)?;

        let expected_replicas = group.acting_set.replicas.len();
        let scanned_replicas = states.len();
        let present_replicas = states.iter().filter(|state| state.present).count();
        let verified_replicas = states
            .iter()
            .filter(|state| {
                group.acting_set.replicas.contains(&state.node)
                    && state.present
                    && state.digest_ok
                    && state.lag_bytes <= self.slow_lag_bytes
            })
            .count();
        let corrupt_replicas = states
            .iter()
            .filter(|state| state.present && !state.digest_ok)
            .count();
        let slow_replicas = states
            .iter()
            .filter(|state| {
                state.present && state.digest_ok && state.lag_bytes > self.slow_lag_bytes
            })
            .count();
        let missing_replicas = expected_replicas.saturating_sub(present_replicas);

        Ok(ReplicaScrub {
            pg: group.id,
            expected_replicas,
            scanned_replicas,
            present_replicas,
            verified_replicas,
            missing_replicas,
            corrupt_replicas,
            slow_replicas,
        })
    }

    pub fn plan(&self, group: &PlacementGroup, states: &[ReplicaState]) -> RsfResult<RepairReport> {
        let scrub = self.scrub(group, states)?;
        let source = self.healthy_source(group.id, states)?;
        let mut report = RepairReport::default();

        for replica in &group.acting_set.replicas {
            match states.iter().find(|state| state.node == *replica) {
                None => self.record_missing(&mut report, group.id, source, *replica),
                Some(state) if !state.present => {
                    self.record_missing(&mut report, group.id, source, *replica);
                }
                Some(state) if !state.digest_ok => {
                    report.findings.push(RepairFinding {
                        pg: group.id,
                        node: Some(*replica),
                        kind: RepairFindingKind::CorruptReplica,
                        detail: format!("replica {} failed scrub digest verification", replica.0),
                    });
                    report.actions.push(RepairAction::RecheckDigest {
                        pg: group.id,
                        node: *replica,
                    });
                    report.actions.push(RepairAction::QuarantineReplica {
                        pg: group.id,
                        node: *replica,
                    });
                    report.actions.push(RepairAction::CopyReplica {
                        pg: group.id,
                        from: source,
                        to: *replica,
                    });
                }
                Some(state) if state.lag_bytes > self.slow_lag_bytes => {
                    report.findings.push(RepairFinding {
                        pg: group.id,
                        node: Some(*replica),
                        kind: RepairFindingKind::SlowReplica,
                        detail: format!(
                            "replica {} is {} bytes behind scrub watermark",
                            replica.0, state.lag_bytes
                        ),
                    });
                    report.actions.push(RepairAction::DemoteSlowReplica {
                        pg: group.id,
                        node: *replica,
                    });
                    report.actions.push(RepairAction::CopyReplica {
                        pg: group.id,
                        from: source,
                        to: *replica,
                    });
                }
                Some(_) => {}
            }
        }

        let healthy_count = scrub.verified_replicas;
        let expected = expected_replicas(&group.policy);
        if healthy_count < expected {
            let deficit = expected - healthy_count;
            report.findings.push(RepairFinding {
                pg: group.id,
                node: None,
                kind: RepairFindingKind::UnderReplicated,
                detail: format!(
                    "pg {} has {healthy_count}/{expected} healthy replicas",
                    group.id.0
                ),
            });
            report.actions.push(RepairAction::RestoreReplication {
                pg: group.id,
                healthy: healthy_count,
                expected,
                deficit,
            });
        }

        Ok(report)
    }

    pub fn execute_copy(
        &self,
        group: &PlacementGroup,
        states: &[ReplicaState],
        action: &RepairAction,
    ) -> RsfResult<ReplicaCopyResult> {
        self.scrub(group, states)?;
        let RepairAction::CopyReplica { pg, from, to } = *action else {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                "repair copy executor requires CopyReplica action",
            ));
        };
        if pg != group.id {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!(
                    "repair copy action pg {} does not match placement group {}",
                    pg.0, group.id.0
                ),
            ));
        }
        if !group.acting_set.replicas.contains(&from) {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!("repair copy source node {} is not in acting set", from.0),
            ));
        }
        if !group.acting_set.replicas.contains(&to) {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!("repair copy target node {} is not in acting set", to.0),
            ));
        }

        let source = states
            .iter()
            .find(|state| state.node == from)
            .ok_or_else(|| {
                RsfError::new(
                    ErrorCode::InvalidInput,
                    format!("repair copy source node {} is missing", from.0),
                )
            })?;
        if !source.present || !source.digest_ok || source.lag_bytes > self.slow_lag_bytes {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!("repair copy source node {} is not healthy", from.0),
            ));
        }
        if source.durable_bytes == 0 {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!("repair copy source node {} has no durable bytes", from.0),
            ));
        }
        source.checksum.validate()?;

        if let Some(target) = states.iter().find(|state| state.node == to) {
            if target.present && target.digest_ok && target.lag_bytes <= self.slow_lag_bytes {
                return Err(RsfError::new(
                    ErrorCode::InvalidInput,
                    format!("repair copy target node {} is already healthy", to.0),
                ));
            }
        }

        let restored = ReplicaState {
            node: to,
            present: true,
            digest_ok: true,
            lag_bytes: 0,
            durable_bytes: source.durable_bytes,
            checksum: source.checksum,
        };

        Ok(ReplicaCopyResult {
            pg,
            from,
            to,
            bytes: restored.durable_bytes,
            checksum: restored.checksum,
            restored,
        })
    }

    pub fn execute_quarantine(
        &self,
        group: &PlacementGroup,
        states: &[ReplicaState],
        action: &RepairAction,
    ) -> RsfResult<ReplicaQuarantineResult> {
        self.scrub(group, states)?;
        let RepairAction::QuarantineReplica { pg, node } = *action else {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                "repair quarantine executor requires QuarantineReplica action",
            ));
        };
        if pg != group.id {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!(
                    "repair quarantine action pg {} does not match placement group {}",
                    pg.0, group.id.0
                ),
            ));
        }
        if !group.acting_set.replicas.contains(&node) {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!(
                    "repair quarantine target node {} is not in acting set",
                    node.0
                ),
            ));
        }
        let corrupt = states
            .iter()
            .find(|state| state.node == node)
            .ok_or_else(|| {
                RsfError::new(
                    ErrorCode::InvalidInput,
                    format!("repair quarantine target node {} is missing", node.0),
                )
            })?;
        if !corrupt.present {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!("repair quarantine target node {} is not present", node.0),
            ));
        }
        if corrupt.digest_ok {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!("repair quarantine target node {} is not corrupt", node.0),
            ));
        }
        corrupt.checksum.validate()?;

        let quarantined = ReplicaState {
            node,
            present: false,
            digest_ok: false,
            lag_bytes: 0,
            durable_bytes: 0,
            checksum: corrupt.checksum,
        };
        let reason = format!("replica {} failed scrub digest verification", node.0);

        Ok(ReplicaQuarantineResult {
            pg,
            node,
            bytes: corrupt.durable_bytes,
            checksum: corrupt.checksum,
            quarantined,
            reason,
        })
    }

    pub fn execute_under_replication_repair(
        &self,
        group: &PlacementGroup,
        states: &[ReplicaState],
        action: &RepairAction,
        report: &RepairReport,
    ) -> RsfResult<UnderReplicationRepairResult> {
        let scrub = self.scrub(group, states)?;
        let RepairAction::RestoreReplication {
            pg,
            healthy,
            expected,
            deficit,
        } = *action
        else {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                "under-replication repair executor requires RestoreReplication action",
            ));
        };
        if pg != group.id {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!(
                    "under-replication action pg {} does not match placement group {}",
                    pg.0, group.id.0
                ),
            ));
        }
        if healthy != scrub.verified_replicas {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!(
                    "under-replication action healthy count {} does not match scrub count {}",
                    healthy, scrub.verified_replicas
                ),
            ));
        }
        let required = expected_replicas(&group.policy);
        if expected != required {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!(
                    "under-replication action expected {} does not match placement policy {}",
                    expected, required
                ),
            ));
        }
        if healthy >= expected || deficit == 0 {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                "under-replication repair requires a real replica deficit",
            ));
        }
        if expected - healthy != deficit {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!(
                    "under-replication deficit {} does not match {healthy}/{expected}",
                    deficit
                ),
            ));
        }

        let mut repaired_targets = Vec::new();
        let mut copy_repairs = 0;
        let mut quarantine_repairs = 0;
        let mut demote_repairs = 0;
        for planned in &report.actions {
            match *planned {
                RepairAction::CopyReplica {
                    pg: action_pg, to, ..
                } if action_pg == group.id => {
                    if !repaired_targets.contains(&to) {
                        self.execute_copy(group, states, planned)?;
                        repaired_targets.push(to);
                        copy_repairs += 1;
                    }
                }
                RepairAction::QuarantineReplica { pg: action_pg, .. } if action_pg == group.id => {
                    self.execute_quarantine(group, states, planned)?;
                    quarantine_repairs += 1;
                }
                RepairAction::DemoteSlowReplica {
                    pg: action_pg,
                    node,
                } if action_pg == group.id => {
                    if !group.acting_set.replicas.contains(&node) {
                        return Err(RsfError::new(
                            ErrorCode::InvalidInput,
                            format!("demote action target node {} is not in acting set", node.0),
                        ));
                    }
                    demote_repairs += 1;
                }
                _ => {}
            }
        }

        let projected_healthy_replicas = (healthy + copy_repairs).min(expected);
        if projected_healthy_replicas < expected {
            return Err(RsfError::new(
                ErrorCode::QuorumUnavailable,
                format!(
                    "under-replication repair only projects {projected_healthy_replicas}/{expected} healthy replicas"
                ),
            ));
        }

        Ok(UnderReplicationRepairResult {
            pg,
            healthy_before: healthy,
            expected,
            deficit,
            copy_repairs,
            quarantine_repairs,
            demote_repairs,
            projected_healthy_replicas,
        })
    }

    fn healthy_source(&self, pg: PlacementGroupId, states: &[ReplicaState]) -> RsfResult<NodeId> {
        states
            .iter()
            .find(|state| {
                state.present && state.digest_ok && state.lag_bytes <= self.slow_lag_bytes
            })
            .map(|state| state.node)
            .ok_or_else(|| {
                RsfError::new(
                    ErrorCode::QuorumUnavailable,
                    format!("pg {} has no healthy repair source", pg.0),
                )
            })
    }

    fn record_missing(
        &self,
        report: &mut RepairReport,
        pg: PlacementGroupId,
        source: NodeId,
        target: NodeId,
    ) {
        report.findings.push(RepairFinding {
            pg,
            node: Some(target),
            kind: RepairFindingKind::MissingReplica,
            detail: format!("replica {} is absent from scrub inventory", target.0),
        });
        report.actions.push(RepairAction::CopyReplica {
            pg,
            from: source,
            to: target,
        });
    }
}

fn validate_scrub_inventory(group: &PlacementGroup, states: &[ReplicaState]) -> RsfResult<()> {
    let mut seen = Vec::new();
    for state in states {
        if !group.acting_set.replicas.contains(&state.node) {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!(
                    "scrub inventory for pg {} includes non-acting replica node {}",
                    group.id.0, state.node.0
                ),
            ));
        }
        if seen.contains(&state.node) {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!(
                    "scrub inventory for pg {} duplicates replica node {}",
                    group.id.0, state.node.0
                ),
            ));
        }
        seen.push(state.node);
    }
    Ok(())
}

fn expected_replicas(policy: &PlacementPolicy) -> usize {
    match policy {
        PlacementPolicy::Replicated { replicas } => usize::from(*replicas),
        PlacementPolicy::ErasureCoded { data, parity } => usize::from(*data + *parity),
    }
}

#[cfg(test)]
mod tests;
