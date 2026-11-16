#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use rsf_core::{ErrorCode, NodeId, RegionId, RsfError, RsfResult};

mod codec;
mod rollout;

pub use rollout::{
    DurableUpgradeRolloutTracker, UpgradeFailure, UpgradeRollout, UpgradeRolloutDecision,
    UpgradeRolloutStatus, UpgradeRolloutTracker,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SoftwareVersion {
    pub major: u16,
    pub minor: u16,
    pub patch: u16,
}

impl SoftwareVersion {
    pub fn new(major: u16, minor: u16, patch: u16) -> Self {
        Self {
            major,
            minor,
            patch,
        }
    }

    pub fn is_wire_compatible_with(&self, other: &Self) -> bool {
        self.major == other.major && self.minor.abs_diff(other.minor) <= 1
    }

    pub fn is_older_than(&self, other: &Self) -> bool {
        (self.major, self.minor, self.patch) < (other.major, other.minor, other.patch)
    }
}

impl fmt::Display for SoftwareVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

impl FromStr for SoftwareVersion {
    type Err = RsfError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let mut parts = value.split('.');
        let major = parse_version_part(parts.next(), "major")?;
        let minor = parse_version_part(parts.next(), "minor")?;
        let patch = parse_version_part(parts.next(), "patch")?;
        if parts.next().is_some() {
            return Err(invalid("software version must be major.minor.patch"));
        }
        Ok(Self::new(major, minor, patch))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeVersion {
    pub node: NodeId,
    pub version: SoftwareVersion,
    pub region: RegionId,
    pub can_drain: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UpgradePolicy {
    pub min_compatible_nodes: usize,
    pub max_unavailable: usize,
    pub require_same_major: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UpgradeGateCheck {
    pub name: String,
    pub passed: bool,
    pub detail: String,
}

impl From<&UpgradeGateCheck> for String {
    fn from(check: &UpgradeGateCheck) -> Self {
        format!(
            "{}={} {}",
            check.name,
            if check.passed { "pass" } else { "fail" },
            check.detail
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UpgradeStepKind {
    Preflight,
    Drain,
    Install,
    Probe,
    Promote,
}

impl UpgradeStepKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Preflight => "preflight",
            Self::Drain => "drain",
            Self::Install => "install",
            Self::Probe => "probe",
            Self::Promote => "promote",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UpgradeStep {
    pub kind: UpgradeStepKind,
    pub node: Option<NodeId>,
    pub detail: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UpgradePlan {
    pub from: SoftwareVersion,
    pub to: SoftwareVersion,
    pub steps: Vec<UpgradeStep>,
    pub compatibility_checks: Vec<String>,
    pub gate_checks: Vec<UpgradeGateCheck>,
}

pub(crate) fn validate_plan(plan: &UpgradePlan) -> RsfResult<()> {
    if plan.steps.is_empty() {
        return Err(invalid("upgrade rollout plan must include steps"));
    }
    if plan.from == plan.to || !plan.from.is_older_than(&plan.to) {
        return Err(invalid("upgrade rollout target must be newer than source"));
    }
    if !matches!(
        plan.steps.first().map(|step| step.kind),
        Some(UpgradeStepKind::Preflight)
    ) {
        return Err(invalid("upgrade rollout must start with preflight"));
    }
    if plan.gate_checks.is_empty() || !plan.gate_checks.iter().all(|check| check.passed) {
        return Err(invalid(
            "upgrade rollout plan must carry passing gate checks",
        ));
    }
    for step in &plan.steps {
        validate_step(step)?;
    }
    Ok(())
}

pub(crate) fn validate_step(step: &UpgradeStep) -> RsfResult<()> {
    if step.detail.trim().is_empty() {
        return Err(invalid("upgrade step detail must not be empty"));
    }
    if step.kind == UpgradeStepKind::Preflight && step.node.is_some() {
        return Err(invalid("upgrade preflight step must not target one node"));
    }
    if step.kind != UpgradeStepKind::Preflight && step.node.is_none() {
        return Err(invalid("upgrade node step must target one node"));
    }
    Ok(())
}

#[derive(Clone, Debug, Default)]
pub struct UpgradePlanner;

impl UpgradePlanner {
    pub fn plan(
        &self,
        nodes: &[NodeVersion],
        target: SoftwareVersion,
        policy: &UpgradePolicy,
    ) -> RsfResult<UpgradePlan> {
        let checks = evaluate_gates(nodes, &target, policy)?;
        let current = oldest_version(nodes)?;
        let compatible = nodes
            .iter()
            .filter(|node| node.version.is_wire_compatible_with(&target))
            .count();

        let mut steps = vec![UpgradeStep {
            kind: UpgradeStepKind::Preflight,
            node: None,
            detail: format!("{} nodes compatible with target {}", compatible, target),
        }];

        for node in rollout_order(nodes) {
            steps.push(UpgradeStep {
                kind: UpgradeStepKind::Drain,
                node: Some(node.node),
                detail: format!("drain node {} in {}", node.node.0, node.region),
            });
            steps.push(UpgradeStep {
                kind: UpgradeStepKind::Install,
                node: Some(node.node),
                detail: format!("install {}", target),
            });
            steps.push(UpgradeStep {
                kind: UpgradeStepKind::Probe,
                node: Some(node.node),
                detail: "run readiness, storage, and wire-compat probes".to_string(),
            });
            steps.push(UpgradeStep {
                kind: UpgradeStepKind::Promote,
                node: Some(node.node),
                detail: "return node to placement and gateway eligibility".to_string(),
            });
        }

        Ok(UpgradePlan {
            from: current,
            to: target,
            steps,
            compatibility_checks: vec![
                format!("same_major={}", policy.require_same_major),
                format!("max_unavailable={}", policy.max_unavailable),
                format!("min_compatible_nodes={}", policy.min_compatible_nodes),
            ],
            gate_checks: checks,
        })
    }
}

pub fn evaluate_gates(
    nodes: &[NodeVersion],
    target: &SoftwareVersion,
    policy: &UpgradePolicy,
) -> RsfResult<Vec<UpgradeGateCheck>> {
    let mut checks = Vec::new();
    if nodes.is_empty() {
        return Err(invalid("upgrade plan needs at least one node"));
    }
    let unique_nodes = nodes
        .iter()
        .map(|node| node.node)
        .collect::<std::collections::BTreeSet<_>>();
    checks.push(UpgradeGateCheck {
        name: "node-identity".to_string(),
        passed: unique_nodes.len() == nodes.len(),
        detail: format!("nodes={} unique_nodes={}", nodes.len(), unique_nodes.len()),
    });
    if policy.min_compatible_nodes == 0 || policy.min_compatible_nodes > nodes.len() {
        return Err(invalid("compatible node floor must fit cluster size"));
    }
    checks.push(UpgradeGateCheck {
        name: "availability-budget".to_string(),
        passed: policy.max_unavailable == 1 && nodes.len() > policy.max_unavailable,
        detail: format!(
            "max_unavailable={} nodes={}",
            policy.max_unavailable,
            nodes.len()
        ),
    });
    checks.push(UpgradeGateCheck {
        name: "drainability".to_string(),
        passed: nodes.iter().all(|node| node.can_drain),
        detail: format!(
            "drainable_nodes={} nodes={}",
            nodes.iter().filter(|node| node.can_drain).count(),
            nodes.len()
        ),
    });
    checks.push(UpgradeGateCheck {
        name: "same-major".to_string(),
        passed: !policy.require_same_major
            || nodes.iter().all(|node| node.version.major == target.major),
        detail: format!(
            "target={} require_same_major={}",
            target, policy.require_same_major
        ),
    });
    checks.push(UpgradeGateCheck {
        name: "forward-only".to_string(),
        passed: nodes.iter().all(|node| node.version.is_older_than(target)),
        detail: format!(
            "oldest={} target={}",
            oldest_version(nodes).unwrap_or_else(|_| target.clone()),
            target
        ),
    });
    let compatible = nodes
        .iter()
        .filter(|node| node.version.is_wire_compatible_with(target))
        .count();
    checks.push(UpgradeGateCheck {
        name: "wire-compatibility".to_string(),
        passed: compatible >= policy.min_compatible_nodes,
        detail: format!(
            "compatible_nodes={} min_compatible_nodes={}",
            compatible, policy.min_compatible_nodes
        ),
    });
    checks.extend(region_checks(nodes));
    checks.extend(rollout_checks(nodes, target, policy));

    if let Some(failed) = checks.iter().find(|check| !check.passed) {
        return Err(invalid(format!(
            "upgrade gate {} failed: {}",
            failed.name, failed.detail
        )));
    }
    Ok(checks)
}

fn region_checks(nodes: &[NodeVersion]) -> Vec<UpgradeGateCheck> {
    let mut counts = BTreeMap::<RegionId, usize>::new();
    for node in nodes {
        *counts.entry(node.region.clone()).or_default() += 1;
    }
    counts
        .into_iter()
        .map(|(region, count)| UpgradeGateCheck {
            name: format!("region-coverage:{region}"),
            passed: count >= 1,
            detail: format!("region={region} nodes={count}"),
        })
        .collect()
}

fn rollout_checks(
    nodes: &[NodeVersion],
    target: &SoftwareVersion,
    policy: &UpgradePolicy,
) -> Vec<UpgradeGateCheck> {
    rollout_order(nodes)
        .iter()
        .map(|draining| {
            let remaining = nodes
                .iter()
                .filter(|node| node.node != draining.node)
                .collect::<Vec<_>>();
            let compatible_remaining = remaining
                .iter()
                .filter(|node| node.version.is_wire_compatible_with(target))
                .count();
            let keeps_region_online = remaining
                .iter()
                .any(|node| node.region == draining.region);
            UpgradeGateCheck {
                name: format!("rollout-node:{}", draining.node.0),
                passed: compatible_remaining >= policy.min_compatible_nodes,
                detail: format!(
                    "drain_node={} compatible_remaining={} min_compatible_nodes={} region={} region_online_after_drain={}",
                    draining.node.0,
                    compatible_remaining,
                    policy.min_compatible_nodes,
                    draining.region,
                    keeps_region_online
                ),
            }
        })
        .collect()
}

fn rollout_order(nodes: &[NodeVersion]) -> Vec<&NodeVersion> {
    let mut ordered = nodes.iter().collect::<Vec<_>>();
    ordered.sort_by_key(|node| (node.region.clone(), node.node));
    ordered
}

fn oldest_version(nodes: &[NodeVersion]) -> RsfResult<SoftwareVersion> {
    nodes
        .iter()
        .map(|node| node.version.clone())
        .min_by_key(|version| (version.major, version.minor, version.patch))
        .ok_or_else(|| invalid("upgrade plan needs at least one node"))
}

fn parse_version_part(value: Option<&str>, label: &str) -> RsfResult<u16> {
    let value = value.ok_or_else(|| invalid("software version must be major.minor.patch"))?;
    if value.is_empty() {
        return Err(invalid(format!("software version {label} is empty")));
    }
    value
        .parse::<u16>()
        .map_err(|_| invalid(format!("software version {label} must be an integer")))
}

fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message)
}

fn corrupt(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::CorruptionDetected, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::{UpgradeWalRecord, decode_upgrade_wal_payload, encode_upgrade_wal_payload};
    use rsf_core::Epoch;

    #[test]
    fn plans_one_node_at_a_time_upgrade() {
        let plan = UpgradePlanner
            .plan(
                &nodes(3, SoftwareVersion::new(1, 2, 0)),
                SoftwareVersion::new(1, 3, 0),
                &policy(2),
            )
            .unwrap();

        assert_eq!(plan.from, SoftwareVersion::new(1, 2, 0));
        assert_eq!(plan.to, SoftwareVersion::new(1, 3, 0));
        assert_eq!(plan.steps.len(), 13);
        assert_eq!(plan.steps[0].kind, UpgradeStepKind::Preflight);
        assert_eq!(plan.steps[1].kind, UpgradeStepKind::Drain);
        assert_eq!(plan.steps[4].kind, UpgradeStepKind::Promote);
        assert_eq!(plan.compatibility_checks.len(), 3);
        assert!(plan.gate_checks.len() >= 6);
        assert!(plan.gate_checks.iter().all(|check| check.passed));
    }

    #[test]
    fn rejects_cross_major_upgrade_when_policy_requires_it() {
        let err = UpgradePlanner
            .plan(
                &nodes(3, SoftwareVersion::new(1, 2, 0)),
                SoftwareVersion::new(2, 0, 0),
                &policy(2),
            )
            .unwrap_err();

        assert_eq!(err.code, ErrorCode::InvalidInput);
    }

    #[test]
    fn rejects_undrainable_nodes() {
        let mut cluster = nodes(3, SoftwareVersion::new(1, 2, 0));
        cluster[1].can_drain = false;
        let err = UpgradePlanner
            .plan(&cluster, SoftwareVersion::new(1, 3, 0), &policy(2))
            .unwrap_err();

        assert_eq!(err.code, ErrorCode::InvalidInput);
    }

    #[test]
    fn rejects_rollout_that_would_drop_below_compatible_floor() {
        let err = UpgradePlanner
            .plan(
                &[
                    NodeVersion {
                        node: NodeId(0),
                        version: SoftwareVersion::new(1, 2, 0),
                        region: RegionId::new("iad").expect("region"),
                        can_drain: true,
                    },
                    NodeVersion {
                        node: NodeId(1),
                        version: SoftwareVersion::new(1, 2, 0),
                        region: RegionId::new("ord").expect("region"),
                        can_drain: true,
                    },
                ],
                SoftwareVersion::new(1, 3, 0),
                &policy(2),
            )
            .unwrap_err();

        assert_eq!(err.code, ErrorCode::InvalidInput);
    }

    #[test]
    fn parses_semver_and_rejects_downgrades() {
        let parsed = "1.3.5".parse::<SoftwareVersion>().expect("version");
        assert_eq!(parsed.to_string(), "1.3.5");

        let err = UpgradePlanner
            .plan(
                &nodes(3, SoftwareVersion::new(1, 3, 0)),
                SoftwareVersion::new(1, 2, 9),
                &policy(2),
            )
            .unwrap_err();

        assert_eq!(err.code, ErrorCode::InvalidInput);
    }

    #[test]
    fn durable_upgrade_recovers_completed_steps() {
        let path = test_path("durable_upgrade_recovers_completed_steps");
        let plan = plan();
        let first = plan.steps[0].clone();
        let second = plan.steps[1].clone();

        {
            let mut rollouts =
                DurableUpgradeRolloutTracker::open(&path, Epoch(3)).expect("open rollouts");
            assert!(matches!(
                rollouts.start("rollout-1", plan.clone()).expect("start"),
                UpgradeRolloutDecision::Started(_)
            ));
            assert!(matches!(
                rollouts
                    .complete_next("rollout-1", &first)
                    .expect("complete preflight"),
                UpgradeRolloutDecision::StepCompleted(_)
            ));
            assert!(matches!(
                rollouts
                    .complete_next("rollout-1", &second)
                    .expect("complete drain"),
                UpgradeRolloutDecision::StepCompleted(_)
            ));
        }

        let mut recovered =
            DurableUpgradeRolloutTracker::open(&path, Epoch(3)).expect("recover rollouts");
        let status = recovered.status("rollout-1").expect("status");
        assert_eq!(status.next_step, 2);
        assert_eq!(status.completed_steps, 2);
        assert_eq!(status.total_steps, plan.steps.len());

        assert!(matches!(
            recovered
                .complete_next("rollout-1", &plan.steps[2])
                .expect("continue after restart"),
            UpgradeRolloutDecision::StepCompleted(_)
        ));

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn durable_upgrade_recovers_failed_rollout_terminality() {
        let path = test_path("durable_upgrade_recovers_failed_rollout_terminality");
        let plan = plan();

        {
            let mut rollouts =
                DurableUpgradeRolloutTracker::open(&path, Epoch(4)).expect("open rollouts");
            rollouts.start("rollout-2", plan.clone()).expect("start");
            rollouts
                .complete_next("rollout-2", &plan.steps[0])
                .expect("preflight");
            assert!(matches!(
                rollouts
                    .fail("rollout-2", "probe failed on node 0")
                    .expect("fail"),
                UpgradeRolloutDecision::Failed(_)
            ));
        }

        let mut recovered =
            DurableUpgradeRolloutTracker::open(&path, Epoch(4)).expect("recover rollouts");
        let status = recovered.status("rollout-2").expect("status");
        assert_eq!(
            status.failed,
            Some(UpgradeFailure {
                step: 1,
                reason: "probe failed on node 0".to_string()
            })
        );
        assert!(matches!(
            recovered
                .complete_next("rollout-2", &plan.steps[1])
                .expect("complete after failure"),
            UpgradeRolloutDecision::Rejected { .. }
        ));

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn upgrade_rollout_rejects_wrong_next_step() {
        let plan = plan();
        let mut rollouts = UpgradeRolloutTracker::default();
        rollouts.start("rollout-3", plan.clone()).expect("start");

        let err = rollouts
            .complete_next("rollout-3", &plan.steps[1])
            .expect_err("wrong step rejected");

        assert_eq!(err.code, ErrorCode::InvalidInput);
    }

    #[test]
    fn upgrade_wal_decode_rejects_trailing_bytes() {
        let mut payload = encode_upgrade_wal_payload(&UpgradeWalRecord::Started {
            id: "rollout-4".to_string(),
            plan: plan(),
        })
        .expect("encode");
        payload.push(99);

        let err = decode_upgrade_wal_payload(&payload).expect_err("trailing bytes reject");

        assert_eq!(err.code, ErrorCode::CorruptionDetected);
    }

    fn nodes(count: u64, version: SoftwareVersion) -> Vec<NodeVersion> {
        (0..count)
            .map(|node| NodeVersion {
                node: NodeId(node),
                version: version.clone(),
                region: RegionId::new(if node % 2 == 0 { "iad" } else { "ord" }).expect("region"),
                can_drain: true,
            })
            .collect()
    }

    fn policy(min_compatible_nodes: usize) -> UpgradePolicy {
        UpgradePolicy {
            min_compatible_nodes,
            max_unavailable: 1,
            require_same_major: true,
        }
    }

    fn plan() -> UpgradePlan {
        UpgradePlanner
            .plan(
                &nodes(3, SoftwareVersion::new(1, 2, 0)),
                SoftwareVersion::new(1, 3, 0),
                &policy(2),
            )
            .expect("plan")
    }

    fn test_path(name: &str) -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "rsf-upgrade-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        path
    }
}
