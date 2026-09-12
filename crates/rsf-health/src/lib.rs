#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::path::Path;

use rsf_core::{
    Epoch, ErrorCode, LogIndex, NodeId, PlacementGroupId, RsfError, RsfResult, cluster::ClusterMap,
    storage::WalRecordKind,
};
use rsf_storage::{SegmentScanFilter, SegmentStore, WalAppend};

const HEALTH_PG: PlacementGroupId = PlacementGroupId(50);
const HEALTH_WAL_MAGIC: &[u8; 7] = b"RSFHLT1";
const HEALTH_WAL_VERSION: u8 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeObservation {
    pub node: NodeId,
    pub disk_free_bps: u16,
    pub p99_latency_ms: u16,
    pub replica_lag_bytes: u64,
    pub missed_heartbeats: u8,
    pub corruption_events: u8,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HealthPolicy {
    pub min_disk_free_bps: u16,
    pub max_p99_latency_ms: u16,
    pub max_replica_lag_bytes: u64,
    pub max_missed_heartbeats: u8,
    pub demote_below_score: u16,
    pub promote_at_score: u16,
}

impl Default for HealthPolicy {
    fn default() -> Self {
        Self {
            min_disk_free_bps: 1_500,
            max_p99_latency_ms: 40,
            max_replica_lag_bytes: 1_048_576,
            max_missed_heartbeats: 2,
            demote_below_score: 65,
            promote_at_score: 85,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HealthClass {
    Healthy,
    Degraded,
    Demote,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeHealth {
    pub node: NodeId,
    pub score: u16,
    pub class: HealthClass,
    pub reasons: Vec<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HealthAction {
    PromoteLearner { node: NodeId },
    DemoteSlowReplica { node: NodeId },
    QuarantineCorruptNode { node: NodeId },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HealthReport {
    pub nodes: Vec<NodeHealth>,
    pub actions: Vec<HealthAction>,
}

#[derive(Clone, Debug, Default)]
pub struct HealthScorer;

impl HealthScorer {
    pub fn evaluate(
        &self,
        cluster: &ClusterMap,
        observations: &[NodeObservation],
        policy: &HealthPolicy,
    ) -> RsfResult<HealthReport> {
        validate_policy(policy)?;
        let mut nodes = Vec::new();
        let mut actions = Vec::new();

        for descriptor in &cluster.nodes {
            let observation = observations
                .iter()
                .find(|candidate| candidate.node == descriptor.id)
                .ok_or_else(|| {
                    invalid(format!(
                        "missing health observation for node {}",
                        descriptor.id.0
                    ))
                })?;
            let health = score_node(observation, policy);
            match health.class {
                HealthClass::Healthy if health.score >= policy.promote_at_score => {
                    actions.push(HealthAction::PromoteLearner { node: health.node });
                }
                HealthClass::Demote => {
                    if observation.corruption_events > 0 {
                        actions.push(HealthAction::QuarantineCorruptNode { node: health.node });
                    } else {
                        actions.push(HealthAction::DemoteSlowReplica { node: health.node });
                    }
                }
                HealthClass::Healthy | HealthClass::Degraded => {}
            }
            nodes.push(health);
        }

        Ok(HealthReport { nodes, actions })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HealthSnapshot {
    pub policy: HealthPolicy,
    pub observations: Vec<NodeObservation>,
}

#[derive(Clone, Debug, Default)]
pub struct HealthMonitor {
    policy: HealthPolicy,
    observations: BTreeMap<NodeId, NodeObservation>,
    scorer: HealthScorer,
}

impl HealthMonitor {
    pub fn new(policy: HealthPolicy) -> RsfResult<Self> {
        validate_policy(&policy)?;
        Ok(Self {
            policy,
            observations: BTreeMap::new(),
            scorer: HealthScorer,
        })
    }

    pub fn set_policy(&mut self, policy: HealthPolicy) -> RsfResult<()> {
        validate_policy(&policy)?;
        self.policy = policy;
        Ok(())
    }

    pub fn record_observation(&mut self, observation: NodeObservation) -> RsfResult<()> {
        validate_observation(&observation)?;
        self.observations.insert(observation.node, observation);
        Ok(())
    }

    pub fn evaluate(&self, cluster: &ClusterMap) -> RsfResult<HealthReport> {
        let observations = self
            .observations
            .values()
            .cloned()
            .collect::<Vec<NodeObservation>>();
        self.scorer.evaluate(cluster, &observations, &self.policy)
    }

    pub fn policy(&self) -> &HealthPolicy {
        &self.policy
    }

    pub fn observation(&self, node: NodeId) -> Option<&NodeObservation> {
        self.observations.get(&node)
    }

    pub fn snapshot(&self) -> HealthSnapshot {
        HealthSnapshot {
            policy: self.policy.clone(),
            observations: self.observations.values().cloned().collect(),
        }
    }
}

#[derive(Debug)]
pub struct DurableHealthMonitor {
    monitor: HealthMonitor,
    store: SegmentStore,
    epoch: Epoch,
    next_index: u64,
}

impl DurableHealthMonitor {
    pub fn open(path: impl AsRef<Path>, epoch: Epoch) -> RsfResult<Self> {
        let store = SegmentStore::open(path)?;
        let scan = store.scan(SegmentScanFilter {
            pg: Some(HEALTH_PG),
        })?;
        let mut monitor = HealthMonitor::new(HealthPolicy::default())?;
        let mut next_index = 1;

        for record in scan
            .wal_records
            .iter()
            .filter(|record| record.kind == WalRecordKind::Health)
        {
            apply_health_record(&mut monitor, decode_health_wal_payload(&record.payload)?)?;
            next_index = next_index.max(record.index.0.saturating_add(1));
        }

        Ok(Self {
            monitor,
            store,
            epoch,
            next_index,
        })
    }

    pub fn set_policy(&mut self, policy: HealthPolicy) -> RsfResult<()> {
        let mut monitor = self.monitor.clone();
        monitor.set_policy(policy.clone())?;
        self.append_record(&HealthWalRecord::Policy(policy))?;
        self.monitor = monitor;
        Ok(())
    }

    pub fn record_observation(&mut self, observation: NodeObservation) -> RsfResult<()> {
        let mut monitor = self.monitor.clone();
        monitor.record_observation(observation.clone())?;
        self.append_record(&HealthWalRecord::Observation(observation))?;
        self.monitor = monitor;
        Ok(())
    }

    pub fn evaluate(&self, cluster: &ClusterMap) -> RsfResult<HealthReport> {
        self.monitor.evaluate(cluster)
    }

    pub fn monitor(&self) -> &HealthMonitor {
        &self.monitor
    }

    fn append_record(&mut self, record: &HealthWalRecord) -> RsfResult<()> {
        let payload = encode_health_wal_payload(record)?;
        let recovery = self.store.recover_current()?.recovery;
        let tail = recovery.pg_tail(HEALTH_PG);
        let previous_hash = tail.map_or([0_u8; 32], |tail| tail.record_hash);
        let index = LogIndex(self.next_index);
        let append = WalAppend {
            pg: HEALTH_PG,
            epoch: self.epoch,
            index,
            kind: WalRecordKind::Health,
            previous_hash,
            payload: &payload,
        };
        let (_, _, record) = self.store.append_wal(append)?;
        self.next_index = record.index.0.saturating_add(1);
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum HealthWalRecord {
    Policy(HealthPolicy),
    Observation(NodeObservation),
}

fn apply_health_record(monitor: &mut HealthMonitor, record: HealthWalRecord) -> RsfResult<()> {
    match record {
        HealthWalRecord::Policy(policy) => monitor.set_policy(policy),
        HealthWalRecord::Observation(observation) => monitor.record_observation(observation),
    }
}

fn encode_health_wal_payload(record: &HealthWalRecord) -> RsfResult<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(HEALTH_WAL_MAGIC);
    out.push(HEALTH_WAL_VERSION);
    match record {
        HealthWalRecord::Policy(policy) => {
            out.push(1);
            write_u16(&mut out, policy.min_disk_free_bps);
            write_u16(&mut out, policy.max_p99_latency_ms);
            write_u64(&mut out, policy.max_replica_lag_bytes);
            out.push(policy.max_missed_heartbeats);
            write_u16(&mut out, policy.demote_below_score);
            write_u16(&mut out, policy.promote_at_score);
        }
        HealthWalRecord::Observation(observation) => {
            validate_observation(observation)?;
            out.push(2);
            write_u64(&mut out, observation.node.0);
            write_u16(&mut out, observation.disk_free_bps);
            write_u16(&mut out, observation.p99_latency_ms);
            write_u64(&mut out, observation.replica_lag_bytes);
            out.push(observation.missed_heartbeats);
            out.push(observation.corruption_events);
        }
    }
    Ok(out)
}

fn decode_health_wal_payload(bytes: &[u8]) -> RsfResult<HealthWalRecord> {
    if bytes.len() < HEALTH_WAL_MAGIC.len() + 2 {
        return Err(corrupt("health WAL payload is too short"));
    }
    if &bytes[..HEALTH_WAL_MAGIC.len()] != HEALTH_WAL_MAGIC {
        return Err(corrupt("health WAL magic mismatch"));
    }
    let mut offset = HEALTH_WAL_MAGIC.len();
    let version = read_u8(bytes, &mut offset)?;
    if version != HEALTH_WAL_VERSION {
        return Err(corrupt("unsupported health WAL version"));
    }
    let tag = read_u8(bytes, &mut offset)?;
    let record = match tag {
        1 => HealthWalRecord::Policy(HealthPolicy {
            min_disk_free_bps: read_u16(bytes, &mut offset)?,
            max_p99_latency_ms: read_u16(bytes, &mut offset)?,
            max_replica_lag_bytes: read_u64(bytes, &mut offset)?,
            max_missed_heartbeats: read_u8(bytes, &mut offset)?,
            demote_below_score: read_u16(bytes, &mut offset)?,
            promote_at_score: read_u16(bytes, &mut offset)?,
        }),
        2 => HealthWalRecord::Observation(NodeObservation {
            node: NodeId(read_u64(bytes, &mut offset)?),
            disk_free_bps: read_u16(bytes, &mut offset)?,
            p99_latency_ms: read_u16(bytes, &mut offset)?,
            replica_lag_bytes: read_u64(bytes, &mut offset)?,
            missed_heartbeats: read_u8(bytes, &mut offset)?,
            corruption_events: read_u8(bytes, &mut offset)?,
        }),
        _ => return Err(corrupt("unknown health WAL record tag")),
    };
    if offset != bytes.len() {
        return Err(corrupt("trailing bytes in health WAL payload"));
    }
    Ok(record)
}

fn score_node(observation: &NodeObservation, policy: &HealthPolicy) -> NodeHealth {
    let mut score = 100u16;
    let mut reasons = Vec::new();

    if observation.disk_free_bps < policy.min_disk_free_bps {
        score = score.saturating_sub(20);
        reasons.push(format!(
            "disk free {}bps below {}bps",
            observation.disk_free_bps, policy.min_disk_free_bps
        ));
    }
    if observation.p99_latency_ms > policy.max_p99_latency_ms {
        score = score.saturating_sub(25);
        reasons.push(format!(
            "p99 latency {}ms above {}ms",
            observation.p99_latency_ms, policy.max_p99_latency_ms
        ));
    }
    if observation.replica_lag_bytes > policy.max_replica_lag_bytes {
        score = score.saturating_sub(25);
        reasons.push(format!(
            "replica lag {} bytes above {}",
            observation.replica_lag_bytes, policy.max_replica_lag_bytes
        ));
    }
    if observation.missed_heartbeats > policy.max_missed_heartbeats {
        score = score.saturating_sub(20);
        reasons.push(format!(
            "missed {} heartbeats above {}",
            observation.missed_heartbeats, policy.max_missed_heartbeats
        ));
    }
    if observation.corruption_events > 0 {
        score = score.saturating_sub(100);
        reasons.push(format!(
            "{} corruption events require quarantine",
            observation.corruption_events
        ));
    }

    let class = if score < policy.demote_below_score {
        HealthClass::Demote
    } else if reasons.is_empty() {
        HealthClass::Healthy
    } else {
        HealthClass::Degraded
    };

    NodeHealth {
        node: observation.node,
        score,
        class,
        reasons,
    }
}

fn validate_policy(policy: &HealthPolicy) -> RsfResult<()> {
    if policy.min_disk_free_bps > 10_000 {
        return Err(invalid("min disk free bps cannot exceed 10000"));
    }
    if policy.demote_below_score >= policy.promote_at_score {
        return Err(invalid("demote score must be below promote score"));
    }
    if policy.promote_at_score > 100 {
        return Err(invalid("promote score cannot exceed 100"));
    }
    Ok(())
}

fn validate_observation(observation: &NodeObservation) -> RsfResult<()> {
    if observation.disk_free_bps > 10_000 {
        return Err(invalid("disk free bps cannot exceed 10000"));
    }
    Ok(())
}

fn write_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn write_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn read_u8(bytes: &[u8], offset: &mut usize) -> RsfResult<u8> {
    if *offset >= bytes.len() {
        return Err(corrupt("unexpected end of health WAL payload"));
    }
    let value = bytes[*offset];
    *offset += 1;
    Ok(value)
}

fn read_u16(bytes: &[u8], offset: &mut usize) -> RsfResult<u16> {
    if bytes.len().saturating_sub(*offset) < 2 {
        return Err(corrupt("unexpected end of health WAL payload"));
    }
    let value = u16::from_le_bytes([bytes[*offset], bytes[*offset + 1]]);
    *offset += 2;
    Ok(value)
}

fn read_u64(bytes: &[u8], offset: &mut usize) -> RsfResult<u64> {
    if bytes.len().saturating_sub(*offset) < 8 {
        return Err(corrupt("unexpected end of health WAL payload"));
    }
    let value = u64::from_le_bytes([
        bytes[*offset],
        bytes[*offset + 1],
        bytes[*offset + 2],
        bytes[*offset + 3],
        bytes[*offset + 4],
        bytes[*offset + 5],
        bytes[*offset + 6],
        bytes[*offset + 7],
    ]);
    *offset += 8;
    Ok(value)
}

fn corrupt(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::CorruptionDetected, message.into())
}

fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message.into())
}

#[cfg(test)]
mod tests {
    use rsf_core::{
        Epoch, FailureDomainId, RegionId,
        cluster::{NodeDescriptor, NodeRole},
    };

    use super::*;

    #[test]
    fn healthy_node_is_promotion_candidate() {
        let report = HealthScorer
            .evaluate(&cluster(1), &[healthy(0)], &HealthPolicy::default())
            .unwrap();

        assert_eq!(
            report.actions,
            vec![HealthAction::PromoteLearner { node: NodeId(0) }]
        );
        assert_eq!(report.nodes[0].class, HealthClass::Healthy);
    }

    #[test]
    fn slow_lagging_node_is_demoted() {
        let report = HealthScorer
            .evaluate(
                &cluster(1),
                &[NodeObservation {
                    node: NodeId(0),
                    disk_free_bps: 5_000,
                    p99_latency_ms: 120,
                    replica_lag_bytes: 2_000_000,
                    missed_heartbeats: 0,
                    corruption_events: 0,
                }],
                &HealthPolicy::default(),
            )
            .unwrap();

        assert_eq!(report.nodes[0].class, HealthClass::Demote);
        assert_eq!(
            report.actions,
            vec![HealthAction::DemoteSlowReplica { node: NodeId(0) }]
        );
    }

    #[test]
    fn corrupt_node_is_quarantined() {
        let mut observation = healthy(0);
        observation.corruption_events = 1;

        let report = HealthScorer
            .evaluate(&cluster(1), &[observation], &HealthPolicy::default())
            .unwrap();

        assert_eq!(
            report.actions,
            vec![HealthAction::QuarantineCorruptNode { node: NodeId(0) }]
        );
    }

    #[test]
    fn missing_observation_is_rejected() {
        let err = HealthScorer
            .evaluate(&cluster(2), &[healthy(0)], &HealthPolicy::default())
            .expect_err("missing observation must fail");

        assert_eq!(err.code, ErrorCode::InvalidInput);
    }

    #[test]
    fn durable_health_recovers_policy_observations_and_actions() {
        let path = test_path("durable_health_recovers_policy_observations_and_actions");
        let policy = HealthPolicy {
            min_disk_free_bps: 2_000,
            max_p99_latency_ms: 40,
            max_replica_lag_bytes: 1_000,
            max_missed_heartbeats: 1,
            demote_below_score: 70,
            promote_at_score: 90,
        };

        {
            let mut monitor = DurableHealthMonitor::open(&path, Epoch(2)).expect("open health");
            monitor.set_policy(policy.clone()).expect("persist policy");
            monitor
                .record_observation(NodeObservation {
                    node: NodeId(0),
                    disk_free_bps: 5_000,
                    p99_latency_ms: 8,
                    replica_lag_bytes: 0,
                    missed_heartbeats: 0,
                    corruption_events: 0,
                })
                .expect("persist healthy");
            monitor
                .record_observation(NodeObservation {
                    node: NodeId(1),
                    disk_free_bps: 5_000,
                    p99_latency_ms: 8,
                    replica_lag_bytes: 0,
                    missed_heartbeats: 0,
                    corruption_events: 1,
                })
                .expect("persist corrupt");
        }

        let recovered = DurableHealthMonitor::open(&path, Epoch(2)).expect("recover health");
        assert_eq!(recovered.monitor().policy(), &policy);
        assert_eq!(
            recovered.monitor().observation(NodeId(0)),
            Some(&NodeObservation {
                node: NodeId(0),
                disk_free_bps: 5_000,
                p99_latency_ms: 8,
                replica_lag_bytes: 0,
                missed_heartbeats: 0,
                corruption_events: 0,
            })
        );

        let report = recovered.evaluate(&cluster(2)).expect("evaluate recovered");
        assert_eq!(
            report.actions,
            vec![
                HealthAction::PromoteLearner { node: NodeId(0) },
                HealthAction::QuarantineCorruptNode { node: NodeId(1) },
            ]
        );
    }

    #[test]
    fn durable_health_rejects_invalid_recovered_policy() {
        let path = test_path("durable_health_rejects_invalid_recovered_policy");
        let payload = encode_health_wal_payload(&HealthWalRecord::Policy(HealthPolicy {
            min_disk_free_bps: 1_000,
            max_p99_latency_ms: 40,
            max_replica_lag_bytes: 1_000,
            max_missed_heartbeats: 1,
            demote_below_score: 90,
            promote_at_score: 90,
        }))
        .expect("encode policy");
        let mut store = SegmentStore::open(&path).expect("segment store");
        let recovery = store.recover_current().expect("recover").recovery;
        let tail = recovery.pg_tail(HEALTH_PG);
        let previous_hash = tail.map_or([0_u8; 32], |tail| tail.record_hash);
        store
            .append_wal(WalAppend {
                pg: HEALTH_PG,
                epoch: Epoch(3),
                index: LogIndex(1),
                kind: WalRecordKind::Health,
                previous_hash,
                payload: &payload,
            })
            .expect("append invalid policy");

        let err = DurableHealthMonitor::open(&path, Epoch(3)).expect_err("replay rejects policy");
        assert_eq!(err.code, ErrorCode::InvalidInput);
    }

    #[test]
    fn health_wal_decode_rejects_trailing_bytes() {
        let mut payload = encode_health_wal_payload(&HealthWalRecord::Observation(healthy(7)))
            .expect("encode observation");
        payload.push(9);

        let err = decode_health_wal_payload(&payload).expect_err("trailing bytes reject");
        assert_eq!(err.code, ErrorCode::CorruptionDetected);
    }

    fn cluster(count: u64) -> ClusterMap {
        ClusterMap {
            epoch: Epoch(1),
            nodes: (0..count)
                .map(|id| NodeDescriptor {
                    id: NodeId(id),
                    region: RegionId::new("iad").expect("region"),
                    failure_domain: FailureDomainId::new(format!("iad-{id}")).expect("domain"),
                    capacity_bytes: 1_000,
                    roles: vec![NodeRole::Storage],
                })
                .collect(),
        }
    }

    fn healthy(node: u64) -> NodeObservation {
        NodeObservation {
            node: NodeId(node),
            disk_free_bps: 5_000,
            p99_latency_ms: 8,
            replica_lag_bytes: 0,
            missed_heartbeats: 0,
            corruption_events: 0,
        }
    }

    fn test_path(name: &str) -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "rsf-health-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ));
        path
    }
}
