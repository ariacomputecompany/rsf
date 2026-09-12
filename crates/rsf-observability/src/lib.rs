#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::path::Path;

use rsf_core::storage::WalRecordKind;
use rsf_core::{
    CommitVersion, Epoch, ErrorCode, LogIndex, NodeId, PlacementGroupId, RsfError, RsfResult,
};
use rsf_storage::{SegmentStore, WalAppend};

const AUDIT_LOG_PG: PlacementGroupId = PlacementGroupId(0);
const AUDIT_RECORD_MAGIC: &[u8; 5] = b"RSFO1";
const AUDIT_RECORD_VERSION: u8 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetricName(pub String);

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MetricValue {
    Counter(u64),
    Gauge(i64),
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum HealthStatus {
    Healthy,
    Degraded,
    Unavailable,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceHealth {
    pub service: String,
    pub status: HealthStatus,
    pub detail: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuditEvent {
    pub service: String,
    pub action: String,
    pub version: Option<CommitVersion>,
    pub node: Option<NodeId>,
    pub pg: Option<PlacementGroupId>,
    pub detail: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuditRecord {
    pub sequence: LogIndex,
    pub event: AuditEvent,
}

#[derive(Debug)]
pub struct DurableAuditLog {
    store: SegmentStore,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HealthReport {
    pub status: HealthStatus,
    pub services: Vec<ServiceHealth>,
    pub counters: usize,
    pub gauges: usize,
    pub audit_events: usize,
}

#[derive(Clone, Debug, Default)]
pub struct ObservabilityRegistry {
    metrics: BTreeMap<String, MetricValue>,
    services: BTreeMap<String, ServiceHealth>,
    audit: Vec<AuditEvent>,
}

impl DurableAuditLog {
    pub fn open(path: impl AsRef<Path>) -> RsfResult<Self> {
        Ok(Self {
            store: SegmentStore::open(path)?,
        })
    }

    pub fn recover(path: impl AsRef<Path>) -> RsfResult<Vec<AuditRecord>> {
        let recovery = SegmentStore::recover(path)?;
        decode_records(recovery.recovery.wal_records.iter())
    }

    pub fn append(&mut self, event: AuditEvent) -> RsfResult<AuditRecord> {
        validate_audit_event(&event)?;
        let recovery = self.store.recover_current()?;
        let tail = recovery.recovery.pg_tail(AUDIT_LOG_PG);
        let previous_hash = tail.map_or([0_u8; 32], |tail| tail.record_hash);
        let sequence = LogIndex(tail.map_or(1, |tail| tail.index.0 + 1));
        let payload = encode_audit_event(&event)?;

        self.store.append_wal(WalAppend {
            pg: AUDIT_LOG_PG,
            epoch: Epoch(0),
            index: sequence,
            kind: WalRecordKind::Observability,
            previous_hash,
            payload: &payload,
        })?;

        Ok(AuditRecord { sequence, event })
    }

    pub fn records(&self) -> RsfResult<Vec<AuditRecord>> {
        let recovery = self.store.recover_current()?;
        decode_records(recovery.recovery.wal_records.iter())
    }
}

impl ObservabilityRegistry {
    pub fn incr(&mut self, name: impl Into<String>, amount: u64) {
        let name = name.into();
        let next = match self.metrics.get(&name) {
            Some(MetricValue::Counter(existing)) => existing.saturating_add(amount),
            Some(MetricValue::Gauge(_)) | None => amount,
        };
        self.metrics.insert(name, MetricValue::Counter(next));
    }

    pub fn try_incr(&mut self, name: impl Into<String>, amount: u64) -> RsfResult<()> {
        let name = name.into();
        validate_metric_name(&name)?;
        let next = match self.metrics.get(&name) {
            Some(MetricValue::Counter(existing)) => existing.saturating_add(amount),
            Some(MetricValue::Gauge(_)) | None => amount,
        };
        self.metrics.insert(name, MetricValue::Counter(next));
        Ok(())
    }

    pub fn gauge(&mut self, name: impl Into<String>, value: i64) {
        self.metrics.insert(name.into(), MetricValue::Gauge(value));
    }

    pub fn try_gauge(&mut self, name: impl Into<String>, value: i64) -> RsfResult<()> {
        let name = name.into();
        validate_metric_name(&name)?;
        self.metrics.insert(name, MetricValue::Gauge(value));
        Ok(())
    }

    pub fn set_health(
        &mut self,
        service: impl Into<String>,
        status: HealthStatus,
        detail: impl Into<String>,
    ) {
        let service = service.into();
        self.services.insert(
            service.clone(),
            ServiceHealth {
                service,
                status,
                detail: detail.into(),
            },
        );
    }

    pub fn audit(&mut self, event: AuditEvent) {
        self.audit.push(event);
    }

    pub fn metric(&self, name: &str) -> Option<&MetricValue> {
        self.metrics.get(name)
    }

    pub fn metrics(&self) -> impl Iterator<Item = (&str, &MetricValue)> {
        self.metrics
            .iter()
            .map(|(name, value)| (name.as_str(), value))
    }

    pub fn render_prometheus(&self) -> RsfResult<String> {
        let mut out = String::new();
        for (name, value) in self.metrics() {
            validate_metric_name(name)?;
            match value {
                MetricValue::Counter(count) => {
                    out.push_str("# TYPE ");
                    out.push_str(name);
                    out.push_str(" counter\n");
                    out.push_str(name);
                    out.push(' ');
                    out.push_str(&count.to_string());
                    out.push('\n');
                }
                MetricValue::Gauge(gauge) => {
                    out.push_str("# TYPE ");
                    out.push_str(name);
                    out.push_str(" gauge\n");
                    out.push_str(name);
                    out.push(' ');
                    out.push_str(&gauge.to_string());
                    out.push('\n');
                }
            }
        }
        Ok(out)
    }

    pub fn audit_events(&self) -> &[AuditEvent] {
        &self.audit
    }

    pub fn service_health(&self, service: &str) -> Option<&ServiceHealth> {
        self.services.get(service)
    }

    pub fn report(&self) -> HealthReport {
        let counters = self
            .metrics
            .values()
            .filter(|metric| matches!(metric, MetricValue::Counter(_)))
            .count();
        let gauges = self
            .metrics
            .values()
            .filter(|metric| matches!(metric, MetricValue::Gauge(_)))
            .count();
        let services = self.services.values().cloned().collect::<Vec<_>>();
        let status = services
            .iter()
            .map(|service| service.status)
            .max()
            .unwrap_or(HealthStatus::Healthy);

        HealthReport {
            status,
            services,
            counters,
            gauges,
            audit_events: self.audit.len(),
        }
    }
}

fn decode_records<'a>(
    records: impl Iterator<Item = &'a rsf_core::storage::WalRecord>,
) -> RsfResult<Vec<AuditRecord>> {
    let mut audit = Vec::new();
    for record in records {
        if record.pg == AUDIT_LOG_PG && record.kind == WalRecordKind::Observability {
            audit.push(AuditRecord {
                sequence: record.index,
                event: decode_audit_event(&record.payload)?,
            });
        }
    }
    Ok(audit)
}

fn encode_audit_event(event: &AuditEvent) -> RsfResult<Vec<u8>> {
    validate_audit_event(event)?;
    let mut out = Vec::new();
    out.extend_from_slice(AUDIT_RECORD_MAGIC);
    out.push(AUDIT_RECORD_VERSION);
    write_string(&event.service, &mut out)?;
    write_string(&event.action, &mut out)?;
    write_optional_u64(event.version.map(|version| version.0), &mut out);
    write_optional_u64(event.node.map(|node| node.0), &mut out);
    write_optional_u64(event.pg.map(|pg| pg.0), &mut out);
    write_string(&event.detail, &mut out)?;
    Ok(out)
}

fn decode_audit_event(bytes: &[u8]) -> RsfResult<AuditEvent> {
    if !is_audit_payload(bytes) {
        return Err(corrupt("audit record payload has invalid magic"));
    }
    let version = *bytes
        .get(AUDIT_RECORD_MAGIC.len())
        .ok_or_else(|| corrupt("audit record payload is missing version"))?;
    if version != AUDIT_RECORD_VERSION {
        return Err(corrupt("unsupported audit record version"));
    }

    let mut offset = AUDIT_RECORD_MAGIC.len() + 1;
    let (service, next) = read_string(bytes, offset)?;
    offset = next;
    let (action, next) = read_string(bytes, offset)?;
    offset = next;
    let (version, next) = read_optional_u64(bytes, offset)?;
    offset = next;
    let (node, next) = read_optional_u64(bytes, offset)?;
    offset = next;
    let (pg, next) = read_optional_u64(bytes, offset)?;
    offset = next;
    let (detail, next) = read_string(bytes, offset)?;
    offset = next;
    if offset != bytes.len() {
        return Err(corrupt("audit record payload has trailing bytes"));
    }

    let event = AuditEvent {
        service,
        action,
        version: version.map(CommitVersion),
        node: node.map(NodeId),
        pg: pg.map(PlacementGroupId),
        detail,
    };
    validate_audit_event(&event)?;
    Ok(event)
}

fn is_audit_payload(bytes: &[u8]) -> bool {
    bytes.starts_with(AUDIT_RECORD_MAGIC)
}

fn validate_audit_event(event: &AuditEvent) -> RsfResult<()> {
    validate_label("audit service", &event.service)?;
    validate_label("audit action", &event.action)?;
    if event.detail.is_empty() {
        return Err(invalid("audit detail must not be empty"));
    }
    if event.detail.len() > 4096 {
        return Err(invalid("audit detail must be at most 4096 bytes"));
    }
    Ok(())
}

fn validate_label(label: &str, value: &str) -> RsfResult<()> {
    if value.is_empty() {
        return Err(invalid(format!("{label} must not be empty")));
    }
    if value.len() > 128 {
        return Err(invalid(format!("{label} must be at most 128 bytes")));
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
    {
        return Err(invalid(format!(
            "{label} may only contain ASCII letters, digits, '-', '_', '.', or ':'"
        )));
    }
    Ok(())
}

fn validate_metric_name(name: &str) -> RsfResult<()> {
    if name.is_empty() {
        return Err(invalid("metric name must not be empty"));
    }
    if name.len() > 128 {
        return Err(invalid("metric name must be at most 128 bytes"));
    }
    let mut bytes = name.bytes();
    let first = bytes.next().expect("non-empty metric name");
    if !(first.is_ascii_alphabetic() || first == b'_') {
        return Err(invalid(
            "metric name must start with an ASCII letter or '_'",
        ));
    }
    if !bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_') {
        return Err(invalid(
            "metric name may only contain ASCII letters, digits, or '_'",
        ));
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message.into())
}

fn corrupt(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::CorruptionDetected, message.into())
}

fn write_u64(value: u64, out: &mut Vec<u8>) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn read_u64(bytes: &[u8], offset: usize) -> RsfResult<u64> {
    let raw = bytes
        .get(offset..offset + 8)
        .ok_or_else(|| corrupt("audit record ended while reading u64"))?;
    Ok(u64::from_le_bytes(raw.try_into().expect("slice length")))
}

fn write_optional_u64(value: Option<u64>, out: &mut Vec<u8>) {
    match value {
        Some(value) => {
            out.push(1);
            write_u64(value, out);
        }
        None => out.push(0),
    }
}

fn read_optional_u64(bytes: &[u8], offset: usize) -> RsfResult<(Option<u64>, usize)> {
    let tag = *bytes
        .get(offset)
        .ok_or_else(|| corrupt("audit record ended while reading optional value"))?;
    match tag {
        0 => Ok((None, offset + 1)),
        1 => Ok((Some(read_u64(bytes, offset + 1)?), offset + 9)),
        _ => Err(corrupt("audit record has invalid optional value tag")),
    }
}

fn write_string(value: &str, out: &mut Vec<u8>) -> RsfResult<()> {
    let len = u64::try_from(value.len())
        .map_err(|_| invalid("audit string length does not fit in record"))?;
    write_u64(len, out);
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

fn read_string(bytes: &[u8], offset: usize) -> RsfResult<(String, usize)> {
    let len = usize::try_from(read_u64(bytes, offset)?)
        .map_err(|_| corrupt("audit string length does not fit in memory"))?;
    let start = offset + 8;
    let end = start + len;
    let raw = bytes
        .get(start..end)
        .ok_or_else(|| corrupt("audit record ended while reading string"))?;
    let value =
        String::from_utf8(raw.to_vec()).map_err(|_| corrupt("audit string is not valid utf-8"))?;
    Ok((value, end))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsf_storage::SegmentScanFilter;

    #[test]
    fn counters_accumulate_saturating_values() {
        let mut registry = ObservabilityRegistry::default();

        registry.incr("protocol_commits_total", 2);
        registry.incr("protocol_commits_total", 3);

        assert_eq!(
            registry.metric("protocol_commits_total"),
            Some(&MetricValue::Counter(5))
        );
    }

    #[test]
    fn gauges_replace_latest_value() {
        let mut registry = ObservabilityRegistry::default();

        registry.gauge("cluster_nodes", 4);
        registry.gauge("cluster_nodes", 5);

        assert_eq!(
            registry.metric("cluster_nodes"),
            Some(&MetricValue::Gauge(5))
        );
    }

    #[test]
    fn rejects_invalid_metric_names() {
        let mut registry = ObservabilityRegistry::default();

        let err = registry
            .try_gauge("cluster.nodes", 1)
            .expect_err("invalid name rejected");

        assert_eq!(err.code, ErrorCode::InvalidInput);
    }

    #[test]
    fn prometheus_renderer_exports_stable_sorted_metrics() {
        let mut registry = ObservabilityRegistry::default();
        registry.gauge("cluster_nodes", 3);
        registry.incr("daemon_ticks_total", 2);

        let rendered = registry.render_prometheus().expect("render");

        assert!(rendered.contains("# TYPE cluster_nodes gauge\ncluster_nodes 3\n"));
        assert!(rendered.contains("# TYPE daemon_ticks_total counter\ndaemon_ticks_total 2\n"));
        assert!(
            rendered.find("cluster_nodes").expect("cluster metric")
                < rendered.find("daemon_ticks_total").expect("daemon metric")
        );
    }

    #[test]
    fn prometheus_renderer_rejects_invalid_legacy_metric_names() {
        let mut registry = ObservabilityRegistry::default();
        registry.gauge("cluster.nodes", 3);

        let err = registry
            .render_prometheus()
            .expect_err("invalid metric names rejected during export");

        assert_eq!(err.code, ErrorCode::InvalidInput);
    }

    #[test]
    fn report_degrades_to_worst_service_health() {
        let mut registry = ObservabilityRegistry::default();

        registry.set_health(
            "catalog",
            HealthStatus::Healthy,
            "metadata quorum available",
        );
        registry.set_health("repair", HealthStatus::Degraded, "one pg under replicated");

        let report = registry.report();

        assert_eq!(report.status, HealthStatus::Degraded);
        assert_eq!(report.services.len(), 2);
    }

    #[test]
    fn audit_events_keep_operational_context() {
        let mut registry = ObservabilityRegistry::default();

        registry.audit(AuditEvent {
            service: "txn".to_string(),
            action: "commit".to_string(),
            version: Some(CommitVersion(42)),
            node: Some(NodeId(7)),
            pg: Some(PlacementGroupId(1)),
            detail: "committed user transaction".to_string(),
        });

        assert_eq!(registry.audit_events().len(), 1);
        assert_eq!(registry.audit_events()[0].version, Some(CommitVersion(42)));
        assert_eq!(registry.report().audit_events, 1);
    }

    #[test]
    fn durable_audit_log_appends_and_recovers_ordered_events() {
        let path = test_path("durable_audit_log_appends_and_recovers_ordered_events");
        let mut log = DurableAuditLog::open(&path).expect("open audit log");

        let first = log
            .append(AuditEvent {
                service: "rsfd".to_string(),
                action: "cluster.init".to_string(),
                version: Some(CommitVersion(1)),
                node: Some(NodeId(7)),
                pg: None,
                detail: "initialized durable cluster state".to_string(),
            })
            .expect("append first");
        let second = log
            .append(AuditEvent {
                service: "rsfd".to_string(),
                action: "policy.apply".to_string(),
                version: Some(CommitVersion(2)),
                node: None,
                pg: Some(PlacementGroupId(3)),
                detail: "applied placement policy".to_string(),
            })
            .expect("append second");

        assert_eq!(first.sequence, LogIndex(1));
        assert_eq!(second.sequence, LogIndex(2));

        let recovered = DurableAuditLog::recover(&path).expect("recover audit log");
        assert_eq!(recovered.len(), 2);
        assert_eq!(recovered[0].event.action, "cluster.init");
        assert_eq!(recovered[1].event.pg, Some(PlacementGroupId(3)));

        let store = SegmentStore::open(&path).expect("open segment store");
        let scan = store
            .scan(SegmentScanFilter {
                pg: Some(AUDIT_LOG_PG),
            })
            .expect("scan audit log");
        assert_eq!(scan.wal_records.len(), 2);
        assert!(
            scan.wal_records
                .iter()
                .all(|record| record.kind == WalRecordKind::Observability)
        );

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn durable_audit_log_rejects_invalid_events() {
        let path = test_path("durable_audit_log_rejects_invalid_events");
        let mut log = DurableAuditLog::open(&path).expect("open audit log");

        let err = log
            .append(AuditEvent {
                service: "rsfd".to_string(),
                action: "policy apply".to_string(),
                version: None,
                node: None,
                pg: None,
                detail: "contains an invalid action label".to_string(),
            })
            .expect_err("invalid action rejected");

        assert_eq!(err.code, ErrorCode::InvalidInput);

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn audit_decoder_rejects_trailing_bytes() {
        let event = AuditEvent {
            service: "rsfd".to_string(),
            action: "cluster.init".to_string(),
            version: None,
            node: None,
            pg: None,
            detail: "initialized".to_string(),
        };
        let mut encoded = encode_audit_event(&event).expect("encode");
        encoded.push(99);

        let err = decode_audit_event(&encoded).expect_err("trailing bytes rejected");

        assert_eq!(err.code, ErrorCode::CorruptionDetected);
    }

    #[test]
    fn durable_audit_log_recovery_rejects_malformed_observability_record() {
        let path = test_path("durable_audit_log_rejects_malformed_observability_record");
        let mut store = SegmentStore::open(&path).expect("open segment store");
        store
            .append_wal(WalAppend {
                pg: AUDIT_LOG_PG,
                epoch: Epoch(0),
                index: LogIndex(1),
                kind: WalRecordKind::Observability,
                previous_hash: [0; 32],
                payload: b"not-audit",
            })
            .expect("append malformed observability record");

        let err =
            DurableAuditLog::recover(&path).expect_err("malformed audit record must fail recovery");
        assert_eq!(err.code, ErrorCode::CorruptionDetected);
        assert!(err.message.contains("invalid magic"));

        let _ = std::fs::remove_dir_all(path);
    }

    fn test_path(name: &str) -> std::path::PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "rsf-observability-{name}-{}-{nonce}",
            std::process::id()
        ))
    }
}
