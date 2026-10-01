use std::io::Write;
use std::io::{BufRead, BufReader};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::data::DataPlaneListener;
use rsf_admin::{AdminPlane, AdmissionState};
use rsf_core::LogIndex;
use rsf_core::cluster::NodeRole;
use rsf_observability::ObservabilityRegistry;
use rsf_policy::PolicyCatalog;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DaemonConfig {
    pub state: PathBuf,
    pub once: bool,
    pub tick: Duration,
    pub listen: Option<SocketAddr>,
    pub data_listen: Option<SocketAddr>,
    pub profile: OperatorProfile,
    pub max_consecutive_errors: u32,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum OperatorProfile {
    #[default]
    Standalone,
    Regional,
    MultiRegion,
}

impl OperatorProfile {
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "standalone" => Ok(Self::Standalone),
            "regional" => Ok(Self::Regional),
            "multi-region" => Ok(Self::MultiRegion),
            _ => Err("operator profile must be standalone, regional, or multi-region".to_string()),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Standalone => "standalone",
            Self::Regional => "regional",
            Self::MultiRegion => "multi-region",
        }
    }

    fn requirements(self) -> ProfileRequirements {
        match self {
            Self::Standalone => ProfileRequirements {
                voters: 1,
                storage_nodes: 1,
                gateway_nodes: 1,
                policy_regions: 1,
            },
            Self::Regional => ProfileRequirements {
                voters: 3,
                storage_nodes: 3,
                gateway_nodes: 1,
                policy_regions: 1,
            },
            Self::MultiRegion => ProfileRequirements {
                voters: 5,
                storage_nodes: 3,
                gateway_nodes: 2,
                policy_regions: 2,
            },
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ProfileRequirements {
    voters: usize,
    storage_nodes: usize,
    gateway_nodes: usize,
    policy_regions: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DaemonSnapshot {
    pub profile: OperatorProfile,
    pub required_voters: usize,
    pub required_storage_nodes: usize,
    pub required_gateway_nodes: usize,
    pub required_policy_regions: usize,
    pub cluster_epoch: u64,
    pub nodes: usize,
    pub promoted_voters: usize,
    pub storage_nodes: usize,
    pub gateway_nodes: usize,
    pub policy_regions: usize,
    pub policy_version: u64,
    pub policy_min_replicas: u8,
    pub policy_placement_groups: u64,
    pub protocol_placement_version: u64,
    pub protocol_placement_groups: usize,
    pub protocol_group_ids: Vec<u64>,
    pub protocol_latest_version: u64,
    pub protocol_certificate_count: usize,
    pub protocol_max_log_index: u64,
    pub replication: crate::replication::ReplicationStatus,
    pub ready: bool,
    pub readiness: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HealthEndpoint {
    Livez,
    Readyz,
    Healthz,
    Metrics,
}

impl HealthEndpoint {
    pub fn path(self) -> &'static str {
        match self {
            Self::Livez => "/livez",
            Self::Readyz => "/readyz",
            Self::Healthz => "/healthz",
            Self::Metrics => "/metrics",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EndpointResponse {
    pub status: u16,
    pub content_type: &'static str,
    pub body: String,
}

pub fn run_daemon(config: DaemonConfig, out: &mut impl Write) -> Result<(), String> {
    validate_config(&config)?;
    let listener = match config.listen {
        Some(addr) => Some(HealthListener::bind(addr)?),
        None => None,
    };
    let data_listener = match config.data_listen {
        Some(addr) => Some(DataPlaneListener::bind(addr, config.state.clone())?),
        None => None,
    };
    if let Some(listener) = &listener {
        writeln!(
            out,
            "daemon listener addr={} endpoints=/livez,/readyz,/healthz,/metrics",
            listener.local_addr()
        )
        .map_err(|err| err.to_string())?;
    }
    if let Some(listener) = &data_listener {
        writeln!(
            out,
            "daemon data_listener addr={} protocol=rsf-wire framing=u32-le-length-prefixed",
            listener.local_addr()
        )
        .map_err(|err| err.to_string())?;
    }
    run_daemon_loop(
        &config,
        listener.as_ref(),
        data_listener.as_ref(),
        out,
        load_snapshot_for_profile,
        std::thread::sleep,
    )
}

fn run_daemon_loop(
    config: &DaemonConfig,
    listener: Option<&HealthListener>,
    data_listener: Option<&DataPlaneListener>,
    out: &mut impl Write,
    mut load_snapshot: impl FnMut(&Path, OperatorProfile) -> Result<DaemonSnapshot, String>,
    mut sleep: impl FnMut(Duration),
) -> Result<(), String> {
    let mut tick = 0_u64;
    let mut supervisor = SupervisorState::new(config.max_consecutive_errors);
    loop {
        match run_daemon_tick(
            config,
            listener,
            data_listener,
            out,
            tick,
            &mut load_snapshot,
        ) {
            Ok(()) => supervisor.record_success(),
            Err(err) => {
                if config.once {
                    return Err(err);
                }
                let failure = supervisor.record_error(err);
                emit_supervision_failure(out, tick, &failure)?;
                if failure.should_stop {
                    return Err(format!(
                        "daemon supervision stopped after {} consecutive runtime errors; last error: {}",
                        failure.consecutive_errors, failure.error
                    ));
                }
            }
        }
        if config.once {
            return Ok(());
        }
        tick += 1;
        sleep(config.tick);
    }
}

fn run_daemon_tick(
    config: &DaemonConfig,
    listener: Option<&HealthListener>,
    data_listener: Option<&DataPlaneListener>,
    out: &mut impl Write,
    tick: u64,
    load_snapshot: &mut impl FnMut(&Path, OperatorProfile) -> Result<DaemonSnapshot, String>,
) -> Result<(), String> {
    let snapshot = load_snapshot(&config.state, config.profile)?;
    emit_snapshot(out, config, tick, &snapshot)?;
    if let Some(listener) = listener {
        listener.serve_pending(&snapshot)?;
    }
    if let Some(listener) = data_listener {
        listener.serve_pending(out)?;
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SupervisionFailure {
    pub error: String,
    pub consecutive_errors: u32,
    pub max_consecutive_errors: u32,
    pub should_stop: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SupervisorState {
    consecutive_errors: u32,
    max_consecutive_errors: u32,
}

impl SupervisorState {
    pub fn new(max_consecutive_errors: u32) -> Self {
        Self {
            consecutive_errors: 0,
            max_consecutive_errors,
        }
    }

    pub fn record_success(&mut self) {
        self.consecutive_errors = 0;
    }

    pub fn record_error(&mut self, error: String) -> SupervisionFailure {
        self.consecutive_errors = self.consecutive_errors.saturating_add(1);
        SupervisionFailure {
            error,
            consecutive_errors: self.consecutive_errors,
            max_consecutive_errors: self.max_consecutive_errors,
            should_stop: self.consecutive_errors >= self.max_consecutive_errors,
        }
    }
}

pub struct HealthListener {
    listener: TcpListener,
}

impl HealthListener {
    pub fn bind(addr: SocketAddr) -> Result<Self, String> {
        let listener = TcpListener::bind(addr)
            .map_err(|err| format!("failed to bind daemon listener {addr}: {err}"))?;
        listener
            .set_nonblocking(true)
            .map_err(|err| format!("failed to configure daemon listener {addr}: {err}"))?;
        Ok(Self { listener })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.listener
            .local_addr()
            .expect("bound listener exposes local address")
    }

    pub fn serve_pending(&self, snapshot: &DaemonSnapshot) -> Result<usize, String> {
        let mut served = 0_usize;
        loop {
            match self.listener.accept() {
                Ok((stream, _peer)) => {
                    if let Err(error) = serve_http_stream(stream, snapshot) {
                        eprintln!("daemon health_connection outcome=rejected reason={error}");
                    }
                    served = served.saturating_add(1);
                }
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => return Ok(served),
                Err(err) => return Err(format!("failed to accept daemon health request: {err}")),
            }
        }
    }
}

fn serve_http_stream(stream: TcpStream, snapshot: &DaemonSnapshot) -> Result<(), String> {
    serve_http_stream_with_timeout(stream, snapshot, Duration::from_secs(2))
}

fn serve_http_stream_with_timeout(
    mut stream: TcpStream,
    snapshot: &DaemonSnapshot,
    timeout: Duration,
) -> Result<(), String> {
    // Accepted sockets may inherit the nonblocking listener's mode on the host.
    stream
        .set_nonblocking(false)
        .map_err(|err| format!("failed to configure health socket: {err}"))?;
    let deadline = std::time::Instant::now() + timeout;
    let mut reader = BufReader::new(
        stream
            .try_clone()
            .map_err(|err| format!("failed to clone health stream: {err}"))?,
    );
    let mut line = Vec::new();
    const MAX_REQUEST_LINE: usize = 8192;
    loop {
        reader
            .get_mut()
            .set_read_timeout(Some(health_time_remaining(deadline)?))
            .map_err(|err| format!("failed to configure health read deadline: {err}"))?;
        let available = match reader.fill_buf() {
            Ok(available) => available,
            Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(err) => return Err(format!("failed to read daemon health request: {err}")),
        };
        if available.is_empty() {
            break;
        }
        let take = available
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(available.len(), |i| i + 1)
            .min(MAX_REQUEST_LINE + 1 - line.len());
        line.extend_from_slice(&available[..take]);
        reader.consume(take);
        if line.last() == Some(&b'\n') || line.len() > MAX_REQUEST_LINE {
            break;
        }
    }
    let response = if line.len() > MAX_REQUEST_LINE {
        http_response(414, "text/plain", "request line too long\n")
    } else if line.last() != Some(&b'\n') {
        http_response(400, "text/plain", "incomplete request line\n")
    } else {
        match std::str::from_utf8(&line) {
            Ok(line) => match parse_health_request_path(line) {
                Ok(endpoint) => endpoint_http_response(render_endpoint(snapshot, endpoint)?)?,
                Err(response) => response,
            },
            Err(_) => http_response(400, "text/plain", "invalid request encoding\n"),
        }
    };
    let mut remaining = response.as_bytes();
    while !remaining.is_empty() {
        stream
            .set_write_timeout(Some(health_time_remaining(deadline)?))
            .map_err(|err| format!("failed to configure health write deadline: {err}"))?;
        match stream.write(remaining) {
            Ok(0) => return Err("health response connection closed".into()),
            Ok(written) => remaining = &remaining[written..],
            Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(err) => return Err(format!("failed to write daemon health response: {err}")),
        }
    }
    Ok(())
}
fn health_time_remaining(deadline: std::time::Instant) -> Result<Duration, String> {
    deadline
        .checked_duration_since(std::time::Instant::now())
        .filter(|time| !time.is_zero())
        .ok_or_else(|| "health connection deadline elapsed".into())
}

fn endpoint_http_response(response: EndpointResponse) -> Result<String, String> {
    Ok(http_response(
        response.status,
        response.content_type,
        &response.body,
    ))
}

fn parse_health_request_path(line: &str) -> Result<HealthEndpoint, String> {
    let mut parts = line.split_whitespace();
    let method = parts
        .next()
        .ok_or_else(|| http_response(400, "text/plain", "malformed request\n"))?;
    let path = parts
        .next()
        .ok_or_else(|| http_response(400, "text/plain", "malformed request\n"))?;
    let version = parts
        .next()
        .ok_or_else(|| http_response(400, "text/plain", "malformed request\n"))?;
    if method != "GET" {
        return Err(http_response(405, "text/plain", "method not allowed\n"));
    }
    if !version.starts_with("HTTP/") {
        return Err(http_response(400, "text/plain", "malformed request\n"));
    }
    match path {
        "/livez" => Ok(HealthEndpoint::Livez),
        "/readyz" => Ok(HealthEndpoint::Readyz),
        "/healthz" => Ok(HealthEndpoint::Healthz),
        "/metrics" => Ok(HealthEndpoint::Metrics),
        _ => Err(http_response(404, "text/plain", "not found\n")),
    }
}

fn http_response(status: u16, content_type: &'static str, body: &str) -> String {
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        405 => "Method Not Allowed",
        414 => "URI Too Long",
        503 => "Service Unavailable",
        _ => "Internal Server Error",
    };
    format!(
        "HTTP/1.1 {status} {reason}\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    )
}

pub fn render_endpoint(
    snapshot: &DaemonSnapshot,
    endpoint: HealthEndpoint,
) -> Result<EndpointResponse, String> {
    let response = match endpoint {
        HealthEndpoint::Livez => EndpointResponse {
            status: 200,
            content_type: "text/plain",
            body: "alive\n".to_string(),
        },
        HealthEndpoint::Readyz => EndpointResponse {
            status: if snapshot.ready { 200 } else { 503 },
            content_type: "text/plain",
            body: format!("{}\n", snapshot.readiness),
        },
        HealthEndpoint::Healthz => EndpointResponse {
            status: if snapshot.ready { 200 } else { 503 },
            content_type: "application/json",
            body: format!(
                "{{\"profile\":\"{}\",\"ready\":{},\"required_voters\":{},\"required_storage_nodes\":{},\"required_gateway_nodes\":{},\"required_policy_regions\":{},\"cluster_epoch\":{},\"nodes\":{},\"promoted_voters\":{},\"storage_nodes\":{},\"gateway_nodes\":{},\"policy_regions\":{},\"policy_version\":{},\"policy_min_replicas\":{},\"policy_placement_groups\":{},\"protocol_placement_version\":{},\"protocol_placement_groups\":{},\"protocol_group_ids\":{},\"protocol_latest_version\":{},\"protocol_certificate_count\":{},\"protocol_max_log_index\":{},\"replication\":\"{}\",\"quorum_available\":{},\"replication_detail\":\"{}\",\"readiness\":\"{}\"}}\n",
                snapshot.profile.as_str(),
                snapshot.ready,
                snapshot.required_voters,
                snapshot.required_storage_nodes,
                snapshot.required_gateway_nodes,
                snapshot.required_policy_regions,
                snapshot.cluster_epoch,
                snapshot.nodes,
                snapshot.promoted_voters,
                snapshot.storage_nodes,
                snapshot.gateway_nodes,
                snapshot.policy_regions,
                snapshot.policy_version,
                snapshot.policy_min_replicas,
                snapshot.policy_placement_groups,
                snapshot.protocol_placement_version,
                snapshot.protocol_placement_groups,
                json_u64_array(&snapshot.protocol_group_ids),
                snapshot.protocol_latest_version,
                snapshot.protocol_certificate_count,
                snapshot.protocol_max_log_index,
                snapshot.replication.mode,
                snapshot.replication.quorum_available,
                json_escape(&snapshot.replication.detail),
                json_escape(&snapshot.readiness),
            ),
        },
        HealthEndpoint::Metrics => EndpointResponse {
            status: if snapshot.ready { 200 } else { 503 },
            content_type: "text/plain; version=0.0.4",
            body: build_metrics(snapshot)?
                .render_prometheus()
                .map_err(|err| err.message)?,
        },
    };
    Ok(response)
}

pub fn build_metrics(snapshot: &DaemonSnapshot) -> Result<ObservabilityRegistry, String> {
    let mut registry = ObservabilityRegistry::default();
    gauge(&mut registry, "rsf_daemon_ready", i64::from(snapshot.ready))?;
    gauge(
        &mut registry,
        "rsf_daemon_quorum_available",
        i64::from(snapshot.replication.quorum_available),
    )?;
    for (mode, metric) in [
        ("standalone", "rsf_daemon_replication_standalone"),
        ("network", "rsf_daemon_replication_network"),
        ("unavailable", "rsf_daemon_replication_unavailable"),
    ] {
        gauge(
            &mut registry,
            metric,
            i64::from(snapshot.replication.mode == mode),
        )?;
    }
    gauge(
        &mut registry,
        "rsf_daemon_profile_standalone",
        bool_gauge(snapshot.profile == OperatorProfile::Standalone),
    )?;
    gauge(
        &mut registry,
        "rsf_daemon_profile_regional",
        bool_gauge(snapshot.profile == OperatorProfile::Regional),
    )?;
    gauge(
        &mut registry,
        "rsf_daemon_profile_multi_region",
        bool_gauge(snapshot.profile == OperatorProfile::MultiRegion),
    )?;
    gauge_usize(
        &mut registry,
        "rsf_daemon_required_voters",
        snapshot.required_voters,
    )?;
    gauge_usize(
        &mut registry,
        "rsf_daemon_required_storage_nodes",
        snapshot.required_storage_nodes,
    )?;
    gauge_usize(
        &mut registry,
        "rsf_daemon_required_gateway_nodes",
        snapshot.required_gateway_nodes,
    )?;
    gauge_usize(
        &mut registry,
        "rsf_daemon_required_policy_regions",
        snapshot.required_policy_regions,
    )?;
    gauge_u64(
        &mut registry,
        "rsf_daemon_cluster_epoch",
        snapshot.cluster_epoch,
    )?;
    gauge_usize(&mut registry, "rsf_daemon_nodes", snapshot.nodes)?;
    gauge_usize(
        &mut registry,
        "rsf_daemon_promoted_voters",
        snapshot.promoted_voters,
    )?;
    gauge_usize(
        &mut registry,
        "rsf_daemon_storage_nodes",
        snapshot.storage_nodes,
    )?;
    gauge_usize(
        &mut registry,
        "rsf_daemon_gateway_nodes",
        snapshot.gateway_nodes,
    )?;
    gauge_usize(
        &mut registry,
        "rsf_daemon_policy_regions",
        snapshot.policy_regions,
    )?;
    gauge_u64(
        &mut registry,
        "rsf_daemon_policy_version",
        snapshot.policy_version,
    )?;
    gauge(
        &mut registry,
        "rsf_daemon_policy_min_replicas",
        i64::from(snapshot.policy_min_replicas),
    )?;
    gauge_u64(
        &mut registry,
        "rsf_daemon_policy_placement_groups",
        snapshot.policy_placement_groups,
    )?;
    gauge_u64(
        &mut registry,
        "rsf_daemon_protocol_placement_version",
        snapshot.protocol_placement_version,
    )?;
    gauge_usize(
        &mut registry,
        "rsf_daemon_protocol_placement_groups",
        snapshot.protocol_placement_groups,
    )?;
    gauge_u64(
        &mut registry,
        "rsf_daemon_protocol_latest_version",
        snapshot.protocol_latest_version,
    )?;
    gauge_usize(
        &mut registry,
        "rsf_daemon_protocol_certificate_count",
        snapshot.protocol_certificate_count,
    )?;
    gauge_u64(
        &mut registry,
        "rsf_daemon_protocol_max_log_index",
        snapshot.protocol_max_log_index,
    )?;
    for pg in &snapshot.protocol_group_ids {
        let group_metric = format!("rsf_daemon_protocol_pg_{pg}_present");
        registry
            .try_gauge(group_metric, 1)
            .map_err(|err| err.message)?;
    }
    Ok(registry)
}

#[cfg(test)]
pub fn load_snapshot(state: &Path) -> Result<DaemonSnapshot, String> {
    load_snapshot_for_profile(state, OperatorProfile::Standalone)
}

pub fn load_snapshot_for_profile(
    state: &Path,
    profile: OperatorProfile,
) -> Result<DaemonSnapshot, String> {
    let admin = AdminPlane::recover(state.join("admin")).map_err(|err| err.message)?;
    let policy = PolicyCatalog::open(state.join("policy")).map_err(|err| err.message)?;
    let requirements = profile.requirements();
    let membership = admin
        .membership_view()
        .ok_or_else(|| "daemon requires at least one promoted voter".to_string())?;
    let voters = membership.voter_nodes();
    let (router, replication) = crate::replication::observe(state, &admin)?;

    let storage_nodes = admin
        .cluster_map()
        .nodes
        .iter()
        .filter(|node| node.roles.contains(&NodeRole::Storage))
        .count();
    let gateway_nodes = admin
        .cluster_map()
        .nodes
        .iter()
        .filter(|node| {
            node.roles.contains(&NodeRole::QueryGateway)
                || node.roles.contains(&NodeRole::ObjectGateway)
        })
        .count();
    let promoted_voters = admin
        .records()
        .filter(|record| record.state == AdmissionState::Promoted)
        .count();
    let policy = policy.current();
    let policy_regions = policy.regions.len();
    let protocol_group_ids = router
        .placement_map()
        .groups
        .iter()
        .map(|pg| pg.0)
        .collect::<Vec<_>>();
    let protocol_groups = protocol_group_ids.len();
    let mut protocol_certificate_count = 0_usize;
    let mut protocol_max_log_index = LogIndex(0);
    for pg in &router.placement_map().groups {
        let certificates = router
            .recovered_certificates(*pg)
            .map_err(|err| err.message)?;
        protocol_certificate_count = protocol_certificate_count.saturating_add(certificates.len());
        if let Some(max_index) = certificates
            .iter()
            .map(|certificate| certificate.index)
            .max()
        {
            protocol_max_log_index = protocol_max_log_index.max(max_index);
        }
    }
    let membership_reconciled = crate::protocol_membership_matches(&router, &voters);
    let ready = replication.quorum_available
        && membership_reconciled
        && promoted_voters >= requirements.voters
        && storage_nodes >= requirements.storage_nodes
        && gateway_nodes >= requirements.gateway_nodes
        && policy_regions >= requirements.policy_regions
        && promoted_voters >= usize::from(policy.min_replicas)
        && storage_nodes >= usize::from(policy.min_replicas)
        && protocol_groups as u64 >= policy.placement_groups
        && router.policy().min_placement_groups <= policy.placement_groups;
    let readiness = if ready {
        format!(
            "ready: profile={} durable admin, policy, protocol router, storage, gateway roles and live primary quorum satisfy operator requirements",
            profile.as_str()
        )
    } else {
        format!(
            "degraded: profile={} voters={}/{} storage={}/{} gateway={}/{} regions={}/{} protocol_groups={} policy_groups={} membership_reconciled={} replication={} quorum_available={} min_replicas={} reason={}",
            profile.as_str(),
            promoted_voters,
            requirements.voters,
            storage_nodes,
            requirements.storage_nodes,
            gateway_nodes,
            requirements.gateway_nodes,
            policy_regions,
            requirements.policy_regions,
            protocol_groups,
            policy.placement_groups,
            membership_reconciled,
            replication.mode,
            replication.quorum_available,
            policy.min_replicas,
            replication.detail
        )
    };

    Ok(DaemonSnapshot {
        profile,
        required_voters: requirements.voters,
        required_storage_nodes: requirements.storage_nodes,
        required_gateway_nodes: requirements.gateway_nodes,
        required_policy_regions: requirements.policy_regions,
        cluster_epoch: admin.cluster_map().epoch.0,
        nodes: admin.cluster_map().nodes.len(),
        promoted_voters,
        storage_nodes,
        gateway_nodes,
        policy_regions,
        policy_version: policy.version,
        policy_min_replicas: policy.min_replicas,
        policy_placement_groups: policy.placement_groups,
        protocol_placement_version: router.placement_map().version,
        protocol_placement_groups: protocol_groups,
        protocol_group_ids,
        protocol_latest_version: router.latest_version().0,
        protocol_certificate_count,
        protocol_max_log_index: protocol_max_log_index.0,
        replication,
        ready,
        readiness,
    })
}

fn validate_config(config: &DaemonConfig) -> Result<(), String> {
    if config.tick.is_zero() {
        return Err("daemon tick interval must be greater than zero".to_string());
    }
    if config.max_consecutive_errors == 0 {
        return Err("daemon max consecutive errors must be greater than zero".to_string());
    }
    if !config.state.exists() {
        return Err(format!(
            "daemon state path does not exist: {}",
            config.state.display()
        ));
    }
    for child in ["admin", "policy"] {
        let path = config.state.join(child);
        if !path.exists() {
            return Err(format!(
                "daemon state path is missing required {child} directory: {}",
                path.display()
            ));
        }
    }
    Ok(())
}

fn emit_supervision_failure(
    out: &mut impl Write,
    tick: u64,
    failure: &SupervisionFailure,
) -> Result<(), String> {
    writeln!(
        out,
        "daemon supervision tick={} status=runtime-error consecutive_errors={} max_consecutive_errors={} action={} error={}",
        tick,
        failure.consecutive_errors,
        failure.max_consecutive_errors,
        if failure.should_stop { "stop" } else { "retry" },
        failure.error
    )
    .map_err(|err| err.to_string())?;
    out.flush().map_err(|err| err.to_string())
}

fn emit_snapshot(
    out: &mut impl Write,
    config: &DaemonConfig,
    tick: u64,
    snapshot: &DaemonSnapshot,
) -> Result<(), String> {
    writeln!(
        out,
        "daemon tick={} state={} readiness={} reason={}",
        tick,
        config.state.display(),
        if snapshot.ready { "ready" } else { "degraded" },
        snapshot.readiness
    )
    .map_err(|err| err.to_string())?;
    writeln!(
        out,
        "daemon profile={} required_voters={} required_storage_nodes={} required_gateway_nodes={} required_policy_regions={}",
        snapshot.profile.as_str(),
        snapshot.required_voters,
        snapshot.required_storage_nodes,
        snapshot.required_gateway_nodes,
        snapshot.required_policy_regions
    )
    .map_err(|err| err.to_string())?;
    writeln!(
        out,
        "daemon cluster epoch={} nodes={} voters={} storage_nodes={} gateway_nodes={} policy_regions={}",
        snapshot.cluster_epoch,
        snapshot.nodes,
        snapshot.promoted_voters,
        snapshot.storage_nodes,
        snapshot.gateway_nodes,
        snapshot.policy_regions
    )
    .map_err(|err| err.to_string())?;
    writeln!(
        out,
        "daemon policy version={} min_replicas={} placement_groups={}",
        snapshot.policy_version, snapshot.policy_min_replicas, snapshot.policy_placement_groups
    )
    .map_err(|err| err.to_string())?;
    writeln!(
        out,
        "daemon protocol placement_map_version={} placement_groups={} group_ids={} latest_version={} certificates={} max_log_index={}",
        snapshot.protocol_placement_version,
        snapshot.protocol_placement_groups,
        format_u64_list(&snapshot.protocol_group_ids),
        snapshot.protocol_latest_version,
        snapshot.protocol_certificate_count,
        snapshot.protocol_max_log_index
    )
    .map_err(|err| err.to_string())?;
    for endpoint in [
        HealthEndpoint::Livez,
        HealthEndpoint::Readyz,
        HealthEndpoint::Healthz,
        HealthEndpoint::Metrics,
    ] {
        let response = render_endpoint(snapshot, endpoint)?;
        writeln!(
            out,
            "daemon endpoint path={} status={} content_type={}",
            endpoint.path(),
            response.status,
            response.content_type
        )
        .map_err(|err| err.to_string())?;
    }
    out.flush().map_err(|err| err.to_string())
}

fn gauge(
    registry: &mut ObservabilityRegistry,
    name: &'static str,
    value: i64,
) -> Result<(), String> {
    registry.try_gauge(name, value).map_err(|err| err.message)
}

fn bool_gauge(value: bool) -> i64 {
    if value { 1 } else { 0 }
}

fn gauge_u64(
    registry: &mut ObservabilityRegistry,
    name: &'static str,
    value: u64,
) -> Result<(), String> {
    let value = i64::try_from(value)
        .map_err(|_| format!("metric {name} value does not fit in signed 64-bit gauge"))?;
    gauge(registry, name, value)
}

fn gauge_usize(
    registry: &mut ObservabilityRegistry,
    name: &'static str,
    value: usize,
) -> Result<(), String> {
    let value = i64::try_from(value)
        .map_err(|_| format!("metric {name} value does not fit in signed 64-bit gauge"))?;
    gauge(registry, name, value)
}

fn json_escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '"' => escaped.push_str("\\\""),
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            ch if ch.is_control() => escaped.push_str(&format!("\\u{:04x}", ch as u32)),
            ch => escaped.push(ch),
        }
    }
    escaped
}

fn json_u64_array(values: &[u64]) -> String {
    let mut out = String::from("[");
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        out.push_str(&value.to_string());
    }
    out.push(']');
    out
}

fn format_u64_list(values: &[u64]) -> String {
    if values.is_empty() {
        return "none".to_string();
    }
    values
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    #[test]
    fn accepted_nonblocking_health_socket_waits_for_the_request() {
        use std::io::Read;
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let mut client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (accepted, _) = listener.accept().unwrap();
        accepted.set_nonblocking(true).unwrap();
        let (send, receive) = std::sync::mpsc::channel();
        let server = std::thread::spawn(move || {
            send.send(()).unwrap();
            serve_http_stream(accepted, &test_snapshot())
        });
        receive.recv().unwrap();
        // Reproduce a connection accepted before its request data is available.
        std::thread::sleep(Duration::from_millis(40));
        client.write_all(b"GET /healthz HTTP/1.1\r\n\r\n").unwrap();
        let mut response = String::new();
        client.read_to_string(&mut response).unwrap();
        server.join().unwrap().unwrap();
        assert!(response.starts_with("HTTP/1.1 200 OK"));
    }

    #[test]
    fn health_connections_have_bounded_lines_and_idle_deadlines() {
        use std::io::Read;
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let mut client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (accepted, _) = listener.accept().unwrap();
        client.write_all(&vec![b'a'; 8193]).unwrap();
        let server = std::thread::spawn(move || serve_http_stream(accepted, &test_snapshot()));
        let mut response = String::new();
        client.read_to_string(&mut response).unwrap();
        server.join().unwrap().unwrap();
        assert!(response.starts_with("HTTP/1.1 414 URI Too Long"));

        let _idle = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (accepted, _) = listener.accept().unwrap();
        assert!(
            serve_http_stream_with_timeout(accepted, &test_snapshot(), Duration::from_millis(20))
                .is_err()
        );
        assert!(health_time_remaining(std::time::Instant::now()).is_err());
    }

    #[test]
    fn supervisor_resets_after_successful_tick() {
        let state = std::env::temp_dir().join("rsfd-supervisor-resets-after-success");
        let config = test_config(state);
        let mut supervisor = SupervisorState::new(config.max_consecutive_errors);

        let first = supervisor.record_error("first failure".to_string());
        assert_eq!(first.consecutive_errors, 1);
        assert!(!first.should_stop);

        supervisor.record_success();
        let second = supervisor.record_error("second failure".to_string());

        assert_eq!(second.consecutive_errors, 1);
        assert!(!second.should_stop);
    }

    #[test]
    fn supervised_loop_stops_after_consecutive_runtime_errors() {
        let state = std::env::temp_dir().join("rsfd-supervised-loop-stops");
        let config = DaemonConfig {
            state,
            once: false,
            tick: Duration::from_millis(1),
            listen: None,
            data_listen: None,
            profile: OperatorProfile::Standalone,
            max_consecutive_errors: 2,
        };
        let mut out = Vec::new();
        let sleeps = Cell::new(0_u32);

        let err = run_daemon_loop(
            &config,
            None,
            None,
            &mut out,
            |_state, _profile| Err("protocol router temporarily unavailable".to_string()),
            |_| sleeps.set(sleeps.get().saturating_add(1)),
        )
        .expect_err("supervisor stops at configured threshold");
        let output = String::from_utf8(out).expect("supervisor output is utf8");

        assert!(err.contains("stopped after 2 consecutive runtime errors"));
        assert_eq!(sleeps.get(), 1);
        assert!(output.contains("status=runtime-error consecutive_errors=1"));
        assert!(output.contains("action=retry"));
        assert!(output.contains("status=runtime-error consecutive_errors=2"));
        assert!(output.contains("action=stop"));
    }

    #[test]
    fn supervised_loop_clears_error_count_after_recovery() {
        let state = std::env::temp_dir().join("rsfd-supervised-loop-recovers");
        let config = DaemonConfig {
            state,
            once: false,
            tick: Duration::from_millis(1),
            listen: None,
            data_listen: None,
            profile: OperatorProfile::Standalone,
            max_consecutive_errors: 2,
        };
        let mut out = Vec::new();
        let calls = Cell::new(0_u32);

        let err = run_daemon_loop(
            &config,
            None,
            None,
            &mut out,
            |_state, _profile| {
                let call = calls.get().saturating_add(1);
                calls.set(call);
                match call {
                    1 => Err("first transient failure".to_string()),
                    2 => Ok(test_snapshot()),
                    _ => Err("second transient failure".to_string()),
                }
            },
            |_tick| {},
        )
        .expect_err("second post-recovery error eventually reaches threshold");
        let output = String::from_utf8(out).expect("supervisor output is utf8");

        assert!(err.contains("stopped after 2 consecutive runtime errors"));
        assert_eq!(calls.get(), 4);
        assert_eq!(output.matches("consecutive_errors=1").count(), 2);
        assert!(output.contains("daemon tick=1"));
        assert!(output.contains("readiness=ready"));
    }

    fn test_config(state: PathBuf) -> DaemonConfig {
        DaemonConfig {
            state,
            once: false,
            tick: Duration::from_millis(1),
            listen: None,
            data_listen: None,
            profile: OperatorProfile::Standalone,
            max_consecutive_errors: 2,
        }
    }

    fn test_snapshot() -> DaemonSnapshot {
        DaemonSnapshot {
            profile: OperatorProfile::Standalone,
            required_voters: 1,
            required_storage_nodes: 1,
            required_gateway_nodes: 1,
            required_policy_regions: 1,
            cluster_epoch: 1,
            nodes: 1,
            promoted_voters: 1,
            storage_nodes: 1,
            gateway_nodes: 1,
            policy_regions: 1,
            policy_version: 1,
            policy_min_replicas: 1,
            policy_placement_groups: 1,
            protocol_placement_version: 1,
            protocol_placement_groups: 1,
            protocol_group_ids: vec![1],
            protocol_latest_version: 1,
            protocol_certificate_count: 1,
            protocol_max_log_index: 1,
            replication: crate::replication::ReplicationStatus {
                mode: "standalone",
                quorum_available: true,
                detail: "test quorum".into(),
            },
            ready: true,
            readiness: "ready: test snapshot".to_string(),
        }
    }
}
