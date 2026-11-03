use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::time::Duration;

use rsf_core::{ClusterId, Epoch, NodeId, PlacementGroupId};
use rsf_rpc::replication::{PeerElectionTiming, PeerEndpoint, PeerIdentity, PeerReplicaServer};
use rsf_vsr::DurableVsrCell;
use serde::Deserialize;

mod client;
mod materializer;
mod membership;
mod role;
mod state_identity;
use materializer::ApplicationMaterializer;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PeerConfig {
    cluster: String,
    node: u64,
    state: PathBuf,
    tls: PathBuf,
    listen: SocketAddr,
    timeout_ms: u64,
    peers: Vec<NodeConfig>,
    cells: Vec<CellConfig>,
    #[serde(default)]
    elections: Option<ElectionConfig>,
    #[serde(default)]
    client: Option<client::ClientConfig>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NodeConfig {
    node: u64,
    address: SocketAddr,
    server_name: String,
    fingerprint: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CellConfig {
    pg: u64,
    view: u64,
    primary: u64,
    voters: Vec<u64>,
    #[serde(default)]
    learner_source: Option<u64>,
    #[serde(default)]
    membership_recovery: Option<membership::RecoveryConfig>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ElectionConfig {
    heartbeat_ms: u64,
    timeout_ms: u64,
    rank_delay_ms: u64,
}
impl ElectionConfig {
    fn timing(&self) -> PeerElectionTiming {
        PeerElectionTiming {
            heartbeat: Duration::from_millis(self.heartbeat_ms),
            timeout: Duration::from_millis(self.timeout_ms),
            rank_delay: Duration::from_millis(self.rank_delay_ms),
        }
    }
}

struct PeerService {
    local: NodeId,
    listener: TcpListener,
    server: PeerReplicaServer,
    _lock: rsf_storage::ExclusiveFileLock,
    materializer: ApplicationMaterializer,
    elections: Option<PeerElectionTiming>,
    last_elections: BTreeMap<u64, (u64, u64, bool, String)>,
    last_membership: BTreeMap<u64, serde_json::Value>,
    poll_interval: Option<Duration>,
    client: Option<client::ClientService>,
}

pub fn run_cli(args: &[String]) -> Result<(), String> {
    if args.first().is_some_and(|arg| arg == "sync-learner") {
        return sync_learner_cli(args);
    }
    let [serve, flag, path] = args else {
        return Err("usage: rsfd peer serve --config <peer.json>".into());
    };
    if serve != "serve" || flag != "--config" {
        return Err("usage: rsfd peer serve --config <peer.json>".into());
    }
    let mut service = PeerService::open(Path::new(path))?;
    println!(
        "{}",
        serde_json::json!({"event":"peer.ready","listen":service.listener.local_addr().map_err(|err|err.to_string())?.to_string(),"client_listen":service.client.as_ref().map(|client|client.local_addr().to_string())})
    );
    std::io::stdout().flush().map_err(|err| err.to_string())?;
    loop {
        let started = std::time::Instant::now();
        for _ in 0..64 {
            let (socket, address) = match service.listener.accept() {
                Ok(connection) => connection,
                Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(err) => return Err(format!("peer accept failed: {err}")),
            };
            let materializer = &mut service.materializer;
            let rpc_started = std::time::Instant::now();
            let mut materialize_time = Duration::ZERO;
            let mut materialize_count = 0_u64;
            let mut last_entry = None;
            let result = service
                .server
                .serve_connection_with_apply(socket, &mut |entry| {
                    let apply_started = std::time::Instant::now();
                    let result = (|| {
                        if let Some(command) = decode_application_entry(entry)? {
                            materializer.apply(&command)?;
                        }
                        Ok(())
                    })();
                    materialize_time += apply_started.elapsed();
                    materialize_count += 1;
                    last_entry = Some((entry.prepare.pg.0, entry.prepare.index.0));
                    result
                });
            let event = serde_json::json!({
                "event":"peer.rpc", "node":service.local.0,
                "outcome":if result.is_ok() { "accepted" } else { "rejected" },
                "remote":address.to_string(), "elapsed_ms":rpc_started.elapsed().as_millis(),
                "materialize_ms":materialize_time.as_millis(), "materialize_count":materialize_count,
                "last_entry":last_entry, "reason":result.as_ref().err()
            });
            if result.is_ok() {
                println!("{event}");
            } else {
                eprintln!("{event}");
            }
            if service.poll_interval.is_none()
                || service
                    .poll_interval
                    .is_some_and(|interval| started.elapsed() >= interval / 2)
            {
                break;
            }
        }
        if service.server.membership_recovery_enabled() {
            let materializer = &mut service.materializer;
            if let Some(update) = service.server.poll_membership_recovery(&mut |entry| {
                if let Some(command) = decode_application_entry(entry)? {
                    materializer.apply(&command)?;
                }
                Ok(())
            })? {
                let state = serde_json::json!({
                    "event": "peer.membership", "pg": update.pg.0,
                    "plan": update.plan.map(|id| id.iter().map(|byte| format!("{byte:02x}")).collect::<String>()),
                    "phase": update.phase, "replica": update.replica.map(|node| node.0),
                    "publication_quorum": update.publication_quorum.is_some(),
                    "detail": update.detail,
                });
                if service.last_membership.get(&update.pg.0) != Some(&state) {
                    println!("{state}");
                    service.last_membership.insert(update.pg.0, state);
                }
            }
        }
        if let Some(timing) = service.elections {
            let materializer = &mut service.materializer;
            if let Some(update) = service.server.poll_elections_with_progress(timing, &mut |entry| {
                if let Some(command) = decode_application_entry(entry)? {
                    materializer.apply(&command)?;
                }
                Ok(())
            }, &mut |progress| {
                println!("{}", serde_json::json!({"event":"peer.control", "node":progress.node.0, "pg":progress.pg.0, "phase":progress.phase, "elapsed_ms":progress.elapsed.as_millis(), "records":progress.records}));
                let _ = std::io::stdout().flush();
            })? {
                let state = (
                    update.view.0,
                    update.primary.0,
                    update.quorum_confirmed,
                    update.detail.clone(),
                );
                if service.last_elections.get(&update.pg.0) != Some(&state) {
                    println!(
                        "{}",
                        serde_json::json!({"event":"peer.election","pg":update.pg.0,"view":update.view.0,"primary":update.primary.0,"quorum_confirmed":update.quorum_confirmed,"detail":update.detail})
                    );
                    service.last_elections.insert(update.pg.0, state);
                }
            }
        }
        if let Some(client) = &service.client {
            client.serve_one(&mut service.server, &mut service.materializer)?;
        }
        if let Some(interval) = service.poll_interval {
            std::thread::sleep(interval.min(Duration::from_millis(10)));
        }
    }
}

struct LoadedPeerConfig {
    config: PeerConfig,
    cluster: ClusterId,
    state: PathBuf,
    identity: PeerIdentity,
    peers: BTreeMap<NodeId, PeerEndpoint>,
}
fn load_config(path: &Path) -> Result<LoadedPeerConfig, String> {
    let raw = std::fs::read(path).map_err(|err| format!("cannot read peer config: {err}"))?;
    if raw.len() > 1024 * 1024 {
        return Err("peer configuration exceeds 1 MiB".into());
    }
    let config: PeerConfig =
        serde_json::from_slice(&raw).map_err(|err| format!("invalid peer config: {err}"))?;
    let cluster = ClusterId::new(&config.cluster).map_err(|err| err.message)?;
    let base = path.parent().unwrap_or(Path::new("."));
    let resolve = |path: &Path| {
        if path.is_absolute() {
            path.to_owned()
        } else {
            base.join(path)
        }
    };
    let state = resolve(&config.state);
    let identity = PeerIdentity::load(&resolve(&config.tls))?;
    let mut peers = BTreeMap::new();
    for peer in &config.peers {
        let bytes = decode_fingerprint(&peer.fingerprint)?;
        if peers
            .insert(
                NodeId(peer.node),
                PeerEndpoint {
                    address: peer.address,
                    server_name: peer.server_name.clone(),
                    fingerprint: bytes,
                },
            )
            .is_some()
        {
            return Err("duplicate peer node".into());
        }
    }
    let mut groups = BTreeSet::new();
    for cell in &config.cells {
        let voters = cell.voters.iter().copied().collect::<BTreeSet<_>>();
        if !groups.insert(cell.pg)
            || cell.view == 0
            || voters.len() != cell.voters.len()
            || voters.len() > 4096
            || match cell.learner_source {
                None => !voters.contains(&config.node),
                Some(source) => {
                    voters.contains(&config.node)
                        || source != cell.primary
                        || source == config.node
                        || !peers.contains_key(&NodeId(source))
                }
            }
            || !voters.contains(&cell.primary)
            || voters
                .iter()
                .any(|node| !peers.contains_key(&NodeId(*node)))
        {
            return Err("invalid peer cell: require unique groups/voters, positive view, and mapped local/primary membership".into());
        }
    }
    // Validate identities and timeouts before opening or creating stores.
    let timeout = Duration::from_millis(config.timeout_ms);
    let _ = rsf_rpc::replication::NetworkVsrTransport::new(
        NodeId(config.node),
        &cluster,
        identity.clone(),
        peers.clone(),
        timeout,
    )?;
    let membership = membership::configured(&config.cells)?;
    if config.client.is_some() && !membership.is_empty() {
        return Err(
            "managed membership recovery cannot admit clients before replicated control admission"
                .into(),
        );
    }
    if let Some(elections) = &config.elections {
        if membership.is_empty()
            && config
                .cells
                .iter()
                .all(|cell| cell.learner_source.is_some())
        {
            return Err("automatic election requires a hosted voting cell".into());
        }
        if !membership.is_empty() {
            // A managed learner may become a voter without a configuration edit.
            // Every future voter must already have a mapped TLS identity.
            validate_cell_timing(elections.timing(), timeout, peers.len(), false)?;
        }
        for cell in config
            .cells
            .iter()
            .filter(|cell| cell.learner_source.is_none())
        {
            let timing = elections.timing();
            validate_cell_timing(timing, timeout, cell.voters.len(), config.client.is_some())?;
        }
    }
    if config.client.is_some()
        && (config.elections.is_none()
            || config
                .cells
                .iter()
                .any(|cell| cell.learner_source.is_some()))
    {
        return Err(
            "peer client service requires automatic elections and hosted voting cells only".into(),
        );
    }
    if config.cells.is_empty() {
        return Err("peer server requires at least one cell".into());
    }
    Ok(LoadedPeerConfig {
        config,
        cluster,
        state,
        identity,
        peers,
    })
}

fn validate_cell_timing(
    timing: PeerElectionTiming,
    timeout: Duration,
    voters: usize,
    client: bool,
) -> Result<(), String> {
    timing.validate(timeout, voters)?;
    if client {
        let minimum = timeout
            .saturating_mul((voters.saturating_sub(1) as u32).saturating_mul(2))
            .saturating_add(timing.heartbeat.saturating_mul(3))
            .saturating_add(crate::data::DATA_CONNECTION_TIMEOUT);
        if timing.timeout < minimum {
            return Err(
                "client election timeout must also cover the cumulative data I/O deadline".into(),
            );
        }
    }
    Ok(())
}

impl PeerService {
    fn open(path: &Path) -> Result<Self, String> {
        let LoadedPeerConfig {
            config,
            cluster,
            state,
            identity,
            peers,
        } = load_config(path)?;
        let timeout = Duration::from_millis(config.timeout_ms);
        let elections = config.elections.as_ref().map(ElectionConfig::timing);
        std::fs::create_dir_all(&state).map_err(|err| err.to_string())?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(state.join(".peer.lock"))
            .map_err(|err| err.to_string())?;
        let lock = rsf_storage::ExclusiveFileLock::acquire(lock)
            .map_err(|err| format!("peer state already has an owner: {err}"))?;
        let mut state_identity =
            state_identity::StateIdentity::bind(&state, &cluster, NodeId(config.node))?;
        let membership = membership::configured(&config.cells)?;
        let managed = membership.keys().copied().collect::<BTreeSet<_>>();
        membership::preflight(
            &state,
            &managed,
            &state_identity.managed,
            config.client.is_some(),
        )?;
        let poll_interval = elections
            .map(|timing| timing.heartbeat)
            .into_iter()
            .chain(membership.values().map(|timing| timing.interval))
            .min();
        let mut cells = BTreeMap::new();
        let mut learners = BTreeMap::new();
        let local = NodeId(config.node);
        let mut promotion_paths = BTreeMap::new();
        for config in config.cells {
            let pg = PlacementGroupId(config.pg);
            let configured_learner = config.learner_source.is_some();
            let role = if managed.contains(&pg) {
                role::open_existing(&state, local, config)?
            } else {
                role::open(&state, local, config)?
            };
            match role {
                role::HostedRole::Voter(cell) => {
                    if configured_learner && elections.is_some() && !managed.contains(&pg) {
                        return Err(
                            "promoted voter elections require an explicit voting configuration"
                                .into(),
                        );
                    }
                    if managed.contains(&pg)
                        && cell.promoted_replica().map_err(|error| error.message)? == Some(local)
                    {
                        promotion_paths.insert(pg, state.join(format!("pg-{}", pg.0)));
                    }
                    cells.insert(pg, *cell);
                }
                role::HostedRole::Learner(learner) => {
                    if managed.contains(&pg) {
                        promotion_paths.insert(pg, state.join(format!("pg-{}", pg.0)));
                    }
                    learners.insert(pg, *learner);
                }
            }
        }
        // Recovered membership can be larger than the bootstrap configuration.
        // Validate its actual network and client I/O budgets before listening.
        if let Some(timing) = elections {
            for cell in cells.values() {
                validate_cell_timing(
                    timing,
                    timeout,
                    cell.replica_count(),
                    config.client.is_some(),
                )?;
            }
        }
        let server = PeerReplicaServer::with_learners(
            NodeId(config.node),
            &cluster,
            identity,
            peers.clone(),
            cells,
            learners,
            timeout,
        )?;
        let server = if managed.is_empty() {
            server
        } else {
            server
                .with_existing_membership_paths(membership::paths(&state, &managed))?
                .with_promotion_paths(promotion_paths)?
                .with_membership_recovery(membership)?
        };
        // The recovery hook proves existing bound custody before stale learner
        // declarations can participate in elections. Enrollment is monotonic
        // admission policy, never a publication-completion or voting grant.
        state_identity.enroll(&state, &managed)?;
        let mut materializer = ApplicationMaterializer::new(state.join("application"));
        let mut recovered = Vec::new();
        for entry in server.committed_entries()? {
            if let Some(command) = decode_application_entry(&entry)? {
                recovered.push(command);
            }
        }
        recovered.sort_by_key(|command| command.commit.version);
        for command in recovered {
            materializer.apply(&command)?;
        }
        if config.client.is_some() && server.membership_admission_required()? {
            return Err(
                "membership publication requires replicated control admission before serving clients"
                    .into(),
            );
        }
        let client = config
            .client
            .as_ref()
            .map(|client| {
                client::ClientService::open(
                    client,
                    path.parent().unwrap_or(Path::new(".")),
                    &state,
                    local,
                    &peers,
                )
            })
            .transpose()?;
        let listener = TcpListener::bind(config.listen)
            .map_err(|err| format!("cannot bind peer listener {}: {err}", config.listen))?;
        if poll_interval.is_some() {
            listener
                .set_nonblocking(true)
                .map_err(|err| err.to_string())?;
        }
        Ok(Self {
            local,
            listener,
            server,
            elections,
            last_elections: BTreeMap::new(),
            last_membership: BTreeMap::new(),
            poll_interval,
            client,
            _lock: lock,
            materializer,
        })
    }
}

fn decode_application_entry(
    entry: &rsf_vsr::VsrCommittedEntry,
) -> Result<Option<rsf_protocol::ReplicatedFabricCommand>, String> {
    let rsf_vsr::VsrOperation::ReplicatedCommand {
        operation_id,
        payload,
    } = &entry.prepare.operation
    else {
        return Ok(None);
    };
    // Raw pre-RSFC1 replication entries have no surface decoding contract.
    if !payload.starts_with(b"RSFC1") {
        return Ok(None);
    }
    let command =
        rsf_protocol::ReplicatedFabricCommand::decode(payload).map_err(|err| err.message)?;
    if command.commit.pg != entry.prepare.pg
        || command.commit.index != entry.prepare.index
        || command.commit.operation_id != *operation_id
    {
        return Err("application command does not match committed VSR entry".into());
    }
    Ok(Some(command))
}
pub(crate) fn decode_fingerprint(value: &str) -> Result<[u8; 32], String> {
    if value.len() != 64 || !value.is_ascii() {
        return Err("peer fingerprint must contain 64 hexadecimal digits".into());
    }
    let mut result = [0; 32];
    for (index, byte) in result.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|_| "peer fingerprint must contain 64 hexadecimal digits")?;
    }
    Ok(result)
}

pub(crate) fn bind_state(root: &Path, cluster: &ClusterId, node: NodeId) -> Result<(), String> {
    state_identity::StateIdentity::bind(root, cluster, node).map(|_| ())
}

fn sync_learner_cli(args: &[String]) -> Result<(), String> {
    let usage =
        "usage: rsfd peer sync-learner --config <source-peer.json> --pg <id> --target <node>";
    let [_, config_flag, path, pg_flag, pg, target_flag, target] = args else {
        return Err(usage.into());
    };
    if config_flag != "--config" || pg_flag != "--pg" || target_flag != "--target" {
        return Err(usage.into());
    }
    let pg = pg.parse::<u64>().map_err(|_| usage.to_string())?;
    let target = target
        .parse::<u64>()
        .map(NodeId)
        .map_err(|_| usage.to_string())?;
    let LoadedPeerConfig {
        config,
        cluster,
        state,
        identity,
        peers,
    } = load_config(Path::new(path))?;
    let specification = config
        .cells
        .iter()
        .find(|cell| cell.pg == pg)
        .ok_or("source config does not host this placement group")?;
    if specification.learner_source.is_some() || specification.primary != config.node {
        return Err("history source must be configured as primary voter".into());
    }
    let directory = state.join(format!("pg-{pg}"));
    let mut source = DurableVsrCell::open_existing(directory, PlacementGroupId(pg))
        .map_err(|err| err.message)?;
    let mut network = rsf_rpc::replication::NetworkVsrTransport::new(
        NodeId(config.node),
        &cluster,
        identity,
        peers,
        Duration::from_millis(config.timeout_ms),
    )?;
    let tail = network
        .sync_learner(target, &mut source)
        .map_err(|err| err.reason)?;
    println!(
        "{}",
        serde_json::json!({"event":"peer.learner-synced","pg":pg,"target":target.0,"committed_tail":tail.0,"role":"learner"})
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn state_identity_cannot_move_to_another_node_or_cluster() {
        let root = std::env::temp_dir().join(format!(
            "rsf-peer-identity-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let cluster = ClusterId::new("alpha").unwrap();
        bind_state(&root, &cluster, NodeId(1)).unwrap();
        bind_state(&root, &cluster, NodeId(1)).unwrap();
        assert!(bind_state(&root, &cluster, NodeId(2)).is_err());
        assert!(bind_state(&root, &ClusterId::new("beta").unwrap(), NodeId(1)).is_err());
        std::fs::write(root.join("IDENTITY.json"), b"torn").unwrap();
        assert!(bind_state(&root, &cluster, NodeId(1)).is_err());
        std::fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn peer_configuration_rejects_ambiguous_fields_and_fingerprints() {
        assert!(serde_json::from_str::<NodeConfig>(r#"{"node":1,"address":"127.0.0.1:1","server_name":"localhost","fingerprint":"ab","extra":true}"#).is_err());
        assert!(decode_fingerprint("ab").is_err());
        assert!(decode_fingerprint(&"z".repeat(64)).is_err());
        assert_eq!(decode_fingerprint(&"ab".repeat(32)).unwrap(), [0xab; 32]);
    }
}
