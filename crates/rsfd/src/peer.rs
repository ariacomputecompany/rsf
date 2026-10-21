use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::time::Duration;

use rsf_core::{ClusterId, Epoch, NodeId, PlacementGroupId};
use rsf_rpc::replication::{PeerEndpoint, PeerIdentity, PeerReplicaServer};
use rsf_vsr::DurableVsrCell;
use serde::Deserialize;

mod materializer;
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
}

struct PeerService {
    listener: TcpListener,
    server: PeerReplicaServer,
    _lock: rsf_storage::ExclusiveFileLock,
    materializer: ApplicationMaterializer,
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
        serde_json::json!({"event":"peer.ready","listen":service.listener.local_addr().map_err(|err|err.to_string())?.to_string()})
    );
    std::io::stdout().flush().map_err(|err| err.to_string())?;
    loop {
        let (socket, address) = match service.listener.accept() {
            Ok(connection) => connection,
            Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(err) => return Err(format!("peer accept failed: {err}")),
        };
        let materializer = &mut service.materializer;
        match service
            .server
            .serve_connection_with_apply(socket, &mut |entry| {
                if let Some(command) = decode_application_entry(entry)? {
                    materializer.apply(&command)?;
                }
                Ok(())
            }) {
            Ok(()) => println!(
                "{}",
                serde_json::json!({"event":"peer.rpc","outcome":"accepted","remote":address.to_string()})
            ),
            Err(reason) => eprintln!(
                "{}",
                serde_json::json!({"event":"peer.rpc","outcome":"rejected","remote":address.to_string(),"reason":reason})
            ),
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
        std::fs::create_dir_all(&state).map_err(|err| err.to_string())?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(state.join(".peer.lock"))
            .map_err(|err| err.to_string())?;
        let lock = rsf_storage::ExclusiveFileLock::acquire(lock)
            .map_err(|err| format!("peer state already has an owner: {err}"))?;
        bind_state(&state, &cluster, NodeId(config.node))?;
        let mut cells = BTreeMap::new();
        let mut learners = BTreeMap::new();
        let local = NodeId(config.node);
        for config in config.cells {
            let pg = PlacementGroupId(config.pg);
            if let Some(source) = config.learner_source {
                if state.join(format!("pg-{}", config.pg)).exists() {
                    return Err("learner cannot replace an existing voting store".into());
                }
                learners.insert(
                    pg,
                    rsf_vsr::LearnerHistoryStore::open(
                        state.join(format!("learner-{}", config.pg)),
                        pg,
                        local,
                        NodeId(source),
                    )
                    .map_err(|err| err.message)?,
                );
                continue;
            }
            if state.join(format!("learner-{}", config.pg)).exists() {
                return Err("learner promotion requires certified membership installation".into());
            }
            cells.insert(
                pg,
                DurableVsrCell::open(
                    state.join(format!("pg-{}", config.pg)),
                    pg,
                    Epoch(config.view),
                    NodeId(config.primary),
                    config.voters.into_iter().map(NodeId),
                )
                .map_err(|err| err.message)?,
            );
        }
        let server = PeerReplicaServer::with_learners(
            NodeId(config.node),
            &cluster,
            identity,
            peers,
            cells,
            learners,
            timeout,
        )?;
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
        let listener = TcpListener::bind(config.listen)
            .map_err(|err| format!("cannot bind peer listener: {err}"))?;
        Ok(Self {
            listener,
            server,
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
    let identity = serde_json::json!({"version":1,"cluster":cluster.as_str(),"node":node.0});
    let path = root.join("IDENTITY.json");
    match std::fs::read(&path) {
        Ok(bytes) => {
            let stored: serde_json::Value =
                serde_json::from_slice(&bytes).map_err(|_| "peer state identity is corrupt")?;
            if stored != identity {
                return Err("peer state belongs to a different cluster or node".into());
            }
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            for entry in std::fs::read_dir(root).map_err(|err| err.to_string())? {
                let name = entry.map_err(|err| err.to_string())?.file_name();
                if name != ".peer.lock" && name != "IDENTITY.pending" {
                    return Err(
                        "unbound peer state contains existing data; migrate it explicitly".into(),
                    );
                }
            }
            let temp = root.join("IDENTITY.pending");
            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&temp)
                .map_err(|err| err.to_string())?;
            file.write_all(&serde_json::to_vec(&identity).map_err(|err| err.to_string())?)
                .map_err(|err| err.to_string())?;
            file.sync_all().map_err(|err| err.to_string())?;
            std::fs::rename(&temp, &path).map_err(|err| err.to_string())?;
            File::open(root)
                .and_then(|directory| directory.sync_all())
                .map_err(|err| err.to_string())?;
        }
        Err(err) => return Err(format!("cannot read peer state identity: {err}")),
    }
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
