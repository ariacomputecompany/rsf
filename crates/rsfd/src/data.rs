mod transport;
type DataRouter = VsrBackedFabricRouter<transport::DataTransport>;

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::time::Duration;

use rsf_admin::AdminPlane;
use rsf_blob::DurableBlobEngine;
use rsf_core::{Epoch, ErrorCode, PlacementGroupId, RsfError, RsfResult, blob::ContentId};
use rsf_kv::{DurableKvEngine, KvBatch, KvMutation};
use rsf_object::DurableObjectStore;
use rsf_protocol::{ClientRequestId, VsrBackedFabricRouter};
use rsf_sql::{DurableSqlEngine, SqlMutationEvidence, SqlOutcome};
use rsf_wire::{
    RequestEnvelope, WireAdapter, WireCodec, WireDecision, WireOperationKind, WireProtocol,
};

const MAX_DATA_FRAME_BYTES: u32 = 64 * 1024;

pub struct DataPlaneListener {
    listener: TcpListener,
    codec: WireCodec,
    adapter: WireAdapter,
    executor: DataPlaneExecutor,
    tls: std::sync::Arc<rustls::ServerConfig>,
}

impl DataPlaneListener {
    pub fn bind(addr: SocketAddr, state: impl Into<PathBuf>) -> Result<Self, String> {
        let state = state.into();
        let tls = crate::tls::server_config(&state)?;
        let listener = TcpListener::bind(addr)
            .map_err(|err| format!("failed to bind daemon data listener {addr}: {err}"))?;
        listener
            .set_nonblocking(true)
            .map_err(|err| format!("failed to configure daemon data listener {addr}: {err}"))?;
        Ok(Self {
            listener,
            codec: WireCodec,
            adapter: WireAdapter,
            executor: DataPlaneExecutor::new(state),
            tls,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.listener
            .local_addr()
            .expect("bound data listener exposes local address")
    }

    pub fn serve_pending(&self, out: &mut impl Write) -> Result<usize, String> {
        let mut served = 0_usize;
        loop {
            match self.listener.accept() {
                Ok((stream, peer)) => {
                    let response = serve_data_stream(
                        stream,
                        &self.codec,
                        &self.adapter,
                        &self.executor,
                        self.tls.clone(),
                    )
                    .unwrap_or_else(|err| DataPlaneResponse::Rejected {
                        protocol: "tls",
                        reason: format!("secure connection failed: {err}"),
                    });
                    emit_data_plane_response(out, peer, &response)?;
                    served = served.saturating_add(1);
                }
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => return Ok(served),
                Err(err) => return Err(format!("failed to accept daemon data request: {err}")),
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DataPlaneResponse {
    Executed {
        protocol: &'static str,
        operation_kind: &'static str,
        surface: &'static str,
        version: Option<u64>,
        bytes: u64,
        payload: Vec<u8>,
        detail: String,
    },
    Rejected {
        protocol: &'static str,
        reason: String,
    },
    Corrupt {
        reason: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataPlaneExecutor {
    state: PathBuf,
}

impl DataPlaneExecutor {
    pub fn new(state: impl Into<PathBuf>) -> Self {
        Self {
            state: state.into(),
        }
    }

    fn execute(&self, envelope: RequestEnvelope, fingerprint: &[u8; 32]) -> DataPlaneResponse {
        if let Err(err) = crate::security::authorize(&self.state, fingerprint, &envelope) {
            return DataPlaneResponse::Rejected {
                protocol: envelope.protocol.as_str(),
                reason: err.message,
            };
        }
        match envelope.protocol {
            WireProtocol::Sql => {
                self.execute_sql(envelope)
                    .unwrap_or_else(|err| DataPlaneResponse::Rejected {
                        protocol: WireProtocol::Sql.as_str(),
                        reason: err.message,
                    })
            }
            WireProtocol::Kv => {
                self.execute_kv(envelope)
                    .unwrap_or_else(|err| DataPlaneResponse::Rejected {
                        protocol: WireProtocol::Kv.as_str(),
                        reason: err.message,
                    })
            }
            WireProtocol::Blob => {
                self.execute_blob(envelope)
                    .unwrap_or_else(|err| DataPlaneResponse::Rejected {
                        protocol: WireProtocol::Blob.as_str(),
                        reason: err.message,
                    })
            }
            WireProtocol::Object => {
                self.execute_object(envelope)
                    .unwrap_or_else(|err| DataPlaneResponse::Rejected {
                        protocol: WireProtocol::Object.as_str(),
                        reason: err.message,
                    })
            }
            protocol => DataPlaneResponse::Rejected {
                protocol: protocol.as_str(),
                reason: "data-plane execution handoff is currently available for sql, kv, blob, and object requests"
                    .to_string(),
            },
        }
    }

    fn execute_sql(&self, envelope: RequestEnvelope) -> RsfResult<DataPlaneResponse> {
        ensure_empty_body(&envelope.body, "sql")?;
        let request_client = sql_request_client_from_envelope(&envelope)?;
        let mut runtime = self.open_sql_runtime(
            envelope
                .namespace
                .as_deref()
                .ok_or_else(|| invalid("namespace is required"))?,
        )?;
        if envelope.operation_kind == WireOperationKind::Read
            && runtime.router.transport_mut().is_network()
        {
            runtime.router.confirm_read_quorum()?;
        }
        let execution = runtime.sql.execute(
            &mut runtime.router,
            PlacementGroupId(1),
            &envelope.operation,
            request_client,
        )?;
        let payload = match &execution.outcome {
            SqlOutcome::Selected { rows } => {
                serde_json::to_vec(&rows.iter().map(|row| &row.columns).collect::<Vec<_>>())
                    .map_err(|err| invalid(format!("failed to encode SQL result: {err}")))?
            }
            _ => Vec::new(),
        };
        Ok(DataPlaneResponse::Executed {
            protocol: envelope.protocol.as_str(),
            operation_kind: operation_kind_label(envelope.operation_kind),
            surface: "sql",
            version: execution.version.map(|version| version.0),
            bytes: payload.len() as u64,
            payload,
            detail: format!(
                "{} replayed={}",
                sql_outcome_detail(&execution.outcome),
                execution.replayed
            ),
        })
    }

    fn execute_kv(&self, envelope: RequestEnvelope) -> RsfResult<DataPlaneResponse> {
        let command = parse_kv_command(&envelope.operation, &envelope.body)?;
        let mut runtime = self.open_kv_runtime(
            envelope
                .namespace
                .as_deref()
                .ok_or_else(|| invalid("namespace is required"))?,
        )?;
        if matches!(&command, KvCommand::Get { .. }) && runtime.router.transport_mut().is_network()
        {
            runtime.router.confirm_read_quorum()?;
        }
        match command {
            KvCommand::Get { key } => {
                let value = runtime.kv.get(key.as_bytes());
                Ok(DataPlaneResponse::Executed {
                    protocol: envelope.protocol.as_str(),
                    operation_kind: operation_kind_label(envelope.operation_kind),
                    surface: "kv",
                    version: value.map(|value| value.version.0),
                    bytes: value.map_or(0, |value| value.value.len() as u64),
                    payload: value.map_or_else(Vec::new, |value| value.value.clone()),
                    detail: match value {
                        Some(value) => format!(
                            "hit key={} version={} deleted={}",
                            key, value.version.0, value.deleted
                        ),
                        None => format!("miss key={key}"),
                    },
                })
            }
            KvCommand::Set { key, value } => {
                let request = client_request_from_envelope(&envelope)?;
                let snapshot = runtime.kv.snapshot(&runtime.router);
                let result = runtime.kv.commit_batch_routed(
                    &mut runtime.router,
                    KvBatch {
                        client_request: Some(request),
                        snapshot,
                        mutations: vec![KvMutation::Set {
                            key: key.as_bytes().to_vec(),
                            value: value.clone(),
                        }],
                    },
                )?;
                Ok(DataPlaneResponse::Executed {
                    protocol: envelope.protocol.as_str(),
                    operation_kind: operation_kind_label(envelope.operation_kind),
                    surface: "kv",
                    version: Some(result.version.0),
                    bytes: value.len() as u64,
                    payload: Vec::new(),
                    detail: format!(
                        "set key={} replayed={} placement_group={} log_index={}",
                        key,
                        result.replayed,
                        result.evidence.placement_group.0,
                        result.evidence.log_index.0
                    ),
                })
            }
            KvCommand::Clear { key } => {
                let request = client_request_from_envelope(&envelope)?;
                let snapshot = runtime.kv.snapshot(&runtime.router);
                let result = runtime.kv.commit_batch_routed(
                    &mut runtime.router,
                    KvBatch {
                        client_request: Some(request),
                        snapshot,
                        mutations: vec![KvMutation::Clear {
                            key: key.as_bytes().to_vec(),
                        }],
                    },
                )?;
                Ok(DataPlaneResponse::Executed {
                    protocol: envelope.protocol.as_str(),
                    operation_kind: operation_kind_label(envelope.operation_kind),
                    surface: "kv",
                    version: Some(result.version.0),
                    bytes: 0,
                    payload: Vec::new(),
                    detail: format!(
                        "clear key={} replayed={} placement_group={} log_index={}",
                        key,
                        result.replayed,
                        result.evidence.placement_group.0,
                        result.evidence.log_index.0
                    ),
                })
            }
        }
    }

    fn execute_blob(&self, envelope: RequestEnvelope) -> RsfResult<DataPlaneResponse> {
        let command = parse_blob_command(&envelope.operation, &envelope.body)?;
        let mut runtime = self.open_blob_runtime(
            envelope
                .namespace
                .as_deref()
                .ok_or_else(|| invalid("namespace is required"))?,
        )?;
        match command {
            BlobCommand::Put { name: _, bytes } => {
                let record = runtime.blobs.put_request(
                    &mut runtime.router,
                    PlacementGroupId(1),
                    client_request_from_envelope_for_surface(&envelope, "blob")?,
                    bytes,
                )?;
                Ok(DataPlaneResponse::Executed {
                    protocol: envelope.protocol.as_str(),
                    operation_kind: operation_kind_label(envelope.operation_kind),
                    surface: "blob",
                    version: Some(record.pin_version.0),
                    bytes: record.bytes,
                    payload: Vec::new(),
                    detail: format!(
                        "put id=blake3:{} deduped={} pins={} placement_group={} log_index={}",
                        hex_bytes(record.id.as_bytes()),
                        record.deduped,
                        record.pins,
                        record.pin_evidence.placement_group.0,
                        record.pin_evidence.log_index.0
                    ),
                })
            }
            BlobCommand::Get { id } => {
                let bytes = runtime.blobs.read_verified(&id)?;
                Ok(DataPlaneResponse::Executed {
                    protocol: envelope.protocol.as_str(),
                    operation_kind: operation_kind_label(envelope.operation_kind),
                    surface: "blob",
                    version: None,
                    bytes: bytes.len() as u64,
                    payload: bytes,
                    detail: format!("hit id=blake3:{}", hex_bytes(id.as_bytes())),
                })
            }
        }
    }

    fn execute_object(&self, envelope: RequestEnvelope) -> RsfResult<DataPlaneResponse> {
        let command = parse_object_command(&envelope.operation, &envelope.body)?;
        let mut runtime = self.open_object_runtime(
            envelope
                .namespace
                .as_deref()
                .ok_or_else(|| invalid("namespace is required"))?,
        )?;
        if matches!(&command, ObjectCommand::Get { .. })
            && runtime.router.transport_mut().is_network()
        {
            runtime.router.confirm_read_quorum()?;
        }
        match command {
            ObjectCommand::CreateBucket { bucket } => {
                let record = runtime.objects.create_bucket_request(
                    &mut runtime.router,
                    PlacementGroupId(1),
                    client_request_from_envelope_for_surface(&envelope, "object")?,
                    bucket,
                )?;
                Ok(DataPlaneResponse::Executed {
                    protocol: envelope.protocol.as_str(),
                    operation_kind: operation_kind_label(envelope.operation_kind),
                    surface: "object",
                    version: Some(record.version.0),
                    bytes: 0,
                    payload: Vec::new(),
                    detail: format!(
                        "create_bucket bucket={} placement_group={} log_index={}",
                        record.name, record.evidence.placement_group.0, record.evidence.log_index.0
                    ),
                })
            }
            ObjectCommand::Put { bucket, key, bytes } => {
                let request = client_request_from_envelope_for_surface(&envelope, "object")?;
                let put = runtime.objects.put(
                    &mut runtime.router,
                    PlacementGroupId(1),
                    PlacementGroupId(1),
                    request,
                    bucket,
                    key,
                    bytes,
                )?;
                Ok(DataPlaneResponse::Executed {
                    protocol: envelope.protocol.as_str(),
                    operation_kind: operation_kind_label(envelope.operation_kind),
                    surface: "object",
                    version: Some(put.head.version.0),
                    bytes: put.head.bytes,
                    payload: Vec::new(),
                    detail: format!(
                        "put bucket={} key={} id=blake3:{} manifest=blake3:{} deduped={} pins={} placement_group={} log_index={}",
                        put.head.bucket,
                        put.head.key,
                        hex_bytes(put.head.content.as_bytes()),
                        hex_bytes(put.head.manifest_digest.as_bytes()),
                        put.deduped,
                        put.pins,
                        put.head.evidence.placement_group.0,
                        put.head.evidence.log_index.0
                    ),
                })
            }
            ObjectCommand::Get { bucket, key } => {
                let bytes = runtime.objects.read_verified(&bucket, &key)?;
                let head = runtime
                    .objects
                    .head(&bucket, &key)
                    .expect("object head exists after verified read");
                Ok(DataPlaneResponse::Executed {
                    protocol: envelope.protocol.as_str(),
                    operation_kind: operation_kind_label(envelope.operation_kind),
                    surface: "object",
                    version: Some(head.version.0),
                    bytes: bytes.len() as u64,
                    payload: bytes,
                    detail: format!(
                        "hit bucket={} key={} id=blake3:{} manifest=blake3:{}",
                        bucket,
                        key,
                        hex_bytes(head.content.as_bytes()),
                        hex_bytes(head.manifest_digest.as_bytes())
                    ),
                })
            }
        }
    }

    fn open_kv_runtime(&self, namespace: &str) -> RsfResult<KvRuntime> {
        let (mut router, epoch) = self.open_router()?;
        let mut kv = DurableKvEngine::open_replicated(
            crate::security::namespace_root(&self.state, namespace).join("kv"),
            epoch,
            namespace,
        )?;
        for command in router.committed_application_commands() {
            if command.application.surface == rsf_protocol::ApplicationSurface::Kv
                && command.application.namespace == namespace
            {
                kv.apply_application_command(&command)?;
            }
        }
        kv.reconcile(&mut router)?;
        Ok(KvRuntime { router, kv })
    }

    fn open_sql_runtime(&self, namespace: &str) -> RsfResult<SqlRuntime> {
        let (mut router, _) = self.open_router()?;
        let mut sql = DurableSqlEngine::open_replicated(
            crate::security::namespace_root(&self.state, namespace).join("sql"),
            namespace,
        )?;
        for command in router.committed_application_commands() {
            if command.application.surface == rsf_protocol::ApplicationSurface::Sql
                && command.application.namespace == namespace
            {
                sql.apply_application_command(&command)?;
            }
        }
        sql.reconcile(&mut router)?;
        Ok(SqlRuntime { router, sql })
    }

    fn open_blob_runtime(&self, namespace: &str) -> RsfResult<BlobRuntime> {
        let (router, _) = self.open_router()?;
        let mut blobs = DurableBlobEngine::open_replicated(
            crate::security::namespace_root(&self.state, namespace).join("blob"),
            namespace,
        )?;
        for command in router.committed_application_commands() {
            if command.application.surface == rsf_protocol::ApplicationSurface::Blob
                && command.application.namespace == namespace
            {
                blobs.apply_application_command(&command)?;
            }
        }
        Ok(BlobRuntime { router, blobs })
    }

    fn open_object_runtime(&self, namespace: &str) -> RsfResult<ObjectRuntime> {
        let (mut router, _) = self.open_router()?;
        let mut objects = DurableObjectStore::open_replicated(
            crate::security::namespace_root(&self.state, namespace).join("object"),
            namespace,
        )?;
        for command in router.committed_application_commands() {
            if command.application.surface == rsf_protocol::ApplicationSurface::Object
                && command.application.namespace == namespace
            {
                objects.apply_application_command(&command)?;
            }
        }
        objects.reconcile(&mut router)?;
        Ok(ObjectRuntime { router, objects })
    }

    fn open_router(&self) -> RsfResult<(DataRouter, Epoch)> {
        let admin = AdminPlane::recover(self.state.join("admin"))?;
        let membership = admin.membership_view().ok_or_else(|| {
            RsfError::new(
                ErrorCode::InvalidInput,
                "data-plane execution requires at least one promoted voter",
            )
        })?;
        let voters = membership.voter_nodes();
        let primary = *voters.first().ok_or_else(|| {
            RsfError::new(
                ErrorCode::InvalidInput,
                "data-plane execution requires at least one promoted voter",
            )
        })?;
        let epoch = Epoch(admin.cluster_map().epoch.0.max(1));
        let transport =
            transport::DataTransport::load(&self.state, &admin, primary).map_err(invalid)?;
        let mut router = VsrBackedFabricRouter::open_with_transport(
            self.state.join("protocol"),
            [PlacementGroupId(1)],
            epoch,
            primary,
            voters.clone(),
            transport,
        )?;
        if !crate::protocol_membership_matches(&router, &voters) {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                "protocol membership differs from the admin map; resume the pending join",
            ));
        }
        for pg in &router.placement_map().groups {
            if router.membership(*pg)?.1 != primary {
                return Err(invalid(
                    "local data node is not the current placement-group primary",
                ));
            }
        }
        router.reconcile_application_commands()?;
        Ok((router, epoch))
    }
}

struct KvRuntime {
    router: DataRouter,
    kv: DurableKvEngine,
}

struct SqlRuntime {
    router: DataRouter,
    sql: DurableSqlEngine,
}

struct BlobRuntime {
    router: DataRouter,
    blobs: DurableBlobEngine,
}

struct ObjectRuntime {
    router: DataRouter,
    objects: DurableObjectStore,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum KvCommand {
    Get { key: String },
    Set { key: String, value: Vec<u8> },
    Clear { key: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum BlobCommand {
    Put { name: String, bytes: Vec<u8> },
    Get { id: ContentId },
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ObjectCommand {
    CreateBucket {
        bucket: String,
    },
    Put {
        bucket: String,
        key: String,
        bytes: Vec<u8>,
    },
    Get {
        bucket: String,
        key: String,
    },
}

fn serve_data_stream(
    mut stream: TcpStream,
    codec: &WireCodec,
    adapter: &WireAdapter,
    executor: &DataPlaneExecutor,
    tls: std::sync::Arc<rustls::ServerConfig>,
) -> Result<DataPlaneResponse, String> {
    stream
        .set_nonblocking(false)
        .map_err(|err| format!("failed to configure data socket: {err}"))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .map_err(|err| format!("failed to configure data read timeout: {err}"))?;
    stream
        .set_write_timeout(Some(Duration::from_secs(2)))
        .map_err(|err| format!("failed to configure data write timeout: {err}"))?;

    let mut connection = rustls::ServerConnection::new(tls).map_err(|err| err.to_string())?;
    while connection.is_handshaking() {
        connection
            .complete_io(&mut stream)
            .map_err(|err| format!("TLS client authentication failed: {err}"))?;
    }
    let certificate = connection
        .peer_certificates()
        .and_then(|certs| certs.first())
        .ok_or("TLS peer certificate is required")?;
    let fingerprint = crate::tls::certificate_fingerprint(certificate.as_ref());
    let mut stream = rustls::StreamOwned::new(connection, stream);
    let response = read_data_frame(&mut stream)
        .and_then(|frame| {
            codec
                .decode_and_admit(&frame, adapter)
                .map_err(|err| err.message)
        })
        .map(|decision| data_plane_response(decision, executor, &fingerprint))
        .unwrap_or_else(|reason| DataPlaneResponse::Corrupt { reason });
    let body = render_data_plane_response(&response);
    stream
        .write_all(body.as_bytes())
        .map_err(|err| format!("failed to write daemon data response: {err}"))?;
    stream.conn.send_close_notify();
    stream
        .flush()
        .map_err(|err| format!("failed to flush daemon data response: {err}"))?;
    Ok(response)
}

fn read_data_frame(stream: &mut impl Read) -> Result<Vec<u8>, String> {
    let mut len = [0_u8; 4];
    stream
        .read_exact(&mut len)
        .map_err(|err| format!("failed to read daemon data frame length: {err}"))?;
    let len = u32::from_le_bytes(len);
    if len == 0 {
        return Err("daemon data frame length must be greater than zero".to_string());
    }
    if len > MAX_DATA_FRAME_BYTES {
        return Err(format!(
            "daemon data frame length {} exceeds {} bytes",
            len, MAX_DATA_FRAME_BYTES
        ));
    }
    let mut frame = vec![0_u8; len as usize];
    stream
        .read_exact(&mut frame)
        .map_err(|err| format!("failed to read daemon data frame body: {err}"))?;
    Ok(frame)
}

fn data_plane_response(
    decision: WireDecision,
    executor: &DataPlaneExecutor,
    fingerprint: &[u8; 32],
) -> DataPlaneResponse {
    match decision {
        WireDecision::Accepted(envelope) => executor.execute(envelope, fingerprint),
        WireDecision::Rejected { protocol, reason } => DataPlaneResponse::Rejected {
            protocol: protocol.as_str(),
            reason,
        },
    }
}

fn parse_kv_command(operation: &str, body: &[u8]) -> RsfResult<KvCommand> {
    let mut parts = operation.split_whitespace();
    let verb = parts
        .next()
        .ok_or_else(|| invalid("kv operation must include a verb"))?
        .to_ascii_uppercase();
    match verb.as_str() {
        "GET" | "READ" => {
            let key = required_token(parts.next(), "kv read key")?;
            ensure_no_extra(parts.next(), "kv read")?;
            ensure_empty_body(body, "kv read")?;
            Ok(KvCommand::Get { key })
        }
        "SET" | "PUT" => {
            let key = required_token(parts.next(), "kv write key")?;
            let inline = parts.next();
            ensure_no_extra(parts.next(), "kv write")?;
            let value = match (inline, body.is_empty()) {
                (Some(value), true) => {
                    validate_token(value, "kv write value")?;
                    value.as_bytes().to_vec()
                }
                (None, false) => body.to_vec(),
                (Some(_), false) => {
                    return Err(invalid(
                        "kv write value must be supplied either as an operation token or a frame body, not both",
                    ));
                }
                (None, true) => return Err(invalid("kv write value or frame body is required")),
            };
            Ok(KvCommand::Set { key, value })
        }
        "CLEAR" | "DELETE" => {
            let key = required_token(parts.next(), "kv clear key")?;
            ensure_no_extra(parts.next(), "kv clear")?;
            ensure_empty_body(body, "kv clear")?;
            Ok(KvCommand::Clear { key })
        }
        _ => Err(invalid(
            "kv operation must be GET, SET, PUT, CLEAR, or DELETE",
        )),
    }
}

fn parse_blob_command(operation: &str, body: &[u8]) -> RsfResult<BlobCommand> {
    let mut parts = operation.split_whitespace();
    let verb = parts
        .next()
        .ok_or_else(|| invalid("blob operation must include a verb"))?
        .to_ascii_uppercase();
    match verb.as_str() {
        "PUT" | "WRITE" => {
            let name = required_token(parts.next(), "blob write name")?;
            ensure_no_extra(parts.next(), "blob write")?;
            if body.is_empty() {
                return Err(invalid("blob write frame body is required"));
            }
            Ok(BlobCommand::Put {
                name,
                bytes: body.to_vec(),
            })
        }
        "GET" | "READ" => {
            let id = parse_content_id(&required_token(parts.next(), "blob read content id")?)?;
            ensure_no_extra(parts.next(), "blob read")?;
            ensure_empty_body(body, "blob read")?;
            Ok(BlobCommand::Get { id })
        }
        _ => Err(invalid("blob operation must be GET, READ, PUT, or WRITE")),
    }
}

fn parse_object_command(operation: &str, body: &[u8]) -> RsfResult<ObjectCommand> {
    let mut parts = operation.split_whitespace();
    let verb = parts
        .next()
        .ok_or_else(|| invalid("object operation must include a verb"))?
        .to_ascii_uppercase();
    match verb.as_str() {
        "CREATE_BUCKET" | "CREATE-BUCKET" | "BUCKET" => {
            let bucket = required_token(parts.next(), "object bucket name")?;
            ensure_no_extra(parts.next(), "object bucket create")?;
            ensure_empty_body(body, "object bucket create")?;
            validate_object_bucket(&bucket)?;
            Ok(ObjectCommand::CreateBucket { bucket })
        }
        "PUT" | "WRITE" => {
            let (bucket, key) =
                parse_object_target(&required_token(parts.next(), "object write target")?)?;
            ensure_no_extra(parts.next(), "object write")?;
            if body.is_empty() {
                return Err(invalid("object write frame body is required"));
            }
            Ok(ObjectCommand::Put {
                bucket,
                key,
                bytes: body.to_vec(),
            })
        }
        "GET" | "READ" => {
            let (bucket, key) =
                parse_object_target(&required_token(parts.next(), "object read target")?)?;
            ensure_no_extra(parts.next(), "object read")?;
            ensure_empty_body(body, "object read")?;
            Ok(ObjectCommand::Get { bucket, key })
        }
        _ => Err(invalid(
            "object operation must be CREATE_BUCKET, GET, READ, PUT, or WRITE",
        )),
    }
}

fn parse_object_target(value: &str) -> RsfResult<(String, String)> {
    let (bucket, key) = value
        .split_once('/')
        .ok_or_else(|| invalid("object target must be <bucket>/<key>"))?;
    validate_object_bucket(bucket)?;
    validate_token(key, "object key")?;
    Ok((bucket.to_string(), key.to_string()))
}

fn validate_object_bucket(bucket: &str) -> RsfResult<()> {
    validate_token(bucket, "object bucket")?;
    if bucket.contains('/') {
        return Err(invalid("object bucket must not contain '/'"));
    }
    Ok(())
}

fn ensure_empty_body(body: &[u8], label: &str) -> RsfResult<()> {
    if !body.is_empty() {
        return Err(invalid(format!("{label} operation must not carry a body")));
    }
    Ok(())
}

fn parse_content_id(value: &str) -> RsfResult<ContentId> {
    let hex = value.strip_prefix("blake3:").unwrap_or(value);
    if hex.len() != 64 {
        return Err(invalid("blob content id must be blake3:<64 hex chars>"));
    }
    let bytes = hex_decode_32(hex)?;
    ContentId::from_blake3(bytes).map_err(|err| invalid(err.message))
}

fn hex_decode_32(value: &str) -> RsfResult<[u8; 32]> {
    let raw = value.as_bytes();
    let mut out = [0_u8; 32];
    for (idx, pair) in raw.chunks_exact(2).enumerate() {
        out[idx] = (hex_nibble(pair[0])? << 4) | hex_nibble(pair[1])?;
    }
    Ok(out)
}

fn hex_nibble(byte: u8) -> RsfResult<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(invalid("blob content id contains a non-hex character")),
    }
}

fn hex_bytes(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn required_token(value: Option<&str>, label: &str) -> RsfResult<String> {
    let value = value.ok_or_else(|| invalid(format!("{label} is required")))?;
    validate_token(value, label)?;
    Ok(value.to_string())
}

fn ensure_no_extra(value: Option<&str>, label: &str) -> RsfResult<()> {
    if value.is_some() {
        return Err(invalid(format!("{label} operation has too many fields")));
    }
    Ok(())
}

fn validate_token(value: &str, label: &str) -> RsfResult<()> {
    if value.is_empty() {
        return Err(invalid(format!("{label} must not be empty")));
    }
    if value.len() > 4096 {
        return Err(invalid(format!("{label} exceeds 4096 bytes")));
    }
    if value.chars().any(char::is_control) {
        return Err(invalid(format!("{label} contains control characters")));
    }
    Ok(())
}

fn client_request_from_envelope(envelope: &RequestEnvelope) -> RsfResult<ClientRequestId> {
    client_request_from_envelope_for_surface(envelope, "kv")
}

fn sql_request_client_from_envelope(envelope: &RequestEnvelope) -> RsfResult<String> {
    match envelope.operation_kind {
        WireOperationKind::Read => Ok(format!(
            "wire:sql:{}:{}:read",
            hex_bytes(envelope.principal.as_bytes()),
            hex_bytes(envelope.namespace.as_deref().unwrap_or("").as_bytes())
        )),
        WireOperationKind::Write | WireOperationKind::Admin => {
            let key = envelope.idempotency_key.as_ref().ok_or_else(|| {
                invalid("sql mutating data-plane request requires an idempotency key")
            })?;
            Ok(format!(
                "wire:sql:{}:{}:write:{}",
                hex_bytes(envelope.principal.as_bytes()),
                hex_bytes(envelope.namespace.as_deref().unwrap_or("").as_bytes()),
                hex_bytes(key.as_bytes())
            ))
        }
    }
}

fn client_request_from_envelope_for_surface(
    envelope: &RequestEnvelope,
    surface: &str,
) -> RsfResult<ClientRequestId> {
    let key = envelope.idempotency_key.as_ref().ok_or_else(|| {
        invalid(format!(
            "{surface} mutating data-plane request requires an idempotency key"
        ))
    })?;
    Ok(ClientRequestId::new(
        format!(
            "wire:{}:{}:{}:{}",
            surface,
            hex_bytes(envelope.principal.as_bytes()),
            hex_bytes(envelope.namespace.as_deref().unwrap_or("").as_bytes()),
            hex_bytes(key.as_bytes())
        ),
        stable_request_nonce(key),
    ))
}

fn stable_request_nonce(value: &str) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message)
}

fn operation_kind_label(kind: WireOperationKind) -> &'static str {
    match kind {
        WireOperationKind::Read => "read",
        WireOperationKind::Write => "write",
        WireOperationKind::Admin => "admin",
    }
}

fn sql_outcome_detail(outcome: &SqlOutcome) -> String {
    match outcome {
        SqlOutcome::TableCreated {
            table, evidence, ..
        } => format!(
            "create_table table={} placement_group={} log_index={}",
            table, evidence.placement_group.0, evidence.log_index.0
        ),
        SqlOutcome::IndexCreated {
            table,
            column,
            evidence,
            ..
        } => format!(
            "create_index table={} column={} placement_group={} log_index={}",
            table, column, evidence.placement_group.0, evidence.log_index.0
        ),
        SqlOutcome::Inserted { row, evidence } => {
            let evidence = committed_sql_evidence(evidence);
            format!(
                "insert table={} primary_key={} placement_group={} log_index={}",
                row.table, row.primary_key, evidence.placement_group.0, evidence.log_index.0
            )
        }
        SqlOutcome::Updated { row, evidence } => {
            let evidence = committed_sql_evidence(evidence);
            format!(
                "update table={} primary_key={} placement_group={} log_index={}",
                row.table, row.primary_key, evidence.placement_group.0, evidence.log_index.0
            )
        }
        SqlOutcome::Deleted {
            table,
            primary_key,
            evidence,
        } => {
            let evidence = committed_sql_evidence(evidence);
            format!(
                "delete table={} primary_key={} placement_group={} log_index={}",
                table, primary_key, evidence.placement_group.0, evidence.log_index.0
            )
        }
        SqlOutcome::Selected { rows } => format!("select rows={}", rows.len()),
        SqlOutcome::TransactionBegan {
            session_id,
            snapshot,
        } => format!("begin session={} snapshot={}", session_id, snapshot.0),
        SqlOutcome::SavepointCreated {
            session_id,
            name,
            staged,
        } => format!(
            "savepoint session={} name={} staged={}",
            session_id, name, staged
        ),
        SqlOutcome::SavepointRolledBack {
            session_id,
            name,
            staged,
        } => format!(
            "rollback_to session={} name={} staged={}",
            session_id, name, staged
        ),
        SqlOutcome::TransactionCommitted {
            session_id,
            evidence,
            mutations,
        } => format!(
            "commit session={} mutations={} placement_group={} log_index={}",
            session_id, mutations, evidence.placement_group.0, evidence.log_index.0
        ),
        SqlOutcome::TransactionRolledBack {
            session_id,
            discarded,
        } => format!("rollback session={} discarded={}", session_id, discarded),
    }
}

fn committed_sql_evidence(evidence: &SqlMutationEvidence) -> &rsf_core::CommitEvidence {
    evidence.expect_committed("daemon sql data-plane autocommit")
}

fn single_line(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        if ch.is_control() {
            escaped.extend(ch.escape_default());
        } else {
            escaped.push(ch);
        }
    }
    escaped
}

fn render_data_plane_response(response: &DataPlaneResponse) -> String {
    match response {
        DataPlaneResponse::Executed {
            protocol,
            operation_kind,
            surface,
            version,
            bytes,
            payload,
            detail,
        } => format!(
            "RSF/1 executed protocol={} operation_kind={} surface={} version={} bytes={} payload_hex={} detail={}\n",
            protocol,
            operation_kind,
            surface,
            version.map_or_else(|| "none".to_string(), |version| version.to_string()),
            bytes,
            hex_bytes(payload),
            single_line(detail)
        ),
        DataPlaneResponse::Rejected { protocol, reason } => {
            format!(
                "RSF/1 rejected protocol={} reason={}\n",
                protocol,
                single_line(reason)
            )
        }
        DataPlaneResponse::Corrupt { reason } => {
            format!("RSF/1 corrupt reason={}\n", single_line(reason))
        }
    }
}

fn emit_data_plane_response(
    out: &mut impl Write,
    peer: SocketAddr,
    response: &DataPlaneResponse,
) -> Result<(), String> {
    match response {
        DataPlaneResponse::Executed {
            protocol,
            operation_kind,
            surface,
            version,
            bytes,
            detail,
            ..
        } => writeln!(
            out,
            "daemon data peer={} status=executed protocol={} operation_kind={} surface={} version={} bytes={} detail={}",
            peer,
            protocol,
            operation_kind,
            surface,
            version.map_or_else(|| "none".to_string(), |version| version.to_string()),
            bytes,
            single_line(detail)
        ),
        DataPlaneResponse::Rejected { protocol, reason } => writeln!(
            out,
            "daemon data peer={} status=rejected protocol={} reason={}",
            peer, protocol, single_line(reason)
        ),
        DataPlaneResponse::Corrupt { reason } => {
            writeln!(
                out,
                "daemon data peer={} status=corrupt reason={}",
                peer, single_line(reason)
            )
        }
    }
    .map_err(|err| err.to_string())?;
    out.flush().map_err(|err| err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sql_request_identity_separates_reads_and_delimiter_containing_scopes() {
        let mut envelope = RequestEnvelope {
            protocol: WireProtocol::Sql,
            principal: "a:b".to_string(),
            namespace: Some("c".to_string()),
            operation: "SELECT * FROM users".to_string(),
            operation_kind: WireOperationKind::Write,
            idempotency_key: Some("read".to_string()),
            body_bytes: 0,
            body: Vec::new(),
        };
        let first = sql_request_client_from_envelope(&envelope).unwrap();
        envelope.principal = "a".to_string();
        envelope.namespace = Some("b:c".to_string());
        let second = sql_request_client_from_envelope(&envelope).unwrap();
        assert_ne!(first, second);
        envelope.operation_kind = WireOperationKind::Read;
        assert_ne!(second, sql_request_client_from_envelope(&envelope).unwrap());
    }

    #[test]
    fn response_encodes_binary_payload_and_keeps_control_characters_out_of_logs() {
        let response = DataPlaneResponse::Executed {
            protocol: "kv-http",
            operation_kind: "read",
            surface: "kv",
            version: Some(1),
            bytes: 4,
            payload: vec![0, 255, b'\n', b'\r'],
            detail: "hit\nforged\rentry".to_string(),
        };
        let wire = render_data_plane_response(&response);
        assert!(wire.contains("payload_hex=00ff0a0d"));
        assert_eq!(wire.lines().count(), 1);
        let mut log = Vec::new();
        emit_data_plane_response(&mut log, "127.0.0.1:1".parse().unwrap(), &response).unwrap();
        let log = String::from_utf8(log).unwrap();
        assert_eq!(log.lines().count(), 1);
        assert!(!log.contains("00ff0a0d"));
        for response in [
            DataPlaneResponse::Rejected {
                protocol: "sql-wire",
                reason: "bad\ninput".to_string(),
            },
            DataPlaneResponse::Corrupt {
                reason: "bad\r\nframe".to_string(),
            },
        ] {
            assert_eq!(render_data_plane_response(&response).lines().count(), 1);
        }
    }

    #[test]
    fn kv_command_parser_accepts_read_write_and_clear() {
        assert_eq!(
            parse_kv_command("GET user/1", &[]).expect("get"),
            KvCommand::Get {
                key: "user/1".to_string()
            }
        );
        assert_eq!(
            parse_kv_command("SET user/1 ada", &[]).expect("set"),
            KvCommand::Set {
                key: "user/1".to_string(),
                value: b"ada".to_vec()
            }
        );
        assert_eq!(
            parse_kv_command("SET user/2", b"grace").expect("body set"),
            KvCommand::Set {
                key: "user/2".to_string(),
                value: b"grace".to_vec()
            }
        );
        assert_eq!(
            parse_kv_command("CLEAR user/1", &[]).expect("clear"),
            KvCommand::Clear {
                key: "user/1".to_string()
            }
        );
    }

    #[test]
    fn kv_command_parser_rejects_ambiguous_operations() {
        let err = parse_kv_command("SET user/1 ada extra", &[]).expect_err("extra field rejected");
        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(err.message.contains("too many fields"));

        let err = parse_kv_command("SET user/1 ada", b"body").expect_err("ambiguous value");
        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(
            err.message
                .contains("either as an operation token or a frame body")
        );

        let err = parse_kv_command("GET user/1", b"body").expect_err("read body");
        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(err.message.contains("must not carry a body"));
    }

    #[test]
    fn blob_command_parser_accepts_payload_writes_and_content_reads() {
        let id = ContentId::for_bytes(b"blob-payload");
        assert_eq!(
            parse_blob_command("PUT backup.bin", b"blob-payload").expect("put"),
            BlobCommand::Put {
                name: "backup.bin".to_string(),
                bytes: b"blob-payload".to_vec()
            }
        );
        assert_eq!(
            parse_blob_command(&format!("GET blake3:{}", hex_bytes(id.as_bytes())), &[])
                .expect("get"),
            BlobCommand::Get { id }
        );
    }

    #[test]
    fn blob_command_parser_rejects_missing_or_malformed_payloads() {
        let err = parse_blob_command("PUT backup.bin", &[]).expect_err("missing body");
        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(err.message.contains("frame body is required"));

        let err = parse_blob_command("GET blake3:not-hex", &[]).expect_err("bad id");
        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(err.message.contains("64 hex"));

        let id = ContentId::for_bytes(b"blob-payload");
        let err = parse_blob_command(&format!("GET blake3:{}", hex_bytes(id.as_bytes())), b"body")
            .expect_err("read body");
        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(err.message.contains("must not carry a body"));
    }

    #[test]
    fn object_command_parser_accepts_bucket_put_and_get() {
        assert_eq!(
            parse_object_command("CREATE_BUCKET media", &[]).expect("bucket"),
            ObjectCommand::CreateBucket {
                bucket: "media".to_string()
            }
        );
        assert_eq!(
            parse_object_command("PUT media/hero.txt", b"object-payload").expect("put"),
            ObjectCommand::Put {
                bucket: "media".to_string(),
                key: "hero.txt".to_string(),
                bytes: b"object-payload".to_vec()
            }
        );
        assert_eq!(
            parse_object_command("GET media/hero.txt", &[]).expect("get"),
            ObjectCommand::Get {
                bucket: "media".to_string(),
                key: "hero.txt".to_string()
            }
        );
    }

    #[test]
    fn object_command_parser_rejects_malformed_targets_and_payloads() {
        let err = parse_object_command("PUT media/hero.txt", &[]).expect_err("missing body");
        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(err.message.contains("frame body is required"));

        let err = parse_object_command("GET media/hero.txt", b"body").expect_err("read body");
        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(err.message.contains("must not carry a body"));

        let err = parse_object_command("GET media", &[]).expect_err("missing key");
        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(err.message.contains("<bucket>/<key>"));
    }
}
