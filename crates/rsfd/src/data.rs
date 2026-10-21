mod deadline;
pub(crate) mod execution;
use crate::replication::ReplicationTransport;
type DataRouter = VsrBackedFabricRouter<ReplicationTransport>;

use std::cell::RefCell;
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

pub(crate) const DATA_CONNECTION_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_DATA_FRAME_BYTES: u32 = 64 * 1024;
const MAX_DATA_REQUESTS_PER_TICK: usize = 64;

pub struct DataPlaneListener {
    listener: TcpListener,
    codec: WireCodec,
    adapter: WireAdapter,
    state: PathBuf,
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
            state,
            tls,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.listener
            .local_addr()
            .expect("bound data listener exposes local address")
    }

    pub fn serve_pending(&self, out: &mut impl Write) -> Result<usize, String> {
        let executor = DataPlaneExecutor::new(self.state.clone());
        self.serve_with(
            MAX_DATA_REQUESTS_PER_TICK,
            &mut |envelope, fingerprint| executor.execute(envelope, fingerprint),
            out,
        )
    }

    /// One connection per peer-control tick bounds application admission work.
    pub(crate) fn serve_one_with(
        &self,
        executor: &mut impl FnMut(RequestEnvelope, &[u8; 32]) -> DataPlaneResponse,
        out: &mut impl Write,
    ) -> Result<usize, String> {
        self.serve_with(1, executor, out)
    }

    fn serve_with(
        &self,
        limit: usize,
        executor: &mut impl FnMut(RequestEnvelope, &[u8; 32]) -> DataPlaneResponse,
        out: &mut impl Write,
    ) -> Result<usize, String> {
        let mut served = 0_usize;
        loop {
            match self.listener.accept() {
                Ok((stream, peer)) => {
                    let response = serve_data_stream(
                        stream,
                        &self.codec,
                        &self.adapter,
                        executor,
                        self.tls.clone(),
                    )
                    .unwrap_or_else(|err| DataPlaneResponse::Rejected {
                        protocol: "tls",
                        // This may be a response-write failure after execution,
                        // not just a handshake/read failure before admission.
                        mutation: MutationOutcome::Unknown,
                        reason: format!("connection failed: {err}"),
                    });
                    emit_data_plane_response(out, peer, &response)?;
                    served = served.saturating_add(1);
                    if served == limit {
                        return Ok(served);
                    }
                }
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => return Ok(served),
                Err(err) => return Err(format!("failed to accept daemon data request: {err}")),
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MutationOutcome {
    NotAttempted,
    Unknown,
}

impl MutationOutcome {
    fn as_str(&self) -> &'static str {
        match self {
            Self::NotAttempted => "not-attempted",
            Self::Unknown => "unknown",
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
        mutation: MutationOutcome,
        reason: String,
    },
    Redirect {
        protocol: &'static str,
        primary: u64,
        address: SocketAddr,
    },
    Corrupt {
        reason: String,
    },
}

#[derive(Debug)]
pub struct DataPlaneExecutor {
    state: PathBuf,
    cached: RefCell<Option<CachedRuntime>>,
}

impl DataPlaneExecutor {
    pub fn new(state: impl Into<PathBuf>) -> Self {
        Self {
            state: state.into(),
            cached: RefCell::new(None),
        }
    }

    fn execute(&self, envelope: RequestEnvelope, fingerprint: &[u8; 32]) -> DataPlaneResponse {
        if let Err(err) = crate::security::authorize(&self.state, fingerprint, &envelope) {
            return DataPlaneResponse::Rejected {
                protocol: envelope.protocol.as_str(),
                mutation: MutationOutcome::NotAttempted,
                reason: err.message,
            };
        }
        let protocol = envelope.protocol;
        // Once an authorized mutation enters execution, storage/replication may
        // have persisted an intent before an error. No generic error code can
        // prove that it was rolled back, including an I/O error during fsync.
        let mutation = match envelope.operation_kind {
            WireOperationKind::Read => MutationOutcome::NotAttempted,
            WireOperationKind::Write | WireOperationKind::Admin => MutationOutcome::Unknown,
        };
        let result = match protocol {
            WireProtocol::Sql => self.execute_sql(envelope),
            WireProtocol::Kv => self.execute_kv(envelope),
            WireProtocol::Blob => self.execute_blob(envelope),
            WireProtocol::Object => self.execute_object(envelope),
            protocol => return DataPlaneResponse::Rejected {
                protocol: protocol.as_str(),
                mutation: MutationOutcome::NotAttempted,
                reason: "data-plane execution handoff is currently available for sql, kv, blob, and object requests"
                    .to_string(),
            },
        };
        result.unwrap_or_else(|err| DataPlaneResponse::Rejected {
            protocol: protocol.as_str(),
            mutation,
            reason: err.message,
        })
    }

    fn execute_sql(&self, envelope: RequestEnvelope) -> RsfResult<DataPlaneResponse> {
        let namespace = envelope
            .namespace
            .as_deref()
            .ok_or_else(|| invalid("namespace is required"))?;
        let mut runtime = self.open_sql_runtime(namespace)?;
        let response = execution::sql(&envelope, &mut runtime.router, &mut runtime.sql);
        self.remember(namespace, Runtime::Sql(runtime));
        response
    }

    fn execute_kv(&self, envelope: RequestEnvelope) -> RsfResult<DataPlaneResponse> {
        let namespace = envelope
            .namespace
            .as_deref()
            .ok_or_else(|| invalid("namespace is required"))?;
        let mut runtime = self.open_kv_runtime(namespace)?;
        let response = execution::kv(&envelope, &mut runtime.router, &mut runtime.kv);
        self.remember(namespace, Runtime::Kv(runtime));
        response
    }

    fn execute_blob(&self, envelope: RequestEnvelope) -> RsfResult<DataPlaneResponse> {
        let namespace = envelope
            .namespace
            .as_deref()
            .ok_or_else(|| invalid("namespace is required"))?;
        let mut runtime = self.open_blob_runtime(namespace)?;
        let response = execution::blob(&envelope, &mut runtime.router, &mut runtime.blobs);
        self.remember(namespace, Runtime::Blob(runtime));
        response
    }

    fn execute_object(&self, envelope: RequestEnvelope) -> RsfResult<DataPlaneResponse> {
        let namespace = envelope
            .namespace
            .as_deref()
            .ok_or_else(|| invalid("namespace is required"))?;
        let mut runtime = self.open_object_runtime(namespace)?;
        let response = execution::object(&envelope, &mut runtime.router, &mut runtime.objects);
        self.remember(namespace, Runtime::Object(runtime));
        response
    }

    fn open_kv_runtime(&self, namespace: &str) -> RsfResult<KvRuntime> {
        if let Some(Runtime::Kv(runtime)) =
            self.take_runtime(namespace, rsf_protocol::ApplicationSurface::Kv)?
        {
            return Ok(runtime);
        }
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
        if let Some(Runtime::Sql(runtime)) =
            self.take_runtime(namespace, rsf_protocol::ApplicationSurface::Sql)?
        {
            return Ok(runtime);
        }
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
        if let Some(Runtime::Blob(runtime)) =
            self.take_runtime(namespace, rsf_protocol::ApplicationSurface::Blob)?
        {
            return Ok(runtime);
        }
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
        if let Some(Runtime::Object(runtime)) =
            self.take_runtime(namespace, rsf_protocol::ApplicationSurface::Object)?
        {
            return Ok(runtime);
        }
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

    fn remember(&self, namespace: &str, runtime: Runtime) {
        *self.cached.borrow_mut() = Some(CachedRuntime {
            namespace: namespace.to_owned(),
            runtime,
        });
    }

    fn take_runtime(
        &self,
        namespace: &str,
        surface: rsf_protocol::ApplicationSurface,
    ) -> RsfResult<Option<Runtime>> {
        let Some(mut cached) = self.cached.borrow_mut().take() else {
            return Ok(None);
        };
        if cached.namespace != namespace || cached.runtime.surface() != surface {
            return Ok(None);
        }
        let router = cached.runtime.router();
        if !router.control_metadata_matches_disk()? {
            return Ok(None);
        }
        // Authorization is still performed by execute for each request. The
        // transport and admitted membership are also refreshed, so a live burst
        // cannot bypass a revoked identity or changed endpoint configuration.
        let admin = AdminPlane::recover(self.state.join("admin"))?;
        let voters = admin
            .membership_view()
            .ok_or_else(|| invalid("missing promoted voters"))?
            .voter_nodes();
        let primary = *voters
            .first()
            .ok_or_else(|| invalid("missing promoted voter"))?;
        if router.coordination_epoch() != Epoch(admin.cluster_map().epoch.0.max(1))
            || !crate::protocol_membership_matches(router, &voters)
            || router.placement_map().groups.iter().any(|pg| {
                router
                    .membership(*pg)
                    .map_or(true, |(_, current, _)| current != primary)
            })
        {
            return Ok(None);
        }
        *router.transport_mut() =
            ReplicationTransport::load(&self.state, &admin, primary).map_err(invalid)?;
        if router.reconcile_application_commands()? != 0 {
            // An interrupted command needs full materializer reconciliation.
            return Ok(None);
        }
        Ok(Some(cached.runtime))
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
            ReplicationTransport::load(&self.state, &admin, primary).map_err(invalid)?;
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

#[derive(Debug)]
struct CachedRuntime {
    namespace: String,
    runtime: Runtime,
}

#[derive(Debug)]
enum Runtime {
    Kv(KvRuntime),
    Sql(SqlRuntime),
    Blob(BlobRuntime),
    Object(ObjectRuntime),
}

impl Runtime {
    fn surface(&self) -> rsf_protocol::ApplicationSurface {
        match self {
            Self::Kv(_) => rsf_protocol::ApplicationSurface::Kv,
            Self::Sql(_) => rsf_protocol::ApplicationSurface::Sql,
            Self::Blob(_) => rsf_protocol::ApplicationSurface::Blob,
            Self::Object(_) => rsf_protocol::ApplicationSurface::Object,
        }
    }
    fn router(&mut self) -> &mut DataRouter {
        match self {
            Self::Kv(runtime) => &mut runtime.router,
            Self::Sql(runtime) => &mut runtime.router,
            Self::Blob(runtime) => &mut runtime.router,
            Self::Object(runtime) => &mut runtime.router,
        }
    }
}

#[derive(Debug)]
struct KvRuntime {
    router: DataRouter,
    kv: DurableKvEngine,
}

#[derive(Debug)]
struct SqlRuntime {
    router: DataRouter,
    sql: DurableSqlEngine,
}

#[derive(Debug)]
struct BlobRuntime {
    router: DataRouter,
    blobs: DurableBlobEngine,
}

#[derive(Debug)]
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
    stream: TcpStream,
    codec: &WireCodec,
    adapter: &WireAdapter,
    executor: &mut impl FnMut(RequestEnvelope, &[u8; 32]) -> DataPlaneResponse,
    tls: std::sync::Arc<rustls::ServerConfig>,
) -> Result<DataPlaneResponse, String> {
    stream
        .set_nonblocking(false)
        .map_err(|err| format!("failed to configure data socket: {err}"))?;
    stream
        .set_nodelay(true)
        .map_err(|err| format!("failed to configure data socket latency: {err}"))?;
    let mut stream = deadline::DeadlineStream::new(stream, DATA_CONNECTION_TIMEOUT);

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
    let admitted = read_data_frame(&mut stream).and_then(|frame| {
        codec
            .decode_and_admit(&frame, adapter)
            .map_err(|err| err.message)
    });
    let response = match admitted {
        Ok(decision) => stream
            .sock
            .execute(|| data_plane_response(decision, executor, &fingerprint))
            .map_err(|err| format!("data I/O deadline expired before execution: {err}"))?,
        Err(reason) => DataPlaneResponse::Corrupt { reason },
    };
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
    executor: &mut impl FnMut(RequestEnvelope, &[u8; 32]) -> DataPlaneResponse,
    fingerprint: &[u8; 32],
) -> DataPlaneResponse {
    match decision {
        WireDecision::Accepted(envelope) => executor(envelope, fingerprint),
        WireDecision::Rejected { protocol, reason } => DataPlaneResponse::Rejected {
            protocol: protocol.as_str(),
            mutation: MutationOutcome::NotAttempted,
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
        DataPlaneResponse::Rejected {
            protocol,
            mutation,
            reason,
        } => {
            format!(
                "RSF/1 rejected protocol={} mutation={} reason={}\n",
                protocol,
                mutation.as_str(),
                single_line(reason)
            )
        }
        DataPlaneResponse::Redirect {
            protocol,
            primary,
            address,
        } => format!(
            "RSF/1 redirect protocol={} mutation=not-attempted primary={} address={}\n",
            protocol, primary, address
        ),
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
        DataPlaneResponse::Rejected { protocol, mutation, reason } => writeln!(
            out,
            "daemon data peer={} status=rejected protocol={} mutation={} reason={}",
            peer, protocol, mutation.as_str(), single_line(reason)
        ),
        DataPlaneResponse::Redirect { protocol, primary, address } => writeln!(
            out, "daemon data peer={} status=redirect protocol={} primary={} address={}",
            peer, protocol, primary, address
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
    fn slow_execution_still_delivers_complete_tls_response_and_close_notification() {
        use std::io::Read;
        let state = crate::tests::test_path("io-budget-execution");
        crate::test_support::provision(&state);
        let tls = crate::tls::server_config(&state).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let socket = listener.accept().unwrap().0;
            serve_data_stream(
                socket,
                &WireCodec,
                &WireAdapter,
                &mut |_, _| {
                    std::thread::sleep(DATA_CONNECTION_TIMEOUT + Duration::from_millis(100));
                    DataPlaneResponse::Executed {
                        protocol: "kv",
                        operation_kind: "read",
                        surface: "kv",
                        version: None,
                        bytes: 8,
                        payload: b"finished".to_vec(),
                        detail: "completed".into(),
                    }
                },
                tls,
            )
            .unwrap()
        });
        let socket = TcpStream::connect(address).unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        socket
            .set_write_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let connection = rustls::ClientConnection::new(
            crate::test_support::pki().client.clone(),
            "localhost".try_into().unwrap(),
        )
        .unwrap();
        let mut stream = rustls::StreamOwned::new(connection, socket);
        let frame = WireCodec
            .encode(&rsf_wire::RawFrame::with_body(
                WireProtocol::Kv,
                "app-client",
                Some("app".into()),
                "GET key",
                Some("slow-read".into()),
                vec![],
            ))
            .unwrap();
        stream
            .write_all(&(frame.len() as u32).to_le_bytes())
            .unwrap();
        stream.write_all(&frame).unwrap();
        stream.flush().unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).unwrap();
        assert!(response.starts_with("RSF/1 executed"), "{response}");
        assert!(response.contains("payload_hex=66696e6973686564"));
        assert!(matches!(
            server.join().unwrap(),
            DataPlaneResponse::Executed { .. }
        ));
    }

    #[test]
    fn a_warm_runtime_rechecks_certificate_revocation_and_replication_configuration() {
        let state = crate::tests::test_path("warm-runtime-security");
        crate::tests::bootstrap_one_node(state.to_str().unwrap());
        let executor = DataPlaneExecutor::new(&state);
        let fingerprint = crate::test_support::pki().fingerprint;
        let envelope = |key: &str| RequestEnvelope {
            protocol: WireProtocol::Kv,
            principal: "app-client".into(),
            namespace: Some("app".into()),
            operation: format!("SET {key}"),
            operation_kind: WireOperationKind::Write,
            idempotency_key: Some(key.into()),
            body_bytes: 5,
            body: b"value".to_vec(),
        };
        assert!(matches!(
            executor.execute(envelope("first"), &fingerprint),
            DataPlaneResponse::Executed { .. }
        ));
        let kv_path = crate::security::namespace_root(&state, "app").join("kv");
        assert!(DurableKvEngine::open_replicated(&kv_path, Epoch(1), "app").is_err());

        let mut auth = rsf_auth::DurableAuthEngine::open(state.join("auth"), Epoch(1)).unwrap();
        auth.revoke_certificate(fingerprint).unwrap();
        assert!(matches!(
            executor.execute(envelope("denied"), &fingerprint),
            DataPlaneResponse::Rejected {
                mutation: MutationOutcome::NotAttempted,
                ..
            }
        ));
        auth.bind_certificate(fingerprint, rsf_auth::PrincipalId("app-client".into()))
            .unwrap();
        drop(auth);
        std::fs::write(state.join("replication.json"), b"invalid configuration").unwrap();
        assert!(matches!(
            executor.execute(envelope("bad-config"), &fingerprint),
            DataPlaneResponse::Rejected {
                mutation: MutationOutcome::Unknown,
                ..
            }
        ));
        drop(executor);
        let kv = DurableKvEngine::open_replicated(&kv_path, Epoch(1), "app").unwrap();
        assert_eq!(kv.get(b"first").unwrap().value, b"value");
        assert!(kv.get(b"denied").is_none());
        assert!(kv.get(b"bad-config").is_none());
        assert_eq!(kv.changes_since(rsf_core::CommitVersion(0)).len(), 1);
        drop(kv);
        std::fs::remove_dir_all(state).unwrap();
    }

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
                mutation: MutationOutcome::Unknown,
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
