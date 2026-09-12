use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::time::Duration;

use rsf_admin::AdminPlane;
use rsf_blob::DurableBlobEngine;
use rsf_core::{Epoch, ErrorCode, PlacementGroupId, RsfError, RsfResult, blob::ContentId};
use rsf_kv::{DurableKvEngine, KvBatch, KvMutation};
use rsf_protocol::{ClientRequestId, VsrBackedFabricRouter};
use rsf_wire::{
    RequestEnvelope, WireAdapter, WireCodec, WireDecision, WireOperationKind, WireProtocol,
};

const MAX_DATA_FRAME_BYTES: u32 = 64 * 1024;

pub struct DataPlaneListener {
    listener: TcpListener,
    codec: WireCodec,
    adapter: WireAdapter,
    executor: DataPlaneExecutor,
}

impl DataPlaneListener {
    pub fn bind(addr: SocketAddr, state: impl Into<PathBuf>) -> Result<Self, String> {
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
                    let response =
                        serve_data_stream(stream, &self.codec, &self.adapter, &self.executor)?;
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

    fn execute(&self, envelope: RequestEnvelope) -> DataPlaneResponse {
        match envelope.protocol {
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
            protocol => DataPlaneResponse::Rejected {
                protocol: protocol.as_str(),
                reason:
                    "data-plane execution handoff is currently available for kv and blob requests"
                        .to_string(),
            },
        }
    }

    fn execute_kv(&self, envelope: RequestEnvelope) -> RsfResult<DataPlaneResponse> {
        let command = parse_kv_command(&envelope.operation, &envelope.body)?;
        let mut runtime = self.open_kv_runtime()?;
        match command {
            KvCommand::Get { key } => {
                let value = runtime.kv.get(key.as_bytes());
                Ok(DataPlaneResponse::Executed {
                    protocol: envelope.protocol.as_str(),
                    operation_kind: operation_kind_label(envelope.operation_kind),
                    surface: "kv",
                    version: value.map(|value| value.version.0),
                    bytes: value.map_or(0, |value| value.value.len() as u64),
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
        let mut runtime = self.open_blob_runtime()?;
        match command {
            BlobCommand::Put { name: _, bytes } => {
                let record = runtime
                    .blobs
                    .put(&mut runtime.router, PlacementGroupId(1), bytes)?;
                Ok(DataPlaneResponse::Executed {
                    protocol: envelope.protocol.as_str(),
                    operation_kind: operation_kind_label(envelope.operation_kind),
                    surface: "blob",
                    version: Some(record.pin_version.0),
                    bytes: record.bytes,
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
                    detail: format!("hit id=blake3:{}", hex_bytes(id.as_bytes())),
                })
            }
        }
    }

    fn open_kv_runtime(&self) -> RsfResult<KvRuntime> {
        let (router, epoch) = self.open_router()?;
        let kv = DurableKvEngine::open(self.state.join("kv"), epoch)?;
        Ok(KvRuntime { router, kv })
    }

    fn open_blob_runtime(&self) -> RsfResult<BlobRuntime> {
        let (router, _) = self.open_router()?;
        let blobs = DurableBlobEngine::open(self.state.join("blob"))?;
        Ok(BlobRuntime { router, blobs })
    }

    fn open_router(&self) -> RsfResult<(VsrBackedFabricRouter, Epoch)> {
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
        let router = VsrBackedFabricRouter::open(
            self.state.join("protocol"),
            [PlacementGroupId(1)],
            epoch,
            primary,
            voters,
        )?;
        Ok((router, epoch))
    }
}

struct KvRuntime {
    router: VsrBackedFabricRouter,
    kv: DurableKvEngine,
}

struct BlobRuntime {
    router: VsrBackedFabricRouter,
    blobs: DurableBlobEngine,
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

fn serve_data_stream(
    mut stream: TcpStream,
    codec: &WireCodec,
    adapter: &WireAdapter,
    executor: &DataPlaneExecutor,
) -> Result<DataPlaneResponse, String> {
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .map_err(|err| format!("failed to configure data read timeout: {err}"))?;
    stream
        .set_write_timeout(Some(Duration::from_secs(2)))
        .map_err(|err| format!("failed to configure data write timeout: {err}"))?;

    let response = read_data_frame(&mut stream)
        .and_then(|frame| {
            codec
                .decode_and_admit(&frame, adapter)
                .map_err(|err| err.message)
        })
        .map(|decision| data_plane_response(decision, executor))
        .unwrap_or_else(|reason| DataPlaneResponse::Corrupt { reason });
    let body = render_data_plane_response(&response);
    stream
        .write_all(body.as_bytes())
        .map_err(|err| format!("failed to write daemon data response: {err}"))?;
    stream
        .flush()
        .map_err(|err| format!("failed to flush daemon data response: {err}"))?;
    Ok(response)
}

fn read_data_frame(stream: &mut TcpStream) -> Result<Vec<u8>, String> {
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

fn data_plane_response(decision: WireDecision, executor: &DataPlaneExecutor) -> DataPlaneResponse {
    match decision {
        WireDecision::Accepted(envelope) => executor.execute(envelope),
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
    let key = envelope
        .idempotency_key
        .as_ref()
        .ok_or_else(|| invalid("kv mutating data-plane request requires an idempotency key"))?;
    Ok(ClientRequestId::new(
        format!(
            "wire:{}:{}:{}",
            envelope.principal,
            envelope.namespace.as_deref().unwrap_or(""),
            key
        ),
        0,
    ))
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

fn render_data_plane_response(response: &DataPlaneResponse) -> String {
    match response {
        DataPlaneResponse::Executed {
            protocol,
            operation_kind,
            surface,
            version,
            bytes,
            detail,
        } => format!(
            "RSF/1 executed protocol={} operation_kind={} surface={} version={} bytes={} detail={}\n",
            protocol,
            operation_kind,
            surface,
            version.map_or_else(|| "none".to_string(), |version| version.to_string()),
            bytes,
            detail
        ),
        DataPlaneResponse::Rejected { protocol, reason } => {
            format!("RSF/1 rejected protocol={} reason={}\n", protocol, reason)
        }
        DataPlaneResponse::Corrupt { reason } => format!("RSF/1 corrupt reason={reason}\n"),
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
        } => writeln!(
            out,
            "daemon data peer={} status=executed protocol={} operation_kind={} surface={} version={} bytes={} detail={}",
            peer,
            protocol,
            operation_kind,
            surface,
            version.map_or_else(|| "none".to_string(), |version| version.to_string()),
            bytes,
            detail
        ),
        DataPlaneResponse::Rejected { protocol, reason } => writeln!(
            out,
            "daemon data peer={} status=rejected protocol={} reason={}",
            peer, protocol, reason
        ),
        DataPlaneResponse::Corrupt { reason } => {
            writeln!(
                out,
                "daemon data peer={} status=corrupt reason={}",
                peer, reason
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
}
