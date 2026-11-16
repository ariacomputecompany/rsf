#![forbid(unsafe_code)]

pub mod sql_stream;

use std::collections::BTreeSet;

use rsf_core::{ErrorCode, RsfError, RsfResult};

const FRAME_MAGIC: &[u8; 4] = b"RSFW";
const FRAME_VERSION: u8 = 1;
const MAX_PRINCIPAL_BYTES: usize = 256;
const MAX_NAMESPACE_BYTES: usize = 256;
const MAX_OPERATION_BYTES: usize = 4096;
const MAX_IDEMPOTENCY_KEY_BYTES: usize = 256;
const MAX_BODY_BYTES: u64 = 1_073_741_824;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum WireProtocol {
    Sql,
    Kv,
    Object,
    Blob,
    Admin,
}

impl WireProtocol {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Sql => "sql-wire",
            Self::Kv => "kv-http",
            Self::Object => "object-s3",
            Self::Blob => "blob-stream",
            Self::Admin => "admin-rpc",
        }
    }

    fn to_u8(self) -> u8 {
        match self {
            Self::Sql => 1,
            Self::Kv => 2,
            Self::Object => 3,
            Self::Blob => 4,
            Self::Admin => 5,
        }
    }

    fn from_u8(value: u8) -> RsfResult<Self> {
        match value {
            1 => Ok(Self::Sql),
            2 => Ok(Self::Kv),
            3 => Ok(Self::Object),
            4 => Ok(Self::Blob),
            5 => Ok(Self::Admin),
            other => Err(corrupt(format!("unknown wire protocol id {other}"))),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawFrame {
    pub protocol: WireProtocol,
    pub principal: String,
    pub namespace: Option<String>,
    pub operation: String,
    pub idempotency_key: Option<String>,
    pub body_bytes: u64,
    pub body: Vec<u8>,
}

impl RawFrame {
    pub fn new(
        protocol: WireProtocol,
        principal: impl Into<String>,
        namespace: Option<String>,
        operation: impl Into<String>,
        idempotency_key: Option<String>,
        body_bytes: u64,
    ) -> Self {
        Self {
            protocol,
            principal: principal.into(),
            namespace,
            operation: operation.into(),
            idempotency_key,
            body_bytes,
            body: Vec::new(),
        }
    }

    pub fn with_body(
        protocol: WireProtocol,
        principal: impl Into<String>,
        namespace: Option<String>,
        operation: impl Into<String>,
        idempotency_key: Option<String>,
        body: impl Into<Vec<u8>>,
    ) -> Self {
        let body = body.into();
        Self {
            protocol,
            principal: principal.into(),
            namespace,
            operation: operation.into(),
            idempotency_key,
            body_bytes: body.len() as u64,
            body,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WireOperationKind {
    Read,
    Write,
    Admin,
}

impl WireOperationKind {
    pub fn mutates_state(self) -> bool {
        !matches!(self, Self::Read)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestEnvelope {
    pub protocol: WireProtocol,
    pub principal: String,
    pub namespace: Option<String>,
    pub operation: String,
    pub operation_kind: WireOperationKind,
    pub idempotency_key: Option<String>,
    pub body_bytes: u64,
    pub body: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WireDecision {
    Accepted(RequestEnvelope),
    Rejected {
        protocol: WireProtocol,
        reason: String,
    },
}

impl WireDecision {
    pub fn is_accepted(&self) -> bool {
        matches!(self, Self::Accepted(_))
    }
}

#[derive(Clone, Debug, Default)]
pub struct WireAdapter;

impl WireAdapter {
    pub fn decode(&self, frame: RawFrame) -> RsfResult<WireDecision> {
        let principal = match normalize_required(&frame.principal, MAX_PRINCIPAL_BYTES, "principal")
        {
            Ok(value) => value,
            Err(err) => return Ok(reject(frame.protocol, err.message)),
        };
        let operation = match normalize_required(&frame.operation, MAX_OPERATION_BYTES, "operation")
        {
            Ok(value) => value,
            Err(err) => return Ok(reject(frame.protocol, err.message)),
        };
        let operation_kind = classify_operation(frame.protocol, &operation);
        let namespace = match normalize_optional(
            frame.namespace.as_deref(),
            MAX_NAMESPACE_BYTES,
            "namespace",
        ) {
            Ok(value) => value,
            Err(err) => return Ok(reject(frame.protocol, err.message)),
        };
        let idempotency_key = match normalize_optional(
            frame.idempotency_key.as_deref(),
            MAX_IDEMPOTENCY_KEY_BYTES,
            "idempotency key",
        ) {
            Ok(value) => value,
            Err(err) => return Ok(reject(frame.protocol, err.message)),
        };

        if principal.is_empty() {
            return Ok(reject(frame.protocol, "principal is required"));
        }
        if operation.is_empty() {
            return Ok(reject(frame.protocol, "operation is required"));
        }
        match frame.protocol {
            WireProtocol::Sql | WireProtocol::Kv | WireProtocol::Object | WireProtocol::Blob => {
                if namespace.is_none() {
                    return Ok(reject(frame.protocol, "namespace is required"));
                }
            }
            WireProtocol::Admin => {}
        }
        if operation_kind.mutates_state()
            && idempotency_key.is_none()
            && requires_idempotency(frame.protocol)
        {
            return Ok(reject(
                frame.protocol,
                "idempotency key is required for mutating protocol",
            ));
        }
        if frame.body_bytes > MAX_BODY_BYTES {
            return Ok(reject(frame.protocol, "request body exceeds wire limit"));
        }
        if frame.body_bytes != frame.body.len() as u64 {
            return Ok(reject(
                frame.protocol,
                "request body byte count must match payload length",
            ));
        }

        Ok(WireDecision::Accepted(RequestEnvelope {
            protocol: frame.protocol,
            principal,
            namespace,
            operation,
            operation_kind,
            idempotency_key,
            body_bytes: frame.body_bytes,
            body: frame.body,
        }))
    }
}

#[derive(Clone, Debug, Default)]
pub struct WireCodec;

impl WireCodec {
    pub fn encode(&self, frame: &RawFrame) -> RsfResult<Vec<u8>> {
        let mut out = Vec::new();
        out.extend_from_slice(FRAME_MAGIC);
        out.push(FRAME_VERSION);
        out.push(frame.protocol.to_u8());
        write_string(&mut out, &frame.principal, MAX_PRINCIPAL_BYTES, "principal")?;
        write_optional_string(
            &mut out,
            frame.namespace.as_deref(),
            MAX_NAMESPACE_BYTES,
            "namespace",
        )?;
        write_string(&mut out, &frame.operation, MAX_OPERATION_BYTES, "operation")?;
        write_optional_string(
            &mut out,
            frame.idempotency_key.as_deref(),
            MAX_IDEMPOTENCY_KEY_BYTES,
            "idempotency key",
        )?;
        out.extend_from_slice(&frame.body_bytes.to_le_bytes());
        if frame.body_bytes != frame.body.len() as u64 {
            return Err(invalid("request body byte count must match payload length"));
        }
        out.extend_from_slice(&frame.body);
        Ok(out)
    }

    pub fn decode(&self, bytes: &[u8]) -> RsfResult<RawFrame> {
        let mut offset = 0;
        take_magic(bytes, &mut offset)?;
        let version = read_u8(bytes, &mut offset)?;
        if version != FRAME_VERSION {
            return Err(corrupt(format!("unsupported wire frame version {version}")));
        }
        let protocol = WireProtocol::from_u8(read_u8(bytes, &mut offset)?)?;
        let principal = read_string(bytes, &mut offset, MAX_PRINCIPAL_BYTES, "principal")?;
        let namespace = read_optional_string(bytes, &mut offset, MAX_NAMESPACE_BYTES, "namespace")?;
        let operation = read_string(bytes, &mut offset, MAX_OPERATION_BYTES, "operation")?;
        let idempotency_key = read_optional_string(
            bytes,
            &mut offset,
            MAX_IDEMPOTENCY_KEY_BYTES,
            "idempotency key",
        )?;
        let body_bytes = read_u64(bytes, &mut offset)?;
        if body_bytes > MAX_BODY_BYTES {
            return Err(corrupt("request body exceeds wire limit"));
        }
        let body_len = usize::try_from(body_bytes)
            .map_err(|_| corrupt("request body length does not fit this platform"))?;
        let body = take(bytes, &mut offset, body_len)?.to_vec();
        if offset != bytes.len() {
            return Err(corrupt("wire frame has trailing bytes"));
        }
        Ok(RawFrame {
            protocol,
            principal,
            namespace,
            operation,
            idempotency_key,
            body_bytes,
            body,
        })
    }

    pub fn decode_and_admit(&self, bytes: &[u8], adapter: &WireAdapter) -> RsfResult<WireDecision> {
        adapter.decode(self.decode(bytes)?)
    }
}

fn reject(protocol: WireProtocol, reason: impl Into<String>) -> WireDecision {
    WireDecision::Rejected {
        protocol,
        reason: reason.into(),
    }
}

pub fn protocol_count(decisions: &[WireDecision]) -> usize {
    let mut protocols = std::collections::BTreeSet::new();
    for decision in decisions {
        let protocol = match decision {
            WireDecision::Accepted(envelope) => envelope.protocol,
            WireDecision::Rejected { protocol, .. } => *protocol,
        };
        protocols.insert(protocol);
    }
    protocols.len()
}

pub fn accepted_body_bytes(decisions: &[WireDecision]) -> u64 {
    decisions
        .iter()
        .filter_map(|decision| match decision {
            WireDecision::Accepted(envelope) => Some(envelope.body_bytes),
            WireDecision::Rejected { .. } => None,
        })
        .sum()
}

pub fn validate_decisions(decisions: &[WireDecision]) -> RsfResult<()> {
    if decisions.is_empty() {
        return Err(RsfError::new(
            ErrorCode::InvalidInput,
            "wire decision set must not be empty",
        ));
    }
    let mut mutating_keys = BTreeSet::new();
    for decision in decisions {
        match decision {
            WireDecision::Accepted(envelope) => {
                validate_envelope(envelope)?;
                if envelope.operation_kind.mutates_state()
                    && requires_idempotency(envelope.protocol)
                    && !mutating_keys.insert((
                        envelope.protocol,
                        envelope.namespace.clone(),
                        envelope
                            .idempotency_key
                            .as_ref()
                            .expect("validated mutating envelope has idempotency key")
                            .clone(),
                    ))
                {
                    return Err(invalid(
                        "wire decision set contains duplicate mutating idempotency key",
                    ));
                }
            }
            WireDecision::Rejected { reason, .. } => {
                validate_field(reason, MAX_OPERATION_BYTES, "rejection reason")?;
                if reason.trim().is_empty() {
                    return Err(invalid("wire rejection reason must not be empty"));
                }
            }
        }
    }
    Ok(())
}

fn validate_envelope(envelope: &RequestEnvelope) -> RsfResult<()> {
    if normalize_required(&envelope.principal, MAX_PRINCIPAL_BYTES, "principal")?
        != envelope.principal
    {
        return Err(invalid("wire envelope principal is not canonical"));
    }
    if normalize_required(&envelope.operation, MAX_OPERATION_BYTES, "operation")?
        != envelope.operation
    {
        return Err(invalid("wire envelope operation is not canonical"));
    }
    match envelope.protocol {
        WireProtocol::Sql | WireProtocol::Kv | WireProtocol::Object | WireProtocol::Blob => {
            let Some(namespace) = envelope.namespace.as_deref() else {
                return Err(invalid("wire envelope namespace is required"));
            };
            if normalize_required(namespace, MAX_NAMESPACE_BYTES, "namespace")? != namespace {
                return Err(invalid("wire envelope namespace is not canonical"));
            }
        }
        WireProtocol::Admin => {
            if envelope.namespace.is_some() {
                return Err(invalid("wire admin envelope must not carry a namespace"));
            }
        }
    }
    if classify_operation(envelope.protocol, &envelope.operation) != envelope.operation_kind {
        return Err(invalid(
            "wire envelope operation kind does not match operation text",
        ));
    }
    if envelope.operation_kind.mutates_state() && requires_idempotency(envelope.protocol) {
        let Some(idempotency_key) = envelope.idempotency_key.as_deref() else {
            return Err(invalid(
                "wire envelope mutating protocol requires idempotency key",
            ));
        };
        if normalize_required(
            idempotency_key,
            MAX_IDEMPOTENCY_KEY_BYTES,
            "idempotency key",
        )? != idempotency_key
        {
            return Err(invalid("wire envelope idempotency key is not canonical"));
        }
    }
    if envelope.body_bytes > MAX_BODY_BYTES {
        return Err(invalid("wire envelope body exceeds wire limit"));
    }
    if envelope.body_bytes != envelope.body.len() as u64 {
        return Err(invalid(
            "wire envelope body byte count must match payload length",
        ));
    }
    Ok(())
}

fn requires_idempotency(protocol: WireProtocol) -> bool {
    matches!(
        protocol,
        WireProtocol::Kv | WireProtocol::Object | WireProtocol::Blob
    )
}

fn classify_operation(protocol: WireProtocol, operation: &str) -> WireOperationKind {
    if protocol == WireProtocol::Admin {
        return WireOperationKind::Admin;
    }
    let verb = operation
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .trim_matches(|ch: char| ch == ':' || ch == '/')
        .to_ascii_uppercase();
    match verb.as_str() {
        "GET" | "HEAD" | "LIST" | "READ" | "SELECT" | "DESCRIBE" | "SHOW" | "EXPLAIN" => {
            WireOperationKind::Read
        }
        _ => WireOperationKind::Write,
    }
}

fn normalize_required(value: &str, max_len: usize, label: &str) -> RsfResult<String> {
    validate_field(value, max_len, label)?;
    Ok(value.trim().to_string())
}

fn normalize_optional(
    value: Option<&str>,
    max_len: usize,
    label: &str,
) -> RsfResult<Option<String>> {
    let Some(value) = value else {
        return Ok(None);
    };
    validate_field(value, max_len, label)?;
    let normalized = value.trim();
    if normalized.is_empty() {
        return Ok(None);
    }
    Ok(Some(normalized.to_string()))
}

fn validate_field(value: &str, max_len: usize, label: &str) -> RsfResult<()> {
    if value.len() > max_len {
        return Err(invalid(format!("{label} exceeds {max_len} bytes")));
    }
    if value
        .chars()
        .any(|ch| ch.is_control() && ch != '\t' && ch != '\n' && ch != '\r')
    {
        return Err(invalid(format!("{label} contains control characters")));
    }
    Ok(())
}

fn write_string(out: &mut Vec<u8>, value: &str, max_len: usize, label: &str) -> RsfResult<()> {
    validate_field(value, max_len, label)?;
    let len = u16::try_from(value.len()).map_err(|_| invalid(format!("{label} is too large")))?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

fn write_optional_string(
    out: &mut Vec<u8>,
    value: Option<&str>,
    max_len: usize,
    label: &str,
) -> RsfResult<()> {
    match value {
        Some(value) => {
            out.push(1);
            write_string(out, value, max_len, label)
        }
        None => {
            out.push(0);
            Ok(())
        }
    }
}

fn read_string(bytes: &[u8], offset: &mut usize, max_len: usize, label: &str) -> RsfResult<String> {
    let len = read_u16(bytes, offset)? as usize;
    if len > max_len {
        return Err(corrupt(format!("{label} length exceeds {max_len} bytes")));
    }
    let raw = take(bytes, offset, len)?;
    let value = String::from_utf8(raw.to_vec())
        .map_err(|_| corrupt(format!("{label} is not valid utf8")))?;
    validate_field(&value, max_len, label).map_err(|err| corrupt(err.message))?;
    Ok(value)
}

fn read_optional_string(
    bytes: &[u8],
    offset: &mut usize,
    max_len: usize,
    label: &str,
) -> RsfResult<Option<String>> {
    match read_u8(bytes, offset)? {
        0 => Ok(None),
        1 => Ok(Some(read_string(bytes, offset, max_len, label)?)),
        other => Err(corrupt(format!(
            "{label} optional presence flag must be 0 or 1, got {other}"
        ))),
    }
}

fn take_magic(bytes: &[u8], offset: &mut usize) -> RsfResult<()> {
    let magic = take(bytes, offset, FRAME_MAGIC.len())?;
    if magic != FRAME_MAGIC {
        return Err(corrupt("wire frame has invalid magic"));
    }
    Ok(())
}

fn read_u64(bytes: &[u8], offset: &mut usize) -> RsfResult<u64> {
    let raw = take(bytes, offset, 8)?;
    let mut buf = [0; 8];
    buf.copy_from_slice(raw);
    Ok(u64::from_le_bytes(buf))
}

fn read_u16(bytes: &[u8], offset: &mut usize) -> RsfResult<u16> {
    let raw = take(bytes, offset, 2)?;
    let mut buf = [0; 2];
    buf.copy_from_slice(raw);
    Ok(u16::from_le_bytes(buf))
}

fn read_u8(bytes: &[u8], offset: &mut usize) -> RsfResult<u8> {
    Ok(*take(bytes, offset, 1)?
        .first()
        .expect("one requested byte must exist"))
}

fn take<'a>(bytes: &'a [u8], offset: &mut usize, len: usize) -> RsfResult<&'a [u8]> {
    let end = offset
        .checked_add(len)
        .ok_or_else(|| corrupt("wire frame offset overflow"))?;
    if end > bytes.len() {
        return Err(corrupt("wire frame is truncated"));
    }
    let out = &bytes[*offset..end];
    *offset = end;
    Ok(out)
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

    #[test]
    fn accepts_sql_frame_without_idempotency_key() {
        let decision = WireAdapter
            .decode(RawFrame::new(
                WireProtocol::Sql,
                "app-client",
                Some("app".to_string()),
                "SELECT * FROM users",
                None,
                0,
            ))
            .unwrap();

        assert!(decision.is_accepted());
        let WireDecision::Accepted(envelope) = decision else {
            panic!("expected accepted envelope");
        };
        assert_eq!(envelope.operation_kind, WireOperationKind::Read);
    }

    #[test]
    fn requires_idempotency_for_object_mutation() {
        let decision = WireAdapter
            .decode(RawFrame::new(
                WireProtocol::Object,
                "app-client",
                Some("app".to_string()),
                "PUT /media/hero.png",
                None,
                0,
            ))
            .unwrap();

        assert_eq!(
            decision,
            WireDecision::Rejected {
                protocol: WireProtocol::Object,
                reason: "idempotency key is required for mutating protocol".to_string()
            }
        );
    }

    #[test]
    fn counts_protocols_and_body_bytes() {
        let adapter = WireAdapter;
        let decisions = vec![
            adapter
                .decode(RawFrame::new(
                    WireProtocol::Sql,
                    "app-client",
                    Some("app".to_string()),
                    "SELECT * FROM users",
                    None,
                    0,
                ))
                .unwrap(),
            adapter
                .decode(RawFrame::with_body(
                    WireProtocol::Blob,
                    "app-client",
                    Some("app".to_string()),
                    "PUT sha256:content",
                    Some("blob-1".to_string()),
                    vec![0x5a; 512],
                ))
                .unwrap(),
        ];

        validate_decisions(&decisions).unwrap();
        assert_eq!(protocol_count(&decisions), 2);
        assert_eq!(accepted_body_bytes(&decisions), 512);
    }

    #[test]
    fn canonical_codec_round_trips_and_admits_normalized_frame() {
        let codec = WireCodec;
        let adapter = WireAdapter;
        let frame = RawFrame::with_body(
            WireProtocol::Object,
            " app-client ",
            Some(" app ".to_string()),
            " PUT /media/hero.png ",
            Some(" object-1 ".to_string()),
            b"image-bytes".to_vec(),
        );

        let encoded = codec.encode(&frame).expect("encode");
        let decoded = codec.decode(&encoded).expect("decode");
        let decision = codec
            .decode_and_admit(&encoded, &adapter)
            .expect("decode and admit");

        assert_eq!(decoded, frame);
        let WireDecision::Accepted(envelope) = decision else {
            panic!("expected accepted envelope");
        };
        assert_eq!(envelope.principal, "app-client");
        assert_eq!(envelope.namespace, Some("app".to_string()));
        assert_eq!(envelope.operation, "PUT /media/hero.png");
        assert_eq!(envelope.operation_kind, WireOperationKind::Write);
        assert_eq!(envelope.idempotency_key, Some("object-1".to_string()));
        assert_eq!(envelope.body, b"image-bytes".to_vec());
    }

    #[test]
    fn object_reads_do_not_require_idempotency_key() {
        let decision = WireAdapter
            .decode(RawFrame::new(
                WireProtocol::Object,
                "app-client",
                Some("app".to_string()),
                "GET /media/hero.png",
                None,
                0,
            ))
            .unwrap();

        let WireDecision::Accepted(envelope) = decision else {
            panic!("expected accepted object read");
        };
        assert_eq!(envelope.operation_kind, WireOperationKind::Read);
        assert_eq!(envelope.idempotency_key, None);
    }

    #[test]
    fn canonical_codec_rejects_trailing_bytes() {
        let codec = WireCodec;
        let mut encoded = codec
            .encode(&RawFrame::with_body(
                WireProtocol::Blob,
                "app-client",
                Some("app".to_string()),
                "PUT sha256:content",
                Some("blob-1".to_string()),
                b"blob bytes".to_vec(),
            ))
            .expect("encode");
        encoded.push(0xff);

        let err = codec.decode(&encoded).unwrap_err();

        assert_eq!(err.code, ErrorCode::CorruptionDetected);
        assert!(err.message.contains("trailing"));
    }

    #[test]
    fn rejects_body_length_without_payload_bytes() {
        let frame = RawFrame::new(
            WireProtocol::Blob,
            "app-client",
            Some("app".to_string()),
            "PUT sha256:content",
            Some("blob-1".to_string()),
            512,
        );

        let encode_err = WireCodec.encode(&frame).expect_err("encode mismatch");
        assert_eq!(encode_err.code, ErrorCode::InvalidInput);
        assert!(encode_err.message.contains("payload length"));

        let decision = WireAdapter.decode(frame).expect("adapter decision");
        assert_eq!(
            decision,
            WireDecision::Rejected {
                protocol: WireProtocol::Blob,
                reason: "request body byte count must match payload length".to_string()
            }
        );
    }

    #[test]
    fn canonical_codec_rejects_truncated_frame() {
        let codec = WireCodec;
        let encoded = codec
            .encode(&RawFrame::with_body(
                WireProtocol::Kv,
                "app-client",
                Some("app".to_string()),
                "WRITE config/a",
                Some("kv-1".to_string()),
                b"stored-value".to_vec(),
            ))
            .expect("encode");

        let err = codec.decode(&encoded[..encoded.len() - 3]).unwrap_err();

        assert_eq!(err.code, ErrorCode::CorruptionDetected);
        assert!(err.message.contains("truncated"));
    }

    #[test]
    fn decode_rejects_oversized_operation_before_acceptance() {
        let operation = "x".repeat(MAX_OPERATION_BYTES + 1);
        let decision = WireAdapter
            .decode(RawFrame::new(
                WireProtocol::Sql,
                "app-client",
                Some("app".to_string()),
                operation,
                None,
                0,
            ))
            .unwrap();

        assert_eq!(
            decision,
            WireDecision::Rejected {
                protocol: WireProtocol::Sql,
                reason: format!("operation exceeds {MAX_OPERATION_BYTES} bytes")
            }
        );
    }

    #[test]
    fn validate_decisions_rejects_noncanonical_accepted_envelope() {
        let decisions = vec![WireDecision::Accepted(RequestEnvelope {
            protocol: WireProtocol::Sql,
            principal: " app-client ".to_string(),
            namespace: Some("app".to_string()),
            operation: "SELECT * FROM users".to_string(),
            operation_kind: WireOperationKind::Read,
            idempotency_key: None,
            body_bytes: 0,
            body: Vec::new(),
        })];

        let err = validate_decisions(&decisions).expect_err("noncanonical envelope must fail");

        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(err.message.contains("principal is not canonical"));
    }

    #[test]
    fn validate_decisions_rejects_duplicate_mutating_idempotency_key() {
        let adapter = WireAdapter;
        let decisions = vec![
            adapter
                .decode(RawFrame::with_body(
                    WireProtocol::Kv,
                    "app-client",
                    Some("app".to_string()),
                    "WRITE config/a",
                    Some("kv-1".to_string()),
                    b"value-a".to_vec(),
                ))
                .unwrap(),
            adapter
                .decode(RawFrame::with_body(
                    WireProtocol::Kv,
                    "app-client",
                    Some("app".to_string()),
                    "WRITE config/b",
                    Some("kv-1".to_string()),
                    b"value-b".to_vec(),
                ))
                .unwrap(),
        ];

        let err = validate_decisions(&decisions).expect_err("duplicate idempotency must fail");

        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(err.message.contains("duplicate mutating idempotency key"));
    }

    #[test]
    fn validate_decisions_rejects_operation_kind_mismatch() {
        let decisions = vec![WireDecision::Accepted(RequestEnvelope {
            protocol: WireProtocol::Object,
            principal: "app-client".to_string(),
            namespace: Some("app".to_string()),
            operation: "GET /media/hero.png".to_string(),
            operation_kind: WireOperationKind::Write,
            idempotency_key: Some("object-1".to_string()),
            body_bytes: 0,
            body: Vec::new(),
        })];

        let err = validate_decisions(&decisions).expect_err("kind mismatch must fail");

        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(err.message.contains("operation kind"));
    }
}
