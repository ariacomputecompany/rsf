//! Correlated binary refusal for a control transfer lacking bounded BEGIN.
use super::*;
const DOMAIN: &[u8; 6] = b"RSFHB1";
const SIZE: usize = DOMAIN.len() + 1 + 8 + 32;
pub(in crate::replication) const BEGIN_REQUIRED_RESPONSE: u8 = 254;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistoryBeginRequired {
    kind: u8,
    pg: PlacementGroupId,
    snapshot: [u8; 32],
}
impl HistoryBeginRequired {
    pub fn placement_group(&self) -> PlacementGroupId {
        self.pg
    }
    pub fn snapshot(&self) -> [u8; 32] {
        self.snapshot
    }
    pub fn is_finish(&self) -> bool {
        self.kind == HISTORY_FINISH
    }
    pub(in crate::replication) fn error(
        kind: u8,
        pg: PlacementGroupId,
        snapshot: [u8; 32],
    ) -> io::Error {
        debug_assert!(matches!(kind, HISTORY_RECORD | HISTORY_FINISH));
        io::Error::new(io::ErrorKind::InvalidData, Self { kind, pg, snapshot })
    }
    pub(in crate::replication) fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(SIZE);
        bytes.extend_from_slice(DOMAIN);
        bytes.push(self.kind);
        bytes.extend_from_slice(&self.pg.0.to_le_bytes());
        bytes.extend_from_slice(&self.snapshot);
        bytes
    }
}
impl std::fmt::Display for HistoryBeginRequired {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "control history requires bounded BEGIN for PG {} before {}",
            self.pg.0,
            if self.is_finish() { "FINISH" } else { "RECORD" }
        )
    }
}
impl std::error::Error for HistoryBeginRequired {}
pub(in crate::replication) fn from_error(error: &io::Error) -> Option<&HistoryBeginRequired> {
    error.get_ref()?.downcast_ref()
}
pub(in crate::replication) fn decode_response(
    kind: u8,
    bytes: &[u8],
    request: &[u8],
) -> io::Result<io::Error> {
    if !matches!(kind, HISTORY_RECORD | HISTORY_FINISH)
        || bytes.len() != SIZE
        || &bytes[..DOMAIN.len()] != DOMAIN
        || bytes[DOMAIN.len()] != kind
        || request.len() < 40
        || (kind == HISTORY_FINISH && request.len() != 40)
        || (kind == HISTORY_RECORD && request.len() < 48)
    {
        return Err(invalid(
            "malformed or unrelated history BEGIN-required response",
        ));
    }
    let offset = DOMAIN.len() + 1;
    if bytes[offset..offset + 40] != request[..40] {
        return Err(invalid(
            "history BEGIN-required response differs from requested PG/snapshot",
        ));
    }
    Ok(HistoryBeginRequired::error(
        kind,
        PlacementGroupId(u64_at(request, 0)?),
        request[8..40].try_into().unwrap(),
    ))
}
