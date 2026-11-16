//! Namespace-bound, principal-owned SQL sessions. Session state is a bounded
//! statement log; materialized SQL data changes only at one certified COMMIT.
use super::*;
use serde::{Deserialize, Serialize};

mod dependencies;
mod execute;
pub(super) mod model;
mod plan;
mod validation;
pub(super) use model::{SessionProposal, SessionRequest, SessionState};
pub(super) use plan::validate_serializable_snapshot;
pub(super) use plan::{prepare, propose, validate_recovered};

pub const MAX_ACTIVE_SQL_SESSIONS: usize = 128;
pub const MAX_SQL_SESSION_STATEMENTS: usize = 64;
pub const MAX_SQL_SESSION_BYTES: usize = 64 * 1024;
pub const MAX_SQL_SAVEPOINTS: usize = 32;
const CONTEXT_MAGIC: &[u8] = b"RSF_SQL_SESSION_V1\n";
const PROPOSAL_MAGIC: &[u8] = b"RSFSP1";
const SERIALIZABLE_PROPOSAL_MAGIC: &[u8] = b"RSFSP2";
const TRACKED_PROPOSAL_MAGIC: &[u8] = b"RSFSP3";
const MAX_SQL_SESSION_DEPENDENCIES: usize = 128;
const MAX_SQL_SESSION_READ_BYTES: usize = 24 * 1024;
const MAX_PROPOSAL_BYTES: usize = 384 * 1024;

fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message)
}
fn corrupt(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::CorruptionDetected, message)
}
fn encode<T: Serialize>(value: &T) -> RsfResult<Vec<u8>> {
    serde_json::to_vec(value).map_err(|err| invalid(format!("cannot encode SQL session: {err}")))
}
fn decode<T: serde::de::DeserializeOwned + Serialize>(bytes: &[u8]) -> RsfResult<T> {
    let value: T = serde_json::from_slice(bytes)
        .map_err(|err| corrupt(format!("invalid SQL session encoding: {err}")))?;
    if encode(&value)? != bytes {
        return Err(corrupt("noncanonical SQL session encoding"));
    }
    Ok(value)
}
fn key(owner: &str, session: &str) -> String {
    format!(
        "{}:{}",
        hex_bytes(owner.as_bytes()),
        hex_bytes(session.as_bytes())
    )
}
fn validate_name(value: &str, label: &str, limit: usize) -> RsfResult<()> {
    if value.is_empty() || value.len() > limit || value.chars().any(char::is_control) {
        return Err(invalid(format!("invalid SQL {label}")));
    }
    Ok(())
}
fn validate_token(token: &str) -> RsfResult<()> {
    if token.len() != 64
        || !token
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(invalid("invalid SQL transaction token"));
    }
    Ok(())
}

/// Optional native SQL request body. BEGIN omits a token; subsequent requests
/// must use the token returned by BEGIN, preventing stale commits after reuse.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SqlSessionContext {
    pub name: String,
    pub transaction: Option<String>,
}
impl SqlSessionContext {
    pub fn encode(&self) -> RsfResult<Vec<u8>> {
        self.validate()?;
        let mut bytes = CONTEXT_MAGIC.to_vec();
        bytes.extend(encode(self)?);
        Ok(bytes)
    }
    pub fn decode(bytes: &[u8]) -> RsfResult<Self> {
        if bytes.len() > 4096 {
            return Err(invalid("SQL session context exceeds limit"));
        }
        let bytes = bytes
            .strip_prefix(CONTEXT_MAGIC)
            .ok_or_else(|| invalid("unknown SQL session context"))?;
        let value: Self = decode(bytes)?;
        value.validate()?;
        Ok(value)
    }
    fn validate(&self) -> RsfResult<()> {
        validate_name(&self.name, "session name", 128)?;
        if let Some(token) = &self.transaction {
            validate_token(token)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
