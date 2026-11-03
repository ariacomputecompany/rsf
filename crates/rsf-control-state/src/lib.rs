//! Committed control state, independent of local administration and Fabric versions.
//! This crate does not initiate membership, authenticate remote reports, grant a
//! primary lease, or materialize application state. Those remain caller duties.
#![forbid(unsafe_code)]

mod codec;
mod model;
mod replay;

pub use model::*;
pub use replay::{CommittedAdmission, ControlOutcome, ControlState, Rejection};

use rsf_core::{ErrorCode, RsfError, RsfResult};

/// Deliberate first-regime bounds; exceeding them requires a new qualified format.
pub const MAX_NODES: usize = 64;
pub const MAX_GROUPS: usize = 64;
pub const MAX_COMMAND_BYTES: usize = rsf_vsr::MAX_REPLICATED_COMMAND_BYTES;
pub const MAX_CONTROL_RECORDS: usize = 16_384;
pub const MAX_REPLAY_BYTES: usize = 64 * 1024 * 1024;
pub const MAX_RECEIPTS: usize = 4096;
pub const MAX_TRANSITIONS: usize = 256;
pub const MAX_IDENTITY_BYTES: usize = 512;

fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message)
}
fn corrupt(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::CorruptionDetected, message)
}
fn nonzero(value: &[u8; 32]) -> RsfResult<()> {
    if *value == [0; 32] {
        Err(invalid("control digest/identifier must be nonzero"))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests;
