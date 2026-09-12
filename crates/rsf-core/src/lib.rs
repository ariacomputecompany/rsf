#![forbid(unsafe_code)]

use std::fmt;
use std::str::FromStr;

pub mod blob;
pub mod bytes;
pub mod catalog;
pub mod cluster;
pub mod digest;
pub mod identity;
pub mod object;
pub mod placement;
pub mod relational;
pub mod storage;
pub mod time;
pub mod transaction;

pub use bytes::{ByteBuf, KeyBytes, KeyRangeBytes, ValueBytes};
pub use digest::{Digest, DigestAlgorithm};
pub use identity::{
    ClusterId, FailureDomainId, LayoutId, NodeIncarnation, OperationId, PolicyId, PolicyVersion,
    RegionId, SurfaceId, TraceId,
};
pub use time::{CommitClock, CommitStamp, DurableTimestamp};

pub type Bytes = Vec<u8>;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct NodeId(pub u64);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Epoch(pub u64);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PlacementGroupId(pub u64);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct LogIndex(pub u64);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CommitVersion(pub u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommitConsistency {
    Strict,
    AvailabilityFirst,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommitEvidence {
    pub placement_group: PlacementGroupId,
    pub log_index: LogIndex,
    pub version: CommitVersion,
    pub consistency: CommitConsistency,
    pub operation_id: OperationId,
}

impl CommitVersion {
    pub const GENESIS: Self = Self(0);
    pub const FIRST: Self = Self(1);

    pub fn checked_next(self) -> RsfResult<Self> {
        let next = self
            .0
            .checked_add(1)
            .ok_or_else(|| RsfError::new(ErrorCode::InvalidInput, "commit version overflowed"))?;
        Ok(Self(next))
    }

    pub fn is_genesis(self) -> bool {
        self == Self::GENESIS
    }

    pub fn is_committed(self) -> bool {
        self.0 > 0
    }

    pub fn validate_committed(self) -> RsfResult<Self> {
        if self.is_genesis() {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                "committed version must be greater than genesis",
            ));
        }
        Ok(self)
    }
}

macro_rules! numeric_id_impls {
    ($ty:ident, $label:literal) => {
        impl fmt::Display for $ty {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        impl FromStr for $ty {
            type Err = RsfError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                let parsed = value.parse::<u64>().map_err(|_| {
                    RsfError::new(
                        ErrorCode::InvalidInput,
                        format!("{} must be an unsigned integer", $label),
                    )
                })?;
                Ok(Self(parsed))
            }
        }
    };
}

numeric_id_impls!(NodeId, "node id");
numeric_id_impls!(Epoch, "epoch");
numeric_id_impls!(PlacementGroupId, "placement group id");
numeric_id_impls!(LogIndex, "log index");
numeric_id_impls!(CommitVersion, "commit version");

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RsfError {
    pub code: ErrorCode,
    pub message: String,
}

impl RsfError {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn category(&self) -> ErrorCategory {
        self.code.category()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorCode {
    InvalidInput,
    NotPrimary,
    QuorumUnavailable,
    CorruptionDetected,
    StaleEpoch,
    Unimplemented,
}

impl ErrorCode {
    pub fn category(self) -> ErrorCategory {
        match self {
            Self::InvalidInput | Self::Unimplemented => ErrorCategory::Configuration,
            Self::NotPrimary | Self::QuorumUnavailable | Self::StaleEpoch => {
                ErrorCategory::Coordination
            }
            Self::CorruptionDetected => ErrorCategory::DataIntegrity,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorCategory {
    Configuration,
    Coordination,
    DataIntegrity,
}

pub type RsfResult<T> = Result<T, RsfError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numeric_ids_parse_and_display() {
        assert_eq!("42".parse::<NodeId>().expect("node"), NodeId(42));
        assert_eq!(PlacementGroupId(7).to_string(), "7");
    }

    #[test]
    fn commit_versions_expose_genesis_and_checked_successor_contracts() {
        assert!(CommitVersion::GENESIS.is_genesis());
        assert_eq!(
            CommitVersion::GENESIS.checked_next().expect("next"),
            CommitVersion::FIRST
        );
        assert_eq!(
            CommitVersion::GENESIS
                .validate_committed()
                .expect_err("genesis is not committed")
                .code,
            ErrorCode::InvalidInput
        );
    }

    #[test]
    fn error_codes_map_to_operator_categories() {
        assert_eq!(
            ErrorCode::CorruptionDetected.category(),
            ErrorCategory::DataIntegrity
        );
        assert_eq!(
            ErrorCode::StaleEpoch.category(),
            ErrorCategory::Coordination
        );
        assert_eq!(
            ErrorCode::InvalidInput.category(),
            ErrorCategory::Configuration
        );
    }
}
