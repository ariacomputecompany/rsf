use std::fmt;
use std::str::FromStr;

use crate::{ErrorCode, RsfError, RsfResult};

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ClusterId(String);

impl ClusterId {
    pub fn new(value: impl Into<String>) -> RsfResult<Self> {
        parse_named_id("cluster id", value)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct NodeIncarnation(pub u64);

impl NodeIncarnation {
    pub fn new(value: u64) -> RsfResult<Self> {
        if value == 0 {
            return Err(invalid("node incarnation must be non-zero"));
        }
        Ok(Self(value))
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RegionId(String);

impl RegionId {
    pub fn new(value: impl Into<String>) -> RsfResult<Self> {
        parse_named_id("region id", value)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FailureDomainId(String);

impl FailureDomainId {
    pub fn new(value: impl Into<String>) -> RsfResult<Self> {
        parse_named_id("failure domain id", value)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PolicyId(String);

impl PolicyId {
    pub fn new(value: impl Into<String>) -> RsfResult<Self> {
        parse_named_id("policy id", value)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PolicyVersion(pub u64);

impl PolicyVersion {
    pub fn new(value: u64) -> RsfResult<Self> {
        if value == 0 {
            return Err(invalid("policy version must be non-zero"));
        }
        Ok(Self(value))
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct LayoutId(String);

impl LayoutId {
    pub fn new(value: impl Into<String>) -> RsfResult<Self> {
        parse_named_id("layout id", value)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SurfaceId(String);

impl SurfaceId {
    pub fn new(value: impl Into<String>) -> RsfResult<Self> {
        parse_named_id("surface id", value)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TraceId(String);

impl TraceId {
    pub fn new(value: impl Into<String>) -> RsfResult<Self> {
        parse_named_id("trace id", value)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OperationId(pub u64);

impl OperationId {
    pub fn new(value: u64) -> RsfResult<Self> {
        if value == 0 {
            return Err(invalid("operation id must be non-zero"));
        }
        Ok(Self(value))
    }
}

macro_rules! named_id_impls {
    ($ty:ident) => {
        impl fmt::Display for $ty {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl FromStr for $ty {
            type Err = RsfError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::new(value)
            }
        }
    };
}

named_id_impls!(ClusterId);
named_id_impls!(RegionId);
named_id_impls!(FailureDomainId);
named_id_impls!(PolicyId);
named_id_impls!(LayoutId);
named_id_impls!(SurfaceId);
named_id_impls!(TraceId);

macro_rules! numeric_id_display {
    ($ty:ident) => {
        impl fmt::Display for $ty {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }
    };
}

numeric_id_display!(NodeIncarnation);
numeric_id_display!(PolicyVersion);
numeric_id_display!(OperationId);

fn parse_named_id<T>(kind: &'static str, value: impl Into<String>) -> RsfResult<T>
where
    T: From<String>,
{
    let value = value.into();
    validate_named_id(kind, &value)?;
    Ok(T::from(value))
}

fn validate_named_id(kind: &'static str, value: &str) -> RsfResult<()> {
    if value.is_empty() {
        return Err(invalid(format!("{kind} must not be empty")));
    }
    if value.len() > 128 {
        return Err(invalid(format!("{kind} must be at most 128 bytes")));
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'/'))
    {
        return Err(invalid(format!(
            "{kind} may only contain ASCII letters, digits, '.', '-', '_', or '/'"
        )));
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message)
}

macro_rules! named_id_from_string {
    ($ty:ident) => {
        impl From<String> for $ty {
            fn from(value: String) -> Self {
                Self(value)
            }
        }
    };
}

named_id_from_string!(ClusterId);
named_id_from_string!(RegionId);
named_id_from_string!(FailureDomainId);
named_id_from_string!(PolicyId);
named_id_from_string!(LayoutId);
named_id_from_string!(SurfaceId);
named_id_from_string!(TraceId);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn named_ids_parse_and_display_stable_values() {
        let region = RegionId::new("us-east/iad-1").expect("region");
        let domain = FailureDomainId::from_str("rack-a.7").expect("domain");

        assert_eq!(region.as_str(), "us-east/iad-1");
        assert_eq!(region.to_string(), "us-east/iad-1");
        assert_eq!(domain.to_string(), "rack-a.7");
    }

    #[test]
    fn named_ids_reject_empty_or_unsafe_values() {
        for value in ["", "us east", "iad:1", "tenant\nbad"] {
            let err = RegionId::new(value).expect_err("invalid region id rejected");
            assert_eq!(err.code, ErrorCode::InvalidInput);
            assert_eq!(err.category(), crate::ErrorCategory::Configuration);
        }
    }

    #[test]
    fn nonzero_versions_and_incarnations_reject_zero() {
        assert_eq!(
            PolicyVersion::new(0).expect_err("zero policy version").code,
            ErrorCode::InvalidInput
        );
        assert_eq!(
            NodeIncarnation::new(0).expect_err("zero incarnation").code,
            ErrorCode::InvalidInput
        );
        assert_eq!(
            OperationId::new(0).expect_err("zero operation").code,
            ErrorCode::InvalidInput
        );
    }
}
