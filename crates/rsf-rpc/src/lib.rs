use rsf_core::{ErrorCode, RsfError, RsfResult, TraceId};

mod ledger;
pub mod replication;

pub use ledger::{DurableRpcLedger, RpcCall, RpcCallId, RpcCallState};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServiceRole {
    Gateway,
    Metadata,
    Storage,
    Sql,
    Object,
    Blob,
    Background,
}

impl ServiceRole {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Gateway => "gateway",
            Self::Metadata => "metadata",
            Self::Storage => "storage",
            Self::Sql => "sql",
            Self::Object => "object",
            Self::Blob => "blob",
            Self::Background => "background",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetryClass {
    Never,
    Idempotent,
    HedgeableRead,
}

impl RetryClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Never => "never",
            Self::Idempotent => "idempotent",
            Self::HedgeableRead => "hedgeable-read",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RpcEnvelope {
    pub trace_id: TraceId,
    pub from: ServiceRole,
    pub to: ServiceRole,
    pub operation: String,
    pub deadline_ms: u32,
    pub attempt: u8,
    pub retry_class: RetryClass,
    pub body_bytes: u64,
}

impl RpcEnvelope {
    pub fn new(
        trace_id: TraceId,
        from: ServiceRole,
        to: ServiceRole,
        operation: impl Into<String>,
        deadline_ms: u32,
        retry_class: RetryClass,
        body_bytes: u64,
    ) -> Self {
        Self {
            trace_id,
            from,
            to,
            operation: operation.into(),
            deadline_ms,
            attempt: 1,
            retry_class,
            body_bytes,
        }
    }

    pub fn try_new(
        trace_id: impl Into<String>,
        from: ServiceRole,
        to: ServiceRole,
        operation: impl Into<String>,
        deadline_ms: u32,
        retry_class: RetryClass,
        body_bytes: u64,
    ) -> RsfResult<Self> {
        Ok(Self::new(
            TraceId::new(trace_id)?,
            from,
            to,
            operation,
            deadline_ms,
            retry_class,
            body_bytes,
        ))
    }

    pub fn attempt(mut self, attempt: u8) -> Self {
        self.attempt = attempt;
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RpcDecision {
    Accepted(RpcEnvelope),
    Retryable(RpcEnvelope),
    Rejected { trace_id: TraceId, reason: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RpcPolicy {
    pub max_deadline_ms: u32,
    pub max_attempts: u8,
    pub max_body_bytes: u64,
}

impl RpcPolicy {
    pub fn strict_database() -> Self {
        Self {
            max_deadline_ms: 5_000,
            max_attempts: 3,
            max_body_bytes: 8 * 1024 * 1024,
        }
    }
}

#[derive(Debug)]
pub struct RpcRouter {
    policy: RpcPolicy,
}

impl RpcRouter {
    pub fn new(policy: RpcPolicy) -> Self {
        Self { policy }
    }

    pub fn route(&self, envelope: RpcEnvelope) -> RpcDecision {
        if envelope.operation.is_empty() {
            return RpcDecision::Rejected {
                trace_id: envelope.trace_id,
                reason: "missing operation".to_string(),
            };
        }
        if envelope.deadline_ms == 0 || envelope.deadline_ms > self.policy.max_deadline_ms {
            return RpcDecision::Rejected {
                trace_id: envelope.trace_id,
                reason: "deadline outside policy".to_string(),
            };
        }
        if envelope.attempt == 0 || envelope.attempt > self.policy.max_attempts {
            return RpcDecision::Rejected {
                trace_id: envelope.trace_id,
                reason: "attempt outside policy".to_string(),
            };
        }
        if envelope.body_bytes > self.policy.max_body_bytes {
            return RpcDecision::Rejected {
                trace_id: envelope.trace_id,
                reason: "body exceeds policy".to_string(),
            };
        }
        if envelope.attempt > 1 && envelope.retry_class != RetryClass::Never {
            RpcDecision::Retryable(envelope)
        } else if envelope.attempt > 1 {
            RpcDecision::Rejected {
                trace_id: envelope.trace_id,
                reason: "non-retryable operation retried".to_string(),
            }
        } else {
            RpcDecision::Accepted(envelope)
        }
    }
}

fn validate_envelope(envelope: &RpcEnvelope) -> RsfResult<()> {
    if envelope.operation.trim().is_empty() {
        return Err(invalid("rpc operation must not be empty"));
    }
    if envelope.deadline_ms == 0 {
        return Err(invalid("rpc deadline must be greater than zero"));
    }
    if envelope.attempt == 0 {
        return Err(invalid("rpc attempt must be greater than zero"));
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message)
}

pub fn accepted_body_bytes(decisions: &[RpcDecision]) -> u64 {
    decisions
        .iter()
        .filter_map(|decision| match decision {
            RpcDecision::Accepted(envelope) | RpcDecision::Retryable(envelope) => {
                Some(envelope.body_bytes)
            }
            RpcDecision::Rejected { .. } => None,
        })
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn trace(id: &str) -> TraceId {
        TraceId::new(id).expect("trace id")
    }

    #[test]
    fn accepts_valid_first_attempt() {
        let router = RpcRouter::new(RpcPolicy::strict_database());
        let decision = router.route(RpcEnvelope::new(
            trace("trace-1"),
            ServiceRole::Gateway,
            ServiceRole::Sql,
            "sql.execute",
            100,
            RetryClass::Never,
            512,
        ));

        assert!(matches!(decision, RpcDecision::Accepted(_)));
    }

    #[test]
    fn classifies_retryable_idempotent_attempts() {
        let router = RpcRouter::new(RpcPolicy::strict_database());
        let decision = router.route(
            RpcEnvelope::new(
                trace("trace-2"),
                ServiceRole::Background,
                ServiceRole::Object,
                "object.repair-read",
                250,
                RetryClass::HedgeableRead,
                128,
            )
            .attempt(2),
        );

        assert!(matches!(decision, RpcDecision::Retryable(_)));
    }

    #[test]
    fn rejects_policy_violations() {
        let router = RpcRouter::new(RpcPolicy::strict_database());

        assert!(matches!(
            router.route(RpcEnvelope::new(
                trace("trace-3"),
                ServiceRole::Gateway,
                ServiceRole::Storage,
                "storage.append",
                10_000,
                RetryClass::Idempotent,
                64,
            )),
            RpcDecision::Rejected { .. }
        ));
        assert!(matches!(
            router.route(
                RpcEnvelope::new(
                    trace("trace-4"),
                    ServiceRole::Gateway,
                    ServiceRole::Storage,
                    "storage.append",
                    100,
                    RetryClass::Never,
                    64,
                )
                .attempt(2)
            ),
            RpcDecision::Rejected { .. }
        ));
    }

    #[test]
    fn try_new_rejects_invalid_trace_ids() {
        let err = RpcEnvelope::try_new(
            "trace with spaces",
            ServiceRole::Gateway,
            ServiceRole::Sql,
            "sql.execute",
            100,
            RetryClass::Never,
            512,
        )
        .expect_err("invalid trace id must reject envelope construction");

        assert_eq!(err.code, rsf_core::ErrorCode::InvalidInput);
    }
}
