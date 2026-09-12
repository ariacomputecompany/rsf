#![forbid(unsafe_code)]

use rsf_core::{CommitVersion, ErrorCode, RsfError, RsfResult};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DatabaseSurface {
    Relational,
    Kv,
    ObjectMetadata,
    BlobPayload,
    Admin,
}

impl DatabaseSurface {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Relational => "relational",
            Self::Kv => "kv",
            Self::ObjectMetadata => "object-metadata",
            Self::BlobPayload => "blob-payload",
            Self::Admin => "admin",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConsistencyClass {
    StrictQuorum,
    AvailabilityFirst,
    BoundedStaleRead,
    ImmutableVerified,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConsistencyOperation {
    Read,
    Write,
    Admin,
}

impl ConsistencyOperation {
    pub fn mutates_state(self) -> bool {
        !matches!(self, Self::Read)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
            Self::Admin => "admin",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConsistencyRequest {
    pub surface: DatabaseSurface,
    pub operation: ConsistencyOperation,
    pub requested: ConsistencyClass,
    pub immutable_proof_verified: bool,
    pub observed_version: CommitVersion,
    pub required_version: CommitVersion,
    pub max_staleness_versions: Option<u64>,
}

impl ConsistencyRequest {
    pub fn strict(
        surface: DatabaseSurface,
        operation: ConsistencyOperation,
        version: CommitVersion,
    ) -> Self {
        Self {
            surface,
            operation,
            requested: ConsistencyClass::StrictQuorum,
            immutable_proof_verified: false,
            observed_version: version,
            required_version: version,
            max_staleness_versions: None,
        }
    }

    pub fn availability_first_payload_write(
        observed_version: CommitVersion,
        required_version: CommitVersion,
        immutable_proof_verified: bool,
    ) -> Self {
        Self {
            surface: DatabaseSurface::BlobPayload,
            operation: ConsistencyOperation::Write,
            requested: ConsistencyClass::AvailabilityFirst,
            immutable_proof_verified,
            observed_version,
            required_version,
            max_staleness_versions: None,
        }
    }

    pub fn bounded_stale_read(
        surface: DatabaseSurface,
        observed_version: CommitVersion,
        required_version: CommitVersion,
        max_staleness_versions: u64,
    ) -> Self {
        Self {
            surface,
            operation: ConsistencyOperation::Read,
            requested: ConsistencyClass::BoundedStaleRead,
            immutable_proof_verified: false,
            observed_version,
            required_version,
            max_staleness_versions: Some(max_staleness_versions),
        }
    }
}

impl ConsistencyClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::StrictQuorum => "strict-quorum",
            Self::AvailabilityFirst => "availability-first",
            Self::BoundedStaleRead => "bounded-stale-read",
            Self::ImmutableVerified => "immutable-verified",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConsistencyDecision {
    pub surface: DatabaseSurface,
    pub class: ConsistencyClass,
    pub quorum_required: bool,
    pub async_allowed: bool,
    pub verification_required: bool,
    pub max_staleness_versions: Option<u64>,
    pub detail: String,
}

#[derive(Clone, Debug, Default)]
pub struct ConsistencyPolicy;

impl ConsistencyPolicy {
    pub fn classify(surface: DatabaseSurface) -> ConsistencyDecision {
        match surface {
            DatabaseSurface::Relational | DatabaseSurface::Kv | DatabaseSurface::Admin => {
                strict(surface, "mutable truth requires quorum before acknowledgement")
            }
            DatabaseSurface::ObjectMetadata => strict(
                surface,
                "object visibility and version heads require quorum metadata commits",
            ),
            DatabaseSurface::BlobPayload => ConsistencyDecision {
                surface,
                class: ConsistencyClass::AvailabilityFirst,
                quorum_required: false,
                async_allowed: true,
                verification_required: true,
                max_staleness_versions: None,
                detail: "immutable payload writes may use availability-first placement after hash verification"
                    .to_string(),
            },
        }
    }

    pub fn validate(
        surface: DatabaseSurface,
        requested: ConsistencyClass,
    ) -> RsfResult<ConsistencyDecision> {
        let expected = Self::classify(surface);
        if requested == expected.class {
            return Ok(expected);
        }
        if expected.quorum_required && requested == ConsistencyClass::AvailabilityFirst {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!(
                    "{} requires strict quorum; availability-first would disguise async replication as strict consistency",
                    surface.as_str()
                ),
            ));
        }
        Err(RsfError::new(
            ErrorCode::InvalidInput,
            format!(
                "{} requires {}; requested {}",
                surface.as_str(),
                expected.class.as_str(),
                requested.as_str()
            ),
        ))
    }

    pub fn validate_request(request: ConsistencyRequest) -> RsfResult<ConsistencyDecision> {
        validate_versions(request.observed_version, request.required_version)?;
        match request.requested {
            ConsistencyClass::StrictQuorum => {
                let expected = Self::classify(request.surface);
                if expected.class == ConsistencyClass::StrictQuorum {
                    return Ok(expected);
                }
                if request.surface == DatabaseSurface::BlobPayload
                    && request.operation == ConsistencyOperation::Read
                    && request.immutable_proof_verified
                {
                    return Ok(ConsistencyDecision {
                        surface: request.surface,
                        class: ConsistencyClass::StrictQuorum,
                        quorum_required: true,
                        async_allowed: false,
                        verification_required: true,
                        max_staleness_versions: None,
                        detail:
                            "blob payload read may be strict when caller also verifies immutable content identity"
                                .to_string(),
                    });
                }
                Err(invalid(format!(
                    "{} normally uses {}; strict quorum requires metadata-backed routing or immutable verification context",
                    request.surface.as_str(),
                    expected.class.as_str()
                )))
            }
            ConsistencyClass::AvailabilityFirst => validate_availability_first(request),
            ConsistencyClass::BoundedStaleRead => validate_bounded_stale_read(request),
            ConsistencyClass::ImmutableVerified => validate_immutable_verified(request),
        }
    }
}

fn strict(surface: DatabaseSurface, detail: &str) -> ConsistencyDecision {
    ConsistencyDecision {
        surface,
        class: ConsistencyClass::StrictQuorum,
        quorum_required: true,
        async_allowed: false,
        verification_required: false,
        max_staleness_versions: None,
        detail: detail.to_string(),
    }
}

fn validate_availability_first(request: ConsistencyRequest) -> RsfResult<ConsistencyDecision> {
    if request.surface != DatabaseSurface::BlobPayload {
        return Err(invalid(format!(
            "{} requires strict quorum; availability-first would disguise async replication as strict consistency",
            request.surface.as_str()
        )));
    }
    if !request.operation.mutates_state() {
        return Err(invalid(
            "availability-first is only valid for immutable payload placement writes",
        ));
    }
    if !request.immutable_proof_verified {
        return Err(invalid(
            "availability-first blob payload writes require verified immutable content identity",
        ));
    }
    Ok(ConsistencyDecision {
        surface: request.surface,
        class: ConsistencyClass::AvailabilityFirst,
        quorum_required: false,
        async_allowed: true,
        verification_required: true,
        max_staleness_versions: None,
        detail:
            "immutable payload write may acknowledge after verified content placement and async repair"
                .to_string(),
    })
}

fn validate_bounded_stale_read(request: ConsistencyRequest) -> RsfResult<ConsistencyDecision> {
    if request.operation != ConsistencyOperation::Read {
        return Err(invalid("bounded-stale consistency is only valid for reads"));
    }
    if matches!(request.surface, DatabaseSurface::Admin) {
        return Err(invalid("admin reads must use strict quorum"));
    }
    if request.surface == DatabaseSurface::BlobPayload {
        return Err(invalid(
            "blob payload reads should use immutable-verified consistency, not bounded staleness",
        ));
    }
    let Some(max_staleness) = request.max_staleness_versions else {
        return Err(invalid(
            "bounded-stale reads require an explicit max staleness budget",
        ));
    };
    if max_staleness == 0 {
        return Err(invalid(
            "bounded-stale reads require a non-zero staleness budget",
        ));
    }
    let lag = request
        .required_version
        .0
        .saturating_sub(request.observed_version.0);
    if lag > max_staleness {
        return Err(invalid(format!(
            "bounded-stale read observed version {} is {lag} versions behind required version {}, exceeding budget {max_staleness}",
            request.observed_version.0, request.required_version.0
        )));
    }
    Ok(ConsistencyDecision {
        surface: request.surface,
        class: ConsistencyClass::BoundedStaleRead,
        quorum_required: false,
        async_allowed: false,
        verification_required: false,
        max_staleness_versions: Some(max_staleness),
        detail: format!(
            "{} bounded-stale read is within {lag}/{max_staleness} version budget",
            request.surface.as_str()
        ),
    })
}

fn validate_immutable_verified(request: ConsistencyRequest) -> RsfResult<ConsistencyDecision> {
    if request.surface != DatabaseSurface::BlobPayload {
        return Err(invalid(format!(
            "{} cannot use immutable-verified payload semantics",
            request.surface.as_str()
        )));
    }
    if request.operation != ConsistencyOperation::Read {
        return Err(invalid(
            "immutable-verified consistency is only valid for payload reads",
        ));
    }
    if !request.immutable_proof_verified {
        return Err(invalid(
            "immutable-verified reads require digest or manifest verification proof",
        ));
    }
    Ok(ConsistencyDecision {
        surface: request.surface,
        class: ConsistencyClass::ImmutableVerified,
        quorum_required: false,
        async_allowed: false,
        verification_required: true,
        max_staleness_versions: None,
        detail: "immutable payload read is accepted after digest or manifest verification"
            .to_string(),
    })
}

fn validate_versions(observed: CommitVersion, required: CommitVersion) -> RsfResult<()> {
    if observed.0 > required.0 {
        return Err(invalid(format!(
            "observed version {} cannot be ahead of required version {}",
            observed.0, required.0
        )));
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relational_kv_object_metadata_and_admin_require_strict_quorum() {
        for surface in [
            DatabaseSurface::Relational,
            DatabaseSurface::Kv,
            DatabaseSurface::ObjectMetadata,
            DatabaseSurface::Admin,
        ] {
            let decision = ConsistencyPolicy::classify(surface);
            assert_eq!(decision.class, ConsistencyClass::StrictQuorum);
            assert!(decision.quorum_required);
            assert!(!decision.async_allowed);
        }
    }

    #[test]
    fn blob_payloads_can_be_availability_first_after_hash_verification() {
        let decision = ConsistencyPolicy::validate(
            DatabaseSurface::BlobPayload,
            ConsistencyClass::AvailabilityFirst,
        )
        .unwrap();

        assert_eq!(decision.class, ConsistencyClass::AvailabilityFirst);
        assert!(!decision.quorum_required);
        assert!(decision.async_allowed);
    }

    #[test]
    fn strict_surfaces_reject_availability_first_downgrade() {
        let error = ConsistencyPolicy::validate(
            DatabaseSurface::Relational,
            ConsistencyClass::AvailabilityFirst,
        )
        .unwrap_err();

        assert!(
            error
                .message
                .contains("would disguise async replication as strict consistency")
        );
    }

    #[test]
    fn availability_first_payload_write_requires_immutable_proof() {
        let error = ConsistencyPolicy::validate_request(
            ConsistencyRequest::availability_first_payload_write(
                CommitVersion(8),
                CommitVersion(8),
                false,
            ),
        )
        .unwrap_err();

        assert_eq!(error.code, ErrorCode::InvalidInput);
        assert!(
            error
                .message
                .contains("verified immutable content identity")
        );

        let decision = ConsistencyPolicy::validate_request(
            ConsistencyRequest::availability_first_payload_write(
                CommitVersion(8),
                CommitVersion(8),
                true,
            ),
        )
        .unwrap();

        assert_eq!(decision.class, ConsistencyClass::AvailabilityFirst);
        assert!(decision.async_allowed);
        assert!(decision.verification_required);
    }

    #[test]
    fn availability_first_is_rejected_for_mutable_truth() {
        let error = ConsistencyPolicy::validate_request(ConsistencyRequest {
            surface: DatabaseSurface::Kv,
            operation: ConsistencyOperation::Write,
            requested: ConsistencyClass::AvailabilityFirst,
            immutable_proof_verified: true,
            observed_version: CommitVersion(4),
            required_version: CommitVersion(4),
            max_staleness_versions: None,
        })
        .unwrap_err();

        assert_eq!(error.code, ErrorCode::InvalidInput);
        assert!(error.message.contains("requires strict quorum"));
    }

    #[test]
    fn bounded_stale_read_requires_budget_and_recovered_version_within_bounds() {
        let decision = ConsistencyPolicy::validate_request(ConsistencyRequest::bounded_stale_read(
            DatabaseSurface::ObjectMetadata,
            CommitVersion(17),
            CommitVersion(20),
            3,
        ))
        .unwrap();

        assert_eq!(decision.class, ConsistencyClass::BoundedStaleRead);
        assert_eq!(decision.max_staleness_versions, Some(3));
        assert!(!decision.quorum_required);

        let error = ConsistencyPolicy::validate_request(ConsistencyRequest::bounded_stale_read(
            DatabaseSurface::ObjectMetadata,
            CommitVersion(16),
            CommitVersion(20),
            3,
        ))
        .unwrap_err();

        assert_eq!(error.code, ErrorCode::InvalidInput);
        assert!(error.message.contains("exceeding budget"));
    }

    #[test]
    fn admin_and_writes_cannot_use_bounded_stale_reads() {
        let admin_error =
            ConsistencyPolicy::validate_request(ConsistencyRequest::bounded_stale_read(
                DatabaseSurface::Admin,
                CommitVersion(8),
                CommitVersion(8),
                1,
            ))
            .unwrap_err();
        assert!(
            admin_error
                .message
                .contains("admin reads must use strict quorum")
        );

        let write_error = ConsistencyPolicy::validate_request(ConsistencyRequest {
            surface: DatabaseSurface::Relational,
            operation: ConsistencyOperation::Write,
            requested: ConsistencyClass::BoundedStaleRead,
            immutable_proof_verified: false,
            observed_version: CommitVersion(8),
            required_version: CommitVersion(8),
            max_staleness_versions: Some(1),
        })
        .unwrap_err();
        assert!(write_error.message.contains("only valid for reads"));
    }

    #[test]
    fn immutable_verified_payload_read_requires_digest_or_manifest_proof() {
        let request = ConsistencyRequest {
            surface: DatabaseSurface::BlobPayload,
            operation: ConsistencyOperation::Read,
            requested: ConsistencyClass::ImmutableVerified,
            immutable_proof_verified: true,
            observed_version: CommitVersion(9),
            required_version: CommitVersion(9),
            max_staleness_versions: None,
        };
        let decision = ConsistencyPolicy::validate_request(request).unwrap();

        assert_eq!(decision.class, ConsistencyClass::ImmutableVerified);
        assert!(decision.verification_required);
        assert!(!decision.async_allowed);

        let error = ConsistencyPolicy::validate_request(ConsistencyRequest {
            immutable_proof_verified: false,
            ..ConsistencyRequest {
                surface: DatabaseSurface::BlobPayload,
                operation: ConsistencyOperation::Read,
                requested: ConsistencyClass::ImmutableVerified,
                immutable_proof_verified: true,
                observed_version: CommitVersion(9),
                required_version: CommitVersion(9),
                max_staleness_versions: None,
            }
        })
        .unwrap_err();

        assert!(error.message.contains("verification proof"));
    }

    #[test]
    fn consistency_request_rejects_observed_version_ahead_of_required() {
        let error = ConsistencyPolicy::validate_request(
            ConsistencyRequest::strict(
                DatabaseSurface::Relational,
                ConsistencyOperation::Read,
                CommitVersion(11),
            )
            .with_versions(CommitVersion(12), CommitVersion(11)),
        )
        .unwrap_err();

        assert_eq!(error.code, ErrorCode::InvalidInput);
        assert!(error.message.contains("cannot be ahead"));
    }

    trait TestRequestExt {
        fn with_versions(self, observed: CommitVersion, required: CommitVersion) -> Self;
    }

    impl TestRequestExt for ConsistencyRequest {
        fn with_versions(mut self, observed: CommitVersion, required: CommitVersion) -> Self {
            self.observed_version = observed;
            self.required_version = required;
            self
        }
    }
}
