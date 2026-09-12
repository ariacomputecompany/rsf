use rsf_auth::{
    AccessRequest, AuthEngine, AuthToken, Authorization, Capability, PrincipalId, Resource,
};
use rsf_core::{CommitVersion, RsfResult};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ApiSurface {
    Sql,
    Kv,
    Object,
    Blob,
    Admin,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApiRequest {
    pub principal: PrincipalId,
    pub surface: ApiSurface,
    pub operation: String,
    pub capability: Capability,
    pub resource: Resource,
}

impl ApiRequest {
    pub fn sql_query(
        principal: PrincipalId,
        namespace: impl Into<String>,
        statement: impl Into<String>,
    ) -> Self {
        Self {
            principal,
            surface: ApiSurface::Sql,
            operation: statement.into(),
            capability: Capability::QueryExecute,
            resource: Resource::Namespace(namespace.into()),
        }
    }

    pub fn kv_write(
        principal: PrincipalId,
        namespace: impl Into<String>,
        key: impl Into<String>,
    ) -> Self {
        Self {
            principal,
            surface: ApiSurface::Kv,
            operation: format!("write {}", key.into()),
            capability: Capability::NamespaceWrite,
            resource: Resource::surface(namespace, "kv").expect("built-in surface id is valid"),
        }
    }

    pub fn object_put(
        principal: PrincipalId,
        namespace: impl Into<String>,
        key: impl Into<String>,
    ) -> Self {
        Self {
            principal,
            surface: ApiSurface::Object,
            operation: format!("put {}", key.into()),
            capability: Capability::ObjectWrite,
            resource: Resource::surface(namespace, "object").expect("built-in surface id is valid"),
        }
    }

    pub fn blob_put(
        principal: PrincipalId,
        namespace: impl Into<String>,
        digest: impl Into<String>,
    ) -> Self {
        Self {
            principal,
            surface: ApiSurface::Blob,
            operation: format!("put {}", digest.into()),
            capability: Capability::BlobWrite,
            resource: Resource::surface(namespace, "blob").expect("built-in surface id is valid"),
        }
    }

    pub fn admin_join(principal: PrincipalId) -> Self {
        Self {
            principal,
            surface: ApiSurface::Admin,
            operation: "join node".to_string(),
            capability: Capability::ClusterJoin,
            resource: Resource::Cluster,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ApiDecision {
    Accepted {
        surface: ApiSurface,
        operation: String,
    },
    Rejected {
        surface: ApiSurface,
        operation: String,
        reason: String,
    },
}

impl ApiDecision {
    pub fn is_accepted(&self) -> bool {
        matches!(self, Self::Accepted { .. })
    }
}

#[derive(Clone, Debug, Default)]
pub struct ApiGateway;

impl ApiGateway {
    pub fn evaluate(
        &self,
        auth: &AuthEngine,
        token: &AuthToken,
        now: CommitVersion,
        request: ApiRequest,
    ) -> RsfResult<ApiDecision> {
        let decision = auth.authorize(
            token,
            now,
            AccessRequest {
                principal: request.principal.clone(),
                capability: request.capability,
                resource: request.resource,
            },
        )?;

        Ok(match decision {
            Authorization::Allowed => ApiDecision::Accepted {
                surface: request.surface,
                operation: request.operation,
            },
            Authorization::Denied { reason } => ApiDecision::Rejected {
                surface: request.surface,
                operation: request.operation,
                reason,
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsf_auth::{Grant, Principal, PrincipalKind};

    #[test]
    fn sql_query_is_accepted_with_query_grant() {
        let (auth, token, principal) = fixture(
            Capability::QueryExecute,
            Resource::Namespace("app".to_string()),
        );
        let decision = ApiGateway
            .evaluate(
                &auth,
                &token,
                CommitVersion(2),
                ApiRequest::sql_query(principal, "app", "SELECT * FROM users"),
            )
            .unwrap();

        assert!(decision.is_accepted());
    }

    #[test]
    fn admin_join_is_rejected_without_cluster_join_grant() {
        let (auth, token, principal) = fixture(
            Capability::QueryExecute,
            Resource::Namespace("app".to_string()),
        );
        let decision = ApiGateway
            .evaluate(
                &auth,
                &token,
                CommitVersion(2),
                ApiRequest::admin_join(principal),
            )
            .unwrap();

        assert_eq!(
            decision,
            ApiDecision::Rejected {
                surface: ApiSurface::Admin,
                operation: "join node".to_string(),
                reason: "no matching grant".to_string()
            }
        );
    }

    #[test]
    fn expired_token_rejects_request() {
        let (auth, token, principal) = fixture(
            Capability::QueryExecute,
            Resource::Namespace("app".to_string()),
        );
        let decision = ApiGateway
            .evaluate(
                &auth,
                &token,
                CommitVersion(20),
                ApiRequest::sql_query(principal, "app", "SELECT * FROM users"),
            )
            .unwrap();

        assert_eq!(
            decision,
            ApiDecision::Rejected {
                surface: ApiSurface::Sql,
                operation: "SELECT * FROM users".to_string(),
                reason: "token expired".to_string()
            }
        );
    }

    fn fixture(capability: Capability, resource: Resource) -> (AuthEngine, AuthToken, PrincipalId) {
        let mut auth = AuthEngine::default();
        let principal = Principal {
            id: PrincipalId("app-client".to_string()),
            kind: PrincipalKind::User,
        };
        auth.register_principal(principal.clone()).unwrap();
        auth.grant(Grant {
            principal: principal.id.clone(),
            capability,
            resource,
        })
        .unwrap();
        let token = auth
            .issue_token(&principal.id, CommitVersion(1), 10)
            .unwrap();
        (auth, token, principal.id)
    }
}
