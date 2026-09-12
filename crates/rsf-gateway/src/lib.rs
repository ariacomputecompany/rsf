#![forbid(unsafe_code)]

use rsf_api::{ApiDecision, ApiRequest, ApiSurface};
use rsf_auth::{Capability, Resource};
use rsf_catalog::{CatalogEngine, CatalogSurface};
use rsf_consistency::{ConsistencyClass, ConsistencyDecision, ConsistencyPolicy, DatabaseSurface};
use rsf_core::{CommitVersion, ErrorCode, PlacementGroupId, RegionId, RsfError, RsfResult};
use rsf_policy::{
    DesiredClusterPolicy, GlobalWriteDecision, ImmutablePayloadPlacementDecision,
    ImmutablePayloadPlacementRequest, ReplicaReadClass, ReplicaReadDecision,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestBudget {
    pub deadline: CommitVersion,
    pub max_rows: Option<usize>,
    pub max_bytes: Option<usize>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GatewayHandler {
    SqlSession,
    KvEngine,
    ObjectStore,
    BlobStore,
    AdminPlane,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RoutedRequest {
    pub surface: ApiSurface,
    pub namespace: Option<String>,
    pub operation: String,
    pub capability: Capability,
    pub handler: GatewayHandler,
    pub consistency: ConsistencyDecision,
    pub placement_group: Option<PlacementGroupId>,
    pub region: Option<RegionRoute>,
    pub snapshot: CommitVersion,
    pub budget: RequestBudget,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RouteContext {
    pub client_region: RegionId,
    pub owner_region: RegionId,
    pub immutable_payload_regions: Vec<RegionId>,
    pub immutable_payload_min_replicas: usize,
    pub metadata_version: CommitVersion,
    pub required_metadata_version: CommitVersion,
    pub policy: DesiredClusterPolicy,
}

impl RouteContext {
    pub fn new(
        client_region: RegionId,
        owner_region: RegionId,
        immutable_payload_regions: Vec<RegionId>,
        immutable_payload_min_replicas: usize,
        metadata_version: CommitVersion,
        required_metadata_version: CommitVersion,
        policy: DesiredClusterPolicy,
    ) -> RsfResult<Self> {
        if immutable_payload_regions.is_empty() {
            return Err(invalid(
                "route context requires at least one immutable payload region",
            ));
        }
        if immutable_payload_min_replicas == 0 {
            return Err(invalid(
                "route context immutable payload minimum replicas must be non-zero",
            ));
        }
        policy.validate()?;
        Ok(Self {
            client_region,
            owner_region,
            immutable_payload_regions,
            immutable_payload_min_replicas,
            metadata_version,
            required_metadata_version,
            policy,
        })
    }

    pub fn try_new(
        client_region: impl Into<String>,
        owner_region: impl Into<String>,
        immutable_payload_regions: impl IntoIterator<Item = impl Into<String>>,
        immutable_payload_min_replicas: usize,
        metadata_version: CommitVersion,
        required_metadata_version: CommitVersion,
        policy: DesiredClusterPolicy,
    ) -> RsfResult<Self> {
        let payload_regions = immutable_payload_regions
            .into_iter()
            .map(RegionId::new)
            .collect::<RsfResult<Vec<_>>>()?;
        Self::new(
            RegionId::new(client_region)?,
            RegionId::new(owner_region)?,
            payload_regions,
            immutable_payload_min_replicas,
            metadata_version,
            required_metadata_version,
            policy,
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegionRoute {
    pub client_region: RegionId,
    pub target_region: RegionId,
    pub kind: RegionRouteKind,
    pub reason: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RegionRouteKind {
    OwnerPrimary,
    LocalImmutablePayload,
    RemoteImmutablePayload,
}

impl RegionRouteKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::OwnerPrimary => "owner-primary",
            Self::LocalImmutablePayload => "local-immutable-payload",
            Self::RemoteImmutablePayload => "remote-immutable-payload",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GatewayDecision {
    Routed(RoutedRequest),
    Rejected {
        surface: ApiSurface,
        operation: String,
        reason: String,
    },
}

#[derive(Clone, Debug, Default)]
pub struct DatabaseGateway;

impl DatabaseGateway {
    pub fn route(
        &self,
        catalog: &CatalogEngine,
        snapshot: CommitVersion,
        budget: RequestBudget,
        request: ApiRequest,
        decision: ApiDecision,
    ) -> RsfResult<GatewayDecision> {
        self.route_with_context(catalog, snapshot, budget, request, decision, None)
    }

    pub fn route_region_aware(
        &self,
        catalog: &CatalogEngine,
        snapshot: CommitVersion,
        budget: RequestBudget,
        request: ApiRequest,
        decision: ApiDecision,
        context: RouteContext,
    ) -> RsfResult<GatewayDecision> {
        self.route_with_context(catalog, snapshot, budget, request, decision, Some(&context))
    }

    fn route_with_context(
        &self,
        catalog: &CatalogEngine,
        snapshot: CommitVersion,
        budget: RequestBudget,
        request: ApiRequest,
        decision: ApiDecision,
        context: Option<&RouteContext>,
    ) -> RsfResult<GatewayDecision> {
        match decision {
            ApiDecision::Rejected {
                surface,
                operation,
                reason,
            } => Ok(GatewayDecision::Rejected {
                surface,
                operation,
                reason,
            }),
            ApiDecision::Accepted { surface, operation } => {
                if surface != request.surface || operation != request.operation {
                    return Err(invalid(
                        "gateway request and authorization decision do not match",
                    ));
                }
                let namespace = request_namespace(&request.resource);
                let handler = handler_for(&surface);
                let consistency = ConsistencyPolicy::validate(
                    database_surface(&surface),
                    requested_consistency(&surface),
                )?;
                let placement_group =
                    placement_for(catalog, namespace.as_deref(), &surface, handler)?;
                let region = if let Some(context) = context {
                    match region_for(&surface, request.capability.clone(), context) {
                        Ok(region) => Some(region),
                        Err(reason) => {
                            return Ok(GatewayDecision::Rejected {
                                surface,
                                operation,
                                reason,
                            });
                        }
                    }
                } else {
                    None
                };

                Ok(GatewayDecision::Routed(RoutedRequest {
                    surface,
                    namespace,
                    operation,
                    capability: request.capability,
                    handler,
                    consistency,
                    placement_group,
                    region,
                    snapshot,
                    budget,
                }))
            }
        }
    }
}

impl GatewayDecision {
    pub fn is_routed(&self) -> bool {
        matches!(self, Self::Routed(_))
    }
}

impl GatewayHandler {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SqlSession => "sql-session",
            Self::KvEngine => "kv-engine",
            Self::ObjectStore => "object-store",
            Self::BlobStore => "blob-store",
            Self::AdminPlane => "admin-plane",
        }
    }
}

fn request_namespace(resource: &Resource) -> Option<String> {
    match resource {
        Resource::Cluster => None,
        Resource::Namespace(namespace) => Some(namespace.clone()),
        Resource::Surface { namespace, .. } => Some(namespace.clone()),
    }
}

fn handler_for(surface: &ApiSurface) -> GatewayHandler {
    match surface {
        ApiSurface::Sql => GatewayHandler::SqlSession,
        ApiSurface::Kv => GatewayHandler::KvEngine,
        ApiSurface::Object => GatewayHandler::ObjectStore,
        ApiSurface::Blob => GatewayHandler::BlobStore,
        ApiSurface::Admin => GatewayHandler::AdminPlane,
    }
}

fn requested_consistency(surface: &ApiSurface) -> ConsistencyClass {
    match surface {
        ApiSurface::Blob => ConsistencyClass::AvailabilityFirst,
        ApiSurface::Sql | ApiSurface::Kv | ApiSurface::Object | ApiSurface::Admin => {
            ConsistencyClass::StrictQuorum
        }
    }
}

fn database_surface(surface: &ApiSurface) -> DatabaseSurface {
    match surface {
        ApiSurface::Sql => DatabaseSurface::Relational,
        ApiSurface::Kv => DatabaseSurface::Kv,
        ApiSurface::Object => DatabaseSurface::ObjectMetadata,
        ApiSurface::Blob => DatabaseSurface::BlobPayload,
        ApiSurface::Admin => DatabaseSurface::Admin,
    }
}

fn placement_for(
    catalog: &CatalogEngine,
    namespace: Option<&str>,
    surface: &ApiSurface,
    handler: GatewayHandler,
) -> RsfResult<Option<PlacementGroupId>> {
    let Some(namespace) = namespace else {
        return Ok(None);
    };
    let Some(catalog_surface) = catalog_surface(surface) else {
        return Ok(None);
    };
    catalog
        .binding(namespace, catalog_surface)
        .map(|binding| Some(binding.placement_group))
        .ok_or_else(|| {
            invalid(format!(
                "no placement binding for namespace {namespace} handler {}",
                handler.as_str()
            ))
        })
}

fn catalog_surface(surface: &ApiSurface) -> Option<CatalogSurface> {
    match surface {
        ApiSurface::Sql => Some(CatalogSurface::Relational),
        ApiSurface::Kv => Some(CatalogSurface::Kv),
        ApiSurface::Object => Some(CatalogSurface::Object),
        ApiSurface::Blob => Some(CatalogSurface::Blob),
        ApiSurface::Admin => None,
    }
}

fn region_for(
    surface: &ApiSurface,
    capability: Capability,
    context: &RouteContext,
) -> Result<RegionRoute, String> {
    match surface {
        ApiSurface::Blob => match context.policy.plan_immutable_payload_placement(
            ImmutablePayloadPlacementRequest {
                client_region: context.client_region.clone(),
                candidate_regions: context.immutable_payload_regions.clone(),
                min_replicas: context.immutable_payload_min_replicas,
                metadata_version: context.metadata_version.0,
                required_metadata_version: context.required_metadata_version.0,
            },
        ) {
            ImmutablePayloadPlacementDecision::Place {
                placements, reason, ..
            } => {
                let target_region = placements
                    .iter()
                    .find(|region| *region == &context.client_region)
                    .cloned()
                    .unwrap_or_else(|| {
                        placements
                            .first()
                            .expect("policy place decision includes requested minimum replicas")
                            .clone()
                    });
                let kind = if target_region == context.client_region {
                    RegionRouteKind::LocalImmutablePayload
                } else {
                    RegionRouteKind::RemoteImmutablePayload
                };
                Ok(RegionRoute {
                    client_region: context.client_region.clone(),
                    target_region,
                    kind,
                    reason,
                })
            }
            ImmutablePayloadPlacementDecision::Rejected { reason, .. } => Err(reason),
        },
        ApiSurface::Sql | ApiSurface::Kv | ApiSurface::Object | ApiSurface::Admin => {
            if is_mutable_truth_capability(capability) {
                return match context.policy.evaluate_global_write(
                    context.client_region.clone(),
                    context.owner_region.clone(),
                    true,
                ) {
                    GlobalWriteDecision::Allowed {
                        target_region,
                        reason,
                        ..
                    } => Ok(RegionRoute {
                        client_region: context.client_region.clone(),
                        target_region,
                        kind: RegionRouteKind::OwnerPrimary,
                        reason,
                    }),
                    GlobalWriteDecision::Rejected { reason, .. } => Err(reason),
                };
            }
            match context.policy.evaluate_replica_read(
                context.client_region.clone(),
                context.owner_region.clone(),
                ReplicaReadClass::Strict,
                false,
            ) {
                ReplicaReadDecision::ServeLocal {
                    target_region,
                    reason,
                    ..
                } => Ok(RegionRoute {
                    client_region: context.client_region.clone(),
                    target_region,
                    kind: RegionRouteKind::OwnerPrimary,
                    reason,
                }),
                ReplicaReadDecision::RoutePrimary {
                    primary_region,
                    reason,
                    ..
                } => Ok(RegionRoute {
                    client_region: context.client_region.clone(),
                    target_region: primary_region,
                    kind: RegionRouteKind::OwnerPrimary,
                    reason,
                }),
                ReplicaReadDecision::Rejected { reason, .. } => Err(reason),
            }
        }
    }
}

fn is_mutable_truth_capability(capability: Capability) -> bool {
    matches!(
        capability,
        Capability::ClusterJoin
            | Capability::ClusterAdmin
            | Capability::NamespaceWrite
            | Capability::ObjectWrite
            | Capability::TransactionCommit
            | Capability::RepairOperate
            | Capability::RetentionOperate
    )
}

fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsf_core::catalog::{
        ColumnDescriptor, ColumnType, NamespaceName, TableDescriptor, TableName,
    };
    use rsf_policy::{RegionPolicy, RegionRole};
    use rsf_protocol::FabricRouter;

    #[test]
    fn routes_accepted_sql_request_to_relational_binding() {
        let (catalog, snapshot) = catalog_fixture();
        let request = ApiRequest::sql_query(
            rsf_auth::PrincipalId("app-client".to_string()),
            "app",
            "SELECT id FROM users",
        );
        let decision = ApiDecision::Accepted {
            surface: ApiSurface::Sql,
            operation: "SELECT id FROM users".to_string(),
        };

        let routed = DatabaseGateway
            .route(&catalog, snapshot, budget(snapshot), request, decision)
            .expect("route");

        let GatewayDecision::Routed(route) = routed else {
            panic!("expected routed request");
        };
        assert_eq!(route.handler, GatewayHandler::SqlSession);
        assert_eq!(route.consistency.class, ConsistencyClass::StrictQuorum);
        assert!(route.consistency.quorum_required);
        assert_eq!(route.placement_group, Some(PlacementGroupId(1)));
        assert_eq!(route.namespace, Some("app".to_string()));
        assert_eq!(route.region, None);
    }

    #[test]
    fn routes_blob_request_to_availability_first_blob_handler() {
        let (catalog, snapshot) = catalog_fixture();
        let request = ApiRequest::blob_put(
            rsf_auth::PrincipalId("app-client".to_string()),
            "app",
            "digest-1",
        );
        let decision = ApiDecision::Accepted {
            surface: ApiSurface::Blob,
            operation: "put digest-1".to_string(),
        };

        let routed = DatabaseGateway
            .route(&catalog, snapshot, budget(snapshot), request, decision)
            .expect("route");

        let GatewayDecision::Routed(route) = routed else {
            panic!("expected routed request");
        };
        assert_eq!(route.handler, GatewayHandler::BlobStore);
        assert_eq!(route.consistency.class, ConsistencyClass::AvailabilityFirst);
        assert!(route.consistency.async_allowed);
        assert_eq!(route.placement_group, Some(PlacementGroupId(3)));
        assert_eq!(route.region, None);
    }

    #[test]
    fn region_aware_mutable_routes_to_owner_region() {
        let (catalog, snapshot) = catalog_fixture();
        let request = ApiRequest::kv_write(
            rsf_auth::PrincipalId("app-client".to_string()),
            "app",
            "config/a",
        );
        let decision = ApiDecision::Accepted {
            surface: ApiSurface::Kv,
            operation: "write config/a".to_string(),
        };

        let routed = DatabaseGateway
            .route_region_aware(
                &catalog,
                snapshot,
                budget(snapshot),
                request,
                decision,
                route_context(
                    "ord",
                    "iad",
                    ["ord", "iad"],
                    CommitVersion(8),
                    CommitVersion(8),
                ),
            )
            .expect("route");

        let GatewayDecision::Routed(route) = routed else {
            panic!("expected routed request");
        };
        let region = route.region.expect("region route");
        assert_eq!(region.client_region, region_id("ord"));
        assert_eq!(region.target_region, region_id("iad"));
        assert_eq!(region.kind, RegionRouteKind::OwnerPrimary);
        assert!(region.reason.contains("primary region"));
    }

    #[test]
    fn region_aware_mutable_write_rejects_read_replica_target_in_strict_policy() {
        let (catalog, snapshot) = catalog_fixture();
        let request = ApiRequest::kv_write(
            rsf_auth::PrincipalId("app-client".to_string()),
            "app",
            "config/a",
        );
        let decision = ApiDecision::Accepted {
            surface: ApiSurface::Kv,
            operation: "write config/a".to_string(),
        };

        let routed = DatabaseGateway
            .route_region_aware(
                &catalog,
                snapshot,
                budget(snapshot),
                request,
                decision,
                route_context(
                    "ord",
                    "ord",
                    ["ord", "iad"],
                    CommitVersion(8),
                    CommitVersion(8),
                ),
            )
            .expect("route");

        assert_eq!(
            routed,
            GatewayDecision::Rejected {
                surface: ApiSurface::Kv,
                operation: "write config/a".to_string(),
                reason: "strict global writes must target the primary region".to_string()
            }
        );
    }

    #[test]
    fn region_aware_strict_read_from_replica_routes_to_primary() {
        let (catalog, snapshot) = catalog_fixture();
        let request = ApiRequest::sql_query(
            rsf_auth::PrincipalId("app-client".to_string()),
            "app",
            "SELECT id FROM users",
        );
        let decision = ApiDecision::Accepted {
            surface: ApiSurface::Sql,
            operation: "SELECT id FROM users".to_string(),
        };

        let routed = DatabaseGateway
            .route_region_aware(
                &catalog,
                snapshot,
                budget(snapshot),
                request,
                decision,
                route_context(
                    "ord",
                    "ord",
                    ["ord", "iad"],
                    CommitVersion(8),
                    CommitVersion(8),
                ),
            )
            .expect("route");

        let GatewayDecision::Routed(route) = routed else {
            panic!("expected routed request");
        };
        let region = route.region.expect("region route");
        assert_eq!(region.client_region, region_id("ord"));
        assert_eq!(region.target_region, region_id("iad"));
        assert_eq!(region.kind, RegionRouteKind::OwnerPrimary);
        assert!(region.reason.contains("primary-region linearization"));
    }

    #[test]
    fn region_aware_blob_routes_to_local_payload_region() {
        let (catalog, snapshot) = catalog_fixture();
        let request = ApiRequest::blob_put(
            rsf_auth::PrincipalId("app-client".to_string()),
            "app",
            "digest-1",
        );
        let decision = ApiDecision::Accepted {
            surface: ApiSurface::Blob,
            operation: "put digest-1".to_string(),
        };

        let routed = DatabaseGateway
            .route_region_aware(
                &catalog,
                snapshot,
                budget(snapshot),
                request,
                decision,
                route_context(
                    "ord",
                    "iad",
                    ["fra", "ord"],
                    CommitVersion(8),
                    CommitVersion(8),
                ),
            )
            .expect("route");

        let GatewayDecision::Routed(route) = routed else {
            panic!("expected routed request");
        };
        let region = route.region.expect("region route");
        assert_eq!(region.target_region, region_id("ord"));
        assert_eq!(region.kind, RegionRouteKind::LocalImmutablePayload);
    }

    #[test]
    fn region_aware_blob_falls_back_to_remote_payload_region() {
        let (catalog, snapshot) = catalog_fixture();
        let request = ApiRequest::blob_put(
            rsf_auth::PrincipalId("app-client".to_string()),
            "app",
            "digest-1",
        );
        let decision = ApiDecision::Accepted {
            surface: ApiSurface::Blob,
            operation: "put digest-1".to_string(),
        };

        let routed = DatabaseGateway
            .route_region_aware(
                &catalog,
                snapshot,
                budget(snapshot),
                request,
                decision,
                route_context(
                    "sfo",
                    "iad",
                    ["fra", "ord"],
                    CommitVersion(8),
                    CommitVersion(8),
                ),
            )
            .expect("route");

        let GatewayDecision::Routed(route) = routed else {
            panic!("expected routed request");
        };
        let region = route.region.expect("region route");
        assert_eq!(region.target_region, region_id("fra"));
        assert_eq!(region.kind, RegionRouteKind::RemoteImmutablePayload);
        assert!(region.reason.contains("metadata version guard"));
    }

    #[test]
    fn region_aware_blob_rejects_stale_metadata_guard() {
        let (catalog, snapshot) = catalog_fixture();
        let request = ApiRequest::blob_put(
            rsf_auth::PrincipalId("app-client".to_string()),
            "app",
            "digest-1",
        );
        let decision = ApiDecision::Accepted {
            surface: ApiSurface::Blob,
            operation: "put digest-1".to_string(),
        };

        let routed = DatabaseGateway
            .route_region_aware(
                &catalog,
                snapshot,
                budget(snapshot),
                request,
                decision,
                route_context(
                    "ord",
                    "iad",
                    ["fra", "ord"],
                    CommitVersion(7),
                    CommitVersion(8),
                ),
            )
            .expect("route");

        assert_eq!(
            routed,
            GatewayDecision::Rejected {
                surface: ApiSurface::Blob,
                operation: "put digest-1".to_string(),
                reason: "metadata version 7 is older than required version 8".to_string()
            }
        );
    }

    #[test]
    fn rejected_api_decision_stays_rejected_before_routing() {
        let (catalog, snapshot) = catalog_fixture();
        let request = ApiRequest::admin_join(rsf_auth::PrincipalId("app-client".to_string()));
        let decision = ApiDecision::Rejected {
            surface: ApiSurface::Admin,
            operation: "join node".to_string(),
            reason: "no matching grant".to_string(),
        };

        let routed = DatabaseGateway
            .route(&catalog, snapshot, budget(snapshot), request, decision)
            .expect("route");

        assert_eq!(
            routed,
            GatewayDecision::Rejected {
                surface: ApiSurface::Admin,
                operation: "join node".to_string(),
                reason: "no matching grant".to_string()
            }
        );
    }

    #[test]
    fn missing_surface_binding_rejects_accepted_request() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut catalog = CatalogEngine::default();
        catalog
            .create_namespace(&mut router, PlacementGroupId(1), "app")
            .expect("namespace");
        let request = ApiRequest::kv_write(
            rsf_auth::PrincipalId("app-client".to_string()),
            "app",
            "config/a",
        );
        let decision = ApiDecision::Accepted {
            surface: ApiSurface::Kv,
            operation: "write config/a".to_string(),
        };

        let err = DatabaseGateway
            .route(
                &catalog,
                router.latest_version(),
                budget(router.latest_version()),
                request,
                decision,
            )
            .expect_err("missing binding");

        assert!(err.message.contains("no placement binding"));
    }

    fn catalog_fixture() -> (CatalogEngine, CommitVersion) {
        let mut router = FabricRouter::with_groups([
            PlacementGroupId(1),
            PlacementGroupId(2),
            PlacementGroupId(3),
        ]);
        let mut catalog = CatalogEngine::default();
        catalog
            .create_namespace(&mut router, PlacementGroupId(1), "app")
            .expect("namespace");
        catalog
            .create_table(
                &mut router,
                PlacementGroupId(1),
                TableDescriptor {
                    namespace: NamespaceName("app".to_string()),
                    name: TableName("users".to_string()),
                    columns: vec![ColumnDescriptor {
                        name: "id".to_string(),
                        ty: ColumnType::Int64,
                        nullable: false,
                    }],
                    primary_key: vec!["id".to_string()],
                },
            )
            .expect("table");
        for (surface, pg) in [
            (CatalogSurface::Relational, PlacementGroupId(1)),
            (CatalogSurface::Kv, PlacementGroupId(1)),
            (CatalogSurface::Object, PlacementGroupId(2)),
            (CatalogSurface::Blob, PlacementGroupId(3)),
        ] {
            catalog
                .bind_surface(&mut router, PlacementGroupId(1), "app", surface, pg)
                .expect("binding");
        }
        (catalog, router.latest_version())
    }

    fn budget(snapshot: CommitVersion) -> RequestBudget {
        RequestBudget {
            deadline: CommitVersion(snapshot.0 + 5),
            max_rows: Some(128),
            max_bytes: Some(1 << 20),
        }
    }

    fn region_id(value: &str) -> RegionId {
        RegionId::new(value).expect("region")
    }

    fn route_context(
        client_region: &str,
        owner_region: &str,
        immutable_payload_regions: impl IntoIterator<Item = &'static str>,
        metadata_version: CommitVersion,
        required_metadata_version: CommitVersion,
    ) -> RouteContext {
        RouteContext::try_new(
            client_region,
            owner_region,
            immutable_payload_regions,
            1,
            metadata_version,
            required_metadata_version,
            strict_policy(),
        )
        .expect("route context")
    }

    fn strict_policy() -> DesiredClusterPolicy {
        DesiredClusterPolicy {
            version: 1,
            min_replicas: 3,
            placement_groups: 3,
            strict_global_writes: true,
            regions: vec![
                RegionPolicy {
                    region: region_id("iad"),
                    role: RegionRole::Primary,
                },
                RegionPolicy {
                    region: region_id("ord"),
                    role: RegionRole::ReadReplica,
                },
                RegionPolicy {
                    region: region_id("fra"),
                    role: RegionRole::ReadReplica,
                },
            ],
        }
    }
}
