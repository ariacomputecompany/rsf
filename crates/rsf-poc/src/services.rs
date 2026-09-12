use std::collections::{BTreeMap, BTreeSet};

use rsf_admin::{AdminPlane, InviteToken, JoinRequest, NodeIdentity};
use rsf_api::{ApiDecision, ApiGateway, ApiRequest, ApiSurface};
use rsf_auth::{
    AccessRequest, AuthEngine, AuthToken, Authorization, Capability, Grant, Principal, PrincipalId,
    PrincipalKind, Resource,
};
use rsf_backup::{BackupItem, BackupItemKind, BackupPlanner};
use rsf_blob::BlobEngine;
use rsf_buffer::{BufferDecision, BufferFrame, BufferPool, DirtyFlushPolicy};
use rsf_cache::{
    CacheAdmission, CacheEntry, CacheKey, CachePolicy, CacheRead, CacheSurface, ReadAheadCandidate,
    ReadCache, StrictCacheRead,
};
use rsf_catalog::{CatalogEngine, CatalogSurface};
use rsf_checkpoint::{
    CheckpointDecision, CheckpointPlanner, CheckpointRoot, CheckpointRootKind, SegmentInventory,
    reject_if_checkpoint_regresses,
};
use rsf_chunkstore::{ChunkClass, ChunkDecision, ChunkStore, ChunkWrite};
use rsf_compaction::{CompactionEntry, CompactionExecutor, CompactionRun};
use rsf_consistency::{ConsistencyClass, ConsistencyPolicy, DatabaseSurface as ConsistencySurface};
use rsf_constraints::{
    ConstraintDecision, ConstraintRule, ConstraintState, ConstraintValidator, ConstraintValue,
    RowMutation,
};
use rsf_core::catalog::{ColumnDescriptor, ColumnType, NamespaceName, TableDescriptor, TableName};
use rsf_core::storage::WalRecordKind;
use rsf_core::transaction::{CommitIntent, KeyRange, Mutation, TransactionId};
use rsf_core::{
    ClusterId, CommitVersion, Digest, Epoch, FailureDomainId, LogIndex, NodeId, PlacementGroupId,
    RegionId, TraceId,
};
use rsf_deferred::{
    CommittedReferences, DeferredConstraintDecision, DeferredConstraintEngine,
    DeferredReferenceRule, DeferredRow,
};
use rsf_deployment::{DeploymentPackageManifest, DeploymentPlanner};
use rsf_erasure::{ErasurePolicy, ShardKind};
use rsf_executor::{ExecutionSource, PhysicalExecutor};
use rsf_gateway::{DatabaseGateway, GatewayDecision, RequestBudget, RouteContext};
use rsf_health::{
    HealthAction, HealthClass, HealthPolicy, HealthScorer, NodeHealth, NodeObservation,
};
use rsf_idempotency::{
    IdempotencyDecision, IdempotencyKey, IdempotencyLedger, RequestEnvelope, ResponseStatus,
    RetryClass, RetryPolicy, StoredResponse,
};
use rsf_index::{IndexDescriptor, IndexPlanner, IndexStore, RowImage};
use rsf_invariants::{InvariantChecker, InvariantInput};
use rsf_isolation::{IsolationDecision, IsolationTracker, IsolationTransaction};
use rsf_jobs::{JobDecision, JobId, JobKind, JobQueue, JobSpec};
use rsf_kv::{KvBatch, KvEngine, KvMutation};
use rsf_lease::{FenceDecision, FenceToken, LeaseTable};
use rsf_manifest::{
    BackupManifest, BlobManifest, ByteRange, ExtentManifest, KvManifest, ManifestChecksumProof,
    ManifestDecision, ManifestEntry, ManifestKind, ObjectManifest, ProviderMetadata,
    ProviderReplica, ProviderStorageClass, SnapshotManifest, SqlManifest, TypedManifest,
};
use rsf_materialize::{MaterializationDecision, RowMaterializer};
use rsf_membership::{Member, MembershipCell, MembershipDecision};
use rsf_migration::{MigrationDecision, MigrationLifecycle};
use rsf_object::ObjectEngine;
use rsf_objectmeta::{ObjectMetadataDecision, ObjectMetadataEngine, ObjectPartRef};
use rsf_observability::{AuditEvent, HealthStatus, ObservabilityRegistry};
use rsf_page::{PageDecision, PageFrame, PageId, PageImage, PageServer, RedoOperation, RedoRecord};
use rsf_planner::{AccessPath, PhysicalPlan, PhysicalPlanner, PhysicalPlanningContext, TableStats};
use rsf_policy::{
    DesiredClusterPolicy, GlobalWriteDecision, ImmutablePayloadPlacementDecision,
    ImmutablePayloadPlacementRequest, PolicyPreflightReport, RegionPolicy, RegionRole,
    ReplicaReadClass, ReplicaReadDecision,
};
use rsf_portal::PortalDecision;
use rsf_prepared::{PreparedCache, PreparedDecision, PreparedParamType, PreparedValue};
use rsf_primary::{PrimaryDecision, PrimaryIndex, PrimaryKeyDescriptor, PrimaryRow};
use rsf_protocol::{ClientRequestId, ConsistencyMode, FabricOperation, FabricRouter};
use rsf_provider::{
    ChunkSurface, ProviderDecision, ProviderDirectory, ProviderQuery, ProviderRecord,
};
use rsf_query::{Predicate, Projection, QueryExecutor, QueryPlanner, QueryRow, QueryValue};
use rsf_quota::{QuotaDecision, QuotaEngine, QuotaKey, QuotaPolicy};
use rsf_range::{CompactionPlan, KeyRange as LayoutRange, RangeId, RangeLayout, SstableManifest};
use rsf_rebalance::{NodeLoad, RebalanceAction, RebalancePlanner, RebalancePolicy};
use rsf_recovery::{
    RecoveryActionKind, RecoveryPlanner, RecoveryRoot, RecoveryRootKind, RestartStepKind,
};
use rsf_relcommit::{CommitGroupDecision, RelationalCommitAuditor};
use rsf_repair::{RepairAction, RepairFindingKind, RepairPlanner, ReplicaState};
use rsf_requirements::{RequirementAuditor, RequirementInput};
use rsf_retention::{
    RetainedBlob, RetainedObject, RetentionDecision, RetentionEngine, RetentionHook,
    RetentionPolicy,
};
use rsf_rowcodec::{GeneratedColumn, RowCodec, RowCodecPolicy, RowValue};
use rsf_rpc::{
    RetryClass as RpcRetryClass, RpcDecision, RpcEnvelope, RpcPolicy, RpcRouter, ServiceRole,
    accepted_body_bytes as accepted_rpc_body_bytes,
};
use rsf_schema::{
    MigrationPolicy, MigrationStep, SchemaChange, SchemaMigration, SchemaPlanner, SchemaVersion,
};
use rsf_session::{SessionBinder, SessionContext};
use rsf_sessiontxn::{SessionTransactionDecision, SessionTransactionEngine};
use rsf_sim::{
    DeliveryDecision, Event, EventKind, FailureDrillInput, FailureDrillReport, FaultPlan,
    LogicalTime, Simulation,
};
use rsf_snapshot::{
    SnapshotBarrierKind, SnapshotCoordinator, SnapshotRequest, SnapshotRoot, SnapshotRootKind,
};
use rsf_sql::{SqlEngine, SqlOutcome};
use rsf_sqldml::{SqlDmlPipeline, SqlDmlPipelineDecision, SqlDmlPipelineRequest};
use rsf_sqlpipeline::{SqlPipeline, SqlPipelineDecision, SqlPipelineRequest};
use rsf_sqltxn::{SqlTransactionDecision, SqlTransactionScriptEngine, SqlTransactionScriptRequest};
use rsf_stats::{ColumnSample, Freshness, StatisticsCatalog, TableSample, ValueSample};
use rsf_storage::{Recovery, WalAppend};
use rsf_stream::{
    FlowDecision, FlowWindow, StreamPlanner, StreamPolicy, StreamSurface, StreamTarget,
};
use rsf_superblock::{BootRoot, BootRootKind, Superblock, SuperblockDecision, SuperblockStore};
use rsf_tenancy::{DatabaseSurface as TenantSurface, TenancyCatalog};
use rsf_testability::{TestabilityInput, TestabilityReport};
use rsf_tuple::{TupleDecision, TupleKey, TupleStore, TupleVersion};
use rsf_txn::{ReadVersion, TransactionCoordinator};
use rsf_upgrade::{NodeVersion, SoftwareVersion, UpgradePlanner, UpgradePolicy, UpgradeStepKind};
use rsf_vacuum::{VacuumDecision, VacuumPlanner, VacuumPolicy};
use rsf_vsr::{VsrCell, VsrCommit, VsrDecision, VsrOperation, VsrPrepare};
use rsf_wal::{FlushRequest, WalDecision, WalEntry, WalGate, WalPolicy};
use rsf_watch::{WatchCursor, WatchRegistry};
use rsf_wire::{
    RawFrame, WireAdapter, WireDecision, WireProtocol, accepted_body_bytes, protocol_count,
    validate_decisions,
};
use rsf_workload::{WorkloadBudget, WorkloadClass, WorkloadEvidence, WorkloadShapeReport};
use rsf_write::{WriteDecision, WritePlan, WritePlanner, WriteRequest, WriteStep};

use crate::model::{Implementation, ProductionRunState};
use crate::trace::Trace;

#[derive(Debug)]
pub struct DatabaseRuntime {
    pub router: FabricRouter,
    pub catalog: CatalogEngine,
    pub auth: AuthEngine,
    pub app_token: Option<AuthToken>,
    pub transactions: TransactionCoordinator,
    pub kv: KvEngine,
    pub watches: WatchRegistry,
    pub sql: SqlEngine,
    pub objects: ObjectEngine,
    pub object_metadata: ObjectMetadataEngine,
    pub blobs: BlobEngine,
    pub admin: AdminPlane,
    pub leases: LeaseTable,
    pub tenancy: TenancyCatalog,
    pub stats: StatisticsCatalog,
    pub session_transactions: SessionTransactionEngine,
    pub deployment_roles: usize,
    pub deployment_endpoints: usize,
    pub deployment_volumes: usize,
    pub deployment_probes: usize,
    pub upgrade_steps: usize,
    pub upgrade_checks: usize,
    pub upgrade_nodes: usize,
    pub upgrade_rejections: usize,
    pub membership_epoch: usize,
    pub membership_voters: usize,
    pub membership_learners: usize,
    pub membership_promotions: usize,
    pub membership_intersections: usize,
    pub membership_rejections: usize,
    pub vsr_view: usize,
    pub vsr_prepares: usize,
    pub vsr_acks: usize,
    pub vsr_commits: usize,
    pub vsr_view_changes: usize,
    pub vsr_rejections: usize,
    pub repair_findings: usize,
    pub repair_actions: usize,
    pub query_rows: usize,
    pub authz_allowed: usize,
    pub authz_denied: usize,
    pub wire_frames: usize,
    pub wire_protocols: usize,
    pub wire_rejections: usize,
    pub wire_body_bytes: usize,
    pub provider_records: usize,
    pub provider_selected: usize,
    pub provider_degraded: usize,
    pub provider_regions: usize,
    pub provider_content: usize,
    pub rpc_calls: usize,
    pub rpc_retries: usize,
    pub rpc_rejections: usize,
    pub rpc_body_bytes: usize,
    pub api_accepted: usize,
    pub api_rejected: usize,
    pub idempotency_first_seen: usize,
    pub idempotency_replays: usize,
    pub idempotency_conflicts: usize,
    pub idempotency_retries: usize,
    pub gateway_routes: usize,
    pub gateway_rejections: usize,
    pub gateway_surfaces: usize,
    pub consistency_strict: usize,
    pub consistency_availability_first: usize,
    pub consistency_rejections: usize,
    pub quota_allowed: usize,
    pub quota_rejected: usize,
    pub quota_policies: usize,
    pub quota_namespaces: usize,
    pub cache_hits: usize,
    pub cache_misses: usize,
    pub cache_evictions: usize,
    pub cache_hints: usize,
    pub workload_classes: usize,
    pub workload_budget_paths: usize,
    pub workload_benchmark_claim: usize,
    pub backup_items: usize,
    pub restore_checks: usize,
    pub restore_executed_items: usize,
    pub restore_executed_surfaces: usize,
    pub restore_executed_bytes: usize,
    pub recovery_roots: usize,
    pub recovery_replays: usize,
    pub recovery_rejections: usize,
    pub recovery_actions: usize,
    pub lease_grants: usize,
    pub fence_rejections: usize,
    pub rebalance_moves: usize,
    pub rebalance_checks: usize,
    pub rebalance_policy_proofs: usize,
    pub fault_events: usize,
    pub dropped_messages: usize,
    pub delayed_messages: usize,
    pub failure_drill_passed: usize,
    pub failure_drill_domains: usize,
    pub health_healthy: usize,
    pub health_degraded: usize,
    pub health_demote: usize,
    pub health_actions: usize,
    pub tenant_surfaces: usize,
    pub schema_steps: usize,
    pub schema_backfills: usize,
    pub schema_rejected: usize,
    pub migration_begun: usize,
    pub migration_validated: usize,
    pub migration_published: usize,
    pub migration_backfilled_rows: usize,
    pub migration_indexed_keys: usize,
    pub migration_resumed: usize,
    pub migration_completed: usize,
    pub migration_rolled_back: usize,
    pub migration_rejected: usize,
    pub jobs_enqueued: usize,
    pub jobs_leased: usize,
    pub jobs_completed: usize,
    pub jobs_retried: usize,
    pub jobs_blocked: usize,
    pub session_bound: usize,
    pub session_authorized: usize,
    pub session_rejected: usize,
    pub prepared_statements: usize,
    pub prepared_binds: usize,
    pub prepared_invalidations: usize,
    pub prepared_deallocations: usize,
    pub prepared_evictions: usize,
    pub prepared_rejections: usize,
    pub portal_parses: usize,
    pub portal_binds: usize,
    pub portal_describes: usize,
    pub portal_executes: usize,
    pub portal_rows: usize,
    pub portal_cancellations: usize,
    pub portal_closes: usize,
    pub portal_statement_closes: usize,
    pub portal_rejections: usize,
    pub rowcodec_encoded: usize,
    pub rowcodec_decoded: usize,
    pub rowcodec_defaults: usize,
    pub rowcodec_generated: usize,
    pub rowcodec_rejections: usize,
    pub rowcodec_bytes: usize,
    pub tuple_versions: usize,
    pub tuple_visible_reads: usize,
    pub tuple_delete_markers: usize,
    pub tuple_rejections: usize,
    pub tuple_bytes: usize,
    pub vacuum_chains: usize,
    pub vacuum_pruned_versions: usize,
    pub vacuum_retained_versions: usize,
    pub vacuum_rejections: usize,
    pub vacuum_reclaimed_bytes: usize,
    pub materialized_rows: usize,
    pub materialized_projected: usize,
    pub materialized_columns: usize,
    pub materialized_rejections: usize,
    pub materialized_bytes: usize,
    pub primary_inserts: usize,
    pub primary_lookups: usize,
    pub primary_updates: usize,
    pub primary_deletes: usize,
    pub primary_rejections: usize,
    pub primary_entries: usize,
    pub index_mutations: usize,
    pub index_entries: usize,
    pub index_rebuilds: usize,
    pub planner_physical_plans: usize,
    pub planner_index_choices: usize,
    pub planner_range_scans: usize,
    pub planner_stream_reads: usize,
    pub executor_plans: usize,
    pub executor_stages: usize,
    pub executor_rows: usize,
    pub executor_remote_reads: usize,
    pub sql_pipeline_bound: usize,
    pub sql_pipeline_authorized: usize,
    pub sql_pipeline_logical: usize,
    pub sql_pipeline_physical: usize,
    pub sql_pipeline_stages: usize,
    pub sql_pipeline_rows: usize,
    pub sql_pipeline_remote_reads: usize,
    pub sql_pipeline_rejections: usize,
    pub sql_dml_bound: usize,
    pub sql_dml_authorized: usize,
    pub sql_dml_planned: usize,
    pub sql_dml_prepared: usize,
    pub sql_dml_committed: usize,
    pub sql_dml_isolated: usize,
    pub sql_dml_kv_applied: usize,
    pub sql_dml_session_staged: usize,
    pub sql_dml_overlay_verified: usize,
    pub sql_dml_rejections: usize,
    pub sql_txn_began: usize,
    pub sql_txn_dml_staged: usize,
    pub sql_txn_overlay_reads: usize,
    pub sql_txn_commits: usize,
    pub sql_txn_rollbacks: usize,
    pub sql_txn_rollback_absent: usize,
    pub sql_txn_savepoints: usize,
    pub sql_txn_savepoint_rewinds: usize,
    pub sql_txn_savepoint_absent: usize,
    pub sql_txn_rejections: usize,
    pub range_partitions: usize,
    pub range_non_overlaps: usize,
    pub range_rejections: usize,
    pub sstable_manifests: usize,
    pub compaction_inputs: usize,
    pub compaction_outputs: usize,
    pub compaction_live_keys: usize,
    pub compaction_tombstones: usize,
    pub compaction_reclaimed_bytes: usize,
    pub stats_rows: usize,
    pub stats_histogram_buckets: usize,
    pub stats_stale: usize,
    pub constraints_checked: usize,
    pub constraints_accepted: usize,
    pub constraints_rejected: usize,
    pub constraints_unique_keys: usize,
    pub deferred_transactions: usize,
    pub deferred_obligations: usize,
    pub deferred_txn_references: usize,
    pub deferred_committed_references: usize,
    pub deferred_commits: usize,
    pub deferred_rejections: usize,
    pub write_plans: usize,
    pub write_steps: usize,
    pub write_index_mutations: usize,
    pub write_conflict_ranges: usize,
    pub relcommit_groups: usize,
    pub relcommit_row_mutations: usize,
    pub relcommit_index_mutations: usize,
    pub relcommit_conflict_ranges: usize,
    pub relcommit_rejections: usize,
    pub page_frames: usize,
    pub page_redo: usize,
    pub page_reads: usize,
    pub page_rejections: usize,
    pub page_checkpoint_bytes: usize,
    pub wal_appends: usize,
    pub wal_syncs: usize,
    pub wal_flush_allows: usize,
    pub wal_rejections: usize,
    pub wal_durable_lsn: usize,
    pub wal_bytes: usize,
    pub buffer_resident: usize,
    pub buffer_hits: usize,
    pub buffer_dirty: usize,
    pub buffer_flushes: usize,
    pub buffer_evictions: usize,
    pub buffer_rejections: usize,
    pub isolation_commits: usize,
    pub isolation_rejections: usize,
    pub isolation_edges: usize,
    pub isolation_ranges: usize,
    pub watch_registered: usize,
    pub watch_deliveries: usize,
    pub watch_resumes: usize,
    pub erasure_data_shards: usize,
    pub erasure_parity_shards: usize,
    pub erasure_tolerated_loss: usize,
    pub erasure_reconstructed_bytes: usize,
    pub manifest_entries: usize,
    pub manifest_surfaces: usize,
    pub manifest_bytes: usize,
    pub manifest_rejections: usize,
    pub chunkstore_chunks: usize,
    pub chunkstore_pinned: usize,
    pub chunkstore_bytes: usize,
    pub chunkstore_verified: usize,
    pub chunkstore_deduped: usize,
    pub chunkstore_rejections: usize,
    pub checkpoint_roots: usize,
    pub checkpoint_segments: usize,
    pub checkpoint_compacted: usize,
    pub checkpoint_reclaimable_bytes: usize,
    pub checkpoint_rejections: usize,
    pub superblock_generation: usize,
    pub superblock_checkpoint: usize,
    pub superblock_roots: usize,
    pub superblock_advances: usize,
    pub superblock_rejections: usize,
    pub stream_write_chunks: usize,
    pub stream_read_lanes: usize,
    pub stream_window_rejections: usize,
    pub stream_verified_parts: usize,
    pub snapshot_roots: usize,
    pub snapshot_barriers: usize,
    pub snapshot_validations: usize,
    pub storage_committed_prefixes: usize,
    pub storage_manifest_covered: usize,
    pub retention_kept: usize,
    pub retention_actions: usize,
}

impl DatabaseRuntime {
    pub fn new(groups: impl IntoIterator<Item = PlacementGroupId>) -> Self {
        Self {
            router: FabricRouter::with_groups(groups),
            catalog: CatalogEngine::default(),
            auth: AuthEngine::default(),
            app_token: None,
            transactions: TransactionCoordinator::default(),
            kv: KvEngine::default(),
            watches: WatchRegistry::default(),
            sql: SqlEngine::default(),
            objects: ObjectEngine::default(),
            object_metadata: ObjectMetadataEngine::default(),
            blobs: BlobEngine::default(),
            admin: AdminPlane::new(InviteToken("rsf-dev-invite".to_string())),
            leases: LeaseTable::default(),
            tenancy: TenancyCatalog::default(),
            stats: StatisticsCatalog::default(),
            session_transactions: SessionTransactionEngine::default(),
            deployment_roles: 0,
            deployment_endpoints: 0,
            deployment_volumes: 0,
            deployment_probes: 0,
            upgrade_steps: 0,
            upgrade_checks: 0,
            upgrade_nodes: 0,
            upgrade_rejections: 0,
            membership_epoch: 0,
            membership_voters: 0,
            membership_learners: 0,
            membership_promotions: 0,
            membership_intersections: 0,
            membership_rejections: 0,
            vsr_view: 0,
            vsr_prepares: 0,
            vsr_acks: 0,
            vsr_commits: 0,
            vsr_view_changes: 0,
            vsr_rejections: 0,
            repair_findings: 0,
            repair_actions: 0,
            query_rows: 0,
            authz_allowed: 0,
            authz_denied: 0,
            wire_frames: 0,
            wire_protocols: 0,
            wire_rejections: 0,
            wire_body_bytes: 0,
            provider_records: 0,
            provider_selected: 0,
            provider_degraded: 0,
            provider_regions: 0,
            provider_content: 0,
            rpc_calls: 0,
            rpc_retries: 0,
            rpc_rejections: 0,
            rpc_body_bytes: 0,
            api_accepted: 0,
            api_rejected: 0,
            idempotency_first_seen: 0,
            idempotency_replays: 0,
            idempotency_conflicts: 0,
            idempotency_retries: 0,
            gateway_routes: 0,
            gateway_rejections: 0,
            gateway_surfaces: 0,
            consistency_strict: 0,
            consistency_availability_first: 0,
            consistency_rejections: 0,
            quota_allowed: 0,
            quota_rejected: 0,
            quota_policies: 0,
            quota_namespaces: 0,
            cache_hits: 0,
            cache_misses: 0,
            cache_evictions: 0,
            cache_hints: 0,
            workload_classes: 0,
            workload_budget_paths: 0,
            workload_benchmark_claim: 0,
            backup_items: 0,
            restore_checks: 0,
            restore_executed_items: 0,
            restore_executed_surfaces: 0,
            restore_executed_bytes: 0,
            recovery_roots: 0,
            recovery_replays: 0,
            recovery_rejections: 0,
            recovery_actions: 0,
            lease_grants: 0,
            fence_rejections: 0,
            rebalance_moves: 0,
            rebalance_checks: 0,
            rebalance_policy_proofs: 0,
            fault_events: 0,
            dropped_messages: 0,
            delayed_messages: 0,
            failure_drill_passed: 0,
            failure_drill_domains: 0,
            health_healthy: 0,
            health_degraded: 0,
            health_demote: 0,
            health_actions: 0,
            tenant_surfaces: 0,
            schema_steps: 0,
            schema_backfills: 0,
            schema_rejected: 0,
            migration_begun: 0,
            migration_validated: 0,
            migration_published: 0,
            migration_backfilled_rows: 0,
            migration_indexed_keys: 0,
            migration_resumed: 0,
            migration_completed: 0,
            migration_rolled_back: 0,
            migration_rejected: 0,
            jobs_enqueued: 0,
            jobs_leased: 0,
            jobs_completed: 0,
            jobs_retried: 0,
            jobs_blocked: 0,
            session_bound: 0,
            session_authorized: 0,
            session_rejected: 0,
            prepared_statements: 0,
            prepared_binds: 0,
            prepared_invalidations: 0,
            prepared_deallocations: 0,
            prepared_evictions: 0,
            prepared_rejections: 0,
            portal_parses: 0,
            portal_binds: 0,
            portal_describes: 0,
            portal_executes: 0,
            portal_rows: 0,
            portal_cancellations: 0,
            portal_closes: 0,
            portal_statement_closes: 0,
            portal_rejections: 0,
            rowcodec_encoded: 0,
            rowcodec_decoded: 0,
            rowcodec_defaults: 0,
            rowcodec_generated: 0,
            rowcodec_rejections: 0,
            rowcodec_bytes: 0,
            tuple_versions: 0,
            tuple_visible_reads: 0,
            tuple_delete_markers: 0,
            tuple_rejections: 0,
            tuple_bytes: 0,
            vacuum_chains: 0,
            vacuum_pruned_versions: 0,
            vacuum_retained_versions: 0,
            vacuum_rejections: 0,
            vacuum_reclaimed_bytes: 0,
            materialized_rows: 0,
            materialized_projected: 0,
            materialized_columns: 0,
            materialized_rejections: 0,
            materialized_bytes: 0,
            primary_inserts: 0,
            primary_lookups: 0,
            primary_updates: 0,
            primary_deletes: 0,
            primary_rejections: 0,
            primary_entries: 0,
            index_mutations: 0,
            index_entries: 0,
            index_rebuilds: 0,
            planner_physical_plans: 0,
            planner_index_choices: 0,
            planner_range_scans: 0,
            planner_stream_reads: 0,
            executor_plans: 0,
            executor_stages: 0,
            executor_rows: 0,
            executor_remote_reads: 0,
            sql_pipeline_bound: 0,
            sql_pipeline_authorized: 0,
            sql_pipeline_logical: 0,
            sql_pipeline_physical: 0,
            sql_pipeline_stages: 0,
            sql_pipeline_rows: 0,
            sql_pipeline_remote_reads: 0,
            sql_pipeline_rejections: 0,
            sql_dml_bound: 0,
            sql_dml_authorized: 0,
            sql_dml_planned: 0,
            sql_dml_prepared: 0,
            sql_dml_committed: 0,
            sql_dml_isolated: 0,
            sql_dml_kv_applied: 0,
            sql_dml_session_staged: 0,
            sql_dml_overlay_verified: 0,
            sql_dml_rejections: 0,
            sql_txn_began: 0,
            sql_txn_dml_staged: 0,
            sql_txn_overlay_reads: 0,
            sql_txn_commits: 0,
            sql_txn_rollbacks: 0,
            sql_txn_rollback_absent: 0,
            sql_txn_savepoints: 0,
            sql_txn_savepoint_rewinds: 0,
            sql_txn_savepoint_absent: 0,
            sql_txn_rejections: 0,
            range_partitions: 0,
            range_non_overlaps: 0,
            range_rejections: 0,
            sstable_manifests: 0,
            compaction_inputs: 0,
            compaction_outputs: 0,
            compaction_live_keys: 0,
            compaction_tombstones: 0,
            compaction_reclaimed_bytes: 0,
            stats_rows: 0,
            stats_histogram_buckets: 0,
            stats_stale: 0,
            constraints_checked: 0,
            constraints_accepted: 0,
            constraints_rejected: 0,
            constraints_unique_keys: 0,
            deferred_transactions: 0,
            deferred_obligations: 0,
            deferred_txn_references: 0,
            deferred_committed_references: 0,
            deferred_commits: 0,
            deferred_rejections: 0,
            write_plans: 0,
            write_steps: 0,
            write_index_mutations: 0,
            write_conflict_ranges: 0,
            relcommit_groups: 0,
            relcommit_row_mutations: 0,
            relcommit_index_mutations: 0,
            relcommit_conflict_ranges: 0,
            relcommit_rejections: 0,
            page_frames: 0,
            page_redo: 0,
            page_reads: 0,
            page_rejections: 0,
            page_checkpoint_bytes: 0,
            wal_appends: 0,
            wal_syncs: 0,
            wal_flush_allows: 0,
            wal_rejections: 0,
            wal_durable_lsn: 0,
            wal_bytes: 0,
            buffer_resident: 0,
            buffer_hits: 0,
            buffer_dirty: 0,
            buffer_flushes: 0,
            buffer_evictions: 0,
            buffer_rejections: 0,
            isolation_commits: 0,
            isolation_rejections: 0,
            isolation_edges: 0,
            isolation_ranges: 0,
            watch_registered: 0,
            watch_deliveries: 0,
            watch_resumes: 0,
            erasure_data_shards: 0,
            erasure_parity_shards: 0,
            erasure_tolerated_loss: 0,
            erasure_reconstructed_bytes: 0,
            manifest_entries: 0,
            manifest_surfaces: 0,
            manifest_bytes: 0,
            manifest_rejections: 0,
            chunkstore_chunks: 0,
            chunkstore_pinned: 0,
            chunkstore_bytes: 0,
            chunkstore_verified: 0,
            chunkstore_deduped: 0,
            chunkstore_rejections: 0,
            checkpoint_roots: 0,
            checkpoint_segments: 0,
            checkpoint_compacted: 0,
            checkpoint_reclaimable_bytes: 0,
            checkpoint_rejections: 0,
            superblock_generation: 0,
            superblock_checkpoint: 0,
            superblock_roots: 0,
            superblock_advances: 0,
            superblock_rejections: 0,
            stream_write_chunks: 0,
            stream_read_lanes: 0,
            stream_window_rejections: 0,
            stream_verified_parts: 0,
            snapshot_roots: 0,
            snapshot_barriers: 0,
            snapshot_validations: 0,
            storage_committed_prefixes: 0,
            storage_manifest_covered: 0,
            retention_kept: 0,
            retention_actions: 0,
        }
    }

    pub fn protocol_commits(&self) -> usize {
        self.router.committed().len()
    }
}

pub struct Bootstrap;

impl Bootstrap {
    pub fn run(trace: &mut Trace, implementation: &Implementation) {
        trace.push(
            "bootstrap",
            "start",
            format!(
                "{} nodes across {}",
                implementation.topology.nodes,
                implementation.topology.regions.join(",")
            ),
        );
        trace.push(
            "bootstrap",
            "load-topology",
            implementation.topology.placement,
        );
    }
}

pub struct Deployment;

impl Deployment {
    pub fn run(trace: &mut Trace, implementation: &Implementation, runtime: &mut DatabaseRuntime) {
        let package = package_manifest(implementation);
        let profile = package.deployment_profile();
        let manifest = DeploymentPlanner
            .plan(profile)
            .expect("simulated deployment manifest must plan");

        runtime.deployment_roles = manifest.roles.len();
        runtime.deployment_endpoints = manifest.endpoints.len();
        runtime.deployment_volumes = manifest.volumes.len();
        runtime.deployment_probes = manifest.probes.len();

        trace.push(
            "deployment",
            "package",
            format!(
                "id={} name={} confidence={} relational={} object={} blob={} kv={}",
                package.id,
                package.name,
                package.confidence,
                package.relational,
                package.object,
                package.blob,
                package.kv
            ),
        );
        trace.push(
            "deployment",
            "manifest",
            format!(
                "implementation={} roles={} endpoints={} volumes={} probes={}",
                manifest.implementation,
                manifest.roles.len(),
                manifest.endpoints.len(),
                manifest.volumes.len(),
                manifest.probes.len()
            ),
        );
        for role in manifest.roles.iter().take(8) {
            trace.push(
                "deployment",
                "role",
                format!(
                    "node={} region={} role={}",
                    role.node.0,
                    role.region,
                    role.role.as_str()
                ),
            );
        }
        for endpoint in &manifest.endpoints {
            trace.push(
                "deployment",
                "endpoint",
                format!(
                    "name={} role={} address={} port={}",
                    endpoint.name,
                    endpoint.role.as_str(),
                    endpoint.address,
                    endpoint.port
                ),
            );
        }
        for volume in manifest.volumes.iter().take(3) {
            trace.push(
                "deployment",
                "volume",
                format!(
                    "node={} name={} bytes={} purpose={}",
                    volume.node.0, volume.name, volume.bytes, volume.purpose
                ),
            );
        }
        for probe in &manifest.probes {
            trace.push(
                "deployment",
                "probe",
                format!(
                    "name={} role={} path={}",
                    probe.name,
                    probe.role.as_str(),
                    probe.path
                ),
            );
        }
    }
}

fn package_manifest(implementation: &Implementation) -> DeploymentPackageManifest {
    let source = match implementation.id.slug() {
        "universal-vsr-rados" => {
            include_str!("../../../demo-impls/universal-vsr-rados/manifest.rsf")
        }
        "foundationdb-extent" => {
            include_str!("../../../demo-impls/foundationdb-extent/manifest.rsf")
        }
        "aurora-socrates" => include_str!("../../../demo-impls/aurora-socrates/manifest.rsf"),
        "geo-local-wpaxos" => {
            include_str!("../../../demo-impls/geo-local-wpaxos/manifest.rsf")
        }
        "dual-mode-cp-ap" => include_str!("../../../demo-impls/dual-mode-cp-ap/manifest.rsf"),
        other => panic!("missing simulated implementation manifest for {other}"),
    };
    let package = DeploymentPackageManifest::parse(source)
        .expect("checked-in implementation manifest must parse");
    assert_eq!(package.id, implementation.id.slug());
    assert_eq!(package.name, implementation.name);
    assert_eq!(package.confidence, implementation.confidence);
    package
}

pub struct Upgrade;

impl Upgrade {
    pub fn run(trace: &mut Trace, implementation: &Implementation, runtime: &mut DatabaseRuntime) {
        let nodes = Self::nodes(implementation);
        let target = SoftwareVersion::new(1, 3, 0);
        let policy = UpgradePolicy {
            min_compatible_nodes: 3.min(nodes.len()),
            max_unavailable: 1,
            require_same_major: true,
        };
        let plan = UpgradePlanner
            .plan(&nodes, target.clone(), &policy)
            .expect("simulated rolling upgrade plan must be compatible");

        runtime.upgrade_steps = plan.steps.len();
        runtime.upgrade_checks = plan.compatibility_checks.len();
        runtime.upgrade_nodes = nodes.len();

        trace.push(
            "upgrade",
            "preflight",
            format!(
                "from={}.{}.{} to={}.{}.{} nodes={} checks={}",
                plan.from.major,
                plan.from.minor,
                plan.from.patch,
                plan.to.major,
                plan.to.minor,
                plan.to.patch,
                nodes.len(),
                plan.compatibility_checks.len()
            ),
        );
        for check in &plan.compatibility_checks {
            trace.push("upgrade", "compatibility", check);
        }
        for step in &plan.steps {
            if step.kind == UpgradeStepKind::Preflight {
                continue;
            }
            let node = step
                .node
                .map(|node| node.0.to_string())
                .unwrap_or_else(|| "cluster".to_string());
            trace.push(
                "upgrade",
                step.kind.as_str(),
                format!("node={} {}", node, step.detail),
            );
        }

        let rejected = UpgradePlanner
            .plan(
                &nodes,
                SoftwareVersion::new(2, 0, 0),
                &UpgradePolicy {
                    min_compatible_nodes: nodes.len(),
                    max_unavailable: 1,
                    require_same_major: true,
                },
            )
            .expect_err("simulated cross-major upgrade must be rejected");
        runtime.upgrade_rejections = 1;
        trace.push(
            "upgrade",
            "reject",
            format!("target=2.0.0 reason={}", rejected.message),
        );
    }

    fn nodes(implementation: &Implementation) -> Vec<NodeVersion> {
        (0..implementation.topology.nodes)
            .map(|node| {
                let region = implementation.topology.regions
                    [usize::from(node) % implementation.topology.regions.len()];
                NodeVersion {
                    node: NodeId(u64::from(node)),
                    version: SoftwareVersion::new(1, 2, 4),
                    region: RegionId::new(region).expect("simulated upgrade region must be valid"),
                    can_drain: true,
                }
            })
            .collect()
    }
}

pub struct Admission;

impl Admission {
    pub fn run(trace: &mut Trace, implementation: &Implementation, runtime: &mut DatabaseRuntime) {
        for id in 0..implementation.topology.nodes {
            let region = implementation.topology.regions
                [usize::from(id) % implementation.topology.regions.len()];
            let record = runtime
                .admin
                .admit(JoinRequest {
                    token: InviteToken("rsf-dev-invite".to_string()),
                    identity: NodeIdentity::new(
                        format!("poc-node-{id}-public-key"),
                        format!("poc-node-{id}-nonce"),
                        "poc.rsf.local",
                        Digest::sha256([id.saturating_add(1); 32])
                            .expect("simulated TLS fingerprint must be non-zero"),
                    )
                    .expect("simulated mTLS node identity must be valid"),
                    region: RegionId::new(region).expect("simulated join region must be valid"),
                    zone: FailureDomainId::new(format!("{region}-{id}"))
                        .expect("simulated join zone must be valid"),
                    capacity_bytes: 1 << 40,
                    supports_storage: true,
                    supports_gateway: id == 0,
                })
                .expect("simulated node admission must succeed");
            let record = runtime
                .admin
                .mark_learner_synced(record.node.id)
                .expect("simulated learner sync must succeed");
            let record = runtime
                .admin
                .promote_synced_learner(record.node.id)
                .expect("simulated learner promotion must succeed");
            trace.push(
                "admission",
                "join",
                format!(
                    "node={} region={} zone={} state={:?}",
                    record.node.id.0, record.node.region, record.node.failure_domain, record.state
                ),
            );
        }
        let nodes = runtime.admin.cluster_map().nodes.len();
        trace.push(
            "admission",
            "validate-nodes",
            format!("accepted {nodes} simulated nodes with invite token"),
        );
        trace.push(
            "admission",
            "assign-roles",
            "storage, metadata, gateway, repair roles assigned",
        );
    }
}

pub struct Membership;

impl Membership {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) {
        let node_ids = runtime
            .admin
            .cluster_map()
            .nodes
            .iter()
            .map(|node| node.id)
            .collect::<Vec<_>>();
        let mut cell = MembershipCell::new(Epoch(1), node_ids.iter().copied().take(3))
            .expect("simulated membership requires at least three admitted voters");

        if let Some(learner) = node_ids.get(3).copied() {
            for decision in [
                cell.add_learner(learner),
                cell.mark_synced(learner),
                cell.promote(learner),
            ] {
                Self::record_decision(trace, runtime, decision);
            }
        }
        Self::record_decision(trace, runtime, cell.remove_voter(NodeId(0)));
        Self::record_decision(trace, runtime, cell.remove_voter(NodeId(1)));

        let view = cell.view();
        runtime.membership_epoch = view.epoch.0 as usize;
        runtime.membership_voters = view.voters();
        runtime.membership_learners = view.learners();
        trace.push(
            "membership",
            "summary",
            format!(
                "epoch={} voters={} learners={} promotions={} quorum_intersections={} rejections={}",
                runtime.membership_epoch,
                runtime.membership_voters,
                runtime.membership_learners,
                runtime.membership_promotions,
                runtime.membership_intersections,
                runtime.membership_rejections
            ),
        );
    }

    fn record_decision(
        trace: &mut Trace,
        runtime: &mut DatabaseRuntime,
        decision: MembershipDecision,
    ) {
        match decision {
            MembershipDecision::AddedLearner(member) => {
                trace.push("membership", "add-learner", membership_member(&member));
            }
            MembershipDecision::MarkedSynced(member) => {
                trace.push("membership", "sync", membership_member(&member));
            }
            MembershipDecision::Promoted {
                member,
                epoch,
                quorum,
            } => {
                runtime.membership_promotions += 1;
                if quorum.preserves_quorum_intersection() {
                    runtime.membership_intersections += 1;
                }
                trace.push(
                    "membership",
                    "promote",
                    format!(
                        "{} epoch={} previous_voters={} next_voters={} previous_quorum={} next_quorum={} shared_voters={} minimum_intersection={} preserves_intersection={}",
                        membership_member(&member),
                        epoch.0,
                        quorum.previous_voters,
                        quorum.next_voters,
                        quorum.previous_quorum,
                        quorum.next_quorum,
                        quorum.shared_voters,
                        quorum.minimum_intersection,
                        quorum.preserves_quorum_intersection()
                    ),
                );
            }
            MembershipDecision::Removed {
                node,
                epoch,
                quorum,
            } => {
                trace.push(
                    "membership",
                    "remove",
                    format!(
                        "node={} epoch={} previous_voters={} next_voters={} shared_voters={} preserves_intersection={}",
                        node.0,
                        epoch.0,
                        quorum.previous_voters,
                        quorum.next_voters,
                        quorum.shared_voters,
                        quorum.preserves_quorum_intersection()
                    ),
                );
            }
            MembershipDecision::Replaced {
                removed,
                replacement,
                epoch,
                quorum,
            } => {
                trace.push(
                    "membership",
                    "replace",
                    format!(
                        "removed={} replacement={} epoch={} previous_voters={} next_voters={} shared_voters={} preserves_intersection={}",
                        removed.0,
                        replacement.node.0,
                        epoch.0,
                        quorum.previous_voters,
                        quorum.next_voters,
                        quorum.shared_voters,
                        quorum.preserves_quorum_intersection()
                    ),
                );
            }
            MembershipDecision::Rejected { node, reason } => {
                runtime.membership_rejections += 1;
                trace.push(
                    "membership",
                    "reject",
                    format!("node={} reason={reason}", node.0),
                );
            }
        }
    }
}

pub struct Placement;

impl Placement {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) -> Vec<PlacementGroupId> {
        let groups = runtime
            .admin
            .map_replicated_groups(3, 3)
            .expect("simulated placement groups must map over admitted nodes");
        let group_ids = groups.iter().map(|group| group.id).collect::<Vec<_>>();
        trace.push(
            "placement",
            "map-groups",
            format!(
                "{} placement groups mapped over {} nodes",
                groups.len(),
                runtime.admin.cluster_map().nodes.len()
            ),
        );
        for group in &groups {
            trace.push(
                "placement",
                "acting-set",
                format!(
                    "pg={} epoch={} primary={} replicas={:?}",
                    group.id.0,
                    group.acting_set.epoch.0,
                    group.acting_set.primary.0,
                    group
                        .acting_set
                        .replicas
                        .iter()
                        .map(|node| node.0)
                        .collect::<Vec<_>>()
                ),
            );
        }
        group_ids
    }
}

pub struct Faults;

impl Faults {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) {
        let mut sim = Simulation::new(7);
        sim.schedule(
            LogicalTime(1),
            Event::new(EventKind::NodeStarted(NodeId(0))),
        );
        sim.schedule(
            LogicalTime(2),
            Event::new(EventKind::NodeCrashed(NodeId(2))),
        );
        sim.schedule(
            LogicalTime(3),
            Event::new(EventKind::MessageDelivered {
                from: NodeId(0),
                to: NodeId(1),
            }),
        );
        sim.schedule(
            LogicalTime(4),
            Event::new(EventKind::MessageDropped {
                from: NodeId(1),
                to: NodeId(2),
            }),
        );
        sim.schedule(
            LogicalTime(5),
            Event::new(EventKind::NodeRestarted(NodeId(2))),
        );

        while let Some(event) = sim.next_event() {
            runtime.fault_events += 1;
            trace.push(
                "fault",
                "event",
                format!("t={} {:?}", event.at.0, event.kind),
            );
        }

        let mut faults = FaultPlan::default();
        faults
            .delay(NodeId(0), NodeId(1), 4)
            .expect("simulated delayed link must be valid");
        Self::record_delivery(
            trace,
            runtime,
            "delay-link",
            faults
                .delivery(NodeId(0), NodeId(1), LogicalTime(10))
                .expect("simulated delayed delivery must decide"),
        );
        faults
            .partition(NodeId(1), NodeId(2))
            .expect("simulated partition link must be valid");
        Self::record_delivery(
            trace,
            runtime,
            "partitioned-link",
            faults
                .delivery(NodeId(1), NodeId(2), LogicalTime(11))
                .expect("simulated partition delivery must decide"),
        );
        faults
            .heal(NodeId(1), NodeId(2))
            .expect("simulated partition heal must be valid");
        Self::record_delivery(
            trace,
            runtime,
            "healed-link",
            faults
                .delivery(NodeId(1), NodeId(2), LogicalTime(12))
                .expect("simulated healed delivery must decide"),
        );
    }

    fn record_delivery(
        trace: &mut Trace,
        runtime: &mut DatabaseRuntime,
        label: &'static str,
        decision: DeliveryDecision,
    ) {
        match decision {
            DeliveryDecision::Deliver { at } => {
                trace.push("fault", "deliver", format!("{label} at t={}", at.0));
            }
            DeliveryDecision::Delay { until } => {
                runtime.delayed_messages += 1;
                trace.push("fault", "delay", format!("{label} until t={}", until.0));
            }
            DeliveryDecision::Drop { reason } => {
                runtime.dropped_messages += 1;
                trace.push("fault", "drop", format!("{label}: {reason}"));
            }
        }
    }
}

pub struct FailureDrill;

impl FailureDrill {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) {
        let report = FailureDrillReport::evaluate(&FailureDrillInput {
            fault_events: runtime.fault_events as u8,
            dropped_messages: runtime.dropped_messages as u8,
            delayed_messages: runtime.delayed_messages as u8,
            recovery_roots: runtime.recovery_roots as u8,
            recovery_rejections: runtime.recovery_rejections as u8,
            provider_degraded: runtime.provider_degraded as u8,
            health_demotions: runtime.health_demote as u8,
            rebalance_moves: runtime.rebalance_moves as u8,
            rebalance_policy_proofs: runtime.rebalance_policy_proofs as u8,
        });

        runtime.failure_drill_passed = usize::from(report.passed);
        runtime.failure_drill_domains = report.domains_covered as usize;
        trace.push(
            "fault",
            "drill",
            format!(
                "passed={} domains={}",
                report.passed, report.domains_covered
            ),
        );
        for finding in report.findings {
            trace.push("fault", "drill-domain", finding);
        }
    }
}

pub struct Lease;

impl Lease {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) {
        let groups = runtime
            .admin
            .map_replicated_groups(3, 3)
            .expect("simulated lease placement groups must map");
        let mut first_pg_fence: Option<FenceToken> = None;
        for group in &groups {
            let lease = runtime
                .leases
                .acquire(
                    group.id,
                    group.acting_set.primary,
                    group.acting_set.epoch,
                    CommitVersion(group.acting_set.epoch.0),
                    20,
                )
                .expect("simulated primary lease must acquire");
            runtime.lease_grants += 1;
            if group.id == PlacementGroupId(1) {
                first_pg_fence = Some(lease.fence.clone());
            }
            trace.push(
                "lease",
                "grant",
                format!(
                    "pg={} primary={} epoch={} fence={} expires_at={}",
                    lease.pg.0,
                    lease.primary.0,
                    lease.epoch.0,
                    lease.fence.sequence,
                    lease.expires_at.0
                ),
            );
        }

        let replacement = runtime
            .leases
            .acquire(
                PlacementGroupId(1),
                rsf_core::NodeId(99),
                Epoch(runtime.admin.cluster_map().epoch.0 + 1),
                CommitVersion(runtime.admin.cluster_map().epoch.0 + 1),
                20,
            )
            .expect("simulated view change lease must replace old primary");
        runtime.lease_grants += 1;
        trace.push(
            "lease",
            "view-change",
            format!(
                "pg=1 new_primary={} epoch={} fence={}",
                replacement.primary.0, replacement.epoch.0, replacement.fence.sequence
            ),
        );

        let stale_fence = first_pg_fence.expect("pg=1 lease must be captured before view change");
        let decision = runtime
            .leases
            .validate_write(&stale_fence, CommitVersion(replacement.epoch.0));
        if let FenceDecision::Rejected { reason } = decision {
            runtime.fence_rejections += 1;
            trace.push(
                "lease",
                "reject-stale",
                format!(
                    "pg=1 old_primary={} rejected: {}",
                    stale_fence.primary.0, reason
                ),
            );
        }
    }
}

pub struct VsrService;

impl VsrService {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) {
        let mut cell = VsrCell::new(
            PlacementGroupId(1),
            Epoch(1),
            NodeId(0),
            [NodeId(0), NodeId(1), NodeId(2)],
        )
        .expect("simulated vsr cell must have quorum replicas");

        for decision in [
            cell.prepare(
                NodeId(0),
                VsrOperation::CatalogMutation {
                    namespace: "app".to_string(),
                    item: "users".to_string(),
                },
                128,
            ),
            cell.acknowledge(NodeId(1), Epoch(1), LogIndex(1)),
            cell.commit(NodeId(0), LogIndex(1)),
            cell.view_change(Epoch(2), NodeId(1)),
            cell.prepare(
                NodeId(0),
                VsrOperation::TransactionCommit {
                    key: "app/users/4".to_string(),
                },
                64,
            ),
            cell.prepare(
                NodeId(1),
                VsrOperation::ObjectManifest {
                    bucket: "media".to_string(),
                    key: "hero.png".to_string(),
                },
                96,
            ),
            cell.acknowledge(NodeId(2), Epoch(2), LogIndex(2)),
            cell.commit(NodeId(1), LogIndex(2)),
        ] {
            Self::record_decision(trace, runtime, decision);
        }

        runtime.vsr_view = cell.view().0 as usize;
        trace.push(
            "vsr",
            "summary",
            format!(
                "view={} primary={} quorum={} prepares={} acks={} commits={} view_changes={} rejections={}",
                cell.view().0,
                cell.primary().0,
                cell.quorum(),
                runtime.vsr_prepares,
                runtime.vsr_acks,
                runtime.vsr_commits,
                runtime.vsr_view_changes,
                runtime.vsr_rejections
            ),
        );
    }

    fn record_decision(trace: &mut Trace, runtime: &mut DatabaseRuntime, decision: VsrDecision) {
        match decision {
            VsrDecision::Prepared(prepare) => {
                runtime.vsr_prepares += 1;
                trace.push("vsr", "prepare", vsr_prepare(&prepare));
            }
            VsrDecision::Acknowledged {
                replica,
                view,
                index,
                acknowledgements,
            } => {
                runtime.vsr_acks += 1;
                trace.push(
                    "vsr",
                    "prepare-ok",
                    format!(
                        "replica={} view={} index={} acknowledgements={}",
                        replica.0, view.0, index.0, acknowledgements
                    ),
                );
            }
            VsrDecision::Committed(commit) => {
                runtime.vsr_commits += 1;
                trace.push("vsr", "commit", vsr_commit(&commit));
            }
            VsrDecision::ViewChanged {
                view,
                primary,
                committed,
            } => {
                runtime.vsr_view_changes += 1;
                trace.push(
                    "vsr",
                    "view-change",
                    format!(
                        "view={} primary={} carried_commits={}",
                        view.0, primary.0, committed
                    ),
                );
            }
            VsrDecision::MembershipChanged {
                view,
                primary,
                replicas,
                committed_tail,
            } => {
                runtime.vsr_view_changes += 1;
                trace.push(
                    "vsr",
                    "membership-change",
                    format!(
                        "view={} primary={} replicas={} committed_tail={}",
                        view.0, primary.0, replicas, committed_tail.0
                    ),
                );
            }
            VsrDecision::Rejected { node, reason } => {
                runtime.vsr_rejections += 1;
                trace.push("vsr", "reject", format!("node={} reason={reason}", node.0));
            }
        }
    }
}

pub struct Fabric;

pub struct Health;

impl Health {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) {
        let report = HealthScorer
            .evaluate(
                runtime.admin.cluster_map(),
                &Self::observations(
                    &runtime
                        .admin
                        .cluster_map()
                        .nodes
                        .iter()
                        .map(|node| node.id)
                        .collect::<Vec<_>>(),
                ),
                &HealthPolicy::default(),
            )
            .expect("simulated node health evaluation must succeed");

        runtime.health_healthy = report
            .nodes
            .iter()
            .filter(|health| health.class == HealthClass::Healthy)
            .count();
        runtime.health_degraded = report
            .nodes
            .iter()
            .filter(|health| health.class == HealthClass::Degraded)
            .count();
        runtime.health_demote = report
            .nodes
            .iter()
            .filter(|health| health.class == HealthClass::Demote)
            .count();
        runtime.health_actions = report.actions.len();

        for health in &report.nodes {
            trace.push(
                "health",
                "score",
                format!(
                    "node={} class={} score={} reasons={}",
                    health.node.0,
                    health_class(health.class),
                    health.score,
                    health_reasons(health)
                ),
            );
        }

        for action in report.actions {
            trace.push("health", "action", health_action(action));
        }
    }

    fn observations(nodes: &[NodeId]) -> Vec<NodeObservation> {
        nodes.iter().copied().map(Self::observation).collect()
    }

    fn observation(node: NodeId) -> NodeObservation {
        match node.0 {
            2 => NodeObservation {
                node,
                disk_free_bps: 4_800,
                p99_latency_ms: 95,
                replica_lag_bytes: 2_500_000,
                missed_heartbeats: 1,
                corruption_events: 0,
            },
            4 => NodeObservation {
                node,
                disk_free_bps: 1_000,
                p99_latency_ms: 19,
                replica_lag_bytes: 0,
                missed_heartbeats: 0,
                corruption_events: 1,
            },
            _ => NodeObservation {
                node,
                disk_free_bps: 5_000 + (node.0 as u16 % 10) * 100,
                p99_latency_ms: 8 + (node.0 as u16 % 5),
                replica_lag_bytes: node.0 * 32,
                missed_heartbeats: 0,
                corruption_events: 0,
            },
        }
    }
}

pub struct Catalog;

pub struct Tenancy;

impl Tenancy {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) {
        let tenant = runtime
            .tenancy
            .create_tenant("acme", "iad")
            .expect("simulated tenant creation must succeed");
        trace.push(
            "tenancy",
            "tenant",
            format!(
                "tenant={} default_region={}",
                tenant.name.0, tenant.default_region
            ),
        );

        let surfaces = [
            TenantSurface::Relational,
            TenantSurface::Kv,
            TenantSurface::Object,
            TenantSurface::Blob,
        ];
        let database = runtime
            .tenancy
            .create_database(&tenant.name, "prod", surfaces)
            .expect("simulated database creation must succeed");
        runtime.tenant_surfaces = database.surfaces.len();
        trace.push(
            "tenancy",
            "database",
            format!(
                "tenant={} database={} surfaces={}",
                database.tenant.0,
                database.name.0,
                database
                    .surfaces
                    .iter()
                    .map(|surface| surface.as_str())
                    .collect::<Vec<_>>()
                    .join(",")
            ),
        );

        let namespace = runtime
            .tenancy
            .bind_namespace(&tenant.name, &database.name, "app")
            .expect("simulated namespace binding must succeed");
        trace.push(
            "tenancy",
            "namespace",
            format!(
                "namespace={} owner={}.{}",
                namespace.namespace.0, namespace.tenant.0, namespace.database.0
            ),
        );

        runtime
            .tenancy
            .requires_surface(&tenant.name, &database.name, TenantSurface::Relational)
            .expect("simulated relational surface must be enabled");
        runtime
            .tenancy
            .requires_surface(&tenant.name, &database.name, TenantSurface::Object)
            .expect("simulated object surface must be enabled");
        trace.push(
            "tenancy",
            "surfaces",
            "validated relational and object surfaces before catalog publication",
        );
    }
}

impl Catalog {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) {
        let namespace = runtime
            .catalog
            .create_namespace(&mut runtime.router, PlacementGroupId(1), "app")
            .expect("simulated namespace create must commit through catalog");
        trace.push(
            "catalog",
            "namespace",
            format!(
                "namespace={} version={} pg={} log_index={} op={}",
                namespace.name.0,
                namespace.version.0,
                namespace.evidence.placement_group.0,
                namespace.evidence.log_index.0,
                namespace.evidence.operation_id.0
            ),
        );

        let table = runtime
            .catalog
            .create_table(
                &mut runtime.router,
                PlacementGroupId(1),
                TableDescriptor {
                    namespace: NamespaceName("app".to_string()),
                    name: TableName("users".to_string()),
                    columns: vec![
                        ColumnDescriptor {
                            name: "id".to_string(),
                            ty: ColumnType::Int64,
                            nullable: false,
                        },
                        ColumnDescriptor {
                            name: "name".to_string(),
                            ty: ColumnType::Text,
                            nullable: false,
                        },
                        ColumnDescriptor {
                            name: "active".to_string(),
                            ty: ColumnType::Bool,
                            nullable: false,
                        },
                    ],
                    primary_key: vec!["id".to_string()],
                },
            )
            .expect("simulated table descriptor must commit through catalog");
        trace.push(
            "catalog",
            "table",
            format!(
                "{}.{} columns={} primary_key={:?}",
                table.namespace.0,
                table.name.0,
                table.columns.len(),
                table.primary_key
            ),
        );

        let bucket = runtime
            .catalog
            .create_bucket(&mut runtime.router, PlacementGroupId(1), "app", "media")
            .expect("simulated bucket descriptor must commit through catalog");
        trace.push(
            "catalog",
            "bucket",
            format!(
                "{}.{} version={} pg={} log_index={} op={}",
                bucket.namespace.0,
                bucket.name,
                bucket.version.0,
                bucket.evidence.placement_group.0,
                bucket.evidence.log_index.0,
                bucket.evidence.operation_id.0
            ),
        );

        for (surface, pg) in [
            (CatalogSurface::Relational, PlacementGroupId(1)),
            (CatalogSurface::Object, PlacementGroupId(2)),
            (CatalogSurface::Blob, PlacementGroupId(3)),
            (CatalogSurface::Kv, PlacementGroupId(1)),
        ] {
            let binding = runtime
                .catalog
                .bind_surface(&mut runtime.router, PlacementGroupId(1), "app", surface, pg)
                .expect("simulated surface binding must commit through catalog");
            trace.push(
                "catalog",
                "binding",
                format!(
                    "{} surface bound to pg={} at version {} evidence_pg={} log_index={} op={}",
                    catalog_surface(&binding.surface),
                    binding.placement_group.0,
                    binding.version.0,
                    binding.evidence.placement_group.0,
                    binding.evidence.log_index.0,
                    binding.evidence.operation_id.0
                ),
            );
        }

        let snapshot = runtime.catalog.snapshot(&runtime.router);
        trace.push(
            "catalog",
            "snapshot",
            format!(
                "version={} namespaces={} tables={} buckets={} bindings={}",
                snapshot.version.0,
                snapshot.namespaces,
                snapshot.tables,
                snapshot.buckets,
                snapshot.bindings
            ),
        );
    }
}

pub struct Schema;

impl Schema {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) {
        let table = runtime
            .catalog
            .table("app", "users")
            .cloned()
            .expect("simulated schema service requires users table descriptor");
        let from = runtime
            .catalog
            .table_version("app", "users")
            .map(SchemaVersion)
            .expect("simulated schema service requires table version");
        let target = SchemaVersion(CommitVersion(from.0.0 + 1));
        let planner = SchemaPlanner;
        let plan = planner
            .plan(
                SchemaMigration {
                    id: "users_add_email".to_string(),
                    table: table.clone(),
                    from: from.clone(),
                    changes: vec![
                        SchemaChange::AddColumn(ColumnDescriptor {
                            name: "email".to_string(),
                            ty: ColumnType::Text,
                            nullable: true,
                        }),
                        SchemaChange::AddIndex {
                            name: "users_email".to_string(),
                            columns: vec!["email".to_string()],
                        },
                    ],
                },
                target.clone(),
                &MigrationPolicy::default(),
            )
            .expect("simulated additive schema migration must plan");
        runtime.schema_steps = plan.steps.len();
        runtime.schema_backfills = plan
            .steps
            .iter()
            .filter(|step| matches!(step, MigrationStep::BackfillColumn { .. }))
            .count();
        trace.push(
            "schema",
            "plan",
            format!(
                "{} {}.{} from={} to={} columns={} old_readers={} old_writers={}",
                plan.id,
                plan.table.namespace.0,
                plan.table.name.0,
                plan.from.0.0,
                plan.to.0.0,
                plan.table.columns.len(),
                plan.compatibility.old_readers,
                plan.compatibility.old_writers
            ),
        );
        for step in &plan.steps {
            trace.push("schema", "step", schema_step(step));
        }

        let rejected = planner
            .plan(
                SchemaMigration {
                    id: "users_drop_id".to_string(),
                    table,
                    from,
                    changes: vec![SchemaChange::DropColumn {
                        name: "id".to_string(),
                    }],
                },
                SchemaVersion(CommitVersion(target.0.0 + 1)),
                &MigrationPolicy::default(),
            )
            .expect_err("simulated unsafe schema drop must be rejected");
        runtime.schema_rejected += 1;
        trace.push(
            "schema",
            "reject",
            format!("unsafe migration rejected: {}", rejected.message),
        );
    }
}

pub struct MigrationExecution;

impl MigrationExecution {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) {
        let table = runtime
            .catalog
            .table("app", "users")
            .cloned()
            .expect("simulated migration service requires users table descriptor");
        let from = runtime
            .catalog
            .table_version("app", "users")
            .map(SchemaVersion)
            .expect("simulated migration service requires table version");
        let planner = SchemaPlanner;
        let mut lifecycle = MigrationLifecycle::default();

        let completed_plan = planner
            .plan(
                SchemaMigration {
                    id: "users_add_status".to_string(),
                    table: table.clone(),
                    from: from.clone(),
                    changes: vec![
                        SchemaChange::AddColumn(ColumnDescriptor {
                            name: "status".to_string(),
                            ty: ColumnType::Text,
                            nullable: false,
                        }),
                        SchemaChange::AddIndex {
                            name: "users_status".to_string(),
                            columns: vec!["status".to_string()],
                        },
                    ],
                },
                SchemaVersion(CommitVersion(from.0.0 + 10)),
                &MigrationPolicy::default(),
            )
            .expect("simulated completed migration must plan");
        Self::record(
            trace,
            runtime,
            lifecycle
                .begin(completed_plan)
                .expect("simulated migration begin must succeed"),
        );
        Self::record(
            trace,
            runtime,
            lifecycle
                .advance("users_add_status", 0, 0)
                .expect("simulated schema validation must advance"),
        );
        Self::record(
            trace,
            runtime,
            lifecycle
                .advance("users_add_status", 0, 0)
                .expect("simulated descriptor publish must advance"),
        );
        Self::record(
            trace,
            runtime,
            lifecycle
                .advance("users_add_status", 1_000, 0)
                .expect("simulated backfill must advance"),
        );
        Self::record(
            trace,
            runtime,
            lifecycle
                .resume("users_add_status")
                .expect("simulated migration resume must report progress"),
        );
        Self::record(
            trace,
            runtime,
            lifecycle
                .advance("users_add_status", 0, 1_000)
                .expect("simulated index catch-up must advance"),
        );
        Self::record(
            trace,
            runtime,
            lifecycle
                .advance("users_add_status", 0, 0)
                .expect("simulated read switch must complete migration"),
        );

        let rollback_plan = planner
            .plan(
                SchemaMigration {
                    id: "users_add_locale".to_string(),
                    table,
                    from,
                    changes: vec![SchemaChange::AddColumn(ColumnDescriptor {
                        name: "locale".to_string(),
                        ty: ColumnType::Text,
                        nullable: true,
                    })],
                },
                SchemaVersion(CommitVersion(99)),
                &MigrationPolicy::default(),
            )
            .expect("simulated rollback migration must plan");
        Self::record(
            trace,
            runtime,
            lifecycle
                .begin(rollback_plan)
                .expect("simulated rollback migration begin must succeed"),
        );
        Self::record(
            trace,
            runtime,
            lifecycle
                .advance("users_add_locale", 0, 0)
                .expect("simulated rollback validation must advance"),
        );
        Self::record(
            trace,
            runtime,
            lifecycle
                .advance("users_add_locale", 0, 0)
                .expect("simulated rollback descriptor publish must advance"),
        );
        Self::record(
            trace,
            runtime,
            lifecycle
                .rollback("users_add_locale", "operator abort before read switch")
                .expect("simulated rollback must record reason"),
        );
        Self::record(
            trace,
            runtime,
            lifecycle
                .advance("users_add_locale", 1, 1)
                .expect("simulated rolled-back migration must reject advance"),
        );

        trace.push(
            "migration",
            "summary",
            format!(
                "begun={} validated={} published={} backfilled_rows={} indexed_keys={} resumed={} completed={} rolled_back={} rejected={}",
                runtime.migration_begun,
                runtime.migration_validated,
                runtime.migration_published,
                runtime.migration_backfilled_rows,
                runtime.migration_indexed_keys,
                runtime.migration_resumed,
                runtime.migration_completed,
                runtime.migration_rolled_back,
                runtime.migration_rejected
            ),
        );
    }

    fn record(trace: &mut Trace, runtime: &mut DatabaseRuntime, decision: MigrationDecision) {
        match decision {
            MigrationDecision::Began(progress) => {
                runtime.migration_begun += 1;
                trace.push("migration", "begin", migration_progress(&progress));
            }
            MigrationDecision::SchemaValidated(progress) => {
                runtime.migration_validated += 1;
                trace.push("migration", "validate", migration_progress(&progress));
            }
            MigrationDecision::DescriptorPublished(progress) => {
                runtime.migration_published += 1;
                trace.push("migration", "publish", migration_progress(&progress));
            }
            MigrationDecision::BackfillBatch(progress) => {
                runtime.migration_backfilled_rows += progress.backfilled_rows as usize;
                trace.push("migration", "backfill", migration_progress(&progress));
            }
            MigrationDecision::IndexCatchUp(progress) => {
                runtime.migration_indexed_keys += progress.indexed_keys as usize;
                trace.push("migration", "index", migration_progress(&progress));
            }
            MigrationDecision::ReadsSwitched(progress) => {
                trace.push("migration", "switch", migration_progress(&progress));
            }
            MigrationDecision::Completed(progress) => {
                runtime.migration_completed += 1;
                trace.push("migration", "complete", migration_progress(&progress));
            }
            MigrationDecision::Resumed(progress) => {
                runtime.migration_resumed += 1;
                trace.push("migration", "resume", migration_progress(&progress));
            }
            MigrationDecision::RolledBack(progress) => {
                runtime.migration_rolled_back += 1;
                trace.push("migration", "rollback", migration_progress(&progress));
            }
            MigrationDecision::Rejected { id, reason } => {
                runtime.migration_rejected += 1;
                trace.push("migration", "reject", format!("id={id} reason={reason}"));
            }
        }
    }
}

pub struct Auth;

pub struct Jobs;

impl Jobs {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) {
        let mut queue = JobQueue::default();
        let jobs = [
            job_spec(
                "schema-users-email",
                JobKind::SchemaBackfill,
                "users.email",
                [],
            ),
            job_spec(
                "index-users-email",
                JobKind::IndexBuild,
                "users_email",
                ["schema-users-email"],
            ),
            job_spec("stats-users-refresh", JobKind::StatsRefresh, "users", []),
            job_spec(
                "compact-users-range",
                JobKind::SstableCompaction,
                "range/users",
                [],
            ),
            job_spec("repair-pg-2", JobKind::RepairCopy, "pg/2", []),
        ];

        for spec in jobs {
            let id = spec.id.0.clone();
            let kind = spec.kind;
            let target = spec.target.clone();
            match queue
                .enqueue(spec)
                .expect("simulated job enqueue must evaluate")
            {
                JobDecision::Enqueued => {
                    runtime.jobs_enqueued += 1;
                    trace.push(
                        "jobs",
                        "enqueue",
                        format!("job={id} kind={} target={target}", kind.as_str()),
                    );
                }
                JobDecision::Duplicate => {
                    trace.push("jobs", "duplicate", format!("job={id} already enqueued"));
                }
                _ => unreachable!("enqueue only returns enqueue-or-duplicate decisions"),
            }
        }

        let blocked = queue
            .lease_ready(
                &JobId("index-users-email".to_string()),
                "worker-index",
                runtime.router.latest_version(),
                5,
            )
            .expect("simulated blocked job lease must evaluate");
        if let JobDecision::Blocked { missing } = blocked {
            runtime.jobs_blocked += 1;
            trace.push(
                "jobs",
                "blocked",
                format!(
                    "job=index-users-email waiting_on={}",
                    missing
                        .iter()
                        .map(|job| job.0.clone())
                        .collect::<Vec<_>>()
                        .join(",")
                ),
            );
        }

        let schema = Self::lease(
            trace,
            runtime,
            &mut queue,
            JobId("schema-users-email".to_string()),
            "worker-schema",
        );
        queue
            .complete(
                &schema,
                CommitVersion(runtime.router.latest_version().0 + 1),
            )
            .expect("simulated job completion must succeed");
        runtime.jobs_completed += 1;
        trace.push(
            "jobs",
            "complete",
            format!("job={} worker={}", schema.id.0, schema.worker),
        );

        let stats = Self::lease(
            trace,
            runtime,
            &mut queue,
            JobId("stats-users-refresh".to_string()),
            "worker-stats",
        );
        let retry = queue
            .fail(&stats, "sample split moved")
            .expect("simulated job retry must evaluate");
        if let JobDecision::Retrying { next_attempt } = retry {
            runtime.jobs_retried += 1;
            trace.push(
                "jobs",
                "retry",
                format!(
                    "job={} worker={} reason=sample-split-moved next_attempt={next_attempt}",
                    stats.id.0, stats.worker
                ),
            );
        }

        trace.push(
            "jobs",
            "summary",
            format!(
                "queued={} enqueued={} leased={} completed={} retried={} blocked={}",
                queue.job_count(),
                runtime.jobs_enqueued,
                runtime.jobs_leased,
                runtime.jobs_completed,
                runtime.jobs_retried,
                runtime.jobs_blocked
            ),
        );
    }

    fn lease(
        trace: &mut Trace,
        runtime: &mut DatabaseRuntime,
        queue: &mut JobQueue,
        id: JobId,
        worker: &'static str,
    ) -> rsf_jobs::JobLease {
        let decision = queue
            .lease_ready(&id, worker, runtime.router.latest_version(), 5)
            .expect("simulated ready job lease must evaluate");
        let JobDecision::Leased(lease) = decision else {
            panic!("simulated job should lease");
        };
        runtime.jobs_leased += 1;
        trace.push(
            "jobs",
            "lease",
            format!(
                "job={} worker={} attempt={} expires_at={}",
                lease.id.0, lease.worker, lease.attempt, lease.expires_at.0
            ),
        );
        lease
    }
}

impl Auth {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) {
        let app = Principal {
            id: PrincipalId("app-client".to_string()),
            kind: PrincipalKind::User,
        };
        let repair = Principal {
            id: PrincipalId("repair-worker".to_string()),
            kind: PrincipalKind::Service,
        };
        runtime
            .auth
            .register_principal(app.clone())
            .expect("simulated app principal registration must succeed");
        runtime
            .auth
            .register_principal(repair.clone())
            .expect("simulated repair principal registration must succeed");
        trace.push(
            "auth",
            "principals",
            "registered app-client user and repair-worker service identities",
        );

        for grant in [
            Grant {
                principal: app.id.clone(),
                capability: Capability::NamespaceRead,
                resource: Resource::Namespace("app".to_string()),
            },
            Grant {
                principal: app.id.clone(),
                capability: Capability::QueryExecute,
                resource: Resource::Namespace("app".to_string()),
            },
            Grant {
                principal: app.id.clone(),
                capability: Capability::NamespaceWrite,
                resource: Resource::Namespace("app".to_string()),
            },
            Grant {
                principal: app.id.clone(),
                capability: Capability::ObjectWrite,
                resource: Resource::surface("app", "object").unwrap(),
            },
            Grant {
                principal: app.id.clone(),
                capability: Capability::BlobWrite,
                resource: Resource::surface("app", "blob").unwrap(),
            },
            Grant {
                principal: repair.id.clone(),
                capability: Capability::RepairOperate,
                resource: Resource::Cluster,
            },
        ] {
            runtime
                .auth
                .grant(grant)
                .expect("simulated auth grant must target a known principal");
        }
        trace.push(
            "auth",
            "grants",
            format!(
                "{} capability grants loaded from simulated control-plane policy",
                runtime.auth.grant_count()
            ),
        );

        let now = runtime.router.latest_version();
        let token = runtime
            .auth
            .issue_token(&app.id, now, 100)
            .expect("simulated app token must issue");
        runtime.app_token = Some(token.clone());
        trace.push(
            "auth",
            "token",
            format!(
                "issued app-client token at version {} expiring at {}",
                token.issued_at.0, token.expires_at.0
            ),
        );

        let allowed = runtime
            .auth
            .authorize(
                &token,
                now,
                AccessRequest {
                    principal: app.id.clone(),
                    capability: Capability::QueryExecute,
                    resource: Resource::surface("app", "relational").unwrap(),
                },
            )
            .expect("simulated query authorization must evaluate");
        Self::record_decision(trace, runtime, "query-execute", allowed);

        let denied = runtime
            .auth
            .authorize(
                &token,
                now,
                AccessRequest {
                    principal: app.id,
                    capability: Capability::RepairOperate,
                    resource: Resource::Cluster,
                },
            )
            .expect("simulated repair authorization must evaluate");
        Self::record_decision(trace, runtime, "repair-operate", denied);
    }

    fn record_decision(
        trace: &mut Trace,
        runtime: &mut DatabaseRuntime,
        action: &'static str,
        decision: Authorization,
    ) {
        match decision {
            Authorization::Allowed => {
                runtime.authz_allowed += 1;
                trace.push("auth", "allow", format!("{action} authorized"));
            }
            Authorization::Denied { reason } => {
                runtime.authz_denied += 1;
                trace.push("auth", "deny", format!("{action} denied: {reason}"));
            }
        }
    }
}

pub struct Api;

pub struct Wire;

impl Wire {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) {
        let token = runtime
            .app_token
            .as_ref()
            .expect("simulated wire adapter must have issued app token");
        let principal = token.principal.0.clone();
        let adapter = WireAdapter;
        let decisions = Self::frames(&principal)
            .into_iter()
            .map(|frame| {
                adapter
                    .decode(frame)
                    .expect("simulated wire frame decode must evaluate")
            })
            .collect::<Vec<_>>();
        validate_decisions(&decisions).expect("simulated wire decisions must validate");

        runtime.wire_frames = decisions.len();
        runtime.wire_protocols = protocol_count(&decisions);
        runtime.wire_rejections = decisions
            .iter()
            .filter(|decision| !decision.is_accepted())
            .count();
        runtime.wire_body_bytes = accepted_body_bytes(&decisions) as usize;

        for decision in decisions {
            match decision {
                WireDecision::Accepted(envelope) => trace.push(
                    "wire",
                    "accept",
                    format!(
                        "protocol={} principal={} namespace={} operation={} idempotency={} bytes={}",
                        envelope.protocol.as_str(),
                        envelope.principal,
                        envelope.namespace.as_deref().unwrap_or("cluster"),
                        envelope.operation,
                        envelope.idempotency_key.as_deref().unwrap_or("none"),
                        envelope.body_bytes
                    ),
                ),
                WireDecision::Rejected { protocol, reason } => trace.push(
                    "wire",
                    "reject",
                    format!("protocol={} reason={}", protocol.as_str(), reason),
                ),
            }
        }
        trace.push(
            "wire",
            "summary",
            format!(
                "frames={} protocols={} rejections={} accepted_body_bytes={}",
                runtime.wire_frames,
                runtime.wire_protocols,
                runtime.wire_rejections,
                runtime.wire_body_bytes
            ),
        );
    }

    fn frames(principal: &str) -> Vec<RawFrame> {
        vec![
            RawFrame::new(
                WireProtocol::Sql,
                principal,
                Some("app".to_string()),
                "SELECT id,name FROM app.users WHERE active = true LIMIT 1",
                None,
                192,
            ),
            RawFrame::new(
                WireProtocol::Kv,
                principal,
                Some("app".to_string()),
                "PUT config/gateway",
                Some("kv-0001".to_string()),
                128,
            ),
            RawFrame::new(
                WireProtocol::Object,
                principal,
                Some("app".to_string()),
                "PUT /media/hero.png",
                Some("object-0001".to_string()),
                256,
            ),
            RawFrame::new(
                WireProtocol::Blob,
                principal,
                Some("app".to_string()),
                "PUT sha256:hero",
                Some("blob-0001".to_string()),
                512,
            ),
            RawFrame::new(
                WireProtocol::Admin,
                principal,
                None,
                "JOIN node=5 region=iad",
                Some("admin-0001".to_string()),
                64,
            ),
            RawFrame::new(
                WireProtocol::Object,
                principal,
                Some("app".to_string()),
                "PUT /media/missing-key.png",
                None,
                96,
            ),
        ]
    }
}

impl Api {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) {
        let gateway = ApiGateway;
        let token = runtime
            .app_token
            .as_ref()
            .expect("simulated API gateway must have an issued app token")
            .clone();
        let principal = token.principal.clone();
        let now = runtime.router.latest_version();

        let sql = gateway
            .evaluate(
                &runtime.auth,
                &token,
                now,
                ApiRequest::sql_query(
                    principal.clone(),
                    "app",
                    "SELECT id,name FROM app.users WHERE active = true LIMIT 1",
                ),
            )
            .expect("simulated SQL API request must evaluate");
        Self::record_decision(trace, runtime, sql);

        let admin = gateway
            .evaluate(
                &runtime.auth,
                &token,
                now,
                ApiRequest::admin_join(principal),
            )
            .expect("simulated admin API request must evaluate");
        Self::record_decision(trace, runtime, admin);
    }

    fn record_decision(trace: &mut Trace, runtime: &mut DatabaseRuntime, decision: ApiDecision) {
        match decision {
            ApiDecision::Accepted { surface, operation } => {
                runtime.api_accepted += 1;
                trace.push(
                    "api",
                    "accept",
                    format!("surface={surface:?} operation={operation}"),
                );
            }
            ApiDecision::Rejected {
                surface,
                operation,
                reason,
            } => {
                runtime.api_rejected += 1;
                trace.push(
                    "api",
                    "reject",
                    format!("surface={surface:?} operation={operation} reason={reason}"),
                );
            }
        }
    }
}

pub struct Idempotency;

impl Idempotency {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) {
        let mut ledger = IdempotencyLedger::default();
        let key = IdempotencyKey::new("app-client/sql/0001")
            .expect("simulated idempotency key must be valid");
        let envelope = RequestEnvelope {
            key: key.clone(),
            fingerprint: "sql:SELECT id,name FROM app.users WHERE id = 1".to_string(),
            now: runtime.router.latest_version(),
            ttl_versions: 12,
        };

        let first = ledger
            .claim(envelope.clone())
            .expect("simulated idempotency claim must evaluate");
        Self::record_decision(trace, runtime, "sql-read", &first);

        ledger
            .finish(
                &key,
                StoredResponse {
                    status: ResponseStatus::Committed,
                    commit: Some(runtime.router.latest_version()),
                    body_digest: "rows:1:users".to_string(),
                },
            )
            .expect("simulated idempotency response must store");
        trace.push(
            "idempotency",
            "finish",
            format!(
                "key={} status=committed commit={} digest=rows:1:users entries={}",
                key.0,
                runtime.router.latest_version().0,
                ledger.entry_count()
            ),
        );

        let replay = ledger
            .claim(envelope.clone())
            .expect("simulated idempotency replay must evaluate");
        Self::record_decision(trace, runtime, "sql-read-replay", &replay);

        let conflict = ledger
            .claim(RequestEnvelope {
                fingerprint: "sql:SELECT id FROM app.users WHERE id = 2".to_string(),
                ..envelope
            })
            .expect("simulated idempotency conflict must evaluate");
        Self::record_decision(trace, runtime, "sql-read-conflict", &conflict);

        let retry = RetryPolicy::default()
            .next_delay_ms(RetryClass::Retryable, 1)
            .expect("simulated retryable failure must schedule one retry");
        runtime.idempotency_retries += 1;
        trace.push(
            "idempotency",
            "retry",
            format!("class=retryable attempt=1 delay_ms={retry}"),
        );
    }

    fn record_decision(
        trace: &mut Trace,
        runtime: &mut DatabaseRuntime,
        label: &'static str,
        decision: &IdempotencyDecision,
    ) {
        match decision {
            IdempotencyDecision::FirstSeen { expires_at } => {
                runtime.idempotency_first_seen += 1;
                trace.push(
                    "idempotency",
                    "claim",
                    format!("{label} first-seen expires_at={}", expires_at.0),
                );
            }
            IdempotencyDecision::Replay { response } => {
                runtime.idempotency_replays += 1;
                trace.push(
                    "idempotency",
                    "replay",
                    format!(
                        "{label} status={} commit={} digest={}",
                        response_status(response.status),
                        response
                            .commit
                            .map(|version| version.0.to_string())
                            .unwrap_or_else(|| "none".to_string()),
                        response.body_digest
                    ),
                );
            }
            IdempotencyDecision::InFlight { expires_at } => {
                trace.push(
                    "idempotency",
                    "in-flight",
                    format!("{label} expires_at={}", expires_at.0),
                );
            }
            IdempotencyDecision::Conflict {
                original_fingerprint,
            } => {
                runtime.idempotency_conflicts += 1;
                trace.push(
                    "idempotency",
                    "conflict",
                    format!("{label} original_fingerprint={original_fingerprint}"),
                );
            }
            IdempotencyDecision::Expired { expired_at } => {
                trace.push(
                    "idempotency",
                    "expired",
                    format!("{label} expired_at={}", expired_at.0),
                );
            }
        }
    }
}

pub struct Gateway;

impl Gateway {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) {
        let api = ApiGateway;
        let gateway = DatabaseGateway;
        let token = runtime
            .app_token
            .as_ref()
            .expect("simulated database gateway must have an issued app token")
            .clone();
        let principal = token.principal.clone();
        let now = runtime.router.latest_version();
        let budget = RequestBudget {
            deadline: CommitVersion(now.0 + 8),
            max_rows: Some(256),
            max_bytes: Some(4 * 1024 * 1024),
        };
        let route_context = RouteContext::try_new(
            "ord",
            "iad",
            ["iad", "ord", "fra"],
            1,
            now,
            now,
            strict_global_policy(),
        )
        .expect("static simulated route context must be valid");
        let global_write_policy = strict_global_policy();

        let requests = vec![
            ApiRequest::sql_query(
                principal.clone(),
                "app",
                "SELECT id,name FROM app.users WHERE id = 1",
            ),
            ApiRequest::kv_write(principal.clone(), "app", "config/gateway"),
            ApiRequest::object_put(principal.clone(), "app", "media/avatar.png"),
            ApiRequest::blob_put(principal.clone(), "app", "blob-digest-avatar"),
            ApiRequest::admin_join(principal),
        ];

        for request in requests {
            let decision = api
                .evaluate(&runtime.auth, &token, now, request.clone())
                .expect("simulated gateway auth evaluation must succeed");
            let routed = gateway
                .route_region_aware(
                    &runtime.catalog,
                    now,
                    budget.clone(),
                    request,
                    decision,
                    route_context.clone(),
                )
                .expect("simulated gateway routing must evaluate");
            Self::record_decision(trace, runtime, routed);
        }
        let rejected_write = global_write_policy.evaluate_global_write(
            RegionId::new("ord").expect("static region id must be valid"),
            RegionId::new("ord").expect("static region id must be valid"),
            true,
        );
        trace.push(
            "policy",
            "strict-global-write-reject",
            global_write_decision(rejected_write),
        );
        for (label, class, mutable_write) in [
            ("read-replica-local", ReplicaReadClass::BoundedStale, false),
            ("read-replica-strict", ReplicaReadClass::Strict, false),
            ("read-replica-write", ReplicaReadClass::BoundedStale, true),
            (
                "read-replica-immutable",
                ReplicaReadClass::ImmutableVerified,
                false,
            ),
        ] {
            let decision = global_write_policy.evaluate_replica_read(
                RegionId::new("ord").expect("static region id must be valid"),
                RegionId::new("ord").expect("static region id must be valid"),
                class,
                mutable_write,
            );
            trace.push("policy", label, replica_read_decision(decision));
        }
        trace.push(
            "policy",
            "preflight",
            policy_preflight_report(global_write_policy.preflight()),
        );
        let payload_placement = global_write_policy.plan_immutable_payload_placement(
            ImmutablePayloadPlacementRequest {
                client_region: RegionId::new("ord").expect("static region id must be valid"),
                candidate_regions: vec![
                    RegionId::new("fra").expect("static region id must be valid"),
                    RegionId::new("ord").expect("static region id must be valid"),
                    RegionId::new("iad").expect("static region id must be valid"),
                ],
                min_replicas: 2,
                metadata_version: now.0,
                required_metadata_version: now.0,
            },
        );
        trace.push(
            "policy",
            "immutable-payload-ap",
            immutable_payload_placement_decision(payload_placement),
        );
        let stale_payload_placement = global_write_policy.plan_immutable_payload_placement(
            ImmutablePayloadPlacementRequest {
                client_region: RegionId::new("ord").expect("static region id must be valid"),
                candidate_regions: vec![
                    RegionId::new("ord").expect("static region id must be valid"),
                    RegionId::new("iad").expect("static region id must be valid"),
                ],
                min_replicas: 2,
                metadata_version: now.0.saturating_sub(1),
                required_metadata_version: now.0,
            },
        );
        trace.push(
            "policy",
            "immutable-payload-ap-reject",
            immutable_payload_placement_decision(stale_payload_placement),
        );
        runtime.gateway_surfaces = runtime.gateway_routes;

        let rejected = ConsistencyPolicy::validate(
            ConsistencySurface::Relational,
            ConsistencyClass::AvailabilityFirst,
        )
        .expect_err("relational work must reject availability-first downgrade");
        runtime.consistency_rejections += 1;
        trace.push(
            "consistency",
            "reject",
            format!(
                "surface=relational requested=availability-first reason={}",
                rejected.message
            ),
        );
    }

    fn record_decision(
        trace: &mut Trace,
        runtime: &mut DatabaseRuntime,
        decision: GatewayDecision,
    ) {
        match decision {
            GatewayDecision::Routed(route) => {
                runtime.gateway_routes += 1;
                let consistency = ConsistencyPolicy::validate(
                    consistency_surface(&route.surface),
                    route.consistency.class,
                )
                .expect("gateway route consistency label must match policy");
                match consistency.class {
                    ConsistencyClass::StrictQuorum => runtime.consistency_strict += 1,
                    ConsistencyClass::AvailabilityFirst => {
                        runtime.consistency_availability_first += 1;
                    }
                    ConsistencyClass::BoundedStaleRead | ConsistencyClass::ImmutableVerified => {}
                }
                trace.push(
                    "consistency",
                    consistency.class.as_str(),
                    format!(
                        "surface={} quorum_required={} async_allowed={} detail={}",
                        consistency.surface.as_str(),
                        consistency.quorum_required,
                        consistency.async_allowed,
                        consistency.detail
                    ),
                );
                trace.push(
                    "gateway",
                    "route",
                    format!(
                        "surface={:?} namespace={} handler={} consistency={} pg={} region={} region_kind={} region_reason={} deadline={} rows={} bytes={} operation={}",
                        route.surface,
                        route.namespace.as_deref().unwrap_or("cluster"),
                        route.handler.as_str(),
                        route.consistency.class.as_str(),
                        placement_label(route.placement_group),
                        route
                            .region
                            .as_ref()
                            .map(|region| region.target_region.as_str())
                            .unwrap_or("none"),
                        route
                            .region
                            .as_ref()
                            .map(|region| region.kind.as_str())
                            .unwrap_or("none"),
                        route
                            .region
                            .as_ref()
                            .map(|region| region.reason.as_str())
                            .unwrap_or("no region-aware context"),
                        route.budget.deadline.0,
                        optional_usize(route.budget.max_rows),
                        optional_usize(route.budget.max_bytes),
                        route.operation
                    ),
                );
                if let Some(region) = &route.region {
                    let policy = strict_global_policy();
                    let mutable_truth = route.consistency.class == ConsistencyClass::StrictQuorum;
                    trace.push(
                        "policy",
                        "strict-global-write",
                        global_write_decision(policy.evaluate_global_write(
                            region.client_region.clone(),
                            region.target_region.clone(),
                            mutable_truth,
                        )),
                    );
                }
            }
            GatewayDecision::Rejected {
                surface,
                operation,
                reason,
            } => {
                runtime.gateway_rejections += 1;
                trace.push(
                    "gateway",
                    "reject",
                    format!("surface={surface:?} operation={operation} reason={reason}"),
                );
            }
        }
    }
}

fn consistency_surface(surface: &ApiSurface) -> ConsistencySurface {
    match surface {
        ApiSurface::Sql => ConsistencySurface::Relational,
        ApiSurface::Kv => ConsistencySurface::Kv,
        ApiSurface::Object => ConsistencySurface::ObjectMetadata,
        ApiSurface::Blob => ConsistencySurface::BlobPayload,
        ApiSurface::Admin => ConsistencySurface::Admin,
    }
}

fn strict_global_policy() -> DesiredClusterPolicy {
    DesiredClusterPolicy {
        version: 1,
        min_replicas: 3,
        placement_groups: 3,
        strict_global_writes: true,
        regions: vec![
            RegionPolicy {
                region: RegionId::new("iad").expect("static region id must be valid"),
                role: RegionRole::Primary,
            },
            RegionPolicy {
                region: RegionId::new("ord").expect("static region id must be valid"),
                role: RegionRole::ReadReplica,
            },
        ],
    }
}

fn global_write_decision(decision: GlobalWriteDecision) -> String {
    match decision {
        GlobalWriteDecision::Allowed {
            client_region,
            target_region,
            primary_region,
            strict,
            reason,
        } => format!(
            "allowed client_region={} target_region={} primary_region={} strict={} reason={}",
            client_region,
            target_region,
            primary_region
                .as_ref()
                .map(RegionId::as_str)
                .unwrap_or("none"),
            strict,
            reason
        ),
        GlobalWriteDecision::Rejected {
            client_region,
            target_region,
            primary_region,
            reason,
        } => format!(
            "rejected client_region={} target_region={} primary_region={} reason={}",
            client_region,
            target_region,
            primary_region
                .as_ref()
                .map(RegionId::as_str)
                .unwrap_or("none"),
            reason
        ),
    }
}

fn replica_read_decision(decision: ReplicaReadDecision) -> String {
    match decision {
        ReplicaReadDecision::ServeLocal {
            client_region,
            target_region,
            class,
            reason,
        } => format!(
            "serve-local client_region={} target_region={} class={:?} reason={}",
            client_region, target_region, class, reason
        ),
        ReplicaReadDecision::RoutePrimary {
            client_region,
            target_region,
            primary_region,
            class,
            reason,
        } => format!(
            "route-primary client_region={} target_region={} primary_region={} class={:?} reason={}",
            client_region, target_region, primary_region, class, reason
        ),
        ReplicaReadDecision::Rejected {
            client_region,
            target_region,
            class,
            reason,
        } => format!(
            "rejected client_region={} target_region={} class={:?} reason={}",
            client_region, target_region, class, reason
        ),
    }
}

fn immutable_payload_placement_decision(decision: ImmutablePayloadPlacementDecision) -> String {
    match decision {
        ImmutablePayloadPlacementDecision::Place {
            client_region,
            placements,
            metadata_version,
            reason,
        } => format!(
            "place client_region={} placements={:?} metadata_version={} reason={}",
            client_region,
            placements.iter().map(RegionId::as_str).collect::<Vec<_>>(),
            metadata_version,
            reason
        ),
        ImmutablePayloadPlacementDecision::Rejected {
            client_region,
            reason,
        } => format!("rejected client_region={} reason={}", client_region, reason),
    }
}

fn policy_preflight_report(report: PolicyPreflightReport) -> String {
    let checks = report
        .checks
        .iter()
        .map(|check| {
            format!(
                "{}={}",
                check.name,
                if check.passed { "pass" } else { "fail" }
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "version={} mode={} passed={} checks={} implications={:?}",
        report.policy_version,
        report.write_mode,
        report.passed(),
        checks,
        report.implications
    )
}

pub struct Quota;

impl Quota {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) {
        let mut quota = QuotaEngine::default();
        for policy in [
            Self::policy(ApiSurface::Sql, 4, Some(1_000), Some(1_000_000)),
            Self::policy(ApiSurface::Kv, 8, None, Some(128_000)),
            Self::policy(ApiSurface::Object, 4, None, Some(8_000_000)),
            Self::policy(ApiSurface::Blob, 2, None, Some(4_000_000)),
        ] {
            quota
                .set_policy(policy)
                .expect("simulated quota policy must validate");
        }
        runtime.quota_policies = quota.policy_count();
        runtime.quota_namespaces = quota.namespace_count();
        trace.push(
            "quota",
            "policy",
            format!(
                "loaded {} surface policies for {} namespace",
                runtime.quota_policies, runtime.quota_namespaces
            ),
        );

        for (surface, rows, bytes, label) in [
            (ApiSurface::Sql, 1, 4_096, "sql point lookup"),
            (ApiSurface::Kv, 0, 128, "kv config write"),
            (ApiSurface::Object, 0, 2_000_000, "object media put"),
            (ApiSurface::Blob, 0, 1_000_000, "blob chunk put"),
            (ApiSurface::Blob, 0, 4_500_000, "oversized blob chunk"),
        ] {
            let decision = quota
                .reserve(QuotaKey::new("app", surface), rows, bytes)
                .expect("simulated quota reservation must evaluate");
            Self::record_decision(trace, runtime, label, decision);
        }
    }

    fn policy(
        surface: ApiSurface,
        max_requests: u64,
        max_rows: Option<u64>,
        max_bytes: Option<u64>,
    ) -> QuotaPolicy {
        QuotaPolicy {
            key: QuotaKey::new("app", surface),
            max_requests,
            max_rows,
            max_bytes,
        }
    }

    fn record_decision(
        trace: &mut Trace,
        runtime: &mut DatabaseRuntime,
        label: &'static str,
        decision: QuotaDecision,
    ) {
        match decision {
            QuotaDecision::Allowed(reservation) => {
                runtime.quota_allowed += 1;
                trace.push(
                    "quota",
                    "allow",
                    format!(
                        "{} surface={:?} requests={} rows={} bytes={} remaining_requests={} remaining_rows={} remaining_bytes={}",
                        label,
                        reservation.key.surface,
                        reservation.usage.requests,
                        reservation.usage.rows,
                        reservation.usage.bytes,
                        reservation.remaining_requests,
                        optional_u64(reservation.remaining_rows),
                        optional_u64(reservation.remaining_bytes)
                    ),
                );
            }
            QuotaDecision::Rejected { key, reason } => {
                runtime.quota_rejected += 1;
                trace.push(
                    "quota",
                    "reject",
                    format!("{label} surface={:?} reason={reason}", key.surface),
                );
            }
        }
    }
}

pub struct Rpc;

impl Rpc {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) {
        let router = RpcRouter::new(RpcPolicy::strict_database());
        let decisions = [
            router.route(RpcEnvelope::new(
                Self::trace_id("trace-sql-1"),
                ServiceRole::Gateway,
                ServiceRole::Sql,
                "sql.execute",
                120,
                RpcRetryClass::Never,
                384,
            )),
            router.route(RpcEnvelope::new(
                Self::trace_id("trace-object-1"),
                ServiceRole::Gateway,
                ServiceRole::Object,
                "object.commit-manifest",
                200,
                RpcRetryClass::Idempotent,
                256,
            )),
            router.route(RpcEnvelope::new(
                Self::trace_id("trace-blob-1"),
                ServiceRole::Object,
                ServiceRole::Blob,
                "blob.pin",
                180,
                RpcRetryClass::Idempotent,
                128,
            )),
            router.route(
                RpcEnvelope::new(
                    Self::trace_id("trace-repair-1"),
                    ServiceRole::Background,
                    ServiceRole::Storage,
                    "repair.copy-shard",
                    500,
                    RpcRetryClass::Idempotent,
                    512,
                )
                .attempt(2),
            ),
            router.route(RpcEnvelope::new(
                Self::trace_id("trace-too-slow"),
                ServiceRole::Gateway,
                ServiceRole::Storage,
                "storage.append",
                9_000,
                RpcRetryClass::Idempotent,
                64,
            )),
            router.route(
                RpcEnvelope::new(
                    Self::trace_id("trace-nonretry"),
                    ServiceRole::Gateway,
                    ServiceRole::Blob,
                    "blob.stream-read",
                    250,
                    RpcRetryClass::Never,
                    64,
                )
                .attempt(2),
            ),
        ];

        runtime.rpc_calls = decisions.len();
        runtime.rpc_body_bytes = accepted_rpc_body_bytes(&decisions) as usize;
        for decision in decisions {
            Self::record_decision(trace, runtime, decision);
        }
        trace.push(
            "rpc",
            "summary",
            format!(
                "calls={} retries={} rejections={} body_bytes={}",
                runtime.rpc_calls,
                runtime.rpc_retries,
                runtime.rpc_rejections,
                runtime.rpc_body_bytes
            ),
        );
    }

    fn record_decision(trace: &mut Trace, runtime: &mut DatabaseRuntime, decision: RpcDecision) {
        match decision {
            RpcDecision::Accepted(envelope) => {
                trace.push("rpc", "accept", rpc_envelope(&envelope));
            }
            RpcDecision::Retryable(envelope) => {
                runtime.rpc_retries += 1;
                trace.push("rpc", "retry", rpc_envelope(&envelope));
            }
            RpcDecision::Rejected { trace_id, reason } => {
                runtime.rpc_rejections += 1;
                trace.push(
                    "rpc",
                    "reject",
                    format!("trace_id={trace_id} reason={reason}"),
                );
            }
        }
    }

    fn trace_id(value: &str) -> TraceId {
        TraceId::new(value).expect("simulated trace id must be valid")
    }
}

impl Fabric {
    pub fn append_seed_records(trace: &mut Trace, log: &mut rsf_storage::SegmentLog) -> Recovery {
        let (_, relational) = log
            .append_wal(WalAppend {
                pg: PlacementGroupId(1),
                epoch: Epoch(1),
                index: LogIndex(1),
                kind: WalRecordKind::RelationalMutation,
                previous_hash: [0; 32],
                payload: b"sql:insert users id=1 name=ada",
            })
            .expect("simulated relational WAL append must succeed");
        trace.push(
            "fabric",
            "append-wal",
            "pg=1 kind=relational_mutation index=1",
        );

        let (_, object) = log
            .append_wal(WalAppend {
                pg: PlacementGroupId(2),
                epoch: Epoch(1),
                index: LogIndex(1),
                kind: WalRecordKind::ObjectManifest,
                previous_hash: [0; 32],
                payload: b"object:bucket=media key=hero.png chunks=2",
            })
            .expect("simulated object WAL append must succeed");
        trace.push("fabric", "append-wal", "pg=2 kind=object_manifest index=1");

        let (_, blob) = log
            .append_wal(WalAppend {
                pg: PlacementGroupId(3),
                epoch: Epoch(1),
                index: LogIndex(1),
                kind: WalRecordKind::BlobManifest,
                previous_hash: [0; 32],
                payload: b"blob:cid=sha256-demo len=4096",
            })
            .expect("simulated blob WAL append must succeed");
        trace.push("fabric", "append-wal", "pg=3 kind=blob_manifest index=1");

        log.append_wal(WalAppend {
            pg: PlacementGroupId(1),
            epoch: Epoch(1),
            index: LogIndex(2),
            kind: WalRecordKind::Commit,
            previous_hash: relational.record_hash,
            payload: b"commit:sql-tx-1",
        })
        .expect("simulated relational commit append must succeed");
        trace.push("fabric", "append-wal", "pg=1 kind=commit index=2");

        log.append_wal(WalAppend {
            pg: PlacementGroupId(2),
            epoch: Epoch(1),
            index: LogIndex(2),
            kind: WalRecordKind::Commit,
            previous_hash: object.record_hash,
            payload: b"commit:object-v1",
        })
        .expect("simulated object commit append must succeed");
        trace.push("fabric", "append-wal", "pg=2 kind=commit index=2");

        log.append_wal(WalAppend {
            pg: PlacementGroupId(3),
            epoch: Epoch(1),
            index: LogIndex(2),
            kind: WalRecordKind::Commit,
            previous_hash: blob.record_hash,
            payload: b"commit:blob-pin",
        })
        .expect("simulated blob commit append must succeed");
        trace.push("fabric", "append-wal", "pg=3 kind=commit index=2");

        let recovery = log
            .recover_current()
            .expect("simulated recovery must succeed");
        trace.push(
            "fabric",
            "recover",
            format!("{} WAL records recovered", recovery.wal_records.len()),
        );
        recovery
    }
}

pub struct Consensus;

impl Consensus {
    pub fn run(trace: &mut Trace, implementation: &Implementation, recovery: &Recovery) {
        trace.push("consensus", "mode", implementation.topology.consensus);
        for summary in recovery.pg_tail_summaries() {
            trace.push(
                "consensus",
                "tail-summary",
                format!(
                    "pg={} epoch={} index={} hash={}",
                    summary.pg.0,
                    summary.tail.epoch.0,
                    summary.tail.index.0,
                    short_hash(summary.tail.record_hash)
                ),
            );
        }
    }
}

pub struct Transaction;

impl Transaction {
    pub fn run(trace: &mut Trace, recovery: &Recovery) {
        let committed = recovery
            .pg_tail_summaries()
            .iter()
            .filter(|summary| {
                recovery
                    .pg_log(summary.pg)
                    .committed_tail
                    .is_some_and(|tail| tail.index == summary.tail.index)
            })
            .count();
        trace.push(
            "transaction",
            "commit-visible",
            format!("{committed} placement groups have committed durable tails"),
        );
    }
}

pub struct TransactionCoordinatorService;

impl TransactionCoordinatorService {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) {
        let read = runtime.transactions.begin(&runtime.router);
        trace.push("txn", "begin", format!("read_version={}", read.version.0));
        let prepared = runtime
            .transactions
            .prepare(read, Self::intent(1, b"app/users/1", b"app/users/2"))
            .expect("simulated transaction prepare must succeed");
        trace.push(
            "txn",
            "prepare",
            "txn=01010101010101010101010101010101 read_conflicts=1 write_conflicts=1 mutations=1",
        );
        let result = runtime
            .transactions
            .commit(&mut runtime.router, PlacementGroupId(1), prepared)
            .expect("simulated transaction commit must succeed");
        trace.push(
            "txn",
            "commit",
            format!(
                "txn=01010101010101010101010101010101 version={}",
                result.version.0
            ),
        );

        let stale = ReadVersion {
            version: CommitVersion(result.version.0 - 1),
        };
        let conflict = runtime
            .transactions
            .prepare(stale, Self::intent(2, b"app/users/1", b"app/users/2"))
            .expect_err("simulated stale overlapping transaction must conflict");
        trace.push(
            "txn",
            "conflict",
            format!(
                "stale overlapping transaction rejected: {}",
                conflict.message
            ),
        );
    }

    fn intent(id: u8, start: &[u8], end: &[u8]) -> CommitIntent {
        CommitIntent {
            id: TransactionId([id; 16]),
            read_conflicts: vec![KeyRange {
                start: start.to_vec(),
                end: end.to_vec(),
            }],
            write_conflicts: vec![KeyRange {
                start: start.to_vec(),
                end: end.to_vec(),
            }],
            mutations: vec![Mutation::Set {
                key: start.to_vec(),
                value: b"prepared".to_vec(),
            }],
        }
    }
}

pub struct OrderedKv;

pub struct Isolation;

impl Isolation {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) {
        let mut tracker = IsolationTracker::default();
        let base = runtime.router.latest_version();
        trace.push(
            "isolation",
            "strict-layer",
            format!("serializable validation begins at read_version={}", base.0),
        );

        let writer = Self::txn(
            11,
            base,
            &[Self::range(b"app/users/1", b"app/users/2")],
            &[Self::range(b"app/users/1", b"app/users/2")],
        );
        Self::record_decision(
            trace,
            runtime,
            tracker
                .commit(writer, CommitVersion(base.0 + 1))
                .expect("simulated isolation writer must evaluate"),
        );

        let disjoint = Self::txn(
            12,
            base,
            &[Self::range(b"app/config/a", b"app/config/b")],
            &[Self::range(b"app/config/b", b"app/config/c")],
        );
        Self::record_decision(
            trace,
            runtime,
            tracker
                .commit(disjoint, CommitVersion(base.0 + 2))
                .expect("simulated disjoint isolation transaction must evaluate"),
        );

        let stale_read = Self::txn(
            13,
            base,
            &[Self::range(b"app/users/1", b"app/users/2")],
            &[],
        );
        Self::record_decision(
            trace,
            runtime,
            tracker
                .commit(stale_read, CommitVersion(base.0 + 3))
                .expect("simulated stale isolation read must evaluate"),
        );

        runtime.isolation_edges = tracker.dependency_count();
        trace.push(
            "isolation",
            "summary",
            format!(
                "committed={} rejected={} dependency_edges={} checked_ranges={}",
                runtime.isolation_commits,
                runtime.isolation_rejections,
                runtime.isolation_edges,
                runtime.isolation_ranges
            ),
        );
    }

    fn record_decision(
        trace: &mut Trace,
        runtime: &mut DatabaseRuntime,
        decision: IsolationDecision,
    ) {
        match decision {
            IsolationDecision::Accepted(commit) => {
                runtime.isolation_commits += 1;
                runtime.isolation_ranges += commit.read_ranges.len() + commit.write_ranges.len();
                trace.push(
                    "isolation",
                    "commit",
                    format!(
                        "txn={} read_version={} commit_version={} reads={} writes={} edges={}",
                        txn_label(&commit.id),
                        commit.read_version.0,
                        commit.commit_version.0,
                        commit.read_ranges.len(),
                        commit.write_ranges.len(),
                        commit.dependencies.len()
                    ),
                );
            }
            IsolationDecision::Rejected {
                transaction,
                reason,
                conflicting_version,
            } => {
                runtime.isolation_rejections += 1;
                trace.push(
                    "isolation",
                    "reject",
                    format!(
                        "txn={} conflict_version={} reason={reason}",
                        txn_label(&transaction),
                        conflicting_version
                            .map_or_else(|| "none".to_string(), |version| version.0.to_string())
                    ),
                );
            }
        }
    }

    fn txn(
        id: u8,
        read_version: CommitVersion,
        read_ranges: &[KeyRange],
        write_ranges: &[KeyRange],
    ) -> IsolationTransaction {
        IsolationTransaction {
            id: TransactionId([id; 16]),
            read_version,
            read_ranges: read_ranges.to_vec(),
            write_ranges: write_ranges.to_vec(),
        }
    }

    fn range(start: &[u8], end: &[u8]) -> KeyRange {
        KeyRange {
            start: start.to_vec(),
            end: end.to_vec(),
        }
    }
}

impl OrderedKv {
    pub fn run(trace: &mut Trace, implementation: &Implementation, runtime: &mut DatabaseRuntime) {
        trace.push("kv", "surface", implementation.database.kv);
        let before = runtime.kv.snapshot(&runtime.router);
        let batch_version = runtime
            .kv
            .commit_batch(
                &mut runtime.router,
                PlacementGroupId(1),
                KvBatch {
                    client_request: None,
                    snapshot: before,
                    mutations: vec![
                        KvMutation::Set {
                            key: b"config/a".to_vec(),
                            value: b"one".to_vec(),
                        },
                        KvMutation::Set {
                            key: b"config/b".to_vec(),
                            value: b"two".to_vec(),
                        },
                    ],
                },
            )
            .expect("simulated KV batch must commit through protocol router");
        trace.push(
            "kv",
            "batch",
            format!("committed 2 ordered keys at version {}", batch_version.0),
        );

        let snapshot = runtime.kv.snapshot(&runtime.router);
        let snapshot_rows = runtime.kv.scan_prefix_at(b"config/", snapshot);
        trace.push(
            "kv",
            "snapshot-scan",
            format!(
                "snapshot version {} returned {} config keys",
                snapshot.version.0,
                snapshot_rows.len()
            ),
        );

        let update_version = runtime
            .kv
            .set(
                &mut runtime.router,
                PlacementGroupId(1),
                b"config/a".to_vec(),
                b"three".to_vec(),
            )
            .expect("simulated KV update must commit through protocol router");
        trace.push(
            "kv",
            "set",
            format!("updated config/a at version {}", update_version.0),
        );

        let clear_snapshot = runtime.kv.snapshot(&runtime.router);
        let clear_version = runtime
            .kv
            .commit_batch(
                &mut runtime.router,
                PlacementGroupId(1),
                KvBatch {
                    client_request: None,
                    snapshot: clear_snapshot,
                    mutations: vec![KvMutation::Clear {
                        key: b"config/b".to_vec(),
                    }],
                },
            )
            .expect("simulated KV clear must commit through protocol router");
        trace.push(
            "kv",
            "clear",
            format!(
                "cleared config/b with tombstone at version {}",
                clear_version.0
            ),
        );

        let changes = runtime.kv.changes_since(snapshot.version);
        trace.push(
            "kv",
            "change-stream",
            format!(
                "{} changes observed after version {}",
                changes.len(),
                snapshot.version.0
            ),
        );
        trace.push(
            "kv",
            "latest-scan",
            format!(
                "latest prefix scan sees {} live key",
                runtime.kv.scan_prefix(b"config/").len()
            ),
        );
        let request = ClientRequestId::new("kv-client", 42);
        let first = runtime
            .router
            .commit_request(
                PlacementGroupId(1),
                request.clone(),
                ConsistencyMode::Strict,
                FabricOperation::KvMutation {
                    key: b"config/idempotent".to_vec(),
                },
            )
            .expect("simulated idempotent request must commit");
        let replay = runtime
            .router
            .commit_request(
                PlacementGroupId(1),
                request,
                ConsistencyMode::Strict,
                FabricOperation::KvMutation {
                    key: b"config/idempotent".to_vec(),
                },
            )
            .expect("simulated idempotent replay must return cached commit");
        trace.push(
            "kv",
            "idempotent-replay",
            format!(
                "request replay={} reused version {} index {}",
                replay.replayed, first.commit.version.0, replay.commit.index.0
            ),
        );
    }
}

pub struct SessionTransactionService;

impl SessionTransactionService {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) {
        let began = runtime
            .session_transactions
            .begin("sql-session-ryw", &runtime.kv, &runtime.router)
            .expect("simulated session transaction must begin");
        trace.push("sessiontxn", "begin", session_txn_decision(began));

        let staged = runtime
            .session_transactions
            .stage_set(
                "sql-session-ryw",
                b"session/cart/1".to_vec(),
                b"pending-total=42".to_vec(),
            )
            .expect("simulated session transaction must stage write");
        trace.push("sessiontxn", "stage", session_txn_decision(staged));

        let overlay = runtime
            .session_transactions
            .read("sql-session-ryw", &runtime.kv, b"session/cart/1".to_vec())
            .expect("simulated session transaction must read its own pending write");
        trace.push("sessiontxn", "read-overlay", session_txn_decision(overlay));

        let snapshot = runtime
            .session_transactions
            .read("sql-session-ryw", &runtime.kv, b"config/a".to_vec())
            .expect("simulated session transaction must read begin snapshot");
        trace.push(
            "sessiontxn",
            "read-snapshot",
            session_txn_decision(snapshot),
        );

        let committed = runtime
            .session_transactions
            .commit(
                "sql-session-ryw",
                &mut runtime.kv,
                &mut runtime.router,
                PlacementGroupId(1),
            )
            .expect("simulated session transaction must commit through KV/protocol");
        trace.push("sessiontxn", "commit", session_txn_decision(committed));

        let rejected = runtime
            .session_transactions
            .read("sql-session-ryw", &runtime.kv, b"session/cart/1".to_vec())
            .expect_err("closed session transaction should reject further overlay reads");
        trace.push(
            "sessiontxn",
            "reject",
            format!("closed transaction rejected: {}", rejected.message),
        );
    }
}

pub struct Watch;

impl Watch {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) {
        let start = runtime
            .kv
            .changes_since(CommitVersion(0))
            .get(1)
            .map(|change| change.version)
            .expect("simulated KV watch requires the initial batch boundary");
        let subscription = runtime
            .watches
            .register("app-client", b"config/".to_vec(), WatchCursor::new(start))
            .expect("simulated watch registration must succeed");
        runtime.watch_registered = runtime.watches.len();
        trace.push(
            "watch",
            "register",
            format!(
                "client={} prefix={} from_version={} watch={}",
                subscription.client,
                bytes_label(&subscription.prefix),
                subscription.cursor.after.0,
                subscription.id.0
            ),
        );

        let changes = runtime.kv.changes_since(subscription.cursor.after);
        let deliveries = runtime
            .watches
            .deliver(subscription.id, &changes)
            .expect("simulated watch delivery must scan KV changes");
        for delivery in &deliveries {
            trace.push(
                "watch",
                "deliver",
                format!(
                    "watch={} version={} key={} op={}",
                    delivery.watch.0,
                    delivery.version.0,
                    bytes_label(&delivery.key),
                    if delivery.deleted() { "clear" } else { "set" }
                ),
            );
        }
        runtime.watch_deliveries = deliveries.len();

        let cursor = runtime
            .watches
            .ack(subscription.id, &deliveries)
            .expect("simulated watch ack must advance cursor");
        trace.push(
            "watch",
            "ack",
            format!("watch={} cursor={}", subscription.id.0, cursor.after.0),
        );

        let resumed = runtime
            .watches
            .resume(subscription.id)
            .expect("simulated watch resume must return durable cursor");
        let pending = runtime
            .watches
            .deliver(resumed.id, &runtime.kv.changes_since(resumed.cursor.after))
            .expect("simulated resumed watch delivery must succeed");
        runtime.watch_resumes = 1;
        trace.push(
            "watch",
            "resume",
            format!(
                "watch={} cursor={} pending={}",
                resumed.id.0,
                resumed.cursor.after.0,
                pending.len()
            ),
        );
    }
}

pub struct Session;

impl Session {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) {
        let binder = SessionBinder;
        let principal = PrincipalId("app-client".to_string());
        let context = SessionContext::new("session-app-1", principal.clone(), "app");
        let token = runtime
            .app_token
            .clone()
            .expect("simulated session requires app token");

        for statement in [
            "SELECT id, name FROM users WHERE id = 1",
            "INSERT INTO users(id, name) VALUES (1, 'ada')",
            "CREATE INDEX users_name ON users(name)",
        ] {
            let bound = binder
                .bind(
                    &runtime.catalog,
                    &context,
                    statement,
                    runtime.router.latest_version(),
                    PlacementGroupId(1),
                )
                .expect("simulated SQL statement must bind through session");
            runtime.session_bound += 1;
            trace.push(
                "session",
                "bind",
                format!(
                    "{} table={} tx={} pg={} consistency=strict capability={}",
                    bound.class.as_str(),
                    bound
                        .table
                        .as_ref()
                        .map(|table| table.name.0.as_str())
                        .unwrap_or("none"),
                    bound.transaction.as_str(),
                    bound.transaction.placement_group().0,
                    capability_label(&bound.capability)
                ),
            );

            let decision = binder
                .authorize(
                    &runtime.auth,
                    &token,
                    runtime.router.latest_version(),
                    &bound,
                    principal.clone(),
                )
                .expect("simulated SQL session authorization must evaluate");
            match decision {
                Authorization::Allowed => {
                    runtime.session_authorized += 1;
                    trace.push(
                        "session",
                        "authorize",
                        format!("{} authorized for {}", context.session_id, bound.statement),
                    );
                }
                Authorization::Denied { reason } => {
                    runtime.session_rejected += 1;
                    trace.push(
                        "session",
                        "deny",
                        format!(
                            "{} denied for {}: {}",
                            context.session_id, statement, reason
                        ),
                    );
                }
            }
        }

        let rejected = binder
            .bind(
                &runtime.catalog,
                &context,
                "SELECT * FROM missing WHERE id = 1",
                runtime.router.latest_version(),
                PlacementGroupId(1),
            )
            .expect_err("simulated session must reject missing table before execution");
        runtime.session_rejected += 1;
        trace.push(
            "session",
            "reject",
            format!(
                "missing table rejected before execution: {}",
                rejected.message
            ),
        );
    }
}

pub struct Relational;

pub struct PreparedSqlService;

impl PreparedSqlService {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) {
        let schema_version = runtime
            .catalog
            .table_version("app", "users")
            .expect("simulated prepared service requires table version");
        let mut cache = PreparedCache::new(2).expect("simulated prepared cache must construct");

        for decision in cache
            .prepare(
                "select_user",
                "SELECT id, name FROM users WHERE id = $1 AND active = $2",
                schema_version,
                vec![PreparedParamType::Int64, PreparedParamType::Bool],
            )
            .expect("simulated prepare must evaluate")
        {
            Self::record(trace, runtime, decision);
        }
        Self::record(
            trace,
            runtime,
            cache
                .bind(
                    "select_user",
                    schema_version,
                    vec![PreparedValue::Int64(1), PreparedValue::Bool(true)],
                )
                .expect("simulated bind must evaluate"),
        );
        Self::record(
            trace,
            runtime,
            cache
                .bind(
                    "select_user",
                    schema_version,
                    vec![
                        PreparedValue::Text("1".to_string()),
                        PreparedValue::Bool(true),
                    ],
                )
                .expect("simulated typed rejection must evaluate"),
        );

        for decision in cache
            .prepare(
                "stale_user",
                "SELECT id FROM users WHERE id = $1",
                schema_version,
                vec![PreparedParamType::Int64],
            )
            .expect("simulated stale prepare must evaluate")
        {
            Self::record(trace, runtime, decision);
        }
        Self::record(
            trace,
            runtime,
            cache
                .bind(
                    "stale_user",
                    CommitVersion(schema_version.0 + 1),
                    vec![PreparedValue::Int64(1)],
                )
                .expect("simulated invalidation must evaluate"),
        );

        for decision in cache
            .prepare(
                "throwaway",
                "SELECT name FROM users WHERE id = $1",
                schema_version,
                vec![PreparedParamType::Int64],
            )
            .expect("simulated deallocate prepare must evaluate")
        {
            Self::record(trace, runtime, decision);
        }
        Self::record(
            trace,
            runtime,
            cache
                .deallocate("throwaway")
                .expect("simulated deallocate must evaluate"),
        );

        let mut tiny = PreparedCache::new(1).expect("simulated tiny cache must construct");
        for decision in tiny
            .prepare(
                "first",
                "SELECT id FROM users WHERE id = $1",
                schema_version,
                vec![PreparedParamType::Int64],
            )
            .expect("simulated first prepare must evaluate")
        {
            Self::record(trace, runtime, decision);
        }
        for decision in tiny
            .prepare(
                "second",
                "SELECT name FROM users WHERE id = $1",
                schema_version,
                vec![PreparedParamType::Int64],
            )
            .expect("simulated eviction prepare must evaluate")
        {
            Self::record(trace, runtime, decision);
        }

        trace.push(
            "prepared",
            "summary",
            format!(
                "statements={} binds={} invalidations={} deallocations={} evictions={} rejections={}",
                runtime.prepared_statements,
                runtime.prepared_binds,
                runtime.prepared_invalidations,
                runtime.prepared_deallocations,
                runtime.prepared_evictions,
                runtime.prepared_rejections
            ),
        );
    }

    fn record(trace: &mut Trace, runtime: &mut DatabaseRuntime, decision: PreparedDecision) {
        match decision {
            PreparedDecision::Prepared {
                name,
                parameters,
                schema_version,
            } => {
                runtime.prepared_statements += 1;
                trace.push(
                    "prepared",
                    "prepare",
                    format!(
                        "name={name} params={parameters} schema_version={}",
                        schema_version.0
                    ),
                );
            }
            PreparedDecision::Bound(bound) => {
                runtime.prepared_binds += 1;
                trace.push(
                    "prepared",
                    "bind",
                    format!(
                        "name={} use_count={} params={} schema_version={} sql={}",
                        bound.name,
                        bound.use_count,
                        bound.parameters.len(),
                        bound.schema_version.0,
                        bound.sql
                    ),
                );
            }
            PreparedDecision::Invalidated {
                name,
                prepared_version,
                current_version,
            } => {
                runtime.prepared_invalidations += 1;
                trace.push(
                    "prepared",
                    "invalidate",
                    format!(
                        "name={name} prepared_version={} current_version={}",
                        prepared_version.0, current_version.0
                    ),
                );
            }
            PreparedDecision::Deallocated { name } => {
                runtime.prepared_deallocations += 1;
                trace.push("prepared", "deallocate", format!("name={name}"));
            }
            PreparedDecision::Evicted { name } => {
                runtime.prepared_evictions += 1;
                trace.push("prepared", "evict", format!("name={name}"));
            }
            PreparedDecision::Rejected { name, reason } => {
                runtime.prepared_rejections += 1;
                trace.push("prepared", "reject", format!("name={name} reason={reason}"));
            }
        }
    }
}

pub struct SqlPortalService;

impl SqlPortalService {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) {
        runtime
            .sql
            .execute(
                &mut runtime.router,
                PlacementGroupId(1),
                "CREATE TABLE portal_users(id PRIMARY KEY, name)",
            )
            .expect("simulated portal table must be created through SQL engine");
        runtime
            .sql
            .execute(
                &mut runtime.router,
                PlacementGroupId(1),
                "INSERT INTO portal_users(id, name) VALUES (1, 'ada')",
            )
            .expect("simulated portal row must be inserted through SQL engine");

        for decision in runtime.sql.prepare_statement(
            "select_user_portal",
            "SELECT id, name FROM portal_users WHERE id = $1",
            vec![PreparedParamType::Int64],
        ) {
            Self::record(trace, runtime, decision);
        }
        let decision = runtime.sql.bind_portal(
            "portal_read_user",
            "select_user_portal",
            vec![PreparedValue::Int64(1)],
        );
        Self::record(trace, runtime, decision);
        let decision = runtime.sql.describe_portal("portal_read_user");
        Self::record(trace, runtime, decision);
        let decision = runtime
            .sql
            .execute_portal(
                &mut runtime.router,
                PlacementGroupId(1),
                "portal_read_user",
                50,
            )
            .expect("simulated portal SELECT must execute through SQL engine")
            .decision;
        Self::record(trace, runtime, decision);

        for decision in runtime.sql.prepare_statement(
            "slow_user_portal",
            "SELECT id FROM portal_users WHERE id = $1",
            vec![PreparedParamType::Int64],
        ) {
            Self::record(trace, runtime, decision);
        }
        let decision = runtime.sql.bind_portal(
            "portal_slow_user",
            "slow_user_portal",
            vec![PreparedValue::Int64(2)],
        );
        Self::record(trace, runtime, decision);
        let decision = runtime
            .sql
            .execute_portal(
                &mut runtime.router,
                PlacementGroupId(1),
                "portal_slow_user",
                1,
            )
            .expect("simulated slow portal SELECT must reach deadline accounting")
            .decision;
        Self::record(trace, runtime, decision);
        let decision = runtime
            .sql
            .cancel_portal("portal_slow_user", "client cancellation request");
        Self::record(trace, runtime, decision);
        let decision = runtime
            .sql
            .execute_portal(
                &mut runtime.router,
                PlacementGroupId(1),
                "portal_slow_user",
                50,
            )
            .map(|execution| execution.decision)
            .unwrap_or_else(|error| PortalDecision::Rejected {
                target: "portal_slow_user".to_string(),
                reason: error.message,
            });
        Self::record(trace, runtime, decision);
        let decision = runtime.sql.close_portal("portal_read_user");
        Self::record(trace, runtime, decision);
        let decision = runtime.sql.close_statement("slow_user_portal");
        Self::record(trace, runtime, decision);
        let decision = runtime.sql.close_portal("missing_portal");
        Self::record(trace, runtime, decision);
        runtime
            .sql
            .execute(
                &mut runtime.router,
                PlacementGroupId(1),
                "DELETE FROM portal_users WHERE id = 1",
            )
            .expect("simulated portal cleanup must execute through SQL engine");

        trace.push(
            "portal",
            "summary",
            format!(
                "parses={} binds={} describes={} executes={} rows={} cancellations={} portal_closes={} statement_closes={} rejections={}",
                runtime.portal_parses,
                runtime.portal_binds,
                runtime.portal_describes,
                runtime.portal_executes,
                runtime.portal_rows,
                runtime.portal_cancellations,
                runtime.portal_closes,
                runtime.portal_statement_closes,
                runtime.portal_rejections
            ),
        );
    }

    fn record(trace: &mut Trace, runtime: &mut DatabaseRuntime, decision: PortalDecision) {
        match decision {
            PortalDecision::Parsed {
                statement,
                parameters,
                schema_version,
            } => {
                runtime.portal_parses += 1;
                trace.push(
                    "portal",
                    "parse",
                    format!(
                        "statement={statement} params={parameters} schema_version={}",
                        schema_version.0
                    ),
                );
            }
            PortalDecision::Bound {
                portal,
                statement,
                parameters,
            } => {
                runtime.portal_binds += 1;
                trace.push(
                    "portal",
                    "bind",
                    format!("portal={portal} statement={statement} params={parameters}"),
                );
            }
            PortalDecision::Described {
                portal,
                columns,
                parameters,
            } => {
                runtime.portal_describes += 1;
                trace.push(
                    "portal",
                    "describe",
                    format!("portal={portal} columns={columns} params={parameters}"),
                );
            }
            PortalDecision::Executed {
                portal,
                rows,
                elapsed_ms,
            } => {
                runtime.portal_executes += 1;
                runtime.portal_rows += rows;
                trace.push(
                    "portal",
                    "execute",
                    format!("portal={portal} rows={rows} elapsed_ms={elapsed_ms}"),
                );
            }
            PortalDecision::Cancelled { portal, reason } => {
                runtime.portal_cancellations += 1;
                trace.push(
                    "portal",
                    "cancel",
                    format!("portal={portal} reason={reason}"),
                );
            }
            PortalDecision::ClosedPortal { portal } => {
                runtime.portal_closes += 1;
                trace.push("portal", "close-portal", format!("portal={portal}"));
            }
            PortalDecision::ClosedStatement { statement } => {
                runtime.portal_statement_closes += 1;
                trace.push(
                    "portal",
                    "close-statement",
                    format!("statement={statement}"),
                );
            }
            PortalDecision::Rejected { target, reason } => {
                runtime.portal_rejections += 1;
                trace.push(
                    "portal",
                    "reject",
                    format!("target={target} reason={reason}"),
                );
            }
        }
    }
}

pub struct PrimaryIndexService;

impl PrimaryIndexService {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) {
        let table = runtime
            .catalog
            .table("app", "users")
            .cloned()
            .expect("simulated primary index service requires users table descriptor");
        let descriptor = PrimaryKeyDescriptor::from_table(&table)
            .expect("simulated primary key descriptor must match catalog table");
        let mut index = PrimaryIndex::default();

        Self::record(
            trace,
            runtime,
            index
                .insert(
                    &descriptor,
                    primary_row("1", "ada", PlacementGroupId(1), CommitVersion(41)),
                )
                .expect("simulated primary insert must evaluate"),
        );
        Self::record(
            trace,
            runtime,
            index
                .lookup(&descriptor, &["1".to_string()])
                .expect("simulated primary lookup must evaluate"),
        );
        Self::record(
            trace,
            runtime,
            index
                .update(
                    &descriptor,
                    &["1".to_string()],
                    primary_row("1", "ada-lovelace", PlacementGroupId(1), CommitVersion(42)),
                )
                .expect("simulated primary update must evaluate"),
        );
        Self::record(
            trace,
            runtime,
            index
                .insert(
                    &descriptor,
                    primary_row("1", "duplicate", PlacementGroupId(1), CommitVersion(43)),
                )
                .expect("simulated duplicate primary insert must reject"),
        );
        Self::record(
            trace,
            runtime,
            index
                .delete(&descriptor, &["1".to_string()])
                .expect("simulated primary delete must evaluate"),
        );
        Self::record(
            trace,
            runtime,
            index
                .lookup(&descriptor, &["1".to_string()])
                .expect("simulated missing primary lookup must evaluate"),
        );
        runtime.primary_entries = index.len();
        trace.push(
            "primary",
            "summary",
            format!(
                "inserts={} lookups={} updates={} deletes={} rejections={} entries={}",
                runtime.primary_inserts,
                runtime.primary_lookups,
                runtime.primary_updates,
                runtime.primary_deletes,
                runtime.primary_rejections,
                runtime.primary_entries
            ),
        );
    }

    fn record(trace: &mut Trace, runtime: &mut DatabaseRuntime, decision: PrimaryDecision) {
        match decision {
            PrimaryDecision::Inserted(entry) => {
                runtime.primary_inserts += 1;
                trace.push("primary", "insert", primary_entry("inserted", &entry));
            }
            PrimaryDecision::Updated { before, after } => {
                runtime.primary_updates += 1;
                trace.push(
                    "primary",
                    "update",
                    format!(
                        "key={} row_key={} pg={} version {}->{}",
                        bytes_label(&after.encoded_key),
                        bytes_label(&after.row_key),
                        after.placement_group.0,
                        before.version.0,
                        after.version.0
                    ),
                );
            }
            PrimaryDecision::Deleted(entry) => {
                runtime.primary_deletes += 1;
                trace.push("primary", "delete", primary_entry("deleted", &entry));
            }
            PrimaryDecision::Found(entry) => {
                runtime.primary_lookups += 1;
                trace.push("primary", "lookup", primary_entry("found", &entry));
            }
            PrimaryDecision::Missing { encoded_key } => {
                runtime.primary_lookups += 1;
                trace.push(
                    "primary",
                    "lookup-miss",
                    format!("key={} missing=true", bytes_label(&encoded_key)),
                );
            }
            PrimaryDecision::Rejected {
                encoded_key,
                reason,
            } => {
                runtime.primary_rejections += 1;
                trace.push(
                    "primary",
                    "reject",
                    format!("key={} reason={reason}", bytes_label(&encoded_key)),
                );
            }
        }
    }
}

pub struct Index;

impl Index {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) {
        let table = runtime
            .catalog
            .table("app", "users")
            .cloned()
            .expect("simulated index service requires users table descriptor");
        let descriptor = IndexDescriptor {
            table: table.name.clone(),
            name: "users_name".to_string(),
            columns: vec!["name".to_string()],
            unique: false,
        };
        let planner = IndexPlanner::new(&table, vec![descriptor.clone()])
            .expect("simulated index descriptor must match catalog table");
        let mut store = IndexStore::default();

        let ada = index_row("1", "ada", true);
        let rebuild = planner
            .plan_rebuild(&descriptor, std::slice::from_ref(&ada))
            .expect("simulated index rebuild must plan");
        store.apply_rebuild(&rebuild);
        runtime.index_rebuilds += 1;
        runtime.index_mutations += rebuild.mutations.len();
        trace.push(
            "index",
            "rebuild",
            format!(
                "index={} scanned_rows={} mutations={}",
                rebuild.index,
                rebuild.scanned_rows,
                rebuild.mutations.len()
            ),
        );

        let insert = planner
            .plan_insert(&table, &ada)
            .expect("simulated index insert must plan");
        store.apply(&insert);
        runtime.index_mutations += insert.mutations.len();
        trace.push(
            "index",
            "insert",
            format!(
                "table={} primary_key={} mutations={}",
                insert.table.0,
                ada.primary_key,
                insert.mutations.len()
            ),
        );

        let grace = index_row("1", "grace", true);
        let update = planner
            .plan_update(&table, &ada, &grace)
            .expect("simulated index update must plan");
        store.apply(&update);
        runtime.index_mutations += update.mutations.len();
        trace.push(
            "index",
            "update",
            format!(
                "primary_key={} old_name=ada new_name=grace mutations={}",
                grace.primary_key,
                update.mutations.len()
            ),
        );

        let lookup_key = rsf_index::encode_index_key(&descriptor, &grace)
            .expect("simulated index lookup key must encode");
        let matches = store.lookup(&descriptor.name, &lookup_key);
        trace.push(
            "index",
            "lookup",
            format!(
                "index={} key={} primary_keys={}",
                descriptor.name,
                bytes_label(&lookup_key),
                matches.join(",")
            ),
        );

        let delete = planner
            .plan_delete(&table, &grace)
            .expect("simulated index delete must plan");
        store.apply(&delete);
        runtime.index_mutations += delete.mutations.len();
        runtime.index_entries = store.entry_count();
        trace.push(
            "index",
            "delete",
            format!(
                "primary_key={} mutations={} remaining_entries={}",
                grace.primary_key,
                delete.mutations.len(),
                runtime.index_entries
            ),
        );
    }
}

impl Relational {
    pub fn run(trace: &mut Trace, implementation: &Implementation, runtime: &mut DatabaseRuntime) {
        trace.push("relational", "catalog", implementation.database.relational);
        runtime
            .sql
            .execute(
                &mut runtime.router,
                PlacementGroupId(1),
                "CREATE TABLE users(id PRIMARY KEY, name)",
            )
            .expect("simulated CREATE TABLE must execute through SQL surface");
        trace.push(
            "relational",
            "schema",
            "executed CREATE TABLE users(id PRIMARY KEY, name)",
        );
        runtime
            .sql
            .execute(
                &mut runtime.router,
                PlacementGroupId(1),
                "CREATE INDEX users_name ON users(name)",
            )
            .expect("simulated CREATE INDEX must execute through SQL surface");
        trace.push(
            "relational",
            "index",
            "executed CREATE INDEX users_name ON users(name)",
        );
        trace.push(
            "relational",
            "execute",
            "INSERT users(id, name) VALUES (1, 'ada')",
        );
        let insert = runtime
            .sql
            .execute(
                &mut runtime.router,
                PlacementGroupId(1),
                "INSERT INTO users(id, name) VALUES (1, 'ada')",
            )
            .expect("simulated SQL insert must commit through protocol router");
        let SqlOutcome::Inserted { row, evidence } = insert else {
            panic!("simulated SQL insert returned non-insert outcome");
        };
        let evidence = evidence.expect_committed("relational insert");
        trace.push(
            "relational",
            "result",
            format!(
                "row users/1 visible at commit version {} through rsf-sql pg={} log_index={} op={}",
                row.version.0,
                evidence.placement_group.0,
                evidence.log_index.0,
                evidence.operation_id.0
            ),
        );
        let select = runtime
            .sql
            .execute(
                &mut runtime.router,
                PlacementGroupId(1),
                "SELECT * FROM users WHERE id = 1",
            )
            .expect("simulated SELECT must execute through SQL surface");
        let SqlOutcome::Selected { rows } = select else {
            panic!("simulated SQL select returned non-select outcome");
        };
        trace.push(
            "relational",
            "select",
            format!(
                "SELECT returned {} row through rsf-sql executor",
                rows.len()
            ),
        );
        let indexed_select = runtime
            .sql
            .execute(
                &mut runtime.router,
                PlacementGroupId(1),
                "SELECT * FROM users WHERE name = 'ada'",
            )
            .expect("simulated indexed SELECT must execute through SQL surface");
        let SqlOutcome::Selected { rows } = indexed_select else {
            panic!("simulated SQL indexed select returned non-select outcome");
        };
        trace.push(
            "relational",
            "index-select",
            format!("secondary index lookup returned {} row", rows.len()),
        );
        let update = runtime
            .sql
            .execute(
                &mut runtime.router,
                PlacementGroupId(1),
                "UPDATE users SET name = 'grace' WHERE id = 1",
            )
            .expect("simulated UPDATE must execute through SQL surface");
        let SqlOutcome::Updated { row, evidence } = update else {
            panic!("simulated SQL update returned non-update outcome");
        };
        let evidence = evidence.expect_committed("relational update");
        trace.push(
            "relational",
            "update",
            format!(
                "updated users/1 name={} at commit version {} pg={} log_index={} op={}",
                row.columns["name"],
                row.version.0,
                evidence.placement_group.0,
                evidence.log_index.0,
                evidence.operation_id.0
            ),
        );
        runtime
            .sql
            .execute(
                &mut runtime.router,
                PlacementGroupId(1),
                "DELETE FROM users WHERE id = 1",
            )
            .expect("simulated DELETE must execute through SQL surface");
        trace.push(
            "relational",
            "delete",
            "deleted users/1 and removed secondary index entries",
        );
    }
}

pub struct RangeLayoutService;

impl RangeLayoutService {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) {
        let mut layout = RangeLayout::default();
        let root = layout
            .create_range(
                LayoutRange::new(b"app/".to_vec(), b"app0".to_vec())
                    .expect("simulated root range must be valid"),
                PlacementGroupId(1),
                runtime.router.latest_version(),
            )
            .expect("simulated root range create must succeed");
        trace.push(
            "range",
            "create",
            format!(
                "range={} span={}..{} pg={}",
                root.id.0,
                bytes_label(&root.span.start),
                bytes_label(&root.span.end),
                root.pg.0
            ),
        );

        let (left, right) = layout
            .split(
                &root.id,
                b"app/users/".to_vec(),
                PlacementGroupId(1),
                PlacementGroupId(2),
                runtime.router.latest_version(),
            )
            .expect("simulated range split must succeed");
        runtime.range_partitions = layout.range_count();
        runtime.range_non_overlaps += usize::from(layout.non_overlapping());
        trace.push(
            "range",
            "split",
            format!(
                "root={} left={} pg={} right={} pg={}",
                root.id.0, left.id.0, left.pg.0, right.id.0, right.pg.0
            ),
        );
        trace.push(
            "range",
            "non-overlap",
            format!(
                "partitions={} non_overlapping={}",
                layout.range_count(),
                layout.non_overlapping()
            ),
        );
        let transfer = layout
            .transfer_ownership(
                &right.id,
                right.version,
                right.owner_epoch,
                PlacementGroupId(3),
                Epoch(right.owner_epoch.0 + 1),
                CommitVersion(right.version.0 + 1),
            )
            .expect("simulated range ownership transfer must succeed");
        trace.push(
            "range",
            "ownership-transfer",
            format!(
                "range={} pg={}=>{} owner_epoch={}=>{} version={}=>{} non_overlapping={}",
                transfer.range.0,
                transfer.previous_pg.0,
                transfer.new_pg.0,
                transfer.previous_owner_epoch.0,
                transfer.new_owner_epoch.0,
                transfer.previous_version.0,
                transfer.version.0,
                layout.non_overlapping()
            ),
        );
        match layout.transfer_ownership(
            &right.id,
            right.version,
            right.owner_epoch,
            PlacementGroupId(4),
            Epoch(right.owner_epoch.0 + 2),
            CommitVersion(right.version.0 + 2),
        ) {
            Ok(_) => trace.push("range", "unexpected", "stale ownership transfer accepted"),
            Err(err) => trace.push(
                "range",
                "ownership-reject",
                format!(
                    "range={} expected_version={} reason={}",
                    right.id.0, right.version.0, err.message
                ),
            ),
        }
        match layout.create_range(
            LayoutRange::new(b"app/config/".to_vec(), b"app/users/2".to_vec())
                .expect("simulated overlap probe range must be valid"),
            PlacementGroupId(3),
            runtime.router.latest_version(),
        ) {
            Ok(_) => trace.push("range", "unexpected", "overlapping range accepted"),
            Err(err) => {
                runtime.range_rejections += 1;
                trace.push("range", "reject", format!("reason={}", err.message));
            }
        }

        for table in [
            Self::table(
                left.id.clone(),
                b"app/config/a",
                b"app/config/z",
                20,
                128,
                1,
            ),
            Self::table(right.id.clone(), b"app/users/1", b"app/users/9", 21, 256, 2),
            Self::table(right.id.clone(), b"app/users/1", b"app/users/9", 22, 384, 3),
        ] {
            layout
                .add_table(table.clone())
                .expect("simulated SSTable manifest must fit range");
            trace.push(
                "range",
                "sstable",
                format!(
                    "range={} level={} first={} last={} bytes={}",
                    table.range.0,
                    table.level,
                    bytes_label(&table.first_key),
                    bytes_label(&table.last_key),
                    table.bytes
                ),
            );
        }

        let plan = layout
            .plan_compaction(&right.id, 6)
            .expect("simulated compaction plan must have inputs");
        runtime.sstable_manifests = layout.table_count();
        runtime.compaction_inputs = plan.inputs.len();
        trace.push(
            "range",
            "compaction",
            format!(
                "range={} inputs={} output_level={} estimated_bytes={}",
                plan.range.0,
                plan.inputs.len(),
                plan.output_level,
                plan.estimated_bytes
            ),
        );
    }

    fn table(
        range: rsf_range::RangeId,
        first: &[u8],
        last: &[u8],
        version: u64,
        bytes: u64,
        checksum_byte: u8,
    ) -> SstableManifest {
        SstableManifest {
            range,
            level: 0,
            first_key: first.to_vec(),
            last_key: last.to_vec(),
            version: CommitVersion(version),
            bytes,
            checksum: Digest::blake3([checksum_byte; 32])
                .expect("simulated SSTable checksum digest must be non-empty"),
        }
    }
}

pub struct Compaction;

impl Compaction {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) {
        let old = Self::table(RangeId(3), b"app/users/1", b"app/users/9", 21, 256, 2);
        let new = Self::table(RangeId(3), b"app/users/1", b"app/users/9", 22, 384, 3);
        let plan = CompactionPlan {
            range: RangeId(3),
            inputs: vec![old.clone(), new.clone()],
            output_level: 1,
            estimated_bytes: old.bytes + new.bytes,
        };
        trace.push(
            "compaction",
            "plan",
            format!(
                "range={} inputs={} output_level={} estimated_bytes={}",
                plan.range.0,
                plan.inputs.len(),
                plan.output_level,
                plan.estimated_bytes
            ),
        );

        let runs = vec![
            CompactionRun {
                manifest: old,
                entries: vec![
                    Self::entry(b"app/users/1", Some(b"ada"), 21),
                    Self::entry(b"app/users/2", Some(b"grace"), 21),
                ],
            },
            CompactionRun {
                manifest: new,
                entries: vec![
                    Self::entry(b"app/users/1", Some(b"ada-lovelace"), 22),
                    Self::entry(b"app/users/2", None, 22),
                ],
            },
        ];
        for run in &runs {
            trace.push(
                "compaction",
                "input",
                format!(
                    "level={} bounds={}..{} entries={} bytes={}",
                    run.manifest.level,
                    bytes_label(&run.manifest.first_key),
                    bytes_label(&run.manifest.last_key),
                    run.entries.len(),
                    run.manifest.bytes
                ),
            );
        }

        let output = CompactionExecutor {
            gc_before: CommitVersion(21),
        }
        .compact(&plan, &runs)
        .expect("simulated compaction must merge planned runs");
        runtime.compaction_outputs = 1;
        runtime.compaction_live_keys = output.live_entries.len();
        runtime.compaction_tombstones = output.tombstones as usize;
        runtime.compaction_reclaimed_bytes = output.reclaimed_bytes as usize;
        trace.push(
            "compaction",
            "merge",
            format!(
                "live_keys={} tombstones={} input_bytes={} output_bytes={} reclaimed_bytes={}",
                output.live_entries.len(),
                output.tombstones,
                output.input_bytes,
                output.output_bytes,
                output.reclaimed_bytes
            ),
        );
        trace.push(
            "compaction",
            "output",
            format!(
                "range={} level={} first={} last={} version={} checksum={}",
                output.manifest.range.0,
                output.manifest.level,
                bytes_label(&output.manifest.first_key),
                bytes_label(&output.manifest.last_key),
                output.manifest.version.0,
                output.manifest.checksum.as_bytes()[0]
            ),
        );
    }

    fn table(
        range: RangeId,
        first: &[u8],
        last: &[u8],
        version: u64,
        bytes: u64,
        checksum_byte: u8,
    ) -> SstableManifest {
        SstableManifest {
            range,
            level: 0,
            first_key: first.to_vec(),
            last_key: last.to_vec(),
            version: CommitVersion(version),
            bytes,
            checksum: Digest::blake3([checksum_byte; 32])
                .expect("simulated SSTable checksum digest must be non-empty"),
        }
    }

    fn entry(key: &[u8], value: Option<&[u8]>, version: u64) -> CompactionEntry {
        CompactionEntry {
            key: key.to_vec(),
            value: value.map(|value| value.to_vec()),
            version: CommitVersion(version),
        }
    }
}

pub struct Statistics;

impl Statistics {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) {
        let table = runtime
            .catalog
            .table("app", "users")
            .cloned()
            .expect("simulated statistics requires users table");
        let estimate = runtime
            .stats
            .publish(&table, Self::sample(&table.name))
            .expect("simulated table statistics must publish");
        runtime.stats_rows = estimate.rows as usize;
        runtime.stats_histogram_buckets = estimate.histogram_bucket_count();
        trace.push(
            "stats",
            "publish",
            format!(
                "table={} rows={} avg_row_bytes={} version={} histogram_buckets={}",
                estimate.table.0,
                estimate.rows,
                estimate.average_row_bytes(),
                estimate.version.0,
                estimate.histogram_bucket_count()
            ),
        );
        for column in &estimate.columns {
            trace.push(
                "stats",
                "histogram",
                format!(
                    "column={} distinct={} nulls={} buckets={}",
                    column.column,
                    column.distinct_values,
                    column.nulls,
                    column
                        .histogram
                        .iter()
                        .map(|bucket| format!("{}:{}", bucket.value, bucket.rows))
                        .collect::<Vec<_>>()
                        .join(",")
                ),
            );
        }
        if let Freshness::Stale { lag } = runtime
            .stats
            .freshness(&table.name, CommitVersion(25), 2)
            .expect("simulated statistics freshness must classify")
        {
            runtime.stats_stale += 1;
            trace.push(
                "stats",
                "stale",
                format!(
                    "table={} lag={} max_lag=2 action=refresh-required",
                    table.name.0, lag
                ),
            );
        }
        runtime
            .sql
            .execute(
                &mut runtime.router,
                PlacementGroupId(1),
                "CREATE TABLE stats_users(id PRIMARY KEY, name, active)",
            )
            .expect("simulated SQL stats table must be created");
        runtime
            .sql
            .execute(
                &mut runtime.router,
                PlacementGroupId(1),
                "INSERT INTO stats_users(id, name, active) VALUES (1, 'ada', true)",
            )
            .expect("simulated SQL stats row must insert");
        runtime
            .sql
            .execute(
                &mut runtime.router,
                PlacementGroupId(1),
                "INSERT INTO stats_users(id, name, active) VALUES (2, 'grace', false)",
            )
            .expect("simulated second SQL stats row must insert");
        let sql_estimate = runtime
            .sql
            .analyze_table("stats_users", runtime.router.latest_version())
            .expect("simulated SQL analyze must publish real estimate");
        trace.push(
            "stats",
            "sql-analyze",
            format!(
                "table={} rows={} avg_row_bytes={} version={} histogram_buckets={}",
                sql_estimate.table.0,
                sql_estimate.rows,
                sql_estimate.average_row_bytes(),
                sql_estimate.version.0,
                sql_estimate.histogram_bucket_count()
            ),
        );
        if let Freshness::Stale { lag } = runtime
            .sql
            .stats_freshness(
                "stats_users",
                CommitVersion(runtime.router.latest_version().0 + 3),
                1,
            )
            .expect("simulated SQL stats freshness must classify")
        {
            trace.push(
                "stats",
                "sql-stale",
                format!("table=stats_users lag={lag} max_lag=1 action=refresh-required"),
            );
        }
        runtime
            .sql
            .execute(
                &mut runtime.router,
                PlacementGroupId(1),
                "DELETE FROM stats_users WHERE id = 1",
            )
            .expect("simulated SQL stats cleanup must delete first row");
        runtime
            .sql
            .execute(
                &mut runtime.router,
                PlacementGroupId(1),
                "DELETE FROM stats_users WHERE id = 2",
            )
            .expect("simulated SQL stats cleanup must delete second row");
    }

    fn sample(table: &TableName) -> TableSample {
        TableSample {
            table: table.clone(),
            rows: 1_024,
            total_bytes: 524_288,
            version: CommitVersion(22),
            columns: vec![
                ColumnSample {
                    column: "name".to_string(),
                    values: vec![
                        ValueSample {
                            value: Some("ada".to_string()),
                            rows: 5,
                        },
                        ValueSample {
                            value: Some("grace".to_string()),
                            rows: 7,
                        },
                    ],
                },
                ColumnSample {
                    column: "active".to_string(),
                    values: vec![
                        ValueSample {
                            value: Some("true".to_string()),
                            rows: 768,
                        },
                        ValueSample {
                            value: Some("false".to_string()),
                            rows: 256,
                        },
                    ],
                },
            ],
        }
    }
}

pub struct RelationalConstraints;

impl RelationalConstraints {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) {
        let table = runtime
            .catalog
            .table("app", "users")
            .cloned()
            .expect("simulated constraints require users table");
        let validator = ConstraintValidator::new(
            &table,
            vec![
                ConstraintRule::PrimaryKey {
                    columns: table.primary_key.clone(),
                },
                ConstraintRule::Unique {
                    name: "users_name".to_string(),
                    columns: vec!["name".to_string()],
                },
            ],
        )
        .expect("simulated constraints must validate against catalog");
        let mut state = ConstraintState::default();
        state.add_unique_key("users_name", ["grace"]);
        runtime.constraints_unique_keys = state.unique_key_count();
        trace.push(
            "constraints",
            "load",
            format!(
                "table={} not_null={} primary_key={:?} unique=users_name(name) existing_unique_keys={}",
                table.name.0,
                table.columns.iter().filter(|column| !column.nullable).count(),
                table.primary_key,
                state.unique_key_count()
            ),
        );

        for (label, mutation) in [
            ("accept", Self::mutation("1", Some("ada"), true)),
            ("duplicate", Self::mutation("2", Some("grace"), true)),
            ("null", Self::mutation("3", None, true)),
        ] {
            let decision = validator
                .validate(&table, &mutation, &state)
                .expect("simulated constraint decision must evaluate");
            match decision {
                ConstraintDecision::Accepted { checks } => {
                    runtime.constraints_checked += checks.len();
                    runtime.constraints_accepted += 1;
                    trace.push(
                        "constraints",
                        label,
                        format!(
                            "accepted checks={} values={}",
                            checks.len(),
                            Self::values(&mutation)
                        ),
                    );
                }
                ConstraintDecision::Rejected { checks, violations } => {
                    runtime.constraints_checked += checks.len();
                    runtime.constraints_rejected += 1;
                    trace.push(
                        "constraints",
                        label,
                        format!(
                            "rejected checks={} violations={}",
                            checks.len(),
                            violations
                                .iter()
                                .map(|violation| violation.rule.clone())
                                .collect::<Vec<_>>()
                                .join(",")
                        ),
                    );
                }
            }
        }
    }

    fn mutation(id: &str, name: Option<&str>, active: bool) -> RowMutation {
        RowMutation {
            table: TableName("users".to_string()),
            values: BTreeMap::from([
                ("id".to_string(), ConstraintValue::Text(id.to_string())),
                (
                    "name".to_string(),
                    name.map(|name| ConstraintValue::Text(name.to_string()))
                        .unwrap_or(ConstraintValue::Null),
                ),
                (
                    "active".to_string(),
                    ConstraintValue::Text(active.to_string()),
                ),
            ]),
        }
    }

    fn values(mutation: &RowMutation) -> String {
        mutation
            .values
            .iter()
            .map(|(column, value)| match value {
                ConstraintValue::Null => format!("{column}=null"),
                ConstraintValue::Text(value) => format!("{column}={value}"),
            })
            .collect::<Vec<_>>()
            .join(",")
    }
}

pub struct DeferredConstraints;

impl DeferredConstraints {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) {
        let mut engine = DeferredConstraintEngine::default();
        let transaction = TransactionId([61; 16]);
        Self::record(
            trace,
            runtime,
            engine
                .begin(transaction.clone())
                .expect("simulated deferred constraint transaction must begin"),
        );
        Self::record(
            trace,
            runtime,
            engine
                .defer_foreign_key(&transaction, Self::rule(), &Self::order("42"))
                .expect("simulated child row must stage deferred reference"),
        );
        Self::record(
            trace,
            runtime,
            engine
                .stage_referenced_key(
                    &transaction,
                    &TableName("users".to_string()),
                    &["id".to_string()],
                    ["42"],
                )
                .expect("simulated parent row reference must stage in same transaction"),
        );
        Self::record(
            trace,
            runtime,
            engine
                .commit(&transaction, &CommittedReferences::default())
                .expect("simulated deferred reference should pass at commit"),
        );

        let rejected = TransactionId([62; 16]);
        Self::record(
            trace,
            runtime,
            engine
                .begin(rejected.clone())
                .expect("simulated rejected deferred transaction must begin"),
        );
        Self::record(
            trace,
            runtime,
            engine
                .defer_foreign_key(&rejected, Self::rule(), &Self::order("missing"))
                .expect("simulated missing parent obligation must stage"),
        );
        Self::record(
            trace,
            runtime,
            engine
                .commit(&rejected, &CommittedReferences::default())
                .expect("simulated missing parent must produce commit-time rejection"),
        );

        let report = engine.report();
        trace.push(
            "deferred",
            "summary",
            format!(
                "transactions={} obligations={} txn_references={} committed_references={} commits={} rejections={}",
                report.transactions,
                report.obligations,
                report.transaction_references,
                report.committed_references,
                report.commits,
                report.rejections
            ),
        );
    }

    fn record(
        trace: &mut Trace,
        runtime: &mut DatabaseRuntime,
        decision: DeferredConstraintDecision,
    ) {
        match decision {
            DeferredConstraintDecision::Began { transaction } => {
                runtime.deferred_transactions += 1;
                trace.push(
                    "deferred",
                    "begin",
                    format!(
                        "txn={} opened deferred constraint context",
                        txn_label(&transaction)
                    ),
                );
            }
            DeferredConstraintDecision::ReferencedKeyStaged {
                transaction,
                reference,
                key,
            } => {
                runtime.deferred_txn_references += 1;
                trace.push(
                    "deferred",
                    "stage-reference",
                    format!(
                        "txn={} reference={} key={} source=transaction",
                        txn_label(&transaction),
                        reference,
                        key.join("/")
                    ),
                );
            }
            DeferredConstraintDecision::ObligationStaged {
                transaction,
                rule,
                key,
            } => {
                runtime.deferred_obligations += 1;
                trace.push(
                    "deferred",
                    "defer",
                    format!(
                        "txn={} rule={} key={} check=commit-time",
                        txn_label(&transaction),
                        rule,
                        key.join("/")
                    ),
                );
            }
            DeferredConstraintDecision::CommitAccepted {
                transaction,
                obligations,
                transaction_references,
                committed_references,
            } => {
                runtime.deferred_commits += 1;
                runtime.deferred_committed_references += committed_references;
                trace.push(
                    "deferred",
                    "commit",
                    format!(
                        "txn={} accepted obligations={} txn_references={} committed_references={}",
                        txn_label(&transaction),
                        obligations,
                        transaction_references,
                        committed_references
                    ),
                );
            }
            DeferredConstraintDecision::CommitRejected {
                transaction,
                violations,
            } => {
                runtime.deferred_rejections += 1;
                trace.push(
                    "deferred",
                    "reject",
                    format!(
                        "txn={} rejected_at_commit violations={}",
                        txn_label(&transaction),
                        violations
                            .iter()
                            .map(|violation| format!(
                                "{}:{}={}",
                                violation.rule,
                                violation.reference,
                                violation.key.join("/")
                            ))
                            .collect::<Vec<_>>()
                            .join(",")
                    ),
                );
            }
        }
    }

    fn rule() -> DeferredReferenceRule {
        DeferredReferenceRule {
            name: "orders_user".to_string(),
            table: TableName("orders".to_string()),
            columns: vec!["user_id".to_string()],
            referenced_table: TableName("users".to_string()),
            referenced_columns: vec!["id".to_string()],
        }
    }

    fn order(user_id: &str) -> DeferredRow {
        DeferredRow {
            table: TableName("orders".to_string()),
            values: BTreeMap::from([
                ("id".to_string(), "900".to_string()),
                ("user_id".to_string(), user_id.to_string()),
            ]),
        }
    }
}

pub struct RowCodecService;

impl RowCodecService {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) {
        let table = Self::codec_table(runtime);
        let codec = RowCodec;
        let policy = RowCodecPolicy {
            schema_version: CommitVersion(
                runtime
                    .catalog
                    .table_version("app", "users")
                    .expect("simulated row codec requires users table version")
                    .0
                    + 1,
            ),
            defaults: BTreeMap::from([("active".to_string(), RowValue::Bool(true))]),
            generated: vec![GeneratedColumn::TextLength {
                column: "name_len".to_string(),
                source: "name".to_string(),
            }],
        };

        let encoded = codec
            .encode(
                &table,
                BTreeMap::from([
                    ("id".to_string(), RowValue::Int64(7)),
                    ("name".to_string(), RowValue::Text("ada".to_string())),
                ]),
                policy.clone(),
            )
            .expect("simulated row codec encode must succeed");
        runtime.rowcodec_encoded += 1;
        runtime.rowcodec_defaults += encoded.defaults_applied;
        runtime.rowcodec_generated += encoded.generated_applied;
        runtime.rowcodec_bytes += encoded.bytes.len();
        trace.push(
            "rowcodec",
            "encode",
            format!(
                "table={} schema_version={} columns={} bytes={} defaults={} generated={}",
                encoded.table,
                encoded.schema_version.0,
                table.columns.len(),
                encoded.bytes.len(),
                encoded.defaults_applied,
                encoded.generated_applied
            ),
        );

        let decoded = codec
            .decode(&table, &encoded.bytes)
            .expect("simulated row codec decode must succeed");
        runtime.rowcodec_decoded += 1;
        trace.push(
            "rowcodec",
            "decode",
            format!(
                "table={} schema_version={} values={} active={} name_len={}",
                decoded.table,
                decoded.schema_version.0,
                decoded.values.len(),
                row_value_label(decoded.values.get("active")),
                row_value_label(decoded.values.get("name_len"))
            ),
        );

        for (label, input) in [
            (
                "missing-required",
                BTreeMap::from([("id".to_string(), RowValue::Int64(8))]),
            ),
            (
                "type-mismatch",
                BTreeMap::from([
                    ("id".to_string(), RowValue::Text("9".to_string())),
                    ("name".to_string(), RowValue::Text("hopper".to_string())),
                    ("active".to_string(), RowValue::Bool(false)),
                    ("name_len".to_string(), RowValue::Int64(6)),
                ]),
            ),
        ] {
            match codec.encode(
                &table,
                input,
                RowCodecPolicy {
                    schema_version: policy.schema_version,
                    defaults: BTreeMap::new(),
                    generated: Vec::new(),
                },
            ) {
                Ok(_) => trace.push("rowcodec", "unexpected", format!("case={label}")),
                Err(error) => {
                    runtime.rowcodec_rejections += 1;
                    trace.push(
                        "rowcodec",
                        "reject",
                        format!("case={label} reason={}", error.message),
                    );
                }
            }
        }

        trace.push(
            "rowcodec",
            "summary",
            format!(
                "encoded={} decoded={} defaults={} generated={} bytes={} rejections={}",
                runtime.rowcodec_encoded,
                runtime.rowcodec_decoded,
                runtime.rowcodec_defaults,
                runtime.rowcodec_generated,
                runtime.rowcodec_bytes,
                runtime.rowcodec_rejections
            ),
        );
    }

    fn codec_table(runtime: &DatabaseRuntime) -> TableDescriptor {
        let mut table = runtime
            .catalog
            .table("app", "users")
            .cloned()
            .expect("simulated row codec requires users table");
        table.columns.push(ColumnDescriptor {
            name: "name_len".to_string(),
            ty: ColumnType::Int64,
            nullable: false,
        });
        table
    }
}

pub struct RelationalWrite;

pub struct TupleVisibilityService;

impl TupleVisibilityService {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) {
        let table = RowCodecService::codec_table(runtime);
        let codec = RowCodec;
        let schema_version = CommitVersion(
            runtime
                .catalog
                .table_version("app", "users")
                .expect("simulated tuple visibility requires users table version")
                .0
                + 1,
        );
        let first = codec
            .encode(
                &table,
                BTreeMap::from([
                    ("id".to_string(), RowValue::Int64(7)),
                    ("name".to_string(), RowValue::Text("ada".to_string())),
                    ("active".to_string(), RowValue::Bool(true)),
                    ("name_len".to_string(), RowValue::Int64(3)),
                ]),
                RowCodecPolicy {
                    schema_version,
                    defaults: BTreeMap::new(),
                    generated: Vec::new(),
                },
            )
            .expect("simulated tuple visibility first row must encode");
        let second = codec
            .encode(
                &table,
                BTreeMap::from([
                    ("id".to_string(), RowValue::Int64(7)),
                    (
                        "name".to_string(),
                        RowValue::Text("ada-lovelace".to_string()),
                    ),
                    ("active".to_string(), RowValue::Bool(true)),
                    ("name_len".to_string(), RowValue::Int64(12)),
                ]),
                RowCodecPolicy {
                    schema_version,
                    defaults: BTreeMap::new(),
                    generated: Vec::new(),
                },
            )
            .expect("simulated tuple visibility updated row must encode");

        let mut store = TupleStore::default();
        let key = TupleKey::new("users", b"id=7".to_vec());

        Self::record(
            trace,
            runtime,
            store
                .insert(key.clone(), first.bytes, CommitVersion(10), schema_version)
                .expect("simulated tuple insert must succeed"),
        );
        Self::record(
            trace,
            runtime,
            store
                .update(&key, second.bytes, CommitVersion(12), schema_version)
                .expect("simulated tuple update must succeed"),
        );
        for snapshot in [CommitVersion(11), CommitVersion(12)] {
            Self::record(trace, runtime, store.read_visible(&key, snapshot));
        }
        Self::record(
            trace,
            runtime,
            store
                .delete(&key, CommitVersion(14))
                .expect("simulated tuple delete must succeed"),
        );
        Self::record(trace, runtime, store.read_visible(&key, CommitVersion(14)));

        for decision in [
            store
                .insert(
                    key.clone(),
                    b"duplicate-live".to_vec(),
                    CommitVersion(13),
                    schema_version,
                )
                .expect("simulated tuple duplicate probe must return decision"),
            store
                .update(
                    &TupleKey::new("users", b"id=404".to_vec()),
                    b"missing".to_vec(),
                    CommitVersion(15),
                    schema_version,
                )
                .expect("simulated tuple missing update must return decision"),
        ] {
            Self::record(trace, runtime, decision);
        }

        trace.push(
            "tuple",
            "summary",
            format!(
                "versions={} visible_reads={} delete_markers={} bytes={} rejections={}",
                runtime.tuple_versions,
                runtime.tuple_visible_reads,
                runtime.tuple_delete_markers,
                runtime.tuple_bytes,
                runtime.tuple_rejections
            ),
        );
    }

    fn record(trace: &mut Trace, runtime: &mut DatabaseRuntime, decision: TupleDecision) {
        match decision {
            TupleDecision::Inserted(version) => {
                runtime.tuple_versions += 1;
                runtime.tuple_bytes += version.row.len();
                trace.push("tuple", "insert", tuple_version_label(&version));
            }
            TupleDecision::Updated { previous, current } => {
                runtime.tuple_versions += 1;
                runtime.tuple_delete_markers += usize::from(previous.delete_version.is_some());
                runtime.tuple_bytes += current.row.len();
                trace.push(
                    "tuple",
                    "update",
                    format!(
                        "{} previous_deleted_at={}",
                        tuple_version_label(&current),
                        previous
                            .delete_version
                            .map(|version| version.0.to_string())
                            .unwrap_or_else(|| "none".to_string())
                    ),
                );
            }
            TupleDecision::Deleted(header) => {
                runtime.tuple_delete_markers += usize::from(header.delete_version.is_some());
                trace.push(
                    "tuple",
                    "delete",
                    format!(
                        "create_version={} delete_version={} schema_version={} row_bytes={}",
                        header.create_version.0,
                        header
                            .delete_version
                            .map(|version| version.0.to_string())
                            .unwrap_or_else(|| "none".to_string()),
                        header.schema_version.0,
                        header.row_bytes
                    ),
                );
            }
            TupleDecision::Visible { snapshot, version } => {
                runtime.tuple_visible_reads += 1;
                trace.push(
                    "tuple",
                    "visible",
                    format!("snapshot={} {}", snapshot.0, tuple_version_label(&version)),
                );
            }
            TupleDecision::Invisible { snapshot, key } => {
                trace.push(
                    "tuple",
                    "invisible",
                    format!(
                        "snapshot={} table={} key={}",
                        snapshot.0,
                        key.table,
                        String::from_utf8_lossy(&key.primary_key)
                    ),
                );
            }
            TupleDecision::Rejected { key, reason } => {
                runtime.tuple_rejections += 1;
                trace.push(
                    "tuple",
                    "reject",
                    format!(
                        "table={} key={} reason={}",
                        key.table,
                        String::from_utf8_lossy(&key.primary_key),
                        reason
                    ),
                );
            }
        }
    }
}

pub struct VacuumService;

impl VacuumService {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) {
        let mut store = TupleStore::default();
        let key = TupleKey::new("users", b"id=7".to_vec());
        store
            .insert(
                key.clone(),
                b"ada-v1".to_vec(),
                CommitVersion(10),
                CommitVersion(3),
            )
            .expect("simulated vacuum seed insert must succeed");
        store
            .update(
                &key,
                b"ada-v2".to_vec(),
                CommitVersion(12),
                CommitVersion(3),
            )
            .expect("simulated vacuum seed update must succeed");
        store
            .update(
                &key,
                b"ada-v3".to_vec(),
                CommitVersion(14),
                CommitVersion(3),
            )
            .expect("simulated vacuum seed second update must succeed");

        let planner = VacuumPlanner;
        let chain = store
            .chain(&key)
            .expect("simulated vacuum requires seeded tuple chain");
        Self::record(
            trace,
            runtime,
            planner.plan(
                chain,
                &VacuumPolicy::new(CommitVersion(14), 1)
                    .expect("simulated vacuum policy must validate"),
            ),
        );
        Self::record(
            trace,
            runtime,
            planner.plan(
                chain,
                &VacuumPolicy::new(CommitVersion(11), 1)
                    .expect("simulated blocked vacuum policy must validate"),
            ),
        );

        Self::record(
            trace,
            runtime,
            planner.plan(
                &rsf_tuple::TupleChain {
                    key: TupleKey::new("users", b"id=404".to_vec()),
                    versions: Vec::new(),
                },
                &VacuumPolicy::new(CommitVersion(20), 1)
                    .expect("simulated vacuum rejection policy must validate"),
            ),
        );

        runtime
            .sql
            .execute(
                &mut runtime.router,
                PlacementGroupId(1),
                "CREATE TABLE vacuum_users(id PRIMARY KEY, name)",
            )
            .expect("simulated SQL vacuum table must be created");
        runtime
            .sql
            .execute(
                &mut runtime.router,
                PlacementGroupId(1),
                "INSERT INTO vacuum_users(id, name) VALUES (1, 'ada')",
            )
            .expect("simulated SQL vacuum row must insert");
        runtime
            .sql
            .execute(
                &mut runtime.router,
                PlacementGroupId(1),
                "UPDATE vacuum_users SET name = 'grace' WHERE id = 1",
            )
            .expect("simulated SQL vacuum row must update");
        runtime
            .sql
            .execute(
                &mut runtime.router,
                PlacementGroupId(1),
                "UPDATE vacuum_users SET name = 'lin' WHERE id = 1",
            )
            .expect("simulated SQL vacuum row must update again");
        let sql_vacuum = runtime
            .sql
            .vacuum_table("vacuum_users", runtime.router.latest_version(), 1)
            .expect("simulated SQL vacuum must prune tuple storage");
        trace.push(
            "vacuum",
            "sql-apply",
            format!(
                "table={} chains={} pruned_versions={} retained_versions={} reclaimed_bytes={} rejections={}",
                sql_vacuum.table,
                sql_vacuum.chains,
                sql_vacuum.pruned_versions,
                sql_vacuum.retained_versions,
                sql_vacuum.reclaimed_bytes,
                sql_vacuum.rejections.len()
            ),
        );
        runtime
            .sql
            .execute(
                &mut runtime.router,
                PlacementGroupId(1),
                "DELETE FROM vacuum_users WHERE id = 1",
            )
            .expect("simulated SQL vacuum cleanup must delete visible row");

        trace.push(
            "vacuum",
            "summary",
            format!(
                "chains={} pruned_versions={} retained_versions={} reclaimed_bytes={} rejections={}",
                runtime.vacuum_chains,
                runtime.vacuum_pruned_versions,
                runtime.vacuum_retained_versions,
                runtime.vacuum_reclaimed_bytes,
                runtime.vacuum_rejections
            ),
        );
    }

    fn record(trace: &mut Trace, runtime: &mut DatabaseRuntime, decision: VacuumDecision) {
        match decision {
            VacuumDecision::Planned(plan) => {
                runtime.vacuum_chains += 1;
                runtime.vacuum_pruned_versions += plan.pruned_versions.len();
                runtime.vacuum_retained_versions += plan.retained_versions.len();
                runtime.vacuum_reclaimed_bytes += plan.reclaimed_bytes as usize;
                trace.push(
                    "vacuum",
                    "plan",
                    format!(
                        "table={} key={} oldest_snapshot={} pruned={} retained={} reclaimed_bytes={}",
                        plan.key.table,
                        String::from_utf8_lossy(&plan.key.primary_key),
                        plan.oldest_snapshot.0,
                        plan.pruned_versions.len(),
                        plan.retained_versions.len(),
                        plan.reclaimed_bytes
                    ),
                );
            }
            VacuumDecision::Rejected { key, reason } => {
                runtime.vacuum_rejections += 1;
                trace.push(
                    "vacuum",
                    "reject",
                    format!(
                        "table={} key={} reason={}",
                        key.table,
                        String::from_utf8_lossy(&key.primary_key),
                        reason
                    ),
                );
            }
        }
    }
}

pub struct MaterializationService;

impl MaterializationService {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) {
        let table = RowCodecService::codec_table(runtime);
        let schema_version = CommitVersion(
            runtime
                .catalog
                .table_version("app", "users")
                .expect("simulated materializer requires users table version")
                .0
                + 1,
        );
        let encoded = RowCodec
            .encode(
                &table,
                BTreeMap::from([
                    ("id".to_string(), RowValue::Int64(7)),
                    ("name".to_string(), RowValue::Text("ada".to_string())),
                    ("active".to_string(), RowValue::Bool(true)),
                    ("name_len".to_string(), RowValue::Int64(3)),
                ]),
                RowCodecPolicy {
                    schema_version,
                    defaults: BTreeMap::new(),
                    generated: Vec::new(),
                },
            )
            .expect("simulated materializer row must encode");
        let tuple = TupleVersion {
            key: TupleKey::new("users", b"id=7".to_vec()),
            header: rsf_tuple::TupleHeader {
                create_version: CommitVersion(12),
                delete_version: None,
                schema_version,
                row_bytes: encoded.bytes.len() as u32,
            },
            row: encoded.bytes,
        };
        let materializer = RowMaterializer::default();

        for decision in [
            materializer.materialize(&table, &tuple, &Projection::All),
            materializer.materialize(
                &table,
                &tuple,
                &Projection::Columns(vec!["name".to_string()]),
            ),
        ] {
            Self::record(trace, runtime, decision);
        }

        let mut wrong_table = tuple.clone();
        wrong_table.key = TupleKey::new("orders", b"id=7".to_vec());
        for decision in [
            materializer.materialize(&table, &wrong_table, &Projection::All),
            materializer.materialize(
                &table,
                &tuple,
                &Projection::Columns(vec!["missing".to_string()]),
            ),
        ] {
            Self::record(trace, runtime, decision);
        }

        trace.push(
            "materialize",
            "summary",
            format!(
                "rows={} projected={} columns={} bytes={} rejections={}",
                runtime.materialized_rows,
                runtime.materialized_projected,
                runtime.materialized_columns,
                runtime.materialized_bytes,
                runtime.materialized_rejections
            ),
        );
    }

    fn record(trace: &mut Trace, runtime: &mut DatabaseRuntime, decision: MaterializationDecision) {
        match decision {
            MaterializationDecision::Materialized(row) => {
                runtime.materialized_rows += 1;
                runtime.materialized_columns += row.row.columns.len();
                runtime.materialized_bytes += row.bytes as usize;
                trace.push(
                    "materialize",
                    "row",
                    format!(
                        "table={} schema_version={} create_version={} columns={} bytes={}",
                        row.row.table.0,
                        row.schema_version.0,
                        row.create_version.0,
                        row.row.columns.len(),
                        row.bytes
                    ),
                );
            }
            MaterializationDecision::Projected(row) => {
                runtime.materialized_rows += 1;
                runtime.materialized_projected += 1;
                runtime.materialized_columns += row.row.columns.len();
                runtime.materialized_bytes += row.bytes as usize;
                trace.push(
                    "materialize",
                    "project",
                    format!(
                        "table={} schema_version={} create_version={} columns={} bytes={}",
                        row.row.table.0,
                        row.schema_version.0,
                        row.create_version.0,
                        row.row.columns.len(),
                        row.bytes
                    ),
                );
            }
            MaterializationDecision::Rejected { table, reason } => {
                runtime.materialized_rejections += 1;
                trace.push(
                    "materialize",
                    "reject",
                    format!("table={} reason={}", table.0, reason),
                );
            }
        }
    }
}

impl RelationalWrite {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) {
        let table = runtime
            .catalog
            .table("app", "users")
            .cloned()
            .expect("simulated write planner requires users table");
        let constraints = ConstraintValidator::new(
            &table,
            vec![
                ConstraintRule::PrimaryKey {
                    columns: table.primary_key.clone(),
                },
                ConstraintRule::Unique {
                    name: "users_name".to_string(),
                    columns: vec!["name".to_string()],
                },
            ],
        )
        .expect("simulated write constraints must validate");
        let planner = WritePlanner::new(
            &table,
            constraints,
            vec![IndexDescriptor {
                table: table.name.clone(),
                name: "users_name".to_string(),
                columns: vec!["name".to_string()],
                unique: true,
            }],
        )
        .expect("simulated write indexes must match table");
        let request = WriteRequest {
            id: TransactionId([0x44; 16]),
            table: table.name.clone(),
            primary_key: "4".to_string(),
            values: BTreeMap::from([
                ("active".to_string(), "true".to_string()),
                ("id".to_string(), "4".to_string()),
                ("name".to_string(), "ada".to_string()),
            ]),
        };

        let decision = planner
            .plan_insert(&table, request, &ConstraintState::default())
            .expect("simulated relational write must plan");
        let WriteDecision::Planned(plan) = decision else {
            panic!("simulated relational write should pass constraints");
        };

        runtime.write_plans += 1;
        runtime.write_steps += plan.steps.len();
        runtime.write_index_mutations += plan.index_mutations.len();
        runtime.write_conflict_ranges += plan.intent.write_conflicts.len();

        trace.push(
            "write",
            "plan",
            format!(
                "table={} primary_key=4 steps={} row_key={} row_bytes={}",
                plan.table.0,
                plan.steps.len(),
                String::from_utf8_lossy(&plan.row_key),
                plan.row_value.len()
            ),
        );
        for step in &plan.steps {
            trace.push("write", "step", write_step(step));
        }
        trace.push(
            "write",
            "intent",
            format!(
                "mutations={} read_conflicts={} write_conflicts={} index_mutations={}",
                plan.intent.mutations.len(),
                plan.intent.read_conflicts.len(),
                plan.intent.write_conflicts.len(),
                plan.index_mutations.len()
            ),
        );
    }
}

pub struct RelationalCommitService;

impl RelationalCommitService {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) {
        let table = runtime
            .catalog
            .table("app", "users")
            .cloned()
            .expect("simulated relational commit group requires users table");
        let constraints = ConstraintValidator::new(
            &table,
            vec![
                ConstraintRule::PrimaryKey {
                    columns: table.primary_key.clone(),
                },
                ConstraintRule::Unique {
                    name: "users_name".to_string(),
                    columns: vec!["name".to_string()],
                },
            ],
        )
        .expect("simulated relational commit constraints must validate");
        let planner = WritePlanner::new(
            &table,
            constraints,
            vec![IndexDescriptor {
                table: table.name.clone(),
                name: "users_name".to_string(),
                columns: vec!["name".to_string()],
                unique: true,
            }],
        )
        .expect("simulated relational commit indexes must match table");

        let insert = planned_write(
            planner
                .plan_insert(
                    &table,
                    write_request(0x55, &table.name, "5", "ada"),
                    &ConstraintState::default(),
                )
                .expect("simulated relational commit insert must plan"),
        );
        let before = RowImage {
            primary_key: "5".to_string(),
            values: BTreeMap::from([
                ("id".to_string(), "5".to_string()),
                ("name".to_string(), "ada".to_string()),
            ]),
        };
        let update = planned_write(
            planner
                .plan_update(
                    &table,
                    before.clone(),
                    write_request(0x56, &table.name, "5", "grace"),
                    &ConstraintState::default(),
                )
                .expect("simulated relational commit update must plan"),
        );
        let delete = planned_write(
            planner
                .plan_delete(&table, before, TransactionId([0x57; 16]))
                .expect("simulated relational commit delete must plan"),
        );

        let auditor = RelationalCommitAuditor;
        for (operation, plan) in [
            ("insert", &insert),
            ("update", &update),
            ("delete", &delete),
        ] {
            Self::record(trace, runtime, operation, auditor.audit(plan));
        }

        let mut partial = insert.clone();
        partial.intent.mutations.pop();
        Self::record(trace, runtime, "partial", auditor.audit(&partial));

        trace.push(
            "relcommit",
            "summary",
            format!(
                "groups={} row_mutations={} index_mutations={} conflict_ranges={} rejections={}",
                runtime.relcommit_groups,
                runtime.relcommit_row_mutations,
                runtime.relcommit_index_mutations,
                runtime.relcommit_conflict_ranges,
                runtime.relcommit_rejections
            ),
        );
    }

    fn record(
        trace: &mut Trace,
        runtime: &mut DatabaseRuntime,
        operation: &str,
        decision: CommitGroupDecision,
    ) {
        match decision {
            CommitGroupDecision::Accepted(group) => {
                runtime.relcommit_groups += 1;
                runtime.relcommit_row_mutations += group.row_mutations;
                runtime.relcommit_index_mutations += group.index_mutations;
                runtime.relcommit_conflict_ranges += group.conflict_ranges;
                trace.push(
                    "relcommit",
                    "group",
                    format!(
                        "op={} table={} row_mutations={} index_mutations={} conflict_ranges={} intent_mutations={}",
                        operation,
                        group.table,
                        group.row_mutations,
                        group.index_mutations,
                        group.conflict_ranges,
                        group.intent_mutations
                    ),
                );
            }
            CommitGroupDecision::Rejected {
                reason,
                row_mutations,
                index_mutations,
                conflict_ranges,
                intent_mutations,
            } => {
                runtime.relcommit_rejections += 1;
                trace.push(
                    "relcommit",
                    "reject",
                    format!(
                        "op={} reason={} row_mutations={} index_mutations={} conflict_ranges={} intent_mutations={}",
                        operation,
                        reason,
                        row_mutations,
                        index_mutations,
                        conflict_ranges,
                        intent_mutations
                    ),
                );
            }
        }
    }
}

pub struct PageService;

impl PageService {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) {
        let mut pages = PageServer::default();
        let page = PageId::new("users", 1);
        Self::record_decision(
            trace,
            runtime,
            pages.create_page(page.clone(), CommitVersion(30), 4096),
        );
        for redo in [
            RedoRecord::new(
                page.clone(),
                CommitVersion(30),
                CommitVersion(31),
                RedoOperation::InsertRow,
                1,
                128,
            ),
            RedoRecord::new(
                page.clone(),
                CommitVersion(31),
                CommitVersion(32),
                RedoOperation::UpdateRow,
                0,
                32,
            ),
        ] {
            trace.push(
                "page",
                "redo",
                format!(
                    "table={} page={} op={} base_lsn={} commit_lsn={} row_delta={} byte_delta={}",
                    redo.page.table,
                    redo.page.number,
                    redo.operation.as_str(),
                    redo.base_lsn.0,
                    redo.commit_lsn.0,
                    redo.row_delta,
                    redo.byte_delta
                ),
            );
            Self::record_decision(trace, runtime, pages.apply_redo(redo));
        }
        Self::record_decision(trace, runtime, pages.read_page(&page, CommitVersion(31)));
        Self::record_decision(
            trace,
            runtime,
            pages.apply_redo(RedoRecord::new(
                page.clone(),
                CommitVersion(30),
                CommitVersion(33),
                RedoOperation::DeleteRow,
                -1,
                -128,
            )),
        );
        let replay_image = PageImage::new(page.clone(), CommitVersion(30), 4096)
            .expect("simulated replay page image validates")
            .encode()
            .expect("simulated replay page image encodes");
        let replay_redo = [
            RedoRecord::insert_row(
                page.clone(),
                CommitVersion(30),
                CommitVersion(31),
                b"user/1".to_vec(),
                b"ada".to_vec(),
            ),
            RedoRecord::update_row(
                page.clone(),
                CommitVersion(31),
                CommitVersion(32),
                b"user/1".to_vec(),
                b"grace".to_vec(),
            ),
        ]
        .into_iter()
        .map(|record| record.encode().expect("simulated replay redo encodes"))
        .collect::<Vec<_>>();
        Self::record_decision(
            trace,
            runtime,
            PageServer::replay(&[replay_image], &replay_redo),
        );
        Self::record_decision(trace, runtime, pages.checkpoint());
        runtime.page_frames = pages.page_count();
        runtime.page_redo = pages.redo_count();
        trace.push(
            "page",
            "summary",
            format!(
                "frames={} redo={} reads={} rejections={} checkpoint_bytes={}",
                runtime.page_frames,
                runtime.page_redo,
                runtime.page_reads,
                runtime.page_rejections,
                runtime.page_checkpoint_bytes
            ),
        );
    }

    fn record_decision(trace: &mut Trace, runtime: &mut DatabaseRuntime, decision: PageDecision) {
        match decision {
            PageDecision::Applied(frame) => {
                trace.push("page", "apply", page_frame(&frame));
            }
            PageDecision::Read(frame) => {
                runtime.page_reads += 1;
                trace.push("page", "read", page_frame(&frame));
            }
            PageDecision::Checkpoint(checkpoint) => {
                runtime.page_checkpoint_bytes = checkpoint.bytes as usize;
                trace.push(
                    "page",
                    "checkpoint",
                    format!(
                        "pages={} low_lsn={} high_lsn={} bytes={} live_rows={} tombstones={} summary_bytes={} summaries={}",
                        checkpoint.pages,
                        checkpoint.low_lsn.0,
                        checkpoint.high_lsn.0,
                        checkpoint.bytes,
                        checkpoint.live_rows,
                        checkpoint.tombstones,
                        checkpoint.summary_bytes,
                        checkpoint.page_summaries.len()
                    ),
                );
            }
            PageDecision::Replay(report) => {
                trace.push(
                    "page",
                    "replay",
                    format!(
                        "pages={} images={} records={} applied={} skipped={} high_lsn={} bytes={}",
                        report.pages,
                        report.decoded_images,
                        report.decoded_records,
                        report.applied_records,
                        report.skipped_records,
                        report.high_lsn.0,
                        report.bytes
                    ),
                );
            }
            PageDecision::Rejected { page, reason } => {
                runtime.page_rejections += 1;
                trace.push(
                    "page",
                    "reject",
                    format!("table={} page={} reason={reason}", page.table, page.number),
                );
            }
        }
    }
}

pub struct BufferPoolService;

pub struct WalService;

impl WalService {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) {
        let policy = WalPolicy::new(1_024, true).expect("simulated wal policy must validate");
        let mut gate = WalGate::new(policy);
        let users_page = PageId::new("users", 1);

        for decision in [
            gate.append(users_page.clone(), CommitVersion(31), 128),
            gate.allow_page_flush(FlushRequest::new(
                users_page.clone(),
                CommitVersion(31),
                4_224,
            )),
            gate.append(users_page.clone(), CommitVersion(32), 32),
            gate.sync(),
            gate.allow_page_flush(FlushRequest::new(users_page, CommitVersion(32), 4_256)),
        ] {
            Self::record_decision(trace, runtime, decision);
        }
        runtime.wal_durable_lsn = gate.durable_lsn().0 as usize;
        runtime.wal_appends = gate.appended_count();
        trace.push(
            "wal",
            "summary",
            format!(
                "appends={} syncs={} flush_allows={} rejections={} durable_lsn={} bytes={}",
                runtime.wal_appends,
                runtime.wal_syncs,
                runtime.wal_flush_allows,
                runtime.wal_rejections,
                runtime.wal_durable_lsn,
                runtime.wal_bytes
            ),
        );
    }

    fn record_decision(trace: &mut Trace, runtime: &mut DatabaseRuntime, decision: WalDecision) {
        match decision {
            WalDecision::Appended(entry) => {
                runtime.wal_bytes += entry.bytes as usize;
                trace.push("wal", "append", wal_entry(&entry));
            }
            WalDecision::Synced {
                durable_lsn,
                entries,
                bytes,
                fsync,
            } => {
                runtime.wal_syncs += 1;
                runtime.wal_durable_lsn = durable_lsn.0 as usize;
                trace.push(
                    "wal",
                    "sync",
                    format!(
                        "durable_lsn={} entries={} bytes={} fsync={}",
                        durable_lsn.0, entries, bytes, fsync
                    ),
                );
            }
            WalDecision::FlushAllowed { page, lsn, bytes } => {
                runtime.wal_flush_allows += 1;
                trace.push(
                    "wal",
                    "flush-allow",
                    format!(
                        "table={} page={} page_lsn={} bytes={}",
                        page.table, page.number, lsn.0, bytes
                    ),
                );
            }
            WalDecision::Rejected { page, reason } => {
                runtime.wal_rejections += 1;
                trace.push("wal", "reject", wal_rejection(page, reason));
            }
        }
    }
}

impl BufferPoolService {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) {
        let mut pool = BufferPool::new(1).expect("simulated buffer capacity must validate");
        let mut wal = WalGate::new(WalPolicy::new(1_024, true).expect("wal policy validates"));
        let policy = DirtyFlushPolicy::new(1, 4_096, 1).expect("simulated dirty policy validates");
        let users_page = PageId::new("users", 1);
        let orders_page = PageId::new("orders", 1);
        let user_lsn = CommitVersion(31);

        let _ = wal.append(users_page.clone(), user_lsn, 128);
        let _ = wal.sync();

        for decision in [
            pool.pin(users_page.clone(), 4256),
            pool.mark_dirty_at(&users_page, user_lsn),
            pool.pin(users_page.clone(), 4256),
            pool.pin(orders_page, 4096),
            pool.unpin(&users_page),
            pool.unpin(&users_page),
        ] {
            Self::record_decision(trace, runtime, decision);
        }
        let plan = pool.dirty_flush_plan(&policy);
        trace.push(
            "buffer",
            "flush-plan",
            format!(
                "dirty_frames={} dirty_bytes={} candidates={} reason={}",
                plan.dirty_frames,
                plan.dirty_bytes,
                plan.candidates.len(),
                plan.reason
            ),
        );
        for decision in [
            pool.flush_after_wal(FlushRequest::new(users_page.clone(), user_lsn, 4256), &wal),
            pool.evict_one(),
        ] {
            Self::record_decision(trace, runtime, decision);
        }
        runtime.buffer_resident = pool.resident_count();
        trace.push(
            "buffer",
            "summary",
            format!(
                "resident={} hits={} dirty={} flushes={} evictions={} rejections={}",
                runtime.buffer_resident,
                runtime.buffer_hits,
                runtime.buffer_dirty,
                runtime.buffer_flushes,
                runtime.buffer_evictions,
                runtime.buffer_rejections
            ),
        );
    }

    fn record_decision(trace: &mut Trace, runtime: &mut DatabaseRuntime, decision: BufferDecision) {
        match decision {
            BufferDecision::Hit(frame) => {
                runtime.buffer_hits += 1;
                trace.push("buffer", "hit", buffer_frame(&frame));
            }
            BufferDecision::Loaded(frame) => {
                trace.push("buffer", "load", buffer_frame(&frame));
            }
            BufferDecision::Dirtied(frame) => {
                runtime.buffer_dirty += 1;
                trace.push("buffer", "dirty", buffer_frame(&frame));
            }
            BufferDecision::Unpinned(frame) => {
                trace.push("buffer", "unpin", buffer_frame(&frame));
            }
            BufferDecision::Flushed { page, bytes } => {
                runtime.buffer_flushes += 1;
                trace.push(
                    "buffer",
                    "flush",
                    format!("table={} page={} bytes={bytes}", page.table, page.number),
                );
            }
            BufferDecision::Evicted { page, dirty } => {
                runtime.buffer_evictions += 1;
                trace.push(
                    "buffer",
                    "evict",
                    format!("table={} page={} dirty={dirty}", page.table, page.number),
                );
            }
            BufferDecision::Rejected { page, reason } => {
                runtime.buffer_rejections += 1;
                trace.push(
                    "buffer",
                    "reject",
                    format!("table={} page={} reason={reason}", page.table, page.number),
                );
            }
        }
    }
}

pub struct Query;

impl Query {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) {
        let table = runtime
            .catalog
            .table("app", "users")
            .expect("simulated catalog table must exist");
        let plan = QueryPlanner
            .select(
                table,
                Projection::Columns(vec!["id".to_string(), "name".to_string()]),
                Some(Predicate::Eq {
                    column: "active".to_string(),
                    value: QueryValue::Bool(true),
                }),
                Some(1),
            )
            .expect("simulated query plan must validate against catalog");
        trace.push(
            "query",
            "plan",
            "SELECT id,name FROM app.users WHERE active = true LIMIT 1",
        );

        let rows = Self::row_source();
        trace.push(
            "query",
            "scan",
            format!("scanned {} simulated rows from users range", rows.len()),
        );
        let result = QueryExecutor
            .execute(&plan, &rows)
            .expect("simulated query execution must succeed");
        runtime.query_rows = result.rows.len();
        trace.push(
            "query",
            "execute",
            format!(
                "returned {} projected row with {} columns",
                result.rows.len(),
                result
                    .rows
                    .first()
                    .map(|row| row.columns.len())
                    .unwrap_or_default()
            ),
        );
    }

    fn row_source() -> Vec<QueryRow> {
        [
            (1, "ada", true),
            (2, "grace", false),
            (3, "katherine", true),
        ]
        .into_iter()
        .map(|(id, name, active)| QueryRow {
            table: TableName("users".to_string()),
            columns: std::collections::BTreeMap::from([
                ("id".to_string(), QueryValue::Int64(id)),
                ("name".to_string(), QueryValue::Text(name.to_string())),
                ("active".to_string(), QueryValue::Bool(active)),
            ]),
        })
        .collect()
    }
}

pub struct PhysicalQueryPlanner;

impl PhysicalQueryPlanner {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) {
        let table = runtime
            .catalog
            .table("app", "users")
            .cloned()
            .expect("simulated physical planner requires users table");
        let context = physical_context(&table, runtime, false);
        trace.push(
            "planner",
            "stats",
            format!(
                "table={} rows={} avg_row_bytes={} stream_threshold={} indexes=users_name(name)",
                context.stats.table.0,
                context.stats.rows,
                context.stats.average_row_bytes,
                context.stream_threshold_bytes
            ),
        );

        let cases = [
            (
                "primary-key",
                QueryPlanner
                    .select(
                        &table,
                        Projection::All,
                        Some(Predicate::Eq {
                            column: "id".to_string(),
                            value: QueryValue::Int64(1),
                        }),
                        None,
                    )
                    .expect("simulated primary-key query must validate"),
                context.clone(),
            ),
            (
                "secondary-index",
                QueryPlanner
                    .select(
                        &table,
                        Projection::Columns(vec!["id".to_string(), "name".to_string()]),
                        Some(Predicate::Eq {
                            column: "name".to_string(),
                            value: QueryValue::Text("ada".to_string()),
                        }),
                        Some(5),
                    )
                    .expect("simulated secondary-index query must validate"),
                PhysicalPlanningContext {
                    cacheable: true,
                    ..context.clone()
                },
            ),
            (
                "range-scan",
                QueryPlanner
                    .select(
                        &table,
                        Projection::Columns(vec!["id".to_string(), "active".to_string()]),
                        Some(Predicate::Eq {
                            column: "active".to_string(),
                            value: QueryValue::Bool(true),
                        }),
                        Some(8),
                    )
                    .expect("simulated range query must validate"),
                context.clone(),
            ),
            (
                "stream-read",
                QueryPlanner
                    .select(&table, Projection::All, None, Some(64))
                    .expect("simulated large query must validate"),
                context,
            ),
        ];

        let planner = PhysicalPlanner;
        for (label, logical, context) in cases {
            let plan = planner
                .plan(&table, &logical, &context)
                .expect("simulated physical plan must build");
            runtime.planner_physical_plans += 1;
            Self::record_counts(&plan, runtime);
            trace.push(
                "planner",
                label,
                format!(
                    "operators={} estimated_rows={} estimated_bytes={}",
                    Self::operator_list(&plan),
                    plan.estimated_rows,
                    plan.estimated_bytes
                ),
            );
        }
    }

    fn record_counts(plan: &PhysicalPlan, runtime: &mut DatabaseRuntime) {
        for operator in &plan.operators {
            match &operator.access {
                AccessPath::PrimaryKeyLookup { .. } | AccessPath::SecondaryIndexLookup { .. } => {
                    runtime.planner_index_choices += 1;
                }
                AccessPath::RangeScan => {
                    runtime.planner_range_scans += 1;
                }
                AccessPath::StreamRead { .. } => {
                    runtime.planner_stream_reads += 1;
                }
                AccessPath::CacheLookup => {}
            }
        }
    }

    fn operator_list(plan: &PhysicalPlan) -> String {
        plan.operators
            .iter()
            .map(|operator| match &operator.access {
                AccessPath::CacheLookup => "cache-lookup".to_string(),
                AccessPath::PrimaryKeyLookup { column } => {
                    format!("primary-key-lookup({column})")
                }
                AccessPath::SecondaryIndexLookup { index, column } => {
                    format!("secondary-index-lookup({index}.{column})")
                }
                AccessPath::RangeScan => "range-scan".to_string(),
                AccessPath::StreamRead { threshold_bytes } => {
                    format!("stream-read(threshold={threshold_bytes})")
                }
            })
            .collect::<Vec<_>>()
            .join(" -> ")
    }
}

pub struct PhysicalQueryExecutor;

impl PhysicalQueryExecutor {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) {
        let table = runtime
            .catalog
            .table("app", "users")
            .cloned()
            .expect("simulated physical executor requires users table");
        let context = physical_context(&table, runtime, true);
        let logical = QueryPlanner
            .select(
                &table,
                Projection::Columns(vec!["id".to_string(), "name".to_string()]),
                Some(Predicate::Eq {
                    column: "name".to_string(),
                    value: QueryValue::Text("ada".to_string()),
                }),
                Some(5),
            )
            .expect("simulated executor query must validate");
        let plan = PhysicalPlanner
            .plan(&table, &logical, &context)
            .expect("simulated executor physical plan must build");
        let report = PhysicalExecutor
            .execute(&plan)
            .expect("simulated executor must execute physical plan");

        runtime.executor_plans += 1;
        runtime.executor_stages += report.stages.len();
        runtime.executor_rows += report.rows as usize;
        runtime.executor_remote_reads += report.remote_reads as usize;

        trace.push(
            "executor",
            "start",
            format!(
                "table={} operators={} planned_rows={} planned_bytes={}",
                plan.table.0,
                plan.operators.len(),
                plan.estimated_rows,
                plan.estimated_bytes
            ),
        );
        for (idx, stage) in report.stages.iter().enumerate() {
            trace.push(
                "executor",
                "stage",
                format!(
                    "{} source={} rows={} bytes={}",
                    idx + 1,
                    Self::source_label(&stage.source),
                    stage.rows,
                    stage.bytes
                ),
            );
        }
        trace.push(
            "executor",
            "finish",
            format!(
                "rows={} bytes={} remote_reads={}",
                report.rows, report.bytes, report.remote_reads
            ),
        );
    }

    fn source_label(source: &ExecutionSource) -> &'static str {
        match source {
            ExecutionSource::Cache => "cache",
            ExecutionSource::PrimaryIndex => "primary-index",
            ExecutionSource::SecondaryIndex => "secondary-index",
            ExecutionSource::Range => "range",
            ExecutionSource::Stream => "stream",
        }
    }
}

pub struct SqlPipelineService;

impl SqlPipelineService {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) {
        let table = runtime
            .catalog
            .table("app", "users")
            .cloned()
            .expect("simulated SQL pipeline requires users table");
        let context = physical_context(&table, runtime, true);
        let token = runtime
            .app_token
            .clone()
            .expect("simulated SQL pipeline requires app token");
        let pipeline = SqlPipeline;

        let request = Self::request(
            "SELECT id, name FROM users WHERE name = 'ada' LIMIT 1",
            context.clone(),
        );
        let (report, decisions) = pipeline
            .execute(&runtime.catalog, &runtime.auth, &token, request)
            .expect("simulated SQL pipeline query must execute");

        for decision in decisions {
            Self::record_decision(trace, runtime, decision);
        }
        runtime.sql_pipeline_stages += report.stage_count();
        runtime.sql_pipeline_rows += report.row_count();
        runtime.sql_pipeline_remote_reads += report.remote_reads() as usize;

        let rejected = pipeline.reject(
            &runtime.catalog,
            &runtime.auth,
            &token,
            Self::request("SELECT missing FROM users WHERE id = 1", context),
        );
        Self::record_decision(trace, runtime, rejected);

        trace.push(
            "sqlpipeline",
            "summary",
            format!(
                "bound={} authorized={} logical={} physical={} stages={} rows={} remote_reads={} rejections={}",
                runtime.sql_pipeline_bound,
                runtime.sql_pipeline_authorized,
                runtime.sql_pipeline_logical,
                runtime.sql_pipeline_physical,
                runtime.sql_pipeline_stages,
                runtime.sql_pipeline_rows,
                runtime.sql_pipeline_remote_reads,
                runtime.sql_pipeline_rejections
            ),
        );
    }

    fn request(statement: &str, context: PhysicalPlanningContext) -> SqlPipelineRequest {
        SqlPipelineRequest {
            context: SessionContext::new(
                "session-sqlpipeline",
                PrincipalId("app-client".into()),
                "app",
            ),
            principal: PrincipalId("app-client".to_string()),
            statement: statement.to_string(),
            snapshot: CommitVersion(22),
            catalog_group: PlacementGroupId(1),
            now: CommitVersion(22),
            stats: context.stats,
            indexes: context.indexes,
            cacheable: context.cacheable,
            stream_threshold_bytes: context.stream_threshold_bytes,
            rows: Query::row_source(),
            transaction_id: TransactionId([0x71; 16]),
            constraints: vec![ConstraintRule::PrimaryKey {
                columns: vec!["id".to_string()],
            }],
            constraint_state: ConstraintState::default(),
        }
    }

    fn record_decision(
        trace: &mut Trace,
        runtime: &mut DatabaseRuntime,
        decision: SqlPipelineDecision,
    ) {
        match decision {
            SqlPipelineDecision::Bound {
                table,
                class,
                placement_group,
            } => {
                runtime.sql_pipeline_bound += 1;
                trace.push(
                    "sqlpipeline",
                    "bind",
                    format!(
                        "table={} class={} pg={}",
                        table.0,
                        class.as_str(),
                        placement_group.0
                    ),
                );
            }
            SqlPipelineDecision::Authorized { principal } => {
                runtime.sql_pipeline_authorized += 1;
                trace.push(
                    "sqlpipeline",
                    "authorize",
                    format!("principal={} allowed=true", principal.0),
                );
            }
            SqlPipelineDecision::LogicalPlanned {
                table,
                projection,
                has_predicate,
                limit,
            } => {
                runtime.sql_pipeline_logical += 1;
                trace.push(
                    "sqlpipeline",
                    "logical-plan",
                    format!(
                        "table={} projection={} predicate={} limit={}",
                        table.0,
                        projection,
                        has_predicate,
                        limit.map_or("none".to_string(), |limit| limit.to_string())
                    ),
                );
            }
            SqlPipelineDecision::PhysicalPlanned {
                table,
                operators,
                estimated_rows,
                estimated_bytes,
            } => {
                runtime.sql_pipeline_physical += 1;
                trace.push(
                    "sqlpipeline",
                    "physical-plan",
                    format!(
                        "table={} operators={} estimated_rows={} estimated_bytes={}",
                        table.0, operators, estimated_rows, estimated_bytes
                    ),
                );
            }
            SqlPipelineDecision::Executed {
                stages,
                rows,
                bytes,
                remote_reads,
            } => {
                trace.push(
                    "sqlpipeline",
                    "execute",
                    format!(
                        "stages={} rows={} bytes={} remote_reads={}",
                        stages, rows, bytes, remote_reads
                    ),
                );
            }
            SqlPipelineDecision::Dml { decision } => {
                trace.push(
                    "sqlpipeline",
                    "dml",
                    format!("statement-level DML decision={decision:?}"),
                );
            }
            SqlPipelineDecision::Rejected { reason } => {
                runtime.sql_pipeline_rejections += 1;
                trace.push("sqlpipeline", "reject", reason);
            }
        }
    }
}

fn physical_context(
    table: &TableDescriptor,
    runtime: &DatabaseRuntime,
    cacheable: bool,
) -> PhysicalPlanningContext {
    let estimate = runtime
        .stats
        .estimate(&table.name)
        .expect("simulated physical planning requires published table statistics");
    PhysicalPlanningContext {
        stats: TableStats {
            table: estimate.table.clone(),
            rows: estimate.rows,
            average_row_bytes: estimate.average_row_bytes(),
        },
        indexes: vec![IndexDescriptor {
            table: table.name.clone(),
            name: "users_name".to_string(),
            columns: vec!["name".to_string()],
            unique: false,
        }],
        stream_threshold_bytes: 8_192,
        cacheable,
    }
}

pub struct ObjectMetadataService;

pub struct SqlDmlService;

pub struct SqlTransactionService;

impl SqlDmlService {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) {
        let table = runtime
            .catalog
            .table("app", "users")
            .cloned()
            .expect("simulated SQL DML pipeline requires users table");
        let token = runtime
            .app_token
            .clone()
            .expect("simulated SQL DML pipeline requires app token");
        let mut isolation = IsolationTracker::default();
        let pipeline = SqlDmlPipeline;
        let index = IndexDescriptor {
            table: table.name.clone(),
            name: "users_name".to_string(),
            columns: vec!["name".to_string()],
            unique: true,
        };
        let mut rows = BTreeMap::new();

        Self::execute(
            trace,
            runtime,
            &mut isolation,
            &pipeline,
            &token,
            Self::request(
                "INSERT INTO users (id, name, active) VALUES (7, 'grace', true)",
                vec![index.clone()],
                ConstraintState::default(),
                CommitVersion(runtime.router.latest_version().0),
                TransactionId([0x55; 16]),
                rows.clone(),
            ),
        );
        rows.insert(
            "7".to_string(),
            BTreeMap::from([
                ("id".to_string(), "7".to_string()),
                ("name".to_string(), "grace".to_string()),
                ("active".to_string(), "true".to_string()),
            ]),
        );

        Self::execute(
            trace,
            runtime,
            &mut isolation,
            &pipeline,
            &token,
            Self::request(
                "UPDATE users SET name = 'hopper', active = false WHERE id = 7",
                vec![index.clone()],
                ConstraintState::default(),
                CommitVersion(runtime.router.latest_version().0),
                TransactionId([0x56; 16]),
                rows.clone(),
            ),
        );
        rows.insert(
            "7".to_string(),
            BTreeMap::from([
                ("id".to_string(), "7".to_string()),
                ("name".to_string(), "hopper".to_string()),
                ("active".to_string(), "false".to_string()),
            ]),
        );

        Self::execute(
            trace,
            runtime,
            &mut isolation,
            &pipeline,
            &token,
            Self::request(
                "DELETE FROM users WHERE id = 7",
                vec![index.clone()],
                ConstraintState::default(),
                CommitVersion(runtime.router.latest_version().0),
                TransactionId([0x57; 16]),
                rows.clone(),
            ),
        );
        rows.clear();

        runtime
            .session_transactions
            .begin("session-sqldml", &runtime.kv, &runtime.router)
            .expect("simulated SQL DML session transaction must begin");
        let (_report, decisions) = pipeline
            .stage_in_session_transaction(
                &runtime.catalog,
                &runtime.auth,
                &token,
                &mut runtime.session_transactions,
                &runtime.kv,
                Self::request(
                    "INSERT INTO users (id, name, active) VALUES (9, 'lovelace', true)",
                    vec![index.clone()],
                    ConstraintState::default(),
                    CommitVersion(runtime.router.latest_version().0),
                    TransactionId([0x59; 16]),
                    rows.clone(),
                ),
            )
            .expect("simulated SQL DML session transaction staging must succeed");
        for decision in decisions {
            Self::record_decision(trace, runtime, decision);
        }
        let session_commit = runtime
            .session_transactions
            .commit(
                "session-sqldml",
                &mut runtime.kv,
                &mut runtime.router,
                PlacementGroupId(1),
            )
            .expect("simulated SQL DML session transaction commit must succeed");
        trace.push(
            "sqldml",
            "session-commit",
            session_txn_decision(session_commit),
        );

        let mut rejected_state = ConstraintState::default();
        rejected_state.add_unique_key("users_name", ["grace"]);
        let rejected = pipeline.reject(
            &runtime.catalog,
            &runtime.auth,
            &token,
            Self::request(
                "INSERT INTO users (id, name, active) VALUES (8, 'grace', true)",
                vec![index],
                rejected_state,
                CommitVersion(runtime.router.latest_version().0),
                TransactionId([0x58; 16]),
                rows,
            ),
        );
        Self::record_decision(trace, runtime, rejected);

        trace.push(
            "sqldml",
            "summary",
            format!(
                "bound={} authorized={} planned={} prepared={} committed={} isolated={} kv_applied={} session_staged={} overlay_verified={} rejections={}",
                runtime.sql_dml_bound,
                runtime.sql_dml_authorized,
                runtime.sql_dml_planned,
                runtime.sql_dml_prepared,
                runtime.sql_dml_committed,
                runtime.sql_dml_isolated,
                runtime.sql_dml_kv_applied,
                runtime.sql_dml_session_staged,
                runtime.sql_dml_overlay_verified,
                runtime.sql_dml_rejections
            ),
        );
    }

    fn request(
        statement: &str,
        indexes: Vec<IndexDescriptor>,
        constraint_state: ConstraintState,
        snapshot: CommitVersion,
        transaction_id: TransactionId,
        rows: BTreeMap<String, BTreeMap<String, String>>,
    ) -> SqlDmlPipelineRequest {
        SqlDmlPipelineRequest {
            context: SessionContext::new(
                "session-sqldml",
                PrincipalId("app-client".to_string()),
                "app",
            ),
            principal: PrincipalId("app-client".to_string()),
            statement: statement.to_string(),
            snapshot,
            catalog_group: PlacementGroupId(1),
            now: CommitVersion(snapshot.0 + 1),
            transaction_id,
            constraints: vec![
                ConstraintRule::PrimaryKey {
                    columns: vec!["id".to_string()],
                },
                ConstraintRule::Unique {
                    name: "users_name".to_string(),
                    columns: vec!["name".to_string()],
                },
            ],
            constraint_state,
            indexes,
            rows,
        }
    }

    fn execute(
        trace: &mut Trace,
        runtime: &mut DatabaseRuntime,
        isolation: &mut IsolationTracker,
        pipeline: &SqlDmlPipeline,
        token: &AuthToken,
        request: SqlDmlPipelineRequest,
    ) {
        let (_report, decisions) = pipeline
            .execute(
                &runtime.catalog,
                &runtime.auth,
                token,
                &mut runtime.transactions,
                isolation,
                &mut runtime.kv,
                &mut runtime.router,
                request,
            )
            .expect("simulated SQL DML pipeline operation must commit");

        for decision in decisions {
            Self::record_decision(trace, runtime, decision);
        }
    }

    fn record_decision(
        trace: &mut Trace,
        runtime: &mut DatabaseRuntime,
        decision: SqlDmlPipelineDecision,
    ) {
        match decision {
            SqlDmlPipelineDecision::Bound {
                table,
                placement_group,
            } => {
                runtime.sql_dml_bound += 1;
                trace.push(
                    "sqldml",
                    "bind",
                    format!("table={} pg={}", table.0, placement_group.0),
                );
            }
            SqlDmlPipelineDecision::Authorized { principal } => {
                runtime.sql_dml_authorized += 1;
                trace.push(
                    "sqldml",
                    "authorize",
                    format!("principal={} allowed=true", principal.0),
                );
            }
            SqlDmlPipelineDecision::WritePlanned {
                operation,
                table,
                steps,
                mutations,
                write_conflicts,
            } => {
                runtime.sql_dml_planned += 1;
                trace.push(
                    "sqldml",
                    "write-plan",
                    format!(
                        "operation={} table={} steps={} mutations={} write_conflicts={}",
                        operation.as_str(),
                        table.0,
                        steps,
                        mutations,
                        write_conflicts
                    ),
                );
            }
            SqlDmlPipelineDecision::CommitGroupAccepted {
                table,
                row_mutations,
                index_mutations,
                conflict_ranges,
                intent_mutations,
            } => {
                runtime.relcommit_groups += 1;
                runtime.relcommit_row_mutations += row_mutations;
                runtime.relcommit_index_mutations += index_mutations;
                runtime.relcommit_conflict_ranges += conflict_ranges;
                trace.push(
                    "sqldml",
                    "commit-group",
                    format!(
                        "table={} row_mutations={} index_mutations={} conflict_ranges={} intent_mutations={}",
                        table,
                        row_mutations,
                        index_mutations,
                        conflict_ranges,
                        intent_mutations
                    ),
                );
            }
            SqlDmlPipelineDecision::TransactionPrepared {
                transaction,
                read_version,
                read_conflicts,
                write_conflicts,
            } => {
                runtime.sql_dml_prepared += 1;
                trace.push(
                    "sqldml",
                    "prepare",
                    format!(
                        "txn={} read_version={} read_conflicts={} write_conflicts={}",
                        txn_label(&transaction),
                        read_version.0,
                        read_conflicts,
                        write_conflicts
                    ),
                );
            }
            SqlDmlPipelineDecision::TransactionCommitted {
                transaction,
                commit_version,
                evidence,
            } => {
                runtime.sql_dml_committed += 1;
                trace.push(
                    "sqldml",
                    "commit",
                    format!(
                        "txn={} version={} pg={} log_index={} consistency={:?} op={}",
                        txn_label(&transaction),
                        commit_version.0,
                        evidence.placement_group.0,
                        evidence.log_index.0,
                        evidence.consistency,
                        evidence.operation_id.0
                    ),
                );
            }
            SqlDmlPipelineDecision::IsolationAccepted {
                transaction,
                ranges,
            } => {
                runtime.sql_dml_isolated += 1;
                trace.push(
                    "sqldml",
                    "isolation",
                    format!(
                        "txn={} serializable_ranges={}",
                        txn_label(&transaction),
                        ranges
                    ),
                );
            }
            SqlDmlPipelineDecision::KvApplied {
                commit_version,
                mutations,
            } => {
                runtime.sql_dml_kv_applied += 1;
                trace.push(
                    "sqldml",
                    "kv-apply",
                    format!("version={} mutations={}", commit_version.0, mutations),
                );
            }
            SqlDmlPipelineDecision::SessionStaged {
                session_id,
                mutations,
            } => {
                runtime.sql_dml_session_staged += 1;
                trace.push(
                    "sqldml",
                    "session-stage",
                    format!("session={} mutations={}", session_id, mutations),
                );
            }
            SqlDmlPipelineDecision::SessionOverlayVerified { session_id, reads } => {
                runtime.sql_dml_overlay_verified += reads;
                trace.push(
                    "sqldml",
                    "session-overlay",
                    format!("session={} overlay_reads={}", session_id, reads),
                );
            }
            SqlDmlPipelineDecision::Rejected { reason } => {
                runtime.sql_dml_rejections += 1;
                trace.push("sqldml", "reject", reason);
            }
        }
    }
}

impl SqlTransactionService {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) {
        let table = runtime
            .catalog
            .table("app", "users")
            .cloned()
            .expect("simulated SQL transaction service requires users table");
        let token = runtime
            .app_token
            .clone()
            .expect("simulated SQL transaction service requires app token");
        let engine = SqlTransactionScriptEngine::default();
        let request = Self::request(
            "session-sqltxn-commit",
            vec![
                "BEGIN",
                "INSERT INTO users (id, name, active) VALUES (10, 'turing', true)",
                "SELECT * FROM users WHERE id = 10",
                "COMMIT",
            ],
            IndexDescriptor {
                table: table.name.clone(),
                name: "users_name".to_string(),
                columns: vec!["name".to_string()],
                unique: true,
            },
        );

        let (_report, decisions) = engine
            .execute(
                &runtime.catalog,
                &runtime.auth,
                &token,
                &mut runtime.session_transactions,
                &mut runtime.kv,
                &mut runtime.router,
                request,
            )
            .expect("simulated SQL transaction script must commit");
        for decision in decisions {
            Self::record_decision(trace, runtime, decision);
        }

        let rollback_request = Self::request(
            "session-sqltxn-rollback",
            vec![
                "BEGIN",
                "INSERT INTO users (id, name, active) VALUES (11, 'lamport', true)",
                "SELECT * FROM users WHERE id = 11",
                "ROLLBACK",
            ],
            IndexDescriptor {
                table: table.name.clone(),
                name: "users_name".to_string(),
                columns: vec!["name".to_string()],
                unique: true,
            },
        );
        let (_report, rollback_decisions) = engine
            .execute(
                &runtime.catalog,
                &runtime.auth,
                &token,
                &mut runtime.session_transactions,
                &mut runtime.kv,
                &mut runtime.router,
                rollback_request,
            )
            .expect("simulated SQL transaction rollback script must abort cleanly");
        for decision in rollback_decisions {
            Self::record_decision(trace, runtime, decision);
        }

        let savepoint_request = Self::request(
            "session-sqltxn-savepoint",
            vec![
                "BEGIN",
                "INSERT INTO users (id, name, active) VALUES (12, 'stonebraker', true)",
                "SAVEPOINT after_first",
                "INSERT INTO users (id, name, active) VALUES (13, 'gray', true)",
                "SELECT * FROM users WHERE id = 13",
                "ROLLBACK TO SAVEPOINT after_first",
                "COMMIT",
            ],
            IndexDescriptor {
                table: table.name.clone(),
                name: "users_name".to_string(),
                columns: vec!["name".to_string()],
                unique: true,
            },
        );
        let (_report, savepoint_decisions) = engine
            .execute(
                &runtime.catalog,
                &runtime.auth,
                &token,
                &mut runtime.session_transactions,
                &mut runtime.kv,
                &mut runtime.router,
                savepoint_request,
            )
            .expect("simulated SQL transaction savepoint script must rewind and commit");
        for decision in savepoint_decisions {
            Self::record_decision(trace, runtime, decision);
        }

        let rejected = engine.reject(
            &runtime.catalog,
            &runtime.auth,
            &token,
            &mut runtime.kv,
            &mut runtime.router,
            Self::request(
                "session-sqltxn-reject",
                vec!["COMMIT"],
                IndexDescriptor {
                    table: table.name,
                    name: "users_name".to_string(),
                    columns: vec!["name".to_string()],
                    unique: true,
                },
            ),
        );
        Self::record_decision(trace, runtime, rejected);

        Self::run_engine_lifecycle(trace, runtime);

        trace.push(
            "sqltxn",
            "summary",
            format!(
                "began={} dml_staged={} overlay_reads={} commits={} rollbacks={} rollback_absent={} savepoints={} savepoint_rewinds={} savepoint_absent={} rejections={}",
                runtime.sql_txn_began,
                runtime.sql_txn_dml_staged,
                runtime.sql_txn_overlay_reads,
                runtime.sql_txn_commits,
                runtime.sql_txn_rollbacks,
                runtime.sql_txn_rollback_absent,
                runtime.sql_txn_savepoints,
                runtime.sql_txn_savepoint_rewinds,
                runtime.sql_txn_savepoint_absent,
                runtime.sql_txn_rejections
            ),
        );
    }

    fn run_engine_lifecycle(trace: &mut Trace, runtime: &mut DatabaseRuntime) {
        let begin = runtime
            .sql
            .execute(&mut runtime.router, PlacementGroupId(1), "BEGIN")
            .expect("engine SQL transaction must begin");
        Self::record_engine_outcome(trace, "begin", begin);
        let staged = runtime
            .sql
            .execute(
                &mut runtime.router,
                PlacementGroupId(1),
                "INSERT INTO users(id, name) VALUES (20, 'bernstein')",
            )
            .expect("engine SQL transaction must stage insert");
        Self::record_engine_outcome(trace, "stage", staged);
        let selected = runtime
            .sql
            .execute(
                &mut runtime.router,
                PlacementGroupId(1),
                "SELECT * FROM users WHERE id = 20",
            )
            .expect("engine SQL transaction must read staged row");
        Self::record_engine_outcome(trace, "overlay-read", selected);
        let rollback = runtime
            .sql
            .execute(&mut runtime.router, PlacementGroupId(1), "ROLLBACK")
            .expect("engine SQL transaction must roll back");
        Self::record_engine_outcome(trace, "rollback", rollback);

        for statement in [
            "BEGIN",
            "INSERT INTO users(id, name) VALUES (21, 'boyce')",
            "SAVEPOINT after_insert",
            "UPDATE users SET name = 'codd' WHERE id = 21",
            "ROLLBACK TO SAVEPOINT after_insert",
            "COMMIT",
            "DELETE FROM users WHERE id = 21",
        ] {
            let outcome = runtime
                .sql
                .execute(&mut runtime.router, PlacementGroupId(1), statement)
                .expect("engine SQL transaction lifecycle statement must execute");
            Self::record_engine_outcome(trace, "lifecycle", outcome);
        }
        let report = runtime.sql.page_wal_report();
        trace.push(
            "sqltxn-engine",
            "page-wal",
            format!(
                "pages={} redo={} wal_entries={} durable_lsn={} checkpoint_lsn={} checkpoint_bytes={} buffer_resident={} ordered_kv_tables={} page_wal_tables={}",
                report.pages,
                report.page_redo,
                report.wal_entries,
                report.durable_lsn.0,
                report.checkpoint_lsn.0,
                report.checkpoint_bytes,
                report.buffer_resident,
                report.ordered_kv_tables,
                report.page_wal_tables
            ),
        );
    }

    fn record_engine_outcome(trace: &mut Trace, phase: &str, outcome: SqlOutcome) {
        match outcome {
            SqlOutcome::TransactionBegan {
                session_id,
                snapshot,
            } => trace.push(
                "sqltxn-engine",
                phase,
                format!("began session={session_id} snapshot={}", snapshot.0),
            ),
            SqlOutcome::Inserted { row, evidence } => trace.push(
                "sqltxn-engine",
                phase,
                format!(
                    "staged insert table={} key={} visible_version={}",
                    row.table,
                    row.primary_key,
                    evidence.version().0
                ),
            ),
            SqlOutcome::Updated { row, evidence } => trace.push(
                "sqltxn-engine",
                phase,
                format!(
                    "staged update table={} key={} visible_version={} name={}",
                    row.table,
                    row.primary_key,
                    evidence.version().0,
                    row.columns["name"]
                ),
            ),
            SqlOutcome::Deleted {
                table,
                primary_key,
                evidence,
            } => trace.push(
                "sqltxn-engine",
                phase,
                format!(
                    "deleted table={table} key={primary_key} version={}",
                    evidence.version().0
                ),
            ),
            SqlOutcome::Selected { rows } => trace.push(
                "sqltxn-engine",
                phase,
                format!("selected rows={}", rows.len()),
            ),
            SqlOutcome::SavepointCreated {
                session_id,
                name,
                staged,
            } => trace.push(
                "sqltxn-engine",
                phase,
                format!("savepoint session={session_id} name={name} staged={staged}"),
            ),
            SqlOutcome::SavepointRolledBack {
                session_id,
                name,
                staged,
            } => trace.push(
                "sqltxn-engine",
                phase,
                format!("rewound session={session_id} name={name} staged={staged}"),
            ),
            SqlOutcome::TransactionCommitted {
                session_id,
                evidence,
                mutations,
            } => trace.push(
                "sqltxn-engine",
                phase,
                format!(
                    "committed session={session_id} mutations={mutations} version={} pg={} log_index={}",
                    evidence.version.0, evidence.placement_group.0, evidence.log_index.0
                ),
            ),
            SqlOutcome::TransactionRolledBack {
                session_id,
                discarded,
            } => trace.push(
                "sqltxn-engine",
                phase,
                format!("rolled_back session={session_id} discarded={discarded}"),
            ),
            SqlOutcome::TableCreated { .. } | SqlOutcome::IndexCreated { .. } => {}
        }
    }

    fn request(
        session_id: &str,
        statements: Vec<&str>,
        index: IndexDescriptor,
    ) -> SqlTransactionScriptRequest {
        SqlTransactionScriptRequest {
            context: SessionContext::new(session_id, PrincipalId("app-client".to_string()), "app"),
            principal: PrincipalId("app-client".to_string()),
            catalog_group: PlacementGroupId(1),
            transaction_group: PlacementGroupId(1),
            now: CommitVersion(1),
            statements: statements.into_iter().map(str::to_string).collect(),
            constraints: vec![
                ConstraintRule::PrimaryKey {
                    columns: vec!["id".to_string()],
                },
                ConstraintRule::Unique {
                    name: "users_name".to_string(),
                    columns: vec!["name".to_string()],
                },
            ],
            constraint_state: ConstraintState::default(),
            indexes: vec![index],
            rows: BTreeMap::new(),
        }
    }

    fn record_decision(
        trace: &mut Trace,
        runtime: &mut DatabaseRuntime,
        decision: SqlTransactionDecision,
    ) {
        match decision {
            SqlTransactionDecision::Began {
                session_id,
                snapshot,
            } => {
                runtime.sql_txn_began += 1;
                trace.push(
                    "sqltxn",
                    "begin",
                    format!(
                        "session={} began SQL transaction at snapshot {}",
                        session_id, snapshot.0
                    ),
                );
            }
            SqlTransactionDecision::DmlStaged {
                statement,
                mutations,
            } => {
                runtime.sql_txn_dml_staged += 1;
                trace.push(
                    "sqltxn",
                    "dml-stage",
                    format!("statement=\"{}\" mutations={}", statement, mutations),
                );
            }
            SqlTransactionDecision::SavepointCreated {
                session_id,
                name,
                pending_writes,
            } => {
                runtime.sql_txn_savepoints += 1;
                trace.push(
                    "sqltxn",
                    "savepoint",
                    format!(
                        "session={} savepoint={} pending_writes={}",
                        session_id, name, pending_writes
                    ),
                );
            }
            SqlTransactionDecision::SavepointRolledBack {
                session_id,
                name,
                rewound_writes,
                pending_writes,
            } => {
                runtime.sql_txn_savepoint_rewinds += 1;
                trace.push(
                    "sqltxn",
                    "savepoint-rewind",
                    format!(
                        "session={} savepoint={} rewound_writes={} pending_writes={}",
                        session_id, name, rewound_writes, pending_writes
                    ),
                );
            }
            SqlTransactionDecision::SavepointAbsent { name, key } => {
                runtime.sql_txn_savepoint_absent += 1;
                trace.push(
                    "sqltxn",
                    "savepoint-absent",
                    format!(
                        "savepoint={} key={} overlay_visible=false globally_visible=false",
                        name,
                        bytes_label(&key)
                    ),
                );
            }
            SqlTransactionDecision::DeferredObligationStaged {
                transaction,
                rule,
                key,
            } => {
                runtime.deferred_obligations += 1;
                trace.push(
                    "sqltxn",
                    "deferred-obligation",
                    format!(
                        "txn={} rule={} key={}",
                        txn_label(&transaction),
                        rule,
                        key.join("/")
                    ),
                );
            }
            SqlTransactionDecision::DeferredReferenceStaged {
                transaction,
                reference,
                key,
            } => {
                runtime.deferred_txn_references += 1;
                trace.push(
                    "sqltxn",
                    "deferred-reference",
                    format!(
                        "txn={} reference={} key={}",
                        txn_label(&transaction),
                        reference,
                        key.join("/")
                    ),
                );
            }
            SqlTransactionDecision::DeferredConstraintsAccepted {
                transaction,
                obligations,
                transaction_references,
                committed_references,
            } => {
                runtime.deferred_commits += 1;
                runtime.deferred_committed_references += committed_references;
                trace.push(
                    "sqltxn",
                    "deferred-commit",
                    format!(
                        "txn={} obligations={} txn_references={} committed_references={}",
                        txn_label(&transaction),
                        obligations,
                        transaction_references,
                        committed_references
                    ),
                );
            }
            SqlTransactionDecision::DeferredConstraintsRejected {
                transaction,
                violations,
            } => {
                runtime.deferred_rejections += 1;
                trace.push(
                    "sqltxn",
                    "deferred-reject",
                    format!("txn={} violations={}", txn_label(&transaction), violations),
                );
            }
            SqlTransactionDecision::OverlayRead {
                statement,
                key,
                value,
            } => {
                runtime.sql_txn_overlay_reads += 1;
                trace.push(
                    "sqltxn",
                    "read-overlay",
                    format!(
                        "statement=\"{}\" key={} value_present={}",
                        statement,
                        bytes_label(&key),
                        value.is_some()
                    ),
                );
            }
            SqlTransactionDecision::SnapshotRead {
                statement,
                key,
                value,
                version,
            } => {
                trace.push(
                    "sqltxn",
                    "read-snapshot",
                    format!(
                        "statement=\"{}\" key={} version={} value_present={}",
                        statement,
                        bytes_label(&key),
                        version.map_or("none".to_string(), |version| version.0.to_string()),
                        value.is_some()
                    ),
                );
            }
            SqlTransactionDecision::Committed {
                session_id,
                version,
                evidence,
                writes,
            } => {
                runtime.sql_txn_commits += 1;
                trace.push(
                    "sqltxn",
                    "commit",
                    format!(
                        "session={} committed SQL transaction at version {} writes={} pg={} log_index={} op={}",
                        session_id,
                        version.0,
                        writes,
                        evidence.placement_group.0,
                        evidence.log_index.0,
                        evidence.operation_id.0
                    ),
                );
            }
            SqlTransactionDecision::RolledBack {
                session_id,
                discarded_writes,
            } => {
                runtime.sql_txn_rollbacks += 1;
                trace.push(
                    "sqltxn",
                    "rollback",
                    format!(
                        "session={} rolled back SQL transaction discarded_writes={}",
                        session_id, discarded_writes
                    ),
                );
            }
            SqlTransactionDecision::RollbackAbsent { key } => {
                runtime.sql_txn_rollback_absent += 1;
                trace.push(
                    "sqltxn",
                    "rollback-absent",
                    format!("key={} globally_visible=false", bytes_label(&key)),
                );
            }
            SqlTransactionDecision::Rejected { reason } => {
                runtime.sql_txn_rejections += 1;
                trace.push("sqltxn", "reject", reason);
            }
        }
    }
}

impl ObjectMetadataService {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) {
        let bucket = runtime
            .object_metadata
            .register_bucket(&mut runtime.router, PlacementGroupId(2), "media")
            .expect("simulated object metadata bucket registration must commit");
        trace.push("objectmeta", "bucket", object_metadata_decision(bucket));
        let bucket_record = runtime
            .object_metadata
            .bucket("media")
            .expect("simulated object metadata bucket catalog must include media");
        trace.push(
            "objectmeta",
            "bucket-catalog",
            format!(
                "bucket={} version={} pg={} log_index={} catalog_size={}",
                bucket_record.name,
                bucket_record.version.0,
                bucket_record.evidence.placement_group.0,
                bucket_record.evidence.log_index.0,
                runtime.object_metadata.bucket_catalog().len()
            ),
        );

        let upload = runtime
            .object_metadata
            .start_multipart(
                &mut runtime.router,
                PlacementGroupId(2),
                "media",
                "hero.png",
            )
            .expect("simulated object metadata upload must start for registered bucket");
        let upload_id = upload
            .upload_id()
            .expect("multipart-start decision must carry upload id");
        trace.push(
            "objectmeta",
            "multipart-start",
            object_metadata_decision(upload),
        );

        for part in [
            ObjectPartRef {
                number: 1,
                digest: Digest::blake3([11; 32])
                    .expect("simulated object part digest must be non-empty"),
                bytes: 10,
            },
            ObjectPartRef {
                number: 2,
                digest: Digest::blake3([12; 32])
                    .expect("simulated object part digest must be non-empty"),
                bytes: 11,
            },
        ] {
            let accepted = runtime
                .object_metadata
                .accept_part(&mut runtime.router, PlacementGroupId(2), upload_id, part)
                .expect("simulated object metadata must accept unique one-based parts");
            trace.push("objectmeta", "part", object_metadata_decision(accepted));
        }

        let committed = runtime
            .object_metadata
            .commit_head(
                &mut runtime.router,
                PlacementGroupId(2),
                upload_id,
                Digest::blake3([42; 32])
                    .expect("simulated object manifest digest must be non-empty"),
            )
            .expect("simulated object metadata must commit a visible object head");
        trace.push(
            "objectmeta",
            "head-commit",
            object_metadata_decision(committed),
        );
        let session = runtime
            .object_metadata
            .multipart_session(upload_id)
            .expect("committed multipart session must remain auditable");
        trace.push(
            "objectmeta",
            "multipart-state",
            format!(
                "upload_id={} state={:?} parts={} completed_at={}",
                session.upload_id,
                session.state,
                session.parts.len(),
                session
                    .completed_at
                    .map(|version| version.0.to_string())
                    .unwrap_or_else(|| "none".to_string())
            ),
        );

        let (head, read) = runtime
            .object_metadata
            .read_head("media", "hero.png")
            .expect("simulated object metadata head must be visible after commit");
        trace.push("objectmeta", "head-read", object_metadata_decision(read));
        trace.push(
            "objectmeta",
            "visibility",
            format!(
                "current head media/hero.png version={} owner_pg={} owner_epoch={} parts={} manifest_digest_prefix={:02x}",
                head.version.0,
                head.owner_pg.0,
                head.owner_epoch.0,
                head.part_count,
                head.manifest_digest.as_bytes()[0]
            ),
        );
        let moved = runtime
            .object_metadata
            .transfer_head_ownership(
                &mut runtime.router,
                PlacementGroupId(2),
                "media",
                "hero.png",
                head.version,
                head.owner_epoch,
                PlacementGroupId(3),
                Epoch(head.owner_epoch.0 + 1),
            )
            .expect("simulated object metadata ownership transfer must commit");
        trace.push(
            "objectmeta",
            "head-owner-transfer",
            object_metadata_decision(moved),
        );
        let stale_move = runtime
            .object_metadata
            .transfer_head_ownership(
                &mut runtime.router,
                PlacementGroupId(2),
                "media",
                "hero.png",
                head.version,
                head.owner_epoch,
                PlacementGroupId(4),
                Epoch(head.owner_epoch.0 + 2),
            )
            .expect_err("simulated object metadata stale ownership transfer must reject");
        trace.push(
            "objectmeta",
            "head-owner-reject",
            format!("stale ownership transfer rejected: {}", stale_move.message),
        );

        let rejection = runtime
            .object_metadata
            .commit_head(
                &mut runtime.router,
                PlacementGroupId(2),
                999,
                Digest::blake3([1; 32])
                    .expect("simulated object manifest digest must be non-empty"),
            )
            .expect_err("simulated object metadata must reject an unknown upload commit");
        trace.push(
            "objectmeta",
            "reject",
            format!("unknown upload rejected: {}", rejection.message),
        );
    }
}

pub struct ObjectStore;

impl ObjectStore {
    pub fn run(trace: &mut Trace, implementation: &Implementation, runtime: &mut DatabaseRuntime) {
        trace.push("object", "metadata", implementation.database.object);
        let bucket = runtime
            .objects
            .create_bucket(&mut runtime.router, PlacementGroupId(2), "media")
            .expect("simulated bucket create must commit through protocol router");
        trace.push(
            "object",
            "bucket",
            format!(
                "media bucket metadata committed at version {} pg={} log_index={} op={}",
                bucket.version.0,
                bucket.evidence.placement_group.0,
                bucket.evidence.log_index.0,
                bucket.evidence.operation_id.0
            ),
        );
        let upload = runtime
            .objects
            .begin_multipart("media", "hero.png")
            .expect("simulated multipart upload must begin for existing bucket");
        trace.push(
            "object",
            "multipart-begin",
            format!(
                "media/hero.png upload_id={} is not yet visible",
                upload.id()
            ),
        );
        let part_one = runtime
            .objects
            .upload_part(
                &mut runtime.router,
                PlacementGroupId(3),
                upload.id(),
                b"simulated ".to_vec(),
            )
            .expect("simulated object part one must write through blob engine");
        trace.push(
            "object",
            "multipart-part",
            format!(
                "part={} offset={} bytes={} content={} chunk_pins={} providers={} pin_version={}",
                part_one.number,
                part_one.offset,
                part_one.len,
                short_hash(*part_one.content.as_bytes()),
                part_one.chunk_pins,
                part_one.providers.len(),
                part_one.pin_version.0
            ),
        );
        let part_two = runtime
            .objects
            .upload_part(
                &mut runtime.router,
                PlacementGroupId(3),
                upload.id(),
                b"image bytes".to_vec(),
            )
            .expect("simulated object part two must write through blob engine");
        trace.push(
            "object",
            "multipart-part",
            format!(
                "part={} offset={} bytes={} content={} chunk_pins={} providers={} pin_version={}",
                part_two.number,
                part_two.offset,
                part_two.len,
                short_hash(*part_two.content.as_bytes()),
                part_two.chunk_pins,
                part_two.providers.len(),
                part_two.pin_version.0
            ),
        );
        let object = runtime
            .objects
            .complete_multipart(&mut runtime.router, PlacementGroupId(2), upload.id())
            .expect("simulated object manifest must commit through protocol router");
        let readback = runtime
            .objects
            .read_object("media", "hero.png")
            .expect("simulated object read must verify blob chunks");
        trace.push(
            "object",
            "put",
            format!(
                "{}/{} manifest committed as version {} with {} parts pg={} log_index={} op={}",
                object.bucket.0,
                object.key.0,
                object.version.0,
                object.manifest.parts.len(),
                object.evidence.placement_group.0,
                object.evidence.log_index.0,
                object.evidence.operation_id.0
            ),
        );
        trace.push(
            "object",
            "read",
            format!("{} bytes streamed and hash-verified", readback.len()),
        );
    }
}

fn object_metadata_decision(decision: ObjectMetadataDecision) -> String {
    match decision {
        ObjectMetadataDecision::BucketRegistered { bucket, evidence } => {
            format!(
                "bucket={bucket} registered in strict object metadata catalog pg={} log_index={} op={}",
                evidence.placement_group.0, evidence.log_index.0, evidence.operation_id.0
            )
        }
        ObjectMetadataDecision::MultipartStarted {
            upload_id,
            bucket,
            key,
            evidence,
        } => {
            format!(
                "{bucket}/{key} upload_id={upload_id} started with no visible head pg={} log_index={} op={}",
                evidence.placement_group.0, evidence.log_index.0, evidence.operation_id.0
            )
        }
        ObjectMetadataDecision::PartAccepted {
            upload_id,
            number,
            bytes,
            evidence,
        } => {
            format!(
                "upload_id={upload_id} part={number} bytes={bytes} accepted into manifest draft pg={} log_index={} op={}",
                evidence.placement_group.0, evidence.log_index.0, evidence.operation_id.0
            )
        }
        ObjectMetadataDecision::HeadCommitted {
            bucket,
            key,
            version,
            evidence,
        } => {
            format!(
                "{bucket}/{key} visible head committed at object metadata version {} pg={} log_index={} op={}",
                version.0,
                evidence.placement_group.0,
                evidence.log_index.0,
                evidence.operation_id.0
            )
        }
        ObjectMetadataDecision::HeadDeleted {
            bucket,
            key,
            version,
            evidence,
        } => {
            format!(
                "{bucket}/{key} delete marker committed at object metadata version {} pg={} log_index={} op={}",
                version.0,
                evidence.placement_group.0,
                evidence.log_index.0,
                evidence.operation_id.0
            )
        }
        ObjectMetadataDecision::HeadRead {
            bucket,
            key,
            version,
        } => {
            format!(
                "{bucket}/{key} current head read at object metadata version {}",
                version.0
            )
        }
        ObjectMetadataDecision::HeadOwnershipTransferred {
            bucket,
            key,
            previous_owner_pg,
            new_owner_pg,
            previous_owner_epoch,
            new_owner_epoch,
            previous_version,
            version,
            evidence,
        } => {
            format!(
                "{bucket}/{key} ownership moved pg={}=>{} owner_epoch={}=>{} version={}=>{} via pg={} log_index={} op={}",
                previous_owner_pg.0,
                new_owner_pg.0,
                previous_owner_epoch.0,
                new_owner_epoch.0,
                previous_version.0,
                version.0,
                evidence.placement_group.0,
                evidence.log_index.0,
                evidence.operation_id.0
            )
        }
        ObjectMetadataDecision::Rejected { reason } => {
            format!("rejected object metadata request: {reason}")
        }
    }
}

pub struct BlobStore;

impl BlobStore {
    pub fn run(trace: &mut Trace, implementation: &Implementation, runtime: &mut DatabaseRuntime) {
        trace.push("blob", "chunks", implementation.database.blob);
        let payload = b"standalone snapshot chunk".to_vec();
        let digest = rsf_core::Digest::blake3_bytes(&payload);
        let blob = runtime
            .blobs
            .put_by_digest(&mut runtime.router, PlacementGroupId(3), digest, payload)
            .expect("simulated blob put must commit through protocol router");
        let readback = runtime
            .blobs
            .read_digest_verified(digest)
            .expect("simulated blob get by digest must verify payload hash");
        trace.push(
            "blob",
            "put-by-digest",
            format!(
                "content-id {} retained at version {} and hash-verified pg={} log_index={} consistency={:?} op={}",
                short_hash(blob.id.0),
                blob.pin_version.0,
                blob.pin_evidence.placement_group.0,
                blob.pin_evidence.log_index.0,
                blob.pin_evidence.consistency,
                blob.pin_evidence.operation_id.0
            ),
        );
        trace.push(
            "blob",
            "get-by-digest",
            format!(
                "content-id {} returned {} verified bytes",
                short_hash(*digest.as_bytes()),
                readback.len()
            ),
        );
    }
}

pub struct ObjectLayout;

impl ObjectLayout {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) {
        let policy = ErasurePolicy::new(3, 2).expect("simulated erasure policy must be valid");
        let nodes = runtime
            .admin
            .cluster_map()
            .nodes
            .iter()
            .map(|node| node.id)
            .collect::<Vec<_>>();
        let payload = b"cold-media-object-body".to_vec();
        let cold_object = runtime
            .objects
            .put_cold_erasure(
                &mut runtime.router,
                PlacementGroupId(2),
                "media",
                "cold.bin",
                payload.clone(),
                policy,
                &nodes,
            )
            .expect("simulated cold object must commit erasure-coded body through object engine");
        let cold_part = cold_object
            .manifest
            .parts
            .first()
            .expect("cold object must have one logical body part");
        let layout = runtime
            .objects
            .cold_layout(&cold_part.content)
            .expect("cold object manifest must reference erasure layout");

        runtime.erasure_data_shards = layout
            .shards
            .iter()
            .filter(|shard| shard.kind == ShardKind::Data)
            .count();
        runtime.erasure_parity_shards = layout
            .shards
            .iter()
            .filter(|shard| shard.kind == ShardKind::Parity)
            .count();
        runtime.erasure_tolerated_loss = usize::from(layout.policy.parity);
        trace.push(
            "erasure",
            "policy",
            format!(
                "cold object media/cold.bin version={} uses {}+{} erasure coding across {} nodes threshold={}",
                cold_object.version.0,
                policy.data,
                policy.parity,
                layout.shards.len(),
                policy.read_threshold()
            ),
        );

        for shard in &layout.shards {
            trace.push(
                "erasure",
                "shard",
                format!(
                    "ordinal={} kind={} node={} bytes={} content={} checksum={}",
                    shard.ordinal,
                    shard_kind(shard.kind),
                    shard.node.0,
                    shard.bytes,
                    short_hash(*shard.content.as_bytes()),
                    short_hash(*shard.checksum.as_bytes()),
                ),
            );
        }

        runtime
            .objects
            .quarantine_cold_shard(&cold_part.content, 1, "simulated data shard outage")
            .expect("simulated degraded read must be able to quarantine data shard");
        trace.push(
            "erasure",
            "degraded",
            "quarantined cold shard ordinal=1 before read to simulate one data shard loss",
        );

        let recovered = runtime
            .objects
            .read_object("media", "cold.bin")
            .expect("simulated cold object read must reconstruct from erasure shards");
        runtime.erasure_reconstructed_bytes = recovered.len();
        trace.push(
            "erasure",
            "reconstruct",
            format!(
                "object-engine read reconstructed {} bytes and verified payload match={}",
                recovered.len(),
                recovered == payload
            ),
        );
    }
}

impl ManifestService {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) -> ExtentManifest {
        let entries = Self::entries();
        Self::record_decision(trace, runtime, ExtentManifest::publish(entries.clone()));

        let manifest = ExtentManifest::new(entries).expect("simulated manifest must validate");
        for decision in manifest.verify() {
            Self::record_decision(trace, runtime, decision);
        }
        for entry in manifest.entries() {
            Self::record_provider_metadata(trace, entry);
        }
        for typed in Self::typed_manifests() {
            Self::record_typed_manifest(trace, typed);
        }
        let checksum_report = manifest
            .verify_checksums(&Self::checksum_proofs(&manifest))
            .expect("simulated manifest checksum proofs must validate");
        trace.push(
            "manifest",
            "checksum",
            format!(
                "checked={} bytes={}",
                checksum_report.checked, checksum_report.total_bytes
            ),
        );

        Self::record_decision(
            trace,
            runtime,
            ExtentManifest::publish(Self::overlapping_entries()),
        );

        runtime.manifest_entries = manifest.entries().len();
        runtime.manifest_surfaces = manifest.surface_count();
        runtime.manifest_bytes = manifest.total_bytes() as usize;
        trace.push(
            "manifest",
            "summary",
            format!(
                "entries={} surfaces={} bytes={} rejections={}",
                runtime.manifest_entries,
                runtime.manifest_surfaces,
                runtime.manifest_bytes,
                runtime.manifest_rejections
            ),
        );
        manifest
    }

    fn record_decision(
        trace: &mut Trace,
        runtime: &mut DatabaseRuntime,
        decision: ManifestDecision,
    ) {
        match decision {
            ManifestDecision::Published {
                entries,
                total_bytes,
                surfaces,
            } => {
                trace.push(
                    "manifest",
                    "publish",
                    format!("entries={entries} surfaces={surfaces} total_bytes={total_bytes}"),
                );
            }
            ManifestDecision::EntryVerified {
                kind,
                object,
                placement_group,
                bytes,
            } => {
                trace.push(
                    "manifest",
                    "entry",
                    format!(
                        "kind={} surface={} object={} pg={} bytes={}",
                        kind.as_str(),
                        kind.surface(),
                        object,
                        placement_group.0,
                        bytes
                    ),
                );
            }
            ManifestDecision::Rejected { reason } => {
                runtime.manifest_rejections += 1;
                trace.push("manifest", "reject", reason);
            }
        }
    }

    fn record_typed_manifest(trace: &mut Trace, manifest: TypedManifest) {
        trace.push(
            "manifest",
            "typed",
            format!(
                "surface={} entries={} bytes={} verified={}",
                manifest.surface().as_str(),
                manifest.entries().len(),
                manifest.total_bytes(),
                manifest.verify().len()
            ),
        );
    }

    fn record_provider_metadata(trace: &mut Trace, entry: &ManifestEntry) {
        let classes = entry
            .provider
            .replicas
            .iter()
            .map(|replica| replica.storage_class.as_str())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>()
            .join(",");
        trace.push(
            "manifest",
            "provider",
            format!(
                "object={} provider={} min_replicas={} replicas={} regions={} classes={}",
                entry.object,
                entry.provider.provider_id,
                entry.provider.min_replicas,
                entry.provider.replica_count(),
                entry.provider.region_count(),
                classes
            ),
        );
    }

    fn entries() -> Vec<ManifestEntry> {
        vec![
            Self::entry(
                ManifestKind::RelationalPage,
                "sql/app/users/page-0",
                1,
                0,
                4_096,
            ),
            Self::entry(
                ManifestKind::RelationalWal,
                "sql/app/users/redo",
                2,
                0,
                2_048,
            ),
            Self::entry(
                ManifestKind::OrderedKvSstable,
                "kv/app/config/run-7",
                3,
                0,
                8_192,
            ),
            Self::entry(
                ManifestKind::ObjectPart,
                "object/media/hero.png/part-0",
                4,
                0,
                3_072,
            ),
            Self::entry(
                ManifestKind::BlobChunk,
                "blob/cid-hero/chunk-0",
                5,
                0,
                1_024,
            ),
            Self::entry(
                ManifestKind::SnapshotRoot,
                "snapshot/app/checkpoint-22",
                6,
                0,
                2_560,
            ),
            Self::entry(
                ManifestKind::BackupBlock,
                "backup/app/checkpoint-22/block-0",
                7,
                0,
                4_096,
            ),
        ]
    }

    fn typed_manifests() -> Vec<TypedManifest> {
        vec![
            SqlManifest::new(vec![
                Self::entry(
                    ManifestKind::RelationalPage,
                    "sql/app/users/page-0",
                    11,
                    0,
                    4_096,
                ),
                Self::entry(
                    ManifestKind::RelationalWal,
                    "sql/app/users/redo",
                    12,
                    0,
                    2_048,
                ),
            ])
            .expect("simulated SQL manifest must validate")
            .typed()
            .clone(),
            KvManifest::new(vec![Self::entry(
                ManifestKind::OrderedKvSstable,
                "kv/app/config/run-7",
                13,
                0,
                8_192,
            )])
            .expect("simulated KV manifest must validate")
            .typed()
            .clone(),
            ObjectManifest::new(vec![Self::entry(
                ManifestKind::ObjectPart,
                "object/media/hero.png/part-0",
                14,
                0,
                3_072,
            )])
            .expect("simulated object manifest must validate")
            .typed()
            .clone(),
            BlobManifest::new(vec![Self::entry(
                ManifestKind::BlobChunk,
                "blob/cid-hero/chunk-0",
                15,
                0,
                1_024,
            )])
            .expect("simulated blob manifest must validate")
            .typed()
            .clone(),
            SnapshotManifest::new(vec![Self::entry(
                ManifestKind::SnapshotRoot,
                "snapshot/app/checkpoint-22",
                16,
                0,
                2_560,
            )])
            .expect("simulated snapshot manifest must validate")
            .typed()
            .clone(),
            BackupManifest::new(vec![Self::entry(
                ManifestKind::BackupBlock,
                "backup/app/checkpoint-22/block-0",
                17,
                0,
                4_096,
            )])
            .expect("simulated backup manifest must validate")
            .typed()
            .clone(),
        ]
    }

    fn checksum_proofs(manifest: &ExtentManifest) -> Vec<ManifestChecksumProof> {
        manifest
            .entries()
            .iter()
            .map(|entry| {
                ManifestChecksumProof::new(entry.extent_id, entry.checksum, entry.range.len)
            })
            .collect()
    }

    fn overlapping_entries() -> Vec<ManifestEntry> {
        let mut entries = Self::entries();
        entries.push(ManifestEntry::new(
            ManifestKind::OrderedKvSstable,
            "app",
            "kv/app/config/run-7",
            CommitVersion(22),
            Digest::blake3([77_u8; 32])
                .expect("simulated manifest extent digest must be non-empty"),
            PlacementGroupId(2),
            ByteRange::new(4_096, 1_024),
            Digest::blake3([87_u8; 32])
                .expect("simulated manifest checksum digest must be non-empty"),
        ));
        entries
    }

    fn entry(kind: ManifestKind, object: &str, seed: u8, offset: u64, len: u64) -> ManifestEntry {
        ManifestEntry::new(
            kind,
            "app",
            object,
            CommitVersion(22),
            Digest::blake3([seed; 32]).expect("simulated manifest extent digest must be non-empty"),
            PlacementGroupId(u64::from(seed % 3) + 1),
            ByteRange::new(offset, len),
            Digest::blake3([seed.wrapping_add(10); 32])
                .expect("simulated manifest checksum digest must be non-empty"),
        )
        .with_provider_metadata(Self::provider_metadata(kind, seed))
    }

    fn provider_metadata(kind: ManifestKind, seed: u8) -> ProviderMetadata {
        let storage_class = match kind {
            ManifestKind::RelationalPage
            | ManifestKind::RelationalWal
            | ManifestKind::OrderedKvSstable => ProviderStorageClass::Hot,
            ManifestKind::ObjectPart | ManifestKind::BlobChunk => ProviderStorageClass::Warm,
            ManifestKind::SnapshotRoot => ProviderStorageClass::Cold,
            ManifestKind::BackupBlock => ProviderStorageClass::Archive,
        };
        ProviderMetadata::new(
            format!("{}-provider", kind.surface()),
            2,
            vec![
                ProviderReplica::new(
                    NodeId(u64::from(seed) + 10),
                    RegionId::new("sfo").expect("simulated provider region must be valid"),
                    storage_class,
                ),
                ProviderReplica::new(
                    NodeId(u64::from(seed) + 20),
                    RegionId::new("iad").expect("simulated provider region must be valid"),
                    storage_class,
                ),
            ],
        )
    }
}

impl StorageRecovery {
    pub fn run(
        trace: &mut Trace,
        recovery: &Recovery,
        manifest: &ExtentManifest,
        runtime: &mut DatabaseRuntime,
    ) {
        let prefixes = recovery.committed_prefixes();
        runtime.storage_committed_prefixes = prefixes.len();
        for prefix in &prefixes {
            trace.push(
                "storage",
                "committed-prefix",
                format!(
                    "pg={} epoch={} index={} hash={}",
                    prefix.pg.0,
                    prefix.epoch.0,
                    prefix.index.0,
                    short_hash(prefix.record_hash)
                ),
            );
        }

        let required_prefixes = prefixes
            .iter()
            .map(|prefix| (prefix.pg, prefix.index))
            .collect::<BTreeMap<_, _>>();
        let coverage = recovery
            .prove_manifest_coverage(manifest, &required_prefixes)
            .expect("simulated manifest entries must be covered by recovered committed prefixes");
        runtime.storage_manifest_covered = coverage.total_entries;
        for entry in &coverage.covered_entries {
            trace.push(
                "storage",
                "manifest-covered",
                format!(
                    "kind={} object={} pg={} required_index={} committed_index={}",
                    entry.kind.as_str(),
                    entry.object,
                    entry.pg.0,
                    entry.required_index.0,
                    entry.committed_prefix.index.0
                ),
            );
        }

        trace.push(
            "storage",
            "summary",
            format!(
                "committed_prefixes={} manifest_entries_covered={} kinds={}",
                runtime.storage_committed_prefixes,
                runtime.storage_manifest_covered,
                coverage.entries_by_surface.len()
            ),
        );
    }
}

impl ChunkStoreService {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) {
        let mut store = ChunkStore::default();
        let writes = [
            ChunkWrite::new(
                ChunkClass::RelationalLargeValue,
                "app",
                b"sql-large-value".to_vec(),
                true,
            ),
            ChunkWrite::new(
                ChunkClass::ObjectBody,
                "app",
                b"object-body-block".to_vec(),
                true,
            ),
            ChunkWrite::new(
                ChunkClass::BlobPayload,
                "app",
                b"blob-payload".to_vec(),
                true,
            ),
            ChunkWrite::new(
                ChunkClass::SstableBlock,
                "app",
                b"kv-sstable-block".to_vec(),
                false,
            ),
            ChunkWrite::new(
                ChunkClass::SnapshotBlock,
                "app",
                b"snapshot-block".to_vec(),
                true,
            ),
            ChunkWrite::new(
                ChunkClass::BackupBlock,
                "app",
                b"object-body-block".to_vec(),
                true,
            ),
        ];

        let mut ids = Vec::new();
        for write in writes {
            let decision = store.put(write);
            if let Some(id) = decision.id() {
                ids.push(id.clone());
            }
            Self::record_decision(trace, runtime, decision);
        }

        for id in ids.iter().take(3) {
            Self::record_decision(trace, runtime, store.read_verified(id));
        }

        if let Some(id) = ids.first() {
            Self::record_decision(trace, runtime, store.release_pin(id));
        }
        Self::record_decision(
            trace,
            runtime,
            store.put(ChunkWrite::new(
                ChunkClass::BlobPayload,
                "app",
                Vec::new(),
                false,
            )),
        );

        let report = store.verify_all();
        runtime.chunkstore_chunks = report.chunks;
        runtime.chunkstore_pinned = report.pinned;
        runtime.chunkstore_bytes = report.total_bytes as usize;
        trace.push(
            "chunkstore",
            "summary",
            format!(
                "chunks={} pinned={} bytes={} verified={} deduped={} rejections={} quarantined={}",
                runtime.chunkstore_chunks,
                runtime.chunkstore_pinned,
                runtime.chunkstore_bytes,
                runtime.chunkstore_verified,
                runtime.chunkstore_deduped,
                runtime.chunkstore_rejections,
                report.quarantined
            ),
        );
    }

    fn record_decision(trace: &mut Trace, runtime: &mut DatabaseRuntime, decision: ChunkDecision) {
        match decision {
            ChunkDecision::Stored {
                id,
                class,
                bytes,
                deduped,
                pins,
            } => {
                if deduped {
                    runtime.chunkstore_deduped += 1;
                }
                trace.push(
                    "chunkstore",
                    if deduped { "dedupe" } else { "store" },
                    format!(
                        "id={} class={} bytes={} pins={}",
                        short_hash(id.0),
                        class.as_str(),
                        bytes,
                        pins
                    ),
                );
            }
            ChunkDecision::ReadVerified {
                id,
                bytes,
                checksum,
            } => {
                runtime.chunkstore_verified += 1;
                trace.push(
                    "chunkstore",
                    "verify",
                    format!(
                        "id={} bytes={} checksum={}",
                        short_hash(id.0),
                        bytes,
                        short_hash(*checksum.as_bytes())
                    ),
                );
            }
            ChunkDecision::Pinned { id, pins } => {
                trace.push(
                    "chunkstore",
                    "pin",
                    format!("id={} pins={pins}", short_hash(id.0)),
                );
            }
            ChunkDecision::Released { id, remaining_pins } => {
                trace.push(
                    "chunkstore",
                    "release",
                    format!("id={} remaining_pins={remaining_pins}", short_hash(id.0)),
                );
            }
            ChunkDecision::Quarantined { id, reason } => {
                runtime.chunkstore_rejections += 1;
                trace.push(
                    "chunkstore",
                    "quarantine",
                    format!("id={} reason={reason}", short_hash(id.0)),
                );
            }
            ChunkDecision::Rejected { reason } => {
                runtime.chunkstore_rejections += 1;
                trace.push("chunkstore", "reject", reason);
            }
        }
    }
}

impl CheckpointService {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) {
        let planner = CheckpointPlanner;
        let plan = planner
            .plan(
                CommitVersion(22),
                Self::checkpoint_roots(),
                Self::segments(),
            )
            .expect("checkpoint POC inputs must be valid");

        for decision in &plan.decisions {
            Self::record_decision(trace, runtime, decision);
        }
        let regression = reject_if_checkpoint_regresses(CommitVersion(22), CommitVersion(21));
        Self::record_decision(trace, runtime, &regression);

        runtime.checkpoint_roots = plan.retained_roots;
        runtime.checkpoint_segments = plan.segments.len();
        runtime.checkpoint_compacted = plan.compacted_segments;
        runtime.checkpoint_reclaimable_bytes = plan.reclaimable_bytes as usize;

        trace.push(
            "checkpoint",
            "summary",
            format!(
                "checkpoint={} roots={} segments={} compacted={} reclaimable_bytes={} high_watermark={} rejections={}",
                plan.checkpoint.0,
                runtime.checkpoint_roots,
                runtime.checkpoint_segments,
                runtime.checkpoint_compacted,
                runtime.checkpoint_reclaimable_bytes,
                plan.high_watermark.0,
                runtime.checkpoint_rejections
            ),
        );
    }

    fn record_decision(
        trace: &mut Trace,
        runtime: &mut DatabaseRuntime,
        decision: &CheckpointDecision,
    ) {
        match decision {
            CheckpointDecision::RetainRoot {
                kind,
                name,
                version,
                bytes,
            } => {
                trace.push(
                    "checkpoint",
                    "retain-root",
                    format!(
                        "kind={} name={} version={} bytes={} required={}",
                        kind.as_str(),
                        name,
                        version.0,
                        bytes,
                        kind.required()
                    ),
                );
            }
            CheckpointDecision::KeepSegment { id, reason } => {
                trace.push(
                    "checkpoint",
                    "keep-segment",
                    format!("id={id} reason={reason}"),
                );
            }
            CheckpointDecision::CompactSegment {
                id,
                reclaimable_bytes,
            } => {
                trace.push(
                    "checkpoint",
                    "compact-segment",
                    format!("id={id} reclaimable_bytes={reclaimable_bytes}"),
                );
            }
            CheckpointDecision::Rejected { reason } => {
                runtime.checkpoint_rejections += 1;
                trace.push("checkpoint", "reject", reason);
            }
        }
    }

    fn checkpoint_roots() -> Vec<CheckpointRoot> {
        vec![
            Self::root(CheckpointRootKind::Catalog, "catalog/main", 22, 512, 1),
            Self::root(
                CheckpointRootKind::TransactionLog,
                "fabric/wal",
                22,
                2_048,
                2,
            ),
            Self::root(CheckpointRootKind::Kv, "kv/app/config", 21, 1_024, 3),
            Self::root(CheckpointRootKind::Sql, "sql/app/users", 21, 4_096, 4),
            Self::root(
                CheckpointRootKind::Object,
                "object/media/hero.png",
                20,
                3_072,
                5,
            ),
            Self::root(CheckpointRootKind::Blob, "blob/sha256-demo", 22, 1_024, 6),
            Self::root(
                CheckpointRootKind::ChunkIndex,
                "chunk/index/main",
                22,
                256,
                7,
            ),
        ]
    }

    fn root(
        kind: CheckpointRootKind,
        name: &str,
        version: u64,
        bytes: u64,
        checksum: u8,
    ) -> CheckpointRoot {
        CheckpointRoot::new(
            kind,
            "app",
            name,
            CommitVersion(version),
            bytes,
            Digest::blake3([checksum; 32])
                .expect("simulated checkpoint checksum digest must be non-empty"),
        )
    }

    fn segments() -> Vec<SegmentInventory> {
        vec![
            SegmentInventory::new("segment-0001", CommitVersion(22), 4_096, 4_096, true),
            SegmentInventory::new("segment-0002", CommitVersion(20), 4_096, 1_024, false),
            SegmentInventory::new("segment-0003", CommitVersion(22), 2_048, 1_536, false),
            SegmentInventory::new("segment-0004", CommitVersion(23), 1_024, 512, false),
        ]
    }
}

pub struct ManifestService;

pub struct StorageRecovery;

pub struct ChunkStoreService;

pub struct CheckpointService;

pub struct Backup;

pub struct SuperblockService;

pub struct Snapshot;

pub struct ProviderDiscovery;

impl ProviderDiscovery {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) {
        let mut directory = ProviderDirectory::default();
        for record in [
            ProviderRecord::new(
                "object:media/hero.png",
                ChunkSurface::Object,
                NodeId(1),
                RegionId::new("sfo").expect("simulated provider region must be valid"),
                5,
            ),
            ProviderRecord::new(
                "object:media/hero.png",
                ChunkSurface::Object,
                NodeId(2),
                RegionId::new("iad").expect("simulated provider region must be valid"),
                34,
            ),
            ProviderRecord::new(
                "object:media/hero.png",
                ChunkSurface::Object,
                NodeId(3),
                RegionId::new("sfo").expect("simulated provider region must be valid"),
                8,
            ),
            ProviderRecord::new(
                "blob:sha256-demo",
                ChunkSurface::Blob,
                NodeId(4),
                RegionId::new("iad").expect("simulated provider region must be valid"),
                19,
            ),
            ProviderRecord::new(
                "blob:sha256-demo",
                ChunkSurface::Blob,
                NodeId(5),
                RegionId::new("lhr").expect("simulated provider region must be valid"),
                76,
            )
            .unavailable(),
            ProviderRecord::new(
                "object:cold-shard-1",
                ChunkSurface::Object,
                NodeId(3),
                RegionId::new("sfo").expect("simulated provider region must be valid"),
                11,
            )
            .unverified(),
        ] {
            trace.push(
                "provider",
                "advertise",
                format!(
                    "content={} surface={} node={} region={} available={} verified={} latency_ms={}",
                    record.content_id,
                    record.surface.as_str(),
                    record.node.0,
                    record.region,
                    record.available,
                    record.verified,
                    record.latency_ms
                ),
            );
            directory
                .advertise(record)
                .expect("simulated provider advertisement must validate");
        }

        let decisions = [
            directory.select(&ProviderQuery::new(
                "object:media/hero.png",
                RegionId::new("sfo").expect("simulated provider query region must be valid"),
                2,
                3,
            )),
            directory.select(&ProviderQuery::new(
                "blob:sha256-demo",
                RegionId::new("iad").expect("simulated provider query region must be valid"),
                2,
                2,
            )),
            directory.select(&ProviderQuery::new(
                "object:missing",
                RegionId::new("sfo").expect("simulated provider query region must be valid"),
                1,
                1,
            )),
        ];
        for decision in &decisions {
            Self::record_decision(trace, runtime, decision);
        }

        runtime.provider_records = directory.provider_count();
        runtime.provider_regions = directory.region_count();
        runtime.provider_content = directory.content_count();
        trace.push(
            "provider",
            "summary",
            format!(
                "records={} content={} regions={} selected={} degraded={}",
                runtime.provider_records,
                runtime.provider_content,
                runtime.provider_regions,
                runtime.provider_selected,
                runtime.provider_degraded
            ),
        );
    }

    fn record_decision(
        trace: &mut Trace,
        runtime: &mut DatabaseRuntime,
        decision: &ProviderDecision,
    ) {
        match decision {
            ProviderDecision::Selected(selection) => {
                runtime.provider_selected += selection.providers.len();
                trace.push(
                    "provider",
                    "select",
                    format!(
                        "content={} surface={} providers={} nodes={}",
                        selection.content_id,
                        selection.surface.as_str(),
                        selection.providers.len(),
                        provider_nodes(&selection.providers)
                    ),
                );
            }
            ProviderDecision::Degraded(selection) => {
                runtime.provider_selected += selection.providers.len();
                runtime.provider_degraded += 1;
                trace.push(
                    "provider",
                    "degraded",
                    format!(
                        "content={} requested={} providers={} nodes={}",
                        selection.content_id,
                        selection.requested_replicas,
                        selection.providers.len(),
                        provider_nodes(&selection.providers)
                    ),
                );
            }
            ProviderDecision::Rejected { content_id, reason } => {
                trace.push(
                    "provider",
                    "reject",
                    format!("content={content_id} reason={reason}"),
                );
            }
        }
    }
}

pub struct Stream;

impl Stream {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) {
        let planner = StreamPlanner::new(StreamPolicy {
            max_chunk_bytes: 8,
            max_inflight_bytes: 16,
            read_lanes: 3,
        })
        .expect("simulated stream policy must validate");
        trace.push(
            "stream",
            "policy",
            "simulated transfer policy chunk=8 inflight=16 read_lanes=3",
        );

        let object_payload = b"object-stream-payload".to_vec();
        let blob_payload = b"blob-stream!".to_vec();
        let object = runtime
            .blobs
            .write_stream(
                &mut runtime.router,
                PlacementGroupId(3),
                &planner,
                StreamTarget::new(StreamSurface::Object, "app", "media/hero.png"),
                object_payload,
            )
            .expect("simulated object stream write must persist chunks");
        let blob = runtime
            .blobs
            .write_stream(
                &mut runtime.router,
                PlacementGroupId(3),
                &planner,
                StreamTarget::new(StreamSurface::Blob, "app", "sha256-demo"),
                blob_payload,
            )
            .expect("simulated blob stream write must persist chunks");
        runtime.stream_write_chunks = object.write.chunks.len() + blob.write.chunks.len();
        Self::record_write(trace, "object", &object.write.chunks);
        Self::record_write(trace, "blob", &blob.write.chunks);

        let mut window = FlowWindow::new(16).expect("simulated flow window must validate");
        for chunk in &object.write.chunks {
            match window.reserve(chunk.len) {
                FlowDecision::Reserved {
                    bytes,
                    remaining_bytes,
                } => trace.push(
                    "stream",
                    "reserve",
                    format!(
                        "key={} offset={} bytes={} remaining={}",
                        stream_target(&chunk.target),
                        chunk.offset,
                        bytes,
                        remaining_bytes
                    ),
                ),
                FlowDecision::Rejected { bytes, reason } => {
                    runtime.stream_window_rejections += 1;
                    trace.push(
                        "stream",
                        "backpressure",
                        format!(
                            "key={} offset={} bytes={} reason={reason}",
                            stream_target(&chunk.target),
                            chunk.offset,
                            bytes
                        ),
                    );
                    window
                        .release(8)
                        .expect("simulated completed chunk releases");
                    let retry = window.reserve(bytes);
                    trace.push(
                        "stream",
                        "retry",
                        format!(
                            "key={} offset={} decision={}",
                            stream_target(&chunk.target),
                            chunk.offset,
                            flow_decision(retry)
                        ),
                    );
                }
            }
        }

        let read = planner
            .plan_read(
                object.write.target.clone(),
                runtime.router.latest_version(),
                object.write.chunks.clone(),
            )
            .expect("simulated parallel object read must plan");
        let read_target = read.target.clone();
        let read_min_version = read.min_version;
        let read_lanes = read.lanes;
        let read_chunks = read.chunks.len();
        let readback = runtime
            .blobs
            .read_stream(&planner, read)
            .expect("simulated stream chunks must verify and reconstruct");
        runtime.stream_read_lanes = read_lanes.into();
        runtime.stream_verified_parts = readback.read.chunks.len();
        trace.push(
            "stream",
            "read",
            format!(
                "key={} min_version={} lanes={} chunks={}",
                stream_target(&read_target),
                read_min_version.0,
                read_lanes,
                read_chunks
            ),
        );
        trace.push(
            "stream",
            "verify",
            format!(
                "verified_chunks={} total_bytes={}",
                readback.read.chunks.len(),
                readback.bytes.len()
            ),
        );
    }

    fn record_write(trace: &mut Trace, label: &'static str, chunks: &[rsf_stream::StreamChunk]) {
        trace.push(
            "stream",
            "write-plan",
            format!("{label} chunks={}", chunks.len()),
        );
        for chunk in chunks {
            trace.push(
                "stream",
                "chunk",
                format!(
                    "key={} offset={} len={} lane={}",
                    stream_target(&chunk.target),
                    chunk.offset,
                    chunk.len,
                    chunk.lane
                ),
            );
        }
    }
}

pub struct Cache;

impl Cache {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) {
        let mut cache =
            ReadCache::new(CachePolicy::bounded(768, 512, 2)).expect("cache policy must validate");
        let snapshot = runtime.router.latest_version();
        trace.push(
            "cache",
            "policy",
            "simulated read-serving cache capacity=768 max_entry=512 read_ahead=2",
        );

        for entry in [
            Self::entry(CacheSurface::Sql, "users/1", snapshot, 128, false),
            Self::entry(CacheSurface::Kv, "config/a", snapshot, 64, false),
            Self::entry(CacheSurface::Object, "media/hero.png", snapshot, 256, false),
        ] {
            Self::record_admission(trace, runtime, cache.put(entry).expect("cache put"));
        }

        for read in [
            cache.read(
                &CacheKey::new(CacheSurface::Sql, "app", "users/1"),
                snapshot,
            ),
            cache.read(
                &CacheKey::new(CacheSurface::Kv, "app", "config/a"),
                snapshot,
            ),
            cache.read(
                &CacheKey::new(CacheSurface::Object, "app", "media/hero.png"),
                snapshot,
            ),
        ] {
            Self::record_read(trace, runtime, read);
        }
        Self::record_strict_read(
            trace,
            runtime,
            cache.strict_read(
                &CacheKey::new(CacheSurface::Sql, "app", "users/1"),
                CommitVersion(snapshot.0 + 1),
            ),
        );
        for read in [cache.read(
            &CacheKey::new(CacheSurface::Blob, "app", "sha256-demo"),
            snapshot,
        )] {
            Self::record_read(trace, runtime, read);
        }

        let blob = Self::entry(CacheSurface::Blob, "sha256-demo", snapshot, 320, false);
        Self::record_admission(trace, runtime, cache.put(blob).expect("cache put"));
        let stale_part = CacheKey::new(CacheSurface::Object, "app", "media/hero.part-stale");
        Self::record_admission(
            trace,
            runtime,
            cache
                .put(CacheEntry {
                    key: stale_part.clone(),
                    version: CommitVersion(snapshot.0.saturating_sub(1)),
                    bytes: 64,
                    cost: 1,
                    pinned: false,
                })
                .expect("cache put stale read-ahead candidate"),
        );

        let plan = cache
            .read_ahead(
                CacheKey::new(CacheSurface::Object, "app", "media/hero.png"),
                snapshot,
                [
                    Self::candidate(CacheSurface::Object, "media/hero.part-1", snapshot, 64),
                    Self::candidate(CacheSurface::Blob, "sha256-demo", snapshot, 64),
                    Self::candidate(CacheSurface::Object, "media/hero.part-stale", snapshot, 64),
                    Self::candidate(CacheSurface::Object, "media/hero.part-2", snapshot, 64),
                    Self::candidate(CacheSurface::Object, "media/hero.part-3", snapshot, 64),
                ],
            )
            .expect("simulated read-ahead plan must validate");
        runtime.cache_hints = plan.hints.len();
        runtime.cache_evictions += plan.evicted.len();
        trace.push(
            "cache",
            "read-ahead",
            format!(
                "anchor={} required_version={} planned_bytes={} hints={} skipped={} evicted={}",
                cache_key(&plan.anchor),
                plan.required_version.0,
                plan.planned_bytes,
                plan.hints
                    .iter()
                    .map(cache_key)
                    .collect::<Vec<_>>()
                    .join(","),
                plan.skipped
                    .iter()
                    .map(|skip| format!("{}:{}", cache_key(&skip.key), skip.reason))
                    .collect::<Vec<_>>()
                    .join(","),
                cache_keys_label(&plan.evicted)
            ),
        );
    }

    fn entry(
        surface: CacheSurface,
        key: &'static str,
        version: CommitVersion,
        bytes: u64,
        pinned: bool,
    ) -> CacheEntry {
        CacheEntry {
            key: CacheKey::new(surface, "app", key),
            version,
            bytes,
            cost: 1,
            pinned,
        }
    }

    fn candidate(
        surface: CacheSurface,
        key: &'static str,
        version: CommitVersion,
        bytes: u64,
    ) -> ReadAheadCandidate {
        ReadAheadCandidate::new(CacheKey::new(surface, "app", key), version, bytes, 1)
    }

    fn record_admission(
        trace: &mut Trace,
        runtime: &mut DatabaseRuntime,
        admission: CacheAdmission,
    ) {
        match admission {
            CacheAdmission::Stored {
                key,
                used_bytes,
                evicted,
            } => {
                runtime.cache_evictions += evicted.len();
                trace.push(
                    "cache",
                    "store",
                    format!(
                        "key={} used_bytes={} evicted={}",
                        cache_key(&key),
                        used_bytes,
                        cache_keys_label(&evicted)
                    ),
                );
            }
            CacheAdmission::Rejected { key, reason } => {
                trace.push(
                    "cache",
                    "reject",
                    format!("key={} reason={reason}", cache_key(&key)),
                );
            }
        }
    }

    fn record_read(trace: &mut Trace, runtime: &mut DatabaseRuntime, read: CacheRead) {
        match read {
            CacheRead::Hit(hit) => {
                runtime.cache_hits += 1;
                trace.push(
                    "cache",
                    "hit",
                    format!(
                        "key={} version={} bytes={}",
                        cache_key(&hit.key),
                        hit.version.0,
                        hit.bytes
                    ),
                );
            }
            CacheRead::Miss { key, reason } => {
                runtime.cache_misses += 1;
                trace.push(
                    "cache",
                    "miss",
                    format!("key={} reason={reason}", cache_key(&key)),
                );
            }
            CacheRead::Stale {
                key,
                cached_version,
                required_version,
            } => {
                runtime.cache_misses += 1;
                runtime.cache_evictions += 1;
                trace.push(
                    "cache",
                    "stale",
                    format!(
                        "key={} cached_version={} required_version={} action=evict-and-read-through",
                        cache_key(&key),
                        cached_version.0,
                        required_version.0
                    ),
                );
            }
        }
    }

    fn record_strict_read(trace: &mut Trace, runtime: &mut DatabaseRuntime, read: StrictCacheRead) {
        match read {
            StrictCacheRead::Serve(hit) => {
                runtime.cache_hits += 1;
                trace.push(
                    "cache",
                    "strict-hit",
                    format!(
                        "key={} version={} bytes={}",
                        cache_key(&hit.key),
                        hit.version.0,
                        hit.bytes
                    ),
                );
            }
            StrictCacheRead::ReadThrough {
                key,
                required_version,
                reason,
            } => {
                runtime.cache_misses += 1;
                if reason.contains("older than required") {
                    runtime.cache_evictions += 1;
                }
                trace.push(
                    "cache",
                    "strict-read-through",
                    format!(
                        "key={} required_version={} reason={reason} action=read-authoritative-source",
                        cache_key(&key),
                        required_version.0
                    ),
                );
            }
        }
    }
}

pub struct WorkloadShape;

impl WorkloadShape {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) {
        let object_metadata = runtime.object_metadata.report();
        let report = WorkloadShapeReport::evaluate(vec![
            Self::budget(
                WorkloadClass::RelationalOltp,
                18,
                10,
                WorkloadEvidence {
                    commits: runtime.sql_dml_committed as u8,
                    cache_hits: runtime.cache_hits as u8,
                    provider_choices: 0,
                    stream_lanes: 0,
                    regions: runtime.provider_regions as u8,
                },
            ),
            Self::budget(
                WorkloadClass::OrderedKv,
                12,
                7,
                WorkloadEvidence {
                    commits: runtime.kv.visible_key_count() as u8,
                    cache_hits: 0,
                    provider_choices: 0,
                    stream_lanes: 0,
                    regions: runtime.provider_regions as u8,
                },
            ),
            Self::budget(
                WorkloadClass::ObjectMultipart,
                45,
                22,
                WorkloadEvidence {
                    commits: object_metadata.visible_heads as u8,
                    cache_hits: 0,
                    provider_choices: runtime.provider_selected as u8,
                    stream_lanes: 0,
                    regions: runtime.provider_regions as u8,
                },
            ),
            Self::budget(
                WorkloadClass::BlobStreaming,
                65,
                34,
                WorkloadEvidence {
                    commits: 1,
                    cache_hits: 0,
                    provider_choices: runtime.provider_selected as u8,
                    stream_lanes: runtime.stream_read_lanes as u8,
                    regions: runtime.provider_regions as u8,
                },
            ),
        ]);

        runtime.workload_classes = report.covered_classes as usize;
        runtime.workload_budget_paths = report.paths_with_budget as usize;
        runtime.workload_benchmark_claim = usize::from(report.benchmark_claim);
        trace.push(
            "workload",
            "shape",
            format!(
                "classes={} budget_paths={} benchmark_claim={} structural={}",
                report.covered_classes,
                report.paths_with_budget,
                report.benchmark_claim,
                report.structural_shape()
            ),
        );
        for budget in &report.budgets {
            trace.push(
                "workload",
                "budget",
                format!(
                    "class={} budget_ms={} observed_path_ms={} commits={} cache_hits={} provider_choices={} stream_lanes={} regions={}",
                    budget.class.as_str(),
                    budget.p99_budget_ms,
                    budget.observed_path_ms,
                    budget.evidence.commits,
                    budget.evidence.cache_hits,
                    budget.evidence.provider_choices,
                    budget.evidence.stream_lanes,
                    budget.evidence.regions
                ),
            );
        }
    }

    fn budget(
        class: WorkloadClass,
        p99_budget_ms: u16,
        observed_path_ms: u16,
        evidence: WorkloadEvidence,
    ) -> WorkloadBudget {
        WorkloadBudget {
            class,
            p99_budget_ms,
            observed_path_ms,
            evidence,
        }
    }
}

impl SuperblockService {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) {
        let checkpoint = runtime.router.latest_version();
        let current = Self::superblock(1, checkpoint);
        Self::record_decision(trace, runtime, SuperblockStore::open(current.clone()));

        let mut store = SuperblockStore::new(current).expect("simulated superblock must validate");
        for decision in store.verify_roots() {
            Self::record_decision(trace, runtime, decision);
        }

        Self::record_decision(
            trace,
            runtime,
            store.advance(Self::superblock(2, checkpoint)),
        );
        Self::record_decision(
            trace,
            runtime,
            SuperblockStore::open(Self::missing_catalog()),
        );

        runtime.superblock_generation = store.current().generation as usize;
        runtime.superblock_checkpoint = store.current().checkpoint.0 as usize;
        runtime.superblock_roots = store.current().roots.len();
        trace.push(
            "superblock",
            "summary",
            format!(
                "generation={} checkpoint={} roots={} advances={} rejections={}",
                runtime.superblock_generation,
                runtime.superblock_checkpoint,
                runtime.superblock_roots,
                runtime.superblock_advances,
                runtime.superblock_rejections
            ),
        );
    }

    fn record_decision(
        trace: &mut Trace,
        runtime: &mut DatabaseRuntime,
        decision: SuperblockDecision,
    ) {
        match decision {
            SuperblockDecision::Accepted {
                generation,
                checkpoint,
                roots,
            } => {
                trace.push(
                    "superblock",
                    "open",
                    format!(
                        "generation={} checkpoint={} roots={}",
                        generation, checkpoint.0, roots
                    ),
                );
            }
            SuperblockDecision::RootVerified {
                kind,
                name,
                version,
            } => {
                trace.push(
                    "superblock",
                    "root",
                    format!(
                        "kind={} name={} version={} required={}",
                        kind.as_str(),
                        name,
                        version.0,
                        kind.required()
                    ),
                );
            }
            SuperblockDecision::Advanced {
                generation,
                checkpoint,
            } => {
                runtime.superblock_advances += 1;
                trace.push(
                    "superblock",
                    "advance",
                    format!("generation={} checkpoint={}", generation, checkpoint.0),
                );
            }
            SuperblockDecision::Rejected { reason } => {
                runtime.superblock_rejections += 1;
                trace.push("superblock", "reject", reason);
            }
        }
    }

    fn superblock(generation: u64, checkpoint: CommitVersion) -> Superblock {
        Superblock {
            cluster: ClusterId::new("rsf-dev").expect("simulated cluster id must be valid"),
            generation,
            layout_epoch: Epoch(5),
            checkpoint,
            roots: Self::roots(checkpoint),
        }
    }

    fn missing_catalog() -> Superblock {
        Superblock {
            cluster: ClusterId::new("rsf-dev").expect("simulated cluster id must be valid"),
            generation: 3,
            layout_epoch: Epoch(5),
            checkpoint: CommitVersion(22),
            roots: vec![BootRoot::new(
                BootRootKind::TransactionLog,
                "app",
                "fabric/wal",
                CommitVersion(22),
                2_048,
                Digest::blake3([2; 32]).expect("simulated superblock digest must be non-empty"),
            )],
        }
    }

    fn roots(checkpoint: CommitVersion) -> Vec<BootRoot> {
        CheckpointService::checkpoint_roots()
            .into_iter()
            .map(|root| {
                let mut boot = BootRoot::from_checkpoint(&root);
                if boot.version > checkpoint {
                    boot.version = checkpoint;
                }
                boot
            })
            .collect()
    }
}

impl Snapshot {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) {
        let checkpoint = runtime.router.latest_version();
        let coordinator = SnapshotCoordinator;
        let snapshot = coordinator
            .create(SnapshotRequest {
                id: "snapshot-app-0001".to_string(),
                namespace: "app".to_string(),
                checkpoint,
                roots: vec![
                    Self::root(
                        SnapshotRootKind::Catalog,
                        "catalog/root",
                        checkpoint,
                        SnapshotBarrierKind::CopyOnWrite,
                    ),
                    Self::root(
                        SnapshotRootKind::TransactionLog,
                        "fabric/wal",
                        checkpoint,
                        SnapshotBarrierKind::CopyOnWrite,
                    ),
                    Self::root(
                        SnapshotRootKind::Kv,
                        "kv/app/config",
                        checkpoint,
                        SnapshotBarrierKind::CopyOnWrite,
                    ),
                    Self::root(
                        SnapshotRootKind::Sql,
                        "sql/app/users",
                        checkpoint,
                        SnapshotBarrierKind::CopyOnWrite,
                    ),
                    Self::root(
                        SnapshotRootKind::Object,
                        "object/media/hero.png",
                        checkpoint,
                        SnapshotBarrierKind::ImmutablePin,
                    ),
                    Self::root(
                        SnapshotRootKind::Blob,
                        "blob/snapshot-chunk",
                        checkpoint,
                        SnapshotBarrierKind::ImmutablePin,
                    ),
                    Self::root(
                        SnapshotRootKind::ChunkIndex,
                        "chunk/index/main",
                        checkpoint,
                        SnapshotBarrierKind::CopyOnWrite,
                    ),
                ],
            })
            .expect("simulated database snapshot roots must validate");
        runtime.snapshot_roots = snapshot.root_count();
        runtime.snapshot_barriers = snapshot.barrier_count();
        runtime.snapshot_validations = 3;

        trace.push(
            "snapshot",
            "create",
            format!(
                "{} namespace={} checkpoint={} roots={} barriers={}",
                snapshot.id,
                snapshot.namespace,
                snapshot.checkpoint.0,
                snapshot.root_count(),
                snapshot.barrier_count()
            ),
        );
        for root in &snapshot.roots {
            trace.push(
                "snapshot",
                "root",
                format!(
                    "kind={} name={} version={} barrier={} required={}",
                    root.kind.as_str(),
                    root.name,
                    root.version.0,
                    root.barrier.as_str(),
                    root.kind.required()
                ),
            );
        }
        for barrier in &snapshot.barriers {
            trace.push(
                "snapshot",
                "barrier",
                format!(
                    "root={} name={} kind={} frozen_at={}",
                    barrier.root.as_str(),
                    barrier.name,
                    barrier.kind.as_str(),
                    barrier.version.0
                ),
            );
        }
        trace.push(
            "snapshot",
            "validate",
            "required database roots present; every root has a matching freeze barrier; no root newer than checkpoint; roots unique",
        );
    }

    fn root(
        kind: SnapshotRootKind,
        name: &'static str,
        version: CommitVersion,
        barrier: SnapshotBarrierKind,
    ) -> SnapshotRoot {
        SnapshotRoot {
            kind,
            namespace: "app".to_string(),
            name: name.to_string(),
            version,
            barrier,
        }
    }
}

impl Backup {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) {
        let checkpoint = runtime.router.latest_version();
        let planner = BackupPlanner;
        let manifest = planner
            .create_manifest(
                "snapshot-app-0001",
                checkpoint,
                1,
                vec![
                    Self::item(
                        BackupItemKind::CatalogRoot,
                        "catalog/root",
                        checkpoint,
                        512,
                        1,
                    ),
                    Self::item(
                        BackupItemKind::TransactionLog,
                        "fabric/wal",
                        checkpoint,
                        2048,
                        2,
                    ),
                    Self::item(BackupItemKind::KvRange, "kv/app/config", checkpoint, 256, 3),
                    Self::item(
                        BackupItemKind::SqlTable,
                        "sql/app/users",
                        checkpoint,
                        384,
                        4,
                    ),
                    Self::item(
                        BackupItemKind::ObjectManifest,
                        "object/media/hero.png",
                        checkpoint,
                        192,
                        5,
                    ),
                    Self::item(
                        BackupItemKind::BlobChunk,
                        "blob/snapshot-chunk",
                        checkpoint,
                        1024,
                        6,
                    ),
                    Self::item(
                        BackupItemKind::ChunkIndex,
                        "chunk/index/main",
                        checkpoint,
                        64,
                        7,
                    ),
                ],
            )
            .expect("simulated database snapshot manifest must validate");
        trace.push(
            "backup",
            "manifest",
            format!(
                "{} checkpoint={} items={} checksum={}",
                manifest.id,
                manifest.checkpoint.0,
                manifest.items.len(),
                hex_backup_digest(&manifest.checksum.bytes)
            ),
        );
        for item in &manifest.items {
            trace.push(
                "backup",
                "item",
                format!(
                    "kind={} name={} version={} bytes={} checksum={}",
                    item.kind.as_str(),
                    item.name,
                    item.version.0,
                    item.bytes,
                    hex_backup_digest(&item.checksum.bytes)
                ),
            );
        }

        let plan = planner
            .plan_restore(&manifest)
            .expect("simulated restore plan must validate required roots");
        runtime.backup_items = plan.item_count;
        runtime.restore_checks = plan.checks.len();
        trace.push(
            "backup",
            "restore-plan",
            format!(
                "manifest={} checkpoint={} items={} bytes={} checks={} steps={} manifest_checksum={}",
                plan.manifest_id,
                plan.checkpoint.0,
                plan.item_count,
                plan.total_bytes,
                plan.checks.len(),
                plan.steps.len(),
                hex_backup_digest(&plan.manifest_checksum.bytes)
            ),
        );

        let execution = planner
            .execute_restore(&manifest, &plan)
            .expect("simulated restore execution must cover the manifest");
        runtime.restore_executed_items = execution.restored_items;
        runtime.restore_executed_surfaces = execution.restored_kinds.len();
        runtime.restore_executed_bytes = execution.restored_bytes as usize;
        trace.push(
            "backup",
            "restore-execute",
            format!(
                "manifest={} checkpoint={} items={} bytes={} surfaces={} applied={} covers_manifest={} covers_plan={} execution_checksum={}",
                execution.manifest_id,
                execution.checkpoint.0,
                execution.restored_items,
                execution.restored_bytes,
                execution.restored_kinds.len(),
                execution.applied.len(),
                execution.covers_manifest(&manifest),
                execution.covers_plan(&plan),
                hex_backup_digest(&execution.execution_checksum.bytes)
            ),
        );
        for applied in &execution.applied {
            trace.push(
                "backup",
                "restore-applied",
                format!(
                    "kind={} name={} version={} bytes={} checksum={}",
                    applied.kind.as_str(),
                    applied.name,
                    applied.version.0,
                    applied.bytes,
                    hex_backup_digest(&applied.checksum.bytes)
                ),
            );
        }
    }

    fn item(
        kind: BackupItemKind,
        name: &'static str,
        version: CommitVersion,
        bytes: u64,
        checksum_byte: u8,
    ) -> BackupItem {
        BackupItem {
            kind,
            namespace: "app".to_string(),
            name: name.to_string(),
            version,
            checksum: Digest::blake3([checksum_byte; 32])
                .expect("simulated backup checksum digest must be non-empty"),
            bytes,
        }
    }
}

fn hex_backup_digest(bytes: &[u8; 32]) -> String {
    let mut out = String::with_capacity(64);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

pub struct DatabaseRecovery;

impl DatabaseRecovery {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) {
        let checkpoint = runtime.router.latest_version();
        let planner = RecoveryPlanner;
        let restart = planner
            .plan_restart(checkpoint, Self::roots(checkpoint, true))
            .expect("simulated database restart plan must validate");
        runtime.recovery_roots = restart.recovery.roots.len();
        runtime.recovery_replays = restart.recovery.replay_count();
        runtime.recovery_actions = restart.recovery.action_count();
        trace.push(
            "recovery",
            "restart-plan",
            format!(
                "checkpoint={} roots={} actions={} steps={} replays={} reusable={} quarantined={} opens_for_traffic={}",
                restart.checkpoint.0,
                restart.recovery.roots.len(),
                restart.recovery.actions.len(),
                restart.step_count(),
                restart.replay_roots,
                restart.reusable_roots,
                restart.quarantined_roots,
                restart.opens_for_traffic
            ),
        );
        for step in &restart.steps {
            let root = step
                .root
                .as_ref()
                .map(|root| {
                    format!(
                        "{}:{}@{}",
                        root.kind.as_str(),
                        root.name,
                        root.durable_version.0
                    )
                })
                .unwrap_or_else(|| "database".to_string());
            trace.push(
                "recovery",
                restart_step(step.kind),
                format!("root={} detail={}", root, step.detail),
            );
        }
        for action in &restart.recovery.actions {
            trace.push(
                "recovery",
                recovery_action(action.kind),
                format!(
                    "root={} name={} durable_version={} detail={}",
                    action.root.kind.as_str(),
                    action.root.name,
                    action.root.durable_version.0,
                    action.detail
                ),
            );
        }

        let rejected = planner
            .plan(checkpoint, Self::roots(checkpoint, false))
            .expect_err("simulated required root corruption must reject recovery");
        runtime.recovery_rejections += 1;
        trace.push(
            "recovery",
            "reject",
            format!("required-root recovery rejected: {}", rejected.message),
        );
    }

    fn roots(checkpoint: CommitVersion, catalog_ok: bool) -> Vec<RecoveryRoot> {
        vec![
            Self::root(
                RecoveryRootKind::Catalog,
                "catalog/root",
                CommitVersion(checkpoint.0 - 4),
                catalog_ok,
            ),
            Self::root(
                RecoveryRootKind::TransactionLog,
                "fabric/wal",
                checkpoint,
                true,
            ),
            Self::root(
                RecoveryRootKind::Kv,
                "kv/app/config",
                CommitVersion(checkpoint.0 - 1),
                true,
            ),
            Self::root(
                RecoveryRootKind::Sql,
                "sql/app/users",
                CommitVersion(checkpoint.0 - 1),
                true,
            ),
            Self::root(
                RecoveryRootKind::Object,
                "object/media/hero.png",
                CommitVersion(checkpoint.0 - 2),
                true,
            ),
            Self::root(
                RecoveryRootKind::Blob,
                "blob/snapshot-chunk",
                checkpoint,
                true,
            ),
            Self::root(
                RecoveryRootKind::ChunkIndex,
                "chunk/index/main",
                checkpoint,
                true,
            ),
        ]
    }

    fn root(
        kind: RecoveryRootKind,
        name: &'static str,
        durable_version: CommitVersion,
        checksum_ok: bool,
    ) -> RecoveryRoot {
        RecoveryRoot {
            kind,
            namespace: "app".to_string(),
            name: name.to_string(),
            durable_version,
            present: true,
            checksum_ok,
        }
    }
}

pub struct Retention;

impl Retention {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) {
        let now = runtime.router.latest_version();
        let policy = RetentionPolicy {
            namespace: "app".to_string(),
            retain_until: CommitVersion(5),
            expire_after_versions: Some(10),
            compact_noncurrent_after_versions: Some(6),
            legal_hold: false,
        };
        trace.push(
            "retention",
            "policy",
            format!(
                "namespace={} retain_until={} expire_after=10 compact_noncurrent_after=6",
                policy.namespace, policy.retain_until.0
            ),
        );
        let report = RetentionEngine
            .evaluate(
                &policy,
                now,
                &[
                    RetainedObject {
                        bucket: "media".to_string(),
                        key: "hero.png".to_string(),
                        version: now,
                        current: true,
                    },
                    RetainedObject {
                        bucket: "media".to_string(),
                        key: "old-hero.png".to_string(),
                        version: CommitVersion(6),
                        current: false,
                    },
                ],
                &[
                    RetainedBlob {
                        id: rsf_core::blob::ContentId([1; 32]),
                        pin_version: now,
                        referenced: true,
                    },
                    RetainedBlob {
                        id: rsf_core::blob::ContentId([2; 32]),
                        pin_version: CommitVersion(6),
                        referenced: false,
                    },
                ],
            )
            .expect("simulated retention policy must evaluate");

        runtime.retention_kept = report.keep_count();
        runtime.retention_actions = report.action_count();
        for decision in report.decisions {
            trace.push("retention", "decision", retention_decision(decision));
        }

        if let Some(current) = runtime.objects.get("media", "hero.png") {
            let mut legal_hold = policy.clone();
            legal_hold.legal_hold = true;
            let denied = RetentionEngine
                .authorize_object_delete(
                    &legal_hold,
                    &RetainedObject {
                        bucket: current.bucket.0.clone(),
                        key: current.key.0.clone(),
                        version: current.version,
                        current: true,
                    },
                )
                .expect_err("legal hold must block object delete hook");
            trace.push(
                "retention",
                "hook-deny",
                format!(
                    "object={}/{} version={} reason={}",
                    current.bucket.0, current.key.0, current.version.0, denied.message
                ),
            );
        }

        let hook = RetentionEngine
            .authorize_blob_pin_release(
                &policy,
                now,
                &RetainedBlob {
                    id: rsf_core::blob::ContentId([2; 32]),
                    pin_version: CommitVersion(6),
                    referenced: false,
                },
            )
            .expect("expired unreferenced blob pin release hook must be allowed");
        trace.push("retention", "hook-allow", retention_hook(hook));
    }
}

pub struct Repair;

impl Repair {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) {
        trace.push(
            "repair",
            "surface",
            "simulated scrub inventory over real placement groups",
        );
        let groups = runtime
            .admin
            .map_replicated_groups(3, 3)
            .expect("simulated repair placement groups must map");
        let planner = RepairPlanner::new(16);

        for group in &groups {
            let states = Self::simulated_states(group.id);
            let scrub = planner
                .scrub(group, &states)
                .expect("simulated repair scrub inventory must validate");
            trace.push(
                "repair",
                "scrub",
                format!(
                    "pg={} expected={} scanned={} present={} verified={} missing={} corrupt={} slow={}",
                    scrub.pg.0,
                    scrub.expected_replicas,
                    scrub.scanned_replicas,
                    scrub.present_replicas,
                    scrub.verified_replicas,
                    scrub.missing_replicas,
                    scrub.corrupt_replicas,
                    scrub.slow_replicas
                ),
            );
            let report = planner
                .plan(group, &states)
                .expect("simulated repair plan must have a healthy source");
            runtime.repair_findings += report.findings.len();
            runtime.repair_actions += report.actions.len();

            if report.findings.is_empty() {
                trace.push(
                    "repair",
                    "healthy",
                    format!("pg={} all replicas passed scrub", group.id.0),
                );
            }
            for finding in &report.findings {
                trace.push(
                    "repair",
                    "finding",
                    format!(
                        "pg={} node={} kind={} detail={}",
                        finding.pg.0,
                        finding
                            .node
                            .map(|node| node.0.to_string())
                            .unwrap_or_else(|| "cluster".to_string()),
                        finding_kind(finding.kind),
                        finding.detail
                    ),
                );
            }
            for action in report.actions.clone() {
                if matches!(action, RepairAction::QuarantineReplica { .. }) {
                    let quarantined = planner
                        .execute_quarantine(group, &states, &action)
                        .expect("simulated corrupt replica quarantine must isolate replica");
                    trace.push(
                        "repair",
                        "quarantine-execute",
                        format!(
                            "pg={} node={} bytes={} checksum={} reason={}",
                            quarantined.pg.0,
                            quarantined.node.0,
                            quarantined.bytes,
                            hex_backup_digest(&quarantined.checksum.bytes),
                            quarantined.reason
                        ),
                    );
                }
                if matches!(action, RepairAction::CopyReplica { .. }) {
                    let copied = planner
                        .execute_copy(group, &states, &action)
                        .expect("simulated repair copy must restore replica");
                    trace.push(
                        "repair",
                        "copy-execute",
                        format!(
                            "pg={} from={} to={} bytes={} checksum={}",
                            copied.pg.0,
                            copied.from.0,
                            copied.to.0,
                            copied.bytes,
                            hex_backup_digest(&copied.checksum.bytes)
                        ),
                    );
                }
                if matches!(action, RepairAction::RestoreReplication { .. }) {
                    let restored = planner
                        .execute_under_replication_repair(group, &states, &action, &report)
                        .expect("simulated under-replication repair must restore target");
                    trace.push(
                        "repair",
                        "under-replication-execute",
                        format!(
                            "pg={} healthy_before={} expected={} deficit={} copy_repairs={} quarantine_repairs={} demote_repairs={} projected_healthy={}",
                            restored.pg.0,
                            restored.healthy_before,
                            restored.expected,
                            restored.deficit,
                            restored.copy_repairs,
                            restored.quarantine_repairs,
                            restored.demote_repairs,
                            restored.projected_healthy_replicas
                        ),
                    );
                }
                trace.push("repair", "action", action_summary(action));
            }
        }
    }

    fn simulated_states(pg: PlacementGroupId) -> Vec<ReplicaState> {
        match pg.0 {
            1 => vec![
                Self::replica(0, true, true, 0),
                Self::replica(1, true, true, 0),
                Self::replica(2, true, true, 0),
            ],
            2 => vec![
                Self::replica(1, true, true, 0),
                Self::replica(2, true, false, 0),
                Self::replica(3, true, true, 0),
            ],
            _ => vec![
                Self::replica(2, true, true, 0),
                Self::replica(3, true, true, 64),
            ],
        }
    }

    fn replica(node: u64, present: bool, digest_ok: bool, lag_bytes: u64) -> ReplicaState {
        ReplicaState {
            node: rsf_core::NodeId(node),
            present,
            digest_ok,
            lag_bytes,
            durable_bytes: if present { 4096 } else { 0 },
            checksum: Digest::blake3_bytes(format!("repair-replica-{node}").as_bytes()),
        }
    }
}

pub struct Rebalance;

impl Rebalance {
    pub fn run(trace: &mut Trace, runtime: &mut DatabaseRuntime) {
        let groups = runtime
            .admin
            .map_replicated_groups(3, 3)
            .expect("simulated rebalance placement groups must map");
        let plan = RebalancePlanner
            .plan(
                runtime.admin.cluster_map(),
                &groups,
                &Self::loads(),
                &RebalancePolicy {
                    high_watermark_bps: 8_000,
                    low_watermark_bps: 2_000,
                    min_replicas: 3,
                    max_moves: 1,
                },
            )
            .expect("simulated rebalance plan must preserve replica guarantees");

        runtime.rebalance_moves = plan.actions.len();
        runtime.rebalance_checks = plan.safety_checks.len();
        runtime.rebalance_policy_proofs = plan
            .policy_proofs
            .iter()
            .filter(|proof| proof.preserves_policy())
            .count();
        trace.push(
            "rebalance",
            "policy",
            "high=80% low=20% min_replicas=3 max_moves=1",
        );
        for check in &plan.safety_checks {
            trace.push(
                "rebalance",
                "check",
                format!("{} passed={}", check.name, check.passed),
            );
        }
        for proof in &plan.policy_proofs {
            trace.push(
                "rebalance",
                "proof",
                format!(
                    "pg={} previous_replicas={} resulting_replicas={} expected_replicas={} min_replicas={} source_was_member={} target_was_member={} target_is_storage={} no_duplicate_replicas={} previous_failure_domains={} resulting_failure_domains={} previous_regions={} resulting_regions={} preserves_policy={}",
                    proof.pg.0,
                    proof.previous_replicas,
                    proof.resulting_replicas,
                    proof.expected_replicas,
                    proof.min_replicas,
                    proof.source_was_member,
                    proof.target_was_member,
                    proof.target_is_storage,
                    proof.no_duplicate_replicas,
                    proof.previous_failure_domains,
                    proof.resulting_failure_domains,
                    proof.previous_regions,
                    proof.resulting_regions,
                    proof.preserves_policy()
                ),
            );
        }
        for action in &plan.actions {
            if let Some(group) = groups
                .iter()
                .find(|group| group.id == rebalance_action_pg(action))
            {
                if let Some(proof) = plan.policy_proofs.iter().find(|proof| proof.pg == group.id) {
                    let moved = RebalancePlanner
                        .execute_move(runtime.admin.cluster_map(), group, action, proof)
                        .expect("simulated placement-group movement must execute");
                    trace.push(
                        "rebalance",
                        "move-execute",
                        format!(
                            "pg={} from={} to={} epoch={}=>{} primary={}=>{} replicas={}=>{}",
                            moved.pg.0,
                            moved.from.0,
                            moved.to.0,
                            moved.previous_epoch.0,
                            moved.resulting_epoch.0,
                            moved.previous_primary.0,
                            moved.resulting_primary.0,
                            node_list(&moved.previous_replicas),
                            node_list(&moved.resulting_replicas)
                        ),
                    );
                }
            }
            trace.push("rebalance", "action", rebalance_action(action.clone()));
        }
    }

    fn loads() -> Vec<NodeLoad> {
        vec![
            NodeLoad {
                node: NodeId(0),
                used_bytes: 100,
                capacity_bytes: 1_000,
            },
            NodeLoad {
                node: NodeId(1),
                used_bytes: 500,
                capacity_bytes: 1_000,
            },
            NodeLoad {
                node: NodeId(2),
                used_bytes: 900,
                capacity_bytes: 1_000,
            },
            NodeLoad {
                node: NodeId(3),
                used_bytes: 500,
                capacity_bytes: 1_000,
            },
            NodeLoad {
                node: NodeId(4),
                used_bytes: 500,
                capacity_bytes: 1_000,
            },
        ]
    }
}

pub struct Observability;

pub struct Invariants;

impl Observability {
    pub fn finish(
        trace: &mut Trace,
        implementation: &Implementation,
        recovery: &Recovery,
        runtime: &DatabaseRuntime,
        nodes: usize,
        groups: usize,
    ) -> ProductionRunState {
        let object_metadata = runtime.object_metadata.report();
        let session_transactions = runtime.session_transactions.report();
        let state = ProductionRunState {
            deployment_roles: runtime.deployment_roles as u8,
            deployment_endpoints: runtime.deployment_endpoints as u8,
            deployment_volumes: runtime.deployment_volumes as u8,
            deployment_probes: runtime.deployment_probes as u8,
            upgrade_steps: runtime.upgrade_steps as u8,
            upgrade_checks: runtime.upgrade_checks as u8,
            upgrade_nodes: runtime.upgrade_nodes as u8,
            upgrade_rejections: runtime.upgrade_rejections as u8,
            admitted_nodes: nodes as u8,
            membership_epoch: runtime.membership_epoch as u8,
            membership_voters: runtime.membership_voters as u8,
            membership_learners: runtime.membership_learners as u8,
            membership_promotions: runtime.membership_promotions as u8,
            membership_intersections: runtime.membership_intersections as u8,
            membership_rejections: runtime.membership_rejections as u8,
            vsr_view: runtime.vsr_view as u8,
            vsr_prepares: runtime.vsr_prepares as u8,
            vsr_acks: runtime.vsr_acks as u8,
            vsr_commits: runtime.vsr_commits as u8,
            vsr_view_changes: runtime.vsr_view_changes as u8,
            vsr_rejections: runtime.vsr_rejections as u8,
            placement_groups: groups as u8,
            tenants: runtime.tenancy.tenant_count() as u8,
            tenant_databases: runtime.tenancy.database_count() as u8,
            tenant_namespaces: runtime.tenancy.namespace_count() as u8,
            tenant_surfaces: runtime.tenant_surfaces as u8,
            catalog_items: runtime.catalog.item_count() as u8,
            auth_grants: runtime.auth.grant_count() as u8,
            authz_allowed: runtime.authz_allowed as u8,
            authz_denied: runtime.authz_denied as u8,
            wire_frames: runtime.wire_frames as u8,
            wire_protocols: runtime.wire_protocols as u8,
            wire_rejections: runtime.wire_rejections as u8,
            wire_body_bytes: runtime.wire_body_bytes as u16,
            provider_records: runtime.provider_records as u8,
            provider_selected: runtime.provider_selected as u8,
            provider_degraded: runtime.provider_degraded as u8,
            provider_regions: runtime.provider_regions as u8,
            rpc_calls: runtime.rpc_calls as u8,
            rpc_retries: runtime.rpc_retries as u8,
            rpc_rejections: runtime.rpc_rejections as u8,
            rpc_body_bytes: runtime.rpc_body_bytes as u16,
            api_accepted: runtime.api_accepted as u8,
            api_rejected: runtime.api_rejected as u8,
            idempotency_first_seen: runtime.idempotency_first_seen as u8,
            idempotency_replays: runtime.idempotency_replays as u8,
            idempotency_conflicts: runtime.idempotency_conflicts as u8,
            idempotency_retries: runtime.idempotency_retries as u8,
            gateway_routes: runtime.gateway_routes as u8,
            gateway_rejections: runtime.gateway_rejections as u8,
            gateway_surfaces: runtime.gateway_surfaces as u8,
            consistency_strict: runtime.consistency_strict as u8,
            consistency_availability_first: runtime.consistency_availability_first as u8,
            consistency_rejections: runtime.consistency_rejections as u8,
            quota_allowed: runtime.quota_allowed as u8,
            quota_rejected: runtime.quota_rejected as u8,
            quota_policies: runtime.quota_policies as u8,
            quota_namespaces: runtime.quota_namespaces as u8,
            cache_hits: runtime.cache_hits as u8,
            cache_misses: runtime.cache_misses as u8,
            cache_evictions: runtime.cache_evictions as u8,
            cache_hints: runtime.cache_hints as u8,
            workload_classes: runtime.workload_classes as u8,
            workload_budget_paths: runtime.workload_budget_paths as u8,
            workload_benchmark_claim: runtime.workload_benchmark_claim as u8,
            backup_items: runtime.backup_items as u8,
            restore_checks: runtime.restore_checks as u8,
            restore_executed_items: runtime.restore_executed_items as u8,
            restore_executed_surfaces: runtime.restore_executed_surfaces as u8,
            restore_executed_bytes: runtime.restore_executed_bytes as u16,
            recovery_roots: runtime.recovery_roots as u8,
            recovery_replays: runtime.recovery_replays as u8,
            recovery_rejections: runtime.recovery_rejections as u8,
            recovery_actions: runtime.recovery_actions as u8,
            lease_grants: runtime.lease_grants as u8,
            fence_rejections: runtime.fence_rejections as u8,
            rebalance_moves: runtime.rebalance_moves as u8,
            rebalance_checks: runtime.rebalance_checks as u8,
            rebalance_policy_proofs: runtime.rebalance_policy_proofs as u8,
            fault_events: runtime.fault_events as u8,
            dropped_messages: runtime.dropped_messages as u8,
            delayed_messages: runtime.delayed_messages as u8,
            failure_drill_passed: runtime.failure_drill_passed as u8,
            failure_drill_domains: runtime.failure_drill_domains as u8,
            health_healthy: runtime.health_healthy as u8,
            health_degraded: runtime.health_degraded as u8,
            health_demote: runtime.health_demote as u8,
            health_actions: runtime.health_actions as u8,
            schema_steps: runtime.schema_steps as u8,
            schema_backfills: runtime.schema_backfills as u8,
            schema_rejected: runtime.schema_rejected as u8,
            migration_begun: runtime.migration_begun as u8,
            migration_validated: runtime.migration_validated as u8,
            migration_published: runtime.migration_published as u8,
            migration_backfilled_rows: runtime.migration_backfilled_rows as u16,
            migration_indexed_keys: runtime.migration_indexed_keys as u16,
            migration_resumed: runtime.migration_resumed as u8,
            migration_completed: runtime.migration_completed as u8,
            migration_rolled_back: runtime.migration_rolled_back as u8,
            migration_rejected: runtime.migration_rejected as u8,
            jobs_enqueued: runtime.jobs_enqueued as u8,
            jobs_leased: runtime.jobs_leased as u8,
            jobs_completed: runtime.jobs_completed as u8,
            jobs_retried: runtime.jobs_retried as u8,
            jobs_blocked: runtime.jobs_blocked as u8,
            session_bound: runtime.session_bound as u8,
            session_authorized: runtime.session_authorized as u8,
            session_rejected: runtime.session_rejected as u8,
            sessiontxn_sessions: session_transactions.sessions as u8,
            sessiontxn_pending_writes: session_transactions.pending_writes as u8,
            sessiontxn_overlay_reads: session_transactions.overlay_reads as u8,
            sessiontxn_snapshot_reads: session_transactions.snapshot_reads as u8,
            sessiontxn_commits: session_transactions.commits as u8,
            sessiontxn_rollbacks: session_transactions.rollbacks as u8,
            sessiontxn_savepoints: session_transactions.savepoints as u8,
            sessiontxn_savepoint_rewinds: session_transactions.savepoint_rewinds as u8,
            sessiontxn_rejections: session_transactions.rejections as u8,
            prepared_statements: runtime.prepared_statements as u8,
            prepared_binds: runtime.prepared_binds as u8,
            prepared_invalidations: runtime.prepared_invalidations as u8,
            prepared_deallocations: runtime.prepared_deallocations as u8,
            prepared_evictions: runtime.prepared_evictions as u8,
            prepared_rejections: runtime.prepared_rejections as u8,
            portal_parses: runtime.portal_parses as u8,
            portal_binds: runtime.portal_binds as u8,
            portal_describes: runtime.portal_describes as u8,
            portal_executes: runtime.portal_executes as u8,
            portal_rows: runtime.portal_rows as u8,
            portal_cancellations: runtime.portal_cancellations as u8,
            portal_closes: runtime.portal_closes as u8,
            portal_statement_closes: runtime.portal_statement_closes as u8,
            portal_rejections: runtime.portal_rejections as u8,
            rowcodec_encoded: runtime.rowcodec_encoded as u8,
            rowcodec_decoded: runtime.rowcodec_decoded as u8,
            rowcodec_defaults: runtime.rowcodec_defaults as u8,
            rowcodec_generated: runtime.rowcodec_generated as u8,
            rowcodec_rejections: runtime.rowcodec_rejections as u8,
            rowcodec_bytes: runtime.rowcodec_bytes as u16,
            tuple_versions: runtime.tuple_versions as u8,
            tuple_visible_reads: runtime.tuple_visible_reads as u8,
            tuple_delete_markers: runtime.tuple_delete_markers as u8,
            tuple_rejections: runtime.tuple_rejections as u8,
            tuple_bytes: runtime.tuple_bytes as u16,
            vacuum_chains: runtime.vacuum_chains as u8,
            vacuum_pruned_versions: runtime.vacuum_pruned_versions as u8,
            vacuum_retained_versions: runtime.vacuum_retained_versions as u8,
            vacuum_rejections: runtime.vacuum_rejections as u8,
            vacuum_reclaimed_bytes: runtime.vacuum_reclaimed_bytes as u16,
            materialized_rows: runtime.materialized_rows as u8,
            materialized_projected: runtime.materialized_projected as u8,
            materialized_columns: runtime.materialized_columns as u8,
            materialized_rejections: runtime.materialized_rejections as u8,
            materialized_bytes: runtime.materialized_bytes as u16,
            primary_inserts: runtime.primary_inserts as u8,
            primary_lookups: runtime.primary_lookups as u8,
            primary_updates: runtime.primary_updates as u8,
            primary_deletes: runtime.primary_deletes as u8,
            primary_rejections: runtime.primary_rejections as u8,
            primary_entries: runtime.primary_entries as u8,
            index_mutations: runtime.index_mutations as u8,
            index_entries: runtime.index_entries as u8,
            index_rebuilds: runtime.index_rebuilds as u8,
            planner_physical_plans: runtime.planner_physical_plans as u8,
            planner_index_choices: runtime.planner_index_choices as u8,
            planner_range_scans: runtime.planner_range_scans as u8,
            planner_stream_reads: runtime.planner_stream_reads as u8,
            executor_plans: runtime.executor_plans as u8,
            executor_stages: runtime.executor_stages as u8,
            executor_rows: runtime.executor_rows as u8,
            executor_remote_reads: runtime.executor_remote_reads as u8,
            sql_pipeline_bound: runtime.sql_pipeline_bound as u8,
            sql_pipeline_authorized: runtime.sql_pipeline_authorized as u8,
            sql_pipeline_logical: runtime.sql_pipeline_logical as u8,
            sql_pipeline_physical: runtime.sql_pipeline_physical as u8,
            sql_pipeline_stages: runtime.sql_pipeline_stages as u8,
            sql_pipeline_rows: runtime.sql_pipeline_rows as u8,
            sql_pipeline_remote_reads: runtime.sql_pipeline_remote_reads as u8,
            sql_pipeline_rejections: runtime.sql_pipeline_rejections as u8,
            sql_dml_bound: runtime.sql_dml_bound as u8,
            sql_dml_authorized: runtime.sql_dml_authorized as u8,
            sql_dml_planned: runtime.sql_dml_planned as u8,
            sql_dml_prepared: runtime.sql_dml_prepared as u8,
            sql_dml_committed: runtime.sql_dml_committed as u8,
            sql_dml_isolated: runtime.sql_dml_isolated as u8,
            sql_dml_kv_applied: runtime.sql_dml_kv_applied as u8,
            sql_dml_session_staged: runtime.sql_dml_session_staged as u8,
            sql_dml_overlay_verified: runtime.sql_dml_overlay_verified as u8,
            sql_dml_rejections: runtime.sql_dml_rejections as u8,
            sql_txn_began: runtime.sql_txn_began as u8,
            sql_txn_dml_staged: runtime.sql_txn_dml_staged as u8,
            sql_txn_overlay_reads: runtime.sql_txn_overlay_reads as u8,
            sql_txn_commits: runtime.sql_txn_commits as u8,
            sql_txn_rollbacks: runtime.sql_txn_rollbacks as u8,
            sql_txn_rollback_absent: runtime.sql_txn_rollback_absent as u8,
            sql_txn_savepoints: runtime.sql_txn_savepoints as u8,
            sql_txn_savepoint_rewinds: runtime.sql_txn_savepoint_rewinds as u8,
            sql_txn_savepoint_absent: runtime.sql_txn_savepoint_absent as u8,
            sql_txn_rejections: runtime.sql_txn_rejections as u8,
            range_partitions: runtime.range_partitions as u8,
            range_non_overlaps: runtime.range_non_overlaps as u8,
            range_rejections: runtime.range_rejections as u8,
            sstable_manifests: runtime.sstable_manifests as u8,
            compaction_inputs: runtime.compaction_inputs as u8,
            compaction_outputs: runtime.compaction_outputs as u8,
            compaction_live_keys: runtime.compaction_live_keys as u8,
            compaction_tombstones: runtime.compaction_tombstones as u8,
            compaction_reclaimed_bytes: runtime.compaction_reclaimed_bytes as u16,
            stats_tables: runtime.stats.table_count() as u8,
            stats_rows: runtime.stats_rows as u16,
            stats_histogram_buckets: runtime.stats_histogram_buckets as u8,
            stats_stale: runtime.stats_stale as u8,
            constraints_checked: runtime.constraints_checked as u8,
            constraints_accepted: runtime.constraints_accepted as u8,
            constraints_rejected: runtime.constraints_rejected as u8,
            constraints_unique_keys: runtime.constraints_unique_keys as u8,
            deferred_transactions: runtime.deferred_transactions as u8,
            deferred_obligations: runtime.deferred_obligations as u8,
            deferred_txn_references: runtime.deferred_txn_references as u8,
            deferred_committed_references: runtime.deferred_committed_references as u8,
            deferred_commits: runtime.deferred_commits as u8,
            deferred_rejections: runtime.deferred_rejections as u8,
            write_plans: runtime.write_plans as u8,
            write_steps: runtime.write_steps as u8,
            write_index_mutations: runtime.write_index_mutations as u8,
            write_conflict_ranges: runtime.write_conflict_ranges as u8,
            relcommit_groups: runtime.relcommit_groups as u8,
            relcommit_row_mutations: runtime.relcommit_row_mutations as u8,
            relcommit_index_mutations: runtime.relcommit_index_mutations as u8,
            relcommit_conflict_ranges: runtime.relcommit_conflict_ranges as u8,
            relcommit_rejections: runtime.relcommit_rejections as u8,
            page_frames: runtime.page_frames as u8,
            page_redo: runtime.page_redo as u8,
            page_reads: runtime.page_reads as u8,
            page_rejections: runtime.page_rejections as u8,
            page_checkpoint_bytes: runtime.page_checkpoint_bytes as u16,
            wal_appends: runtime.wal_appends as u8,
            wal_syncs: runtime.wal_syncs as u8,
            wal_flush_allows: runtime.wal_flush_allows as u8,
            wal_rejections: runtime.wal_rejections as u8,
            wal_durable_lsn: runtime.wal_durable_lsn as u8,
            wal_bytes: runtime.wal_bytes as u16,
            buffer_resident: runtime.buffer_resident as u8,
            buffer_hits: runtime.buffer_hits as u8,
            buffer_dirty: runtime.buffer_dirty as u8,
            buffer_flushes: runtime.buffer_flushes as u8,
            buffer_evictions: runtime.buffer_evictions as u8,
            buffer_rejections: runtime.buffer_rejections as u8,
            coordinator_transactions: runtime.transactions.committed().len() as u8,
            isolation_commits: runtime.isolation_commits as u8,
            isolation_rejections: runtime.isolation_rejections as u8,
            isolation_edges: runtime.isolation_edges as u8,
            isolation_ranges: runtime.isolation_ranges as u8,
            query_rows: runtime.query_rows as u8,
            kv_keys: runtime.kv.visible_key_count() as u8,
            kv_changes: runtime.kv.changes_since(CommitVersion(0)).len() as u8,
            watch_registered: runtime.watch_registered as u8,
            watch_deliveries: runtime.watch_deliveries as u8,
            watch_resumes: runtime.watch_resumes as u8,
            relational_rows: runtime.sql.row_count() as u8,
            object_versions: 1,
            objectmeta_buckets: object_metadata.buckets as u8,
            objectmeta_uploads: object_metadata.uploads_started as u8,
            objectmeta_parts: object_metadata.parts_accepted as u8,
            objectmeta_heads: object_metadata.visible_heads as u8,
            objectmeta_rejections: object_metadata.rejections as u8,
            blob_chunks: 1,
            provider_content: runtime.provider_content as u8,
            stream_write_chunks: runtime.stream_write_chunks as u8,
            stream_read_lanes: runtime.stream_read_lanes as u8,
            stream_window_rejections: runtime.stream_window_rejections as u8,
            stream_verified_parts: runtime.stream_verified_parts as u8,
            erasure_data_shards: runtime.erasure_data_shards as u8,
            erasure_parity_shards: runtime.erasure_parity_shards as u8,
            erasure_tolerated_loss: runtime.erasure_tolerated_loss as u8,
            erasure_reconstructed_bytes: runtime.erasure_reconstructed_bytes as u8,
            manifest_entries: runtime.manifest_entries as u8,
            manifest_surfaces: runtime.manifest_surfaces as u8,
            manifest_bytes: runtime.manifest_bytes as u16,
            manifest_rejections: runtime.manifest_rejections as u8,
            chunkstore_chunks: runtime.chunkstore_chunks as u8,
            chunkstore_pinned: runtime.chunkstore_pinned as u8,
            chunkstore_bytes: runtime.chunkstore_bytes as u16,
            chunkstore_verified: runtime.chunkstore_verified as u8,
            chunkstore_deduped: runtime.chunkstore_deduped as u8,
            chunkstore_rejections: runtime.chunkstore_rejections as u8,
            checkpoint_roots: runtime.checkpoint_roots as u8,
            checkpoint_segments: runtime.checkpoint_segments as u8,
            checkpoint_compacted: runtime.checkpoint_compacted as u8,
            checkpoint_reclaimable_bytes: runtime.checkpoint_reclaimable_bytes as u16,
            checkpoint_rejections: runtime.checkpoint_rejections as u8,
            superblock_generation: runtime.superblock_generation as u8,
            superblock_checkpoint: runtime.superblock_checkpoint as u8,
            superblock_roots: runtime.superblock_roots as u8,
            superblock_advances: runtime.superblock_advances as u8,
            superblock_rejections: runtime.superblock_rejections as u8,
            snapshot_roots: runtime.snapshot_roots as u8,
            snapshot_barriers: runtime.snapshot_barriers as u8,
            snapshot_validations: runtime.snapshot_validations as u8,
            protocol_commits: runtime.protocol_commits() as u8,
            committed_transactions: recovery
                .pg_tail_summaries()
                .iter()
                .filter(|summary| recovery.pg_log(summary.pg).committed_tail.is_some())
                .count() as u8,
            storage_committed_prefixes: runtime.storage_committed_prefixes as u8,
            storage_manifest_covered: runtime.storage_manifest_covered as u8,
            repair_findings: runtime.repair_findings as u8,
            repair_actions: runtime.repair_actions as u8,
            retention_kept: runtime.retention_kept as u8,
            retention_actions: runtime.retention_actions as u8,
            testability_checks: 0,
            testability_passed: 0,
            testability_trace_events: 0,
            testability_trace_services: 0,
            testability_replay_ready: 0,
            requirement_checks: 0,
            requirement_covered: 0,
            invariant_checks: 0,
            invariant_passed: 0,
            invariant_failed: 0,
            observability_metrics: 0,
            observability_audits: 0,
            cluster_health: "unknown",
        };
        let mut registry = ObservabilityRegistry::default();
        registry.gauge("deployment.roles", state.deployment_roles.into());
        registry.gauge("deployment.endpoints", state.deployment_endpoints.into());
        registry.gauge("deployment.volumes", state.deployment_volumes.into());
        registry.gauge("deployment.probes", state.deployment_probes.into());
        registry.gauge("upgrade.steps", state.upgrade_steps.into());
        registry.gauge("upgrade.checks", state.upgrade_checks.into());
        registry.gauge("upgrade.nodes", state.upgrade_nodes.into());
        registry.incr("upgrade.rejections", state.upgrade_rejections.into());
        registry.gauge("cluster.nodes", state.admitted_nodes.into());
        registry.gauge("membership.epoch", state.membership_epoch.into());
        registry.gauge("membership.voters", state.membership_voters.into());
        registry.gauge("membership.learners", state.membership_learners.into());
        registry.incr("membership.promotions", state.membership_promotions.into());
        registry.gauge(
            "membership.quorum_intersections",
            state.membership_intersections.into(),
        );
        registry.incr("membership.rejections", state.membership_rejections.into());
        registry.gauge("vsr.view", state.vsr_view.into());
        registry.gauge("vsr.prepares", state.vsr_prepares.into());
        registry.incr("vsr.acks", state.vsr_acks.into());
        registry.incr("vsr.commits", state.vsr_commits.into());
        registry.incr("vsr.view_changes", state.vsr_view_changes.into());
        registry.incr("vsr.rejections", state.vsr_rejections.into());
        registry.gauge("cluster.placement_groups", state.placement_groups.into());
        registry.gauge("tenancy.tenants", state.tenants.into());
        registry.gauge("tenancy.databases", state.tenant_databases.into());
        registry.gauge("tenancy.namespaces", state.tenant_namespaces.into());
        registry.gauge("tenancy.surfaces", state.tenant_surfaces.into());
        registry.gauge("catalog.items", state.catalog_items.into());
        registry.gauge("auth.grants", state.auth_grants.into());
        registry.gauge("wire.frames", state.wire_frames.into());
        registry.gauge("wire.protocols", state.wire_protocols.into());
        registry.incr("wire.rejections", state.wire_rejections.into());
        registry.gauge("wire.body_bytes", state.wire_body_bytes.into());
        registry.gauge("provider.records", state.provider_records.into());
        registry.gauge("provider.content", state.provider_content.into());
        registry.gauge("provider.regions", state.provider_regions.into());
        registry.gauge("provider.selected", state.provider_selected.into());
        registry.incr("provider.degraded", state.provider_degraded.into());
        registry.gauge("rpc.calls", state.rpc_calls.into());
        registry.incr("rpc.retries", state.rpc_retries.into());
        registry.incr("rpc.rejections", state.rpc_rejections.into());
        registry.gauge("rpc.body_bytes", state.rpc_body_bytes.into());
        registry.incr("api.accepted", state.api_accepted.into());
        registry.incr("api.rejected", state.api_rejected.into());
        registry.incr(
            "idempotency.first_seen",
            state.idempotency_first_seen.into(),
        );
        registry.incr("idempotency.replays", state.idempotency_replays.into());
        registry.incr("idempotency.conflicts", state.idempotency_conflicts.into());
        registry.incr("idempotency.retries", state.idempotency_retries.into());
        registry.incr("gateway.routes", state.gateway_routes.into());
        registry.incr("gateway.rejections", state.gateway_rejections.into());
        registry.gauge("gateway.surfaces", state.gateway_surfaces.into());
        registry.gauge("consistency.strict", state.consistency_strict.into());
        registry.gauge(
            "consistency.availability_first",
            state.consistency_availability_first.into(),
        );
        registry.incr(
            "consistency.rejections",
            state.consistency_rejections.into(),
        );
        registry.incr("quota.allowed", state.quota_allowed.into());
        registry.incr("quota.rejected", state.quota_rejected.into());
        registry.gauge("quota.policies", state.quota_policies.into());
        registry.gauge("quota.namespaces", state.quota_namespaces.into());
        registry.incr("cache.hits", state.cache_hits.into());
        registry.incr("cache.misses", state.cache_misses.into());
        registry.incr("cache.evictions", state.cache_evictions.into());
        registry.gauge("cache.hints", state.cache_hints.into());
        registry.gauge("workload.classes", state.workload_classes.into());
        registry.gauge("workload.budget_paths", state.workload_budget_paths.into());
        registry.gauge(
            "workload.benchmark_claim",
            state.workload_benchmark_claim.into(),
        );
        registry.gauge("stream.write_chunks", state.stream_write_chunks.into());
        registry.gauge("stream.read_lanes", state.stream_read_lanes.into());
        registry.incr(
            "stream.window_rejections",
            state.stream_window_rejections.into(),
        );
        registry.incr("stream.verified_parts", state.stream_verified_parts.into());
        registry.gauge("backup.items", state.backup_items.into());
        registry.gauge("restore.checks", state.restore_checks.into());
        registry.gauge(
            "restore.executed_items",
            state.restore_executed_items.into(),
        );
        registry.gauge(
            "restore.executed_surfaces",
            state.restore_executed_surfaces.into(),
        );
        registry.gauge(
            "restore.executed_bytes",
            state.restore_executed_bytes.into(),
        );
        registry.gauge("recovery.roots", state.recovery_roots.into());
        registry.gauge("recovery.replays", state.recovery_replays.into());
        registry.incr("recovery.rejections", state.recovery_rejections.into());
        registry.gauge("recovery.actions", state.recovery_actions.into());
        registry.gauge("lease.grants", state.lease_grants.into());
        registry.incr("fence.rejections", state.fence_rejections.into());
        registry.gauge("rebalance.moves", state.rebalance_moves.into());
        registry.gauge("rebalance.checks", state.rebalance_checks.into());
        registry.gauge(
            "rebalance.policy_proofs",
            state.rebalance_policy_proofs.into(),
        );
        registry.gauge("fault.events", state.fault_events.into());
        registry.incr("network.dropped", state.dropped_messages.into());
        registry.incr("network.delayed", state.delayed_messages.into());
        registry.gauge("failure_drill.passed", state.failure_drill_passed.into());
        registry.gauge("failure_drill.domains", state.failure_drill_domains.into());
        registry.gauge("health.healthy", state.health_healthy.into());
        registry.gauge("health.degraded", state.health_degraded.into());
        registry.gauge("health.demote", state.health_demote.into());
        registry.incr("health.actions", state.health_actions.into());
        registry.gauge("schema.steps", state.schema_steps.into());
        registry.gauge("schema.backfills", state.schema_backfills.into());
        registry.incr("schema.rejected", state.schema_rejected.into());
        registry.gauge("migration.begun", state.migration_begun.into());
        registry.gauge("migration.validated", state.migration_validated.into());
        registry.gauge("migration.published", state.migration_published.into());
        registry.gauge(
            "migration.backfilled_rows",
            state.migration_backfilled_rows.into(),
        );
        registry.gauge(
            "migration.indexed_keys",
            state.migration_indexed_keys.into(),
        );
        registry.gauge("migration.resumed", state.migration_resumed.into());
        registry.gauge("migration.completed", state.migration_completed.into());
        registry.gauge("migration.rolled_back", state.migration_rolled_back.into());
        registry.incr("migration.rejected", state.migration_rejected.into());
        registry.gauge("jobs.enqueued", state.jobs_enqueued.into());
        registry.gauge("jobs.leased", state.jobs_leased.into());
        registry.incr("jobs.completed", state.jobs_completed.into());
        registry.incr("jobs.retried", state.jobs_retried.into());
        registry.gauge("jobs.blocked", state.jobs_blocked.into());
        registry.gauge("session.bound", state.session_bound.into());
        registry.incr("session.authorized", state.session_authorized.into());
        registry.incr("session.rejected", state.session_rejected.into());
        registry.gauge("sessiontxn.sessions", state.sessiontxn_sessions.into());
        registry.gauge(
            "sessiontxn.pending_writes",
            state.sessiontxn_pending_writes.into(),
        );
        registry.incr(
            "sessiontxn.overlay_reads",
            state.sessiontxn_overlay_reads.into(),
        );
        registry.incr(
            "sessiontxn.snapshot_reads",
            state.sessiontxn_snapshot_reads.into(),
        );
        registry.incr("sessiontxn.commits", state.sessiontxn_commits.into());
        registry.incr("sessiontxn.rollbacks", state.sessiontxn_rollbacks.into());
        registry.incr("sessiontxn.savepoints", state.sessiontxn_savepoints.into());
        registry.incr(
            "sessiontxn.savepoint_rewinds",
            state.sessiontxn_savepoint_rewinds.into(),
        );
        registry.incr("sessiontxn.rejections", state.sessiontxn_rejections.into());
        registry.gauge("prepared.statements", state.prepared_statements.into());
        registry.incr("prepared.binds", state.prepared_binds.into());
        registry.incr(
            "prepared.invalidations",
            state.prepared_invalidations.into(),
        );
        registry.incr(
            "prepared.deallocations",
            state.prepared_deallocations.into(),
        );
        registry.incr("prepared.evictions", state.prepared_evictions.into());
        registry.incr("prepared.rejections", state.prepared_rejections.into());
        registry.incr("portal.parses", state.portal_parses.into());
        registry.incr("portal.binds", state.portal_binds.into());
        registry.incr("portal.describes", state.portal_describes.into());
        registry.incr("portal.executes", state.portal_executes.into());
        registry.gauge("portal.rows", state.portal_rows.into());
        registry.incr("portal.cancellations", state.portal_cancellations.into());
        registry.incr("portal.closes", state.portal_closes.into());
        registry.incr(
            "portal.statement_closes",
            state.portal_statement_closes.into(),
        );
        registry.incr("portal.rejections", state.portal_rejections.into());
        registry.incr("rowcodec.encoded", state.rowcodec_encoded.into());
        registry.incr("rowcodec.decoded", state.rowcodec_decoded.into());
        registry.incr("rowcodec.defaults", state.rowcodec_defaults.into());
        registry.incr("rowcodec.generated", state.rowcodec_generated.into());
        registry.incr("rowcodec.rejections", state.rowcodec_rejections.into());
        registry.gauge("rowcodec.bytes", state.rowcodec_bytes.into());
        registry.gauge("tuple.versions", state.tuple_versions.into());
        registry.incr("tuple.visible_reads", state.tuple_visible_reads.into());
        registry.incr("tuple.delete_markers", state.tuple_delete_markers.into());
        registry.incr("tuple.rejections", state.tuple_rejections.into());
        registry.gauge("tuple.bytes", state.tuple_bytes.into());
        registry.gauge("vacuum.chains", state.vacuum_chains.into());
        registry.incr(
            "vacuum.pruned_versions",
            state.vacuum_pruned_versions.into(),
        );
        registry.gauge(
            "vacuum.retained_versions",
            state.vacuum_retained_versions.into(),
        );
        registry.incr("vacuum.rejections", state.vacuum_rejections.into());
        registry.gauge(
            "vacuum.reclaimed_bytes",
            state.vacuum_reclaimed_bytes.into(),
        );
        registry.incr("materialize.rows", state.materialized_rows.into());
        registry.incr("materialize.projected", state.materialized_projected.into());
        registry.gauge("materialize.columns", state.materialized_columns.into());
        registry.incr(
            "materialize.rejections",
            state.materialized_rejections.into(),
        );
        registry.gauge("materialize.bytes", state.materialized_bytes.into());
        registry.incr("primary.inserts", state.primary_inserts.into());
        registry.incr("primary.lookups", state.primary_lookups.into());
        registry.incr("primary.updates", state.primary_updates.into());
        registry.incr("primary.deletes", state.primary_deletes.into());
        registry.incr("primary.rejections", state.primary_rejections.into());
        registry.gauge("primary.entries", state.primary_entries.into());
        registry.incr("index.mutations", state.index_mutations.into());
        registry.gauge("index.entries", state.index_entries.into());
        registry.incr("index.rebuilds", state.index_rebuilds.into());
        registry.gauge(
            "planner.physical_plans",
            state.planner_physical_plans.into(),
        );
        registry.gauge("planner.index_choices", state.planner_index_choices.into());
        registry.gauge("planner.range_scans", state.planner_range_scans.into());
        registry.gauge("planner.stream_reads", state.planner_stream_reads.into());
        registry.gauge("executor.plans", state.executor_plans.into());
        registry.gauge("executor.stages", state.executor_stages.into());
        registry.gauge("executor.rows", state.executor_rows.into());
        registry.gauge("executor.remote_reads", state.executor_remote_reads.into());
        registry.gauge("sqlpipeline.bound", state.sql_pipeline_bound.into());
        registry.incr(
            "sqlpipeline.authorized",
            state.sql_pipeline_authorized.into(),
        );
        registry.gauge("sqlpipeline.logical", state.sql_pipeline_logical.into());
        registry.gauge("sqlpipeline.physical", state.sql_pipeline_physical.into());
        registry.gauge("sqlpipeline.stages", state.sql_pipeline_stages.into());
        registry.gauge("sqlpipeline.rows", state.sql_pipeline_rows.into());
        registry.gauge(
            "sqlpipeline.remote_reads",
            state.sql_pipeline_remote_reads.into(),
        );
        registry.incr(
            "sqlpipeline.rejections",
            state.sql_pipeline_rejections.into(),
        );
        registry.gauge("sqldml.bound", state.sql_dml_bound.into());
        registry.incr("sqldml.authorized", state.sql_dml_authorized.into());
        registry.gauge("sqldml.planned", state.sql_dml_planned.into());
        registry.gauge("sqldml.prepared", state.sql_dml_prepared.into());
        registry.incr("sqldml.committed", state.sql_dml_committed.into());
        registry.incr("sqldml.isolated", state.sql_dml_isolated.into());
        registry.incr("sqldml.kv_applied", state.sql_dml_kv_applied.into());
        registry.incr("sqldml.session_staged", state.sql_dml_session_staged.into());
        registry.incr(
            "sqldml.overlay_verified",
            state.sql_dml_overlay_verified.into(),
        );
        registry.incr("sqldml.rejections", state.sql_dml_rejections.into());
        registry.incr("sqltxn.began", state.sql_txn_began.into());
        registry.incr("sqltxn.dml_staged", state.sql_txn_dml_staged.into());
        registry.incr("sqltxn.overlay_reads", state.sql_txn_overlay_reads.into());
        registry.incr("sqltxn.commits", state.sql_txn_commits.into());
        registry.incr("sqltxn.rollbacks", state.sql_txn_rollbacks.into());
        registry.incr(
            "sqltxn.rollback_absent",
            state.sql_txn_rollback_absent.into(),
        );
        registry.incr("sqltxn.savepoints", state.sql_txn_savepoints.into());
        registry.incr(
            "sqltxn.savepoint_rewinds",
            state.sql_txn_savepoint_rewinds.into(),
        );
        registry.incr(
            "sqltxn.savepoint_absent",
            state.sql_txn_savepoint_absent.into(),
        );
        registry.incr("sqltxn.rejections", state.sql_txn_rejections.into());
        registry.gauge("range.partitions", state.range_partitions.into());
        registry.gauge("range.non_overlaps", state.range_non_overlaps.into());
        registry.incr("range.rejections", state.range_rejections.into());
        registry.gauge("range.sstables", state.sstable_manifests.into());
        registry.gauge("range.compaction_inputs", state.compaction_inputs.into());
        registry.gauge("compaction.outputs", state.compaction_outputs.into());
        registry.gauge("compaction.live_keys", state.compaction_live_keys.into());
        registry.gauge("compaction.tombstones", state.compaction_tombstones.into());
        registry.gauge(
            "compaction.reclaimed_bytes",
            state.compaction_reclaimed_bytes.into(),
        );
        registry.gauge("stats.tables", state.stats_tables.into());
        registry.gauge("stats.rows", state.stats_rows.into());
        registry.gauge(
            "stats.histogram_buckets",
            state.stats_histogram_buckets.into(),
        );
        registry.incr("stats.stale", state.stats_stale.into());
        registry.gauge("constraints.checked", state.constraints_checked.into());
        registry.incr("constraints.accepted", state.constraints_accepted.into());
        registry.incr("constraints.rejected", state.constraints_rejected.into());
        registry.gauge(
            "constraints.unique_keys",
            state.constraints_unique_keys.into(),
        );
        registry.gauge("deferred.transactions", state.deferred_transactions.into());
        registry.gauge("deferred.obligations", state.deferred_obligations.into());
        registry.gauge(
            "deferred.txn_references",
            state.deferred_txn_references.into(),
        );
        registry.gauge(
            "deferred.committed_references",
            state.deferred_committed_references.into(),
        );
        registry.incr("deferred.commits", state.deferred_commits.into());
        registry.incr("deferred.rejections", state.deferred_rejections.into());
        registry.gauge("write.plans", state.write_plans.into());
        registry.gauge("write.steps", state.write_steps.into());
        registry.gauge("write.index_mutations", state.write_index_mutations.into());
        registry.gauge("write.conflict_ranges", state.write_conflict_ranges.into());
        registry.gauge("relcommit.groups", state.relcommit_groups.into());
        registry.gauge(
            "relcommit.row_mutations",
            state.relcommit_row_mutations.into(),
        );
        registry.gauge(
            "relcommit.index_mutations",
            state.relcommit_index_mutations.into(),
        );
        registry.gauge(
            "relcommit.conflict_ranges",
            state.relcommit_conflict_ranges.into(),
        );
        registry.incr("relcommit.rejections", state.relcommit_rejections.into());
        registry.gauge("page.frames", state.page_frames.into());
        registry.gauge("page.redo", state.page_redo.into());
        registry.gauge("page.reads", state.page_reads.into());
        registry.incr("page.rejections", state.page_rejections.into());
        registry.gauge("page.checkpoint_bytes", state.page_checkpoint_bytes.into());
        registry.gauge("wal.appends", state.wal_appends.into());
        registry.incr("wal.syncs", state.wal_syncs.into());
        registry.incr("wal.flush_allows", state.wal_flush_allows.into());
        registry.incr("wal.rejections", state.wal_rejections.into());
        registry.gauge("wal.durable_lsn", state.wal_durable_lsn.into());
        registry.gauge("wal.bytes", state.wal_bytes.into());
        registry.gauge("buffer.resident", state.buffer_resident.into());
        registry.incr("buffer.hits", state.buffer_hits.into());
        registry.gauge("buffer.dirty", state.buffer_dirty.into());
        registry.incr("buffer.flushes", state.buffer_flushes.into());
        registry.incr("buffer.evictions", state.buffer_evictions.into());
        registry.incr("buffer.rejections", state.buffer_rejections.into());
        registry.gauge(
            "transactions.committed",
            state.coordinator_transactions.into(),
        );
        registry.gauge("isolation.commits", state.isolation_commits.into());
        registry.incr("isolation.rejections", state.isolation_rejections.into());
        registry.gauge("isolation.edges", state.isolation_edges.into());
        registry.gauge("isolation.ranges", state.isolation_ranges.into());
        registry.gauge("query.rows", state.query_rows.into());
        registry.gauge("objectmeta.buckets", state.objectmeta_buckets.into());
        registry.gauge("objectmeta.uploads", state.objectmeta_uploads.into());
        registry.gauge("objectmeta.parts", state.objectmeta_parts.into());
        registry.gauge("objectmeta.heads", state.objectmeta_heads.into());
        registry.incr("objectmeta.rejections", state.objectmeta_rejections.into());
        registry.gauge("watch.registered", state.watch_registered.into());
        registry.incr("watch.deliveries", state.watch_deliveries.into());
        registry.incr("watch.resumes", state.watch_resumes.into());
        registry.gauge("erasure.data_shards", state.erasure_data_shards.into());
        registry.gauge("erasure.parity_shards", state.erasure_parity_shards.into());
        registry.gauge(
            "erasure.tolerated_loss",
            state.erasure_tolerated_loss.into(),
        );
        registry.gauge(
            "erasure.reconstructed_bytes",
            state.erasure_reconstructed_bytes.into(),
        );
        registry.gauge("manifest.entries", state.manifest_entries.into());
        registry.gauge("manifest.surfaces", state.manifest_surfaces.into());
        registry.gauge("manifest.bytes", state.manifest_bytes.into());
        registry.incr("manifest.rejections", state.manifest_rejections.into());
        registry.gauge("chunkstore.chunks", state.chunkstore_chunks.into());
        registry.gauge("chunkstore.pinned", state.chunkstore_pinned.into());
        registry.gauge("chunkstore.bytes", state.chunkstore_bytes.into());
        registry.incr("chunkstore.verified", state.chunkstore_verified.into());
        registry.incr("chunkstore.deduped", state.chunkstore_deduped.into());
        registry.incr("chunkstore.rejections", state.chunkstore_rejections.into());
        registry.gauge("checkpoint.roots", state.checkpoint_roots.into());
        registry.gauge("checkpoint.segments", state.checkpoint_segments.into());
        registry.gauge("checkpoint.compacted", state.checkpoint_compacted.into());
        registry.gauge(
            "checkpoint.reclaimable_bytes",
            state.checkpoint_reclaimable_bytes.into(),
        );
        registry.incr("checkpoint.rejections", state.checkpoint_rejections.into());
        registry.gauge("superblock.generation", state.superblock_generation.into());
        registry.gauge("superblock.checkpoint", state.superblock_checkpoint.into());
        registry.gauge("superblock.roots", state.superblock_roots.into());
        registry.incr("superblock.advances", state.superblock_advances.into());
        registry.incr("superblock.rejections", state.superblock_rejections.into());
        registry.gauge("snapshot.roots", state.snapshot_roots.into());
        registry.gauge("snapshot.barriers", state.snapshot_barriers.into());
        registry.incr("snapshot.validations", state.snapshot_validations.into());
        registry.incr("auth.allowed", state.authz_allowed.into());
        registry.incr("auth.denied", state.authz_denied.into());
        registry.incr("protocol.commits", state.protocol_commits.into());
        registry.incr(
            "storage.committed_tails",
            state.committed_transactions.into(),
        );
        registry.incr(
            "storage.committed_prefixes",
            state.storage_committed_prefixes.into(),
        );
        registry.incr(
            "storage.manifest_entries_covered",
            state.storage_manifest_covered.into(),
        );
        registry.incr("repair.findings", state.repair_findings.into());
        registry.incr("repair.actions", state.repair_actions.into());
        registry.incr("retention.kept", state.retention_kept.into());
        registry.incr("retention.actions", state.retention_actions.into());
        registry.set_health(
            "deployment",
            HealthStatus::Healthy,
            "deployment manifest assigned control-plane, gateway, metadata, storage, SQL, object, blob, and background-worker roles with endpoints, volumes, and readiness probes",
        );
        registry.set_health(
            "upgrade",
            HealthStatus::Healthy,
            "rolling upgrade planner validated compatibility, staged one-node-at-a-time drain/install/probe/promote work, and rejected unsafe cross-major rollout",
        );
        registry.set_health(
            "membership",
            HealthStatus::Healthy,
            "membership cell added a learner, waited for sync, proved promotion quorum intersection, promoted it through an epoch change, and rejected unsafe voter removal",
        );
        registry.set_health(
            "vsr",
            HealthStatus::Healthy,
            "VSR metadata cell prepared and committed quorum-backed records, changed view, and rejected a stale primary prepare",
        );
        registry.set_health(
            "wire",
            HealthStatus::Healthy,
            "wire adapters decoded SQL, KV, object, blob, and admin protocol frames before API authorization and rejected a mutating object frame without idempotency",
        );
        registry.set_health(
            "recovery",
            HealthStatus::Healthy,
            "database recovery planner validated required roots, replayed lagging roots, reused checkpoint roots, and rejected corrupted catalog restart",
        );
        registry.set_health(
            "manifest",
            HealthStatus::Healthy,
            "format-independent manifest validated relational, KV, object, blob, and snapshot extents before storage-fabric addressing",
        );
        registry.set_health(
            "storage",
            HealthStatus::Healthy,
            "recovered committed placement-group prefixes prove manifest extent coverage",
        );
        registry.set_health(
            "chunkstore",
            HealthStatus::Healthy,
            "immutable chunk store accepted format-tagged SQL, object, blob, SSTable, snapshot, and backup chunks with dedupe, pinning, verified reads, and malformed payload rejection",
        );
        registry.set_health(
            "checkpoint",
            HealthStatus::Healthy,
            "local checkpoint planner retained database roots, compacted eligible old segments, kept required/future segments, and rejected checkpoint regression",
        );
        registry.set_health(
            "superblock",
            HealthStatus::Healthy,
            "local boot record validated required roots, advanced generation at committed checkpoint, and rejected malformed root inventory",
        );
        registry.set_health(
            "isolation",
            HealthStatus::Healthy,
            "strict serializable validator accepted disjoint read/write ranges and rejected a stale read range",
        );
        registry.set_health(
            "stream",
            HealthStatus::Healthy,
            "chunked object/blob streaming produced bounded write chunks, parallel read lanes, flow-control backpressure, and hash verification",
        );
        registry.set_health(
            "cache",
            HealthStatus::Healthy,
            "read-serving cache admitted hot SQL, KV, object, and blob keys with version-aware misses and read-ahead hints",
        );
        registry.set_health(
            "quota",
            HealthStatus::Healthy,
            "namespace quota engine admitted SQL, KV, object, and blob work while rejecting oversized blob usage",
        );
        registry.set_health(
            "snapshot",
            HealthStatus::Healthy,
            "database snapshot coordinator froze catalog, transaction, KV, SQL, object, and blob roots",
        );
        registry.set_health(
            "index",
            HealthStatus::Healthy,
            "relational index planner produced rebuild, insert, update, delete, and lookup paths",
        );
        registry.set_health(
            "planner",
            HealthStatus::Healthy,
            "physical query planner selected primary-key lookup, secondary-index lookup, range scan, cache lookup, and stream read operators from simulated stats",
        );
        registry.set_health(
            "executor",
            HealthStatus::Healthy,
            "physical query executor consumed planner operators and simulated cache, index, range, and stream read stages",
        );
        registry.set_health(
            "sqlpipeline",
            HealthStatus::Healthy,
            "SQL pipeline bound and authorized a SELECT, lowered it into logical and physical plans, executed physical stages, materialized rows, and rejected an invalid projection",
        );
        registry.set_health(
            "gateway",
            HealthStatus::Healthy,
            "database gateway routed SQL, KV, object, and blob requests through catalog placement bindings",
        );
        registry.set_health(
            "tenancy",
            HealthStatus::Healthy,
            "tenant control plane created acme.prod, bound the app namespace, and validated enabled database surfaces before catalog publication",
        );
        registry.set_health(
            "idempotency",
            HealthStatus::Healthy,
            "user-facing idempotency ledger classified first-seen, replay, conflicting reuse, and retry scheduling before gateway routing",
        );
        registry.set_health(
            "watch",
            HealthStatus::Healthy,
            "ordered KV subscription delivered resumable prefix changes with tombstone propagation",
        );
        registry.set_health(
            "schema",
            HealthStatus::Healthy,
            "schema migration planner produced online rollout steps and rejected unsafe drop",
        );
        registry.set_health(
            "jobs",
            HealthStatus::Healthy,
            "background job queue enqueued schema, index, stats, compaction, and repair work with dependency gating and retry accounting",
        );
        registry.set_health(
            "health",
            HealthStatus::Degraded,
            "node health scoring produced slow-replica demotion and corruption quarantine actions",
        );
        registry.set_health(
            "objectmeta",
            HealthStatus::Healthy,
            "strict object metadata coordinator registered bucket state, tracked multipart draft parts, committed a visible version head, served the current head, and rejected an unknown upload",
        );
        registry.set_health(
            "erasure",
            HealthStatus::Healthy,
            "cold object body striped with erasure-coded shard manifest and recovery check",
        );
        registry.set_health(
            "session",
            HealthStatus::Healthy,
            "SQL session binding selected capability, consistency, transaction shape, and route",
        );
        registry.set_health(
            "sessiontxn",
            HealthStatus::Healthy,
            "strict session transaction overlay served pending writes before global visibility, preserved snapshot reads, committed through KV/protocol, and rejected a closed transaction read",
        );
        registry.set_health(
            "range",
            HealthStatus::Healthy,
            "ordered range split and SSTable compaction plan validated",
        );
        registry.set_health(
            "compaction",
            HealthStatus::Healthy,
            "SSTable compaction executor merged planned runs, retained newest values, dropped tombstoned keys, and emitted an output manifest",
        );
        registry.set_health(
            "stats",
            HealthStatus::Degraded,
            "statistics catalog published table estimates and histogram buckets, then flagged stale optimizer stats for refresh",
        );
        registry.set_health(
            "constraints",
            HealthStatus::Healthy,
            "relational constraint validator accepted one row and rejected duplicate unique-key and null required-column mutations",
        );
        registry.set_health(
            "write",
            HealthStatus::Healthy,
            "relational write planner ordered constraint checks, row KV bytes, secondary-index maintenance, conflict ranges, and commit intent",
        );
        registry.set_health(
            "wal",
            HealthStatus::Healthy,
            "relational WAL gate appended redo, rejected page flush before sync, advanced durable LSN, and allowed the page flush after fsync",
        );
        registry.set_health(
            "fault",
            HealthStatus::Healthy,
            "deterministic failure controls exercised delay, drop, heal, crash, and restart",
        );
        registry.set_health(
            "rebalance",
            HealthStatus::Healthy,
            "movement plan preserved replica guarantees",
        );
        registry.set_health(
            "lease",
            HealthStatus::Healthy,
            "primary fences rejected stale writer after view change",
        );
        registry.set_health(
            "backup",
            HealthStatus::Healthy,
            "snapshot manifest and restore plan validated database roots",
        );
        registry.set_health(
            "api",
            HealthStatus::Healthy,
            "gateway classified user requests and enforced authorization",
        );
        registry.set_health(
            "auth",
            HealthStatus::Healthy,
            "capability grants enforced for app and repair service identities",
        );
        registry.set_health(
            "catalog",
            HealthStatus::Healthy,
            "metadata quorum simulated",
        );
        registry.set_health(
            "query",
            HealthStatus::Healthy,
            "query executor returned rows",
        );
        registry.set_health(
            "repair",
            HealthStatus::Degraded,
            "scrub found under-replicated placement groups",
        );
        registry.set_health(
            "retention",
            HealthStatus::Healthy,
            "lifecycle policy evaluated object and blob records",
        );
        registry.audit(AuditEvent {
            service: "txn".to_string(),
            action: "commit".to_string(),
            version: Some(runtime.router.latest_version()),
            node: None,
            pg: Some(PlacementGroupId(1)),
            detail: "transaction coordinator allocated a database commit version".to_string(),
        });
        registry.audit(AuditEvent {
            service: "repair".to_string(),
            action: "plan".to_string(),
            version: None,
            node: None,
            pg: Some(PlacementGroupId(3)),
            detail: "repair planner produced copy and demotion actions".to_string(),
        });
        let report = registry.report();
        let state = ProductionRunState {
            observability_metrics: (report.counters + report.gauges) as u16,
            observability_audits: report.audit_events as u8,
            cluster_health: health_status(report.status),
            ..state
        };
        trace.push(
            "observability",
            "summary",
            format!(
                "{} complete: roles={} nodes={} pgs={} catalog_items={} txns={} query_rows={} storage_tx={} protocol_commits={} confidence={}%",
                implementation.id,
                state.deployment_roles,
                state.admitted_nodes,
                state.placement_groups,
                state.catalog_items,
                state.coordinator_transactions,
                state.query_rows,
                state.committed_transactions,
                state.protocol_commits,
                implementation.confidence
            ),
        );
        trace.push(
            "observability",
            "deployment",
            format!(
                "deployment roles={} endpoints={} volumes={} probes={}",
                state.deployment_roles,
                state.deployment_endpoints,
                state.deployment_volumes,
                state.deployment_probes
            ),
        );
        trace.push(
            "observability",
            "upgrade",
            format!(
                "upgrade steps={} checks={} nodes={} rejections={}",
                state.upgrade_steps,
                state.upgrade_checks,
                state.upgrade_nodes,
                state.upgrade_rejections
            ),
        );
        trace.push(
            "observability",
            "membership",
            format!(
                "membership epoch={} voters={} learners={} promotions={} quorum_intersections={} rejections={}",
                state.membership_epoch,
                state.membership_voters,
                state.membership_learners,
                state.membership_promotions,
                state.membership_intersections,
                state.membership_rejections
            ),
        );
        trace.push(
            "observability",
            "vsr",
            format!(
                "vsr view={} prepares={} acks={} commits={} view_changes={} rejections={}",
                state.vsr_view,
                state.vsr_prepares,
                state.vsr_acks,
                state.vsr_commits,
                state.vsr_view_changes,
                state.vsr_rejections
            ),
        );
        trace.push(
            "observability",
            "wire",
            format!(
                "wire frames={} protocols={} rejections={} body_bytes={}",
                state.wire_frames,
                state.wire_protocols,
                state.wire_rejections,
                state.wire_body_bytes
            ),
        );
        trace.push(
            "observability",
            "provider",
            format!(
                "provider records={} content={} selected={} degraded={} regions={}",
                state.provider_records,
                state.provider_content,
                state.provider_selected,
                state.provider_degraded,
                state.provider_regions
            ),
        );
        trace.push(
            "observability",
            "rpc",
            format!(
                "rpc calls={} retries={} rejections={} body_bytes={}",
                state.rpc_calls, state.rpc_retries, state.rpc_rejections, state.rpc_body_bytes
            ),
        );
        trace.push(
            "observability",
            "schema",
            format!(
                "schema steps={} backfills={} rejected={}",
                state.schema_steps, state.schema_backfills, state.schema_rejected
            ),
        );
        trace.push(
            "observability",
            "migration",
            format!(
                "migration begun={} validated={} published={} backfilled_rows={} indexed_keys={} resumed={} completed={} rolled_back={} rejected={}",
                state.migration_begun,
                state.migration_validated,
                state.migration_published,
                state.migration_backfilled_rows,
                state.migration_indexed_keys,
                state.migration_resumed,
                state.migration_completed,
                state.migration_rolled_back,
                state.migration_rejected
            ),
        );
        trace.push(
            "observability",
            "jobs",
            format!(
                "jobs enqueued={} leased={} completed={} retried={} blocked={}",
                state.jobs_enqueued,
                state.jobs_leased,
                state.jobs_completed,
                state.jobs_retried,
                state.jobs_blocked
            ),
        );
        trace.push(
            "observability",
            "tenancy",
            format!(
                "tenancy tenants={} databases={} namespaces={} surfaces={}",
                state.tenants,
                state.tenant_databases,
                state.tenant_namespaces,
                state.tenant_surfaces
            ),
        );
        trace.push(
            "observability",
            "health",
            format!(
                "health healthy={} degraded={} demote={} actions={}",
                state.health_healthy,
                state.health_degraded,
                state.health_demote,
                state.health_actions
            ),
        );
        trace.push(
            "observability",
            "quota",
            format!(
                "quota allowed={} rejected={} policies={} namespaces={}",
                state.quota_allowed,
                state.quota_rejected,
                state.quota_policies,
                state.quota_namespaces
            ),
        );
        trace.push(
            "observability",
            "isolation",
            format!(
                "isolation commits={} rejections={} edges={} ranges={}",
                state.isolation_commits,
                state.isolation_rejections,
                state.isolation_edges,
                state.isolation_ranges
            ),
        );
        trace.push(
            "observability",
            "cache",
            format!(
                "cache hits={} misses={} evictions={} hints={} workload_classes={} workload_budget_paths={} workload_benchmark_claim={}",
                state.cache_hits,
                state.cache_misses,
                state.cache_evictions,
                state.cache_hints,
                state.workload_classes,
                state.workload_budget_paths,
                state.workload_benchmark_claim
            ),
        );
        trace.push(
            "observability",
            "stream",
            format!(
                "stream write_chunks={} read_lanes={} window_rejections={} verified_parts={}",
                state.stream_write_chunks,
                state.stream_read_lanes,
                state.stream_window_rejections,
                state.stream_verified_parts
            ),
        );
        trace.push(
            "observability",
            "manifest",
            format!(
                "manifest entries={} surfaces={} bytes={} rejections={}",
                state.manifest_entries,
                state.manifest_surfaces,
                state.manifest_bytes,
                state.manifest_rejections
            ),
        );
        trace.push(
            "observability",
            "chunkstore",
            format!(
                "chunkstore chunks={} pinned={} bytes={} verified={} deduped={} rejections={}",
                state.chunkstore_chunks,
                state.chunkstore_pinned,
                state.chunkstore_bytes,
                state.chunkstore_verified,
                state.chunkstore_deduped,
                state.chunkstore_rejections
            ),
        );
        trace.push(
            "observability",
            "checkpoint",
            format!(
                "checkpoint roots={} segments={} compacted={} reclaimable_bytes={} rejections={}",
                state.checkpoint_roots,
                state.checkpoint_segments,
                state.checkpoint_compacted,
                state.checkpoint_reclaimable_bytes,
                state.checkpoint_rejections
            ),
        );
        trace.push(
            "observability",
            "superblock",
            format!(
                "superblock generation={} checkpoint={} roots={} advances={} rejections={}",
                state.superblock_generation,
                state.superblock_checkpoint,
                state.superblock_roots,
                state.superblock_advances,
                state.superblock_rejections
            ),
        );
        trace.push(
            "observability",
            "snapshot",
            format!(
                "snapshot roots={} barriers={} validations={}",
                state.snapshot_roots, state.snapshot_barriers, state.snapshot_validations
            ),
        );
        trace.push(
            "observability",
            "index",
            format!(
                "index mutations={} entries={} rebuilds={}",
                state.index_mutations, state.index_entries, state.index_rebuilds
            ),
        );
        trace.push(
            "observability",
            "sqlpipeline",
            format!(
                "sqlpipeline bound={} authorized={} logical={} physical={} stages={} rows={} remote_reads={} rejections={}",
                state.sql_pipeline_bound,
                state.sql_pipeline_authorized,
                state.sql_pipeline_logical,
                state.sql_pipeline_physical,
                state.sql_pipeline_stages,
                state.sql_pipeline_rows,
                state.sql_pipeline_remote_reads,
                state.sql_pipeline_rejections
            ),
        );
        trace.push(
            "observability",
            "sqldml",
            format!(
                "sqldml bound={} authorized={} planned={} prepared={} committed={} isolated={} kv_applied={} session_staged={} overlay_verified={} rejections={}",
                state.sql_dml_bound,
                state.sql_dml_authorized,
                state.sql_dml_planned,
                state.sql_dml_prepared,
                state.sql_dml_committed,
                state.sql_dml_isolated,
                state.sql_dml_kv_applied,
                state.sql_dml_session_staged,
                state.sql_dml_overlay_verified,
                state.sql_dml_rejections
            ),
        );
        trace.push(
            "observability",
            "sqltxn",
            format!(
                "sqltxn began={} dml_staged={} overlay_reads={} commits={} rollbacks={} rollback_absent={} savepoints={} savepoint_rewinds={} savepoint_absent={} rejections={}",
                state.sql_txn_began,
                state.sql_txn_dml_staged,
                state.sql_txn_overlay_reads,
                state.sql_txn_commits,
                state.sql_txn_rollbacks,
                state.sql_txn_rollback_absent,
                state.sql_txn_savepoints,
                state.sql_txn_savepoint_rewinds,
                state.sql_txn_savepoint_absent,
                state.sql_txn_rejections
            ),
        );
        trace.push(
            "observability",
            "gateway",
            format!(
                "gateway routes={} rejections={} surfaces={}",
                state.gateway_routes, state.gateway_rejections, state.gateway_surfaces
            ),
        );
        trace.push(
            "observability",
            "consistency",
            format!(
                "consistency strict={} availability_first={} rejections={}",
                state.consistency_strict,
                state.consistency_availability_first,
                state.consistency_rejections
            ),
        );
        trace.push(
            "observability",
            "idempotency",
            format!(
                "idempotency first_seen={} replays={} conflicts={} retries={}",
                state.idempotency_first_seen,
                state.idempotency_replays,
                state.idempotency_conflicts,
                state.idempotency_retries
            ),
        );
        trace.push(
            "observability",
            "watch",
            format!(
                "watch registered={} deliveries={} resumes={}",
                state.watch_registered, state.watch_deliveries, state.watch_resumes
            ),
        );
        trace.push(
            "observability",
            "erasure",
            format!(
                "erasure data_shards={} parity_shards={} tolerated_loss={} reconstructed_bytes={}",
                state.erasure_data_shards,
                state.erasure_parity_shards,
                state.erasure_tolerated_loss,
                state.erasure_reconstructed_bytes
            ),
        );
        trace.push(
            "observability",
            "session",
            format!(
                "session bound={} authorized={} rejected={}",
                state.session_bound, state.session_authorized, state.session_rejected
            ),
        );
        trace.push(
            "observability",
            "sessiontxn",
            format!(
                "sessiontxn sessions={} pending_writes={} overlay_reads={} snapshot_reads={} commits={} rollbacks={} savepoints={} savepoint_rewinds={} rejections={}",
                state.sessiontxn_sessions,
                state.sessiontxn_pending_writes,
                state.sessiontxn_overlay_reads,
                state.sessiontxn_snapshot_reads,
                state.sessiontxn_commits,
                state.sessiontxn_rollbacks,
                state.sessiontxn_savepoints,
                state.sessiontxn_savepoint_rewinds,
                state.sessiontxn_rejections
            ),
        );
        trace.push(
            "observability",
            "prepared",
            format!(
                "prepared statements={} binds={} invalidations={} deallocations={} evictions={} rejections={}",
                state.prepared_statements,
                state.prepared_binds,
                state.prepared_invalidations,
                state.prepared_deallocations,
                state.prepared_evictions,
                state.prepared_rejections
            ),
        );
        trace.push(
            "observability",
            "portal",
            format!(
                "portal parses={} binds={} describes={} executes={} rows={} cancellations={} portal_closes={} statement_closes={} rejections={}",
                state.portal_parses,
                state.portal_binds,
                state.portal_describes,
                state.portal_executes,
                state.portal_rows,
                state.portal_cancellations,
                state.portal_closes,
                state.portal_statement_closes,
                state.portal_rejections
            ),
        );
        trace.push(
            "observability",
            "rowcodec",
            format!(
                "rowcodec encoded={} decoded={} defaults={} generated={} bytes={} rejections={}",
                state.rowcodec_encoded,
                state.rowcodec_decoded,
                state.rowcodec_defaults,
                state.rowcodec_generated,
                state.rowcodec_bytes,
                state.rowcodec_rejections
            ),
        );
        trace.push(
            "observability",
            "tuple",
            format!(
                "tuple versions={} visible_reads={} delete_markers={} bytes={} rejections={}",
                state.tuple_versions,
                state.tuple_visible_reads,
                state.tuple_delete_markers,
                state.tuple_bytes,
                state.tuple_rejections
            ),
        );
        trace.push(
            "observability",
            "vacuum",
            format!(
                "vacuum chains={} pruned_versions={} retained_versions={} reclaimed_bytes={} rejections={}",
                state.vacuum_chains,
                state.vacuum_pruned_versions,
                state.vacuum_retained_versions,
                state.vacuum_reclaimed_bytes,
                state.vacuum_rejections
            ),
        );
        trace.push(
            "observability",
            "materialize",
            format!(
                "materialize rows={} projected={} columns={} bytes={} rejections={}",
                state.materialized_rows,
                state.materialized_projected,
                state.materialized_columns,
                state.materialized_bytes,
                state.materialized_rejections
            ),
        );
        trace.push(
            "observability",
            "primary",
            format!(
                "primary inserts={} lookups={} updates={} deletes={} rejections={} entries={}",
                state.primary_inserts,
                state.primary_lookups,
                state.primary_updates,
                state.primary_deletes,
                state.primary_rejections,
                state.primary_entries
            ),
        );
        trace.push(
            "observability",
            "range",
            format!(
                "range partitions={} non_overlaps={} rejections={} sstables={} compaction_inputs={}",
                state.range_partitions,
                state.range_non_overlaps,
                state.range_rejections,
                state.sstable_manifests,
                state.compaction_inputs
            ),
        );
        trace.push(
            "observability",
            "compaction",
            format!(
                "compaction outputs={} live_keys={} tombstones={} reclaimed_bytes={}",
                state.compaction_outputs,
                state.compaction_live_keys,
                state.compaction_tombstones,
                state.compaction_reclaimed_bytes
            ),
        );
        trace.push(
            "observability",
            "stats",
            format!(
                "stats tables={} rows={} histogram_buckets={} stale={}",
                state.stats_tables,
                state.stats_rows,
                state.stats_histogram_buckets,
                state.stats_stale
            ),
        );
        trace.push(
            "observability",
            "constraints",
            format!(
                "constraints checked={} accepted={} rejected={} unique_keys={}",
                state.constraints_checked,
                state.constraints_accepted,
                state.constraints_rejected,
                state.constraints_unique_keys
            ),
        );
        trace.push(
            "observability",
            "deferred",
            format!(
                "deferred transactions={} obligations={} txn_references={} committed_references={} commits={} rejections={}",
                state.deferred_transactions,
                state.deferred_obligations,
                state.deferred_txn_references,
                state.deferred_committed_references,
                state.deferred_commits,
                state.deferred_rejections
            ),
        );
        trace.push(
            "observability",
            "write",
            format!(
                "write plans={} steps={} index_mutations={} conflict_ranges={}",
                state.write_plans,
                state.write_steps,
                state.write_index_mutations,
                state.write_conflict_ranges
            ),
        );
        trace.push(
            "observability",
            "relcommit",
            format!(
                "relcommit groups={} row_mutations={} index_mutations={} conflict_ranges={} rejections={}",
                state.relcommit_groups,
                state.relcommit_row_mutations,
                state.relcommit_index_mutations,
                state.relcommit_conflict_ranges,
                state.relcommit_rejections
            ),
        );
        trace.push(
            "observability",
            "page",
            format!(
                "page frames={} redo={} reads={} rejections={} checkpoint_bytes={}",
                state.page_frames,
                state.page_redo,
                state.page_reads,
                state.page_rejections,
                state.page_checkpoint_bytes
            ),
        );
        trace.push(
            "observability",
            "wal",
            format!(
                "wal appends={} syncs={} flush_allows={} rejections={} durable_lsn={} bytes={}",
                state.wal_appends,
                state.wal_syncs,
                state.wal_flush_allows,
                state.wal_rejections,
                state.wal_durable_lsn,
                state.wal_bytes
            ),
        );
        trace.push(
            "observability",
            "buffer",
            format!(
                "buffer resident={} hits={} dirty={} flushes={} evictions={} rejections={}",
                state.buffer_resident,
                state.buffer_hits,
                state.buffer_dirty,
                state.buffer_flushes,
                state.buffer_evictions,
                state.buffer_rejections
            ),
        );
        trace.push(
            "observability",
            "fault",
            format!(
                "fault_events={} dropped_messages={} delayed_messages={} failure_drill_passed={} failure_drill_domains={}",
                state.fault_events,
                state.dropped_messages,
                state.delayed_messages,
                state.failure_drill_passed,
                state.failure_drill_domains
            ),
        );
        trace.push(
            "observability",
            "rebalance",
            format!(
                "rebalance moves={} checks={} policy_proofs={}",
                state.rebalance_moves, state.rebalance_checks, state.rebalance_policy_proofs
            ),
        );
        trace.push(
            "observability",
            "lease",
            format!(
                "lease grants={} fence_rejections={}",
                state.lease_grants, state.fence_rejections
            ),
        );
        trace.push(
            "observability",
            "backup",
            format!(
                "backup items={} restore_checks={} restore_executed_items={} restore_executed_surfaces={} restore_executed_bytes={}",
                state.backup_items,
                state.restore_checks,
                state.restore_executed_items,
                state.restore_executed_surfaces,
                state.restore_executed_bytes
            ),
        );
        trace.push(
            "observability",
            "recovery",
            format!(
                "recovery roots={} replays={} rejections={} actions={}",
                state.recovery_roots,
                state.recovery_replays,
                state.recovery_rejections,
                state.recovery_actions
            ),
        );
        trace.push(
            "observability",
            "api",
            format!(
                "api accepted={} rejected={}",
                state.api_accepted, state.api_rejected
            ),
        );
        trace.push(
            "observability",
            "authz",
            format!(
                "auth grants={} allowed={} denied={}",
                state.auth_grants, state.authz_allowed, state.authz_denied
            ),
        );
        trace.push(
            "observability",
            "objectmeta",
            format!(
                "objectmeta buckets={} uploads={} parts={} heads={} rejections={}",
                state.objectmeta_buckets,
                state.objectmeta_uploads,
                state.objectmeta_parts,
                state.objectmeta_heads,
                state.objectmeta_rejections
            ),
        );
        trace.push(
            "observability",
            "database-commits",
            format!(
                "{} protocol commits across catalog/KV/SQL/object-metadata/object/blob personalities",
                state.protocol_commits
            ),
        );
        trace.push(
            "observability",
            "repair-health",
            format!(
                "{} scrub findings yielded {} planned repair actions",
                state.repair_findings, state.repair_actions
            ),
        );
        trace.push(
            "observability",
            "registry",
            format!(
                "health={} metrics={} audit_events={}",
                state.cluster_health, state.observability_metrics, state.observability_audits
            ),
        );
        state
    }
}

impl Invariants {
    pub fn run(trace: &mut Trace, state: &mut ProductionRunState) {
        let report = InvariantChecker::evaluate(&InvariantInput {
            admitted_nodes: state.admitted_nodes,
            placement_groups: state.placement_groups,
            membership_voters: state.membership_voters,
            membership_promotions: state.membership_promotions,
            membership_intersections: state.membership_intersections,
            vsr_commits: state.vsr_commits,
            vsr_rejections: state.vsr_rejections,
            fence_rejections: state.fence_rejections,
            manifest_entries: state.manifest_entries,
            manifest_surfaces: state.manifest_surfaces,
            manifest_rejections: state.manifest_rejections,
            superblock_roots: state.superblock_roots,
            superblock_rejections: state.superblock_rejections,
            snapshot_roots: state.snapshot_roots,
            restore_executed_items: state.restore_executed_items,
            restore_executed_surfaces: state.restore_executed_surfaces,
            recovery_roots: state.recovery_roots,
            recovery_rejections: state.recovery_rejections,
            failure_drill_passed: state.failure_drill_passed,
            failure_drill_domains: state.failure_drill_domains,
            repair_findings: state.repair_findings,
            repair_actions: state.repair_actions,
            rebalance_moves: state.rebalance_moves,
            rebalance_policy_proofs: state.rebalance_policy_proofs,
            workload_classes: state.workload_classes,
            workload_budget_paths: state.workload_budget_paths,
            workload_benchmark_claim: state.workload_benchmark_claim,
            stream_verified_parts: state.stream_verified_parts,
            erasure_tolerated_loss: state.erasure_tolerated_loss,
            chunkstore_chunks: state.chunkstore_chunks,
            chunkstore_verified: state.chunkstore_verified,
            chunkstore_deduped: state.chunkstore_deduped,
            chunkstore_rejections: state.chunkstore_rejections,
            checkpoint_roots: state.checkpoint_roots,
            checkpoint_compacted: state.checkpoint_compacted,
            checkpoint_reclaimable_bytes: state.checkpoint_reclaimable_bytes,
            checkpoint_rejections: state.checkpoint_rejections,
            sql_pipeline_bound: state.sql_pipeline_bound,
            sql_pipeline_authorized: state.sql_pipeline_authorized,
            sql_pipeline_logical: state.sql_pipeline_logical,
            sql_pipeline_physical: state.sql_pipeline_physical,
            sql_pipeline_rows: state.sql_pipeline_rows,
            sql_pipeline_rejections: state.sql_pipeline_rejections,
            sql_dml_bound: state.sql_dml_bound,
            sql_dml_authorized: state.sql_dml_authorized,
            sql_dml_planned: state.sql_dml_planned,
            sql_dml_prepared: state.sql_dml_prepared,
            sql_dml_committed: state.sql_dml_committed,
            sql_dml_isolated: state.sql_dml_isolated,
            sql_dml_kv_applied: state.sql_dml_kv_applied,
            sql_dml_session_staged: state.sql_dml_session_staged,
            sql_dml_overlay_verified: state.sql_dml_overlay_verified,
            sql_dml_rejections: state.sql_dml_rejections,
            deferred_transactions: state.deferred_transactions,
            deferred_obligations: state.deferred_obligations,
            deferred_txn_references: state.deferred_txn_references,
            deferred_commits: state.deferred_commits,
            deferred_rejections: state.deferred_rejections,
            migration_begun: state.migration_begun,
            migration_validated: state.migration_validated,
            migration_published: state.migration_published,
            migration_backfilled_rows: state.migration_backfilled_rows,
            migration_indexed_keys: state.migration_indexed_keys,
            migration_resumed: state.migration_resumed,
            migration_completed: state.migration_completed,
            migration_rolled_back: state.migration_rolled_back,
            migration_rejected: state.migration_rejected,
            sql_txn_began: state.sql_txn_began,
            sql_txn_dml_staged: state.sql_txn_dml_staged,
            sql_txn_overlay_reads: state.sql_txn_overlay_reads,
            sql_txn_commits: state.sql_txn_commits,
            sql_txn_rollbacks: state.sql_txn_rollbacks,
            sql_txn_rollback_absent: state.sql_txn_rollback_absent,
            sql_txn_savepoints: state.sql_txn_savepoints,
            sql_txn_savepoint_rewinds: state.sql_txn_savepoint_rewinds,
            sql_txn_savepoint_absent: state.sql_txn_savepoint_absent,
            sql_txn_rejections: state.sql_txn_rejections,
            consistency_strict: state.consistency_strict,
            consistency_availability_first: state.consistency_availability_first,
            consistency_rejections: state.consistency_rejections,
            objectmeta_buckets: state.objectmeta_buckets,
            objectmeta_uploads: state.objectmeta_uploads,
            objectmeta_parts: state.objectmeta_parts,
            objectmeta_heads: state.objectmeta_heads,
            objectmeta_rejections: state.objectmeta_rejections,
            sessiontxn_sessions: state.sessiontxn_sessions,
            sessiontxn_pending_writes: state.sessiontxn_pending_writes,
            sessiontxn_overlay_reads: state.sessiontxn_overlay_reads,
            sessiontxn_snapshot_reads: state.sessiontxn_snapshot_reads,
            sessiontxn_commits: state.sessiontxn_commits,
            sessiontxn_rollbacks: state.sessiontxn_rollbacks,
            sessiontxn_savepoints: state.sessiontxn_savepoints,
            sessiontxn_savepoint_rewinds: state.sessiontxn_savepoint_rewinds,
            sessiontxn_rejections: state.sessiontxn_rejections,
            prepared_statements: state.prepared_statements,
            prepared_binds: state.prepared_binds,
            prepared_invalidations: state.prepared_invalidations,
            prepared_deallocations: state.prepared_deallocations,
            prepared_evictions: state.prepared_evictions,
            prepared_rejections: state.prepared_rejections,
            portal_parses: state.portal_parses,
            portal_binds: state.portal_binds,
            portal_describes: state.portal_describes,
            portal_executes: state.portal_executes,
            portal_rows: state.portal_rows,
            portal_cancellations: state.portal_cancellations,
            portal_closes: state.portal_closes,
            portal_statement_closes: state.portal_statement_closes,
            portal_rejections: state.portal_rejections,
            rowcodec_encoded: state.rowcodec_encoded,
            rowcodec_decoded: state.rowcodec_decoded,
            rowcodec_defaults: state.rowcodec_defaults,
            rowcodec_generated: state.rowcodec_generated,
            rowcodec_rejections: state.rowcodec_rejections,
            rowcodec_bytes: state.rowcodec_bytes,
            tuple_versions: state.tuple_versions,
            tuple_visible_reads: state.tuple_visible_reads,
            tuple_delete_markers: state.tuple_delete_markers,
            tuple_rejections: state.tuple_rejections,
            tuple_bytes: state.tuple_bytes,
            vacuum_chains: state.vacuum_chains,
            vacuum_pruned_versions: state.vacuum_pruned_versions,
            vacuum_retained_versions: state.vacuum_retained_versions,
            vacuum_rejections: state.vacuum_rejections,
            vacuum_reclaimed_bytes: state.vacuum_reclaimed_bytes,
            materialized_rows: state.materialized_rows,
            materialized_projected: state.materialized_projected,
            materialized_columns: state.materialized_columns,
            materialized_rejections: state.materialized_rejections,
            materialized_bytes: state.materialized_bytes,
            relcommit_groups: state.relcommit_groups,
            relcommit_row_mutations: state.relcommit_row_mutations,
            relcommit_index_mutations: state.relcommit_index_mutations,
            relcommit_conflict_ranges: state.relcommit_conflict_ranges,
            relcommit_rejections: state.relcommit_rejections,
            primary_inserts: state.primary_inserts,
            primary_lookups: state.primary_lookups,
            primary_updates: state.primary_updates,
            primary_deletes: state.primary_deletes,
            primary_rejections: state.primary_rejections,
            primary_entries: state.primary_entries,
            range_partitions: state.range_partitions,
            range_non_overlaps: state.range_non_overlaps,
            range_rejections: state.range_rejections,
            sstable_manifests: state.sstable_manifests,
            compaction_inputs: state.compaction_inputs,
            isolation_rejections: state.isolation_rejections,
            committed_transactions: state.committed_transactions,
            storage_committed_prefixes: state.storage_committed_prefixes,
            storage_manifest_covered: state.storage_manifest_covered,
            protocol_commits: state.protocol_commits,
        });

        state.invariant_checks = report.checks.len() as u8;
        state.invariant_passed = report.passed() as u8;
        state.invariant_failed = report.failed() as u8;

        for check in &report.checks {
            trace.push(
                "invariants",
                if check.passed { "pass" } else { "fail" },
                format!("{} {}", check.id.as_str(), check.detail),
            );
        }
        trace.push(
            "invariants",
            "summary",
            format!(
                "checks={} passed={} failed={} status={}",
                state.invariant_checks,
                state.invariant_passed,
                state.invariant_failed,
                if report.is_ok() { "ok" } else { "failed" }
            ),
        );
    }
}

pub struct Requirements;

pub struct Testability;

impl Testability {
    pub fn run(trace: &mut Trace, state: &mut ProductionRunState) {
        let report = TestabilityReport::evaluate(&TestabilityInput {
            trace_events: trace.events().len() as u16,
            trace_services: trace.services().len() as u8,
            invariant_failed: state.invariant_failed,
            observability_metrics: state.observability_metrics,
            observability_audits: state.observability_audits,
        });

        state.testability_checks = report.total() as u8;
        state.testability_passed = report.passed() as u8;
        state.testability_trace_events = trace.events().len() as u16;
        state.testability_trace_services = trace.services().len() as u8;
        state.testability_replay_ready = u8::from(report.replay_ready());

        for check in &report.checks {
            trace.push(
                "testability",
                if check.passed { "pass" } else { "fail" },
                format!("{} {}", check.name, check.detail),
            );
        }
        trace.push(
            "testability",
            "summary",
            format!(
                "checks={} passed={} trace_events={} trace_services={} replay_ready={}",
                state.testability_checks,
                state.testability_passed,
                state.testability_trace_events,
                state.testability_trace_services,
                state.testability_replay_ready
            ),
        );
    }
}

impl Requirements {
    pub fn run(trace: &mut Trace, implementation: &Implementation, state: &mut ProductionRunState) {
        let report = RequirementAuditor::evaluate(&RequirementInput {
            implementation: implementation.id.slug().to_string(),
            nodes: implementation.topology.nodes,
            regions: implementation.topology.regions.len() as u8,
            deployment_roles: state.deployment_roles,
            deployment_endpoints: state.deployment_endpoints,
            deployment_volumes: state.deployment_volumes,
            deployment_probes: state.deployment_probes,
            admitted_nodes: state.admitted_nodes,
            membership_epoch: state.membership_epoch,
            membership_promotions: state.membership_promotions,
            membership_intersections: state.membership_intersections,
            membership_rejections: state.membership_rejections,
            vsr_commits: state.vsr_commits,
            vsr_view_changes: state.vsr_view_changes,
            vsr_rejections: state.vsr_rejections,
            tenant_surfaces: state.tenant_surfaces,
            catalog_items: state.catalog_items,
            auth_grants: state.auth_grants,
            wire_protocols: state.wire_protocols,
            gateway_surfaces: state.gateway_surfaces,
            consistency_strict: state.consistency_strict,
            consistency_availability_first: state.consistency_availability_first,
            consistency_rejections: state.consistency_rejections,
            quota_policies: state.quota_policies,
            rpc_calls: state.rpc_calls,
            idempotency_replays: state.idempotency_replays,
            schema_steps: state.schema_steps,
            migration_begun: state.migration_begun,
            migration_backfilled_rows: state.migration_backfilled_rows,
            migration_indexed_keys: state.migration_indexed_keys,
            migration_completed: state.migration_completed,
            migration_rolled_back: state.migration_rolled_back,
            migration_rejected: state.migration_rejected,
            jobs_enqueued: state.jobs_enqueued,
            session_bound: state.session_bound,
            primary_inserts: state.primary_inserts,
            primary_lookups: state.primary_lookups,
            primary_updates: state.primary_updates,
            primary_deletes: state.primary_deletes,
            primary_rejections: state.primary_rejections,
            index_mutations: state.index_mutations,
            sql_pipeline_rows: state.sql_pipeline_rows,
            sql_pipeline_rejections: state.sql_pipeline_rejections,
            sql_dml_bound: state.sql_dml_bound,
            sql_dml_committed: state.sql_dml_committed,
            sql_dml_kv_applied: state.sql_dml_kv_applied,
            sql_dml_session_staged: state.sql_dml_session_staged,
            sql_dml_overlay_verified: state.sql_dml_overlay_verified,
            sql_dml_rejections: state.sql_dml_rejections,
            deferred_transactions: state.deferred_transactions,
            deferred_obligations: state.deferred_obligations,
            deferred_txn_references: state.deferred_txn_references,
            deferred_commits: state.deferred_commits,
            deferred_rejections: state.deferred_rejections,
            sql_txn_began: state.sql_txn_began,
            sql_txn_dml_staged: state.sql_txn_dml_staged,
            sql_txn_overlay_reads: state.sql_txn_overlay_reads,
            sql_txn_commits: state.sql_txn_commits,
            sql_txn_rollbacks: state.sql_txn_rollbacks,
            sql_txn_rollback_absent: state.sql_txn_rollback_absent,
            sql_txn_savepoints: state.sql_txn_savepoints,
            sql_txn_savepoint_rewinds: state.sql_txn_savepoint_rewinds,
            sql_txn_savepoint_absent: state.sql_txn_savepoint_absent,
            sql_txn_rejections: state.sql_txn_rejections,
            query_rows: state.query_rows,
            planner_physical_plans: state.planner_physical_plans,
            executor_stages: state.executor_stages,
            page_frames: state.page_frames,
            page_redo: state.page_redo,
            wal_flush_allows: state.wal_flush_allows,
            wal_rejections: state.wal_rejections,
            buffer_hits: state.buffer_hits,
            buffer_flushes: state.buffer_flushes,
            kv_keys: state.kv_keys,
            range_partitions: state.range_partitions,
            range_non_overlaps: state.range_non_overlaps,
            range_rejections: state.range_rejections,
            sstable_manifests: state.sstable_manifests,
            compaction_inputs: state.compaction_inputs,
            sessiontxn_overlay_reads: state.sessiontxn_overlay_reads,
            sessiontxn_commits: state.sessiontxn_commits,
            sessiontxn_rollbacks: state.sessiontxn_rollbacks,
            sessiontxn_savepoints: state.sessiontxn_savepoints,
            sessiontxn_savepoint_rewinds: state.sessiontxn_savepoint_rewinds,
            sessiontxn_rejections: state.sessiontxn_rejections,
            prepared_statements: state.prepared_statements,
            prepared_binds: state.prepared_binds,
            prepared_invalidations: state.prepared_invalidations,
            prepared_deallocations: state.prepared_deallocations,
            prepared_evictions: state.prepared_evictions,
            prepared_rejections: state.prepared_rejections,
            portal_parses: state.portal_parses,
            portal_binds: state.portal_binds,
            portal_describes: state.portal_describes,
            portal_executes: state.portal_executes,
            portal_rows: state.portal_rows,
            portal_cancellations: state.portal_cancellations,
            portal_closes: state.portal_closes,
            portal_statement_closes: state.portal_statement_closes,
            portal_rejections: state.portal_rejections,
            rowcodec_encoded: state.rowcodec_encoded,
            rowcodec_decoded: state.rowcodec_decoded,
            rowcodec_defaults: state.rowcodec_defaults,
            rowcodec_generated: state.rowcodec_generated,
            rowcodec_rejections: state.rowcodec_rejections,
            rowcodec_bytes: state.rowcodec_bytes,
            tuple_versions: state.tuple_versions,
            tuple_visible_reads: state.tuple_visible_reads,
            tuple_delete_markers: state.tuple_delete_markers,
            tuple_rejections: state.tuple_rejections,
            tuple_bytes: state.tuple_bytes,
            vacuum_chains: state.vacuum_chains,
            vacuum_pruned_versions: state.vacuum_pruned_versions,
            vacuum_retained_versions: state.vacuum_retained_versions,
            vacuum_rejections: state.vacuum_rejections,
            vacuum_reclaimed_bytes: state.vacuum_reclaimed_bytes,
            materialized_rows: state.materialized_rows,
            materialized_projected: state.materialized_projected,
            materialized_columns: state.materialized_columns,
            materialized_rejections: state.materialized_rejections,
            materialized_bytes: state.materialized_bytes,
            relcommit_groups: state.relcommit_groups,
            relcommit_row_mutations: state.relcommit_row_mutations,
            relcommit_index_mutations: state.relcommit_index_mutations,
            relcommit_conflict_ranges: state.relcommit_conflict_ranges,
            relcommit_rejections: state.relcommit_rejections,
            watch_deliveries: state.watch_deliveries,
            object_versions: state.object_versions,
            objectmeta_buckets: state.objectmeta_buckets,
            objectmeta_heads: state.objectmeta_heads,
            objectmeta_rejections: state.objectmeta_rejections,
            blob_chunks: state.blob_chunks,
            manifest_entries: state.manifest_entries,
            manifest_surfaces: state.manifest_surfaces,
            chunkstore_chunks: state.chunkstore_chunks,
            chunkstore_verified: state.chunkstore_verified,
            chunkstore_deduped: state.chunkstore_deduped,
            checkpoint_roots: state.checkpoint_roots,
            superblock_roots: state.superblock_roots,
            snapshot_roots: state.snapshot_roots,
            backup_items: state.backup_items,
            restore_executed_items: state.restore_executed_items,
            restore_executed_surfaces: state.restore_executed_surfaces,
            recovery_roots: state.recovery_roots,
            recovery_rejections: state.recovery_rejections,
            repair_actions: state.repair_actions,
            retention_actions: state.retention_actions,
            provider_selected: state.provider_selected,
            provider_degraded: state.provider_degraded,
            stream_read_lanes: state.stream_read_lanes,
            stream_verified_parts: state.stream_verified_parts,
            erasure_tolerated_loss: state.erasure_tolerated_loss,
            cache_hits: state.cache_hits,
            workload_classes: state.workload_classes,
            workload_budget_paths: state.workload_budget_paths,
            workload_benchmark_claim: state.workload_benchmark_claim,
            rebalance_moves: state.rebalance_moves,
            rebalance_policy_proofs: state.rebalance_policy_proofs,
            health_demote: state.health_demote,
            fault_events: state.fault_events,
            dropped_messages: state.dropped_messages,
            delayed_messages: state.delayed_messages,
            failure_drill_passed: state.failure_drill_passed,
            failure_drill_domains: state.failure_drill_domains,
            protocol_commits: state.protocol_commits,
            committed_transactions: state.committed_transactions,
            storage_committed_prefixes: state.storage_committed_prefixes,
            storage_manifest_covered: state.storage_manifest_covered,
            testability_checks: state.testability_checks,
            testability_passed: state.testability_passed,
            testability_trace_events: state.testability_trace_events,
            testability_trace_services: state.testability_trace_services,
            testability_replay_ready: state.testability_replay_ready,
            invariant_failed: state.invariant_failed,
            observability_metrics: state.observability_metrics,
        });

        state.requirement_checks = report.total() as u8;
        state.requirement_covered = report.covered() as u8;

        for check in &report.checks {
            trace.push(
                "requirements",
                check.status.as_str(),
                format!("{} {}", check.id, check.detail),
            );
        }
        trace.push(
            "requirements",
            "summary",
            format!(
                "checks={} covered={} structural={} missing={}",
                report.total(),
                report.covered(),
                report.structural(),
                report.missing()
            ),
        );
    }
}

fn health_status(status: HealthStatus) -> &'static str {
    match status {
        HealthStatus::Healthy => "healthy",
        HealthStatus::Degraded => "degraded",
        HealthStatus::Unavailable => "unavailable",
    }
}

fn response_status(status: ResponseStatus) -> &'static str {
    match status {
        ResponseStatus::Committed => "committed",
        ResponseStatus::Rejected => "rejected",
    }
}

fn job_spec(
    id: &'static str,
    kind: JobKind,
    target: &'static str,
    depends_on: impl IntoIterator<Item = &'static str>,
) -> JobSpec {
    JobSpec {
        id: JobId(id.to_string()),
        kind,
        namespace: "app".to_string(),
        target: target.to_string(),
        depends_on: depends_on
            .into_iter()
            .map(|dependency| JobId(dependency.to_string()))
            .collect(),
        max_attempts: 3,
    }
}

fn write_step(step: &WriteStep) -> String {
    match step {
        WriteStep::CheckConstraints { checks } => format!("check-constraints({checks})"),
        WriteStep::WriteRow { key } => format!("write-row({})", String::from_utf8_lossy(key)),
        WriteStep::UpdateIndexes { mutations } => format!("update-indexes({mutations})"),
        WriteStep::DeclareConflicts { ranges } => format!("declare-conflicts({ranges})"),
        WriteStep::Commit => "commit".to_string(),
    }
}

fn planned_write(decision: WriteDecision) -> WritePlan {
    match decision {
        WriteDecision::Planned(plan) => plan,
        WriteDecision::Rejected { violations, .. } => {
            panic!("simulated write should plan, rejected by {violations:?}")
        }
    }
}

fn write_request(id_seed: u8, table: &TableName, primary_key: &str, name: &str) -> WriteRequest {
    WriteRequest {
        id: TransactionId([id_seed; 16]),
        table: table.clone(),
        primary_key: primary_key.to_string(),
        values: BTreeMap::from([
            ("active".to_string(), "true".to_string()),
            ("id".to_string(), primary_key.to_string()),
            ("name".to_string(), name.to_string()),
        ]),
    }
}

fn capability_label(capability: &Capability) -> &'static str {
    match capability {
        Capability::ClusterJoin => "cluster_join",
        Capability::ClusterAdmin => "cluster_admin",
        Capability::NamespaceRead => "namespace_read",
        Capability::NamespaceWrite => "namespace_write",
        Capability::ObjectRead => "object_read",
        Capability::ObjectWrite => "object_write",
        Capability::BlobRead => "blob_read",
        Capability::BlobWrite => "blob_write",
        Capability::QueryExecute => "query_execute",
        Capability::TransactionCommit => "transaction_commit",
        Capability::RepairOperate => "repair_operate",
        Capability::RetentionOperate => "retention_operate",
    }
}

fn shard_kind(kind: ShardKind) -> &'static str {
    match kind {
        ShardKind::Data => "data",
        ShardKind::Parity => "parity",
    }
}

fn health_class(class: HealthClass) -> &'static str {
    match class {
        HealthClass::Healthy => "healthy",
        HealthClass::Degraded => "degraded",
        HealthClass::Demote => "demote",
    }
}

fn health_reasons(health: &NodeHealth) -> String {
    if health.reasons.is_empty() {
        return "none".to_string();
    }
    health.reasons.join("; ")
}

fn health_action(action: HealthAction) -> String {
    match action {
        HealthAction::PromoteLearner { node } => {
            format!("promote learner node={} into usable capacity", node.0)
        }
        HealthAction::DemoteSlowReplica { node } => {
            format!("demote slow replica node={}", node.0)
        }
        HealthAction::QuarantineCorruptNode { node } => {
            format!("quarantine corrupt node={}", node.0)
        }
    }
}

fn schema_step(step: &MigrationStep) -> String {
    match step {
        MigrationStep::ValidateCurrentSchema => "validate current schema descriptor".to_string(),
        MigrationStep::PublishDescriptor { version } => {
            format!("publish descriptor version={}", version.0.0)
        }
        MigrationStep::BackfillColumn { name, batch_rows } => {
            format!("backfill column={name} batch_rows={batch_rows}")
        }
        MigrationStep::BuildIndex { name, columns } => {
            format!("build index={name} columns={}", columns.join(","))
        }
        MigrationStep::SwitchReads { version } => {
            format!("switch reads to version={}", version.0.0)
        }
        MigrationStep::RetireColumn { name } => {
            format!("retire column={name}")
        }
    }
}

fn migration_progress(progress: &rsf_migration::MigrationProgress) -> String {
    let rollback = progress
        .rollback_reason
        .as_ref()
        .map(|reason| format!(" rollback_reason={reason}"))
        .unwrap_or_default();
    format!(
        "id={} table={} phase={} steps={}/{} backfilled_rows={} indexed_keys={} durable_events={} terminal={}{}",
        progress.id,
        progress.table,
        progress.phase.as_str(),
        progress.completed_steps,
        progress.total_steps,
        progress.backfilled_rows,
        progress.indexed_keys,
        progress.durable_events,
        progress.is_terminal(),
        rollback
    )
}

fn finding_kind(kind: RepairFindingKind) -> &'static str {
    match kind {
        RepairFindingKind::MissingReplica => "missing_replica",
        RepairFindingKind::CorruptReplica => "corrupt_replica",
        RepairFindingKind::SlowReplica => "slow_replica",
        RepairFindingKind::UnderReplicated => "under_replicated",
    }
}

fn action_summary(action: RepairAction) -> String {
    match action {
        RepairAction::CopyReplica { pg, from, to } => {
            format!("copy pg={} from node={} to node={}", pg.0, from.0, to.0)
        }
        RepairAction::QuarantineReplica { pg, node } => {
            format!("quarantine corrupt replica pg={} node={}", pg.0, node.0)
        }
        RepairAction::RecheckDigest { pg, node } => {
            format!("recheck digest pg={} node={}", pg.0, node.0)
        }
        RepairAction::DemoteSlowReplica { pg, node } => {
            format!("demote slow replica pg={} node={}", pg.0, node.0)
        }
        RepairAction::RestoreReplication {
            pg,
            healthy,
            expected,
            deficit,
        } => {
            format!(
                "restore replication pg={} healthy={} expected={} deficit={}",
                pg.0, healthy, expected, deficit
            )
        }
    }
}

fn txn_label(id: &TransactionId) -> String {
    id.0.iter()
        .take(4)
        .map(|byte| format!("{byte:02x}"))
        .collect::<Vec<_>>()
        .join("")
}

fn session_txn_decision(decision: SessionTransactionDecision) -> String {
    match decision {
        SessionTransactionDecision::Began {
            session_id,
            snapshot,
        } => format!(
            "session={session_id} began strict transaction at snapshot {}",
            snapshot.0
        ),
        SessionTransactionDecision::StagedWrite { session_id, key } => {
            format!(
                "session={session_id} staged key={} in read-your-writes overlay",
                bytes_label(&key)
            )
        }
        SessionTransactionDecision::SavepointCreated {
            session_id,
            name,
            pending_writes,
        } => format!(
            "session={session_id} savepoint={name} captured pending_writes={pending_writes}"
        ),
        SessionTransactionDecision::SavepointRolledBack {
            session_id,
            name,
            rewound_writes,
            pending_writes,
        } => format!(
            "session={session_id} rewound savepoint={name} rewound_writes={rewound_writes} pending_writes={pending_writes}"
        ),
        SessionTransactionDecision::OverlayRead {
            session_id,
            key,
            value,
        } => format!(
            "session={session_id} read key={} from overlay value={}",
            bytes_label(&key),
            optional_bytes_label(value)
        ),
        SessionTransactionDecision::SnapshotRead {
            session_id,
            key,
            version,
            value,
        } => format!(
            "session={session_id} read key={} from snapshot version={} value={}",
            bytes_label(&key),
            version.map_or_else(|| "none".to_string(), |version| version.0.to_string()),
            optional_bytes_label(value)
        ),
        SessionTransactionDecision::Committed {
            session_id,
            version,
            evidence,
            writes,
        } => format!(
            "session={session_id} committed {writes} overlay write(s) at version {} pg={} log_index={} op={}",
            version.0, evidence.placement_group.0, evidence.log_index.0, evidence.operation_id.0
        ),
        SessionTransactionDecision::RolledBack {
            session_id,
            discarded_writes,
        } => {
            format!("session={session_id} rolled back {discarded_writes} overlay write(s)")
        }
    }
}

fn optional_bytes_label(value: Option<Vec<u8>>) -> String {
    value
        .as_deref()
        .map(bytes_label)
        .unwrap_or_else(|| "none".to_string())
}

fn rebalance_action(action: RebalanceAction) -> String {
    match action {
        RebalanceAction::MoveReplica { pg, from, to } => {
            format!(
                "move pg={} replica from node={} to node={}",
                pg.0, from.0, to.0
            )
        }
    }
}

fn rebalance_action_pg(action: &RebalanceAction) -> PlacementGroupId {
    match action {
        RebalanceAction::MoveReplica { pg, .. } => *pg,
    }
}

fn node_list(nodes: &[NodeId]) -> String {
    nodes
        .iter()
        .map(|node| node.0.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

fn recovery_action(kind: RecoveryActionKind) -> &'static str {
    match kind {
        RecoveryActionKind::Reuse => "reuse",
        RecoveryActionKind::Replay => "replay",
        RecoveryActionKind::Quarantine => "quarantine",
    }
}

fn restart_step(kind: RestartStepKind) -> &'static str {
    kind.as_str()
}

fn membership_member(member: &Member) -> String {
    format!(
        "node={} role={} synced={}",
        member.node.0,
        member.role.as_str(),
        member.synced
    )
}

fn vsr_prepare(prepare: &VsrPrepare) -> String {
    format!(
        "pg={} view={} index={} primary={} op={} bytes={}",
        prepare.pg.0,
        prepare.view.0,
        prepare.index.0,
        prepare.primary.0,
        prepare.operation.as_str(),
        prepare.bytes
    )
}

fn vsr_commit(commit: &VsrCommit) -> String {
    format!(
        "pg={} view={} index={} primary={} acknowledgements={}",
        commit.pg.0, commit.view.0, commit.index.0, commit.primary.0, commit.acknowledgements
    )
}

fn cache_key(key: &CacheKey) -> String {
    format!("{}/{}:{}", key.namespace, key.surface.as_str(), key.key)
}

fn cache_keys_label(keys: &[CacheKey]) -> String {
    if keys.is_empty() {
        return "none".to_string();
    }
    keys.iter().map(cache_key).collect::<Vec<_>>().join(",")
}

fn provider_nodes(records: &[ProviderRecord]) -> String {
    records
        .iter()
        .map(|record| {
            format!(
                "{}@{}:{}ms",
                record.node.0, record.region, record.latency_ms
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn page_frame(frame: &PageFrame) -> String {
    format!(
        "table={} page={} lsn={} rows={} bytes={}",
        frame.id.table, frame.id.number, frame.lsn.0, frame.rows, frame.bytes
    )
}

fn wal_entry(entry: &WalEntry) -> String {
    format!(
        "table={} page={} lsn={} bytes={} durable={}",
        entry.page.table, entry.page.number, entry.lsn.0, entry.bytes, entry.durable
    )
}

fn wal_rejection(page: Option<PageId>, reason: String) -> String {
    match page {
        Some(page) => format!("table={} page={} reason={reason}", page.table, page.number),
        None => format!("reason={reason}"),
    }
}

fn buffer_frame(frame: &BufferFrame) -> String {
    format!(
        "table={} page={} pins={} dirty={} dirty_lsn={} bytes={}",
        frame.page.table,
        frame.page.number,
        frame.pins,
        frame.dirty,
        frame.dirty_lsn.map_or(0, |lsn| lsn.0),
        frame.bytes
    )
}

fn rpc_envelope(envelope: &RpcEnvelope) -> String {
    format!(
        "trace_id={} from={} to={} operation={} deadline_ms={} attempt={} retry_class={} body_bytes={}",
        envelope.trace_id,
        envelope.from.as_str(),
        envelope.to.as_str(),
        envelope.operation,
        envelope.deadline_ms,
        envelope.attempt,
        envelope.retry_class.as_str(),
        envelope.body_bytes
    )
}

fn stream_target(target: &StreamTarget) -> String {
    format!(
        "{}/{}:{}",
        target.namespace,
        target.surface.as_str(),
        target.name
    )
}

fn flow_decision(decision: FlowDecision) -> String {
    match decision {
        FlowDecision::Reserved {
            bytes,
            remaining_bytes,
        } => format!("reserved bytes={bytes} remaining={remaining_bytes}"),
        FlowDecision::Rejected { bytes, reason } => {
            format!("rejected bytes={bytes} reason={reason}")
        }
    }
}

fn bytes_label(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

fn index_row(primary_key: &str, name: &str, active: bool) -> RowImage {
    RowImage {
        primary_key: primary_key.to_string(),
        values: BTreeMap::from([
            ("id".to_string(), primary_key.to_string()),
            ("name".to_string(), name.to_string()),
            ("active".to_string(), active.to_string()),
        ]),
    }
}

fn primary_row(
    primary_key: &str,
    name: &str,
    placement_group: PlacementGroupId,
    version: CommitVersion,
) -> PrimaryRow {
    PrimaryRow {
        values: BTreeMap::from([
            ("id".to_string(), primary_key.to_string()),
            ("name".to_string(), name.to_string()),
        ]),
        row_key: format!("row/users/{primary_key}").into_bytes(),
        placement_group,
        version,
    }
}

fn primary_entry(action: &str, entry: &rsf_primary::PrimaryEntry) -> String {
    format!(
        "{action} key={} row_key={} pg={} version={}",
        bytes_label(&entry.encoded_key),
        bytes_label(&entry.row_key),
        entry.placement_group.0,
        entry.version.0
    )
}

fn row_value_label(value: Option<&RowValue>) -> String {
    match value {
        Some(RowValue::Null) => "null".to_string(),
        Some(RowValue::Int64(value)) => value.to_string(),
        Some(RowValue::Text(value)) => value.clone(),
        Some(RowValue::Bool(value)) => value.to_string(),
        Some(RowValue::Bytes(value)) => format!("{} bytes", value.len()),
        None => "missing".to_string(),
    }
}

fn tuple_version_label(version: &TupleVersion) -> String {
    format!(
        "table={} key={} create_version={} delete_version={} schema_version={} row_bytes={}",
        version.key.table,
        String::from_utf8_lossy(&version.key.primary_key),
        version.header.create_version.0,
        version
            .header
            .delete_version
            .map(|delete_version| delete_version.0.to_string())
            .unwrap_or_else(|| "none".to_string()),
        version.header.schema_version.0,
        version.header.row_bytes
    )
}

fn placement_label(placement_group: Option<PlacementGroupId>) -> String {
    placement_group.map_or_else(|| "none".to_string(), |pg| pg.0.to_string())
}

fn optional_usize(value: Option<usize>) -> String {
    value.map_or_else(|| "none".to_string(), |value| value.to_string())
}

fn optional_u64(value: Option<u64>) -> String {
    value.map_or_else(|| "none".to_string(), |value| value.to_string())
}

fn retention_decision(decision: RetentionDecision) -> String {
    match decision {
        RetentionDecision::Keep { target, reason } => {
            format!("keep {} reason={:?}", retention_target(target), reason)
        }
        RetentionDecision::ExpireObject {
            bucket,
            key,
            version,
        } => {
            format!("expire object {bucket}/{key} version={}", version.0)
        }
        RetentionDecision::CompactObjectVersion {
            bucket,
            key,
            version,
        } => {
            format!("compact object {bucket}/{key} version={}", version.0)
        }
        RetentionDecision::ReleaseBlobPin { id, pin_version } => {
            format!(
                "release blob pin digest={} version={}",
                short_hash(id.0),
                pin_version.0
            )
        }
    }
}

fn retention_hook(hook: RetentionHook) -> String {
    match hook {
        RetentionHook::ObjectDeleteAllowed {
            bucket,
            key,
            version,
        } => {
            format!("object-delete {bucket}/{key} version={} allowed", version.0)
        }
        RetentionHook::BlobReleaseAllowed { id, pin_version } => {
            format!(
                "blob-release content={} pin_version={} allowed",
                short_hash(id.0),
                pin_version.0
            )
        }
    }
}

fn retention_target(target: rsf_retention::RetentionTarget) -> String {
    match target {
        rsf_retention::RetentionTarget::Object {
            bucket,
            key,
            version,
        } => format!("object {bucket}/{key} version={}", version.0),
        rsf_retention::RetentionTarget::Blob { id, pin_version } => {
            format!("blob {} version={}", short_hash(id.0), pin_version.0)
        }
    }
}

fn catalog_surface(surface: &CatalogSurface) -> &'static str {
    match surface {
        CatalogSurface::Relational => "relational",
        CatalogSurface::Object => "object",
        CatalogSurface::Blob => "blob",
        CatalogSurface::Kv => "kv",
    }
}

fn short_hash(hash: [u8; 32]) -> String {
    hash.iter()
        .take(4)
        .map(|byte| format!("{byte:02x}"))
        .collect::<Vec<_>>()
        .join("")
}
