#![forbid(unsafe_code)]

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum InvariantId {
    CapacityAfterAdmission,
    LearnerPromotionQuorumIntersection,
    QuorumMetadataPath,
    StalePrimaryRejected,
    MultiSurfaceLayout,
    BootRootsRequired,
    SnapshotRecoveryCoverage,
    RestoreExecutionCoverage,
    FailureDrillCoverage,
    RepairPlannedForFindings,
    RebalancePlacementPolicy,
    WorkloadBudgetShape,
    LargePayloadVerified,
    ImmutableChunkStoreVerified,
    CheckpointCompactionSafe,
    SqlPipelineCovered,
    SqlDmlAtomicMutation,
    DeferredConstraintsCommitTime,
    SchemaMigrationLifecycle,
    SqlTransactionLifecycle,
    ConsistencyModesSeparated,
    ObjectMetadataHeadVisible,
    StrictSessionReadYourWrites,
    PreparedSqlLifecycle,
    SqlPortalLifecycle,
    RelationalRowEncoding,
    RelationalTupleVisibility,
    RelationalVacuumSafety,
    RelationalRowMaterialization,
    RelationalCommitGroupAtomicity,
    PrimaryIndexIdentity,
    OrderedRangeNonOverlap,
    StrictIsolationRejectedStaleRead,
    StorageCommitTailsPresent,
}

impl InvariantId {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CapacityAfterAdmission => "capacity-after-admission",
            Self::LearnerPromotionQuorumIntersection => "learner-promotion-quorum-intersection",
            Self::QuorumMetadataPath => "quorum-metadata-path",
            Self::StalePrimaryRejected => "stale-primary-rejected",
            Self::MultiSurfaceLayout => "multi-surface-layout",
            Self::BootRootsRequired => "boot-roots-required",
            Self::SnapshotRecoveryCoverage => "snapshot-recovery-coverage",
            Self::RestoreExecutionCoverage => "restore-execution-coverage",
            Self::FailureDrillCoverage => "failure-drill-coverage",
            Self::RepairPlannedForFindings => "repair-planned-for-findings",
            Self::RebalancePlacementPolicy => "rebalance-placement-policy",
            Self::WorkloadBudgetShape => "workload-budget-shape",
            Self::LargePayloadVerified => "large-payload-verified",
            Self::ImmutableChunkStoreVerified => "immutable-chunk-store-verified",
            Self::CheckpointCompactionSafe => "checkpoint-compaction-safe",
            Self::SqlPipelineCovered => "sql-pipeline-covered",
            Self::SqlDmlAtomicMutation => "sql-dml-atomic-mutation",
            Self::DeferredConstraintsCommitTime => "deferred-constraints-commit-time",
            Self::SchemaMigrationLifecycle => "schema-migration-lifecycle",
            Self::SqlTransactionLifecycle => "sql-transaction-lifecycle",
            Self::ConsistencyModesSeparated => "consistency-modes-separated",
            Self::ObjectMetadataHeadVisible => "object-metadata-head-visible",
            Self::StrictSessionReadYourWrites => "strict-session-read-your-writes",
            Self::PreparedSqlLifecycle => "prepared-sql-lifecycle",
            Self::SqlPortalLifecycle => "sql-portal-lifecycle",
            Self::RelationalRowEncoding => "relational-row-encoding",
            Self::RelationalTupleVisibility => "relational-tuple-visibility",
            Self::RelationalVacuumSafety => "relational-vacuum-safety",
            Self::RelationalRowMaterialization => "relational-row-materialization",
            Self::RelationalCommitGroupAtomicity => "relational-commit-group-atomicity",
            Self::PrimaryIndexIdentity => "primary-index-identity",
            Self::OrderedRangeNonOverlap => "ordered-range-non-overlap",
            Self::StrictIsolationRejectedStaleRead => "strict-isolation-rejected-stale-read",
            Self::StorageCommitTailsPresent => "storage-commit-tails-present",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InvariantInput {
    pub admitted_nodes: u8,
    pub placement_groups: u8,
    pub membership_voters: u8,
    pub membership_promotions: u8,
    pub membership_intersections: u8,
    pub vsr_commits: u8,
    pub vsr_rejections: u8,
    pub fence_rejections: u8,
    pub manifest_entries: u8,
    pub manifest_surfaces: u8,
    pub manifest_rejections: u8,
    pub superblock_roots: u8,
    pub superblock_rejections: u8,
    pub snapshot_roots: u8,
    pub restore_executed_items: u8,
    pub restore_executed_surfaces: u8,
    pub recovery_roots: u8,
    pub recovery_rejections: u8,
    pub failure_drill_passed: u8,
    pub failure_drill_domains: u8,
    pub repair_findings: u8,
    pub repair_actions: u8,
    pub rebalance_moves: u8,
    pub rebalance_policy_proofs: u8,
    pub workload_classes: u8,
    pub workload_budget_paths: u8,
    pub workload_benchmark_claim: u8,
    pub stream_verified_parts: u8,
    pub erasure_tolerated_loss: u8,
    pub chunkstore_chunks: u8,
    pub chunkstore_verified: u8,
    pub chunkstore_deduped: u8,
    pub chunkstore_rejections: u8,
    pub checkpoint_roots: u8,
    pub checkpoint_compacted: u8,
    pub checkpoint_reclaimable_bytes: u16,
    pub checkpoint_rejections: u8,
    pub sql_pipeline_bound: u8,
    pub sql_pipeline_authorized: u8,
    pub sql_pipeline_logical: u8,
    pub sql_pipeline_physical: u8,
    pub sql_pipeline_rows: u8,
    pub sql_pipeline_rejections: u8,
    pub sql_dml_bound: u8,
    pub sql_dml_authorized: u8,
    pub sql_dml_planned: u8,
    pub sql_dml_prepared: u8,
    pub sql_dml_committed: u8,
    pub sql_dml_isolated: u8,
    pub sql_dml_kv_applied: u8,
    pub sql_dml_session_staged: u8,
    pub sql_dml_overlay_verified: u8,
    pub sql_dml_rejections: u8,
    pub deferred_transactions: u8,
    pub deferred_obligations: u8,
    pub deferred_txn_references: u8,
    pub deferred_commits: u8,
    pub deferred_rejections: u8,
    pub migration_begun: u8,
    pub migration_validated: u8,
    pub migration_published: u8,
    pub migration_backfilled_rows: u16,
    pub migration_indexed_keys: u16,
    pub migration_resumed: u8,
    pub migration_completed: u8,
    pub migration_rolled_back: u8,
    pub migration_rejected: u8,
    pub sql_txn_began: u8,
    pub sql_txn_dml_staged: u8,
    pub sql_txn_overlay_reads: u8,
    pub sql_txn_commits: u8,
    pub sql_txn_rollbacks: u8,
    pub sql_txn_rollback_absent: u8,
    pub sql_txn_savepoints: u8,
    pub sql_txn_savepoint_rewinds: u8,
    pub sql_txn_savepoint_absent: u8,
    pub sql_txn_rejections: u8,
    pub consistency_strict: u8,
    pub consistency_availability_first: u8,
    pub consistency_rejections: u8,
    pub objectmeta_buckets: u8,
    pub objectmeta_uploads: u8,
    pub objectmeta_parts: u8,
    pub objectmeta_heads: u8,
    pub objectmeta_rejections: u8,
    pub sessiontxn_sessions: u8,
    pub sessiontxn_pending_writes: u8,
    pub sessiontxn_overlay_reads: u8,
    pub sessiontxn_snapshot_reads: u8,
    pub sessiontxn_commits: u8,
    pub sessiontxn_rollbacks: u8,
    pub sessiontxn_savepoints: u8,
    pub sessiontxn_savepoint_rewinds: u8,
    pub sessiontxn_rejections: u8,
    pub prepared_statements: u8,
    pub prepared_binds: u8,
    pub prepared_invalidations: u8,
    pub prepared_deallocations: u8,
    pub prepared_evictions: u8,
    pub prepared_rejections: u8,
    pub portal_parses: u8,
    pub portal_binds: u8,
    pub portal_describes: u8,
    pub portal_executes: u8,
    pub portal_rows: u8,
    pub portal_cancellations: u8,
    pub portal_closes: u8,
    pub portal_statement_closes: u8,
    pub portal_rejections: u8,
    pub rowcodec_encoded: u8,
    pub rowcodec_decoded: u8,
    pub rowcodec_defaults: u8,
    pub rowcodec_generated: u8,
    pub rowcodec_rejections: u8,
    pub rowcodec_bytes: u16,
    pub tuple_versions: u8,
    pub tuple_visible_reads: u8,
    pub tuple_delete_markers: u8,
    pub tuple_rejections: u8,
    pub tuple_bytes: u16,
    pub vacuum_chains: u8,
    pub vacuum_pruned_versions: u8,
    pub vacuum_retained_versions: u8,
    pub vacuum_rejections: u8,
    pub vacuum_reclaimed_bytes: u16,
    pub materialized_rows: u8,
    pub materialized_projected: u8,
    pub materialized_columns: u8,
    pub materialized_rejections: u8,
    pub materialized_bytes: u16,
    pub relcommit_groups: u8,
    pub relcommit_row_mutations: u8,
    pub relcommit_index_mutations: u8,
    pub relcommit_conflict_ranges: u8,
    pub relcommit_rejections: u8,
    pub primary_inserts: u8,
    pub primary_lookups: u8,
    pub primary_updates: u8,
    pub primary_deletes: u8,
    pub primary_rejections: u8,
    pub primary_entries: u8,
    pub range_partitions: u8,
    pub range_non_overlaps: u8,
    pub range_rejections: u8,
    pub sstable_manifests: u8,
    pub compaction_inputs: u8,
    pub isolation_rejections: u8,
    pub committed_transactions: u8,
    pub storage_committed_prefixes: u8,
    pub storage_manifest_covered: u8,
    pub protocol_commits: u8,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InvariantCheck {
    pub id: InvariantId,
    pub passed: bool,
    pub detail: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InvariantReport {
    pub checks: Vec<InvariantCheck>,
}

impl InvariantReport {
    pub fn passed(&self) -> usize {
        self.checks.iter().filter(|check| check.passed).count()
    }

    pub fn failed(&self) -> usize {
        self.checks.len() - self.passed()
    }

    pub fn is_ok(&self) -> bool {
        self.failed() == 0
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct InvariantChecker;

impl InvariantChecker {
    pub fn evaluate(input: &InvariantInput) -> InvariantReport {
        InvariantReport {
            checks: vec![
                check(
                    InvariantId::CapacityAfterAdmission,
                    input.admitted_nodes >= 3
                        && input.placement_groups >= 3
                        && input.membership_promotions >= 1,
                    format!(
                        "nodes={} placement_groups={} promotions={}",
                        input.admitted_nodes, input.placement_groups, input.membership_promotions
                    ),
                ),
                check(
                    InvariantId::LearnerPromotionQuorumIntersection,
                    input.membership_voters >= 3
                        && input.membership_promotions >= 1
                        && input.membership_intersections >= input.membership_promotions,
                    format!(
                        "voters={} promotions={} quorum_intersections={}",
                        input.membership_voters,
                        input.membership_promotions,
                        input.membership_intersections
                    ),
                ),
                check(
                    InvariantId::QuorumMetadataPath,
                    input.membership_voters >= 3 && input.vsr_commits >= 2,
                    format!(
                        "voters={} vsr_commits={}",
                        input.membership_voters, input.vsr_commits
                    ),
                ),
                check(
                    InvariantId::StalePrimaryRejected,
                    input.vsr_rejections >= 1 && input.fence_rejections >= 1,
                    format!(
                        "vsr_rejections={} fence_rejections={}",
                        input.vsr_rejections, input.fence_rejections
                    ),
                ),
                check(
                    InvariantId::MultiSurfaceLayout,
                    input.manifest_entries >= 6
                        && input.manifest_surfaces >= 5
                        && input.manifest_rejections >= 1,
                    format!(
                        "manifest_entries={} surfaces={} rejections={}",
                        input.manifest_entries, input.manifest_surfaces, input.manifest_rejections
                    ),
                ),
                check(
                    InvariantId::BootRootsRequired,
                    input.superblock_roots >= 6 && input.superblock_rejections >= 1,
                    format!(
                        "superblock_roots={} superblock_rejections={}",
                        input.superblock_roots, input.superblock_rejections
                    ),
                ),
                check(
                    InvariantId::SnapshotRecoveryCoverage,
                    input.snapshot_roots == input.recovery_roots
                        && input.snapshot_roots >= 6
                        && input.recovery_rejections >= 1,
                    format!(
                        "snapshot_roots={} recovery_roots={} recovery_rejections={}",
                        input.snapshot_roots, input.recovery_roots, input.recovery_rejections
                    ),
                ),
                check(
                    InvariantId::RestoreExecutionCoverage,
                    input.restore_executed_items >= 6 && input.restore_executed_surfaces >= 6,
                    format!(
                        "restore_executed_items={} restore_executed_surfaces={}",
                        input.restore_executed_items, input.restore_executed_surfaces
                    ),
                ),
                check(
                    InvariantId::FailureDrillCoverage,
                    input.failure_drill_passed == 1 && input.failure_drill_domains >= 6,
                    format!(
                        "failure_drill_passed={} failure_drill_domains={}",
                        input.failure_drill_passed, input.failure_drill_domains
                    ),
                ),
                check(
                    InvariantId::RepairPlannedForFindings,
                    input.repair_findings >= 1 && input.repair_actions >= input.repair_findings,
                    format!(
                        "repair_findings={} repair_actions={}",
                        input.repair_findings, input.repair_actions
                    ),
                ),
                check(
                    InvariantId::RebalancePlacementPolicy,
                    input.rebalance_moves >= 1
                        && input.rebalance_policy_proofs >= input.rebalance_moves,
                    format!(
                        "moves={} policy_proofs={}",
                        input.rebalance_moves, input.rebalance_policy_proofs
                    ),
                ),
                check(
                    InvariantId::WorkloadBudgetShape,
                    input.workload_classes >= 4
                        && input.workload_budget_paths >= 4
                        && input.workload_benchmark_claim == 0,
                    format!(
                        "workload_classes={} workload_budget_paths={} workload_benchmark_claim={}",
                        input.workload_classes,
                        input.workload_budget_paths,
                        input.workload_benchmark_claim
                    ),
                ),
                check(
                    InvariantId::LargePayloadVerified,
                    input.stream_verified_parts >= 3 && input.erasure_tolerated_loss >= 2,
                    format!(
                        "stream_verified_parts={} erasure_tolerated_loss={}",
                        input.stream_verified_parts, input.erasure_tolerated_loss
                    ),
                ),
                check(
                    InvariantId::ImmutableChunkStoreVerified,
                    input.chunkstore_chunks >= 5
                        && input.chunkstore_verified >= 3
                        && input.chunkstore_deduped >= 1
                        && input.chunkstore_rejections >= 1,
                    format!(
                        "chunkstore_chunks={} verified={} deduped={} rejections={}",
                        input.chunkstore_chunks,
                        input.chunkstore_verified,
                        input.chunkstore_deduped,
                        input.chunkstore_rejections
                    ),
                ),
                check(
                    InvariantId::CheckpointCompactionSafe,
                    input.checkpoint_roots >= 7
                        && input.checkpoint_compacted >= 1
                        && input.checkpoint_reclaimable_bytes > 0
                        && input.checkpoint_rejections >= 1,
                    format!(
                        "checkpoint_roots={} compacted={} reclaimable_bytes={} rejections={}",
                        input.checkpoint_roots,
                        input.checkpoint_compacted,
                        input.checkpoint_reclaimable_bytes,
                        input.checkpoint_rejections
                    ),
                ),
                check(
                    InvariantId::SqlPipelineCovered,
                    input.sql_pipeline_bound >= 1
                        && input.sql_pipeline_authorized >= 1
                        && input.sql_pipeline_logical >= 1
                        && input.sql_pipeline_physical >= 1
                        && input.sql_pipeline_rows >= 1
                        && input.sql_pipeline_rejections >= 1,
                    format!(
                        "bound={} authorized={} logical={} physical={} rows={} rejections={}",
                        input.sql_pipeline_bound,
                        input.sql_pipeline_authorized,
                        input.sql_pipeline_logical,
                        input.sql_pipeline_physical,
                        input.sql_pipeline_rows,
                        input.sql_pipeline_rejections
                    ),
                ),
                check(
                    InvariantId::ConsistencyModesSeparated,
                    input.consistency_strict >= 3
                        && input.consistency_availability_first >= 1
                        && input.consistency_rejections >= 1,
                    format!(
                        "strict={} availability_first={} rejections={}",
                        input.consistency_strict,
                        input.consistency_availability_first,
                        input.consistency_rejections
                    ),
                ),
                check(
                    InvariantId::SqlDmlAtomicMutation,
                    input.sql_dml_bound >= 3
                        && input.sql_dml_authorized >= 3
                        && input.sql_dml_planned >= 3
                        && input.sql_dml_prepared >= 3
                        && input.sql_dml_committed >= 3
                        && input.sql_dml_isolated >= 3
                        && input.sql_dml_kv_applied >= 3
                        && input.sql_dml_session_staged >= 1
                        && input.sql_dml_overlay_verified >= 2
                        && input.sql_dml_rejections >= 1,
                    format!(
                        "bound={} authorized={} planned={} prepared={} committed={} isolated={} kv_applied={} session_staged={} overlay_verified={} rejections={}",
                        input.sql_dml_bound,
                        input.sql_dml_authorized,
                        input.sql_dml_planned,
                        input.sql_dml_prepared,
                        input.sql_dml_committed,
                        input.sql_dml_isolated,
                        input.sql_dml_kv_applied,
                        input.sql_dml_session_staged,
                        input.sql_dml_overlay_verified,
                        input.sql_dml_rejections
                    ),
                ),
                check(
                    InvariantId::DeferredConstraintsCommitTime,
                    input.deferred_transactions >= 2
                        && input.deferred_obligations >= 2
                        && input.deferred_txn_references >= 1
                        && input.deferred_commits >= 1
                        && input.deferred_rejections >= 1,
                    format!(
                        "transactions={} obligations={} txn_references={} commits={} rejections={}",
                        input.deferred_transactions,
                        input.deferred_obligations,
                        input.deferred_txn_references,
                        input.deferred_commits,
                        input.deferred_rejections
                    ),
                ),
                check(
                    InvariantId::SchemaMigrationLifecycle,
                    input.migration_begun >= 2
                        && input.migration_validated >= 2
                        && input.migration_published >= 2
                        && input.migration_backfilled_rows >= 1
                        && input.migration_indexed_keys >= 1
                        && input.migration_resumed >= 1
                        && input.migration_completed >= 1
                        && input.migration_rolled_back >= 1
                        && input.migration_rejected >= 1,
                    format!(
                        "begun={} validated={} published={} backfilled_rows={} indexed_keys={} resumed={} completed={} rolled_back={} rejected={}",
                        input.migration_begun,
                        input.migration_validated,
                        input.migration_published,
                        input.migration_backfilled_rows,
                        input.migration_indexed_keys,
                        input.migration_resumed,
                        input.migration_completed,
                        input.migration_rolled_back,
                        input.migration_rejected
                    ),
                ),
                check(
                    InvariantId::SqlTransactionLifecycle,
                    input.sql_txn_began >= 1
                        && input.sql_txn_dml_staged >= 1
                        && input.sql_txn_overlay_reads >= 1
                        && input.sql_txn_commits >= 1
                        && input.sql_txn_rollbacks >= 1
                        && input.sql_txn_rollback_absent >= 1
                        && input.sql_txn_savepoints >= 1
                        && input.sql_txn_savepoint_rewinds >= 1
                        && input.sql_txn_savepoint_absent >= 1
                        && input.sql_txn_rejections >= 1,
                    format!(
                        "began={} dml_staged={} overlay_reads={} commits={} rollbacks={} rollback_absent={} savepoints={} savepoint_rewinds={} savepoint_absent={} rejections={}",
                        input.sql_txn_began,
                        input.sql_txn_dml_staged,
                        input.sql_txn_overlay_reads,
                        input.sql_txn_commits,
                        input.sql_txn_rollbacks,
                        input.sql_txn_rollback_absent,
                        input.sql_txn_savepoints,
                        input.sql_txn_savepoint_rewinds,
                        input.sql_txn_savepoint_absent,
                        input.sql_txn_rejections
                    ),
                ),
                check(
                    InvariantId::ObjectMetadataHeadVisible,
                    input.objectmeta_buckets >= 1
                        && input.objectmeta_uploads >= 1
                        && input.objectmeta_parts >= 2
                        && input.objectmeta_heads >= 1
                        && input.objectmeta_rejections >= 1,
                    format!(
                        "buckets={} uploads={} parts={} heads={} rejections={}",
                        input.objectmeta_buckets,
                        input.objectmeta_uploads,
                        input.objectmeta_parts,
                        input.objectmeta_heads,
                        input.objectmeta_rejections
                    ),
                ),
                check(
                    InvariantId::StrictSessionReadYourWrites,
                    input.sessiontxn_sessions >= 1
                        && input.sessiontxn_pending_writes >= 1
                        && input.sessiontxn_overlay_reads >= 1
                        && input.sessiontxn_snapshot_reads >= 1
                        && input.sessiontxn_commits >= 1
                        && input.sessiontxn_rollbacks >= 1
                        && input.sessiontxn_savepoints >= 1
                        && input.sessiontxn_savepoint_rewinds >= 1
                        && input.sessiontxn_rejections >= 1,
                    format!(
                        "sessions={} pending_writes={} overlay_reads={} snapshot_reads={} commits={} rollbacks={} savepoints={} savepoint_rewinds={} rejections={}",
                        input.sessiontxn_sessions,
                        input.sessiontxn_pending_writes,
                        input.sessiontxn_overlay_reads,
                        input.sessiontxn_snapshot_reads,
                        input.sessiontxn_commits,
                        input.sessiontxn_rollbacks,
                        input.sessiontxn_savepoints,
                        input.sessiontxn_savepoint_rewinds,
                        input.sessiontxn_rejections
                    ),
                ),
                check(
                    InvariantId::PrimaryIndexIdentity,
                    input.primary_inserts >= 1
                        && input.primary_lookups >= 2
                        && input.primary_updates >= 1
                        && input.primary_deletes >= 1
                        && input.primary_rejections >= 1
                        && input.primary_entries == 0,
                    format!(
                        "inserts={} lookups={} updates={} deletes={} rejections={} entries={}",
                        input.primary_inserts,
                        input.primary_lookups,
                        input.primary_updates,
                        input.primary_deletes,
                        input.primary_rejections,
                        input.primary_entries
                    ),
                ),
                check(
                    InvariantId::OrderedRangeNonOverlap,
                    input.range_partitions >= 2
                        && input.range_non_overlaps >= 1
                        && input.range_rejections >= 1
                        && input.sstable_manifests >= 3
                        && input.compaction_inputs >= 2,
                    format!(
                        "partitions={} non_overlaps={} rejections={} sstables={} compaction_inputs={}",
                        input.range_partitions,
                        input.range_non_overlaps,
                        input.range_rejections,
                        input.sstable_manifests,
                        input.compaction_inputs
                    ),
                ),
                check(
                    InvariantId::PreparedSqlLifecycle,
                    input.prepared_statements >= 5
                        && input.prepared_binds >= 1
                        && input.prepared_invalidations >= 1
                        && input.prepared_deallocations >= 1
                        && input.prepared_evictions >= 1
                        && input.prepared_rejections >= 1,
                    format!(
                        "statements={} binds={} invalidations={} deallocations={} evictions={} rejections={}",
                        input.prepared_statements,
                        input.prepared_binds,
                        input.prepared_invalidations,
                        input.prepared_deallocations,
                        input.prepared_evictions,
                        input.prepared_rejections
                    ),
                ),
                check(
                    InvariantId::SqlPortalLifecycle,
                    input.portal_parses >= 2
                        && input.portal_binds >= 2
                        && input.portal_describes >= 1
                        && input.portal_executes >= 1
                        && input.portal_rows >= 1
                        && input.portal_cancellations >= 1
                        && input.portal_closes >= 1
                        && input.portal_statement_closes >= 1
                        && input.portal_rejections >= 2,
                    format!(
                        "parses={} binds={} describes={} executes={} rows={} cancellations={} portal_closes={} statement_closes={} rejections={}",
                        input.portal_parses,
                        input.portal_binds,
                        input.portal_describes,
                        input.portal_executes,
                        input.portal_rows,
                        input.portal_cancellations,
                        input.portal_closes,
                        input.portal_statement_closes,
                        input.portal_rejections
                    ),
                ),
                check(
                    InvariantId::RelationalRowEncoding,
                    input.rowcodec_encoded >= 1
                        && input.rowcodec_decoded >= 1
                        && input.rowcodec_defaults >= 1
                        && input.rowcodec_generated >= 1
                        && input.rowcodec_rejections >= 2
                        && input.rowcodec_bytes > 0,
                    format!(
                        "encoded={} decoded={} defaults={} generated={} bytes={} rejections={}",
                        input.rowcodec_encoded,
                        input.rowcodec_decoded,
                        input.rowcodec_defaults,
                        input.rowcodec_generated,
                        input.rowcodec_bytes,
                        input.rowcodec_rejections
                    ),
                ),
                check(
                    InvariantId::RelationalTupleVisibility,
                    input.tuple_versions >= 2
                        && input.tuple_visible_reads >= 2
                        && input.tuple_delete_markers >= 2
                        && input.tuple_rejections >= 2
                        && input.tuple_bytes > 0,
                    format!(
                        "versions={} visible_reads={} delete_markers={} bytes={} rejections={}",
                        input.tuple_versions,
                        input.tuple_visible_reads,
                        input.tuple_delete_markers,
                        input.tuple_bytes,
                        input.tuple_rejections
                    ),
                ),
                check(
                    InvariantId::RelationalVacuumSafety,
                    input.vacuum_chains >= 2
                        && input.vacuum_pruned_versions >= 2
                        && input.vacuum_retained_versions >= 4
                        && input.vacuum_rejections >= 1
                        && input.vacuum_reclaimed_bytes > 0,
                    format!(
                        "chains={} pruned_versions={} retained_versions={} reclaimed_bytes={} rejections={}",
                        input.vacuum_chains,
                        input.vacuum_pruned_versions,
                        input.vacuum_retained_versions,
                        input.vacuum_reclaimed_bytes,
                        input.vacuum_rejections
                    ),
                ),
                check(
                    InvariantId::RelationalRowMaterialization,
                    input.materialized_rows >= 2
                        && input.materialized_projected >= 1
                        && input.materialized_columns >= 5
                        && input.materialized_rejections >= 2
                        && input.materialized_bytes > 0,
                    format!(
                        "rows={} projected={} columns={} bytes={} rejections={}",
                        input.materialized_rows,
                        input.materialized_projected,
                        input.materialized_columns,
                        input.materialized_bytes,
                        input.materialized_rejections
                    ),
                ),
                check(
                    InvariantId::RelationalCommitGroupAtomicity,
                    input.relcommit_groups >= 3
                        && input.relcommit_row_mutations >= 3
                        && input.relcommit_index_mutations >= 4
                        && input.relcommit_conflict_ranges >= 6
                        && input.relcommit_rejections >= 1,
                    format!(
                        "groups={} row_mutations={} index_mutations={} conflict_ranges={} rejections={}",
                        input.relcommit_groups,
                        input.relcommit_row_mutations,
                        input.relcommit_index_mutations,
                        input.relcommit_conflict_ranges,
                        input.relcommit_rejections
                    ),
                ),
                check(
                    InvariantId::StrictIsolationRejectedStaleRead,
                    input.isolation_rejections >= 1,
                    format!("isolation_rejections={}", input.isolation_rejections),
                ),
                check(
                    InvariantId::StorageCommitTailsPresent,
                    input.committed_transactions >= 3
                        && input.storage_committed_prefixes >= 3
                        && input.storage_manifest_covered >= input.manifest_entries
                        && input.protocol_commits >= 20,
                    format!(
                        "storage_commit_tails={} committed_prefixes={} manifest_covered={} protocol_commits={}",
                        input.committed_transactions,
                        input.storage_committed_prefixes,
                        input.storage_manifest_covered,
                        input.protocol_commits
                    ),
                ),
            ],
        }
    }
}

fn check(id: InvariantId, passed: bool, detail: String) -> InvariantCheck {
    InvariantCheck { id, passed, detail }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_complete_poc_shape() {
        let report = InvariantChecker::evaluate(&complete_input());

        assert!(report.is_ok());
        assert_eq!(report.passed(), 34);
        assert_eq!(report.failed(), 0);
    }

    #[test]
    fn rejects_missing_manifest_coverage() {
        let mut input = complete_input();
        input.manifest_surfaces = 2;

        let report = InvariantChecker::evaluate(&input);

        assert!(!report.is_ok());
        assert_eq!(report.failed(), 1);
        assert_eq!(
            report
                .checks
                .iter()
                .find(|check| check.id == InvariantId::MultiSurfaceLayout)
                .unwrap()
                .passed,
            false
        );
    }

    #[test]
    fn rejects_missing_stale_primary_evidence() {
        let mut input = complete_input();
        input.vsr_rejections = 0;

        let report = InvariantChecker::evaluate(&input);

        assert!(!report.is_ok());
        assert!(
            report
                .checks
                .iter()
                .any(|check| { check.id == InvariantId::StalePrimaryRejected && !check.passed })
        );
    }

    fn complete_input() -> InvariantInput {
        InvariantInput {
            admitted_nodes: 5,
            placement_groups: 3,
            membership_voters: 3,
            membership_promotions: 1,
            membership_intersections: 1,
            vsr_commits: 2,
            vsr_rejections: 1,
            fence_rejections: 1,
            manifest_entries: 6,
            manifest_surfaces: 5,
            manifest_rejections: 1,
            superblock_roots: 7,
            superblock_rejections: 1,
            snapshot_roots: 7,
            restore_executed_items: 7,
            restore_executed_surfaces: 7,
            recovery_roots: 7,
            recovery_rejections: 1,
            failure_drill_passed: 1,
            failure_drill_domains: 6,
            repair_findings: 5,
            repair_actions: 8,
            rebalance_moves: 1,
            rebalance_policy_proofs: 1,
            workload_classes: 4,
            workload_budget_paths: 4,
            workload_benchmark_claim: 0,
            stream_verified_parts: 3,
            erasure_tolerated_loss: 2,
            chunkstore_chunks: 5,
            chunkstore_verified: 3,
            chunkstore_deduped: 1,
            chunkstore_rejections: 1,
            checkpoint_roots: 7,
            checkpoint_compacted: 2,
            checkpoint_reclaimable_bytes: 3_584,
            checkpoint_rejections: 1,
            sql_pipeline_bound: 1,
            sql_pipeline_authorized: 1,
            sql_pipeline_logical: 1,
            sql_pipeline_physical: 1,
            sql_pipeline_rows: 1,
            sql_pipeline_rejections: 1,
            sql_dml_bound: 4,
            sql_dml_authorized: 4,
            sql_dml_planned: 4,
            sql_dml_prepared: 3,
            sql_dml_committed: 3,
            sql_dml_isolated: 3,
            sql_dml_kv_applied: 3,
            sql_dml_session_staged: 1,
            sql_dml_overlay_verified: 2,
            sql_dml_rejections: 1,
            deferred_transactions: 2,
            deferred_obligations: 2,
            deferred_txn_references: 1,
            deferred_commits: 1,
            deferred_rejections: 1,
            migration_begun: 2,
            migration_validated: 2,
            migration_published: 2,
            migration_backfilled_rows: 1_000,
            migration_indexed_keys: 1_000,
            migration_resumed: 1,
            migration_completed: 1,
            migration_rolled_back: 1,
            migration_rejected: 1,
            sql_txn_began: 3,
            sql_txn_dml_staged: 4,
            sql_txn_overlay_reads: 3,
            sql_txn_commits: 2,
            sql_txn_rollbacks: 1,
            sql_txn_rollback_absent: 1,
            sql_txn_savepoints: 1,
            sql_txn_savepoint_rewinds: 1,
            sql_txn_savepoint_absent: 1,
            sql_txn_rejections: 1,
            consistency_strict: 3,
            consistency_availability_first: 1,
            consistency_rejections: 1,
            objectmeta_buckets: 1,
            objectmeta_uploads: 1,
            objectmeta_parts: 2,
            objectmeta_heads: 1,
            objectmeta_rejections: 1,
            sessiontxn_sessions: 5,
            sessiontxn_pending_writes: 11,
            sessiontxn_overlay_reads: 14,
            sessiontxn_snapshot_reads: 2,
            sessiontxn_commits: 4,
            sessiontxn_rollbacks: 1,
            sessiontxn_savepoints: 1,
            sessiontxn_savepoint_rewinds: 1,
            sessiontxn_rejections: 1,
            prepared_statements: 5,
            prepared_binds: 1,
            prepared_invalidations: 1,
            prepared_deallocations: 1,
            prepared_evictions: 1,
            prepared_rejections: 1,
            portal_parses: 2,
            portal_binds: 2,
            portal_describes: 1,
            portal_executes: 1,
            portal_rows: 1,
            portal_cancellations: 1,
            portal_closes: 1,
            portal_statement_closes: 1,
            portal_rejections: 2,
            rowcodec_encoded: 1,
            rowcodec_decoded: 1,
            rowcodec_defaults: 1,
            rowcodec_generated: 1,
            rowcodec_rejections: 2,
            rowcodec_bytes: 80,
            tuple_versions: 2,
            tuple_visible_reads: 2,
            tuple_delete_markers: 2,
            tuple_rejections: 2,
            tuple_bytes: 160,
            vacuum_chains: 2,
            vacuum_pruned_versions: 2,
            vacuum_retained_versions: 4,
            vacuum_rejections: 1,
            vacuum_reclaimed_bytes: 12,
            materialized_rows: 2,
            materialized_projected: 1,
            materialized_columns: 5,
            materialized_rejections: 2,
            materialized_bytes: 166,
            relcommit_groups: 3,
            relcommit_row_mutations: 3,
            relcommit_index_mutations: 4,
            relcommit_conflict_ranges: 6,
            relcommit_rejections: 1,
            primary_inserts: 1,
            primary_lookups: 2,
            primary_updates: 1,
            primary_deletes: 1,
            primary_rejections: 1,
            primary_entries: 0,
            range_partitions: 2,
            range_non_overlaps: 1,
            range_rejections: 1,
            sstable_manifests: 3,
            compaction_inputs: 2,
            isolation_rejections: 1,
            committed_transactions: 3,
            storage_committed_prefixes: 3,
            storage_manifest_covered: 6,
            protocol_commits: 34,
        }
    }
}
