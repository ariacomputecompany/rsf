#![forbid(unsafe_code)]

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequirementStatus {
    Structural,
    Missing,
}

impl RequirementStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Structural => "structural",
            Self::Missing => "missing",
        }
    }

    pub fn is_covered(self) -> bool {
        matches!(self, Self::Structural)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequirementCheck {
    pub id: &'static str,
    pub title: &'static str,
    pub status: RequirementStatus,
    pub detail: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequirementReport {
    pub implementation: String,
    pub checks: Vec<RequirementCheck>,
}

impl RequirementReport {
    pub fn total(&self) -> usize {
        self.checks.len()
    }

    pub fn covered(&self) -> usize {
        self.checks
            .iter()
            .filter(|check| check.status.is_covered())
            .count()
    }

    pub fn structural(&self) -> usize {
        self.count_status(RequirementStatus::Structural)
    }

    pub fn missing(&self) -> usize {
        self.count_status(RequirementStatus::Missing)
    }

    fn count_status(&self, status: RequirementStatus) -> usize {
        self.checks
            .iter()
            .filter(|check| check.status == status)
            .count()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequirementInput {
    pub implementation: String,
    pub nodes: u8,
    pub regions: u8,
    pub deployment_roles: u8,
    pub deployment_endpoints: u8,
    pub deployment_volumes: u8,
    pub deployment_probes: u8,
    pub admitted_nodes: u8,
    pub membership_epoch: u8,
    pub membership_promotions: u8,
    pub membership_intersections: u8,
    pub membership_rejections: u8,
    pub vsr_commits: u8,
    pub vsr_view_changes: u8,
    pub vsr_rejections: u8,
    pub tenant_surfaces: u8,
    pub catalog_items: u8,
    pub auth_grants: u8,
    pub wire_protocols: u8,
    pub gateway_surfaces: u8,
    pub consistency_strict: u8,
    pub consistency_availability_first: u8,
    pub consistency_rejections: u8,
    pub quota_policies: u8,
    pub rpc_calls: u8,
    pub idempotency_replays: u8,
    pub schema_steps: u8,
    pub migration_begun: u8,
    pub migration_backfilled_rows: u16,
    pub migration_indexed_keys: u16,
    pub migration_completed: u8,
    pub migration_rolled_back: u8,
    pub migration_rejected: u8,
    pub jobs_enqueued: u8,
    pub session_bound: u8,
    pub primary_inserts: u8,
    pub primary_lookups: u8,
    pub primary_updates: u8,
    pub primary_deletes: u8,
    pub primary_rejections: u8,
    pub index_mutations: u8,
    pub sql_pipeline_rows: u8,
    pub sql_pipeline_rejections: u8,
    pub sql_dml_bound: u8,
    pub sql_dml_committed: u8,
    pub sql_dml_kv_applied: u8,
    pub sql_dml_session_staged: u8,
    pub sql_dml_overlay_verified: u8,
    pub sql_dml_rejections: u8,
    pub deferred_transactions: u8,
    pub deferred_obligations: u8,
    pub deferred_txn_references: u8,
    pub deferred_commits: u8,
    pub deferred_rejections: u8,
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
    pub query_rows: u8,
    pub planner_physical_plans: u8,
    pub executor_stages: u8,
    pub page_frames: u8,
    pub page_redo: u8,
    pub wal_flush_allows: u8,
    pub wal_rejections: u8,
    pub buffer_hits: u8,
    pub buffer_flushes: u8,
    pub kv_keys: u8,
    pub range_partitions: u8,
    pub range_non_overlaps: u8,
    pub range_rejections: u8,
    pub sstable_manifests: u8,
    pub compaction_inputs: u8,
    pub sessiontxn_overlay_reads: u8,
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
    pub watch_deliveries: u8,
    pub object_versions: u8,
    pub objectmeta_buckets: u8,
    pub objectmeta_heads: u8,
    pub objectmeta_rejections: u8,
    pub blob_chunks: u8,
    pub manifest_entries: u8,
    pub manifest_surfaces: u8,
    pub chunkstore_chunks: u8,
    pub chunkstore_verified: u8,
    pub chunkstore_deduped: u8,
    pub checkpoint_roots: u8,
    pub superblock_roots: u8,
    pub snapshot_roots: u8,
    pub backup_items: u8,
    pub restore_executed_items: u8,
    pub restore_executed_surfaces: u8,
    pub recovery_roots: u8,
    pub recovery_rejections: u8,
    pub repair_actions: u8,
    pub retention_actions: u8,
    pub provider_selected: u8,
    pub provider_degraded: u8,
    pub stream_read_lanes: u8,
    pub stream_verified_parts: u8,
    pub erasure_tolerated_loss: u8,
    pub cache_hits: u8,
    pub workload_classes: u8,
    pub workload_budget_paths: u8,
    pub workload_benchmark_claim: u8,
    pub rebalance_moves: u8,
    pub rebalance_policy_proofs: u8,
    pub health_demote: u8,
    pub fault_events: u8,
    pub dropped_messages: u8,
    pub delayed_messages: u8,
    pub failure_drill_passed: u8,
    pub failure_drill_domains: u8,
    pub protocol_commits: u8,
    pub committed_transactions: u8,
    pub storage_committed_prefixes: u8,
    pub storage_manifest_covered: u8,
    pub testability_checks: u8,
    pub testability_passed: u8,
    pub testability_trace_events: u16,
    pub testability_trace_services: u8,
    pub testability_replay_ready: u8,
    pub invariant_failed: u8,
    pub observability_metrics: u16,
}

#[derive(Clone, Debug, Default)]
pub struct RequirementAuditor;

impl RequirementAuditor {
    pub fn evaluate(input: &RequirementInput) -> RequirementReport {
        let checks = vec![
            check(
                "full-database-surfaces",
                "Full database surfaces",
                input.tenant_surfaces >= 4
                    && input.gateway_surfaces >= 4
                    && input.wire_protocols >= 5
                    && input.catalog_items >= 7,
                RequirementStatus::Structural,
                format!(
                    "surfaces={} gateway_surfaces={} wire_protocols={} catalog_items={}",
                    input.tenant_surfaces,
                    input.gateway_surfaces,
                    input.wire_protocols,
                    input.catalog_items
                ),
            ),
            check(
                "single-binary-join",
                "One binary node join and promotion",
                input.admitted_nodes == input.nodes
                    && input.deployment_roles >= input.nodes
                    && input.membership_promotions >= 1
                    && input.membership_intersections >= input.membership_promotions
                    && input.membership_rejections >= 1,
                RequirementStatus::Structural,
                format!(
                    "nodes={} admitted={} roles={} promotions={} quorum_intersections={} unsafe_rejections={}",
                    input.nodes,
                    input.admitted_nodes,
                    input.deployment_roles,
                    input.membership_promotions,
                    input.membership_intersections,
                    input.membership_rejections
                ),
            ),
            check(
                "deployment-upgrade-envelope",
                "Deployment and upgrade envelope",
                input.deployment_endpoints >= 8
                    && input.deployment_volumes >= input.nodes
                    && input.deployment_probes >= 8
                    && input.schema_steps >= 1
                    && input.migration_begun >= 2
                    && input.migration_backfilled_rows >= 1
                    && input.migration_indexed_keys >= 1
                    && input.migration_completed >= 1
                    && input.migration_rolled_back >= 1
                    && input.migration_rejected >= 1
                    && input.jobs_enqueued >= 1,
                RequirementStatus::Structural,
                format!(
                    "endpoints={} volumes={} probes={} schema_steps={} migration_begun={} migration_backfilled_rows={} migration_indexed_keys={} migration_completed={} migration_rolled_back={} migration_rejected={} jobs={}",
                    input.deployment_endpoints,
                    input.deployment_volumes,
                    input.deployment_probes,
                    input.schema_steps,
                    input.migration_begun,
                    input.migration_backfilled_rows,
                    input.migration_indexed_keys,
                    input.migration_completed,
                    input.migration_rolled_back,
                    input.migration_rejected,
                    input.jobs_enqueued
                ),
            ),
            check(
                "quorum-mutable-truth",
                "Quorum mutable truth",
                input.vsr_commits >= 2
                    && input.vsr_view_changes >= 1
                    && input.vsr_rejections >= 1
                    && input.protocol_commits >= 1
                    && input.committed_transactions >= 1,
                RequirementStatus::Structural,
                format!(
                    "vsr_commits={} view_changes={} stale_primary_rejections={} protocol_commits={} durable_tails={}",
                    input.vsr_commits,
                    input.vsr_view_changes,
                    input.vsr_rejections,
                    input.protocol_commits,
                    input.committed_transactions
                ),
            ),
            check(
                "relational-native-path",
                "Native relational path",
                input.session_bound >= 1
                    && input.primary_inserts >= 1
                    && input.primary_lookups >= 2
                    && input.primary_updates >= 1
                    && input.primary_deletes >= 1
                    && input.primary_rejections >= 1
                    && input.index_mutations >= 1
                    && input.page_frames >= 1
                    && input.page_redo >= 1
                    && input.wal_flush_allows >= 1
                    && input.wal_rejections >= 1
                    && input.buffer_hits >= 1
                    && input.buffer_flushes >= 1
                    && input.rowcodec_encoded >= 1
                    && input.rowcodec_decoded >= 1
                    && input.rowcodec_defaults >= 1
                    && input.rowcodec_generated >= 1
                    && input.rowcodec_rejections >= 2
                    && input.rowcodec_bytes > 0
                    && input.tuple_versions >= 2
                    && input.tuple_visible_reads >= 2
                    && input.tuple_delete_markers >= 2
                    && input.tuple_rejections >= 2
                    && input.tuple_bytes > 0
                    && input.vacuum_chains >= 2
                    && input.vacuum_pruned_versions >= 2
                    && input.vacuum_retained_versions >= 4
                    && input.vacuum_rejections >= 1
                    && input.vacuum_reclaimed_bytes > 0
                    && input.materialized_rows >= 2
                    && input.materialized_projected >= 1
                    && input.materialized_columns >= 5
                    && input.materialized_rejections >= 2
                    && input.materialized_bytes > 0
                    && input.relcommit_groups >= 3
                    && input.relcommit_row_mutations >= 3
                    && input.relcommit_index_mutations >= 4
                    && input.relcommit_conflict_ranges >= 6
                    && input.relcommit_rejections >= 1,
                RequirementStatus::Structural,
                format!(
                    "sessions={} primary_inserts={} primary_lookups={} primary_updates={} primary_deletes={} primary_rejections={} index_mutations={} pages={} redo={} wal_allows={} wal_rejections={} buffer_hits={} buffer_flushes={} rowcodec_encoded={} rowcodec_decoded={} rowcodec_defaults={} rowcodec_generated={} rowcodec_rejections={} rowcodec_bytes={} tuple_versions={} tuple_visible_reads={} tuple_delete_markers={} tuple_rejections={} tuple_bytes={} vacuum_chains={} vacuum_pruned_versions={} vacuum_retained_versions={} vacuum_rejections={} vacuum_reclaimed_bytes={} materialized_rows={} materialized_projected={} materialized_columns={} materialized_rejections={} materialized_bytes={} relcommit_groups={} relcommit_row_mutations={} relcommit_index_mutations={} relcommit_conflict_ranges={} relcommit_rejections={}",
                    input.session_bound,
                    input.primary_inserts,
                    input.primary_lookups,
                    input.primary_updates,
                    input.primary_deletes,
                    input.primary_rejections,
                    input.index_mutations,
                    input.page_frames,
                    input.page_redo,
                    input.wal_flush_allows,
                    input.wal_rejections,
                    input.buffer_hits,
                    input.buffer_flushes,
                    input.rowcodec_encoded,
                    input.rowcodec_decoded,
                    input.rowcodec_defaults,
                    input.rowcodec_generated,
                    input.rowcodec_rejections,
                    input.rowcodec_bytes,
                    input.tuple_versions,
                    input.tuple_visible_reads,
                    input.tuple_delete_markers,
                    input.tuple_rejections,
                    input.tuple_bytes,
                    input.vacuum_chains,
                    input.vacuum_pruned_versions,
                    input.vacuum_retained_versions,
                    input.vacuum_rejections,
                    input.vacuum_reclaimed_bytes,
                    input.materialized_rows,
                    input.materialized_projected,
                    input.materialized_columns,
                    input.materialized_rejections,
                    input.materialized_bytes,
                    input.relcommit_groups,
                    input.relcommit_row_mutations,
                    input.relcommit_index_mutations,
                    input.relcommit_conflict_ranges,
                    input.relcommit_rejections
                ),
            ),
            check(
                "sql-query-execution",
                "SQL query execution pipeline",
                input.sql_pipeline_rows >= 1
                    && input.sql_pipeline_rejections >= 1
                    && input.sql_dml_bound >= 3
                    && input.sql_dml_committed >= 3
                    && input.sql_dml_kv_applied >= 3
                    && input.sql_dml_session_staged >= 1
                    && input.sql_dml_overlay_verified >= 2
                    && input.sql_dml_rejections >= 1
                    && input.deferred_transactions >= 2
                    && input.deferred_obligations >= 2
                    && input.deferred_txn_references >= 1
                    && input.deferred_commits >= 1
                    && input.deferred_rejections >= 1
                    && input.sql_txn_began >= 1
                    && input.sql_txn_dml_staged >= 1
                    && input.sql_txn_overlay_reads >= 1
                    && input.sql_txn_commits >= 1
                    && input.sql_txn_rollbacks >= 1
                    && input.sql_txn_rollback_absent >= 1
                    && input.sql_txn_savepoints >= 1
                    && input.sql_txn_savepoint_rewinds >= 1
                    && input.sql_txn_savepoint_absent >= 1
                    && input.sql_txn_rejections >= 1
                    && input.prepared_statements >= 5
                    && input.prepared_binds >= 1
                    && input.prepared_invalidations >= 1
                    && input.prepared_deallocations >= 1
                    && input.prepared_evictions >= 1
                    && input.prepared_rejections >= 1
                    && input.portal_parses >= 2
                    && input.portal_binds >= 2
                    && input.portal_describes >= 1
                    && input.portal_executes >= 1
                    && input.portal_rows >= 1
                    && input.portal_cancellations >= 1
                    && input.portal_closes >= 1
                    && input.portal_statement_closes >= 1
                    && input.portal_rejections >= 2
                    && input.query_rows >= 1
                    && input.planner_physical_plans >= 1
                    && input.executor_stages >= 1,
                RequirementStatus::Structural,
                format!(
                    "pipeline_rows={} pipeline_rejections={} dml_bound={} dml_committed={} dml_kv_applied={} dml_session_staged={} dml_overlay_verified={} dml_rejections={} deferred_transactions={} deferred_obligations={} deferred_txn_references={} deferred_commits={} deferred_rejections={} sqltxn_began={} sqltxn_dml_staged={} sqltxn_overlay_reads={} sqltxn_commits={} sqltxn_rollbacks={} sqltxn_rollback_absent={} sqltxn_savepoints={} sqltxn_savepoint_rewinds={} sqltxn_savepoint_absent={} sqltxn_rejections={} prepared_statements={} prepared_binds={} prepared_invalidations={} prepared_deallocations={} prepared_evictions={} prepared_rejections={} portal_parses={} portal_binds={} portal_describes={} portal_executes={} portal_rows={} portal_cancellations={} portal_closes={} portal_statement_closes={} portal_rejections={} query_rows={} plans={} stages={}",
                    input.sql_pipeline_rows,
                    input.sql_pipeline_rejections,
                    input.sql_dml_bound,
                    input.sql_dml_committed,
                    input.sql_dml_kv_applied,
                    input.sql_dml_session_staged,
                    input.sql_dml_overlay_verified,
                    input.sql_dml_rejections,
                    input.deferred_transactions,
                    input.deferred_obligations,
                    input.deferred_txn_references,
                    input.deferred_commits,
                    input.deferred_rejections,
                    input.sql_txn_began,
                    input.sql_txn_dml_staged,
                    input.sql_txn_overlay_reads,
                    input.sql_txn_commits,
                    input.sql_txn_rollbacks,
                    input.sql_txn_rollback_absent,
                    input.sql_txn_savepoints,
                    input.sql_txn_savepoint_rewinds,
                    input.sql_txn_savepoint_absent,
                    input.sql_txn_rejections,
                    input.prepared_statements,
                    input.prepared_binds,
                    input.prepared_invalidations,
                    input.prepared_deallocations,
                    input.prepared_evictions,
                    input.prepared_rejections,
                    input.portal_parses,
                    input.portal_binds,
                    input.portal_describes,
                    input.portal_executes,
                    input.portal_rows,
                    input.portal_cancellations,
                    input.portal_closes,
                    input.portal_statement_closes,
                    input.portal_rejections,
                    input.query_rows,
                    input.planner_physical_plans,
                    input.executor_stages
                ),
            ),
            check(
                "ordered-kv-contract",
                "Ordered KV contract",
                input.kv_keys >= 1
                    && input.watch_deliveries >= 1
                    && input.sessiontxn_overlay_reads >= 1
                    && input.sessiontxn_commits >= 1
                    && input.sessiontxn_rollbacks >= 1
                    && input.sessiontxn_savepoints >= 1
                    && input.sessiontxn_savepoint_rewinds >= 1
                    && input.sessiontxn_rejections >= 1
                    && input.range_partitions >= 2
                    && input.range_non_overlaps >= 1
                    && input.range_rejections >= 1
                    && input.sstable_manifests >= 3
                    && input.compaction_inputs >= 2,
                RequirementStatus::Structural,
                format!(
                    "visible_keys={} watch_deliveries={} ryw_overlay_reads={} ryw_commits={} ryw_rollbacks={} ryw_savepoints={} ryw_savepoint_rewinds={} ryw_rejections={} range_partitions={} range_non_overlaps={} range_rejections={} sstables={} compaction_inputs={}",
                    input.kv_keys,
                    input.watch_deliveries,
                    input.sessiontxn_overlay_reads,
                    input.sessiontxn_commits,
                    input.sessiontxn_rollbacks,
                    input.sessiontxn_savepoints,
                    input.sessiontxn_savepoint_rewinds,
                    input.sessiontxn_rejections,
                    input.range_partitions,
                    input.range_non_overlaps,
                    input.range_rejections,
                    input.sstable_manifests,
                    input.compaction_inputs
                ),
            ),
            check(
                "object-blob-contract",
                "Object and blob contract",
                input.object_versions >= 1
                    && input.objectmeta_buckets >= 1
                    && input.objectmeta_heads >= 1
                    && input.objectmeta_rejections >= 1
                    && input.blob_chunks >= 1
                    && input.provider_selected >= 1
                    && input.retention_actions >= 1,
                RequirementStatus::Structural,
                format!(
                    "object_versions={} objectmeta_buckets={} objectmeta_heads={} objectmeta_rejections={} blob_chunks={} providers_selected={} retention_actions={}",
                    input.object_versions,
                    input.objectmeta_buckets,
                    input.objectmeta_heads,
                    input.objectmeta_rejections,
                    input.blob_chunks,
                    input.provider_selected,
                    input.retention_actions
                ),
            ),
            check(
                "format-independent-layout",
                "Format-independent layout",
                input.manifest_entries >= 6
                    && input.manifest_surfaces >= 5
                    && input.storage_manifest_covered >= input.manifest_entries
                    && input.storage_committed_prefixes >= 3
                    && input.chunkstore_chunks >= 1
                    && input.checkpoint_roots >= 7
                    && input.superblock_roots >= 6
                    && input.snapshot_roots >= 6,
                RequirementStatus::Structural,
                format!(
                    "manifest_entries={} manifest_surfaces={} covered_entries={} committed_prefixes={} chunks={} checkpoint_roots={} superblock_roots={} snapshot_roots={}",
                    input.manifest_entries,
                    input.manifest_surfaces,
                    input.storage_manifest_covered,
                    input.storage_committed_prefixes,
                    input.chunkstore_chunks,
                    input.checkpoint_roots,
                    input.superblock_roots,
                    input.snapshot_roots
                ),
            ),
            check(
                "large-payload-repairability",
                "Large payload repairability",
                input.chunkstore_verified >= 1
                    && input.chunkstore_deduped >= 1
                    && input.stream_read_lanes >= 2
                    && input.stream_verified_parts >= 1
                    && input.erasure_tolerated_loss >= 1
                    && input.repair_actions >= 1,
                RequirementStatus::Structural,
                format!(
                    "verified_chunks={} dedupe={} read_lanes={} verified_parts={} erasure_loss={} repair_actions={}",
                    input.chunkstore_verified,
                    input.chunkstore_deduped,
                    input.stream_read_lanes,
                    input.stream_verified_parts,
                    input.erasure_tolerated_loss,
                    input.repair_actions
                ),
            ),
            check(
                "failure-model-coverage",
                "Failure model coverage",
                input.fault_events >= 5
                    && input.dropped_messages >= 1
                    && input.delayed_messages >= 1
                    && input.recovery_roots >= 6
                    && input.recovery_rejections >= 1
                    && input.provider_degraded >= 1
                    && input.health_demote >= 1
                    && input.rebalance_moves >= 1
                    && input.rebalance_policy_proofs >= input.rebalance_moves
                    && input.failure_drill_passed == 1
                    && input.failure_drill_domains >= 6,
                RequirementStatus::Structural,
                format!(
                    "faults={} dropped={} delayed={} recovery_roots={} recovery_rejections={} degraded_providers={} demotions={} rebalance_moves={} rebalance_policy_proofs={} failure_drill_passed={} failure_drill_domains={}",
                    input.fault_events,
                    input.dropped_messages,
                    input.delayed_messages,
                    input.recovery_roots,
                    input.recovery_rejections,
                    input.provider_degraded,
                    input.health_demote,
                    input.rebalance_moves,
                    input.rebalance_policy_proofs,
                    input.failure_drill_passed,
                    input.failure_drill_domains
                ),
            ),
            check(
                "recovery-backup-roots",
                "Recovery and backup roots",
                input.backup_items >= 6
                    && input.restore_executed_items >= input.backup_items
                    && input.restore_executed_surfaces >= 6
                    && input.recovery_roots >= 6,
                RequirementStatus::Structural,
                format!(
                    "backup_items={} restore_executed_items={} restore_executed_surfaces={} recovery_roots={}",
                    input.backup_items,
                    input.restore_executed_items,
                    input.restore_executed_surfaces,
                    input.recovery_roots
                ),
            ),
            check(
                "deterministic-testability",
                "Deterministic replay testability",
                input.invariant_failed == 0
                    && input.observability_metrics >= 300
                    && input.testability_checks >= 5
                    && input.testability_passed == input.testability_checks
                    && input.testability_trace_events >= 600
                    && input.testability_trace_services >= 60
                    && input.testability_replay_ready == 1,
                RequirementStatus::Structural,
                format!(
                    "invariant_failed={} observability_metrics={} testability_checks={} testability_passed={} trace_events={} trace_services={} replay_ready={}",
                    input.invariant_failed,
                    input.observability_metrics,
                    input.testability_checks,
                    input.testability_passed,
                    input.testability_trace_events,
                    input.testability_trace_services,
                    input.testability_replay_ready
                ),
            ),
            check(
                "low-latency-production-shape",
                "Low-latency production shape",
                input.cache_hits >= 1
                    && input.stream_read_lanes >= 2
                    && input.provider_selected >= 1
                    && input.regions >= 1
                    && input.workload_classes >= 4
                    && input.workload_budget_paths >= 4
                    && input.workload_benchmark_claim == 0,
                RequirementStatus::Structural,
                format!(
                    "cache_hits={} read_lanes={} provider_selected={} regions={} workload_classes={} workload_budget_paths={} benchmark_claim=false",
                    input.cache_hits,
                    input.stream_read_lanes,
                    input.provider_selected,
                    input.regions,
                    input.workload_classes,
                    input.workload_budget_paths
                ),
            ),
            check(
                "explicit-mode-boundaries",
                "Explicit consistency boundaries",
                input.auth_grants >= 1
                    && input.quota_policies >= 1
                    && input.rpc_calls >= 1
                    && input.idempotency_replays >= 1
                    && input.consistency_strict >= 3
                    && input.consistency_availability_first >= 1
                    && input.consistency_rejections >= 1,
                RequirementStatus::Structural,
                format!(
                    "auth_grants={} quota_policies={} rpc_calls={} idempotency_replays={} strict_modes={} availability_first_modes={} consistency_rejections={}",
                    input.auth_grants,
                    input.quota_policies,
                    input.rpc_calls,
                    input.idempotency_replays,
                    input.consistency_strict,
                    input.consistency_availability_first,
                    input.consistency_rejections
                ),
            ),
        ];
        RequirementReport {
            implementation: input.implementation.clone(),
            checks,
        }
    }
}

fn check(
    id: &'static str,
    title: &'static str,
    condition: bool,
    covered_status: RequirementStatus,
    detail: String,
) -> RequirementCheck {
    RequirementCheck {
        id,
        title,
        status: if condition {
            covered_status
        } else {
            RequirementStatus::Missing
        },
        detail,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn complete_production_state_covers_all_requirement_families() {
        let report = RequirementAuditor::evaluate(&complete_input());
        assert_eq!(report.total(), 15);
        assert_eq!(report.covered(), 15);
        assert_eq!(report.structural(), 15);
        assert_eq!(report.missing(), 0);
    }

    #[test]
    fn missing_sql_pipeline_is_not_covered() {
        let mut input = complete_input();
        input.sql_pipeline_rows = 0;
        input.sql_pipeline_rejections = 0;
        let report = RequirementAuditor::evaluate(&input);
        assert_eq!(report.missing(), 1);
        assert_eq!(
            report
                .checks
                .iter()
                .find(|check| check.id == "sql-query-execution")
                .unwrap()
                .status,
            RequirementStatus::Missing
        );
    }

    #[test]
    fn performance_requirement_is_structural_shape_not_a_benchmark_claim() {
        let report = RequirementAuditor::evaluate(&complete_input());
        let check = report
            .checks
            .iter()
            .find(|check| check.id == "low-latency-production-shape")
            .unwrap();
        assert_eq!(check.status, RequirementStatus::Structural);
        assert!(check.detail.contains("benchmark_claim=false"));
    }

    fn complete_input() -> RequirementInput {
        RequirementInput {
            implementation: "universal-vsr-rados".to_string(),
            nodes: 5,
            regions: 2,
            deployment_roles: 18,
            deployment_endpoints: 8,
            deployment_volumes: 5,
            deployment_probes: 8,
            admitted_nodes: 5,
            membership_epoch: 3,
            membership_promotions: 1,
            membership_intersections: 1,
            membership_rejections: 1,
            vsr_commits: 2,
            vsr_view_changes: 1,
            vsr_rejections: 1,
            tenant_surfaces: 4,
            catalog_items: 7,
            auth_grants: 6,
            wire_protocols: 5,
            gateway_surfaces: 4,
            consistency_strict: 3,
            consistency_availability_first: 1,
            consistency_rejections: 1,
            quota_policies: 4,
            rpc_calls: 6,
            idempotency_replays: 1,
            schema_steps: 4,
            migration_begun: 2,
            migration_backfilled_rows: 1_000,
            migration_indexed_keys: 1_000,
            migration_completed: 1,
            migration_rolled_back: 1,
            migration_rejected: 1,
            jobs_enqueued: 5,
            session_bound: 3,
            primary_inserts: 1,
            primary_lookups: 2,
            primary_updates: 1,
            primary_deletes: 1,
            primary_rejections: 1,
            index_mutations: 5,
            sql_pipeline_rows: 1,
            sql_pipeline_rejections: 1,
            sql_dml_bound: 4,
            sql_dml_committed: 3,
            sql_dml_kv_applied: 3,
            sql_dml_session_staged: 1,
            sql_dml_overlay_verified: 2,
            sql_dml_rejections: 1,
            deferred_transactions: 2,
            deferred_obligations: 2,
            deferred_txn_references: 1,
            deferred_commits: 1,
            deferred_rejections: 1,
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
            query_rows: 1,
            planner_physical_plans: 4,
            executor_stages: 2,
            page_frames: 1,
            page_redo: 2,
            wal_flush_allows: 1,
            wal_rejections: 1,
            buffer_hits: 1,
            buffer_flushes: 1,
            kv_keys: 8,
            range_partitions: 2,
            range_non_overlaps: 1,
            range_rejections: 1,
            sstable_manifests: 3,
            compaction_inputs: 2,
            sessiontxn_overlay_reads: 14,
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
            watch_deliveries: 2,
            object_versions: 1,
            objectmeta_buckets: 1,
            objectmeta_heads: 1,
            objectmeta_rejections: 1,
            blob_chunks: 1,
            manifest_entries: 6,
            manifest_surfaces: 5,
            chunkstore_chunks: 5,
            chunkstore_verified: 3,
            chunkstore_deduped: 1,
            checkpoint_roots: 7,
            superblock_roots: 6,
            snapshot_roots: 6,
            backup_items: 6,
            restore_executed_items: 6,
            restore_executed_surfaces: 6,
            recovery_roots: 6,
            recovery_rejections: 1,
            repair_actions: 8,
            retention_actions: 2,
            provider_selected: 4,
            provider_degraded: 1,
            stream_read_lanes: 3,
            stream_verified_parts: 3,
            erasure_tolerated_loss: 2,
            cache_hits: 3,
            workload_classes: 4,
            workload_budget_paths: 4,
            workload_benchmark_claim: 0,
            rebalance_moves: 1,
            rebalance_policy_proofs: 1,
            health_demote: 2,
            fault_events: 5,
            dropped_messages: 1,
            delayed_messages: 1,
            failure_drill_passed: 1,
            failure_drill_domains: 6,
            protocol_commits: 34,
            committed_transactions: 3,
            storage_committed_prefixes: 3,
            storage_manifest_covered: 6,
            testability_checks: 5,
            testability_passed: 5,
            testability_trace_events: 700,
            testability_trace_services: 70,
            testability_replay_ready: 1,
            invariant_failed: 0,
            observability_metrics: 301,
        }
    }
}
