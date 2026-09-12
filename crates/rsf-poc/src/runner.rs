use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use rsf_core::{ErrorCode, PlacementGroupId, RsfError, RsfResult};
use rsf_storage::SegmentLog;

use crate::model::{ImplementationId, ProductionRunState};
use crate::scenario::implementations;
use crate::services::{
    Admission, Api, Auth, Backup, BlobStore, Bootstrap, BufferPoolService, Cache, Catalog,
    CheckpointService, ChunkStoreService, Compaction, Consensus, DatabaseRecovery, DatabaseRuntime,
    DeferredConstraints, Deployment, Fabric, FailureDrill, Faults, Gateway, Health, Idempotency,
    Index, Invariants, Isolation, Jobs, Lease, ManifestService, MaterializationService, Membership,
    MigrationExecution, ObjectLayout, ObjectMetadataService, ObjectStore, Observability, OrderedKv,
    PageService, PhysicalQueryExecutor, PhysicalQueryPlanner, Placement, PreparedSqlService,
    PrimaryIndexService, ProviderDiscovery, Query, Quota, RangeLayoutService, Rebalance,
    Relational, RelationalCommitService, RelationalConstraints, RelationalWrite, Repair,
    Requirements, Retention, RowCodecService, Rpc, Schema, Session, SessionTransactionService,
    Snapshot, SqlDmlService, SqlPipelineService, SqlPortalService, SqlTransactionService,
    Statistics, StorageRecovery, Stream, SuperblockService, Tenancy, Testability, Transaction,
    TransactionCoordinatorService, TupleVisibilityService, Upgrade, VacuumService, VsrService,
    WalService, Watch, Wire, WorkloadShape,
};
use crate::trace::Trace;

static RUN_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunOutput {
    pub implementation: ImplementationId,
    pub trace: Trace,
    pub state: ProductionRunState,
}

impl RunOutput {
    pub fn write_logs(&self, root: impl AsRef<Path>) -> RsfResult<RunArtifacts> {
        let root = root.as_ref();
        fs::create_dir_all(root).map_err(storage_error)?;

        let full_log = root.join("full.log");
        fs::write(&full_log, self.trace.render()).map_err(storage_error)?;

        let summary = root.join("summary.txt");
        fs::write(
            &summary,
            format!(
                "implementation={}\ndeployment_roles={}\ndeployment_endpoints={}\ndeployment_volumes={}\ndeployment_probes={}\nupgrade_steps={}\nupgrade_checks={}\nupgrade_nodes={}\nupgrade_rejections={}\nnodes={}\nmembership_epoch={}\nmembership_voters={}\nmembership_learners={}\nmembership_promotions={}\nmembership_intersections={}\nmembership_rejections={}\nvsr_view={}\nvsr_prepares={}\nvsr_acks={}\nvsr_commits={}\nvsr_view_changes={}\nvsr_rejections={}\nplacement_groups={}\ntenants={}\ntenant_databases={}\ntenant_namespaces={}\ntenant_surfaces={}\ncatalog_items={}\nauth_grants={}\nauthz_allowed={}\nauthz_denied={}\nwire_frames={}\nwire_protocols={}\nwire_rejections={}\nwire_body_bytes={}\nprovider_records={}\nprovider_selected={}\nprovider_degraded={}\nprovider_regions={}\nrpc_calls={}\nrpc_retries={}\nrpc_rejections={}\nrpc_body_bytes={}\napi_accepted={}\napi_rejected={}\nidempotency_first_seen={}\nidempotency_replays={}\nidempotency_conflicts={}\nidempotency_retries={}\ngateway_routes={}\ngateway_rejections={}\ngateway_surfaces={}\nconsistency_strict={}\nconsistency_availability_first={}\nconsistency_rejections={}\nquota_allowed={}\nquota_rejected={}\nquota_policies={}\nquota_namespaces={}\ncache_hits={}\ncache_misses={}\ncache_evictions={}\ncache_hints={}\nworkload_classes={}\nworkload_budget_paths={}\nworkload_benchmark_claim={}\nbackup_items={}\nrestore_checks={}\nrestore_executed_items={}\nrestore_executed_surfaces={}\nrestore_executed_bytes={}\nrecovery_roots={}\nrecovery_replays={}\nrecovery_rejections={}\nrecovery_actions={}\nlease_grants={}\nfence_rejections={}\nrebalance_moves={}\nrebalance_checks={}\nrebalance_policy_proofs={}\nfault_events={}\ndropped_messages={}\ndelayed_messages={}\nfailure_drill_passed={}\nfailure_drill_domains={}\nhealth_healthy={}\nhealth_degraded={}\nhealth_demote={}\nhealth_actions={}\nschema_steps={}\nschema_backfills={}\nschema_rejected={}\njobs_enqueued={}\njobs_leased={}\njobs_completed={}\njobs_retried={}\njobs_blocked={}\nsession_bound={}\nsession_authorized={}\nsession_rejected={}\nsessiontxn_sessions={}\nsessiontxn_pending_writes={}\nsessiontxn_overlay_reads={}\nsessiontxn_snapshot_reads={}\nsessiontxn_commits={}\nsessiontxn_rollbacks={}\nsessiontxn_savepoints={}\nsessiontxn_savepoint_rewinds={}\nsessiontxn_rejections={}\nindex_mutations={}\nindex_entries={}\nindex_rebuilds={}\nplanner_physical_plans={}\nplanner_index_choices={}\nplanner_range_scans={}\nplanner_stream_reads={}\nexecutor_plans={}\nexecutor_stages={}\nexecutor_rows={}\nexecutor_remote_reads={}\nsql_pipeline_bound={}\nsql_pipeline_authorized={}\nsql_pipeline_logical={}\nsql_pipeline_physical={}\nsql_pipeline_stages={}\nsql_pipeline_rows={}\nsql_pipeline_remote_reads={}\nsql_pipeline_rejections={}\nrange_partitions={}\nrange_non_overlaps={}\nrange_rejections={}\nsstable_manifests={}\ncompaction_inputs={}\ncompaction_outputs={}\ncompaction_live_keys={}\ncompaction_tombstones={}\ncompaction_reclaimed_bytes={}\nstats_tables={}\nstats_rows={}\nstats_histogram_buckets={}\nstats_stale={}\nconstraints_checked={}\nconstraints_accepted={}\nconstraints_rejected={}\nconstraints_unique_keys={}\ndeferred_transactions={}\ndeferred_obligations={}\ndeferred_txn_references={}\ndeferred_committed_references={}\ndeferred_commits={}\ndeferred_rejections={}\nwrite_plans={}\nwrite_steps={}\nwrite_index_mutations={}\nwrite_conflict_ranges={}\nrelcommit_groups={}\nrelcommit_row_mutations={}\nrelcommit_index_mutations={}\nrelcommit_conflict_ranges={}\nrelcommit_rejections={}\npage_frames={}\npage_redo={}\npage_reads={}\npage_rejections={}\npage_checkpoint_bytes={}\nwal_appends={}\nwal_syncs={}\nwal_flush_allows={}\nwal_rejections={}\nwal_durable_lsn={}\nwal_bytes={}\nbuffer_resident={}\nbuffer_hits={}\nbuffer_dirty={}\nbuffer_flushes={}\nbuffer_evictions={}\nbuffer_rejections={}\ntuple_versions={}\ntuple_visible_reads={}\ntuple_delete_markers={}\ntuple_rejections={}\ntuple_bytes={}\nvacuum_chains={}\nvacuum_pruned_versions={}\nvacuum_retained_versions={}\nvacuum_rejections={}\nvacuum_reclaimed_bytes={}\nmaterialized_rows={}\nmaterialized_projected={}\nmaterialized_columns={}\nmaterialized_rejections={}\nmaterialized_bytes={}\ncoordinator_transactions={}\nisolation_commits={}\nisolation_rejections={}\nisolation_edges={}\nisolation_ranges={}\nquery_rows={}\nkv_keys={}\nkv_changes={}\nwatch_registered={}\nwatch_deliveries={}\nwatch_resumes={}\nrelational_rows={}\nobject_versions={}\nobjectmeta_buckets={}\nobjectmeta_uploads={}\nobjectmeta_parts={}\nobjectmeta_heads={}\nobjectmeta_rejections={}\nblob_chunks={}\nprovider_content={}\nstream_write_chunks={}\nstream_read_lanes={}\nstream_window_rejections={}\nstream_verified_parts={}\nerasure_data_shards={}\nerasure_parity_shards={}\nerasure_tolerated_loss={}\nerasure_reconstructed_bytes={}\nmanifest_entries={}\nmanifest_surfaces={}\nmanifest_bytes={}\nmanifest_rejections={}\nchunkstore_chunks={}\nchunkstore_pinned={}\nchunkstore_bytes={}\nchunkstore_verified={}\nchunkstore_deduped={}\nchunkstore_rejections={}\ncheckpoint_roots={}\ncheckpoint_segments={}\ncheckpoint_compacted={}\ncheckpoint_reclaimable_bytes={}\ncheckpoint_rejections={}\nsuperblock_generation={}\nsuperblock_checkpoint={}\nsuperblock_roots={}\nsuperblock_advances={}\nsuperblock_rejections={}\nsnapshot_roots={}\nsnapshot_barriers={}\nsnapshot_validations={}\nprotocol_commits={}\ncommitted_transactions={}\nrepair_findings={}\nrepair_actions={}\nretention_kept={}\nretention_actions={}\nrequirement_checks={}\nrequirement_covered={}\ninvariant_checks={}\ninvariant_passed={}\ninvariant_failed={}\nobservability_metrics={}\nobservability_audits={}\ncluster_health={}\n",
                self.implementation,
                self.state.deployment_roles,
                self.state.deployment_endpoints,
                self.state.deployment_volumes,
                self.state.deployment_probes,
                self.state.upgrade_steps,
                self.state.upgrade_checks,
                self.state.upgrade_nodes,
                self.state.upgrade_rejections,
                self.state.admitted_nodes,
                self.state.membership_epoch,
                self.state.membership_voters,
                self.state.membership_learners,
                self.state.membership_promotions,
                self.state.membership_intersections,
                self.state.membership_rejections,
                self.state.vsr_view,
                self.state.vsr_prepares,
                self.state.vsr_acks,
                self.state.vsr_commits,
                self.state.vsr_view_changes,
                self.state.vsr_rejections,
                self.state.placement_groups,
                self.state.tenants,
                self.state.tenant_databases,
                self.state.tenant_namespaces,
                self.state.tenant_surfaces,
                self.state.catalog_items,
                self.state.auth_grants,
                self.state.authz_allowed,
                self.state.authz_denied,
                self.state.wire_frames,
                self.state.wire_protocols,
                self.state.wire_rejections,
                self.state.wire_body_bytes,
                self.state.provider_records,
                self.state.provider_selected,
                self.state.provider_degraded,
                self.state.provider_regions,
                self.state.rpc_calls,
                self.state.rpc_retries,
                self.state.rpc_rejections,
                self.state.rpc_body_bytes,
                self.state.api_accepted,
                self.state.api_rejected,
                self.state.idempotency_first_seen,
                self.state.idempotency_replays,
                self.state.idempotency_conflicts,
                self.state.idempotency_retries,
                self.state.gateway_routes,
                self.state.gateway_rejections,
                self.state.gateway_surfaces,
                self.state.consistency_strict,
                self.state.consistency_availability_first,
                self.state.consistency_rejections,
                self.state.quota_allowed,
                self.state.quota_rejected,
                self.state.quota_policies,
                self.state.quota_namespaces,
                self.state.cache_hits,
                self.state.cache_misses,
                self.state.cache_evictions,
                self.state.cache_hints,
                self.state.workload_classes,
                self.state.workload_budget_paths,
                self.state.workload_benchmark_claim,
                self.state.backup_items,
                self.state.restore_checks,
                self.state.restore_executed_items,
                self.state.restore_executed_surfaces,
                self.state.restore_executed_bytes,
                self.state.recovery_roots,
                self.state.recovery_replays,
                self.state.recovery_rejections,
                self.state.recovery_actions,
                self.state.lease_grants,
                self.state.fence_rejections,
                self.state.rebalance_moves,
                self.state.rebalance_checks,
                self.state.rebalance_policy_proofs,
                self.state.fault_events,
                self.state.dropped_messages,
                self.state.delayed_messages,
                self.state.failure_drill_passed,
                self.state.failure_drill_domains,
                self.state.health_healthy,
                self.state.health_degraded,
                self.state.health_demote,
                self.state.health_actions,
                self.state.schema_steps,
                self.state.schema_backfills,
                self.state.schema_rejected,
                self.state.jobs_enqueued,
                self.state.jobs_leased,
                self.state.jobs_completed,
                self.state.jobs_retried,
                self.state.jobs_blocked,
                self.state.session_bound,
                self.state.session_authorized,
                self.state.session_rejected,
                self.state.sessiontxn_sessions,
                self.state.sessiontxn_pending_writes,
                self.state.sessiontxn_overlay_reads,
                self.state.sessiontxn_snapshot_reads,
                self.state.sessiontxn_commits,
                self.state.sessiontxn_rollbacks,
                self.state.sessiontxn_savepoints,
                self.state.sessiontxn_savepoint_rewinds,
                self.state.sessiontxn_rejections,
                self.state.index_mutations,
                self.state.index_entries,
                self.state.index_rebuilds,
                self.state.planner_physical_plans,
                self.state.planner_index_choices,
                self.state.planner_range_scans,
                self.state.planner_stream_reads,
                self.state.executor_plans,
                self.state.executor_stages,
                self.state.executor_rows,
                self.state.executor_remote_reads,
                self.state.sql_pipeline_bound,
                self.state.sql_pipeline_authorized,
                self.state.sql_pipeline_logical,
                self.state.sql_pipeline_physical,
                self.state.sql_pipeline_stages,
                self.state.sql_pipeline_rows,
                self.state.sql_pipeline_remote_reads,
                self.state.sql_pipeline_rejections,
                self.state.range_partitions,
                self.state.range_non_overlaps,
                self.state.range_rejections,
                self.state.sstable_manifests,
                self.state.compaction_inputs,
                self.state.compaction_outputs,
                self.state.compaction_live_keys,
                self.state.compaction_tombstones,
                self.state.compaction_reclaimed_bytes,
                self.state.stats_tables,
                self.state.stats_rows,
                self.state.stats_histogram_buckets,
                self.state.stats_stale,
                self.state.constraints_checked,
                self.state.constraints_accepted,
                self.state.constraints_rejected,
                self.state.constraints_unique_keys,
                self.state.deferred_transactions,
                self.state.deferred_obligations,
                self.state.deferred_txn_references,
                self.state.deferred_committed_references,
                self.state.deferred_commits,
                self.state.deferred_rejections,
                self.state.write_plans,
                self.state.write_steps,
                self.state.write_index_mutations,
                self.state.write_conflict_ranges,
                self.state.relcommit_groups,
                self.state.relcommit_row_mutations,
                self.state.relcommit_index_mutations,
                self.state.relcommit_conflict_ranges,
                self.state.relcommit_rejections,
                self.state.page_frames,
                self.state.page_redo,
                self.state.page_reads,
                self.state.page_rejections,
                self.state.page_checkpoint_bytes,
                self.state.wal_appends,
                self.state.wal_syncs,
                self.state.wal_flush_allows,
                self.state.wal_rejections,
                self.state.wal_durable_lsn,
                self.state.wal_bytes,
                self.state.buffer_resident,
                self.state.buffer_hits,
                self.state.buffer_dirty,
                self.state.buffer_flushes,
                self.state.buffer_evictions,
                self.state.buffer_rejections,
                self.state.tuple_versions,
                self.state.tuple_visible_reads,
                self.state.tuple_delete_markers,
                self.state.tuple_rejections,
                self.state.tuple_bytes,
                self.state.vacuum_chains,
                self.state.vacuum_pruned_versions,
                self.state.vacuum_retained_versions,
                self.state.vacuum_rejections,
                self.state.vacuum_reclaimed_bytes,
                self.state.materialized_rows,
                self.state.materialized_projected,
                self.state.materialized_columns,
                self.state.materialized_rejections,
                self.state.materialized_bytes,
                self.state.coordinator_transactions,
                self.state.isolation_commits,
                self.state.isolation_rejections,
                self.state.isolation_edges,
                self.state.isolation_ranges,
                self.state.query_rows,
                self.state.kv_keys,
                self.state.kv_changes,
                self.state.watch_registered,
                self.state.watch_deliveries,
                self.state.watch_resumes,
                self.state.relational_rows,
                self.state.object_versions,
                self.state.objectmeta_buckets,
                self.state.objectmeta_uploads,
                self.state.objectmeta_parts,
                self.state.objectmeta_heads,
                self.state.objectmeta_rejections,
                self.state.blob_chunks,
                self.state.provider_content,
                self.state.stream_write_chunks,
                self.state.stream_read_lanes,
                self.state.stream_window_rejections,
                self.state.stream_verified_parts,
                self.state.erasure_data_shards,
                self.state.erasure_parity_shards,
                self.state.erasure_tolerated_loss,
                self.state.erasure_reconstructed_bytes,
                self.state.manifest_entries,
                self.state.manifest_surfaces,
                self.state.manifest_bytes,
                self.state.manifest_rejections,
                self.state.chunkstore_chunks,
                self.state.chunkstore_pinned,
                self.state.chunkstore_bytes,
                self.state.chunkstore_verified,
                self.state.chunkstore_deduped,
                self.state.chunkstore_rejections,
                self.state.checkpoint_roots,
                self.state.checkpoint_segments,
                self.state.checkpoint_compacted,
                self.state.checkpoint_reclaimable_bytes,
                self.state.checkpoint_rejections,
                self.state.superblock_generation,
                self.state.superblock_checkpoint,
                self.state.superblock_roots,
                self.state.superblock_advances,
                self.state.superblock_rejections,
                self.state.snapshot_roots,
                self.state.snapshot_barriers,
                self.state.snapshot_validations,
                self.state.protocol_commits,
                self.state.committed_transactions,
                self.state.repair_findings,
                self.state.repair_actions,
                self.state.retention_kept,
                self.state.retention_actions,
                self.state.requirement_checks,
                self.state.requirement_covered,
                self.state.invariant_checks,
                self.state.invariant_passed,
                self.state.invariant_failed,
                self.state.observability_metrics,
                self.state.observability_audits,
                self.state.cluster_health
            ),
        )
        .map_err(storage_error)?;
        fs::OpenOptions::new()
            .append(true)
            .open(&summary)
            .and_then(|mut file| {
                writeln!(
                    file,
                    "migration_begun={}\nmigration_validated={}\nmigration_published={}\nmigration_backfilled_rows={}\nmigration_indexed_keys={}\nmigration_resumed={}\nmigration_completed={}\nmigration_rolled_back={}\nmigration_rejected={}\nprepared_statements={}\nprepared_binds={}\nprepared_invalidations={}\nprepared_deallocations={}\nprepared_evictions={}\nprepared_rejections={}\nportal_parses={}\nportal_binds={}\nportal_describes={}\nportal_executes={}\nportal_rows={}\nportal_cancellations={}\nportal_closes={}\nportal_statement_closes={}\nportal_rejections={}\nrowcodec_encoded={}\nrowcodec_decoded={}\nrowcodec_defaults={}\nrowcodec_generated={}\nrowcodec_rejections={}\nrowcodec_bytes={}\ntuple_versions={}\ntuple_visible_reads={}\ntuple_delete_markers={}\ntuple_rejections={}\ntuple_bytes={}\nvacuum_chains={}\nvacuum_pruned_versions={}\nvacuum_retained_versions={}\nvacuum_rejections={}\nvacuum_reclaimed_bytes={}\nmaterialized_rows={}\nmaterialized_projected={}\nmaterialized_columns={}\nmaterialized_rejections={}\nmaterialized_bytes={}\nrelcommit_groups={}\nrelcommit_row_mutations={}\nrelcommit_index_mutations={}\nrelcommit_conflict_ranges={}\nrelcommit_rejections={}\nprimary_inserts={}\nprimary_lookups={}\nprimary_updates={}\nprimary_deletes={}\nprimary_rejections={}\nprimary_entries={}\nsql_dml_bound={}\nsql_dml_authorized={}\nsql_dml_planned={}\nsql_dml_prepared={}\nsql_dml_committed={}\nsql_dml_isolated={}\nsql_dml_kv_applied={}\nsql_dml_session_staged={}\nsql_dml_overlay_verified={}\nsql_dml_rejections={}\nsql_txn_began={}\nsql_txn_dml_staged={}\nsql_txn_overlay_reads={}\nsql_txn_commits={}\nsql_txn_rollbacks={}\nsql_txn_rollback_absent={}\nsql_txn_savepoints={}\nsql_txn_savepoint_rewinds={}\nsql_txn_savepoint_absent={}\nsql_txn_rejections={}\nstorage_committed_prefixes={}\nstorage_manifest_covered={}\ntestability_checks={}\ntestability_passed={}\ntestability_trace_events={}\ntestability_trace_services={}\ntestability_replay_ready={}",
                    self.state.migration_begun,
                    self.state.migration_validated,
                    self.state.migration_published,
                    self.state.migration_backfilled_rows,
                    self.state.migration_indexed_keys,
                    self.state.migration_resumed,
                    self.state.migration_completed,
                    self.state.migration_rolled_back,
                    self.state.migration_rejected,
                    self.state.prepared_statements,
                    self.state.prepared_binds,
                    self.state.prepared_invalidations,
                    self.state.prepared_deallocations,
                    self.state.prepared_evictions,
                    self.state.prepared_rejections,
                    self.state.portal_parses,
                    self.state.portal_binds,
                    self.state.portal_describes,
                    self.state.portal_executes,
                    self.state.portal_rows,
                    self.state.portal_cancellations,
                    self.state.portal_closes,
                    self.state.portal_statement_closes,
                    self.state.portal_rejections,
                    self.state.rowcodec_encoded,
                    self.state.rowcodec_decoded,
                    self.state.rowcodec_defaults,
                    self.state.rowcodec_generated,
                    self.state.rowcodec_rejections,
                    self.state.rowcodec_bytes,
                    self.state.tuple_versions,
                    self.state.tuple_visible_reads,
                    self.state.tuple_delete_markers,
                    self.state.tuple_rejections,
                    self.state.tuple_bytes,
                    self.state.vacuum_chains,
                    self.state.vacuum_pruned_versions,
                    self.state.vacuum_retained_versions,
                    self.state.vacuum_rejections,
                    self.state.vacuum_reclaimed_bytes,
                    self.state.materialized_rows,
                    self.state.materialized_projected,
                    self.state.materialized_columns,
                    self.state.materialized_rejections,
                    self.state.materialized_bytes,
                    self.state.relcommit_groups,
                    self.state.relcommit_row_mutations,
                    self.state.relcommit_index_mutations,
                    self.state.relcommit_conflict_ranges,
                    self.state.relcommit_rejections,
                    self.state.primary_inserts,
                    self.state.primary_lookups,
                    self.state.primary_updates,
                    self.state.primary_deletes,
                    self.state.primary_rejections,
                    self.state.primary_entries,
                    self.state.sql_dml_bound,
                    self.state.sql_dml_authorized,
                    self.state.sql_dml_planned,
                    self.state.sql_dml_prepared,
                    self.state.sql_dml_committed,
                    self.state.sql_dml_isolated,
                    self.state.sql_dml_kv_applied,
                    self.state.sql_dml_session_staged,
                    self.state.sql_dml_overlay_verified,
                    self.state.sql_dml_rejections,
                    self.state.sql_txn_began,
                    self.state.sql_txn_dml_staged,
                    self.state.sql_txn_overlay_reads,
                    self.state.sql_txn_commits,
                    self.state.sql_txn_rollbacks,
                    self.state.sql_txn_rollback_absent,
                    self.state.sql_txn_savepoints,
                    self.state.sql_txn_savepoint_rewinds,
                    self.state.sql_txn_savepoint_absent,
                    self.state.sql_txn_rejections,
                    self.state.storage_committed_prefixes,
                    self.state.storage_manifest_covered,
                    self.state.testability_checks,
                    self.state.testability_passed,
                    self.state.testability_trace_events,
                    self.state.testability_trace_services,
                    self.state.testability_replay_ready
                )
            })
            .map_err(storage_error)?;

        let services_dir = root.join("services");
        fs::create_dir_all(&services_dir).map_err(storage_error)?;
        let service_logs = self
            .trace
            .services()
            .into_iter()
            .map(|service| {
                let path = services_dir.join(format!("{service}.log"));
                fs::write(&path, self.trace.render_service(service)).map_err(storage_error)?;
                Ok(path)
            })
            .collect::<RsfResult<Vec<_>>>()?;

        Ok(RunArtifacts {
            root: root.to_path_buf(),
            full_log,
            summary,
            service_logs,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunArtifacts {
    pub root: PathBuf,
    pub full_log: PathBuf,
    pub summary: PathBuf,
    pub service_logs: Vec<PathBuf>,
}

pub fn run_one(id: ImplementationId) -> RsfResult<RunOutput> {
    run_one_with_trace(id, Trace::new())
}

pub fn run_one_streaming_stdout(id: ImplementationId) -> RsfResult<RunOutput> {
    run_one_with_trace(id, Trace::streaming_stdout())
}

pub fn run_all_streaming_stdout() -> RsfResult<Vec<RunOutput>> {
    implementations()
        .into_iter()
        .map(|implementation| run_one_streaming_stdout(implementation.id))
        .collect()
}

fn run_one_with_trace(id: ImplementationId, mut trace: Trace) -> RsfResult<RunOutput> {
    let implementation = implementations()
        .into_iter()
        .find(|implementation| implementation.id == id)
        .ok_or_else(|| {
            RsfError::new(
                ErrorCode::InvalidInput,
                format!("unknown implementation {id}"),
            )
        })?;

    trace.push(
        "poc",
        "simulation",
        "all services are deterministic simulations with real module boundaries",
    );

    Bootstrap::run(&mut trace, &implementation);
    let mut runtime = DatabaseRuntime::new([
        PlacementGroupId(1),
        PlacementGroupId(2),
        PlacementGroupId(3),
    ]);
    Deployment::run(&mut trace, &implementation, &mut runtime);
    Upgrade::run(&mut trace, &implementation, &mut runtime);
    Admission::run(&mut trace, &implementation, &mut runtime);
    Membership::run(&mut trace, &mut runtime);
    let groups = Placement::run(&mut trace, &mut runtime);
    Faults::run(&mut trace, &mut runtime);
    Lease::run(&mut trace, &mut runtime);
    VsrService::run(&mut trace, &mut runtime);
    Health::run(&mut trace, &mut runtime);
    let nodes = runtime.admin.cluster_map().nodes.len();
    Tenancy::run(&mut trace, &mut runtime);
    Catalog::run(&mut trace, &mut runtime);
    Schema::run(&mut trace, &mut runtime);
    MigrationExecution::run(&mut trace, &mut runtime);
    Jobs::run(&mut trace, &mut runtime);
    Auth::run(&mut trace, &mut runtime);
    Wire::run(&mut trace, &mut runtime);
    Api::run(&mut trace, &mut runtime);
    Idempotency::run(&mut trace, &mut runtime);
    Gateway::run(&mut trace, &mut runtime);
    Quota::run(&mut trace, &mut runtime);
    Rpc::run(&mut trace, &mut runtime);

    let path = segment_path(id);
    let mut log = SegmentLog::open(&path)?;
    let recovery = Fabric::append_demo_records(&mut trace, &mut log);
    drop(log);
    let _ = fs::remove_file(&path);

    Consensus::run(&mut trace, &implementation, &recovery);
    Transaction::run(&mut trace, &recovery);
    TransactionCoordinatorService::run(&mut trace, &mut runtime);
    Isolation::run(&mut trace, &mut runtime);
    OrderedKv::run(&mut trace, &implementation, &mut runtime);
    Watch::run(&mut trace, &mut runtime);
    SessionTransactionService::run(&mut trace, &mut runtime);
    RangeLayoutService::run(&mut trace, &mut runtime);
    Compaction::run(&mut trace, &mut runtime);
    Statistics::run(&mut trace, &mut runtime);
    RelationalConstraints::run(&mut trace, &mut runtime);
    DeferredConstraints::run(&mut trace, &mut runtime);
    RowCodecService::run(&mut trace, &mut runtime);
    TupleVisibilityService::run(&mut trace, &mut runtime);
    VacuumService::run(&mut trace, &mut runtime);
    MaterializationService::run(&mut trace, &mut runtime);
    RelationalWrite::run(&mut trace, &mut runtime);
    RelationalCommitService::run(&mut trace, &mut runtime);
    PageService::run(&mut trace, &mut runtime);
    WalService::run(&mut trace, &mut runtime);
    BufferPoolService::run(&mut trace, &mut runtime);
    Session::run(&mut trace, &mut runtime);
    PreparedSqlService::run(&mut trace, &mut runtime);
    SqlPortalService::run(&mut trace, &mut runtime);
    PrimaryIndexService::run(&mut trace, &mut runtime);
    Index::run(&mut trace, &mut runtime);
    Relational::run(&mut trace, &implementation, &mut runtime);
    Query::run(&mut trace, &mut runtime);
    PhysicalQueryPlanner::run(&mut trace, &mut runtime);
    PhysicalQueryExecutor::run(&mut trace, &mut runtime);
    SqlPipelineService::run(&mut trace, &mut runtime);
    SqlDmlService::run(&mut trace, &mut runtime);
    SqlTransactionService::run(&mut trace, &mut runtime);
    ObjectMetadataService::run(&mut trace, &mut runtime);
    ObjectStore::run(&mut trace, &implementation, &mut runtime);
    BlobStore::run(&mut trace, &implementation, &mut runtime);
    ObjectLayout::run(&mut trace, &mut runtime);
    let manifest = ManifestService::run(&mut trace, &mut runtime);
    StorageRecovery::run(&mut trace, &recovery, &manifest, &mut runtime);
    ChunkStoreService::run(&mut trace, &mut runtime);
    CheckpointService::run(&mut trace, &mut runtime);
    ProviderDiscovery::run(&mut trace, &mut runtime);
    Stream::run(&mut trace, &mut runtime);
    Cache::run(&mut trace, &mut runtime);
    WorkloadShape::run(&mut trace, &mut runtime);
    SuperblockService::run(&mut trace, &mut runtime);
    Snapshot::run(&mut trace, &mut runtime);
    Backup::run(&mut trace, &mut runtime);
    DatabaseRecovery::run(&mut trace, &mut runtime);
    Retention::run(&mut trace, &mut runtime);
    Repair::run(&mut trace, &mut runtime);
    Rebalance::run(&mut trace, &mut runtime);
    FailureDrill::run(&mut trace, &mut runtime);
    let mut state = Observability::finish(
        &mut trace,
        &implementation,
        &recovery,
        &runtime,
        nodes,
        groups.len(),
    );
    Invariants::run(&mut trace, &mut state);
    Testability::run(&mut trace, &mut state);
    Requirements::run(&mut trace, &implementation, &mut state);

    Ok(RunOutput {
        implementation: id,
        trace,
        state,
    })
}

pub fn run_all() -> RsfResult<Vec<RunOutput>> {
    implementations()
        .into_iter()
        .map(|implementation| run_one(implementation.id))
        .collect()
}

fn segment_path(id: ImplementationId) -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must be after UNIX_EPOCH")
        .as_nanos();
    let counter = RUN_COUNTER.fetch_add(1, Ordering::Relaxed);
    let process = std::process::id();
    std::env::temp_dir().join(format!(
        "rsf-poc-{}-{process}-{counter}-{unique}.segment",
        id.slug()
    ))
}

fn storage_error(err: std::io::Error) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_implementation_runs_full_production_database_path() {
        let outputs = run_all().unwrap();
        assert_eq!(outputs.len(), 5);

        for output in outputs {
            let services = output
                .trace
                .events()
                .iter()
                .map(|event| event.service.as_str())
                .collect::<Vec<_>>();

            for required in [
                "bootstrap",
                "deployment",
                "upgrade",
                "admission",
                "membership",
                "placement",
                "fault",
                "lease",
                "vsr",
                "health",
                "tenancy",
                "catalog",
                "schema",
                "migration",
                "jobs",
                "auth",
                "wire",
                "api",
                "idempotency",
                "rpc",
                "consistency",
                "cache",
                "fabric",
                "consensus",
                "transaction",
                "txn",
                "isolation",
                "kv",
                "range",
                "compaction",
                "stats",
                "constraints",
                "deferred",
                "rowcodec",
                "tuple",
                "vacuum",
                "materialize",
                "relcommit",
                "session",
                "sessiontxn",
                "prepared",
                "portal",
                "primary",
                "page",
                "wal",
                "buffer",
                "relational",
                "query",
                "planner",
                "executor",
                "sqlpipeline",
                "object",
                "blob",
                "provider",
                "stream",
                "erasure",
                "manifest",
                "storage",
                "chunkstore",
                "checkpoint",
                "superblock",
                "backup",
                "recovery",
                "retention",
                "repair",
                "rebalance",
                "observability",
                "invariants",
                "testability",
                "requirements",
            ] {
                assert!(
                    services.contains(&required),
                    "missing service {required} for {}",
                    output.implementation
                );
            }

            assert_eq!(output.state.tenants, 1);
            assert!(output.state.deployment_roles >= output.state.admitted_nodes);
            assert_eq!(output.state.deployment_endpoints, 8);
            assert_eq!(output.state.deployment_volumes, output.state.admitted_nodes);
            assert_eq!(output.state.deployment_probes, 8);
            assert_eq!(
                output.state.upgrade_steps,
                1 + usize::from(output.state.admitted_nodes) as u8 * 4
            );
            assert_eq!(output.state.upgrade_checks, 3);
            assert_eq!(output.state.upgrade_nodes, output.state.admitted_nodes);
            assert_eq!(output.state.upgrade_rejections, 1);
            assert_eq!(output.state.membership_epoch, 3);
            assert_eq!(output.state.membership_voters, 3);
            assert_eq!(output.state.membership_learners, 0);
            assert_eq!(output.state.membership_promotions, 1);
            assert_eq!(output.state.membership_intersections, 1);
            assert_eq!(output.state.membership_rejections, 1);
            assert_eq!(output.state.vsr_view, 2);
            assert_eq!(output.state.vsr_prepares, 2);
            assert_eq!(output.state.vsr_acks, 2);
            assert_eq!(output.state.vsr_commits, 2);
            assert_eq!(output.state.vsr_view_changes, 1);
            assert_eq!(output.state.vsr_rejections, 1);
            assert_eq!(output.state.tenant_databases, 1);
            assert_eq!(output.state.tenant_namespaces, 1);
            assert_eq!(output.state.tenant_surfaces, 4);
            assert_eq!(output.state.catalog_items, 7);
            assert_eq!(output.state.auth_grants, 6);
            assert_eq!(output.state.authz_allowed, 1);
            assert_eq!(output.state.authz_denied, 1);
            assert_eq!(output.state.wire_frames, 6);
            assert_eq!(output.state.wire_protocols, 5);
            assert_eq!(output.state.wire_rejections, 1);
            assert_eq!(output.state.wire_body_bytes, 1_152);
            assert_eq!(output.state.provider_records, 6);
            assert_eq!(output.state.provider_selected, 4);
            assert_eq!(output.state.provider_degraded, 1);
            assert_eq!(output.state.provider_regions, 3);
            assert_eq!(output.state.rpc_calls, 6);
            assert_eq!(output.state.rpc_retries, 1);
            assert_eq!(output.state.rpc_rejections, 2);
            assert_eq!(output.state.rpc_body_bytes, 1_280);
            assert_eq!(output.state.api_accepted, 1);
            assert_eq!(output.state.api_rejected, 1);
            assert_eq!(output.state.idempotency_first_seen, 1);
            assert_eq!(output.state.idempotency_replays, 1);
            assert_eq!(output.state.idempotency_conflicts, 1);
            assert_eq!(output.state.idempotency_retries, 1);
            assert_eq!(output.state.gateway_routes, 4);
            assert_eq!(output.state.gateway_rejections, 1);
            assert_eq!(output.state.gateway_surfaces, 4);
            assert_eq!(output.state.consistency_strict, 3);
            assert_eq!(output.state.consistency_availability_first, 1);
            assert_eq!(output.state.consistency_rejections, 1);
            assert_eq!(output.state.quota_allowed, 4);
            assert_eq!(output.state.quota_rejected, 1);
            assert_eq!(output.state.quota_policies, 4);
            assert_eq!(output.state.quota_namespaces, 1);
            assert_eq!(output.state.cache_hits, 3);
            assert_eq!(output.state.cache_misses, 2);
            assert_eq!(output.state.cache_evictions, 2);
            assert_eq!(output.state.cache_hints, 2);
            assert_eq!(output.state.workload_classes, 4);
            assert_eq!(output.state.workload_budget_paths, 4);
            assert_eq!(output.state.workload_benchmark_claim, 0);
            assert_eq!(output.state.backup_items, 7);
            assert_eq!(output.state.restore_checks, 5);
            assert_eq!(output.state.restore_executed_items, 7);
            assert_eq!(output.state.restore_executed_surfaces, 7);
            assert_eq!(output.state.restore_executed_bytes, 4_480);
            assert_eq!(output.state.recovery_roots, 7);
            assert_eq!(output.state.recovery_replays, 4);
            assert_eq!(output.state.recovery_rejections, 1);
            assert_eq!(output.state.recovery_actions, 7);
            assert_eq!(output.state.lease_grants, 4);
            assert_eq!(output.state.fence_rejections, 1);
            assert_eq!(output.state.rebalance_moves, 1);
            assert_eq!(output.state.rebalance_checks, 4);
            assert_eq!(output.state.rebalance_policy_proofs, 1);
            assert_eq!(output.state.fault_events, 5);
            assert_eq!(output.state.dropped_messages, 1);
            assert_eq!(output.state.delayed_messages, 1);
            assert_eq!(output.state.failure_drill_passed, 1);
            assert_eq!(output.state.failure_drill_domains, 6);
            assert_eq!(
                output.state.health_healthy
                    + output.state.health_degraded
                    + output.state.health_demote,
                output.state.admitted_nodes
            );
            assert_eq!(output.state.health_degraded, 0);
            assert_eq!(output.state.health_demote, 2);
            assert_eq!(
                output.state.health_actions,
                output.state.health_healthy + output.state.health_demote
            );
            assert_eq!(output.state.schema_steps, 4);
            assert_eq!(output.state.schema_backfills, 0);
            assert_eq!(output.state.schema_rejected, 1);
            assert_eq!(output.state.migration_begun, 2);
            assert_eq!(output.state.migration_validated, 2);
            assert_eq!(output.state.migration_published, 2);
            assert_eq!(output.state.migration_backfilled_rows, 1_000);
            assert_eq!(output.state.migration_indexed_keys, 1_000);
            assert_eq!(output.state.migration_resumed, 1);
            assert_eq!(output.state.migration_completed, 1);
            assert_eq!(output.state.migration_rolled_back, 1);
            assert_eq!(output.state.migration_rejected, 1);
            assert_eq!(output.state.jobs_enqueued, 5);
            assert_eq!(output.state.jobs_leased, 2);
            assert_eq!(output.state.jobs_completed, 1);
            assert_eq!(output.state.jobs_retried, 1);
            assert_eq!(output.state.jobs_blocked, 1);
            assert_eq!(output.state.session_bound, 3);
            assert_eq!(output.state.session_authorized, 3);
            assert_eq!(output.state.session_rejected, 1);
            assert_eq!(output.state.primary_inserts, 1);
            assert_eq!(output.state.primary_lookups, 2);
            assert_eq!(output.state.primary_updates, 1);
            assert_eq!(output.state.primary_deletes, 1);
            assert_eq!(output.state.primary_rejections, 1);
            assert_eq!(output.state.primary_entries, 0);
            assert_eq!(output.state.index_mutations, 5);
            assert_eq!(output.state.index_entries, 0);
            assert_eq!(output.state.index_rebuilds, 1);
            assert_eq!(output.state.planner_physical_plans, 4);
            assert_eq!(output.state.planner_index_choices, 2);
            assert_eq!(output.state.planner_range_scans, 2);
            assert_eq!(output.state.planner_stream_reads, 1);
            assert_eq!(output.state.executor_plans, 1);
            assert_eq!(output.state.executor_stages, 2);
            assert_eq!(output.state.executor_rows, 5);
            assert_eq!(output.state.executor_remote_reads, 1);
            assert_eq!(output.state.sql_pipeline_bound, 1);
            assert_eq!(output.state.sql_pipeline_authorized, 1);
            assert_eq!(output.state.sql_pipeline_logical, 1);
            assert_eq!(output.state.sql_pipeline_physical, 1);
            assert_eq!(output.state.sql_pipeline_stages, 2);
            assert_eq!(output.state.sql_pipeline_rows, 1);
            assert_eq!(output.state.sql_pipeline_remote_reads, 1);
            assert_eq!(output.state.sql_pipeline_rejections, 1);
            assert_eq!(output.state.sql_dml_bound, 4);
            assert_eq!(output.state.sql_dml_authorized, 4);
            assert_eq!(output.state.sql_dml_planned, 4);
            assert_eq!(output.state.sql_dml_prepared, 3);
            assert_eq!(output.state.sql_dml_committed, 3);
            assert_eq!(output.state.sql_dml_isolated, 3);
            assert_eq!(output.state.sql_dml_kv_applied, 3);
            assert_eq!(output.state.sql_dml_session_staged, 1);
            assert_eq!(output.state.sql_dml_overlay_verified, 2);
            assert_eq!(output.state.sql_dml_rejections, 1);
            assert_eq!(output.state.range_partitions, 2);
            assert_eq!(output.state.range_non_overlaps, 1);
            assert_eq!(output.state.range_rejections, 1);
            assert_eq!(output.state.sstable_manifests, 3);
            assert_eq!(output.state.compaction_inputs, 2);
            assert_eq!(output.state.compaction_outputs, 1);
            assert_eq!(output.state.compaction_live_keys, 1);
            assert_eq!(output.state.compaction_tombstones, 1);
            assert_eq!(output.state.compaction_reclaimed_bytes, 617);
            assert_eq!(output.state.stats_tables, 1);
            assert_eq!(output.state.stats_rows, 1_024);
            assert_eq!(output.state.stats_histogram_buckets, 4);
            assert_eq!(output.state.stats_stale, 1);
            assert_eq!(output.state.constraints_checked, 15);
            assert_eq!(output.state.constraints_accepted, 1);
            assert_eq!(output.state.constraints_rejected, 2);
            assert_eq!(output.state.constraints_unique_keys, 1);
            assert_eq!(output.state.deferred_transactions, 2);
            assert_eq!(output.state.deferred_obligations, 2);
            assert_eq!(output.state.deferred_txn_references, 1);
            assert_eq!(output.state.deferred_committed_references, 0);
            assert_eq!(output.state.deferred_commits, 3);
            assert_eq!(output.state.deferred_rejections, 1);
            assert_eq!(output.state.write_plans, 1);
            assert_eq!(output.state.write_steps, 5);
            assert_eq!(output.state.write_index_mutations, 1);
            assert_eq!(output.state.write_conflict_ranges, 2);
            assert_eq!(output.state.relcommit_groups, 7);
            assert_eq!(output.state.relcommit_row_mutations, 7);
            assert_eq!(output.state.relcommit_index_mutations, 9);
            assert_eq!(output.state.relcommit_conflict_ranges, 14);
            assert_eq!(output.state.relcommit_rejections, 1);
            assert_eq!(output.state.page_frames, 1);
            assert_eq!(output.state.page_redo, 2);
            assert_eq!(output.state.page_reads, 1);
            assert_eq!(output.state.page_rejections, 1);
            assert_eq!(output.state.page_checkpoint_bytes, 4_096);
            assert_eq!(output.state.wal_appends, 2);
            assert_eq!(output.state.wal_syncs, 1);
            assert_eq!(output.state.wal_flush_allows, 1);
            assert_eq!(output.state.wal_rejections, 1);
            assert_eq!(output.state.wal_durable_lsn, 32);
            assert_eq!(output.state.wal_bytes, 160);
            assert_eq!(output.state.buffer_resident, 0);
            assert_eq!(output.state.buffer_hits, 1);
            assert_eq!(output.state.buffer_dirty, 1);
            assert_eq!(output.state.buffer_flushes, 1);
            assert_eq!(output.state.buffer_evictions, 1);
            assert_eq!(output.state.buffer_rejections, 1);
            assert_eq!(output.state.coordinator_transactions, 4);
            assert_eq!(output.state.isolation_commits, 2);
            assert_eq!(output.state.isolation_rejections, 1);
            assert_eq!(output.state.isolation_edges, 0);
            assert_eq!(output.state.isolation_ranges, 4);
            assert_eq!(output.state.query_rows, 1);
            assert_eq!(output.state.kv_keys, 8);
            assert_eq!(output.state.kv_changes, 18);
            assert_eq!(output.state.watch_registered, 1);
            assert_eq!(output.state.watch_deliveries, 2);
            assert_eq!(output.state.watch_resumes, 1);
            assert_eq!(output.state.sessiontxn_sessions, 5);
            assert_eq!(output.state.sessiontxn_pending_writes, 11);
            assert_eq!(output.state.sessiontxn_overlay_reads, 14);
            assert_eq!(output.state.sessiontxn_snapshot_reads, 2);
            assert_eq!(output.state.sessiontxn_commits, 4);
            assert_eq!(output.state.sessiontxn_rollbacks, 1);
            assert_eq!(output.state.sessiontxn_savepoints, 1);
            assert_eq!(output.state.sessiontxn_savepoint_rewinds, 1);
            assert_eq!(output.state.sessiontxn_rejections, 1);
            assert_eq!(output.state.sql_txn_began, 3);
            assert_eq!(output.state.sql_txn_dml_staged, 4);
            assert_eq!(output.state.sql_txn_overlay_reads, 3);
            assert_eq!(output.state.sql_txn_commits, 2);
            assert_eq!(output.state.sql_txn_rollbacks, 1);
            assert_eq!(output.state.sql_txn_rollback_absent, 1);
            assert_eq!(output.state.sql_txn_savepoints, 1);
            assert_eq!(output.state.sql_txn_savepoint_rewinds, 1);
            assert_eq!(output.state.sql_txn_savepoint_absent, 1);
            assert_eq!(output.state.sql_txn_rejections, 1);
            assert_eq!(output.state.prepared_statements, 5);
            assert_eq!(output.state.prepared_binds, 1);
            assert_eq!(output.state.prepared_invalidations, 1);
            assert_eq!(output.state.prepared_deallocations, 1);
            assert_eq!(output.state.prepared_evictions, 1);
            assert_eq!(output.state.prepared_rejections, 1);
            assert_eq!(output.state.portal_parses, 2);
            assert_eq!(output.state.portal_binds, 2);
            assert_eq!(output.state.portal_describes, 1);
            assert_eq!(output.state.portal_executes, 2);
            assert_eq!(output.state.portal_rows, 1);
            assert_eq!(output.state.portal_cancellations, 1);
            assert_eq!(output.state.portal_closes, 1);
            assert_eq!(output.state.portal_statement_closes, 1);
            assert_eq!(output.state.portal_rejections, 2);
            assert_eq!(output.state.rowcodec_encoded, 1);
            assert_eq!(output.state.rowcodec_decoded, 1);
            assert_eq!(output.state.rowcodec_defaults, 1);
            assert_eq!(output.state.rowcodec_generated, 1);
            assert_eq!(output.state.rowcodec_rejections, 2);
            assert!(output.state.rowcodec_bytes > 0);
            assert_eq!(output.state.tuple_versions, 2);
            assert_eq!(output.state.tuple_visible_reads, 2);
            assert_eq!(output.state.tuple_delete_markers, 2);
            assert_eq!(output.state.tuple_rejections, 2);
            assert!(output.state.tuple_bytes > 0);
            assert_eq!(output.state.vacuum_chains, 2);
            assert_eq!(output.state.vacuum_pruned_versions, 2);
            assert_eq!(output.state.vacuum_retained_versions, 4);
            assert_eq!(output.state.vacuum_rejections, 1);
            assert_eq!(output.state.vacuum_reclaimed_bytes, 12);
            assert_eq!(output.state.materialized_rows, 2);
            assert_eq!(output.state.materialized_projected, 1);
            assert_eq!(output.state.materialized_columns, 5);
            assert_eq!(output.state.materialized_rejections, 2);
            assert!(output.state.materialized_bytes > 0);
            assert_eq!(output.state.relational_rows, 0);
            assert_eq!(output.state.object_versions, 1);
            assert_eq!(output.state.objectmeta_buckets, 1);
            assert_eq!(output.state.objectmeta_uploads, 1);
            assert_eq!(output.state.objectmeta_parts, 2);
            assert_eq!(output.state.objectmeta_heads, 1);
            assert_eq!(output.state.objectmeta_rejections, 2);
            assert_eq!(output.state.blob_chunks, 1);
            assert_eq!(output.state.provider_content, 3);
            assert_eq!(output.state.stream_write_chunks, 5);
            assert_eq!(output.state.stream_read_lanes, 3);
            assert_eq!(output.state.stream_window_rejections, 1);
            assert_eq!(output.state.stream_verified_parts, 3);
            assert_eq!(output.state.erasure_data_shards, 3);
            assert_eq!(output.state.erasure_parity_shards, 2);
            assert_eq!(output.state.erasure_tolerated_loss, 2);
            assert_eq!(output.state.erasure_reconstructed_bytes, 22);
            assert_eq!(output.state.manifest_entries, 7);
            assert_eq!(output.state.manifest_surfaces, 6);
            assert_eq!(output.state.manifest_bytes, 25_088);
            assert_eq!(output.state.manifest_rejections, 1);
            assert_eq!(output.state.chunkstore_chunks, 5);
            assert_eq!(output.state.chunkstore_pinned, 3);
            assert_eq!(output.state.chunkstore_bytes, 74);
            assert_eq!(output.state.chunkstore_verified, 3);
            assert_eq!(output.state.chunkstore_deduped, 1);
            assert_eq!(output.state.chunkstore_rejections, 1);
            assert_eq!(output.state.checkpoint_roots, 7);
            assert_eq!(output.state.checkpoint_segments, 4);
            assert_eq!(output.state.checkpoint_compacted, 2);
            assert_eq!(output.state.checkpoint_reclaimable_bytes, 3_584);
            assert_eq!(output.state.checkpoint_rejections, 1);
            assert_eq!(output.state.superblock_generation, 2);
            assert_eq!(output.state.superblock_checkpoint, 56);
            assert_eq!(output.state.superblock_roots, 7);
            assert_eq!(output.state.superblock_advances, 1);
            assert_eq!(output.state.superblock_rejections, 1);
            assert_eq!(output.state.snapshot_roots, 7);
            assert_eq!(output.state.snapshot_barriers, 7);
            assert_eq!(output.state.snapshot_validations, 3);
            assert_eq!(output.state.protocol_commits, 56);
            assert_eq!(output.state.committed_transactions, 3);
            assert_eq!(output.state.storage_committed_prefixes, 3);
            assert_eq!(output.state.storage_manifest_covered, 7);
            assert_eq!(output.state.repair_findings, 5);
            assert_eq!(output.state.repair_actions, 8);
            assert_eq!(output.state.retention_kept, 2);
            assert_eq!(output.state.retention_actions, 2);
            assert_eq!(output.state.requirement_checks, 15);
            assert_eq!(output.state.requirement_covered, 15);
            assert_eq!(output.state.invariant_checks, 34);
            assert_eq!(output.state.invariant_passed, 34);
            assert_eq!(output.state.invariant_failed, 0);
            assert_eq!(output.state.observability_metrics, 301);
            assert_eq!(output.state.observability_audits, 2);
            assert_eq!(output.state.testability_checks, 5);
            assert_eq!(output.state.testability_passed, 5);
            assert!(output.state.testability_trace_events >= 600);
            assert!(output.state.testability_trace_services >= 60);
            assert_eq!(output.state.testability_replay_ready, 1);
            assert_eq!(output.state.cluster_health, "degraded");
        }
    }

    #[test]
    fn run_output_writes_full_summary_and_service_logs() {
        let output = run_one(ImplementationId::UniversalVsrRados).unwrap();
        let dir = std::env::temp_dir().join(format!(
            "rsf-poc-artifacts-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));

        let artifacts = output.write_logs(&dir).unwrap();
        assert!(artifacts.full_log.exists());
        assert!(artifacts.summary.exists());
        assert!(artifacts.service_logs.iter().any(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "deployment.log")
        }));
        assert!(artifacts.service_logs.iter().any(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "upgrade.log")
        }));
        assert!(artifacts.service_logs.iter().any(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "membership.log")
        }));
        assert!(artifacts.service_logs.iter().any(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "vsr.log")
        }));
        assert!(artifacts.service_logs.iter().any(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "wire.log")
        }));
        assert!(artifacts.service_logs.iter().any(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "provider.log")
        }));
        assert!(artifacts.service_logs.iter().any(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "rpc.log")
        }));
        assert!(artifacts.service_logs.iter().any(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "relational.log")
        }));
        assert!(artifacts.service_logs.iter().any(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "catalog.log")
        }));
        assert!(artifacts.service_logs.iter().any(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "tenancy.log")
        }));
        assert!(artifacts.service_logs.iter().any(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "quota.log")
        }));
        assert!(artifacts.service_logs.iter().any(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "cache.log")
        }));
        assert!(artifacts.service_logs.iter().any(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "gateway.log")
        }));
        assert!(artifacts.service_logs.iter().any(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "consistency.log")
        }));
        assert!(artifacts.service_logs.iter().any(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "idempotency.log")
        }));
        assert!(artifacts.service_logs.iter().any(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "schema.log")
        }));
        assert!(artifacts.service_logs.iter().any(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "jobs.log")
        }));
        assert!(artifacts.service_logs.iter().any(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "txn.log")
        }));
        assert!(artifacts.service_logs.iter().any(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "isolation.log")
        }));
        assert!(artifacts.service_logs.iter().any(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "sessiontxn.log")
        }));
        assert!(artifacts.service_logs.iter().any(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "index.log")
        }));
        assert!(artifacts.service_logs.iter().any(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "planner.log")
        }));
        assert!(artifacts.service_logs.iter().any(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "executor.log")
        }));
        assert!(artifacts.service_logs.iter().any(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "sqlpipeline.log")
        }));
        assert!(artifacts.service_logs.iter().any(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "sqldml.log")
        }));
        assert!(artifacts.service_logs.iter().any(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "objectmeta.log")
        }));
        assert!(artifacts.service_logs.iter().any(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "compaction.log")
        }));
        assert!(artifacts.service_logs.iter().any(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "stats.log")
        }));
        assert!(artifacts.service_logs.iter().any(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "constraints.log")
        }));
        assert!(artifacts.service_logs.iter().any(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "write.log")
        }));
        assert!(artifacts.service_logs.iter().any(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "health.log")
        }));
        assert!(artifacts.service_logs.iter().any(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "session.log")
        }));
        assert!(artifacts.service_logs.iter().any(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "page.log")
        }));
        assert!(artifacts.service_logs.iter().any(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "wal.log")
        }));
        assert!(artifacts.service_logs.iter().any(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "buffer.log")
        }));
        assert!(artifacts.service_logs.iter().any(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "manifest.log")
        }));
        assert!(artifacts.service_logs.iter().any(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "storage.log")
        }));
        assert!(artifacts.service_logs.iter().any(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "chunkstore.log")
        }));
        assert!(artifacts.service_logs.iter().any(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "checkpoint.log")
        }));
        assert!(artifacts.service_logs.iter().any(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "superblock.log")
        }));
        assert!(artifacts.service_logs.iter().any(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "snapshot.log")
        }));
        assert!(artifacts.service_logs.iter().any(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "stream.log")
        }));
        assert!(artifacts.service_logs.iter().any(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "recovery.log")
        }));
        assert!(artifacts.service_logs.iter().any(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "erasure.log")
        }));
        assert!(artifacts.service_logs.iter().any(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "watch.log")
        }));
        assert!(artifacts.service_logs.iter().any(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "query.log")
        }));
        assert!(artifacts.service_logs.iter().any(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "invariants.log")
        }));
        assert!(artifacts.service_logs.iter().any(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "requirements.log")
        }));
        assert!(artifacts.service_logs.iter().any(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "repair.log")
        }));
        assert!(artifacts.service_logs.iter().any(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "retention.log")
        }));
        assert!(
            fs::read_to_string(&artifacts.full_log)
                .unwrap()
                .contains("relational")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("protocol_commits=56")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("deployment_endpoints=8")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("upgrade_rejections=1")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("membership_promotions=1")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("membership_intersections=1")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("vsr_view_changes=1")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("wire_protocols=5")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("provider_selected=4")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("provider_content=3")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("rpc_calls=6")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("tenant_surfaces=4")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("quota_allowed=4")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("cache_hits=3")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("gateway_routes=4")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("idempotency_replays=1")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("health_actions=5")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("schema_steps=4")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("migration_completed=1")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("migration_rolled_back=1")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("primary_inserts=1")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("primary_rejections=1")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("jobs_enqueued=5")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("index_mutations=5")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("planner_physical_plans=4")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("executor_stages=2")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("compaction_reclaimed_bytes=617")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("stats_histogram_buckets=4")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("constraints_rejected=2")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("write_steps=5")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("page_redo=2")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("wal_flush_allows=1")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("buffer_evictions=1")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("session_bound=3")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("sessiontxn_overlay_reads=14")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("sessiontxn_commits=4")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("sessiontxn_rollbacks=1")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("sessiontxn_savepoints=1")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("sessiontxn_savepoint_rewinds=1")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("watch_deliveries=2")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("stream_write_chunks=5")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("isolation_rejections=1")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("erasure_data_shards=3")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("manifest_entries=7")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("manifest_rejections=1")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("storage_committed_prefixes=3")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("storage_manifest_covered=7")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("chunkstore_chunks=5")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("chunkstore_deduped=1")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("checkpoint_compacted=2")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("checkpoint_rejections=1")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("superblock_generation=2")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("superblock_rejections=1")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("snapshot_roots=7")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("recovery_replays=4")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("restore_executed_items=7")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("restore_executed_surfaces=7")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("rebalance_policy_proofs=1")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("repair_actions=8")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("sql_pipeline_rows=1")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("sql_pipeline_rejections=1")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("deferred_transactions=2")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("deferred_commits=3")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("deferred_rejections=1")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("consistency_rejections=1")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("objectmeta_heads=1")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("objectmeta_rejections=2")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("range_non_overlaps=1")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("range_rejections=1")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("failure_drill_passed=1")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("failure_drill_domains=6")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("workload_classes=4")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("workload_budget_paths=4")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("workload_benchmark_claim=0")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("invariant_passed=34")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("prepared_statements=5")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("prepared_binds=1")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("prepared_invalidations=1")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("prepared_deallocations=1")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("prepared_evictions=1")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("prepared_rejections=1")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("portal_parses=2")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("portal_binds=2")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("portal_executes=2")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("portal_cancellations=1")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("portal_rejections=2")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("rowcodec_encoded=1")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("rowcodec_decoded=1")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("rowcodec_defaults=1")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("rowcodec_generated=1")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("rowcodec_rejections=2")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("tuple_versions=2")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("tuple_visible_reads=2")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("tuple_delete_markers=2")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("tuple_rejections=2")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("vacuum_chains=2")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("vacuum_pruned_versions=2")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("vacuum_retained_versions=4")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("vacuum_rejections=1")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("materialized_rows=2")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("materialized_projected=1")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("materialized_columns=5")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("materialized_rejections=2")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("relcommit_groups=7")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("relcommit_row_mutations=7")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("relcommit_index_mutations=9")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("relcommit_rejections=1")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("sql_dml_committed=3")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("sql_dml_kv_applied=3")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("sql_dml_session_staged=1")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("sql_dml_overlay_verified=2")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("sql_txn_began=3")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("sql_txn_overlay_reads=3")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("sql_txn_commits=2")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("sql_txn_rollbacks=1")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("sql_txn_rollback_absent=1")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("sql_txn_savepoints=1")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("sql_txn_savepoint_rewinds=1")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("sql_txn_savepoint_absent=1")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("requirement_covered=15")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("testability_checks=5")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("testability_passed=5")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("testability_replay_ready=1")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("invariant_failed=0")
        );
        assert!(
            fs::read_to_string(&artifacts.summary)
                .unwrap()
                .contains("cluster_health=degraded")
        );

        let _ = fs::remove_dir_all(dir);
    }
}
