import json
import sys
from pathlib import Path

import re

def test_passed(line, name):
    # Host child diagnostics can prefix a libtest result on the same line.
    # Require a complete test path, exact name and successful result token.
    return re.search(r"(?<!\S)test [A-Za-z0-9_:]+::" + re.escape(name)
                     + r" \.\.\. ok(?:\s|$)", line) is not None

root, seed, passed, failed, before, after = sys.argv[1:]
metrics = {}
automatic_client_metrics = {}
for name in ("daemon-stress", "release-stress"):
    trace = Path(root, name + ".trace.fozzy")
    if not trace.exists():
        continue
    # Event summaries truncate stdout. Decisions retain the complete recorded
    # process result, including metrics after verbose fault diagnostics.
    for fields in json.loads(trace.read_text()).get("decisions", []):
        if fields.get("kind") != "proc_spawn" or fields.get("exit_code") != 0:
            continue
        for line in fields.get("stdout", "").splitlines():
            start = line.find("{")
            if start < 0:
                continue
            try:
                value = json.loads(line[start:])
            except json.JSONDecodeError:
                continue
            if value.get("workload") == "native-kv-process-stress":
                metrics[name] = value
workloads_valid = all(
    name in metrics
    and metrics[name].get("profile") == profile
    and metrics[name].get("clients") == 4
    and metrics[name].get("rounds") == 3
    and metrics[name].get("acknowledged") == writes * 4 * 3 + 6
    and metrics[name].get("abrupt_process_shutdowns") == 18
    and metrics[name].get("uncertain_writes_resolved") == 3
    for name, profile, writes in (("daemon-stress", "debug", 16), ("release-stress", "release", 64))
)
automatic_trace = Path(root, "automatic-client.trace.fozzy")
if automatic_trace.exists():
    for fields in json.loads(automatic_trace.read_text()).get("decisions", []):
        if fields.get("kind") != "proc_spawn" or fields.get("exit_code") != 0:
            continue
        for line in fields.get("stdout", "").splitlines():
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if value.get("workload") == "automatic-client-takeover":
                automatic_client_metrics[value.get("profile")] = value
automatic_valid = all(
    profile in automatic_client_metrics
    and automatic_client_metrics[profile].get("concurrent_clients") == 4
    and automatic_client_metrics[profile].get("submitted_requests") == 32
    and 4 <= automatic_client_metrics[profile].get("acknowledged_before_crash", 0) < 32
    and automatic_client_metrics[profile].get("requests_without_acknowledgement") == 32 - automatic_client_metrics[profile].get("acknowledged_before_crash", 0)
    and automatic_client_metrics[profile].get("retried_requests") == 32
    and automatic_client_metrics[profile].get("verified_logical_kv_writes") == 162
    and automatic_client_metrics[profile].get("history_growth_writes") == 128
    and automatic_client_metrics[profile].get("coordinator_reuses_before_crash", 0) >= 128
    and automatic_client_metrics[profile].get("coordinator_recoveries_before_crash", 9999) < automatic_client_metrics[profile].get("coordinator_reuses_before_crash", 0)
    and automatic_client_metrics[profile].get("surviving_replicas_checked") == 2
    and automatic_client_metrics[profile].get("primary_state_deleted") is True
    and automatic_client_metrics[profile].get("replacement_restarts") == 1
    and automatic_client_metrics[profile].get("surface_count") == 4
    and automatic_client_metrics[profile].get("active_sql_session_resumed") is True
    and automatic_client_metrics[profile].get("sql_session_original_results_replayed") is True
    and automatic_client_metrics[profile].get("serializable_sql_phantom_rejected") is True
    and automatic_client_metrics[profile].get("precrash_sql_read_dependency_recovered") is True
    and automatic_client_metrics[profile].get("disjoint_sql_sessions_committed") is True
    and automatic_client_metrics[profile].get("sql_key_components_isolated") is True
    and automatic_client_metrics[profile].get("sql_stream_after_takeover") is True
    and automatic_client_metrics[profile].get("indexed_sql_after_takeover") is True
    for profile in ("debug", "release")
)
session_regressions = [
    "replicated_sql_table_and_primary_key_components_cannot_alias",
    "framed_relational_components_are_injective_ordered_and_domain_separated",
    "legacy_key_namespaces_reject_aliasing_before_submission_and_cold_followers_inherit_format",
    "overlapping_legacy_catalog_recovery_fails_repeatedly_without_rewriting_metadata",
    "framed_key_recovery_rejects_mixed_or_unowned_rows_and_format_downgrades",
    "framed_command_key_format_is_bound_and_cannot_switch_a_nonempty_namespace",
    "sql_index_rebuilds_do_not_clear_another_tables_same_named_index",
    "rebuilds_are_scoped_by_table_and_reject_foreign_mutations_before_clearing",
    "tracked_reads_allow_disjoint_commits_and_replay_after_every_follower_restart",
    "savepoint_rewinds_retain_predicates_and_implicit_absence_reads_after_restart",
    "tracked_read_limits_fail_before_intents_and_identical_reads_reuse_certificates",
    "legacy_active_sessions_keep_coarse_validation_and_original_rsfsp2_receipts",
    "killed_session_read_recovers_certified_dependency_and_original_result_before_acknowledgement",
    "serializable_sessions_reject_write_skew_after_restart_and_follower_recovery",
    "serializable_sessions_reject_phantoms_and_tombstones_including_read_only_commits",
    "serializable_upgrade_preserves_legacy_receipts_and_fences_old_pending_commits",
    "durable_sessions_commit_schema_rows_and_savepoints_atomically_and_replay_original_results",
    "durable_sessions_isolate_owners_detect_snapshot_conflicts_and_preserve_unrelated_commits",
    "durable_session_commands_replay_active_transactions_and_indexes_after_each_follower_restart",
    "session_context_rejects_noncanonical_unknown_and_oversized_inputs",
    "session_limits_reject_before_intents_and_leave_transactions_usable",
    "session_recovery_rejects_divergent_state_results_and_catalog_and_allows_schema_abort",
    "killed_session_commit_recovers_atomic_rows_schema_and_original_receipt",
    "bound_v5_checkpoint_upgrades_without_changing_autocommit_receipts",
]
workspace_trace = Path(root, "workspace.trace.fozzy")
workspace_stdout = ""
if workspace_trace.exists():
    workspace_stdout = "\n".join(
        decision.get("stdout", "")
        for decision in json.loads(workspace_trace.read_text()).get("decisions", [])
        if decision.get("kind") == "proc_spawn" and decision.get("exit_code") == 0
    )
session_evidence = {
    name: any(test_passed(line, name)
              for line in workspace_stdout.splitlines())
    for name in session_regressions
}
sessions_valid = all(session_evidence.values())
uncertain_begin_name = "automatic_client_takeover_uncertain_sql_begin_preserves_token_after_both_peer_restarts"
uncertain_begin_workspace = any(test_passed(line, uncertain_begin_name)
                               for line in workspace_stdout.splitlines())
uncertain_begin_profile_runs = 0
if automatic_trace.exists():
    uncertain_begin_profile_runs = sum(
        any(test_passed(line, uncertain_begin_name)
            for line in decision.get("stdout", "").splitlines())
        for decision in json.loads(automatic_trace.read_text()).get("decisions", [])
        if decision.get("kind") == "proc_spawn" and decision.get("exit_code") == 0
    )
uncertain_begin_valid = uncertain_begin_workspace and uncertain_begin_profile_runs == 2
client_transport_name = "automatic_client_takeover_transport_loss_preserves_exact_sql_mutation_after_restart"
client_transport_workspace = any(test_passed(line, client_transport_name)
                                 for line in workspace_stdout.splitlines())
client_transport_profile_runs = 0
if automatic_trace.exists():
    client_transport_profile_runs = sum(
        any(test_passed(line, client_transport_name)
            for line in decision.get("stdout", "").splitlines())
        for decision in json.loads(automatic_trace.read_text()).get("decisions", [])
        if decision.get("kind") == "proc_spawn" and decision.get("exit_code") == 0
    )
client_transport_valid = client_transport_workspace and client_transport_profile_runs == 2
kv_batch_evidence = {
    name: any(test_passed(line, name)
              for line in workspace_stdout.splitlines())
    for name in (
        "rejected_committed_batches_preserve_all_values_versions_and_changes",
        "committed_batch_preflight_covers_order_tombstones_and_duplicate_replay",
    )
}
kv_batches_valid = all(kv_batch_evidence.values())
stream_regressions = [
    "borrowed_prefix_scans_preserve_order_bounds_and_tombstones",
    "pull_execution_stops_at_limit_and_sink_failure_without_reading_ahead",
    "sql_pull_limit_does_not_decode_later_rows_and_sink_failure_stops",
    "sql_pull_merges_ordered_overlay_and_retains_only_latest_report",
    "sql_pull_index_filter_skips_nonmatching_nullable_rows_before_projection",
    "sql_stream_decoder_requires_complete_ordered_bounded_results",
    "sql_row_chunks_bound_frames_and_preserve_escaped_json",
    "sql_stream_sink_failure_never_publishes_completion",
    "streaming_application_work_cannot_renew_blocked_socket_budget",
    "native_sql_streams_large_results_and_bounds_legacy_responses",
]
stream_evidence = {
    name: any(test_passed(line, name)
              for line in workspace_stdout.splitlines())
    for name in stream_regressions
}
stream_metrics = {}
stream_trace = Path(root, "sql-stream.trace.fozzy")
if stream_trace.exists():
    for decision in json.loads(stream_trace.read_text()).get("decisions", []):
        if decision.get("kind") != "proc_spawn" or decision.get("exit_code") != 0:
            continue
        for line in decision.get("stdout", "").splitlines():
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if value.get("workload") == "native-sql-stream":
                stream_metrics[value.get("profile")] = value
streams_valid = all(stream_evidence.values()) and all(
    profile in stream_metrics
    and stream_metrics[profile].get("rows") == 128
    and stream_metrics[profile].get("bytes", 0) > 65536
    and stream_metrics[profile].get("chunk_bound") == 4096
    and stream_metrics[profile].get("buffered_overflow_rejected") is True
    and stream_metrics[profile].get("quorum_loss_rejected") is True
    and stream_metrics[profile].get("indexed_queries_verified") is True
    for profile in ("debug", "release")
)
indexed_names = [
    "borrowed_index_lookup_preserves_order_scope_and_storage_identity",
    "indexed_select_uses_only_matching_candidates_and_stops_at_limit",
    "typed_secondary_index_keys_are_canonical_across_writes_and_recovery",
    "indexed_transaction_overlay_merges_entries_movements_and_savepoint_rewinds",
    "indexed_reads_reject_missing_or_mismatched_committed_entries",
    "query_size_metadata_tracks_physical_writes_transactions_and_recovery",
]
indexed_evidence = {
    name: any(test_passed(line, name)
              for line in workspace_stdout.splitlines())
    for name in indexed_names
}
indexed_valid = all(indexed_evidence.values())
membership_approval_names = [
    "surviving_minority_report_completes_exact_old_quorum_after_primary_loss",
    "membership_continuation_rejects_spoofed_reports_conflicts_and_election_promises",
    "killed_membership_continuation_recovers_exact_fence_before_acknowledgement",
    "membership_minority_continuation_completes_over_tls_after_initiator_loss",
    "membership_continuation_without_surviving_report_never_initiates_proposal",
    "membership_proposal_codec_binds_old_authority_new_voters_and_target",
    "membership_fence_survives_restart_preserves_prefix_and_blocks_authority_shortcuts",
    "membership_old_quorum_approval_rejects_mixed_duplicate_and_tampered_histories",
    "malformed_membership_fence_envelopes_fail_repeated_recovery_without_rewrite",
    "killed_membership_writer_recovers_synced_fence_before_acknowledgement",
    "membership_approval_recovers_from_real_old_voters_after_primary_loss",
]
membership_approval_evidence = {
    name: any(test_passed(line, name)
              for line in workspace_stdout.splitlines())
    for name in membership_approval_names
}
membership_approval_valid = all(membership_approval_evidence.values())
membership_decision_names = ['membership_input_and_decision_codecs_bind_exact_quorums_and_reject_truncation', 'membership_higher_round_retains_minority_nomination_and_chosen_plan_after_old_data_loss', 'membership_upload_retries_are_atomic_bounded_and_fenced_by_new_rounds', 'membership_decision_rejects_missing_proofs_wrong_roles_and_malformed_durable_state', 'killed_membership_coordinator_recovers_synced_nomination_before_reply', 'every_new_quorum_intersection_preserves_chosen_input_across_recovery_rounds', 'torn_membership_input_batch_recovers_previous_prefix_and_cannot_be_accepted']
membership_decision_evidence = {
    name: any(test_passed(line, name)
              for line in workspace_stdout.splitlines())
    for name in membership_decision_names
}
membership_decision_valid = all(membership_decision_evidence.values())
membership_peer_names = ['membership_authority_inbox_resumes_validated_proof_without_voting_authority', 'real_tls_membership_plan_choice_survives_old_voter_and_coordinator_data_loss', 'real_tls_membership_authority_rejects_unpublished_learners_and_foreign_bootstrap', 'membership_acceptance_resolves_one_lost_tls_reply_without_duplicate_vote']
membership_peer_evidence = {
    name: any(test_passed(line, name)
              for line in workspace_stdout.splitlines())
    for name in membership_peer_names
}
membership_peer_valid = all(membership_peer_evidence.values())
membership_staging_names = ['chosen_membership_retention_retry_rejects_missing_input_without_recreation', 'partial_ancestor_creation_and_existing_bytes_are_preserved', 'file_obstructions_are_errors_without_replacement_or_sync_success', 'chosen_membership_stage_merges_commit_and_receipt_evidence_deterministically', 'membership_stage_materialization_is_boot_scoped_and_preserves_receiver_receipts', 'staged_membership_quorums_reject_mixed_duplicate_and_invalid_receipts', 'membership_stage_reopen_rejects_torn_or_missing_sealed_history_without_rewrite', 'killed_membership_stage_recovers_receipt_and_replays_materialization_before_report', 'real_tls_staged_membership_requires_materializer_and_survives_peer_restart']
membership_staging_evidence = {
    name: any(test_passed(line, name)
              for line in workspace_stdout.splitlines())
    for name in membership_staging_names
}
membership_staging_valid = all(membership_staging_evidence.values())
membership_activation_names = ['membership_activation_preserves_certificates_receipts_pending_data_and_later_commits', 'membership_activation_requires_boot_materialization_matching_quorum_and_root_fence', 'membership_activation_replay_rejects_truncation_corrupt_boundaries_and_ordinary_markers', 'membership_activation_publication_boundaries_recover_old_fence_or_complete_new_authority', 'copied_membership_activation_history_retains_quorum_authority_guards', 'killed_membership_activation_recovers_sealed_authority_before_any_reply', 'successive_membership_activations_preserve_authority_and_fence_removed_voters', 'copied_view_installation_history_retains_quorum_authority_guards', 'published_view_recovery_rejects_lost_pointer_and_corrupt_publication_marker']
membership_activation_evidence = {
    name: any(test_passed(line, name)
              for line in workspace_stdout.splitlines())
    for name in membership_activation_names
}
membership_activation_valid = all(membership_activation_evidence.values())
idle_application_names = ['idle_sql_writer_survives_polls_while_status_and_transport_checks_remain_fresh', 'idle_application_rejects_same_version_divergent_protocol_history_repeatedly']
idle_application_evidence = {
    name: any(test_passed(line, name)
              for line in workspace_stdout.splitlines())
    for name in idle_application_names
}
idle_application_valid = all(idle_application_evidence.values())
primary_recovery_names = [
    "restarted_partial_primary_recovers_above_another_voters_higher_promise",
    "previously_confirmed_primary_recovers_quorum_after_higher_promise",
    "stale_primary_waits_once_for_newer_installed_view_then_recovers_if_repair_never_arrives",
]
primary_recovery_evidence = {
    name: any(test_passed(line, name)
              for line in workspace_stdout.splitlines())
    for name in primary_recovery_names
}
primary_recovery_valid = all(primary_recovery_evidence.values())
membership_capacity_names = [
    "membership_capacity_rejects_oversized_transition_before_any_durable_fence",
    "membership_capacity_matches_complete_encoded_input_and_recovery_decision",
    "historical_oversized_membership_fence_remains_readable_and_exact_retry_does_not_rewrite_it",
    "membership_capacity_checks_old_and_new_cardinalities_at_the_publication_boundary",
    "oversized_membership_is_rejected_before_any_tls_voter_is_fenced",
]
membership_capacity_evidence = {
    name: any(test_passed(line, name) for line in workspace_stdout.splitlines())
    for name in membership_capacity_names
}
membership_capacity_valid = all(membership_capacity_evidence.values())
learner_promotion_names = [
    "learner_promotion_publishes_owned_new_voter_and_cold_recovers_original_receipts",
    "learner_promotion_retry_preserves_later_quorum_commit_and_ordinary_view_change",
    "learner_promotion_rejects_unowned_overlapping_symlink_and_wrong_reporter_targets",
    "learner_promotion_missing_or_changed_owner_and_pointer_fail_without_republication",
    "killed_learner_promotion_resumes_owned_target_without_exposing_old_authority",
    "learner_promotion_retry_cannot_reactivate_a_voter_removed_by_later_membership",
]
learner_promotion_evidence = {
    name: any(test_passed(line, name) for line in workspace_stdout.splitlines())
    for name in learner_promotion_names
}
learner_promotion_valid = all(learner_promotion_evidence.values())
native_role_names = [
    "native_peer_recovers_certified_promotion_and_rejects_foreign_owner_or_group",
    "native_peer_keeps_an_unpublished_owned_promotion_nonvoting_then_recovers_after_publication",
    "native_peer_rejects_a_valid_promoted_store_under_another_node_identity",
    "native_peer_rejects_corrupt_publication_without_downgrading_or_recreating_authority",
    "native_peer_requires_explicit_promoted_voter_configuration_for_mixed_group_elections",
    "native_peer_validates_election_budget_against_recovered_membership",
]
native_role_evidence = {
    name: any(test_passed(line, name) for line in workspace_stdout.splitlines())
    for name in native_role_names
}
native_role_valid = all(native_role_evidence.values())
publication_recovery_names = [
    'membership_publication_ancestry_survives_proof_variants_new_views_and_promises',
    'membership_publication_ancestry_ignores_inactive_generations_and_matching_voters',
    'membership_publication_ancestry_requires_healthy_active_custody',
    'membership_publication_input_codec_and_bound_local_stage_reject_tampering',
    'membership_publication_retention_preserves_first_quorum_and_requires_boot_materialization',
    'membership_publication_retention_rejects_conflicting_authority_without_overwrite',
    'membership_publication_retention_corruption_or_missing_custody_never_recreates_authority',
    'membership_publication_retention_io_failure_fences_handle_and_resumes_durable_input',
    'membership_publication_retention_rejects_foreign_temporary_paths_and_resumes_regular_torn_files',
    'killed_membership_publication_retention_recovers_input_without_activating_or_acknowledging',
    'publication_quorums_require_distinct_majorities_of_one_canonical_transition',
    'publication_codecs_reject_truncation_extra_bytes_and_noncanonical_order',
]
publication_recovery_evidence = {
    name: any(test_passed(line, name) for line in workspace_stdout.splitlines())
    for name in publication_recovery_names
}
publication_recovery_valid = all(publication_recovery_evidence.values())
peer_publication_names = [
    'published_stage_materialization_applies_current_commits_before_renewing_receipt',
    'real_tls_membership_publication_survives_candidate_loss_cold_roles_and_newer_views',
    'real_tls_membership_publication_rejects_wrong_authority_missing_apply_and_corrupt_owner',
    'real_tls_membership_publication_resolves_one_lost_reply_and_binds_reporter',
]
peer_publication_evidence = {
    name: any(test_passed(line, name) for line in workspace_stdout.splitlines())
    for name in peer_publication_names
}
peer_publication_valid = all(peer_publication_evidence.values())
prechoice_prerequisite_names = [
    'resumed_membership_begin_requires_publication_sync_before_ack_or_journal_binding',
    'exact_factory_prefixes_are_read_only_until_explicit_resume',
    'manifestless_empty_segment_stays_invalid_to_generic_recovery',
    'final_manifest_presence_never_enters_initialization_and_options_are_not_inferred',
    'nonempty_foreign_and_noncanonical_factory_states_are_never_repaired',
    'symlink_and_hardlink_factory_files_cannot_redirect_reads_or_writes',
    'every_factory_interruption_resumes_or_is_already_published_without_data_reset',
    'empty_factory_requires_existing_parent_and_excludes_an_active_manifest_owner',
    'killed_factory_owner_releases_custody_and_preserves_resumable_physical_state',
    'membership_recovery_metadata_reconstructs_accepted_majority_without_chosen_survivor',
    'membership_recovery_round_floor_uses_lexicographic_order_and_rejects_exhaustion',
    'membership_chosen_import_resumes_exact_unbound_prefix_across_restart_without_lowering_promise',
    'membership_recovery_never_repairs_missing_or_corrupt_acknowledged_input',
    'membership_recovery_rejects_valid_but_incomplete_acknowledged_prefix_even_on_old_append_path',
    'membership_recovery_rejects_malformed_unbound_custody_and_failed_handle_metadata',
    'membership_recovery_codecs_bound_variants_and_reject_impossible_facts',
    'membership_input_factory_prefixes_resume_only_while_unbound_and_never_grant_a_vote',
    'membership_input_factory_rejects_oversized_temp_foreign_descriptor_and_nonempty_unpublished_wal',
    'real_tls_prechoice_recovers_highest_accepted_input_in_bounded_exchanges_after_coordinator_loss',
    'real_tls_chosen_import_resumes_partial_unbound_input_after_receiver_restart',
    'real_tls_prechoice_rejects_foreign_context_and_missing_acknowledged_custody_without_mutation',
    'real_tls_prechoice_lost_promise_and_chosen_finish_replies_are_idempotent_and_metadata_binds_reporter',
]
prechoice_prerequisite_evidence = {
    name: any(test_passed(line, name) for line in workspace_stdout.splitlines())
    for name in prechoice_prerequisite_names
}
prechoice_prerequisites_valid = all(prechoice_prerequisite_evidence.values())
integrated_recovery_names = [
    'interrupted_mkdir_retries_every_existing_ancestor_and_sync_failure',
    'partial_ancestor_creation_and_existing_bytes_are_preserved',
    'file_obstructions_are_errors_without_replacement_or_sync_success',
    'relative_empty_and_parent_components_keep_filesystem_resolution',
    'provisioned_aliases_sync_physical_and_lexical_routes_without_replacement',
    'final_file_alias_publication_covers_both_parents_and_every_failure',
    'process_kill_after_mkdir_requires_barriers_on_visible_retry',
    'newly_created_segment_is_present_before_publication_and_failure_returns_no_writer',
    'existing_segment_failure_preserves_wal_and_releases_lock_before_retry',
    'direct_open_locked_creation_also_executes_publication_before_returning',
    'generic_manifestless_segment_rejection_still_preserves_residue',
    'published_learner_token_rejects_warm_missing_or_corrupt_outer_custody_without_repair',
    'published_learner_token_rejects_changed_inner_generation_and_outer_aliases',
    'newer_legitimate_learner_publication_changes_opaque_token_without_voting_authority',
    'published_learner_token_rejects_missing_inner_directory_and_trailing_dot_root_alias',
    'accepted_majority_reconstruction_never_mixes_rounds_or_inputs',
    'relayed_or_unchanged_promises_do_not_keep_dead_coordinator_preferred',
    'recovery_grace_covers_global_fair_cycles_and_checks_arithmetic',
    'durable_driver_preserves_minority_b_across_partial_copy_restart',
    'delayed_older_state_never_downgrades_or_renews_newer_direct_activity',
    'another_survivor_restarts_phase_one_after_unaccepted_selected_source_dies',
    'additional_copy_source_is_promised_without_replacing_frozen_phase_one_proof',
    'lost_accept_reply_then_metadata_majority_rebinds_complete_copy_as_chosen',
    'real_tls_round_bound_copy_activity_rejects_stale_reads_and_keeps_observers_inert',
    'managed_recovery_drop_joins_live_tls_flight_before_releasing_transport_custody',
    'real_tls_two_group_recovery_serves_inbound_and_rotates_past_a_blackhole',
    'real_tls_expired_ordinary_publication_reply_keeps_peer_and_exact_receipt_live',
    'durable_group_flushes_each_dirty_segment_before_successor_manifest_and_return',
    'durable_group_is_byte_identical_to_sequential_appends_across_rotation',
    'durable_group_rotated_and_final_flush_failures_fence_without_acknowledging',
    'group_rotation_failure_keeps_the_flushed_prefix_and_fences_the_owner',
    'group_post_flush_stamp_loss_never_recaptures_missing_custody',
    'empty_suffix_after_failed_sync_renews_active_data_before_success',
    'legacy_failed_single_sync_then_reopen_rotation_requires_closed_prefix_renewal',
    'closed_prefix_renewal_rejects_alias_and_stays_fenced_after_restoration',
    'ordinary_append_keeps_original_sync_policy_after_group_success',
    'group_first_validation_is_retryable_but_late_rejection_fences_partial_owner',
    'group_empty_and_none_policy_never_claim_a_flush',
    'group_refreshes_other_handles_and_preserves_own_revision_and_custody_boundaries',
    'input_iterator_unwind_cannot_expose_an_unflushed_live_cursor',
    'rotated_generation_group_preserves_sealed_bytes_publication_boundaries_and_later_appends',
    'installation_input_rejects_wrong_plan_identity_with_valid_quorum_bytes_after_reopen',
    'managed_recovery_accepts_delayed_historical_reply_after_real_tls_view_and_promise',
    'managed_recovery_discards_real_delayed_reply_after_active_custody_or_context_loss',
    'managed_recovery_rejects_authenticated_reply_for_another_input_then_recovers',
    'native_prechoice_reconstructs_accepted_majority_and_keeps_stale_learner_clients_closed',
    'native_prechoice_competing_survivors_keep_highest_minority_b_and_original_receipt',
    'native_prechoice_coordinator_crash_preserves_an_actively_advancing_partial_input',
    'native_prechoice_two_groups_progress_with_removed_candidate_and_serve_inbound_queries',
    'native_prechoice_rejects_missing_or_valid_shortened_accepted_custody_without_repair',
    'canonical_commands_reject_every_truncated_prefix_trailing_data_and_unknown_variants',
    'malformed_counts_duplicates_and_oversize_plans_cannot_acquire_an_intent',
    'valid_stale_commands_are_rejected_but_malformed_committed_authority_is_corruption',
    'all_pg_finalization_is_atomic_and_requires_new_control_authority',
    'request_id_reuse_and_identity_rebinding_cannot_mutate_an_existing_plan',
    'a_b_a_membership_keeps_distinct_admission_roots',
    'public_projection_comes_from_committed_live_history_and_survives_cold_replay',
    'uncertain_begin_commit_keeps_its_result_after_survivor_reprepare_and_cold_replay',
    'finalized_catalog_and_admission_survive_an_ordinary_control_election_and_retry',
    'old_control_commit_majority_prevents_begin_reprepare_across_membership',
    'membership_lineage_is_stable_across_ordinary_views_and_exact_across_cold_history',
    'legacy_membership_round_trip_never_aliases_bootstrap_lineage',
    'unpersisted_legacy_bootstrap_does_not_supply_lineage_before_explicit_migration',
    'certified_lineage_does_not_retroactively_certify_legacy_membership_epochs',
    'peer_open_ancestor_barrier_failure_precedes_identity_lock_or_listener',
    'decision_factory_ancestor_barrier_failure_returns_no_owner_or_authority',
    'public_begin_ancestor_barrier_failure_preserves_journal_and_retries_factory',
    'resumed_membership_begin_requires_publication_sync_before_ack_or_journal_binding',
    'strict_existing_replay_does_not_publish_directory_ancestry',
    'create_capable_store_recovery_publishes_fresh_and_existing_routes',
    'public_segment_recovery_publishes_created_file_and_existing_retry',
    'owned_segment_recovery_never_recreates_a_missing_acknowledged_file',
    'ephemeral_segment_lock_does_not_grant_durable_directory_publication',
    'strict_existing_recovery_keeps_missing_store_and_segment_fail_closed',
    'owned_stamp_rejects_missing_truncated_aliased_and_replaced_files_without_repair',
    'own_wal_append_rotation_and_cold_open_establish_new_stamps_without_generic_refresh',
    'generic_second_writer_remains_supported_but_exclusive_stamp_refuses_to_adopt_it',
    'trusted_ancestor_alias_parent_route_is_preserved_but_final_owned_alias_is_rejected',
    'first_unbound_accept_or_choice_requires_publication_before_journal_binding',
    'published_default_history_latches_missing_or_changed_physical_store_custody',
    'managed_learner_poll_rejects_lost_default_manifest_before_dispatch_or_promise',
    'finish_recomputes_digest_from_replaced_valid_live_stage_bytes',
    'finish_missing_live_stage_custody_does_not_recreate_it',
    'finish_revalidates_torn_live_tail_before_resumable_retry',
    'concurrent_same_name_same_clock_fixtures_preserve_existing_custody_and_isolation',
    'created_stage_input_copy_resumes_exact_prefix_and_rejects_conflicting_retry',
    'bound_choice_uses_live_custody_but_cold_owner_still_renews_ancestors',
    'selected_inbox_begin_reuses_owner_but_still_requires_input_publication',
    'selected_inbox_begin_rejects_missing_manifest_without_recreating_input',
    'ordinary_history_without_learned_exceptions_keeps_public_prepare_validation',
    'learned_exception_binds_the_complete_prepare_after_real_installation_and_restart',
    'chosen_finish_revalidates_missing_and_valid_short_input_before_first_choice',
    'chosen_finish_valid_retry_keeps_one_journal_entry_and_checks_custody_again',
    'membership_cold_validator_visits_each_distinct_complete_input_once_with_replayed_identity',
    'membership_cold_validator_leaves_staged_only_prefix_resumable_without_approval',
    'membership_cold_host_validator_rejection_preserves_promises_and_all_retained_bytes',
    'membership_cold_dedup_never_downgrades_a_complete_bound_input_to_partial',
    'membership_cold_validator_rejects_input_alias_without_touching_its_target',
    'real_tls_strict_cold_open_reuses_validated_inputs_without_rejecting_published_b_ancestry',
]
integrated_recovery_evidence = {
    name: any(test_passed(line, name) for line in workspace_stdout.splitlines())
    for name in integrated_recovery_names
}
integrated_recovery_valid = all(integrated_recovery_evidence.values())
native_prechoice_names = [
    'native_prechoice_reconstructs_accepted_majority_and_keeps_stale_learner_clients_closed',
    'native_prechoice_competing_survivors_keep_highest_minority_b_and_original_receipt',
    'native_prechoice_coordinator_crash_preserves_an_actively_advancing_partial_input',
    'native_prechoice_two_groups_progress_with_removed_candidate_and_serve_inbound_queries',
    'native_prechoice_rejects_missing_or_valid_shortened_accepted_custody_without_repair',
]
native_membership_names = [
    'chosen_input_exact_proof_reuse_preserves_higher_promise_and_first_certificate_cold',
    'chosen_input_proof_reuse_rejects_damaged_acknowledged_input_before_copy_boundary',
    'chosen_input_proof_reuse_rereads_after_copy_and_public_learning_still_replays_full_input',
    'membership_bulk_input_copy_preserves_prefix_rejects_conflict_and_retries_without_new_wal',
    'membership_bound_input_valid_short_prefix_never_repairs_acknowledged_custody',
    'membership_unacknowledged_staged_prefix_still_resumes_after_cold_reopen',
    'authority_input_temporaries_preserve_foreign_targets_warm_and_cold',
    'authority_input_torn_regular_temporary_resumes_without_replacing_authority',
    'authority_input_routes_reject_aliases_before_recovery_or_append_warm_and_cold',
    'authority_input_missing_bound_files_are_not_recreated',
    'accepted_input_is_readable_before_choice_and_imports_a_surviving_certificate_cold',
    'retained_input_never_downgrades_missing_or_corrupt_bound_custody_to_absence',
    'partial_retained_input_is_an_error_and_a_conflicting_choice_cannot_replace_acceptance',
    'inbox_existing_rejects_missing_or_lost_identity_without_initialization',
    'inbox_existing_never_recreates_empty_or_torn_to_zero_identity',
    'inbox_existing_reuses_valid_identity_and_recovers_interrupted_later_selection',
    'inbox_existing_rejects_symlink_and_non_file_custody_without_touching_targets',
    'inbox_retains_complete_proof_across_later_selection_without_finishing_that_upload',
    'inbox_retained_lookup_rejects_missing_corrupt_and_incomplete_input',
    'real_tls_stage_imports_chosen_from_accepted_input_without_old_sender_inbox',
    'real_tls_stage_rejects_corrupt_accepted_custody_without_inbox_fallback_or_repair',
    'real_tls_stage_uses_finished_inbox_b_after_another_upload_selects_a',
    'real_tls_stage_does_not_repair_lost_or_corrupt_nonselected_inbox_b',
    'recovery_rotates_ready_groups_and_peers_without_waiting_for_backoff',
    'staged_rejection_after_one_completed_provision_does_not_loop_upload_or_claim_repair',
    'incremental_authority_batches_preserve_exact_cross_snapshot_order_and_byte_bounds',
    'real_tls_managed_recovery_polls_both_survivors_while_transport_waits',
    'managed_recovery_keeps_exact_proposal_guard_after_caught_materializer_unwind',
    'strict_membership_recovery_paths_do_not_create_a_missing_decision',
    'managed_identity_is_monotonic_and_binding_never_downgrades_policy',
    'managed_identity_rejects_duplicate_unknown_and_noncanonical_policy_fields',
    'managed_identity_rejects_foreign_temporary_paths_without_touching_policy',
    'managed_policy_blocks_flag_removal_and_never_recreates_missing_custody',
    'native_managed_membership_recovers_without_candidate_and_never_admits_clients_from_local_policy',
    'native_client_rechecks_membership_ancestry_after_live_ordinary_installation',
    'native_published_voters_require_admission_even_with_unmanaged_v1_identity',
]
native_membership_evidence = {
    name: any(test_passed(line, name) for line in workspace_stdout.splitlines())
    for name in native_membership_names
}
native_membership_profiles = {
    profile: {name: False for name in native_membership_names[-3:] + native_prechoice_names}
    for profile in ("debug", "release")
}
native_membership_trace = Path(root, "native-membership.trace.fozzy")
if native_membership_trace.exists():
    for fields in json.loads(native_membership_trace.read_text()).get("decisions", []):
        if fields.get("kind") != "proc_spawn" or fields.get("exit_code") != 0:
            continue
        args = fields.get("args", [])
        profile = "release" if "--release" in args else "debug"
        for name in native_membership_profiles[profile]:
            if name in args and any(test_passed(line, name) for line in fields.get("stdout", "").splitlines()):
                native_membership_profiles[profile][name] = True
native_membership_valid = all(native_membership_evidence.values()) and all(
    all(results.values()) for results in native_membership_profiles.values()
)
summary = {
    "all_pass": not failed.strip() and before == after and workloads_valid and automatic_valid and sessions_valid and kv_batches_valid and streams_valid and indexed_valid and membership_approval_valid and membership_decision_valid and membership_peer_valid and membership_staging_valid and uncertain_begin_valid and client_transport_valid and membership_activation_valid and idle_application_valid and primary_recovery_valid and membership_capacity_valid and learner_promotion_valid and native_role_valid and publication_recovery_valid and peer_publication_valid and native_membership_valid and prechoice_prerequisites_valid and integrated_recovery_valid,
    "source_unchanged": before == after,
    "source_sha256_before": before,
    "source_sha256_after": after,
    "seed": int(seed),
    "passed_domains": passed.split(),
    "failed_domains": failed.split(),
    "trace_directory": root,
    "integrated_recovery_evidence_valid": integrated_recovery_valid,
    "integrated_recovery_regressions": integrated_recovery_evidence,
    "integrated_recovery_scope": "automatic pre-choice scheduling, learner custody, retry-safe directory publication and replay-derived control state; managed control initiation/admission and deployment qualification remain unfinished",
    "prechoice_prerequisites_evidence_valid": prechoice_prerequisites_valid,
    "prechoice_prerequisites_regressions": prechoice_prerequisite_evidence,
    "prechoice_prerequisites_scope": "strict unpublished empty factory continuation, typed decision and input progress, authenticated bounded higher-round operations and certified input import; native coordinator coverage is checked separately",
    "native_membership_evidence_valid": native_membership_valid,
    "native_membership_regressions": native_membership_evidence,
    "native_membership_profiles": native_membership_profiles,
    "native_membership_scope": "existing chosen-transition recovery through native peers, retained accepted/input custody, bounded transport scheduling, managed role restart, durable client-admission denial and replay-derived startup/runtime ancestry admission; pre-choice native profiles are required separately; repeated transitions and integrated replicated control admission remain unfinished",
    "workload_evidence_valid": workloads_valid,
    "workload_metrics": metrics,
    "automatic_client_metrics": automatic_client_metrics,
    "automatic_client_evidence_valid": automatic_valid,
    "client_transport_evidence_valid": client_transport_valid,
    "client_transport_workspace": client_transport_workspace,
    "client_transport_profile_runs": client_transport_profile_runs,
    "client_transport_scope": "real native SQL commit before client timeout and empty TLS response; identical retries, exact result across all daemon restarts and no additional commits",
    "uncertain_sql_begin_evidence_valid": uncertain_begin_valid,
    "uncertain_sql_begin_workspace": uncertain_begin_workspace,
    "uncertain_sql_begin_profile_runs": uncertain_begin_profile_runs,
    "uncertain_sql_begin_scope": "lost caller observation after real TLS peer commit/materialization; exact retry and changed-payload rejection across two peer restarts; not injected TCP packet loss",
    "learner_promotion_evidence_valid": learner_promotion_valid,
    "learner_promotion_regressions": learner_promotion_evidence,
    "learner_promotion_scope": "owned resumable local voter publication, cold provenance, later-history preservation and real process kills",
    "publication_recovery_evidence_valid": publication_recovery_valid,
    "publication_recovery_regressions": publication_recovery_evidence,
    "publication_recovery_scope": "replay-proven historical activation, durable staged-quorum input custody and semantic publication receipt codecs",
    "peer_publication_evidence_valid": peer_publication_valid,
    "peer_publication_regressions": peer_publication_evidence,
    "peer_publication_scope": "authenticated publish/query RPC, owned live learner handoff, current-prefix application and historical recovery after candidate loss through real TLS peer processes",
    "native_role_evidence_valid": native_role_valid,
    "native_role_regressions": native_role_evidence,
    "native_role_scope": "real native TLS peer restart from certified local promotion, non-voting partial claims, fail-closed provenance and actual recovered election budgets",
    "membership_capacity_evidence_valid": membership_capacity_valid,
    "membership_capacity_regressions": membership_capacity_evidence,
    "membership_capacity_scope": "bounded complete proofs rejected before new old-voter fences; historical fence decoding/retry preserved",
    "primary_recovery_evidence_valid": primary_recovery_valid,
    "primary_recovery_regressions": primary_recovery_evidence,
    "primary_recovery_scope": "ordinary quorum election above superseding durable promises; unchanged native takeover deadlines and workload",
    "idle_application_evidence_valid": idle_application_valid,
    "idle_application_regressions": idle_application_evidence,
    "idle_application_scope": "one owned application writer survives idle polls; protocol ownership is released and reopens against exact history, current authority, transport and per-request authorization",
    "membership_activation_evidence_valid": membership_activation_valid,
    "membership_activation_regressions": membership_activation_evidence,
    "membership_activation_scope": "proof-bound active VSR generation publication and replay with owned learner promotion",
    "membership_staging_evidence_valid": membership_staging_valid,
    "membership_staging_regressions": membership_staging_evidence,
    "membership_staging_scope": "sealed chosen prefixes and boot-scoped materialization receipts",
    "membership_peer_evidence_valid": membership_peer_valid,
    "membership_peer_regressions": membership_peer_evidence,
    "membership_peer_scope": "authenticated peer plan consensus with anchored authority",
    "membership_decision_evidence_valid": membership_decision_valid,
    "membership_decision_regressions": membership_decision_evidence,
    "membership_decision_scope": "transition-only library plan consensus with peer and staged-kernel integration",
    "membership_approval_evidence_valid": membership_approval_valid,
    "membership_approval_regressions": membership_approval_evidence,
    "membership_approval_scope": "old-quorum approval evidence",
    "sql_index_evidence_valid": indexed_valid,
    "sql_index_regressions": indexed_evidence,
    "sql_stream_evidence_valid": streams_valid,
    "sql_stream_regressions": stream_evidence,
    "sql_stream_metrics": stream_metrics,
    "kv_atomic_batch_evidence_valid": kv_batches_valid,
    "kv_atomic_batch_regressions": kv_batch_evidence,
    "sql_session_evidence_valid": sessions_valid,
    "sql_session_regressions": session_evidence,
    "coverage_per_passing_domain": [
        "five-run doctor", "strict deterministic test", "real host recorded run",
        "strict trace verification", "replay", "CI"
    ],
}
Path(root, "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
print(json.dumps(summary, indent=2))
raise SystemExit(0 if summary["all_pass"] else 1)
