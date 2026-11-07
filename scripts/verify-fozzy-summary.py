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
    'cold_recovered_valid_changed_stage_preserves_semantic_digest_refusal',
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
    'real_tls_cached_learner_ack_rejects_lost_custody_until_validated_restart',
    'managed_anchor_validation_survives_temporarily_taken_recovery',
    'real_tls_managed_cached_stage_rejects_lost_learner_without_receipt_or_application_work',
    'real_tls_managed_cached_stage_survives_auxiliary_journal_loss_but_not_hosted_loss',
    'real_tls_managed_changed_learner_revalidates_original_authority_before_caching',
    'real_tls_managed_chosen_b_accepts_matching_learner_commit_and_original_receipt',
    'real_tls_managed_new_choice_revalidates_b_even_when_learner_token_is_unchanged',
    'real_tls_managed_promise_and_accept_reject_lost_learner_before_journal_mutation',
    'real_tls_managed_decision_retries_reject_warm_journal_loss_before_durable_ack',
    'managed_startup_handoff_cannot_substitute_a_for_distinct_chosen_b',
    'managed_startup_handoff_reuses_exact_a_without_rereading_its_wal',
    'real_tls_managed_historical_publication_survives_auxiliary_journal_loss_only',
    'real_tls_managed_receiver_retains_published_ancestry_after_ordinary_view_and_promise',
    'real_tls_fresh_publication_rechecks_chosen_input_with_a_healthy_cached_stage',
    'owned_groups_preserve_borrowed_and_single_append_wal_bytes_across_rotation',
    'owned_producer_error_is_not_eof_and_late_failure_fences_actual_prefix',
    'owned_group_releases_each_payload_before_polling_next_and_fences_unwind',
    'owned_groups_keep_real_sync_fault_and_rotation_boundaries',
    'owned_empty_retry_renews_closed_and_active_bytes_after_complete_visible_error',
    'same_clock_fixture_drop_preserves_another_owned_store',
    'local_copy_stream_preserves_exact_packing_bytes_for_partial_and_empty_suffixes',
    'local_copy_late_encoding_error_fences_store_and_existing_caller_latch',
    'local_copy_failure_guard_survives_unwind_without_reenabling_caller',
    'local_copy_complete_suffix_still_reaches_closed_and_active_renewal_boundaries',
    'unsealed_complete_visible_source_and_selected_groups_renew_before_stage_seal',
    'complete_local_copy_renewal_failure_prevents_meta_for_source_and_selected',
    'real_tls_cached_stage_rejects_own_history_loss_before_callback_and_stays_latched',
    'real_tls_stage_loss_cannot_block_independently_backed_historical_publication',
    'immutable_capture_rejects_equal_byte_replacement_before_scanning',
    'immutable_custody_rejects_replacement_alias_and_hardlink_without_reading_wal_on_validation',
    'immutable_recovery_never_repairs_torn_active_tail_or_creates_missing_sidecars',
    'fixed_metadata_preserves_default_missing_pointer_and_valid_installed_generation',
    'fixed_pointer_seal_and_marker_reject_sparse_trailing_truncated_and_corrupt_files_without_repair',
    'fixed_publication_reader_bounds_even_an_unending_source_to_one_extra_byte',
    'fixed_publication_reader_propagates_io_failure_without_returning_partial_success',
    'cached_published_learner_success_requires_outer_and_inner_custody_before_mutation',
    'unpublished_token_error_does_not_poison_later_valid_publication_and_retries',
    'membership_decision_exact_retries_reject_lost_journal_with_complete_input_intact',
    'membership_decision_metadata_preserves_historical_acceptance_without_scrubbing_input_data',
    'membership_decision_post_append_custody_failure_returns_no_ack_or_logical_swap',
    'membership_decision_read_only_failure_latches_even_after_exact_path_restoration',
    'each_stage_effect_rejects_its_first_custody_fault_before_callback_or_target_publication',
    'sealed_stage_cold_recovery_never_truncates_source_or_selected_torn_tail',
    'stage_callback_and_publication_hook_cannot_repair_or_acknowledge_lost_custody',
    'stage_own_receipt_and_publication_transitions_preserve_first_proof_and_need_no_wal_read',
    'stage_report_first_observes_each_owned_loss_and_stays_failed_after_original_inode_restore',
    'stage_root_replacement_and_retained_publication_loss_require_fresh_owner',
    'actual_promoted_pointer_and_owner_fixed_metadata_reject_damage_and_preserve_cold_replay',
    'stage_lock_creation_never_follows_a_symlink_substituted_after_precheck',
    'stage_lock_rejects_existing_symlink_and_hardlink_without_changing_target',
    'stage_owner_keeps_original_descriptor_locked_and_rejects_renamed_path_replacement',
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
bounded_control_names = [
    'accepted_business_rejections_reconcile_in_order_without_fabricating_a_committed_intent',
    'accepted_callback_error_still_latches_root_active_and_inner_custody_loss',
    'active_renewal_changed_torn_tail_never_scans_or_repairs_and_restoration_stays_fenced',
    'active_renewal_empty_and_none_policy_never_claim_a_data_flush_or_adopt_foreign_stamp',
    'active_renewal_incomplete_bytes_on_held_inode_reject_without_tail_repair',
    'active_renewal_injected_sync_unwind_keeps_owner_fenced_without_claiming_syscall_completion',
    'active_renewal_oversized_manifest_rejects_without_parser_read_refresh_or_repair',
    'active_renewal_real_sync_boundary_failure_returns_no_success_and_cannot_clear_prior_latch',
    'active_renewal_syncs_only_actual_nonempty_descriptor_and_preserves_cursor_without_reads',
    'actual_published_learner_relays_after_source_loss_and_cold_partial_resume',
    'application_borrow_restores_exact_control_owner_on_error_panic_and_collision',
    'bounded_active_renewal_rejects_new_incomplete_tail_without_repair_or_marker',
    'bounded_append_rejects_mid_call_torn_tail_without_repair_or_ack',
    'bounded_caller_budget_tightens_every_frame_before_payload_allocation',
    'bounded_captured_pointer_or_held_writer_lock_loss_after_replay_cannot_complete_marker',
    'bounded_cold_admission_cannot_certify_replacement_or_mask_it_as_capacity',
    'bounded_cold_begin_rejects_tail_loss_between_closed_and_active_renewal_without_repair',
    'bounded_cold_published_open_never_resolves_late_real_generation_or_truncates_marker_alias',
    'bounded_compacted_prefix_is_typed_unsupported_without_payload_reads',
    'bounded_compacted_storage_is_explicitly_unsupported_before_semantic_replay',
    'bounded_custody_change_overrides_visitor_error_stop_and_admission_limit',
    'bounded_empty_existing_store_never_creates_bootstrap_and_owned_root_alias_rejects',
    'bounded_empty_factory_completes_post_manifest_and_post_sidecar_crash_prefixes',
    'bounded_empty_factory_never_repairs_nonempty_or_foreign_shapes',
    'bounded_exact_physical_limits_succeed_and_one_under_stops_before_replay_without_mutation',
    'bounded_existing_rejects_unmigrated_legacy_then_reuses_actual_persisted_migration',
    'bounded_fenced_voter_replays_exact_existing_membership_fence_without_unfencing',
    'bounded_flat_visible_suffix_renews_before_owner_and_preserves_wal_bytes',
    'bounded_flat_voter_preserves_committed_accepted_receipts_and_later_real_commit',
    'bounded_foreign_or_aliased_publication_temporary_is_never_truncated_or_followed',
    'bounded_handoff_keeps_original_closed_inode_across_same_store_move',
    'bounded_installed_missing_damaged_or_aliased_active_metadata_never_repairs',
    'bounded_installed_visible_suffix_renews_before_marker_and_failed_hook_cannot_advance_it',
    'bounded_installed_voter_preserves_sealed_prefix_receipt_tail_and_later_promise',
    'bounded_invalid_policy_is_rejected_before_any_namespace_access',
    'bounded_live_owner_excludes_duplicate_and_latches_observed_inner_loss_after_original_inode_restore',
    'bounded_membership_installed_old_voter_retains_certified_lineage_without_promotion_claim',
    'bounded_missing_owned_files_and_wrong_pg_never_create_or_repair',
    'bounded_owned_custody_rejects_lost_or_replaced_actual_held_sidecars',
    'bounded_prefix_stop_reads_only_the_complete_selected_prefix',
    'bounded_prepared_marker_replacement_cannot_publish_or_modify_foreign_file',
    'bounded_promoted_voter_recovers_actual_role_tail_and_receipts_without_source_or_learner',
    'bounded_promotion_requires_existing_exact_bounded_owner_and_active_pointer',
    'bounded_provisional_completion_does_not_recreate_a_nonempty_wal_sidecar',
    'bounded_published_flat_absence_remains_live_and_observed_loss_is_sticky',
    'bounded_published_marker_error_returns_no_owner_and_cold_retry_retains_exact_history',
    'bounded_published_open_refuses_the_same_missing_sidecar_the_unbound_factory_can_complete',
    'bounded_record_and_aggregate_caps_count_filtered_frames_before_allocation',
    'bounded_renewal_custody_failure_returns_no_owner_and_same_inode_cold_retry_succeeds',
    'bounded_returned_installed_owner_latches_observed_pointer_loss_with_same_inode_restore',
    'bounded_shared_parser_preserves_supported_checksum_formats_and_rejects_bad_headers',
    'bounded_shared_recovery_matches_generic_mixed_records_across_rotations',
    'bounded_size_helpers_and_nonzero_policy_reject_overflow_without_io',
    'bounded_sparse_frame_and_manifest_reject_at_budget_before_checksum_or_replay',
    'bounded_sparse_record_and_manifest_limits_precede_payload_allocation',
    'bounded_stage_finish_never_resolves_late_real_generation_or_truncates_marker_alias',
    'bounded_strict_recovery_preserves_checksum_chain_and_epoch_errors',
    'bounded_strict_writer_rejects_replaced_name_of_reentrant_held_lock',
    'bounded_strict_writer_rejects_torn_tail_without_repair_then_generic_can_repair',
    'bounded_strict_writer_requires_existing_regular_data_and_sidecars',
    'bounded_strict_writer_uses_recovered_cursor_and_preserves_append_sync_policy',
    'bounded_valid_active_completes_only_missing_marker_and_canonical_temporary_prefix',
    'bounded_writer_admission_rejection_cannot_create_a_partial_success',
    'bounds_fail_before_append_and_exact_duplicate_retries_do_not_reallocate_log_indices',
    'cached_prefix_callback_error_and_unwind_obey_same_physical_custody_contract',
    'callback_success_or_error_cannot_hide_replaced_prior_or_candidate_custody',
    'canonical_outcome_distinguishes_exact_rejected_retry_from_uncommitted_request_conflict',
    'clean_semantic_rejection_retains_prior_authority_and_exact_resumable_bytes',
    'cold_bounded_resume_never_repairs_missing_sidecars_and_explicit_cancel_allows_fresh_copy',
    'committed_callback_error_still_latches_root_active_and_inner_custody_loss',
    'committed_origin_never_moves_on_retry_conflict_reopen_or_publication_and_expires_after_finalize',
    'configured_catalog_rejects_first_fence_authority_and_stage_before_effects',
    'configured_genesis_receive_is_exact_and_does_not_expose_an_empty_control_state',
    'control_only_constructor_rejects_invalid_policy_before_touching_absent_root',
    'control_policy_before_open_refuses_raw_enrollment_and_bounded_cold_history',
    'control_provisioning_rejects_foreign_bootstrap_inventory_and_peer_fingerprints',
    'current_active_begin_cannot_drop_lost_or_replaced_warm_stage_custody',
    'exact_published_prefix_retry_runs_authoritative_control_validator_again',
    'exhausted_receipt_budget_blocks_fresh_writer_but_preserves_accepted_capacity_outcome',
    'existing_decision_is_observable_but_only_exact_enrolled_recovery_can_mutate',
    'finalization_reservation_cannot_exhaust_or_wrap_qualified_history_capacity',
    'genesis_delivery_rejects_actual_certified_control_membership_change',
    'genesis_request_rejects_follower_pending_promise_and_foreign_provisioning_without_append',
    'healthy_committed_and_accepted_callback_errors_preserve_the_same_observation',
    'healthy_current_active_begin_may_reclaim_provisional_stage_after_validation',
    'held_lock_metadata_stays_with_original_inode_after_named_replacement',
    'historical_control_transfer_preserves_begin_origin_and_retained_validation_under_promise',
    'incoming_prepare_checks_exact_index_operation_domain_and_current_authority_without_writes',
    'installed_commit_without_ordinary_boundary_requires_installation_export',
    'installed_invalid_control_prefix_stays_pending_and_cannot_ack_finish_or_probe',
    'intent_operation_digest_distinguishes_operation_ids_at_the_same_index_and_command',
    'invalid_peer_certificate_rejects_before_owner_handoff_and_a_valid_begin_can_follow',
    'learner_publication_guard_rejects_replaced_zero_byte_held_lock',
    'malformed_or_foreign_accepted_control_commands_are_never_repaired_or_skipped',
    'newer_certified_genesis_replaces_older_accepted_id_without_changing_configured_catalog',
    'ordinary_election_materializes_actual_control_owner_and_receiver_before_confirmation',
    'ordinary_reprepare_preserves_full_operation_origin_but_not_the_old_live_token',
    'original_bootstrap_prefix_is_certified_but_legacy_reconfigure_remains_rejected',
    'owned_group_actual_sync_failure_or_producer_unwind_never_returns_authorized_custody',
    'owned_group_clean_first_input_error_keeps_original_owner_but_late_error_is_not_eof',
    'owned_group_empty_retry_renews_active_and_none_policy_never_claims_durability',
    'owned_group_iterator_drop_occurs_before_final_sync_and_its_loss_cannot_be_adopted',
    'owned_group_late_input_error_checks_drop_custody_and_returns_physical_failure',
    'owned_group_matches_sequential_bytes_and_returns_actual_tail_and_authorized_custody',
    'owned_group_observed_physical_fault_stays_failed_when_iterator_drop_restores_original_inode',
    'owned_group_payload_drop_fault_is_detected_before_next_producer_poll',
    'owned_group_preexisting_torn_tail_or_oversized_manifest_never_enters_refresh_or_repair',
    'owned_group_releases_each_owned_payload_before_polling_the_next',
    'owned_rotation_manifest_publication_failure_fences_live_owner_and_exact_empty_orphan_cold_retry_works',
    'owned_rotation_rejects_nonempty_or_aliased_provisional_names_without_foreign_truncation',
    'owned_rotation_resumes_only_empty_next_files_and_exact_canonical_manifest_prefix',
    'pending_binding_survives_a_recovery_promise_only_after_fresh_owned_replay',
    'promised_source_exports_earlier_prefix_without_releasing_promise_or_accepted_suffix',
    'published_control_observation_preserves_first_begin_and_expires_with_real_learner_custody',
    'published_replay_borrows_only_committed_history_and_preserves_cold_lineage',
    'published_replay_checks_outer_custody_at_completion_and_never_recreates_it',
    'real_tls_accepted_genesis_survives_original_primary_loss_and_certified_election',
    'real_tls_accepted_only_follower_cannot_report_local_primary_commit_then_exact_retry_repairs',
    'real_tls_accepted_only_foreign_command_is_rejected_before_control_publication',
    'real_tls_begin_required_rejects_malformed_foreign_and_uncorrelated_responses',
    'real_tls_canonical_rejected_command_and_accepted_receipt_conflict_remain_valid_history',
    'real_tls_cold_accepted_genesis_resumes_without_request_and_empty_poll_never_appends',
    'real_tls_control_bounded_begin_rejects_oversize_then_accepts_valid_descriptor',
    'real_tls_control_cold_record_and_finish_require_begin_even_for_active_id',
    'real_tls_control_genesis_begin_commit_and_transfer_do_not_enter_fabric',
    'real_tls_control_learner_finish_is_observation_only_and_rechecks_same_id_custody',
    'real_tls_control_observation_uses_historical_custody_under_pending_ordinary_promise',
    'real_tls_control_only_constructor_enforces_cold_context_and_semantic_policy',
    'real_tls_control_report_rejects_foreign_identity_mixed_key_and_noncanonical_frames',
    'real_tls_fresh_genesis_distinguishes_accepted_followers_then_cold_recollects_exact_three_roots',
    'real_tls_genesis_conflicting_minority_requires_certified_installation_then_preserves_selected_operation',
    'real_tls_genesis_goal_reconciles_rejected_and_pending_suffix_before_delivery_without_duplicate_genesis',
    'real_tls_genesis_late_prepare_reply_cannot_commit_after_source_view_changes',
    'real_tls_genesis_late_prepare_reply_cannot_revive_lost_then_restored_source',
    'real_tls_genesis_lost_durable_commit_reply_recollects_without_duplicate_operation',
    'real_tls_genesis_lost_durable_prepare_reply_retries_same_operation_before_commit',
    'real_tls_genesis_request_repeats_preserve_flight_and_incompatible_goals_do_not_reset_it',
    'real_tls_invalid_control_commit_cannot_replace_published_active',
    'real_tls_late_prepare_after_local_commit_delivers_to_surviving_majority',
    'real_tls_primary_only_genesis_commit_survives_original_source_loss_from_accepted_survivor',
    'real_tls_rejected_accepted_suffix_reconciles_before_new_begin_and_retries_remain_durable_receipts',
    'real_tls_successful_prepare_reply_is_rejected_after_local_view_changes_before_consumption',
    'real_tls_successful_prepare_reply_never_revives_lost_then_restored_source_custody',
    'real_tls_writer_recovers_commit_reply_lost_after_actual_durable_delivery',
    'real_tls_writer_reprepares_earliest_stable_operation_before_new_request_and_keeps_origin',
    'same_id_bounded_begin_binds_raw_publication_flat_guard_after_semantic_error',
    'same_id_bounded_begin_binds_raw_publication_flat_guard_after_successful_callback',
    'selected_foreign_authority_upload_cannot_mutate_under_another_enrolled_journal',
    'shared_bounded_open_checks_finite_policy_before_provisional_factory_effects',
    'strict_cold_open_rejects_budget_loss_or_alias_without_recreating_authority',
    'valid_candidate_cannot_erase_prior_receipt_and_clean_rejection_can_resume_or_switch',
    'validator_unwind_leaves_live_owner_fenced_and_cold_prior_publication_intact',
    'writer_checks_actual_control_primary_and_fresh_target_before_new_append',
    'writer_never_returns_cached_observation_after_live_control_custody_loss',
    'fresh_store_open_uses_one_full_route_plus_final_parent_while_existing_renews_full_route',
    'fresh_route_replacement_never_adopts_new_ancestors_and_cold_retry_uses_original_locks',
    'fresh_final_segment_and_lock_substitutions_return_no_owner_after_real_publication',
    'fresh_writer_descriptor_cannot_hide_behind_restored_original_segment_name',
    'fresh_same_target_alias_replacement_is_not_new_publication_authority',
    'fresh_publication_rechecks_current_directory_access_without_replacing_original_custody',
    'fresh_publication_errors_and_unwinds_release_original_locks_without_residue_repair',
    'fresh_store_open_uncertainty_after_each_real_sync_returns_no_owner_and_retries_full_route',
    'existing_store_open_uncertainty_releases_original_locks_and_preserves_every_acknowledged_byte',
    'torn_unacknowledged_tail_is_repaired_before_final_uncertainty_without_losing_acknowledged_prefix',
    'double_alias_store_renews_resolved_directory_before_recovery_and_final_target_routes',
    'discard_is_read_only_bounded_inventory_then_two_phase_without_recovery_or_lock_creation',
    'discard_accepts_actual_factory_and_owned_rotation_interruption_prefixes_without_parsing_wal',
    'discard_exact_segment_budget_excludes_metadata_and_limits_refuse_without_mutation',
    'discard_declared_name_and_temporary_metadata_caps_remain_independent_of_segment_data',
    'every_discard_unlink_rmdir_and_sync_boundary_failure_preserves_recognizable_cold_remainder',
    'discard_barriers_keep_manifest_last_and_factory_temporary_before_its_required_segment',
    'discard_callback_error_still_observes_physical_loss_and_never_deletes_through_replacement',
    'discard_clean_callback_refusal_precedes_effects_and_callback_unwind_returns_no_capability',
    'discard_rejects_foreign_aliased_hardlinked_nonempty_sidecars_and_rotation_residue_before_effects',
    'discard_rejects_root_replacement_or_new_name_after_inventory_without_adopting_it',
    'discard_remainder_refuses_surviving_auxiliary_and_reappeared_removed_children',
    'missing_declared_files_are_discard_only_while_manifestless_sidecars_and_auxiliaries_reject',
    'actual_unlink_permission_failure_returns_no_remainder_and_leaves_anchor_and_children',
    'complete_namespace_preflight_rejects_unknown_or_aliased_obsolete_content_before_any_deletion',
    'malformed_last_candidate_and_over_capacity_refuse_before_retiring_a_healthy_stage',
    'bounded_cold_existing_factory_is_not_repaired_into_cursor_and_explicit_active_begin_cancels_it',
    'healthy_semantic_refusal_keeps_actual_completed_writer_and_exact_duplicate_append',
    'rejected_owner_original_sidecar_loss_blocks_abandonment_and_restored_inode_does_not_clear_failure',
    'import_callback_namespace_mutation_dominates_ok_or_semantic_error_without_publication',
    'cold_lost_finish_reply_recollects_actual_successor_without_republication_or_old_history_loss',
    'read_only_capacity_refusal_keeps_live_stage_and_allows_exact_policy_retry',
    'smaller_byte_policy_cannot_delete_a_live_stage_to_fit_the_active_history',
    'actual_caller_unlink_denial_keeps_prior_publication_and_fences_after_permission_restore',
    'every_actual_caller_disposal_boundary_reopens_the_exact_current_publication_and_completes_cancel',
    'actual_successor_pointer_barrier_precedes_old_descriptor_and_candidate_removal',
    'interrupted_replacement_retains_rejected_b_and_requires_explicit_cancel_of_cold_factory_c',
    'bounded_ordinary_api_does_not_claim_cold_detection_of_whole_unrecorded_stage_loss',
    'successful_replacement_reaches_three_validated_stores_with_original_owners_still_locked',
    'three_slot_existing_resume_preflights_every_candidate_before_cleanup_and_then_succeeds',
    'real_owned_rotation_name_failure_is_strictly_refused_then_explicitly_cancelled_by_current_active',
    'recovered_admin_retains_the_original_writer_and_exact_replayed_generation',
    'recovered_admin_refuses_framed_invalid_domain_event_without_rewriting_wal',
    'owned_full_recovery_reads_real_mixed_bytes_and_preserves_writer_custody',
    'owned_full_recovery_rejects_replaced_owner_files_before_payload_reads',
    'owned_full_recovery_original_custody_failure_overrides_admission_limit',
    'owned_full_recovery_checks_actual_checksum_even_when_metadata_still_matches',
    'warm_promotion_retains_publication_boundaries_and_live_writer_exclusion',
    'warm_promotion_rechecks_actual_pointer_seal_and_owner_after_published_hook',
    'warm_promotion_rejects_same_byte_owner_replacements_across_hooks',
    'protocol_open_retains_metadata_writer_generation_through_receipt_recovery',
    'protocol_open_preserves_each_missing_metadata_prefix_and_never_reappends_on_reopen',
    'protocol_open_keeps_placement_before_corrupt_policy_failure_and_refuses_repeatedly',
    'protocol_open_reused_receipts_still_reject_duplicate_physical_commit_records',
    'real_tls_stage_resumes_zero_staged_prefix_from_complete_inbox',
    'real_tls_stage_resumes_nonempty_staged_prefix_from_complete_inbox',
    'real_tls_partial_stage_refuses_missing_authority',
    'real_tls_partial_stage_refuses_short_authority',
    'real_tls_partial_stage_refuses_corrupt_authority',
    'real_tls_partial_stage_refuses_corrupt_staged_prefix',
    'real_tls_stage_refuses_short_accepted_despite_complete_inbox',
    'real_tls_stage_refuses_missing_chosen_despite_complete_inbox',
    'staged_source_supplier_error_preserves_healthy_owner_and_resolution_is_read_only',
    'staged_source_supplier_swap_success_and_error_fence_original_owner_after_restoration',
    'staged_source_supplier_unwind_fences_original_owner_without_durable_choice',
    'absent_stage_source_pins_authority_across_supplier_success_and_error',
    'stage_source_requires_original_namespace_lock_for_every_supplier_path',
    'staged_source_rejects_real_maintenance_and_nonwal_frames_before_supplier',
    'complete_stage_source_cannot_bypass_original_namespace_owner',
    'stage_copy_uses_original_opened_generation_without_repairing_replacement',
    'stage_copy_reuses_actual_open_recovery_for_empty_partial_and_complete_prefixes',
    'stage_copy_open_recovery_keeps_corrupt_prefix_refusal_and_sticky_failure',
    'stage_copy_rejects_same_byte_manifest_replacement_after_actual_open',
    'decision_copy_retains_opening_manifest_custody_and_fences_after_restoration',
    'decision_copy_resumes_staged_prefixes_and_preserves_cold_choice',
    'decision_copy_never_repairs_corrupt_acknowledged_input',
    'ordinary_outer_lock_loss_is_observed_and_original_inode_restore_stays_fenced',
    'strict_outer_lock_loss_is_observed_and_original_inode_restore_stays_fenced',
    'ordinary_inner_lock_loss_is_observed_and_original_inode_restore_stays_fenced',
    'strict_outer_lock_equal_byte_replacement_is_not_recaptured',
    'ordinary_identity_equal_byte_replacement_is_not_recaptured',
    'strict_active_equal_byte_replacement_is_not_recaptured',
    'ordinary_descriptor_equal_byte_replacement_is_not_recaptured',
    'unpublished_outer_owner_loss_fences_before_begin_can_create_a_candidate',
    'ordinary_cold_published_missing_manifest_is_not_recreated',
    'ordinary_cold_published_missing_writer_lock_is_not_recreated',
    'ordinary_cold_published_torn_tail_is_rejected_without_truncation',
    'bounded_callback_success_cannot_adopt_replaced_active',
    'bounded_callback_semantic_error_cannot_hide_replaced_active',
    'ordinary_and_bounded_publications_renew_only_their_actual_active_transition',
    'ordinary_original_root_identity_survives_equal_byte_tree_substitution',
    'ordinary_original_candidate_identity_survives_equal_byte_tree_substitution',
    'existing_export_batch_observation_cannot_bypass_lost_outer_exclusion',
    'ordinary_cold_published_missing_outer_lock_is_not_recreated',
    'ordinary_transfer_preserves_actual_view_installation_replay_in_its_root_store',
    'ordinary_finish_refuses_authentic_generation_substitution_for_its_retained_store',
    'ordinary_complete_finish_torn_tail_never_repairs_acknowledged_bytes_or_clears_fence',
    'ordinary_complete_finish_keeps_original_descriptor_through_same_byte_replacement',
    'initial_selection_precedes_cursor_and_same_owner_publication_handoff',
    'selected_ack_zero_never_repairs_missing_sidecar_warm_or_cold',
    'uncertain_selection_error_is_sticky_after_original_identity_restored',
    'actual_pin_error_cold_resume_requires_same_complete_candidate',
    'missing_pinned_candidate_and_missing_published_active_never_reinitialize',
    'clean_semantic_rejection_then_aba_replacement_rejects_old_generations',
    'callback_panic_keeps_initial_owner_fenced_without_active',
    'selected_cleanup_refuses_unknown_or_alias_and_preserves_current',
    'final_restriction_error_never_returns_owner_and_cold_resume_obeys_actual_native_state',
    'initial_replacement_generation_overflow_has_no_effect_and_preserves_exact_selected_owner',
    'selected_cleanup_crash_retries_same_requested_id_without_third_candidate_or_aba_alias',
    'initial_handoff_rejects_same_bytes_closed_inode_replacement_before_native_pin',
    'replayed_initial_refusal_retains_unpublished_owner_and_original_transfer_guards',
    'initial_original_candidate_directory_loss_stays_fenced_after_exact_restoration',
    'initial_consumed_publication_keeps_original_outer_lock_proof',
    'initial_consumed_publication_keeps_original_active_proof',
    'initial_consumed_publication_keeps_original_candidate_proof',
    'ordinary_duplicate_exact_and_semantic_mismatch_keep_healthy_owner_and_bytes',
    'ordinary_duplicate_mid_call_manifest_replacement_never_adopts_new_custody',
    'ordinary_duplicate_torn_tail_refuses_without_repair_and_restoration_stays_fenced',
    'ordinary_cold_begin_after_real_renewal_loss_refuses_count_and_restoration_stays_fenced',
    'ordinary_cold_begin_after_real_renewal_unwind_fences_without_ack',
    'ordinary_cold_begin_changed_tail_between_real_barriers_never_repairs',
    'ordinary_cold_begin_renews_before_count_then_continues_exact_history',
    'ordinary_cold_published_open_renews_legacy_data_before_owner_escape',
    'strict_cold_published_open_renews_legacy_data_before_owner_escape',
    'real_tls_initial_genesis_requires_native_barriers_and_cold_published_origin',
    'real_tls_initial_rejects_ordinary_history_missing_adapter_and_foreign_policy_before_effects',
    'actual_initial_source_is_bounded_and_invalidated_by_real_view_promise',
    'initial_reply_codec_rejects_other_request_policy_and_noncanonical_length',
    'real_tls_current_primary_two_relays_historical_genesis_one_after_original_source_loss',
    'real_tls_initial_finished_reply_loss_cold_retries_without_republication',
    'real_tls_initial_semantic_rejection_allows_exact_generation_replacement',
    'real_tls_old_initial_finish_observes_later_control_publication_without_rewind',
    'actual_initial_source_latches_closed_file_and_held_lock_loss_after_original_inode_restore',
    'actual_initial_delivery_and_export_cannot_be_used_on_another_healthy_owner',
    'initial_control_only_owner_checks_complete_policy_before_creating_a_root',
    'real_tls_initial_accepted_suffix_refuses_pin_and_retains_healthy_replacement',
    'real_tls_application_history_publishes_before_and_after_initial_control_handoff',
    'native_control_legacy_published_learner_requires_explicit_adoption_before_bounded_open',
    'native_control_existing_voter_uses_actual_bounded_owner_and_missing_manifest_is_not_initialized',
    'real_tls_native_pin_and_final_publication_uncertainty_resume_without_source_retarget',
    'real_tls_native_guard_brackets_no_transition_handlers_and_semantic_errors',
    'native_control_selected_and_pinned_missing_candidates_never_reenter_factory',
    'native_control_v3_roundtrip_preserves_managed_enrollment_and_refuses_aba_selection',
    'real_tls_native_published_origin_survives_later_control_history_and_cold_reopen',
    'real_tls_native_mixed_application_history_remains_separate_across_control_handoff',
    'native_control_invalid_provisioning_rejects_before_identity_or_reserved_root',
    'real_tls_native_initial_selection_pin_and_publication_follow_real_identity_barriers',
    'real_tls_native_selection_write_uncertainty_is_not_acknowledged_and_cold_resume_is_exact',
    'native_control_reservation_blocks_config_omission_raw_sync_and_legacy_binding',
    'native_control_v3_schema_and_explicit_legacy_migration_preserve_exact_policy',
    'native_identity_retains_the_actual_exclusive_peer_lock_and_monotonic_managed_set',
    'native_identity_schema_preserves_legacy_and_rejects_duplicate_null_noncanonical_fields',
    'native_identity_uncertain_publication_requires_cold_selection_and_never_promotes_pending',
    'native_identity_panic_and_observed_file_root_lock_loss_use_distinct_sticky_path',
    'native_identity_missing_or_aliased_acknowledged_metadata_is_not_recreated',
    'native_identity_prepared_inode_replacement_cannot_be_adopted_after_rename',
    'managed_identity_never_follows_or_replaces_foreign_temporary_paths',
    'native_identity_transition_schema_rejects_overflow_retarget_and_skipped_publication',
    'real_tls_native_changed_pending_selection_is_not_acknowledged_or_promoted_on_reopen',
    'native_identity_rejects_same_length_valid_pending_rewrite_before_rename',
    'native_identity_rejects_appended_pending_bytes_before_rename',
    'native_identity_rejects_same_inode_rewrite_hidden_by_rename_timestamp_refresh',
    'native_control_empty_custody_variants_reject_unknown_fields_without_changing_wire_shape',
    'native_fresh_voter_canonical_preflight_rejects_before_reservation',
    'native_fresh_voter_never_adopts_preexisting_control_paths',
    'native_fresh_voter_initializes_before_listening_and_cold_reopens_same_bootstrap',
    'native_fresh_voter_initialized_missing_storage_is_strict_and_noncreating',
    'native_fresh_follower_never_queues_genesis_and_primary_reconciles_same_accepted_tail',
    'native_fresh_initialized_promise_reopens_for_recovery_without_scheduling_old_primary',
    'native_fresh_voter_publication_faults_fence_warm_owner_and_cold_use_actual_final_role',
    'native_fresh_voter_unwind_fences_native_identity_without_exposing_a_voter',
    'native_fresh_voter_retains_original_peer_lock_through_actual_completion',
    'native_fresh_genesis_checks_native_custody_before_any_writer_mutation',
    'native_fresh_completion_rechecks_original_kernel_lock_after_native_publication',
    'native_fresh_genesis_rechecks_native_owner_after_actual_local_prepare',
    'native_fresh_voter_process_child',
    'native_fresh_three_process_tls_genesis_has_actual_reporter_quorum_and_strict_cold_roots',
    'application_history_source_retains_real_binding_and_existing_cell_without_native_creation',
    'application_history_source_refuses_misbound_missing_corrupt_and_lockless_binding',
    'history_source_native_markers_never_fall_back_to_healthy_application_binding',
    'history_source_control_configuration_and_reservation_cannot_use_application_route',
    'application_history_source_route_replacement_is_sticky_after_original_restoration',
    'application_history_source_identity_replacement_is_sticky_after_original_restoration',
    'application_history_source_binding_directory_and_lock_substitutions_remain_fenced',
    'application_history_source_marker_appearance_and_identity_unwind_fence_same_owner',
    'application_history_source_rejects_aliases_and_noncanonical_routes_without_effects',
    'learner_cli_transfers_historical_configurations_and_materializes_after_process_restart',
    'native_client_rechecks_membership_ancestry_after_live_ordinary_installation',
    'native_prechoice_competing_survivors_keep_highest_minority_b_and_original_receipt',
    'native_managed_membership_recovers_without_candidate_and_never_admits_clients_from_local_policy',
    'native_prechoice_reconstructs_accepted_majority_and_keeps_stale_learner_clients_closed',
    'native_prechoice_two_groups_progress_with_removed_candidate_and_serve_inbound_queries',
    'native_prechoice_coordinator_crash_preserves_an_actively_advancing_partial_input',
    'dedicated_control_voter_keeps_full_policy_with_exact_one_actual_owner',
    'dedicated_control_voter_rejects_foreign_actual_owner_and_full_catalog_tls_mismatch',
    'published_learner_token_rejects_missing_inner_directory_and_trailing_dot_root_alias',
    'finish_recomputes_digest_from_replaced_valid_live_stage_bytes',
    'cold_recovered_valid_changed_stage_preserves_semantic_digest_refusal',
    'finish_missing_live_stage_custody_does_not_recreate_it',
    'finish_revalidates_torn_live_tail_before_resumable_retry',
    'provisional_vsr_spec_uses_canonical_encoder_and_rejects_invalid_policy_before_io',
    'provisional_vsr_completion_consumes_same_actual_store_and_auxiliary_into_voter',
    'provisional_vsr_cold_reserved_prefixes_complete_ordinary_codec_bytes_without_replacement',
    'provisional_vsr_foreign_history_markers_aliases_and_hardlinks_are_not_repaired',
    'provisional_vsr_failed_or_unwound_callback_returns_no_owner_and_allows_only_cold_retry',
    'provisional_vsr_completion_rejects_original_file_substitution_for_ok_and_error_callbacks',
    'provisional_vsr_observed_custody_failure_stays_sticky_after_original_inode_restoration',
    'provisional_vsr_completion_reads_actual_bytes_after_callback_even_with_matching_metadata',
    'provisional_vsr_initialized_loss_uses_strict_existing_noncreating_recovery',
    'provisional_vsr_original_directory_and_auxiliary_guard_span_factory_boundaries',
    'provisional_vsr_process_child',
    'provisional_vsr_actual_process_kill_releases_nonvoting_owner_for_cold_reserved_resume',
    'provisional_bootstrap_every_physical_prefix_matches_ordinary_append_and_strict_replay',
    'provisional_bootstrap_actual_nonprefix_and_oversize_are_refused_without_repair',
    'provisional_bootstrap_each_factory_boundary_cold_continues_but_warm_failure_is_sticky',
    'provisional_bootstrap_every_manifest_temporary_prefix_completes_exact_bytes',
    'provisional_bootstrap_auxiliary_swap_and_hardlink_at_every_phase_fence_original_handle',
    'provisional_bootstrap_inventory_and_caps_refuse_before_creating_storage',
    'provisional_bootstrap_transfers_actual_exclusive_locks_and_rejects_live_writer_reentry',
    'provisional_bootstrap_warm_segment_substitution_fails_without_writing_replacement',
    'provisional_bootstrap_readback_rejects_actual_same_length_mutation_before_sync',
    'provisional_bootstrap_failed_real_sync_boundary_withholds_owner_and_cold_renews',
    'provisional_bootstrap_cold_aliases_and_hardlinks_never_become_custody',
    'provisional_bootstrap_warm_directory_swap_never_populates_replacement',
    'provisional_bootstrap_real_manifest_publication_precedes_any_wal_suffix',
    'explicit_application_identity_migration_preserves_legacy_bytes_and_repeats_all_barriers',
    'explicit_application_identity_migration_rejects_schema_binding_and_namespace_without_effects',
    'explicit_application_identity_migration_refuses_native_and_missing_routes',
    'explicit_application_identity_migration_rejects_aliases_and_nonempty_lock',
    'explicit_application_identity_migration_refuses_actual_held_lock_without_replacement',
    'explicit_application_identity_migration_original_route_loss_precedes_lock_creation',
    'explicit_application_identity_migration_postcreation_error_checks_original_custody',
    'explicit_application_identity_migration_interrupted_boundaries_repeat_without_lock_recreation',
    'explicit_application_identity_migration_unwind_releases_owner_and_preserves_retry_inode',
]
if sys.platform == "linux":
    bounded_control_names.append("complete_namespace_preflight_rejects_non_utf8_obsolete_name_without_deletion")
bounded_control_evidence = {
    name: any(test_passed(line, name) for line in workspace_stdout.splitlines())
    for name in bounded_control_names
}
bounded_control_valid = all(bounded_control_evidence.values())
summary = {
    "all_pass": not failed.strip() and before == after and workloads_valid and automatic_valid and sessions_valid and kv_batches_valid and streams_valid and indexed_valid and membership_approval_valid and membership_decision_valid and membership_peer_valid and membership_staging_valid and uncertain_begin_valid and client_transport_valid and membership_activation_valid and idle_application_valid and primary_recovery_valid and membership_capacity_valid and learner_promotion_valid and native_role_valid and publication_recovery_valid and peer_publication_valid and native_membership_valid and prechoice_prerequisites_valid and integrated_recovery_valid and bounded_control_valid,
    "source_unchanged": before == after,
    "source_sha256_before": before,
    "source_sha256_after": after,
    "seed": int(seed),
    "passed_domains": passed.split(),
    "failed_domains": failed.split(),
    "trace_directory": root,
    "bounded_control_evidence_valid": bounded_control_valid,
    "bounded_control_regressions": bounded_control_evidence,
    "bounded_control_scope": "bounded historical replay and owned append, dedicated control learner policy, managed control routing and authenticated Genesis/intent delivery observations; bounded ordinary namespace disposal; initial receiver, native persistence and admission remain unfinished",
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
        "strict trace verification", "replay", "local trace parity (fozzy ci)"
    ],
}
Path(root, "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
print(json.dumps(summary, indent=2))
raise SystemExit(0 if summary["all_pass"] else 1)
