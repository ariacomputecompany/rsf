#!/usr/bin/env sh
set -eu

seed="${FOZZY_SEED:-20260912}"
fozzy_bin="${FOZZY_BIN:-fozzy}"
if [ -z "${FOZZY_BIN:-}" ]; then
  case "$("$fozzy_bin" --help 2>/dev/null)" in
    *"deterministic full-stack testing"*) ;;
    *)
      if [ -x "$HOME/.cargo/bin/fozzy" ]; then
        fozzy_bin="$HOME/.cargo/bin/fozzy"
      fi
      ;;
  esac
fi
# Some hosts also install the FozzyLang compiler under this name. Require the
# determinism engine; a scripted process expectation alone is not a host test.
case "$("$fozzy_bin" --help)" in
  *"deterministic full-stack testing"*) ;;
  *) echo "FOZZY_BIN must point to the Fozzy determinism engine, not the fz compiler" >&2; exit 1 ;;
esac
mkdir -p artifacts
artifact_root=$(mktemp -d artifacts/verification.XXXXXX)

source_fingerprint() {
  python3 - <<'PY'
import hashlib
from pathlib import Path

paths = [Path("Cargo.toml"), Path("Cargo.lock")]
for root in ("crates", "tests", "scripts"):
    paths.extend(path for path in Path(root).rglob("*")
                 if path.is_file() and path.name != ".DS_Store")
digest = hashlib.sha256()
for path in sorted(paths):
    digest.update(path.as_posix().encode() + b"\0")
    digest.update(hashlib.sha256(path.read_bytes()).digest())
print(digest.hexdigest())
PY
}

source_before=$(source_fingerprint)
passed_domains=""
failed_domains=""

verify_domain() {
  name="$1"
  scenario="$2"
  trace="$artifact_root/$name.trace.fozzy"

  echo "== fozzy domain=${name} scenario=${scenario} trace=${trace}"
  "$fozzy_bin" doctor --deep --scenario "$scenario" --runs 5 --seed "$seed" --json || return $?
  "$fozzy_bin" test --det --strict --seed "$seed" "$scenario" --json || return $?
  # proc_when is a scripted response and also an exact output assertion on the
  # host backend. Cargo's output includes timing and test scheduling; retain the
  # real proc_spawn exit assertions without comparing them to empty mock output.
  host_scenario="$artifact_root/$name.host.fozzy.json"
  python3 - "$scenario" "$host_scenario" "$name" <<'PY'
import json
import sys
from pathlib import Path

scenario = json.loads(Path(sys.argv[1]).read_text())
scenario["name"] += "-host-execution"
scenario["steps"] = [step for step in scenario["steps"] if step["type"] != "proc_when"]
processes = [step for step in scenario["steps"] if step["type"] == "proc_spawn"]
if not processes or any(step.get("expect_exit") != 0 for step in processes):
    raise SystemExit("host verification requires process success assertions")
if sys.argv[3] == "workspace":
    # Preserve Cargo's raw log and exact status even if a descendant retains
    # stderr after Cargo exits. This does not alter tests or their scheduling.
    for step in processes:
        step["args"] = ["scripts/capture-host-process.sh",
                        str(Path(sys.argv[2]).with_name("workspace.process.log")),
                        step["cmd"], *step.get("args", [])]
        step["cmd"] = "sh"
Path(sys.argv[2]).write_text(json.dumps(scenario, indent=2) + "\n")
PY
  [ "$?" -eq 0 ] || return 1
  "$fozzy_bin" run "$host_scenario" --det --seed "$seed" --record "$trace" \
    --proc-backend host --fs-backend host --http-backend host --json || return $?
  "$fozzy_bin" trace verify "$trace" --strict --json || return $?
  "$fozzy_bin" replay "$trace" --json || return $?
  "$fozzy_bin" ci "$trace" --json || return $?
}

run_domain() {
  if verify_domain "$1" "$2"; then
    passed_domains="$passed_domains $1"
  else
    failed_domains="$failed_domains $1"
    echo "verification failed for $1; continuing independent domains" >&2
  fi
}

run_domain storage tests/storage-host.pass.fozzy.json
run_domain vsr tests/vsr-host.pass.fozzy.json
run_domain membership tests/membership-host.pass.fozzy.json
run_domain membership-decision tests/membership-decision-host.pass.fozzy.json
run_domain membership-peer tests/membership-peer-host.pass.fozzy.json
run_domain membership-staging tests/membership-staging-host.pass.fozzy.json
run_domain membership-activation tests/membership-activation-host.pass.fozzy.json
run_domain protocol tests/protocol-host.pass.fozzy.json
run_domain peer-rpc tests/rpc-host.pass.fozzy.json
run_domain kv-transactions tests/kv-txn-host.pass.fozzy.json
run_domain sql tests/sql-host.pass.fozzy.json
run_domain authentication tests/auth-host.pass.fozzy.json
run_domain tenancy tests/tenancy-host.pass.fozzy.json
run_domain daemon tests/rsfd-host.pass.fozzy.json
run_domain automatic-client tests/automatic-client-host.pass.fozzy.json
run_domain sql-stream tests/sql-stream-host.pass.fozzy.json
run_domain daemon-stress tests/stress-host.pass.fozzy.json
run_domain release-stress tests/stress-release-host.pass.fozzy.json
run_domain object-blob tests/object-blob-host.pass.fozzy.json
run_domain recovery tests/recovery-host.pass.fozzy.json
run_domain policy tests/policy-host.pass.fozzy.json
run_domain end-to-end tests/poc-output-host.pass.fozzy.json
run_domain workspace tests/workspace-host.pass.fozzy.json

source_after=$(source_fingerprint)
python3 - "$artifact_root" "$seed" "$passed_domains" "$failed_domains" "$source_before" "$source_after" <<'PY'
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
membership_staging_names = ['chosen_membership_retention_retry_rejects_missing_input_without_recreation', 'synced_directory_creation_preserves_existing_files_across_nested_retries', 'synced_directory_creation_rejects_file_obstructions_without_replacing_data', 'chosen_membership_stage_merges_commit_and_receipt_evidence_deterministically', 'membership_stage_materialization_is_boot_scoped_and_preserves_receiver_receipts', 'staged_membership_quorums_reject_mixed_duplicate_and_invalid_receipts', 'membership_stage_reopen_rejects_torn_or_missing_sealed_history_without_rewrite', 'killed_membership_stage_recovers_receipt_and_replays_materialization_before_report', 'real_tls_staged_membership_requires_materializer_and_survives_peer_restart']
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
summary = {
    "all_pass": not failed.strip() and before == after and workloads_valid and automatic_valid and sessions_valid and kv_batches_valid and streams_valid and indexed_valid and membership_approval_valid and membership_decision_valid and membership_peer_valid and membership_staging_valid and uncertain_begin_valid and client_transport_valid and membership_activation_valid and idle_application_valid and primary_recovery_valid and membership_capacity_valid and learner_promotion_valid and native_role_valid and publication_recovery_valid,
    "source_unchanged": before == after,
    "source_sha256_before": before,
    "source_sha256_after": after,
    "seed": int(seed),
    "passed_domains": passed.split(),
    "failed_domains": failed.split(),
    "trace_directory": root,
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
    "learner_promotion_scope": "owned resumable local voter publication, cold provenance, later-history preservation and real process kills; native peer publication quorum, live role handoff and atomic control admission remain unfinished",
    "publication_recovery_evidence_valid": publication_recovery_valid,
    "publication_recovery_regressions": publication_recovery_evidence,
    "publication_recovery_scope": "replay-proven historical activation, durable staged-quorum input custody and semantic publication receipt codecs; native publish/query RPC, live role handoff, publication completion and atomic control admission remain unfinished",
    "native_role_evidence_valid": native_role_valid,
    "native_role_regressions": native_role_evidence,
    "native_role_scope": "real native TLS peer restart from certified local promotion, non-voting partial claims, fail-closed provenance and actual recovered election budgets; no native publication quorum, live role handoff or atomic client/control admission",
    "membership_capacity_evidence_valid": membership_capacity_valid,
    "membership_capacity_regressions": membership_capacity_evidence,
    "membership_capacity_scope": "bounded complete proofs rejected before new old-voter fences; historical fence decoding/retry preserved; native membership publication/promotion and control admission remain unfinished",
    "primary_recovery_evidence_valid": primary_recovery_valid,
    "primary_recovery_regressions": primary_recovery_evidence,
    "primary_recovery_scope": "ordinary quorum election above superseding durable promises; unchanged native takeover deadlines and workload",
    "idle_application_evidence_valid": idle_application_valid,
    "idle_application_regressions": idle_application_evidence,
    "idle_application_scope": "one owned application writer survives idle polls; protocol ownership is released and reopens against exact history, current authority, transport and per-request authorization",
    "membership_activation_evidence_valid": membership_activation_valid,
    "membership_activation_regressions": membership_activation_evidence,
    "membership_activation_scope": "proof-bound active VSR generation publication and replay; owned learner promotion factory implemented; native peer publication, role/restart custody, atomic control admission and complete production qualification remain unfinished",
    "membership_staging_evidence_valid": membership_staging_valid,
    "membership_staging_regressions": membership_staging_evidence,
    "membership_staging_scope": "sealed chosen prefixes and boot-scoped materialization receipts; native active publication and atomic control admission remain unfinished",
    "membership_peer_evidence_valid": membership_peer_valid,
    "membership_peer_regressions": membership_peer_evidence,
    "membership_peer_scope": "authenticated peer plan consensus with anchored authority; native active publication, voting-role custody and atomic control admission remain unfinished",
    "membership_decision_evidence_valid": membership_decision_valid,
    "membership_decision_regressions": membership_decision_evidence,
    "membership_decision_scope": "transition-only library plan consensus; peer and staged-kernel integration exist; native active publication and atomic control admission remain unfinished",
    "membership_approval_evidence_valid": membership_approval_valid,
    "membership_approval_regressions": membership_approval_evidence,
    "membership_approval_scope": "old-quorum approval evidence; native quorum publication, role handoff and complete membership qualification remain unfinished",
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
PY
