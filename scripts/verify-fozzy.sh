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
  python3 - "$scenario" "$host_scenario" <<'PY'
import json
import sys
from pathlib import Path

scenario = json.loads(Path(sys.argv[1]).read_text())
scenario["name"] += "-host-execution"
scenario["steps"] = [step for step in scenario["steps"] if step["type"] != "proc_when"]
processes = [step for step in scenario["steps"] if step["type"] == "proc_spawn"]
if not processes or any(step.get("expect_exit") != 0 for step in processes):
    raise SystemExit("host verification requires process success assertions")
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
    name: any(line.startswith("test ") and f"::{name} ... ok" in line
              for line in workspace_stdout.splitlines())
    for name in session_regressions
}
sessions_valid = all(session_evidence.values())
kv_batch_evidence = {
    name: any(line.startswith("test ") and f"::{name} ... ok" in line
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
    name: any(line.startswith("test ") and f"::{name} ... ok" in line
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
    name: any(line.startswith("test ") and f"::{name} ... ok" in line
              for line in workspace_stdout.splitlines())
    for name in indexed_names
}
indexed_valid = all(indexed_evidence.values())
membership_approval_names = [
    "membership_proposal_codec_binds_old_authority_new_voters_and_target",
    "membership_fence_survives_restart_preserves_prefix_and_blocks_authority_shortcuts",
    "membership_old_quorum_approval_rejects_mixed_duplicate_and_tampered_histories",
    "malformed_membership_fence_envelopes_fail_repeated_recovery_without_rewrite",
    "killed_membership_writer_recovers_synced_fence_before_acknowledgement",
    "membership_approval_recovers_from_real_old_voters_after_primary_loss",
]
membership_approval_evidence = {
    name: any(line.startswith("test ") and f"::{name} ... ok" in line
              for line in workspace_stdout.splitlines())
    for name in membership_approval_names
}
membership_approval_valid = all(membership_approval_evidence.values())
summary = {
    "all_pass": not failed.strip() and before == after and workloads_valid and automatic_valid and sessions_valid and kv_batches_valid and streams_valid and indexed_valid and membership_approval_valid,
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
    "membership_approval_evidence_valid": membership_approval_valid,
    "membership_approval_regressions": membership_approval_evidence,
    "membership_approval_scope": "old-quorum approval only; new-quorum activation, learner promotion and complete membership qualification remain unfinished",
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
