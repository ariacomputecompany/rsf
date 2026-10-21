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
    for event in json.loads(trace.read_text()).get("events", []):
        fields = event.get("fields", {})
        if event.get("name") != "proc_spawn" or fields.get("exit_code") != 0:
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
    for event in json.loads(automatic_trace.read_text()).get("events", []):
        fields = event.get("fields", {})
        if event.get("name") != "proc_spawn" or fields.get("exit_code") != 0:
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
    and automatic_client_metrics[profile].get("verified_logical_kv_writes") == 34
    and automatic_client_metrics[profile].get("surviving_replicas_checked") == 2
    and automatic_client_metrics[profile].get("primary_state_deleted") is True
    and automatic_client_metrics[profile].get("replacement_restarts") == 1
    and automatic_client_metrics[profile].get("surface_count") == 4
    for profile in ("debug", "release")
)
summary = {
    "all_pass": not failed.strip() and before == after and workloads_valid and automatic_valid,
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
    "coverage_per_passing_domain": [
        "five-run doctor", "strict deterministic test", "real host recorded run",
        "strict trace verification", "replay", "CI"
    ],
}
Path(root, "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
print(json.dumps(summary, indent=2))
raise SystemExit(0 if summary["all_pass"] else 1)
PY
