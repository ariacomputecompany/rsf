#!/usr/bin/env sh
set -eu

seed="${FOZZY_SEED:-20260912}"
fozzy_bin="${FOZZY_BIN:-fozzy}"
# Some hosts also install the FozzyLang compiler under this name. Require the
# determinism engine; a scripted process expectation alone is not a host test.
case "$("$fozzy_bin" --help)" in
  *"deterministic full-stack testing"*) ;;
  *) echo "FOZZY_BIN must point to the Fozzy determinism engine, not the fz compiler" >&2; exit 1 ;;
esac
mkdir -p artifacts
artifact_root=$(mktemp -d artifacts/verification.XXXXXX)

run_domain() {
  name="$1"
  scenario="$2"
  trace="$artifact_root/$name.trace.fozzy"

  echo "== fozzy domain=${name} scenario=${scenario} trace=${trace}"
  "$fozzy_bin" doctor --deep --scenario "$scenario" --runs 5 --seed "$seed" --json
  "$fozzy_bin" test --det --strict --seed "$seed" "$scenario" --json
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
  "$fozzy_bin" run "$host_scenario" --det --seed "$seed" --record "$trace" \
    --proc-backend host --fs-backend host --http-backend host --json
  "$fozzy_bin" trace verify "$trace" --strict --json
  "$fozzy_bin" replay "$trace" --json
  "$fozzy_bin" ci "$trace" --json
}

run_domain storage tests/storage-host.pass.fozzy.json
run_domain vsr tests/vsr-host.pass.fozzy.json
run_domain membership tests/membership-host.pass.fozzy.json
run_domain protocol tests/protocol-host.pass.fozzy.json
run_domain kv-transactions tests/kv-txn-host.pass.fozzy.json
run_domain sql tests/sql-host.pass.fozzy.json
run_domain authentication tests/auth-host.pass.fozzy.json
run_domain tenancy tests/tenancy-host.pass.fozzy.json
run_domain daemon tests/rsfd-host.pass.fozzy.json
run_domain object-blob tests/object-blob-host.pass.fozzy.json
run_domain recovery tests/recovery-host.pass.fozzy.json
run_domain policy tests/policy-host.pass.fozzy.json
run_domain end-to-end tests/poc-output-host.pass.fozzy.json
run_domain workspace tests/workspace-host.pass.fozzy.json
