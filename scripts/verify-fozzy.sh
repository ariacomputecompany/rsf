#!/usr/bin/env sh
set -eu

seed="${FOZZY_SEED:-20260912}"
mkdir -p artifacts

run_domain() {
  name="$1"
  scenario="$2"
  trace="$3"

  echo "== fozzy domain=${name} scenario=${scenario} trace=${trace}"
  fozzy doctor --deep --scenario "$scenario" --runs 5 --seed "$seed" --json
  fozzy test --det --strict-verify "$scenario" --json
  fozzy run "$scenario" --det --record "$trace" --json
  fozzy trace verify "$trace" --strict --json
  fozzy replay "$trace" --json
  fozzy ci "$trace" --json
}

run_domain storage tests/storage-host.pass.fozzy.json artifacts/verification-storage.trace.fozzy
run_domain vsr tests/vsr-host.pass.fozzy.json artifacts/verification-vsr.trace.fozzy
run_domain membership tests/membership-host.pass.fozzy.json artifacts/verification-membership.trace.fozzy
run_domain protocol tests/protocol-host.pass.fozzy.json artifacts/verification-protocol.trace.fozzy
run_domain kv-transactions tests/kv-txn-host.pass.fozzy.json artifacts/verification-kv-transactions.trace.fozzy
run_domain sql tests/sql-host.pass.fozzy.json artifacts/verification-sql.trace.fozzy
run_domain object-blob tests/object-blob-host.pass.fozzy.json artifacts/verification-object-blob.trace.fozzy
run_domain recovery tests/recovery-host.pass.fozzy.json artifacts/verification-recovery.trace.fozzy
run_domain policy tests/policy-host.pass.fozzy.json artifacts/verification-policy.trace.fozzy
run_domain end-to-end tests/poc-output-host.pass.fozzy.json artifacts/verification-end-to-end.trace.fozzy
