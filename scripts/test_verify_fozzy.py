#!/usr/bin/env python3
"""Verifier-only tests. The fake engine executes no Cargo, RSF, or Fozzy scenario."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock
import sys
import subprocess

sys.dont_write_bytecode = True
SCRIPT = Path(__file__).with_name("verify-fozzy.py")
spec = importlib.util.spec_from_file_location("verification_runner", SCRIPT)
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)

FAKE = r'''#!/usr/bin/env python3
import json, os, sys
from pathlib import Path
control = json.loads(Path(os.environ["FAKE_CONTROL"]).read_text())
a = sys.argv[1:]
if a == ["--help"]:
    print("fake deterministic full-stack testing engine; verifier tests only")
    raise SystemExit(0)
if a == ["--version"]:
    print(control.get("version", "fake-engine-v1"))
    raise SystemExit(0)
phase = {"test":"strict", "run":"host", "trace":"verify"}.get(a[0], a[0])
with Path(control["calls"]).open("a") as out:
    out.write(json.dumps({"phase":phase, "argv":a})+"\n")
if control.get("fail") == phase:
    print(json.dumps({"status":"fail", "fake":True}))
    raise SystemExit(31)
seed = int(a[a.index("--seed")+1]) if "--seed" in a else 7
if phase == "doctor":
    value = {"ok":True,"issues":[],"determinism_audit":{
        "scenario":a[a.index("--scenario")+1], "runs":5,"seed":seed,
        "consistent":True,"signatures":["0"*64]*5}}
elif phase == "strict":
    value = {"status":"pass","mode":"test","identity":{"runId":"fake-test","seed":seed},
             "tests":{"passed":1,"failed":0,"skipped":0}}
elif phase == "host":
    host = Path(a[1]); trace = Path(a[a.index("--record")+1])
    value = {"status":"pass","mode":"run","identity":{"runId":"fake-host","seed":seed,"tracePath":str(trace)}}
    scenario = json.loads(host.read_text())
    commands = [s for s in scenario["steps"] if s["type"] == "proc_spawn"]
    trace.write_text(json.dumps({"format":"fozzy-trace","version":4,"mode":"run",
        "scenario_path":str(host),"scenario":scenario,"checksum":"0"*64,"summary":value,
        "decisions":[{"kind":"proc_spawn","cmd":s["cmd"],"args":s.get("args",[]),
                      "exit_code":0,"stdout":"FAKE: no RSF tests ran\n","stderr":""} for s in commands],
        "events":[{"name":"proc_spawn","fields":{"backend":"host"}} for s in commands]}))
elif phase == "verify":
    value = {"ok":True,"path":a[2],"version":4,"checksumPresent":True,"checksumValid":True}
elif phase == "replay":
    value = {"status":"pass","mode":"replay","identity":{"runId":"fake-replay","seed":seed,"tracePath":a[1]}}
else:
    names = ["trace_verify","replay_outcome_class","replay_warning_parity","strict_warning_policy",
             "replay_memory_parity","memory_policy","artifacts_zip_integrity"]
    value = {"schemaVersion":"fozzy.ci_report.v1","ok":True,"checks":[{"name":n,"ok":True} for n in names]}
if control.get("malformed") == phase:
    value = {"status":"pass"}
if control.get("nested") == phase:
    if phase == "doctor": value["determinism_audit"] = []
    elif phase == "strict": value["identity"] = []
    elif phase == "ci": value["checks"] = [None]*7
print(json.dumps(value))
'''


class ResumeTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="verifier-only-")
        self.base = Path(self.temporary.name).resolve()
        self.repo = self.base / "repo"
        self.repo.mkdir()
        for name in ("crates", "scripts", "tests", "artifacts"):
            (self.repo / name).mkdir()
        for name in ("Cargo.toml", "Cargo.lock"):
            (self.repo / name).write_text("verifier fixture, never passed to Cargo\n")
        for name in ("verify-fozzy.py", "verify-fozzy-summary.py", "verify-fozzy.sh"):
            (self.repo / "scripts" / name).write_bytes(SCRIPT.with_name(name).read_bytes())
        self.scenario = "tests/storage-host.pass.fozzy.json"
        (self.repo / self.scenario).write_text(json.dumps({"version":1,"name":"fake-storage",
            "steps":[{"type":"proc_when","cmd":"must-never-execute","exit_code":0},
                     {"type":"proc_spawn","cmd":"must-never-execute","args":["example"],"expect_exit":0}]}))
        self.toolbin = self.base / "toolbin"
        self.toolbin.mkdir()
        for name, arguments in (("cargo", ["--version"]), ("rustc", ["-vV"])):
            tool = self.toolbin / name
            tool.write_text("#!/usr/bin/env python3\nimport json, os, sys\nfrom pathlib import Path\n"
                            + f"assert sys.argv[1:] == {arguments!r}, 'fake tool cannot execute builds'\n"
                            + "control = json.loads(Path(os.environ['FAKE_CONTROL']).read_text())\n"
                            + f"print(control.get('{name}_version', 'fake-{name}-v1'))\n")
            tool.chmod(0o755)
        self.cargo_home = self.base / "cargo-home"
        self.cargo_home.mkdir()
        self.binary = self.base / "fake-fozzy"
        self.binary.write_text(FAKE)
        self.binary.chmod(0o755)
        self.calls = self.base / "calls.jsonl"
        self.control = self.base / "control.json"
        self.settings()
        self.previous = Path.cwd()
        os.chdir(self.repo)
        self.environment = mock.patch.dict(os.environ, {
            "FOZZY_BIN":str(self.binary), "FOZZY_SEED":"7", "FAKE_CONTROL":str(self.control),
            "CARGO_HOME":str(self.cargo_home), "PATH":str(self.toolbin) + os.pathsep + os.environ["PATH"]})
        self.environment.start()
        self.domains = mock.patch.object(gate, "DOMAINS", (("storage", self.scenario),))
        self.domains.start()

    def tearDown(self):
        self.domains.stop()
        self.environment.stop()
        os.chdir(self.previous)
        self.temporary.cleanup()

    def settings(self, **extra):
        self.control.write_text(json.dumps({"calls":str(self.calls), **extra}))

    def call_count(self):
        return len(self.calls.read_text().splitlines()) if self.calls.exists() else 0

    def start(self, name="gate"):
        root = self.repo / "artifacts" / name
        # The real unchanged summary must reject fake evidence: no required RSF
        # metrics/regression names are fabricated by this fake engine.
        self.assertEqual(gate.main(["--artifact-dir", str(root)]), 1)
        self.assertFalse(json.loads((root / "summary.json").read_text())["all_pass"])
        return root

    def checkpoint(self, root):
        return json.loads((root / "checkpoint.json").read_text())

    def attestation(self, root):
        evidence = self.base / "no-live-fixture-processes.txt"
        evidence.write_text("Verifier fixture capture.wait() reaped every fake child; interruption is injected after wait. No RSF process was launched.\n")
        proof = self.base / "resume-proof.json"
        proof.write_text(json.dumps({"version":1,"artifact_root":str(root),
            "checkpoint_sha256":gate.sha(root / "checkpoint.json"),"no_live_processes":True,
            "checked_by":"verifier-only test fixture, not a production process audit",
            "evidence":str(evidence),"evidence_sha256":gate.sha(evidence)}))
        return proof

    def interrupted(self, name, fail=False):
        self.settings(**({"fail":"doctor"} if fail else {}))
        root = self.repo / "artifacts" / name
        original = gate.capture
        def after_wait(*args, **kwargs):
            result = original(*args, **kwargs)
            if args[1] == "doctor":
                raise KeyboardInterrupt("fixture owner disappears after child wait")
            return result
        with mock.patch.object(gate, "capture", after_wait):
            with self.assertRaises(KeyboardInterrupt):
                gate.main(["--artifact-dir", str(root)])
        return root

    def test_complete_pipeline_is_skipped_but_fake_summary_stays_failed(self):
        root = self.start()
        self.assertEqual(self.checkpoint(root)["domains"]["storage"]["status"], "passed")
        count = self.call_count()
        before = (root / "checkpoint.json").read_bytes()
        self.assertEqual(gate.main(["--resume", str(root)]), 1)
        self.assertEqual(self.call_count(), count)
        self.assertEqual((root / "checkpoint.json").read_bytes(), before)

    def test_changed_source_cannot_resume_or_rewrite_checkpoint(self):
        root = self.start()
        before = (root / "checkpoint.json").read_bytes()
        count = self.call_count()
        (self.repo / "Cargo.toml").write_text("changed source\n")
        with self.assertRaisesRegex(gate.Rejected, "source"):
            gate.main(["--resume", str(root)])
        self.assertEqual(self.call_count(), count)
        self.assertEqual((root / "checkpoint.json").read_bytes(), before)

    def test_failure_in_each_phase_is_permanent_with_original_raw_output(self):
        for phase in gate.PHASES:
            with self.subTest(phase=phase):
                self.settings(fail=phase)
                root = self.start(phase)
                before = {p.relative_to(root):p.read_bytes() for p in root.rglob("*") if p.is_file()}
                count = self.call_count()
                self.settings()
                self.assertEqual(gate.main(["--resume", str(root)]), 1)
                self.assertEqual(self.call_count(), count)
                self.assertEqual(self.checkpoint(root)["domains"]["storage"]["status"], "failed")
                self.assertEqual({p.relative_to(root):p.read_bytes() for p in root.rglob("*") if p.is_file()}, before)

    def test_zero_exit_with_malformed_success_is_failure(self):
        self.settings(malformed="verify")
        root = self.start()
        self.assertEqual(self.checkpoint(root)["domains"]["storage"]["status"], "failed")
        self.settings()
        count = self.call_count()
        self.assertEqual(gate.main(["--resume", str(root)]), 1)
        self.assertEqual(self.call_count(), count)

    def test_nested_malformed_success_is_terminal_and_independent_domains_continue(self):
        for phase in ("doctor", "strict", "ci"):
            with self.subTest(phase=phase):
                self.settings(nested=phase)
                domains = (("storage", self.scenario), ("second", self.scenario))
                with mock.patch.object(gate, "DOMAINS", domains):
                    root = self.start("nested-" + phase)
                    state = self.checkpoint(root)
                    self.assertTrue(all(value["status"] == "failed" for value in state["domains"].values()))
                    count = self.call_count()
                    before = {p.relative_to(root):p.read_bytes() for p in root.rglob("*") if p.is_file()}
                    self.settings()
                    self.assertEqual(gate.main(["--resume", str(root)]), 1)
                    self.assertEqual(self.call_count(), count)
                    self.assertEqual({p.relative_to(root):p.read_bytes() for p in root.rglob("*") if p.is_file()}, before)

    def test_mid_run_config_change_permanently_invalidates_before_publication(self):
        root = self.repo / "artifacts/mixed-context"
        original = gate.capture
        config = self.repo / ".cargo/config.toml"
        def mutate_after_ci(*args, **kwargs):
            result = original(*args, **kwargs)
            if args[1] == "ci":
                config.parent.mkdir(exist_ok=True)
                config.write_text("[build]\njobs = 1\n")
            return result
        with mock.patch.object(gate, "capture", mutate_after_ci):
            with self.assertRaisesRegex(gate.Rejected, "execution context changed"):
                gate.main(["--artifact-dir", str(root)])
        state = self.checkpoint(root)
        self.assertIn("execution context", state["invalidated"]["reason"])
        self.assertNotEqual(state["domains"]["storage"]["status"], "passed")
        self.assertFalse((root / "storage.trace.fozzy").exists())
        self.assertFalse((root / "storage/attempt-0001/terminal.json").exists())
        self.assertFalse((root / "summary.json").exists())
        config.unlink()
        count = self.call_count()
        proof = self.attestation(root)
        with self.assertRaisesRegex(gate.Rejected, "invalidated"):
            gate.main(["--resume", str(root), "--no-live-processes", str(proof)])
        self.assertEqual(self.call_count(), count)
        self.assertEqual(self.checkpoint(root), state)

    def test_failed_identity_probe_cannot_be_restored_into_a_passing_domain(self):
        root = self.repo / "artifacts/unreadable-context"
        original = gate.capture
        rustc = self.toolbin / "rustc"
        tool_bytes = rustc.read_bytes()
        def break_probe_after_ci(*args, **kwargs):
            result = original(*args, **kwargs)
            if args[1] == "ci":
                rustc.write_text("#!/usr/bin/env python3\nraise SystemExit(23)\n")
            return result
        with mock.patch.object(gate, "capture", break_probe_after_ci):
            with self.assertRaises((gate.Rejected, subprocess.SubprocessError)):
                gate.main(["--artifact-dir", str(root)])
        state = self.checkpoint(root)
        self.assertIsNotNone(state["invalidated"], "unobservable execution identity must not remain reusable")
        self.assertNotEqual(state["domains"]["storage"]["status"], "passed")
        self.assertFalse((root / "summary.json").exists())
        rustc.write_bytes(tool_bytes)
        count = self.call_count()
        proof = self.attestation(root)
        with self.assertRaisesRegex(gate.Rejected, "invalidated"):
            gate.main(["--resume", str(root), "--no-live-processes", str(proof)])
        self.assertEqual(self.call_count(), count)
        self.assertEqual(self.checkpoint(root), state)

    def test_no_skip_without_all_pipeline_outputs_and_trace(self):
        for name in ("ci.exit.json", "replay.stdout", "trace.fozzy", "host.fozzy.json"):
            with self.subTest(missing=name):
                root = self.start(name.replace(".", "-"))
                (root / "storage/attempt-0001" / name).unlink()
                count = self.call_count()
                with self.assertRaises((gate.Rejected, OSError)):
                    gate.main(["--resume", str(root)])
                self.assertEqual(self.call_count(), count)

    def test_engine_version_and_duplicate_checkpoint_keys_are_rejected(self):
        root = self.start()
        self.settings(version="fake-engine-v2")
        with self.assertRaises(gate.Rejected):
            gate.main(["--resume", str(root)])
        self.settings()
        path = root / "checkpoint.json"
        path.write_text(path.read_text().replace('{', '{"version":1,', 1))
        with self.assertRaisesRegex(gate.Rejected, "duplicate"):
            gate.main(["--resume", str(root)])

    def test_toolchain_cargo_configs_and_profile_overrides_cannot_change(self):
        root = self.start()
        source = gate.source_fingerprint(self.repo)
        count = self.call_count()
        self.settings(rustc_version="different-fake-rustc")
        with self.assertRaises(gate.Rejected):
            gate.main(["--resume", str(root)])
        self.settings()
        for directory in (self.repo / ".cargo", self.cargo_home):
            directory.mkdir(exist_ok=True)
            config = directory / "config.toml"
            config.write_text("test-secret-token must only be hashed\n")
            self.assertEqual(gate.source_fingerprint(self.repo), source)
            with self.assertRaises(gate.Rejected):
                gate.main(["--resume", str(root)])
            config.unlink()
        for name in ("CARGO_PROFILE_RELEASE_LTO", "RUST_TEST_THREADS"):
            changed = "2" if os.environ.get(name) == "1" else "1"
            with mock.patch.dict(os.environ, {name:changed}):
                with self.assertRaises(gate.Rejected):
                    gate.main(["--resume", str(root)])
        self.assertEqual(self.call_count(), count)
        self.assertNotIn("test-secret-token", (root / "checkpoint.json").read_text())

    def test_unknown_checkpoint_version_and_unexpected_attempt_file_are_refused(self):
        root = self.start()
        path = root / "checkpoint.json"
        before = path.read_bytes()
        value = json.loads(before)
        value["version"] = 2
        path.write_text(json.dumps(value))
        count = self.call_count()
        with self.assertRaises(gate.Rejected):
            gate.main(["--resume", str(root)])
        path.write_bytes(before)
        (root / "storage/attempt-0001/unexpected.json").write_text("{}")
        with self.assertRaisesRegex(gate.Rejected, "unexpected"):
            gate.main(["--resume", str(root)])
        self.assertEqual(self.call_count(), count)

    def test_legacy_trace_directory_is_not_imported(self):
        root = self.repo / "artifacts/legacy"
        root.mkdir()
        (root / "storage.trace.fozzy").write_text("{}")
        with self.assertRaises(OSError):
            gate.main(["--resume", str(root)])
        self.assertEqual(self.call_count(), 0)

    def test_interrupted_owner_requires_bound_attestation_and_new_attempt(self):
        root = self.interrupted("interrupted")
        first = root / "storage/attempt-0001"
        retained = {p.name:p.read_bytes() for p in first.iterdir()}
        count = self.call_count()
        with self.assertRaisesRegex(gate.Rejected, "no-live-process"):
            gate.main(["--resume", str(root)])
        self.assertEqual(self.call_count(), count)
        proof = self.attestation(root)
        value = json.loads(proof.read_text())
        value["checkpoint_sha256"] = "0"*64
        proof.write_text(json.dumps(value))
        with self.assertRaisesRegex(gate.Rejected, "stale"):
            gate.main(["--resume", str(root), "--no-live-processes", str(proof)])
        proof = self.attestation(root)
        self.assertEqual(gate.main(["--resume", str(root), "--no-live-processes", str(proof)]), 1)
        self.assertTrue((root / "storage/attempt-0002").is_dir())
        self.assertTrue((first / "interruption.json").is_file())
        self.assertEqual({name:(first/name).read_bytes() for name in retained}, retained)
        count = self.call_count()
        (first / "doctor.stdout").write_text("changed archived evidence")
        with self.assertRaisesRegex(gate.Rejected, "archived"):
            gate.main(["--resume", str(root)])
        self.assertEqual(self.call_count(), count)

    def test_recorded_failure_before_interruption_is_not_retried(self):
        root = self.interrupted("known-failed", fail=True)
        self.settings()
        count = self.call_count()
        proof = self.attestation(root)
        self.assertEqual(gate.main(["--resume", str(root), "--no-live-processes", str(proof)]), 1)
        self.assertEqual(self.call_count(), count)
        self.assertEqual(self.checkpoint(root)["domains"]["storage"], {"status":"failed","attempt":1})
        self.assertFalse((root / "storage/attempt-0002").exists())


if __name__ == "__main__":
    unittest.main()
