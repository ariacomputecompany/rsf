#!/usr/bin/env python3
"""Durable verification bookkeeping; certificates remain Fozzy's responsibility."""
import argparse
import fcntl
import hashlib
import json
import os
import platform
from pathlib import Path
import shutil
import socket
import stat
import subprocess
import sys
import tempfile
import uuid

VERSION = 1
DOMAINS = (
    ('storage', 'tests/storage-host.pass.fozzy.json'),
    ('vsr', 'tests/vsr-host.pass.fozzy.json'),
    ('control-state', 'tests/control-state-host.pass.fozzy.json'),
    ('membership', 'tests/membership-host.pass.fozzy.json'),
    ('membership-decision', 'tests/membership-decision-host.pass.fozzy.json'),
    ('membership-peer', 'tests/membership-peer-host.pass.fozzy.json'),
    ('membership-staging', 'tests/membership-staging-host.pass.fozzy.json'),
    ('membership-activation', 'tests/membership-activation-host.pass.fozzy.json'),
    ('protocol', 'tests/protocol-host.pass.fozzy.json'),
    ('peer-rpc', 'tests/rpc-host.pass.fozzy.json'),
    ('kv-transactions', 'tests/kv-txn-host.pass.fozzy.json'),
    ('sql', 'tests/sql-host.pass.fozzy.json'),
    ('authentication', 'tests/auth-host.pass.fozzy.json'),
    ('tenancy', 'tests/tenancy-host.pass.fozzy.json'),
    ('daemon', 'tests/rsfd-host.pass.fozzy.json'),
    ('native-membership', 'tests/native-membership-host.pass.fozzy.json'),
    ('automatic-client', 'tests/automatic-client-host.pass.fozzy.json'),
    ('sql-stream', 'tests/sql-stream-host.pass.fozzy.json'),
    ('daemon-stress', 'tests/stress-host.pass.fozzy.json'),
    ('release-stress', 'tests/stress-release-host.pass.fozzy.json'),
    ('object-blob', 'tests/object-blob-host.pass.fozzy.json'),
    ('recovery', 'tests/recovery-host.pass.fozzy.json'),
    ('policy', 'tests/policy-host.pass.fozzy.json'),
    ('end-to-end', 'tests/poc-output-host.pass.fozzy.json'),
    ('workspace', 'tests/workspace-host.pass.fozzy.json'),
)
PHASES = ("doctor", "strict", "host", "verify", "replay", "ci")
CAPTURE_SUFFIXES = (".started.json", ".pid.json", ".stdout", ".stderr", ".exit.json")
CI_CHECKS = {"trace_verify", "replay_outcome_class", "replay_warning_parity",
             "strict_warning_policy", "replay_memory_parity", "memory_policy",
             "artifacts_zip_integrity"}
ENV_KEYS = ("CARGO_BUILD_TARGET", "CARGO_ENCODED_RUSTFLAGS", "RUSTFLAGS",
            "RUSTUP_TOOLCHAIN", "CARGO_HOME", "RUSTUP_HOME", "CARGO_INCREMENTAL",
            "RUSTC", "RUSTC_WRAPPER", "RUSTC_WORKSPACE_WRAPPER", "RUSTDOC",
            "RUSTDOCFLAGS", "CARGO_BUILD_RUSTFLAGS", "CARGO_BUILD_JOBS", "RUST_TEST_THREADS")


class Rejected(Exception):
    pass


def require(condition, message):
    if not condition:
        raise Rejected(message)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def hex_digest(value):
    return isinstance(value, str) and len(value) == 64 and all(c in "0123456789abcdef" for c in value)


def regular(path):
    info = path.lstat()
    require(stat.S_ISREG(info.st_mode), f"not a regular evidence file: {path}")
    return path.read_bytes()


def sha(path):
    return digest(regular(path))


def parse(data):
    def object_pairs(pairs):
        value = {}
        for key, item in pairs:
            require(key not in value, f"duplicate JSON key: {key}")
            value[key] = item
        return value
    return json.loads(data, object_pairs_hook=object_pairs,
                      parse_constant=lambda value: (_ for _ in ()).throw(
                          Rejected(f"non-finite JSON value: {value}")))


def read_json(path):
    return parse(regular(path))


def keys(value, expected, name):
    require(isinstance(value, dict) and set(value) == set(expected),
            f"malformed {name} fields")


def sync_dir(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def write_new(path, data):
    with path.open("xb") as output:
        output.write(data)
        output.flush()
        os.fsync(output.fileno())
    sync_dir(path.parent)


def encoded(value):
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode()


def atomic_json(path, value):
    temporary = path.with_name(path.name + ".tmp-" + uuid.uuid4().hex)
    write_new(temporary, encoded(value))
    os.replace(temporary, path)
    sync_dir(path.parent)


def source_fingerprint(repo):
    # Keep the original source scope and framing, including untracked inputs.
    paths = [Path("Cargo.toml"), Path("Cargo.lock")]
    for directory in ("crates", "tests", "scripts"):
        paths.extend(path.relative_to(repo) for path in (repo / directory).rglob("*")
                     if path.is_file() and path.name != ".DS_Store")
    value = hashlib.sha256()
    for path in sorted(paths):
        value.update(path.as_posix().encode() + b"\0")
        value.update(hashlib.sha256((repo / path).read_bytes()).digest())
    return value.hexdigest()


def engine_identity():
    configured = os.environ.get("FOZZY_BIN")
    path = shutil.which(configured or "fozzy")
    def output(candidate, argument):
        return subprocess.run([str(candidate), argument], capture_output=True,
                              check=True).stdout.decode()
    try:
        initial_help = output(path, "--help") if path else ""
    except (OSError, subprocess.SubprocessError):
        initial_help = ""
    if not configured and "deterministic full-stack testing" not in initial_help:
        fallback = Path.home() / ".cargo/bin/fozzy"
        path = str(fallback) if fallback.is_file() else path
    require(path is not None, "Fozzy determinism engine not found")
    path = Path(path).resolve(strict=True)
    require("deterministic full-stack testing" in output(path, "--help"),
            "FOZZY_BIN must name the Fozzy engine, not the fz compiler")
    version = output(path, "--version").strip()
    require(bool(version), "empty Fozzy version")
    return {"path": str(path), "sha256": sha(path), "version": version}



def execution_context(repo):
    tools = {}
    for name, args in (("rustc", ["-vV"]), ("cargo", ["--version"])):
        executable = shutil.which(name)
        require(executable is not None, f"missing toolchain executable: {name}")
        result = subprocess.run([executable, *args], capture_output=True, check=True)
        tools[name] = {"path": str(Path(executable).absolute()),
                       "version": result.stdout.decode().strip()}
        require(bool(tools[name]["version"]), f"empty {name} version")
    cargo_home = Path(os.environ.get("CARGO_HOME", str(Path.home() / ".cargo"))).absolute()
    directories = {ancestor / ".cargo" for ancestor in (repo, *repo.parents)}
    directories.add(cargo_home)
    configs = {}
    for directory in sorted(directories):
        for name in ("config", "config.toml"):
            path = directory / name
            if path.exists() or path.is_symlink():
                target = path.resolve(strict=True)
                configs[str(path)] = {"target": str(target), "sha256": sha(target)}
            else:
                configs[str(path)] = None
    names = set(ENV_KEYS) | {name for name in os.environ
                            if name.startswith(("CARGO_PROFILE_", "CARGO_TARGET_"))}
    # Values can contain wrapper arguments or credentials; retain change
    # detection without dumping configuration contents or environment secrets.
    environment = {name: digest(os.environ[name].encode()) if name in os.environ else None
                   for name in sorted(names)}
    return {"tools": tools, "cargo_configs": configs, "environment_sha256": environment,
            "host": {"system": platform.system(), "release": platform.release(),
                     "machine": platform.machine(), "python": platform.python_version()}}


def host_scenario(repo, scenario, directory, name):
    value = read_json(repo / scenario)
    value["name"] += "-host-execution"
    value["steps"] = [step for step in value["steps"] if step["type"] != "proc_when"]
    processes = [step for step in value["steps"] if step["type"] == "proc_spawn"]
    require(processes and all(step.get("expect_exit") == 0 for step in processes),
            "host verification requires process success assertions")
    if name == "workspace":
        for step in processes:
            step["args"] = ["scripts/capture-host-process.sh",
                            str(directory / "workspace.process.log"), step["cmd"],
                            *step.get("args", [])]
            step["cmd"] = "sh"
    return value


def commands(engine, seed, scenario, directory):
    binary = engine["path"]
    trace = str(directory / "trace.fozzy")
    return {
        "doctor": [binary, "doctor", "--deep", "--scenario", scenario, "--runs", "5", "--seed", str(seed), "--json"],
        "strict": [binary, "test", "--det", "--strict", "--seed", str(seed), scenario, "--json"],
        "host": [binary, "run", str(directory / "host.fozzy.json"), "--det", "--seed", str(seed), "--record", trace,
                 "--proc-backend", "host", "--fs-backend", "host", "--http-backend", "host", "--json"],
        "verify": [binary, "trace", "verify", trace, "--strict", "--json"],
        "replay": [binary, "replay", trace, "--json"],
        "ci": [binary, "ci", trace, "--json"],
    }


def validate_reply(phase, value, seed, scenario, directory):
    require(isinstance(value, dict), f"{phase}: expected one JSON object")
    trace = str(directory / "trace.fozzy")
    if phase == "doctor":
        audit = value.get("determinism_audit")
        require(isinstance(audit, dict), "malformed doctor audit")
        signatures = audit.get("signatures")
        require(value.get("ok") is True and value.get("issues") == []
                and audit.get("scenario") == scenario and audit.get("runs") == 5
                and audit.get("seed") == seed and audit.get("consistent") is True
                and isinstance(signatures, list) and len(signatures) == 5
                and all(hex_digest(s) for s in signatures)
                and len(set(signatures)) == 1, "incomplete doctor evidence")
    elif phase in ("strict", "host", "replay"):
        mode = {"strict": "test", "host": "run", "replay": "replay"}[phase]
        identity = value.get("identity")
        require(isinstance(identity, dict), f"malformed {phase} identity")
        require(value.get("status") == "pass" and value.get("mode") == mode
                and identity.get("seed") == seed and isinstance(identity.get("runId"), str)
                and identity["runId"], f"incomplete {phase} result")
        if phase == "strict":
            require(value.get("tests") == {"passed": 1, "failed": 0, "skipped": 0}
                    and all(type(number) is int for number in value["tests"].values()),
                    "strict test did not execute exactly one passing scenario")
        else:
            require(identity.get("tracePath") == trace, f"{phase} trace identity differs")
    elif phase == "verify":
        require(value.get("ok") is True and value.get("path") == trace
                and value.get("version") == 4 and value.get("checksumPresent") is True
                and value.get("checksumValid") is True, "strict trace evidence incomplete")
    else:
        checks = value.get("checks")
        require(value.get("schemaVersion") == "fozzy.ci_report.v1" and value.get("ok") is True
                and isinstance(checks, list) and len(checks) == len(CI_CHECKS)
                and all(isinstance(check, dict) for check in checks)
                and {check.get("name") for check in checks} == CI_CHECKS
                and all(check.get("ok") is True for check in checks), "incomplete/unknown CI evidence")


def validate_trace(directory, seed):
    trace = read_json(directory / "trace.fozzy")
    host = read_json(directory / "host.fozzy.json")
    expected = [step for step in host["steps"] if step["type"] == "proc_spawn"]
    require(isinstance(trace, dict), "trace must be an object")
    require(trace.get("format") == "fozzy-trace" and trace.get("version") == 4
            and trace.get("mode") == "run" and trace.get("scenario_path") == str(directory / "host.fozzy.json")
            and hex_digest(trace.get("checksum")),
            "unsupported or mismatched recorded trace")
    validate_reply("host", trace.get("summary"), seed, "", directory)
    require(trace["summary"] == read_json(directory / "host.stdout"),
            "trace does not belong to the recorded host-run result")
    require(isinstance(trace.get("decisions"), list)
            and all(isinstance(item, dict) for item in trace["decisions"])
            and isinstance(trace.get("events"), list)
            and all(isinstance(item, dict) for item in trace["events"]),
            "malformed trace decisions/events")
    actual = [item for item in trace["decisions"] if item.get("kind") == "proc_spawn"]
    events = [item for item in trace["events"] if item.get("name") == "proc_spawn"]
    require(len(actual) == len(expected) == len(events), "trace is missing required host processes")
    for command, result, event in zip(expected, actual, events):
        require(isinstance(event.get("fields"), dict), "malformed host event fields")
        require(result.get("cmd") == command["cmd"] and result.get("args") == command.get("args", [])
                and result.get("exit_code") == 0 and isinstance(result.get("stdout"), str)
                and isinstance(result.get("stderr"), str) and event.get("fields", {}).get("backend") == "host",
                "trace lacks exact successful host command evidence")


def capture(directory, phase, argv, context):
    write_new(directory / (phase + ".started.json"), encoded({
        "argv": argv, "runner_pid": os.getpid(), "hostname": socket.gethostname()}))
    print(f"== {context}: {phase}; raw outputs in {directory}", flush=True)
    with (directory / (phase + ".stdout")).open("xb") as out, \
            (directory / (phase + ".stderr")).open("xb") as err:
        try:
            child = subprocess.Popen(argv, stdout=out, stderr=err, close_fds=True)
        except OSError as error:
            write_new(directory / (phase + ".pid.json"), encoded({"pid": None}))
            err.write((f"verification launch failed: {error}\n").encode())
            code = 127
        else:
            write_new(directory / (phase + ".pid.json"), encoded({"pid": child.pid}))
            code = child.wait()
        out.flush()
        err.flush()
        os.fsync(out.fileno())
        os.fsync(err.fileno())
    # Persist exit status before semantic interpretation. A nonzero exit cannot
    # become an interrupted retry if the owner dies between these two writes.
    result = {"argv": argv, "exit_code": code,
              "stdout_sha256": sha(directory / (phase + ".stdout")),
              "stderr_sha256": sha(directory / (phase + ".stderr"))}
    write_new(directory / (phase + ".exit.json"), encoded(result))
    for suffix, target in (("stdout", sys.stdout.buffer), ("stderr", sys.stderr.buffer)):
        with (directory / (phase + "." + suffix)).open("rb") as stream:
            shutil.copyfileobj(stream, target)
        target.flush()
    return result


class Gate:
    def __init__(self, repo, root, state):
        self.repo, self.root, self.state = repo, root, state

    def save(self):
        atomic_json(self.root / "checkpoint.json", self.state)

    def unchanged(self):
        require(self.state["invalidated"] is None, "attempt identity was invalidated; use a new root")
        try:
            source = source_fingerprint(self.repo)
            engine = sha(Path(self.state["engine"]["path"]))
            context = execution_context(self.repo)
        except (Rejected, OSError, ValueError, subprocess.SubprocessError) as error:
            # An unobservable identity is not evidence of continuity. Preserve
            # the failed check so restoring a tool/config cannot bless this run.
            self.state["invalidated"] = {"reason": "execution identity could not be verified"}
            self.save()
            raise Rejected("execution identity could not be verified; use a new artifact root") from error
        changed = [name for name, before, after in (
            ("source", self.state["source_sha256"], source),
            ("engine", self.state["engine"]["sha256"], engine),
            ("execution context", self.state["execution_context"], context),
        ) if before != after]
        if changed:
            reason = ", ".join(changed) + " changed during verification"
            # Remember the observation. Restoring old config/source must not
            # make an already mixed attempt eligible for terminal publication.
            self.state["invalidated"] = {
                "reason": reason, "source_sha256": source, "engine_sha256": engine,
                "execution_context_sha256": digest(encoded(context)),
            }
            self.save()
            raise Rejected(reason + "; use a new artifact root")

    def directory(self, name, number):
        return self.root / name / f"attempt-{number:04d}"

    def inspect(self, name, scenario, number, completed):
        directory = self.directory(name, number)
        if not directory.exists():
            require(not completed, "terminal attempt directory is missing")
            return "interrupted", "attempt did not start"
        require(not directory.is_symlink() and directory.is_dir(), "aliased attempt directory")
        expected = commands(self.state["engine"], self.state["seed"], scenario, directory)
        allowed = {"host.fozzy.json", "trace.fozzy", "terminal.json", "interruption.json"}
        allowed.update(phase + suffix for phase in PHASES for suffix in CAPTURE_SUFFIXES)
        if name == "workspace":
            allowed.update(("workspace.process.log", "workspace.process.log.stderr"))
        require(all(path.name in allowed and path.is_file() and not path.is_symlink()
                    for path in directory.iterdir()), "unexpected/aliased attempt evidence")
        host = directory / "host.fozzy.json"
        if host.exists():
            require(read_json(host) == host_scenario(self.repo, scenario, directory, name), "host scenario changed")
        seen = 0
        failed = None
        partial = False
        for phase in PHASES:
            exit_path = directory / (phase + ".exit.json")
            if not exit_path.exists():
                present = [directory / (phase + suffix) for suffix in CAPTURE_SUFFIXES
                           if (directory / (phase + suffix)).exists()]
                if present:
                    require(not partial and failed is None, "out-of-order partial phase evidence")
                    start = read_json(directory / (phase + ".started.json"))
                    keys(start, ("argv", "runner_pid", "hostname"), "partial phase start")
                    require(start["argv"] == expected[phase], "partial command differs")
                    if (directory / (phase + ".pid.json")).exists():
                        child = read_json(directory / (phase + ".pid.json"))
                        keys(child, ("pid",), "partial child")
                        require(child["pid"] is None or (type(child["pid"]) is int and child["pid"] > 0), "invalid partial child")
                partial = True
                continue
            require(not partial and failed is None, "out-of-order or post-failure phase evidence")
            started = read_json(directory / (phase + ".started.json"))
            keys(started, ("argv", "runner_pid", "hostname"), "phase start")
            require(started["argv"] == expected[phase] and type(started["runner_pid"]) is int
                    and started["runner_pid"] > 0 and isinstance(started["hostname"], str), "phase start changed")
            child = read_json(directory / (phase + ".pid.json"))
            keys(child, ("pid",), "child identity")
            require(child["pid"] is None or (type(child["pid"]) is int and child["pid"] > 0), "invalid child identity")
            result = read_json(exit_path)
            keys(result, ("argv", "exit_code", "stdout_sha256", "stderr_sha256"), "phase exit")
            require(result["argv"] == expected[phase] and type(result["exit_code"]) is int,
                    "recorded command/version differs")
            for suffix in ("stdout", "stderr"):
                require(sha(directory / (phase + "." + suffix)) == result[suffix + "_sha256"], "raw phase evidence changed")
            if child["pid"] is None:
                require(result["exit_code"] == 127 and regular(directory / (phase + ".stderr")).startswith(b"verification launch failed: "), "invalid launch-failure evidence")
            if result["exit_code"] != 0:
                failed = f"{phase} exited {result['exit_code']}"
            else:
                try:
                    validate_reply(phase, read_json(directory / (phase + ".stdout")),
                                   self.state["seed"], scenario, directory)
                    if phase == "host":
                        validate_trace(directory, self.state["seed"])
                except (Rejected, OSError, ValueError, KeyError, TypeError) as error:
                    failed = f"{phase} evidence rejected: {error}"
            seen += 1
        if (directory / "trace.fozzy").exists():
            require((directory / "host.started.json").exists(), "trace exists without a host invocation")
        if failed:
            return "failed", failed
        if seen == len(PHASES):
            require(host.exists(), "completed attempt lost its host scenario")
            return "passed", None
        require(not completed, "terminal pass lacks the complete six-phase pipeline")
        return "interrupted", "pipeline did not terminate"

    def validate_history(self, name, scenario, entry):
        parent = self.root / name
        if not parent.exists():
            require(entry["attempt"] == 0 or entry["status"] == "running", "attempt history missing")
            return
        require(parent.is_dir() and not parent.is_symlink(), "aliased domain evidence directory")
        expected = {f"attempt-{number:04d}" for number in range(1, entry["attempt"] + 1)}
        require({path.name for path in parent.iterdir()} <= expected, "unexpected attempt number")
        last = entry["attempt"] + (entry["status"] == "interrupted")
        for number in range(1, last):
            directory = self.directory(name, number)
            marker = read_json(directory / "interruption.json")
            keys(marker, ("version", "checkpoint_sha256", "files"), "interruption archive")
            require(type(marker["version"]) is int and marker["version"] == VERSION,
                    "unknown interruption marker version")
            actual = {path.name: sha(path) for path in directory.iterdir() if path.name != "interruption.json"}
            require(actual == marker["files"], "archived interrupted evidence changed")
            status, _ = self.inspect(name, scenario, number, False)
            require(status == "interrupted", "known terminal result cannot be an interrupted retry")

    def validate_terminal(self, name, scenario, entry):
        status, reason = self.inspect(name, scenario, entry["attempt"], True)
        require(status == entry["status"], "terminal status conflicts with phase evidence")
        terminal = read_json(self.directory(name, entry["attempt"]) / "terminal.json")
        keys(terminal, ("status", "reason", "trace_sha256"), "domain terminal")
        require(terminal["status"] == status and terminal["reason"] == reason, "terminal record changed")
        trace = self.directory(name, entry["attempt"]) / "trace.fozzy"
        require(terminal["trace_sha256"] == (sha(trace) if trace.exists() else None), "retained trace changed")
        published = self.root / (name + ".trace.fozzy")
        if terminal["trace_sha256"] is not None:
            require(sha(published) == terminal["trace_sha256"], "summary trace differs from terminal trace")
        else:
            require(not published.exists(), "unexpected summary trace")

    def terminal(self, name, scenario, number):
        directory = self.directory(name, number)
        status, reason = self.inspect(name, scenario, number, False)
        require(status != "interrupted", "cannot terminalize incomplete pipeline")
        trace = directory / "trace.fozzy"
        terminal = {"status": status, "reason": reason,
                    "trace_sha256": sha(trace) if trace.exists() else None}
        path = directory / "terminal.json"
        if path.exists():
            require(read_json(path) == terminal, "conflicting terminal record")
        else:
            write_new(path, encoded(terminal))
        if trace.exists():
            published = self.root / (name + ".trace.fozzy")
            if published.exists():
                require(sha(published) == terminal["trace_sha256"], "conflicting summary trace")
            else:
                write_new(published, regular(trace))
        self.state["domains"][name] = {"status": status, "attempt": number}
        self.state["active"] = None
        self.save()

    def run_domain(self, name, scenario):
        entry = self.state["domains"][name]
        if entry["status"] in ("passed", "failed"):
            self.validate_terminal(name, scenario, entry)
            print(f"== preserving terminal {entry['status']}: {name}", flush=True)
            return
        self.unchanged()
        number = entry["attempt"] + 1
        directory = self.directory(name, number)
        directory.parent.mkdir(exist_ok=True)
        self.state["domains"][name] = {"status": "running", "attempt": number}
        self.state["active"] = {"domain": name, "attempt": number}
        self.save()
        directory.mkdir()
        sync_dir(directory.parent)
        write_new(directory / "host.fozzy.json", encoded(host_scenario(self.repo, scenario, directory, name)))
        for phase, argv in commands(self.state["engine"], self.state["seed"], scenario, directory).items():
            result = capture(directory, phase, argv, name)
            if result["exit_code"] != 0:
                break
            try:
                validate_reply(phase, read_json(directory / (phase + ".stdout")),
                               self.state["seed"], scenario, directory)
                if phase == "host":
                    validate_trace(directory, self.state["seed"])
            except (Rejected, OSError, ValueError, KeyError, TypeError):
                break
        self.unchanged()
        self.terminal(name, scenario, number)

    def authorize_interruption(self, attestation):
        active = self.state["active"]
        if active is None:
            require(attestation is None, "no interrupted active owner to authorize")
            return
        require(attestation is not None, "active attempt requires root's no-live-process attestation; a stale lock/PID is not proof")
        proof = read_json(attestation)
        keys(proof, ("version", "artifact_root", "checkpoint_sha256", "no_live_processes", "checked_by", "evidence", "evidence_sha256"), "resume attestation")
        require(type(proof["version"]) is int and proof["version"] == VERSION and proof["artifact_root"] == str(self.root)
                and proof["checkpoint_sha256"] == sha(self.root / "checkpoint.json")
                and proof["no_live_processes"] is True and isinstance(proof["checked_by"], str)
                and proof["checked_by"].strip(), "stale or invalid resume attestation")
        evidence = Path(proof["evidence"])
        require(evidence.is_absolute() and sha(evidence) == proof["evidence_sha256"], "missing process-inspection evidence")
        archive = self.root / ("resume-" + uuid.uuid4().hex)
        archive.mkdir()
        write_new(archive / "attestation.json", regular(attestation))
        write_new(archive / "process-evidence", regular(evidence))
        if active["domain"] != "summary":
            name, number = active["domain"], active["attempt"]
            scenario = dict(DOMAINS)[name]
            status, _ = self.inspect(name, scenario, number, False)
            if status in ("passed", "failed"):
                self.terminal(name, scenario, number)
                return
            # The old logs never change. The marker records their exact snapshot.
            directory = self.directory(name, number)
            directory.mkdir(parents=True, exist_ok=True)
            marker = {"version": VERSION, "checkpoint_sha256": proof["checkpoint_sha256"],
                      "files": {path.name: sha(path) for path in directory.iterdir()
                                if path.name != "interruption.json"}}
            marker_path = directory / "interruption.json"
            if marker_path.exists():
                require(read_json(marker_path) == marker, "conflicting interruption archive")
            else:
                write_new(marker_path, encoded(marker))
            self.state["domains"][name] = {"status": "interrupted", "attempt": number}
        else:
            directory = self.root / f"summary-{active['attempt']:04d}"
            if (directory / "summary.exit.json").exists():
                self.finish_summary(directory)
                return
            # The original summary oracle is byte-identical. Preserve any output
            # from its interrupted write before it receives the same root again.
            if (self.root / "summary.json").exists():
                target = directory / "interrupted-summary.json"
                require(not target.exists(), "conflicting interrupted summary")
                os.rename(self.root / "summary.json", target)
                sync_dir(directory)
                sync_dir(self.root)
        self.state["active"] = None
        self.save()

    def summary_command(self):
        passed = " ".join(name for name, _ in DOMAINS if self.state["domains"][name]["status"] == "passed")
        failed = " ".join(name for name, _ in DOMAINS if self.state["domains"][name]["status"] == "failed")
        return [sys.executable, "-B", str(self.repo / "scripts/verify-fozzy-summary.py"),
                str(self.root), str(self.state["seed"]), passed, failed,
                self.state["source_sha256"], self.state["source_sha256"]]

    def summary_evidence(self, directory):
        result = read_json(directory / "summary.exit.json")
        keys(result, ("argv", "exit_code", "stdout_sha256", "stderr_sha256"), "summary exit")
        require(type(result["exit_code"]) is int and result["argv"] == self.summary_command(), "invalid summary exit/command")
        for suffix in ("stdout", "stderr"):
            require(sha(directory / ("summary." + suffix)) == result[suffix + "_sha256"], "summary raw output changed")
        path = self.root / "summary.json"
        code = result["exit_code"]
        if code == 0:
            summary = read_json(path)
            require(isinstance(summary, dict), "malformed summary object")
            require(summary.get("all_pass") is True and summary.get("source_unchanged") is True,
                    "zero summary exit lacks passing summary evidence")
            require(regular(path) == regular(directory / "summary.stdout"), "summary differs from its raw output")
        return {"sha256": sha(path) if path.exists() else None, "exit_code": code}

    def finish_summary(self, directory):
        self.state["summary"] = self.summary_evidence(directory)
        self.state["active"] = None
        self.save()
        return self.state["summary"]["exit_code"]

    def summarize(self):
        self.unchanged()
        if self.state["summary"] is not None:
            summary = self.state["summary"]
            directory = self.root / f"summary-{self.state['summary_attempts']:04d}"
            require(self.summary_evidence(directory) == summary, "summary changed")
            return summary["exit_code"]
        require(not (self.root / "summary.json").exists(), "uncommitted summary must be archived through interruption authorization")
        number = self.state["summary_attempts"] + 1
        directory = self.root / f"summary-{number:04d}"
        self.state["summary_attempts"] = number
        self.state["active"] = {"domain": "summary", "attempt": number}
        self.save()
        directory.mkdir()
        capture(directory, "summary", self.summary_command(), "unchanged summary gates")
        self.unchanged()
        return self.finish_summary(directory)


def validate_state(state, expected):
    keys(state, (*expected, "domains", "active", "summary", "summary_attempts", "invalidated"), "checkpoint")
    require(state["invalidated"] is None, "attempt identity was invalidated; use a new root")
    require(type(state["version"]) is int and type(state["seed"]) is int, "invalid checkpoint version/seed type")
    require(all(state[key] == value for key, value in expected.items()), "checkpoint version, source, engine, seed or execution context differs")
    require(set(state["domains"]) == {name for name, _ in DOMAINS}, "domain inventory differs")
    running = []
    for name, entry in state["domains"].items():
        keys(entry, ("status", "attempt"), "domain state")
        require(entry["status"] in ("pending", "running", "interrupted", "passed", "failed")
                and type(entry["attempt"]) is int and entry["attempt"] >= 0
                and (entry["status"] == "pending") == (entry["attempt"] == 0), "invalid domain state")
        if entry["status"] == "running":
            running.append({"domain": name, "attempt": entry["attempt"]})
    require(type(state["summary_attempts"]) is int and state["summary_attempts"] >= 0, "invalid summary attempts")
    if state["active"] is not None:
        keys(state["active"], ("domain", "attempt"), "active owner")
        require(state["active"] in running or (not running and state["active"] == {"domain": "summary", "attempt": state["summary_attempts"]}), "active owner differs from running domain")
    else:
        require(not running, "running domain has no active owner")
    if state["summary"] is not None:
        keys(state["summary"], ("sha256", "exit_code"), "summary status")
        require(type(state["summary"]["exit_code"]) is int and all(v["status"] in ("passed", "failed") for v in state["domains"].values()), "summary precedes terminal domains")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    choice = parser.add_mutually_exclusive_group()
    choice.add_argument("--artifact-dir", type=Path, help="new, nonexistent evidence directory")
    choice.add_argument("--resume", type=Path, help="existing versioned checkpoint; never imports legacy traces")
    parser.add_argument("--no-live-processes", type=Path, help="root-authored process-inspection attestation bound to checkpoint hash")
    args = parser.parse_args(argv)
    repo = Path.cwd().resolve()
    engine = engine_identity()
    seed = int(os.environ.get("FOZZY_SEED", "20260912"))
    require(0 <= seed < 2**64, "seed is outside u64")
    source = source_fingerprint(repo)
    if args.resume:
        root = args.resume.absolute()
        require(root.is_dir() and not root.is_symlink(), "resume root missing or aliased")
        root = root.resolve(strict=True)
    else:
        require(args.no_live_processes is None, "attestation only applies to resume")
        if args.artifact_dir:
            root = args.artifact_dir.resolve()
            root.mkdir(parents=False)
        else:
            (repo / "artifacts").mkdir(exist_ok=True)
            root = Path(tempfile.mkdtemp(prefix="verification.", dir=repo / "artifacts"))
        sync_dir(root.parent)
    print(f"FOZZY_ARTIFACT_ROOT={root}", flush=True)
    # Acquiring this lock excludes another runner, NOT orphan Fozzy/Cargo/daemon
    # descendants. A root attestation is additionally required for active state.
    require(not (root / ".runner.lock").is_symlink(), "aliased runner lock")
    with (root / ".runner.lock").open("a+b") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        expected = {"version": VERSION, "repository": str(repo), "artifact_root": str(root),
                    "source_sha256": source, "engine": engine, "seed": seed,
                    "execution_context": execution_context(repo),
                    "plan": [list(pair) for pair in DOMAINS]}
        if args.resume:
            state = read_json(root / "checkpoint.json")
            validate_state(state, expected)
        else:
            state = {**expected, "domains": {name: {"status": "pending", "attempt": 0} for name, _ in DOMAINS},
                     "active": None, "summary": None, "summary_attempts": 0, "invalidated": None}
        gate = Gate(repo, root, state)
        if not args.resume:
            gate.save()
        # Validate every retained terminal domain before any new subprocess.
        for name, scenario in DOMAINS:
            gate.validate_history(name, scenario, state["domains"][name])
            if state["domains"][name]["status"] in ("passed", "failed"):
                gate.validate_terminal(name, scenario, state["domains"][name])
        gate.authorize_interruption(args.no_live_processes)
        for name, scenario in DOMAINS:
            gate.run_domain(name, scenario)
        return gate.summarize()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (Rejected, OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as error:
        print(f"verification refused: {error}", file=sys.stderr)
        raise SystemExit(2)
