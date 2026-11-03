#!/usr/bin/env python3
"""Build the reviewed engine source into a new prefix; never use PATH's fozzy."""
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import stat
import subprocess
import sys
import tempfile
import tomllib


def output(argv, **kwargs):
    return subprocess.run(argv, check=True, capture_output=True, text=True, **kwargs).stdout.strip()


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def require(value, message):
    if not value:
        raise RuntimeError(message)


def source_identity(source, pin):
    require(output(["git", "-C", str(source), "rev-parse", "HEAD"]) == pin["commit"],
            "engine checkout differs from pinned commit")
    require(output(["git", "-C", str(source), "rev-parse", "HEAD^{tree}"]) == pin["tree"],
            "engine checkout differs from pinned tree")
    require(not output(["git", "-C", str(source), "status", "--porcelain", "--untracked-files=all"]),
            "engine source has modified or untracked files")
    # Index flags and stat caches can hide changed tracked bytes from status.
    # Compare the physical checkout with the pinned immutable tree instead.
    tree = output(["git", "-C", str(source), "ls-tree", "-r", "-z", pin["commit"]])
    tracked = set()
    for record in tree.split("\0"):
        if not record:
            continue
        header, relative = record.split("\t", 1)
        mode, kind, expected = header.split()
        parts = Path(relative).parts
        require(parts and not Path(relative).is_absolute() and ".." not in parts,
                "invalid path in pinned source tree")
        require(kind == "blob" and mode in ("100644", "100755"),
                f"unsupported source tree entry: {relative}")
        require(relative not in tracked, "duplicate source tree entry")
        tracked.add(relative)
        path = source
        for component in parts:
            path /= component
            require(not path.is_symlink(), f"source alias is unsupported: {relative}")
        require(path.is_file(), f"missing tracked source: {relative}")
        data = path.read_bytes()
        actual = hashlib.sha1(b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest()
        executable = bool(path.stat().st_mode & stat.S_IXUSR)
        require(actual == expected and executable == (mode == "100755"),
                f"tracked source bytes or executable mode changed: {relative}")
    require(tracked, "pinned source tree is empty")
    # A fresh qualification checkout may not contain ignored build inputs either.
    # Only root .git metadata is exempt; target output lives outside the checkout.
    physical = set()
    for directory, directories, files in os.walk(source, followlinks=False):
        if Path(directory) == source:
            directories[:] = [name for name in directories if name != ".git"]
            files = [name for name in files if name != ".git"]
        for name in directories + files:
            path = Path(directory) / name
            require(not path.is_symlink(), f"source alias is unsupported: {path.relative_to(source)}")
        for name in files:
            path = Path(directory) / name
            require(path.is_file(), "unsupported special file in source checkout")
            physical.add(path.relative_to(source).as_posix())
    require(physical == tracked, "source contains extra or ignored files")
    for relative, expected in pin["files"].items():
        path = source / relative
        require(path.is_file() and not path.is_symlink() and sha(path) == expected,
                f"engine source hash mismatch: {relative}")


def build_context(source):
    redirects = {"RUSTC", "RUSTC_WRAPPER", "RUSTC_WORKSPACE_WRAPPER",
                 "CARGO_BUILD_RUSTC", "CARGO_BUILD_RUSTC_WRAPPER",
                 "CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER"}
    require(not redirects.intersection(os.environ), "compiler redirection environment is unsupported")
    cargo_home = Path(os.environ.get("CARGO_HOME", str(Path.home() / ".cargo")))
    require(cargo_home.is_absolute(), "CARGO_HOME must be absolute")
    directories = {p / ".cargo" for p in (source, *source.parents)} | {cargo_home}
    configs = {}
    for directory in sorted(directories):
        for name in ("config", "config.toml"):
            path = directory / name
            if path.exists() or path.is_symlink():
                target = path.resolve(strict=True)
                config = tomllib.loads(target.read_text())
                require("include" not in config, "Cargo config includes are unsupported")
                require(not {"paths", "source", "patch"}.intersection(config),
                        "Cargo dependency redirection is unsupported")
                require(not {"rustc", "rustc-wrapper", "rustc-workspace-wrapper"}.intersection(
                    config.get("build", {})), "Cargo compiler redirection is unsupported")
                require(not redirects.intersection(config.get("env", {})),
                        "Cargo environment compiler redirection is unsupported")
                configs[str(path)] = {"target": str(target), "sha256": sha(target)}
    names = {"RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "CARGO_BUILD_RUSTFLAGS", "CARGO_INCREMENTAL",
             "RUSTC", "RUSTC_WRAPPER", "RUSTC_WORKSPACE_WRAPPER", "CARGO_BUILD_TARGET",
             "CC", "CFLAGS", "AR"}
    names |= {name for name in os.environ if name.startswith(("CARGO_PROFILE_", "CARGO_TARGET_"))}
    return {"cargo_configs": configs,
            "environment_sha256": {name: hashlib.sha256(os.environ[name].encode()).hexdigest()
                                   if name in os.environ else None for name in sorted(names)}}


def main():
    require(len(sys.argv) == 3, "usage: bootstrap-fozzy.py <source-checkout> <new-install-prefix>")
    require(platform.system() in ("Linux", "Darwin"), "this bootstrap qualifies Unix hosts only")
    require(os.geteuid() != 0, "qualification must run as an unprivileged user")
    source = Path(sys.argv[1]).resolve(strict=True)
    prefix = Path(sys.argv[2]).absolute()
    require(not prefix.exists() and not prefix.is_symlink(), "engine install prefix must be new")
    pin_path = Path(__file__).with_name("fozzy-engine.json")
    pin_bytes = pin_path.read_bytes()
    pin = json.loads(pin_bytes)
    # The reviewed engine is a standalone package. Cargo discovers ancestor
    # workspaces even across nested Git repositories; require an isolated route.
    for ancestor in source.parents:
        manifest = ancestor / "Cargo.toml"
        require(not manifest.is_file() or "workspace" not in tomllib.loads(manifest.read_text()),
                "engine source must be outside an ancestor Cargo workspace")
    source_identity(source, pin)
    toolchain = pin["rust_toolchain"]
    rustc = output(["rustup", "run", toolchain, "rustc", "-vV"])
    cargo = output(["rustup", "run", toolchain, "cargo", "--version"])
    fields = dict(line.split(": ", 1) for line in rustc.splitlines() if ": " in line)
    require(fields["release"] == toolchain, "unexpected Rust compiler release")
    require(output(["rustc", "-vV"]) == rustc and output(["cargo", "--version"]) == cargo,
            "PATH must select the same pinned toolchain for subsequent RSF checks")
    context = build_context(source)
    target = fields["host"]
    require(target in ("x86_64-unknown-linux-gnu", "aarch64-unknown-linux-gnu",
                       "x86_64-apple-darwin", "aarch64-apple-darwin"), "unqualified build target")
    prefix.mkdir(parents=True)
    binary = prefix / "bin" / "fozzy"
    binary.parent.mkdir()
    environment = dict(os.environ, FOZZY_COMMIT=pin["commit"], RUSTUP_TOOLCHAIN=toolchain)
    environment.pop("FOZZY_BUILD_DATE", None)
    # Build outside the checkout. Failed builds retain logs but cannot leave an
    # installed binary; every invocation requires a fresh install prefix.
    with tempfile.TemporaryDirectory(prefix="rsf-fozzy-build-") as build:
        argv = ["rustup", "run", toolchain, "cargo", "build", "--locked", "--release",
                "--manifest-path", str(source / "Cargo.toml"), "--target", target,
                "--target-dir", build, "--bin", "fozzy"]
        (prefix / "build-command.json").write_text(json.dumps(argv, indent=2) + "\n")
        with (prefix / "build.stdout").open("w") as stdout, (prefix / "build.stderr").open("w") as stderr:
            subprocess.run(argv, env=environment, cwd=source, stdout=stdout, stderr=stderr, check=True)
        source_identity(source, pin)
        require(build_context(source) == context, "build configuration changed during compilation")
        require(pin_path.read_bytes() == pin_bytes, "engine pin changed during compilation")
        shutil.copy2(Path(build) / target / "release" / "fozzy", binary)
    require("deterministic full-stack testing" in output([str(binary), "--help"]),
            "built executable is not the Fozzy determinism engine")
    version = json.loads(output([str(binary), "version", "--json"], cwd=prefix))
    require(version.get("version") == pin["version"] and version.get("commit") == pin["commit"],
            "engine does not embed the exact reviewed source commit")
    runner_keys = ("GITHUB_REPOSITORY", "GITHUB_SHA", "GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT",
                   "GITHUB_WORKFLOW_REF", "RUNNER_OS", "RUNNER_ARCH", "ImageOS", "ImageVersion")
    provenance = {
        "source": pin, "pin_sha256": hashlib.sha256(pin_bytes).hexdigest(), "binary": str(binary),
        "build_context": context,
        "binary_sha256": sha(binary), "version": version, "rustc": rustc, "cargo": cargo,
        "host": {"system": platform.system(), "release": platform.release(),
                 "machine": platform.machine(), "python": platform.python_version(),
                 "cpu_count": os.cpu_count(), "disk_after_build": shutil.disk_usage(prefix)._asdict(),
                 "cc": output(["cc", "--version"])},
        "runner": {name: os.environ.get(name) for name in runner_keys},
        "scope": "Pinned source build identity; not an engine equivalence or RSF qualification result",
    }
    (prefix / "provenance.json").write_text(json.dumps(provenance, indent=2, sort_keys=True) + "\n")
    print(str(binary))


if __name__ == "__main__":
    main()
