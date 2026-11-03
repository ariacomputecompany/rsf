#!/usr/bin/env python3
"""Bootstrap bookkeeping tests: every subprocess is fake, no engine is built."""
import contextlib
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import types
import unittest
from unittest import mock


BOOTSTRAP = Path(__file__).with_name("bootstrap-fozzy.py")
if "--bootstrap" in sys.argv:
    argument = sys.argv.index("--bootstrap")
    BOOTSTRAP = Path(sys.argv[argument + 1]).resolve()
    del sys.argv[argument:argument + 2]


class BootstrapTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="bootstrap-bookkeeping-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.source = self.root / "source"
        self.source.mkdir()
        self.prefix = self.root / "install"
        self.scripts = self.root / "scripts"
        self.scripts.mkdir()
        files = {"Cargo.toml": b'[package]\nname="fozzy"\nversion="0.1.0"\n',
                 "Cargo.lock": b"version = 4\n", "src/platform/envinfo.rs": b"// identity\n",
                 "src/main.rs": b"fn main() {}\n"}
        tree = []
        for relative, data in files.items():
            path = self.source / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
            path.chmod(0o644)
            blob = hashlib.sha1(b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest()
            tree.append(f"100644 blob {blob}\t{relative}\0")
        self.tree = "".join(tree)
        self.pin = {"commit": "1" * 40, "tree": "2" * 40, "version": "0.1.0",
                    "rust_toolchain": "1.92.0", "files": {
                        relative: hashlib.sha256(data).hexdigest()
                        for relative, data in files.items() if relative != "src/main.rs"}}
        self.pin_path = self.scripts / "fozzy-engine.json"
        self.pin_path.write_text(json.dumps(self.pin))
        self.pin_bytes = self.pin_path.read_bytes()
        spec = importlib.util.spec_from_file_location("bootstrap_under_test", BOOTSTRAP)
        self.module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.module)
        self.module.__file__ = str(self.scripts / "bootstrap-fozzy.py")
        self.environment = {"CARGO_HOME": str(self.root / "cargo-home"),
                            "RUSTUP_TOOLCHAIN": "1.92.0", "PATH": "/fake/toolchain"}
        self.calls = []
        self.builds = 0
        self.status = ""
        self.head = self.pin["commit"]
        self.tree_id = self.pin["tree"]
        self.rustc = "rustc 1.92.0\nrelease: 1.92.0\nhost: x86_64-unknown-linux-gnu"
        self.path_rustc = None
        self.help = "Fozzy deterministic full-stack testing"
        self.version = {"version": "0.1.0", "commit": self.pin["commit"]}
        self.during_build = lambda: None
        self.fail_build = False

    def fake_process(self, argv, **kwargs):
        argv = [str(value) for value in argv]
        self.calls.append(argv)
        if argv[:3] == ["git", "-C", str(self.source)]:
            command = argv[3:]
            if command == ["rev-parse", "HEAD"]:
                result = self.head
            elif command == ["rev-parse", "HEAD^{tree}"]:
                result = self.tree_id
            elif command == ["status", "--porcelain", "--untracked-files=all"]:
                result = self.status
            elif command == ["ls-tree", "-r", "-z", self.pin["commit"]]:
                result = self.tree
            else:
                raise AssertionError(f"unexpected fake Git command: {argv}")
        elif argv[:5] == ["rustup", "run", "1.92.0", "cargo", "build"]:
            self.builds += 1
            self.assertIn("--locked", argv)
            self.assertEqual(kwargs["env"]["FOZZY_COMMIT"], self.pin["commit"])
            self.assertNotIn("FOZZY_BUILD_DATE", kwargs["env"])
            self.assertEqual(kwargs["cwd"], self.source)
            kwargs["stdout"].write("FAKE BUILD: no compiler was invoked\n")
            kwargs["stderr"].write("FAKE BUILD diagnostics\n")
            if self.fail_build:
                raise subprocess.CalledProcessError(101, argv)
            build = Path(argv[argv.index("--target-dir") + 1])
            binary = build / argv[argv.index("--target") + 1] / "release" / "fozzy"
            binary.parent.mkdir(parents=True)
            binary.write_bytes(b"INERT FAKE BUILD OUTPUT; NOT AN EXECUTABLE\n")
            self.during_build()
            result = ""
        elif argv == ["rustup", "run", "1.92.0", "rustc", "-vV"]:
            result = self.rustc
        elif argv == ["rustc", "-vV"]:
            result = self.rustc if self.path_rustc is None else self.path_rustc
        elif argv in (["cargo", "--version"], ["rustup", "run", "1.92.0", "cargo", "--version"]):
            result = "cargo 1.92.0"
        elif argv == [str(self.prefix / "bin/fozzy"), "--help"]:
            result = self.help
        elif argv == [str(self.prefix / "bin/fozzy"), "version", "--json"]:
            self.assertEqual(kwargs["cwd"], self.prefix)
            result = json.dumps(self.version)
        elif argv == ["cc", "--version"]:
            result = "FAKE C COMPILER; never invoked for compilation"
        else:
            raise AssertionError(f"unmocked subprocess request: {argv}")
        return types.SimpleNamespace(stdout=result)

    def invoke(self):
        with mock.patch.dict(os.environ, self.environment, clear=True), \
                mock.patch.object(sys, "argv", ["bootstrap", str(self.source), str(self.prefix)]), \
                mock.patch.object(self.module.os, "geteuid", return_value=1001), \
                mock.patch.object(self.module.platform, "system", return_value="Linux"), \
                mock.patch.object(self.module.subprocess, "run", side_effect=self.fake_process), \
                contextlib.redirect_stdout(io.StringIO()):
            self.module.main()

    def reject_before_build(self, pattern):
        with self.assertRaisesRegex(RuntimeError, pattern):
            self.invoke()
        self.assertEqual(self.builds, 0)
        self.assertFalse((self.prefix / "provenance.json").exists())

    def test_valid_fake_build_records_exact_identity(self):
        self.environment["FOZZY_BUILD_DATE"] = "must not leak into build"
        self.invoke()
        self.assertEqual(self.builds, 1)
        provenance = json.loads((self.prefix / "provenance.json").read_text())
        self.assertEqual(provenance["source"], self.pin)
        self.assertEqual(provenance["pin_sha256"], hashlib.sha256(self.pin_bytes).hexdigest())
        self.assertEqual(provenance["binary_sha256"],
                         hashlib.sha256((self.prefix / "bin/fozzy").read_bytes()).hexdigest())
        self.assertIn("not an engine equivalence", provenance["scope"])

    def test_wrong_commit(self):
        self.head = "0" * 40
        self.reject_before_build("pinned commit")

    def test_wrong_tree(self):
        self.tree_id = "0" * 40
        self.reject_before_build("pinned tree")

    def test_dirty_or_untracked_status(self):
        for status in (" M src/main.rs", "?? build.rs"):
            with self.subTest(status=status):
                self.status = status
                self.reject_before_build("modified or untracked")

    def test_clean_status_cannot_hide_modified_unpinned_source(self):
        (self.source / "src/main.rs").write_text("fn main() { panic!(); }\n")
        self.reject_before_build("tracked source bytes")

    def test_clean_status_cannot_hide_changed_mode(self):
        (self.source / "src/main.rs").chmod(0o755)
        self.reject_before_build("executable mode")

    def test_clean_status_cannot_hide_ignored_build_input(self):
        (self.source / "build.rs").write_text("fn main() {}\n")
        self.reject_before_build("extra or ignored")

    def test_changed_pinned_lockfile(self):
        (self.source / "Cargo.lock").write_text("version = 3\n")
        self.reject_before_build("source bytes|source hash")

    def test_source_file_alias(self):
        path = self.source / "src/main.rs"
        target = self.root / "elsewhere.rs"
        path.rename(target)
        path.symlink_to(target)
        self.reject_before_build("source alias")

    def test_source_directory_alias(self):
        path = self.source / "src"
        target = self.root / "elsewhere"
        path.rename(target)
        path.symlink_to(target, target_is_directory=True)
        self.reject_before_build("source alias")

    def test_wrong_compiler_release(self):
        self.rustc = self.rustc.replace("1.92.0", "1.91.0")
        self.reject_before_build("compiler release")

    def test_path_compiler_mismatch(self):
        self.path_rustc = "rustc 1.91.0"
        self.reject_before_build("PATH must select")

    def test_unqualified_target(self):
        self.rustc = self.rustc.replace("x86_64-unknown-linux-gnu", "wasm32-unknown-unknown")
        self.reject_before_build("unqualified build target")

    def test_direct_compiler_environment_override(self):
        self.environment["RUSTC"] = "/different/compiler"
        self.reject_before_build("compiler redirection")

    def test_compiler_environment_redirects(self):
        for name in ("RUSTC_WRAPPER", "RUSTC_WORKSPACE_WRAPPER", "CARGO_BUILD_RUSTC",
                     "CARGO_BUILD_RUSTC_WRAPPER", "CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER"):
            with self.subTest(name=name):
                self.environment[name] = "/different/compiler"
                self.reject_before_build("compiler redirection")
                del self.environment[name]

    def test_compiler_config_redirects(self):
        directory = Path(self.environment["CARGO_HOME"])
        directory.mkdir()
        for body in ('[build]\nrustc="/different/compiler"\n',
                     '[build]\nrustc-wrapper="/different/wrapper"\n',
                     '[build]\nrustc-workspace-wrapper="/different/wrapper"\n',
                     '[env]\nRUSTC="/different/compiler"\n',
                     'include="elsewhere.toml"\n'):
            with self.subTest(body=body):
                (directory / "config.toml").write_text(body)
                self.reject_before_build("redirection|includes")

    def test_relative_cargo_home(self):
        self.environment["CARGO_HOME"] = "relative-cargo"
        self.reject_before_build("CARGO_HOME must be absolute")

    def test_dependency_config_redirects(self):
        directory = Path(self.environment["CARGO_HOME"])
        directory.mkdir()
        for body in ('paths=["/local/dependency"]\n',
                     '[source.crates-io]\nreplace-with="local"\n',
                     '[patch.crates-io]\nexample={path="/local/dependency"}\n'):
            with self.subTest(body=body):
                (directory / "config.toml").write_text(body)
                self.reject_before_build("dependency redirection")

    def test_ancestor_workspace(self):
        (self.root / "Cargo.toml").write_text("[workspace]\nmembers=[]\n")
        self.reject_before_build("ancestor Cargo workspace")

    def test_reused_prefix(self):
        self.prefix.mkdir()
        (self.prefix / "do-not-touch").write_text("owned")
        self.reject_before_build("prefix must be new")
        self.assertEqual((self.prefix / "do-not-touch").read_text(), "owned")

    def test_dangling_prefix_alias(self):
        self.prefix.symlink_to(self.root / "absent")
        self.reject_before_build("prefix must be new")
        self.assertFalse((self.root / "absent").exists())

    def test_failed_build_keeps_logs_without_installed_binary(self):
        self.fail_build = True
        with self.assertRaises(subprocess.CalledProcessError):
            self.invoke()
        self.assertIn("FAKE BUILD", (self.prefix / "build.stderr").read_text())
        self.assertFalse((self.prefix / "bin/fozzy").exists())
        self.assertFalse((self.prefix / "provenance.json").exists())

    def test_wrong_engine_help(self):
        self.help = "compiler, not determinism engine"
        with self.assertRaisesRegex(RuntimeError, "not the Fozzy"):
            self.invoke()
        self.assertFalse((self.prefix / "provenance.json").exists())

    def test_cwd_derived_or_wrong_commit_metadata(self):
        self.version["commit"] = "4805d9d719cf"
        with self.assertRaisesRegex(RuntimeError, "exact reviewed source commit"):
            self.invoke()
        self.assertFalse((self.prefix / "provenance.json").exists())

    def test_wrong_engine_version(self):
        self.version["version"] = "9.9.9"
        with self.assertRaisesRegex(RuntimeError, "exact reviewed source commit"):
            self.invoke()
        self.assertFalse((self.prefix / "provenance.json").exists())

    def test_source_changes_during_build(self):
        self.during_build = lambda: (self.source / "src/main.rs").write_text("changed\n")
        with self.assertRaisesRegex(RuntimeError, "tracked source bytes"):
            self.invoke()
        self.assertFalse((self.prefix / "bin/fozzy").exists())

    def test_configuration_changes_during_build(self):
        directory = Path(self.environment["CARGO_HOME"])
        directory.mkdir()
        self.during_build = lambda: (directory / "config.toml").write_text("[build]\njobs=2\n")
        with self.assertRaisesRegex(RuntimeError, "configuration changed"):
            self.invoke()
        self.assertFalse((self.prefix / "bin/fozzy").exists())

    def test_pin_changes_during_build(self):
        self.during_build = lambda: self.pin_path.write_text(json.dumps(dict(self.pin, version="9.9.9")))
        with self.assertRaisesRegex(RuntimeError, "pin changed"):
            self.invoke()
        self.assertFalse((self.prefix / "provenance.json").exists())


if __name__ == "__main__":
    unittest.main(verbosity=2)
