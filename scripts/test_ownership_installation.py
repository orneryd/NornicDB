"""The installed runtime must include and build the actual binding helper."""
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


class InstallationTests(unittest.TestCase):
    def test_standalone_runtime_contains_all_ownership_dependencies(self):
        scripts = Path(__file__).resolve().parent
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            destination = root / "installation"
            env = {**os.environ, "GIT_CONFIG_GLOBAL": str(root / "gitconfig")}
            command = [sys.executable, str(scripts / "install_code_intelligence_global.py"),
                       "--python", sys.executable, "--destination", str(destination)]
            subprocess.run(command, check=True, env=env, capture_output=True, text=True)
            runtime = destination / "runtime"
            result = subprocess.run([sys.executable, "-c",
                "import pr_duplicates; print(pr_duplicates.ownership.GoOwnership.__name__)"],
                cwd=runtime, env=env, check=True, capture_output=True, text=True)
            self.assertEqual(result.stdout.strip(), "GoOwnership")
            helper = runtime / "go_owner_bindings"
            sources = sorted(p.name for p in helper.glob("*.go"))
            self.assertTrue(sources)
            self.assertFalse(any(p.endswith("_test.go") for p in sources))
            self.assertFalse((helper / "go.mod").exists())
            subprocess.run(["go", "build", "-o", str(root / "bindings"), *sources],
                           cwd=helper, env=env, check=True, capture_output=True, text=True)
            # A renamed/deleted source must not survive a later deployment.
            stale = helper / "obsolete.go"
            stale.write_text("package main\nfunc main() {}\n")
            subprocess.run(command, check=True, env=env, capture_output=True, text=True)
            self.assertFalse(stale.exists())


if __name__ == "__main__":
    unittest.main()
