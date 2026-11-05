"""Check that make-test shards partition the discovered Go package set."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).with_name("make-test-shard.sh").resolve()
BASH = os.environ.get("TEST_BASH", "bash")
SHARDS = ("cypher", "storage", "server", "storagecensus", "capturecensus", "command", "other")
PACKAGES = ("example/pkg/cypher", "example/pkg/cypher/antlr", "example/pkg/storage", "example/pkg/storage/lifecycle",
            "example/pkg/server", "example/testing/storagecensus", "example/testing/capturecensus", "example/cmd/nornicdb",
            "example/pkg/nornicdb", "example/pkg/newly_added", "example/testing/new_package")


class Shards(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        directory = Path(self.temp.name)
        fake = directory / "go"
        fake.write_text("#!/usr/bin/env bash\nif [[ $1 == list ]]; then\n  [[ ${FAIL_LIST:-0} != 1 ]] || exit 7\n  [[ ${EMPTY_LIST:-0} != 1 ]] || exit 0\n  printf '%s\\n' " + " ".join(PACKAGES) + "\nelse\n  printf '%s\\n' \"$@\" >> \"$SHARD_LOG\"\nfi\n", encoding="utf-8")
        fake.chmod(0o755)
        self.log = directory / "run.log"
        self.env = dict(os.environ, PATH=str(directory) + os.pathsep + os.environ["PATH"], SHARD_LOG=str(self.log))

    def run_shard(self, shard, **env):
        return subprocess.run([BASH, str(SCRIPT), shard], env={**self.env, **env}, text=True, capture_output=True)

    def test_all_discovered_packages_run_once_with_make_test_flags(self):
        for shard in SHARDS:
            result = self.run_shard(shard)
            self.assertEqual(result.returncode, 0, result.stderr)
        lines = self.log.read_text().splitlines()
        self.assertCountEqual([line for line in lines if line.startswith("example/")], PACKAGES)
        self.assertEqual(lines.count("test"), len(SHARDS))
        self.assertEqual(lines.count("-p"), len(SHARDS))
        self.assertEqual(lines.count("-parallel"), len(SHARDS))
        self.assertEqual(lines.count("30m"), len(SHARDS))

    def test_failed_package_discovery_fails(self):
        self.assertNotEqual(self.run_shard("other", FAIL_LIST="1").returncode, 0)

    def test_empty_package_discovery_fails(self):
        self.assertNotEqual(self.run_shard("other", EMPTY_LIST="1").returncode, 0)

    def test_invalid_shard_fails(self):
        self.assertNotEqual(self.run_shard("missing").returncode, 0)


if __name__ == "__main__":
    unittest.main()
