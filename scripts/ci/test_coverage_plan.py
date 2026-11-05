"""Compare serial coverage groups with the per-group CI execution plan."""
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
BASH = os.environ.get('TEST_BASH', 'bash')


class CoveragePlan(unittest.TestCase):
    def test_each_listed_group_matches_the_serial_run(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / 'scripts/ci').mkdir(parents=True)
            shutil.copy(ROOT / 'scripts/ci/test-groups.sh', root / 'scripts/ci/test-groups.sh')
            fake = root / 'scripts/generate-coverage.sh'
            fake.write_text('''#!/usr/bin/env bash
set -euo pipefail
if [[ $1 == --append ]]; then
  [[ -s $2 ]] || printf 'mode: set\\n' > "$2"
  printf '%s\\n' "$3" >> "$2"
  printf '%s\\n' "$3" >> "$CALL_LOG"
else
  cp "$2" "$3"
fi
''', encoding='utf-8')
            env = dict(os.environ, CALL_LOG=str(root / 'calls.log'))
            def run(*args, **extra):
                return subprocess.run([BASH, 'scripts/ci/test-groups.sh', *args], cwd=root,
                                      env={**env, **extra}, text=True, capture_output=True, check=True)
            groups = run('--list').stdout.splitlines()
            self.assertTrue(groups)
            self.assertEqual(len(groups), len(set(groups)))
            run('serial.raw', 'serial.out')
            expected = (root / 'serial.raw').read_text().splitlines()
            (root / 'calls.log').unlink()
            combined = ['mode: set']
            for group in groups:
                run('part.raw', 'part.out', TEST_GROUPS_ONLY=group)
                lines = (root / 'part.raw').read_text().splitlines()
                self.assertEqual(lines[0], 'mode: set')
                combined.extend(lines[1:])
            self.assertEqual(combined, expected)
            self.assertEqual((root / 'calls.log').read_text().splitlines(), groups)


if __name__ == '__main__':
    unittest.main()
