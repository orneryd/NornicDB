"""Changed-line coverage, the way Coveralls sees it.

usage: python -I changedcov.py <diff.txt> <coverprofile> [module-prefix]

diff.txt is `git diff -U0 origin/main HEAD -- '*.go' ':!*_test.go'` of the
worktree; coverprofile comes from `go test -coverprofile` run per package
with only that package's tests (no -coverpkg), as CI runs it. Prints every
changed line that sits in an uncovered block, and the covered share of the
changed lines that are in any block.
"""
import re
import sys

diff_path, profile_path = sys.argv[1], sys.argv[2]
prefix = sys.argv[3] if len(sys.argv) > 3 else "github.com/orneryd/nornicdb/"

changed = {}
current = None
for line in open(diff_path, encoding="utf-8", errors="replace"):
    if line.startswith("+++ "):
        name = line[4:].strip()
        current = name[2:] if name.startswith("b/") else None
        continue
    match = re.match(r"@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@", line)
    if match and current:
        start, count = int(match.group(1)), int(match.group(2) or 1)
        changed.setdefault(current, set()).update(range(start, start + count))

blocks = {}
for line in open(profile_path, encoding="utf-8"):
    match = re.match(r"(.+?):(\d+)\.\d+,(\d+)\.\d+ \d+ (\d+)$", line.strip())
    if not match:
        continue
    path = match.group(1)
    if path.startswith(prefix):
        path = path[len(prefix):]
    blocks.setdefault(path, []).append((int(match.group(2)), int(match.group(3)), int(match.group(4)) > 0))

total = covered = 0
for path in sorted(changed):
    for number in sorted(changed[path]):
        states = [hit for start, end, hit in blocks.get(path, []) if start <= number <= end]
        if not states:
            continue
        total += 1
        if any(states):
            covered += 1
        else:
            print(f"UNCOVERED {path}:{number}")
print(f"changed lines in blocks: {total}, covered: {covered} ({100.0 * covered / total if total else 100:.1f}%)")
