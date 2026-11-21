"""Report covered Go instrumentation blocks intersecting a Git diff.

Usage: python diff_coverage.py BASE COVERPROFILE
This is block-overlap coverage, not an exact changed-line or branch metric.
"""
import collections
import re
import subprocess
import sys

if len(sys.argv) != 3:
    raise SystemExit(__doc__)
base, profile = sys.argv[1:]
diff = subprocess.check_output(
    ["git", "diff", "--unified=0", f"{base}...HEAD", "--", "pkg/*.go"],
    text=True,
)
changed = collections.defaultdict(set)
path = None
line = 0
for entry in diff.splitlines():
    if entry.startswith("+++ b/"):
        path = entry[6:]
    elif entry.startswith("@@"):
        line = int(re.search(r"\+(\d+)(?:,\d+)?", entry).group(1))
    elif entry.startswith("+") and not entry.startswith("+++"):
        if path and not path.endswith("_test.go"):
            changed[path].add(line)
        line += 1
    elif not entry.startswith(("-", "\\")):
        line += 1

blocks = collections.defaultdict(list)
with open(profile, encoding="utf-8") as source:
    for entry in source:
        match = re.match(
            r"github.com/orneryd/nornicdb/(.*?):(\d+)\.\d+,(\d+)\.\d+\s+(\d+)\s+(\d+)",
            entry,
        )
        if match:
            path, first, last, statements, hits = match.groups()
            blocks[path].append(tuple(map(int, (first, last, statements, hits))))

covered = total = 0
for path, lines in sorted(changed.items()):
    selected = [(n, hit) for first, last, n, hit in blocks[path]
                if any(first <= line <= last for line in lines)]
    if not selected:
        print(f"{path}: no changed lines overlap instrumented blocks")
        continue
    count = sum(n for n, _ in selected)
    hits = sum(n for n, hit in selected if hit)
    covered += hits
    total += count
    print(f"{path}: {hits}/{count} {100 * hits / count:.1f}%")
if total == 0:
    raise SystemExit("No changed production blocks intersect the profile")
print(f"Overall changed-overlap blocks: {covered}/{total} {100 * covered / total:.1f}%")
