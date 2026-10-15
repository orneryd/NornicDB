#!/usr/bin/env bash
# Exercise the actual path matcher with enough output to fill a pipe buffer.
# Variables are consumed by the functions loaded from verify.sh below.
# shellcheck disable=SC2034
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
verify="${1:-${root}/scripts/verify.sh}"

# Load only the matcher and its path filters; do not fetch or run checks.
# shellcheck disable=SC1091
source /dev/stdin <<<"$(sed -n '/^workflow_of() {/,/^}/p; /^watched() {/,/^}/p; /^triggered() {/,/^}/p; /^step_allocations() {/,/^}/p' "${verify}")"
ALL=0
CHANGED=""
paths="$(awk 'BEGIN { for (i = 1; i < 200000; i++) print "unwatched/path-" i }')"
for pair in 'build:pkg/cypher/changed.go' 'conformance:pkg/cypher/changed.go' 'docs:docs/changed.md'; do
  step="${pair%%:*}"
  path="${pair#*:}"
  for position in first last; do
    if [[ "${position}" == first ]]; then CHANGED="${path}"$'\n'"${paths}";
    else CHANGED="${paths}"$'\n'"${path}"; fi
    if ! triggered "${step}"; then
      echo "FAIL: ${step} skipped with matching path ${position} in 200000 changed paths" >&2
      exit 1
    fi
  done
done
CHANGED="${paths}"$'\n''unwatched/final'
for step in build conformance docs; do
  if triggered "${step}"; then echo "FAIL: ${step} matched unwatched paths" >&2; exit 1; fi
done
CHANGED=""
if triggered build; then echo 'FAIL: empty changes matched' >&2; exit 1; fi
ALL=1
triggered build
echo 'PASS: verify path filters (200000 paths, first/last match, no match, empty, ALL=1)'

# Exercise allocation worktree selection against a base that moved after the
# branch forked. Substitute only the benchmark runner, keeping real Git refs
# and worktrees so this stays a fast local test without running benchmarks.
fixture="$(mktemp -d)"
trap 'rm -rf "${fixture}"' EXIT
git init --quiet "${fixture}/repo"
cd "${fixture}/repo"
git config user.name 'Verify self-test'
git config user.email 'verify-test@example.invalid'
git commit --quiet --allow-empty -m base
fork="$(git rev-parse HEAD)"
git checkout --quiet -b branch
git commit --quiet --allow-empty -m branch
HEAD_SHA="$(git rev-parse HEAD)"
git checkout --quiet -b moved-base "${fork}"
git commit --quiet --allow-empty -m 'base moved'
BASE=moved-base
MERGE_BASE="$(git merge-base "${BASE}" branch)"
STATE_DIR="${fixture}/state"
LOG_DIR="${fixture}/logs"

python3() {
  # The first invocation checks the runner itself; the second names both trees.
  [[ "$2" == --self-test ]] && return 0
  [[ "$2" == --base && "$4" == --head ]] || return 1
  local actual_base actual_head
  actual_base="$(git -C "$3" rev-parse HEAD)"
  actual_head="$(git -C "$5" rev-parse HEAD)"
  if [[ "${actual_base}" != "${MERGE_BASE}" || "${actual_head}" != "${HEAD_SHA}" ]]; then
    echo "FAIL: allocation trees: base ${actual_base}, expected merge base ${MERGE_BASE}; head ${actual_head}" >&2
    return 1
  fi
}
step_allocations
echo 'PASS: allocation base is the branch merge base when the base tip moves'
