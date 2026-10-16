#!/usr/bin/env bash
# Exercise the actual path matcher with enough output to fill a pipe buffer.
# Variables are consumed by the functions loaded from verify.sh below.
# shellcheck disable=SC2034
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
verify="${1:-${root}/scripts/verify.sh}"
# Resolve caller-relative overrides before entering any fixture directories.
verify="$(cd "$(dirname "${verify}")" && pwd)/$(basename "${verify}")"

# Load only the matcher and its path filters; do not fetch or run checks.
# shellcheck disable=SC1091
source /dev/stdin <<<"$(sed -n '/^workflow_of() {/,/^}/p; /^watched() {/,/^}/p; /^triggered() {/,/^}/p; /^selected() {/,/^}/p; /^step_allocations() {/,/^}/p' "${verify}")"
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

# Check every mapped step, including exclusions and selection precedence.
steps='localization ui llama-libs build tests allocations conformance differential docs'
ci_steps='localization ui llama-libs build tests'
conformance_steps='allocations conformance differential'
ALL=0
ONLY='  '
SKIP='  '
expect_steps() {
  local expected="$1" step actual=''
  for step in ${steps}; do
    if selected "${step}" && triggered "${step}"; then actual="${actual:+${actual} }${step}"; fi
  done
  if [[ "${actual}" != "${expected}" ]]; then
    echo "FAIL: paths '${CHANGED}', ALL=${ALL}, ONLY='${ONLY}', SKIP='${SKIP}': steps '${actual}', expected '${expected}'" >&2
    exit 1
  fi
}
for CHANGED in Makefile .github/workflows/ci.yml; do
  expected="${ci_steps}"
  [[ "${CHANGED}" != Makefile ]] || expected="${expected} ${conformance_steps}"
  expect_steps "${expected}"
done
for CHANGED in Makefile.bak docs/Makefile.md .github/workflows/ci.yml.orig nested/Makefile nested/.github/workflows/ci.yml; do
  expected=''
  [[ "${CHANGED}" != docs/Makefile.md ]] || expected=docs
  expect_steps "${expected}"
done
CHANGED=Makefile
ONLY=' tests conformance '
expect_steps 'tests conformance'
SKIP=' conformance '
expect_steps tests
CHANGED=''
expect_steps ''
ALL=1
expect_steps tests
ONLY='  '
expect_steps 'localization ui llama-libs build tests allocations differential docs'
SKIP='  '
expect_steps "${steps}"
echo 'PASS: build input gates (exact paths, every mapped step, near misses, ALL/ONLY/SKIP precedence)'

# Exercise allocation worktree selection against a base that moved after the
# branch forked. Substitute only the benchmark runner, keeping real Git refs
# and worktrees so this stays a fast local test without running benchmarks.
fixture="$(mktemp -d)"
fixture="$(cd "${fixture}" && pwd)"
trap 'rm -rf "${fixture}"' EXIT

# Run the real verifier in plan-only mode on isolated, committed file changes.
# No tools or checks should run; each plan must select exactly the mapped steps.
mkdir -p "${fixture}/plan/scripts"
cp "${verify}" "${fixture}/plan/scripts/verify.sh"
git init --quiet "${fixture}/plan"
cd "${fixture}/plan"
git config user.name 'Verify self-test'
git config user.email 'verify-test@example.invalid'
git add scripts/verify.sh
git commit --quiet -m 'plan fixture base'
plan_base="$(git rev-parse HEAD)"
for path in Makefile .github/workflows/ci.yml; do
  git checkout --quiet --detach "${plan_base}"
  mkdir -p "$(dirname "${path}")"
  printf '# isolated build input change\n' > "${path}"
  git add "${path}"
  git commit --quiet -m "change only ${path}"
  plan="$(VERIFY_BASE="${plan_base}" VERIFY_FETCH=0 VERIFY_LIST=1 VERIFY_ALL=0 \
    VERIFY_ONLY='' VERIFY_SKIP='' VERIFY_BRANCH='' bash scripts/verify.sh)"
  actual="$(awk '$2 == "run" { steps = steps (steps ? " " : "") $1 } END { print steps }' <<<"${plan}")"
  expected="${ci_steps}"
  [[ "${path}" != Makefile ]] || expected="${expected} ${conformance_steps}"
  if [[ "${actual}" != "${expected}" || "${plan}" == *'Nothing to run'* || "${plan}" != *'1 changed files)'* ]]; then
    echo "FAIL: isolated ${path} plan: expected '${expected}', got:" >&2
    echo "${plan}" >&2
    exit 1
  fi
done
echo 'PASS: plan-only verifier selects checks for isolated Makefile and CI workflow changes'

# Preserve both names of real detected renames, in dirty and committed diffs.
rename_case=0
for path in Makefile .github/workflows/ci.yml near-miss; do
  expected="${ci_steps}"
  [[ "${path}" != Makefile ]] || expected="${expected} ${conformance_steps}"
  [[ "${path}" != near-miss ]] || expected=''
  for direction in from to; do
    rename_case=$((rename_case + 1))
    mkdir -p "${fixture}/rename-${rename_case}/scripts"
    cp "${verify}" "${fixture}/rename-${rename_case}/scripts/verify.sh"
    cd "${fixture}/rename-${rename_case}"
    git init --quiet
    git config user.name 'Verify self-test'
    git config user.email 'verify-test@example.invalid'
    git config diff.renames true
    old="${path}"
    new="${path}.old copy"
    if [[ "${path}" == near-miss ]]; then
      old=Makefile.bak
      new=.github/workflows/ci.yml.orig
    fi
    if [[ "${direction}" == to ]]; then swap="${old}"; old="${new}"; new="${swap}"; fi
    mkdir -p "$(dirname "${old}")" "$(dirname "${new}")"
    printf '# unchanged rename fixture\n' > "${old}"
    git add .
    git commit --quiet -m 'rename fixture base'
    rename_base="$(git rev-parse HEAD)"
    git mv "${old}" "${new}"
    for state in staged committed; do
      if [[ "${state}" == committed ]]; then git commit --quiet -m 'rename input'; fi
      # Ensure this exercises rename detection, rather than an unrelated add/delete.
      summary="$(git diff -M --summary "${rename_base}")"
      if [[ "${summary}" != *'rename '*'(100%)'* ]]; then
        echo "FAIL: fixture did not detect ${state} rename ${old} -> ${new}" >&2
        exit 1
      fi
      plan="$(VERIFY_BASE="${rename_base}" VERIFY_FETCH=0 VERIFY_LIST=1 VERIFY_ALL=0 \
        VERIFY_ONLY='' VERIFY_SKIP='' VERIFY_BRANCH='' bash scripts/verify.sh)"
      actual="$(awk '$2 == "run" { steps = steps (steps ? " " : "") $1 } END { print steps }' <<<"${plan}")"
      if [[ "${actual}" != "${expected}" ]]; then
        echo "FAIL: ${state} rename ${old} -> ${new}: expected '${expected}', got:" >&2
        echo "${plan}" >&2
        exit 1
      fi
    done
  done
done
echo 'PASS: staged/committed renames to/from build inputs and near misses select exactly their checks'

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

# Re-run from the repository root with a caller-relative override. Supplying
# the argument prevents recursion; all fixture copies must still find it.
if (( $# == 0 )); then
  (cd "${root}" && bash scripts/ci/verify_selftest.sh scripts/verify.sh)
  echo 'PASS: relative verifier override survives fixture directory changes'
fi
