#!/usr/bin/env bash
#
# Run the checks GitHub Actions runs for a pull request into develop, locally,
# with one summary at the end. See `make verify-help`.
#
#   ci.yml                    localization, ui, llama-libs, build, tests
#   cypher-conformance.yml    allocations, conformance, differential
#   docs-pages.yml            docs
#
# Like CI, a step runs only when the branch changes a path its workflow
# watches (compared with the merge base of BASE). Image builds and releases
# (cd*.yml, release-macos.yml) publish artifacts and are not checks.
#
# Environment (the make variables of the same name without VERIFY_):
#   VERIFY_BRANCH      verify this branch/ref in a worktree under .verify/ (default: the current checkout)
#   VERIFY_BASE        compare against this ref (default: origin/develop)
#                      release branches: VERIFY_BASE=origin/main (make verify BASE=origin/main)
#   VERIFY_ONLY        run only these steps, e.g. "build tests"
#   VERIFY_SKIP        skip these steps
#   VERIFY_ALL=1       ignore path filters and run every step
#   VERIFY_FAIL_FAST=1 stop at the first failure (default: run everything, report at the end)
#   VERIFY_GROUPS      only these test groups (scripts/ci/test-groups.sh --list)
#   VERIFY_FETCH=0     do not `git fetch` BASE first
#   VERIFY_LIST=1      show the plan and exit
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STEPS="localization ui llama-libs build tests allocations conformance differential docs"
BASE="${VERIFY_BASE:-origin/develop}"
BRANCH="${VERIFY_BRANCH:-}"
ONLY=" ${VERIFY_ONLY:-} "
SKIP=" ${VERIFY_SKIP:-} "
ALL="${VERIFY_ALL:-0}"
FAIL_FAST="${VERIFY_FAIL_FAST:-0}"
LIST="${VERIFY_LIST:-0}"
RUN_ID="$(date +%Y%m%d-%H%M%S)"
STATE_DIR="${ROOT}/.verify"
LOG_DIR="${STATE_DIR}/runs/${RUN_ID}"

if [[ -t 1 ]]; then
  BOLD=$'\033[1m'; DIM=$'\033[2m'; RED=$'\033[31m'; GREEN=$'\033[32m'; YELLOW=$'\033[33m'; RESET=$'\033[0m'
else
  BOLD=""; DIM=""; RED=""; GREEN=""; YELLOW=""; RESET=""
fi
say()  { printf '%s\n' "$*"; }
note() { printf '%s%s%s\n' "${DIM}" "$*" "${RESET}"; }
warn() { printf '%swarning:%s %s\n' "${YELLOW}" "${RESET}" "$*" >&2; }
die()  { printf '%serror:%s %s\n' "${RED}" "${RESET}" "$*" >&2; exit 2; }

# --- what each step is -------------------------------------------------------

workflow_of() {
  case "$1" in
    localization|ui|llama-libs|build|tests) echo ci ;;
    allocations|conformance|differential) echo conformance ;;
    docs) echo docs ;;
  esac
}

describe() {
  case "$1" in
    localization) echo "localization catalogs: tests, generated manifest up to date, catalog check" ;;
    ui)           echo "UI assets: npm ci && npm run build" ;;
    llama-libs)   echo "llama CPU libs (Linux amd64 only; needs Docker)" ;;
    build)        echo "make build" ;;
    tests)        echo "all test groups with coverage, as CI (scripts/ci/test-groups.sh)" ;;
    allocations)  echo "allocation ratchet against the merge base (fixed-count workloads, 1% B/op tolerance)" ;;
    conformance)  echo "make cypher-conformance (pinned TCK corpus + regressions)" ;;
    differential) echo "make cypher-differential (fixed corpus vs pinned Neo4j; needs Docker)" ;;
    docs)         echo "mkdocs build --strict (pushes to main)" ;;
  esac
}

# The `paths:` filters of each workflow, as extended regexes over changed files.
watched() {
  case "$1" in
    ci)          echo '^(docker|lib|macos|pkg|plugins|scripts|ui)/|^cmd/nornicdb/|^cmd/nornicdb-admin/|^go\.(mod|sum)$|^Makefile$|^\.github/workflows/ci\.yml$' ;;
    conformance) echo '^pkg/|^testing/cypher/|^scripts/cypher-tck/|^docs/plans/cypher-convergence-[^/]*\.md$|^openspec/|^\.github/workflows/cypher-conformance\.yml$|^go\.(mod|sum)$|^Makefile$' ;;
    docs)        echo '^docs/|^mkdocs\.yml$|^\.github/workflows/docs-pages\.yml$|^requirements-docs\.txt$' ;;
  esac
}

# --- which tree we verify ------------------------------------------------------

cd "${ROOT}" || die "cannot enter ${ROOT}"
if [[ "${VERIFY_FETCH:-1}" == "1" && "${BASE}" == origin/* ]]; then
  note "fetching ${BASE#origin/} from origin ..."
  git fetch --quiet origin "${BASE#origin/}" || warn "could not fetch ${BASE}; using the local copy"
fi
git rev-parse --verify --quiet "${BASE}^{commit}" >/dev/null || die "BASE ${BASE} is not a commit"

TREE="${ROOT}"
DIRTY=""
if [[ -n "${BRANCH}" ]]; then
  ref="${BRANCH}"
  if ! git rev-parse --verify --quiet "${ref}^{commit}" >/dev/null; then
    git fetch --quiet origin "${BRANCH}" 2>/dev/null && ref="FETCH_HEAD"
  fi
  git rev-parse --verify --quiet "${ref}^{commit}" >/dev/null || die "cannot find branch or ref ${BRANCH} (locally or on origin)"
  sha="$(git rev-parse "${ref}^{commit}")"
  TREE="${STATE_DIR}/worktrees/$(printf '%s' "${BRANCH}" | tr -c 'A-Za-z0-9._-' '_')"
  if [[ -d "${TREE}" ]]; then
    git -C "${TREE}" checkout --quiet --force --detach "${sha}" || die "cannot update worktree ${TREE}"
    git -C "${TREE}" clean --quiet -fd -e node_modules -e lib/llama
  else
    mkdir -p "${STATE_DIR}/worktrees"
    git worktree add --quiet --detach "${TREE}" "${sha}" || die "cannot create worktree ${TREE}"
  fi
  LABEL="${BRANCH} @ ${sha:0:9}"
else
  LABEL="$(git rev-parse --abbrev-ref HEAD) @ $(git rev-parse --short HEAD)"
  if [[ -n "$(git status --porcelain --untracked-files=normal)" ]]; then
    DIRTY="1"
  fi
fi

HEAD_SHA="$(git -C "${TREE}" rev-parse HEAD)"
MERGE_BASE="$(git -C "${TREE}" merge-base "${BASE}" HEAD 2>/dev/null || true)"
[[ -n "${MERGE_BASE}" ]] || die "${LABEL} has no merge base with ${BASE}"

# Treat renames as deletion/addition so both paths reach the single matcher.
CHANGED="$(git -C "${TREE}" diff --no-renames --name-only "${MERGE_BASE}" HEAD)"
if [[ -n "${DIRTY}" ]]; then
  CHANGED="$(printf '%s\n%s\n%s' "${CHANGED}" "$(git -C "${TREE}" diff --no-renames --name-only HEAD)" \
    "$(git -C "${TREE}" ls-files --others --exclude-standard)" | sed '/^$/d' | sort -u)"
fi
CHANGED_COUNT="$(printf '%s\n' "${CHANGED}" | sed '/^$/d' | wc -l | tr -d ' ')"

triggered() {
  [[ "${ALL}" == "1" ]] && return 0
  grep -Eq "$(watched "$(workflow_of "$1")")" <<<"${CHANGED}"
}

selected() {
  local step="$1"
  if [[ "${ONLY}" != "  " && "${ONLY}" != *" ${step} "* ]]; then return 1; fi
  if [[ "${SKIP}" == *" ${step} "* ]]; then return 1; fi
  return 0
}

for word in ${VERIFY_ONLY:-} ${VERIFY_SKIP:-}; do
  [[ " ${STEPS} " == *" ${word} "* ]] || die "unknown step '${word}' (steps: ${STEPS})"
done

# --- the plan -------------------------------------------------------------------

say "${BOLD}verify${RESET} ${LABEL}  against ${BASE} (merge base ${MERGE_BASE:0:9}, ${CHANGED_COUNT} changed files)"
[[ -n "${BRANCH}" ]] && note "worktree: ${TREE}"
if [[ -n "${DIRTY}" ]]; then
  warn "uncommitted changes are included in the path filters and in every step except 'allocations', which measures commits; commit before pulling in."
fi
PLAN=""
for step in ${STEPS}; do
  if ! selected "${step}"; then
    reason="not selected"
  elif ! triggered "${step}"; then
    reason="no changes under $(workflow_of "${step}") paths"
  else
    PLAN="${PLAN} ${step}"; reason=""
  fi
  if [[ -n "${reason}" ]]; then
    printf '  %s%-13s skip  (%s)%s\n' "${DIM}" "${step}" "${reason}" "${RESET}"
  else
    printf '  %-13s %srun%s   %s\n' "${step}" "${GREEN}" "${RESET}" "$(describe "${step}")"
  fi
done
if [[ -z "${PLAN}" ]]; then
  say ""
  say "Nothing to run. Use ALL=1 to run every check regardless of the changed paths."
  exit 0
fi
[[ "${LIST}" == "1" ]] && exit 0

# --- tool checks ----------------------------------------------------------------

CI_GO="$(sed -n 's/.*go-version: *"\([0-9.]*\)".*/\1/p' "${TREE}/.github/workflows/ci.yml" | sed -n '1p')"
if command -v go >/dev/null; then
  local_go="$(go env GOVERSION 2>/dev/null | sed 's/^go//')"
  [[ -n "${CI_GO}" && "${local_go}" != "${CI_GO}" ]] && warn "Go ${local_go} here, CI uses ${CI_GO}"
else
  die "go is not installed"
fi
needs_plan() { [[ " ${PLAN} " == *" $1 "* ]]; }
if needs_plan ui; then
  command -v npm >/dev/null || die "npm is needed for the ui step"
  node_major="$(node -p 'process.versions.node.split(".")[0]' 2>/dev/null || echo "?")"
  [[ "${node_major}" != "20" ]] && warn "Node ${node_major} here, CI uses 20"
fi
DOCKER_OK=0
if docker info >/dev/null 2>&1; then DOCKER_OK=1; fi
if (needs_plan differential || needs_plan llama-libs) && [[ "${DOCKER_OK}" != "1" ]]; then
  warn "Docker is not running: 'differential' (and 'llama-libs' on Linux) will fail"
fi

# --- the steps --------------------------------------------------------------------

step_localization() {
  go test ./pkg/localization ./scripts/localization_catalog ./scripts/localization_inventory &&
    go generate ./pkg/localization &&
    { git diff --exit-code -- pkg/localization/catalog_manifest_gen.go ||
      { echo "pkg/localization/catalog_manifest_gen.go is stale: run 'go generate ./pkg/localization' and commit it"; false; }; } &&
    go run ./scripts/localization_catalog -check
}

step_ui() {
  (cd ui && npm ci && npm run build)
}

step_llama_libs() {
  if [[ "$(uname -s)" != "Linux" || "$(uname -m)" != "x86_64" ]]; then
    echo "CI hydrates the Linux amd64 llama libraries; not needed on $(uname -s)/$(uname -m)."
    return 3
  fi
  [[ "${DOCKER_OK}" == "1" ]] || { echo "Docker is not running"; return 1; }
  bash scripts/hydrate-llama-cpu-libs.sh
}

step_build() {
  make build
}

step_tests() {
  local keep=1
  [[ "${FAIL_FAST}" == "1" ]] && keep=0
  TEST_GROUPS_KEEP_GOING="${keep}" TEST_GROUPS_ONLY="${VERIFY_GROUPS:-}" \
    bash scripts/ci/test-groups.sh "${LOG_DIR}/coverage.raw.out" "${LOG_DIR}/coverage.out"
}

# Base and head are measured in throwaway worktrees: the ratchet writes a
# benchmark fixture into each checkout, and CI's baseline resolution runs a
# shallow fetch that would turn a full clone shallow.
step_allocations() {
  local base_sha head_dir base_dir
  base_sha="${MERGE_BASE}"
  head_dir="${STATE_DIR}/worktrees/allocations-head"
  base_dir="${STATE_DIR}/worktrees/allocations-base"
  for pair in "${head_dir}:${HEAD_SHA}" "${base_dir}:${base_sha}"; do
    local dir="${pair%%:*}" sha="${pair##*:}"
    if [[ -d "${dir}" ]]; then
      git -C "${dir}" checkout --quiet --force --detach "${sha}" && git -C "${dir}" clean --quiet -fd || return 1
    else
      mkdir -p "${STATE_DIR}/worktrees" && git worktree add --quiet --detach "${dir}" "${sha}" || return 1
    fi
  done
  echo "base ${base_sha} (merge base with ${BASE}), head ${HEAD_SHA}"
  python3 "${head_dir}/scripts/cypher-tck/allocation_ratchet.py" --self-test &&
    python3 "${head_dir}/scripts/cypher-tck/allocation_ratchet.py" \
      --base "${base_dir}" --head "${head_dir}" --parser nornic \
      --operations 1000 --samples 5 --tolerance-bytes-pct 1.0 \
      --artifacts "${LOG_DIR}/allocations"
}

step_conformance() {
  make cypher-conformance
}

step_differential() {
  [[ "${DOCKER_OK}" == "1" ]] || { echo "Docker is not running (the pinned Neo4j runs in a container)"; return 1; }
  make cypher-differential
}

step_docs() {
  local venv="${STATE_DIR}/docs-venv" stamp
  stamp="$(shasum requirements-docs.txt | cut -d' ' -f1)"
  if [[ ! -x "${venv}/bin/mkdocs" || "$(cat "${venv}/.requirements" 2>/dev/null)" != "${stamp}" ]]; then
    python3 -m venv "${venv}" && "${venv}/bin/pip" install --quiet -r requirements-docs.txt || return 1
    echo "${stamp}" > "${venv}/.requirements"
  fi
  "${venv}/bin/mkdocs" build --strict --site-dir "${LOG_DIR}/site"
}

# --- run ----------------------------------------------------------------------------

mkdir -p "${LOG_DIR}"
ln -sfn "${LOG_DIR}" "${STATE_DIR}/latest"
RESULTS=""
FAILED=0
STARTED_ALL=$SECONDS
for step in ${PLAN}; do
  log="${LOG_DIR}/${step}.log"
  say ""
  say "${BOLD}▶ ${step}${RESET}  $(describe "${step}")"
  started=$SECONDS
  ( cd "${TREE}" && "step_${step//-/_}" ) > >(tee "${log}") 2>&1
  status=$?
  elapsed=$((SECONDS - started))
  case "${status}" in
    0) result="pass" ;;
    3) result="n/a" ;;
    *) result="FAIL"; FAILED=$((FAILED + 1)) ;;
  esac
  RESULTS="${RESULTS}${step} ${result} ${elapsed}"$'\n'
  if [[ "${result}" == "FAIL" && "${FAIL_FAST}" == "1" ]]; then
    break
  fi
done

say ""
say "${BOLD}verify summary${RESET}  ${LABEL}  ($(( (SECONDS - STARTED_ALL) / 60 ))m $(( (SECONDS - STARTED_ALL) % 60 ))s)"
printf '%s' "${RESULTS}" | while read -r step result elapsed; do
  case "${result}" in
    pass) color="${GREEN}" ;;
    FAIL) color="${RED}" ;;
    *)    color="${DIM}" ;;
  esac
  printf '  %-13s %s%-4s%s %5ss   %s\n' "${step}" "${color}" "${result}" "${RESET}" "${elapsed}" \
    "$([[ "${result}" == "FAIL" ]] && echo "${LOG_DIR}/${step}.log")"
done
for step in ${STEPS}; do
  [[ " ${PLAN} " == *" ${step} "* ]] || printf '  %s%-13s skip%s\n' "${DIM}" "${step}" "${RESET}"
done
note "logs: ${LOG_DIR}  (also .verify/latest)"
if (( FAILED > 0 )); then
  say "${RED}${BOLD}${FAILED} check(s) failed.${RESET}"
  exit 1
fi
say "${GREEN}${BOLD}All checks passed.${RESET}"
