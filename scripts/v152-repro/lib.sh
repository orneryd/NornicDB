# Shared steps of the v1.5.2 upgrade-store reproductions (research scripts, not for merge).
# Sourced by the item scripts in this directory. Each item script:
#   1. writes ONE scenario with the old build's writer, at the writer commit, into a fresh temporary root;
#   2. runs the reader tests of THIS checkout (the candidate build) against that root;
#   3. prints one line:  REPRO <item> candidate=<label> <sha> RESULT=PASS|FAIL <detail>
# Exit status: 0 PASS (the claimed defect does not occur), 1 FAIL (it occurs), 2 the harness could not run.
#
# Needs: git, go (the version in go.mod), python3, GNU cp with --sparse, and the writer commit in this
# repository (it is an ancestor of this branch, so a clone of the branch has it).
# Environment (all optional):
#   REPRO_TMP    parent of the temporary root; default /work/tnrun/v152-upg-repro when writable, else $TMPDIR or /tmp
#   REPRO_KEEP=1 keep the temporary root (its path is printed)
#   GOFLAGS      default "-p=2 -tags=noui,nolocalllm" (the flags of the lab runs)

set -uo pipefail

REPRO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(git -C "$REPRO_DIR" rev-parse --show-toplevel)"
export GOFLAGS="${GOFLAGS:--p=2 -tags=noui,nolocalllm}"
CANDIDATE_LABEL="$(sed -n 's/^label=//p' "$REPRO_DIR/CANDIDATE")"
CANDIDATE_SHA="$(sed -n 's/^sha=//p' "$REPRO_DIR/CANDIDATE")"

say() { echo "REPRO_STEP $*"; }

harness_fail() {
  echo "REPRO ${ITEM:-?} candidate=${CANDIDATE_LABEL} ${CANDIDATE_SHA} RESULT=HARNESS-ERROR $*"
  exit 2
}

repro_result() { # PASS|FAIL detail...
  local r=$1; shift
  echo "REPRO ${ITEM} candidate=${CANDIDATE_LABEL} ${CANDIDATE_SHA} RESULT=$r $*"
  [[ $r == PASS ]] && exit 0 || exit 1
}

repro_root() {
  local parent="${REPRO_TMP:-}"
  if [[ -z $parent ]]; then
    if mkdir -p /work/tnrun/v152-upg-repro 2>/dev/null && [[ -w /work/tnrun/v152-upg-repro ]]; then
      parent=/work/tnrun/v152-upg-repro
    else
      parent="${TMPDIR:-/tmp}"
    fi
  fi
  ROOT="$(mktemp -d "$parent/repro-XXXXXX")" || harness_fail "cannot create a temporary root under $parent"
  if [[ ${REPRO_KEEP:-0} != 1 ]]; then trap 'rm -rf "$ROOT"' EXIT; fi
  say "root=$ROOT"
  cp --help 2>&1 | grep -q -- '--sparse' || harness_fail "cp does not support --sparse (GNU coreutils needed)"
  command -v go >/dev/null || harness_fail "go not on PATH"
  command -v python3 >/dev/null || harness_fail "python3 not on PATH (the writer's journal scan uses it)"
  say "candidate=$CANDIDATE_LABEL $CANDIDATE_SHA head=$(git -C "$REPO" rev-parse HEAD) go=$(go env GOVERSION)"
}

# write_store <build> <writer-commit> <scenario>[,<scenario>...]
# Runs the old build's Bolt writer (TestUORBoltWrite) at <writer-commit> with UOR_ROOT=$ROOT/<build>/uor and
# UOR_ONLY=<scenarios>; checks that every scenario has expected.json and data/MANIFEST.
write_store() {
  local build=$1 sha=$2 scenarios=$3
  git -C "$REPO" cat-file -e "${sha}^{commit}" 2>/dev/null || harness_fail "writer commit $sha is not in this repository"
  local src="$ROOT/writer-$build"
  mkdir -p "$src" && git -C "$REPO" archive "$sha" | tar -x -C "$src" || harness_fail "git archive $sha failed"
  say "writer build=$build commit=$sha scenarios=$scenarios"
  ( cd "$src" && UOR_ROOT="$ROOT/$build/uor" UOR_ONLY="$scenarios" \
      go test ./pkg/bolt -run '^TestUORBoltWrite$' -count=1 -timeout=60m -v ) > "$ROOT/writer-$build.log" 2>&1
  local rc=$?
  grep -E '^(--- |ok |FAIL|panic)' "$ROOT/writer-$build.log" | sed 's/^/REPRO_WRITER /' | tail -n 20
  [[ $rc -eq 0 ]] || { tail -n 40 "$ROOT/writer-$build.log"; harness_fail "writer exited $rc"; }
  local sc
  for sc in ${scenarios//,/ }; do
    local d="$ROOT/$build/uor/stores/$sc"
    [[ -s $d/expected.json && -f $d/data/MANIFEST ]] || harness_fail "writer produced no complete store $build/$sc ($d)"
    say "store $build/$sc written: $(du -sh "$d" | cut -f1)"
  done
  rm -rf "$src"
}

# run_reader <log> <go test args...>: runs a reader of this checkout; returns its exit status.
run_reader() {
  local log=$1; shift
  say "reader: go test $*"
  ( cd "$REPO" && go test "$@" ) > "$log" 2>&1
  local rc=$?
  say "reader exit=$rc"
  return $rc
}
