# Shared steps of the v1.5.2 upgrade-store reproductions (research scripts, not for merge).
# Sourced by the item scripts in this directory. Each item script:
#   1. prints its self-description (REPRO_ABOUT lines) and runs the preflight (REPRO_PREFLIGHT lines);
#   2. writes its scenario with the old build's writer, at the writer commit, into REPRO_STORES fresh instances;
#   3. runs the reader tests of THIS checkout (the candidate build) against each instance;
#   4. prints one line:  REPRO <item> candidate=<label> <sha> RESULT=PASS|FAIL|HARNESS-ERROR <detail>
# Exit status: 0 PASS (the claimed defect was not observed in this run), 1 FAIL (it was observed), 2 HARNESS-ERROR (could not run).
#
# Environment (all optional):
#   REPRO_TMP    parent of the temporary root; default /work/tnrun/v152-upg-repro when writable, else $TMPDIR or /tmp
#   REPRO_KEEP=1 keep the temporary root (its path is printed)
#   REPRO_STORES number of independent store instances (integer >= 1; item default in its header)
#   REPRO_COUNT  repetitions or starts per instance (integer >= 1; item default in its header)
#   UPR_COPY     store copy method the reader tests use: gnu | gcp | plain (default: chosen by the preflight)
#   GOFLAGS      default "-p=2 -tags=noui,nolocalllm" (the flags of the lab runs)

set -uo pipefail

REPRO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(git -C "$REPRO_DIR" rev-parse --show-toplevel 2>/dev/null)"
export GOFLAGS="${GOFLAGS:--p=2 -tags=noui,nolocalllm}"
CANDIDATE_LABEL="$(sed -n 's/^label=//p' "$REPRO_DIR/CANDIDATE" 2>/dev/null)"
CANDIDATE_SHA="$(sed -n 's/^sha=//p' "$REPRO_DIR/CANDIDATE" 2>/dev/null)"

say() { echo "REPRO_STEP $*"; }

harness_fail() {
  echo "REPRO ${ITEM:-?} candidate=${CANDIDATE_LABEL:-?} ${CANDIDATE_SHA:-?} RESULT=HARNESS-ERROR $*"
  exit 2
}

repro_result() { # PASS|FAIL detail...
  local r=$1; shift
  echo "REPRO ${ITEM} candidate=${CANDIDATE_LABEL} ${CANDIDATE_SHA} RESULT=$r $*"
  [[ $r == PASS ]] && exit 0 || exit 1
}

# about <line>...: the item's self-description, printed first (REPRO_ABOUT lines). The item scripts call it with the
# meaning of every counter in their result line, written next to the code that computes it.
about() {
  local l
  for l in "$@"; do echo "REPRO_ABOUT $l"; done
  echo "REPRO_ABOUT RESULT forms: PASS (exit 0), FAIL (exit 1), HARNESS-ERROR (exit 2), under the conditions of the Verdict line above; HARNESS-ERROR also when the preflight fails, REPRO_STORES or REPRO_COUNT is not an integer >= 1, or the writer exits nonzero or leaves no expected.json or data/MANIFEST."
}

# require_count <name> <value>: an integer >= 1, else HARNESS-ERROR (an empty instance list would run nothing).
require_count() {
  [[ $2 =~ ^[0-9]+$ && $2 -ge 1 ]] || harness_fail "$1=$2: must be an integer >= 1"
}

# go_version_at_least <have> <want>: compares dotted versions (1.27.4 >= 1.27.3).
go_version_at_least() {
  local IFS=.
  local -a h=($1) w=($2)
  local i
  for i in 0 1 2; do
    local a=${h[i]:-0} b=${w[i]:-0}
    ((10#$a > 10#$b)) && return 0
    ((10#$a < 10#$b)) && return 1
  done
  return 0
}

# preflight <writer-commit>: every tool the script and its readers use; HARNESS-ERROR naming each one missing.
#   bash >= 3.1; git with this checkout and the writer commit; go >= the "go" line of the candidate's go.mod and of the
#   writer commit's go.mod; python3 (the writer's journal scan); tar, mktemp, find, sort, sed, grep, du, cut, seq,
#   uniq, tr, awk;
#   sha256sum or "shasum -a 256" (store fingerprint); a store copy for the reader tests: GNU cp with --sparse
#   (UPR_COPY=gnu), else gcp (GNU cp installed on macOS/BSD, UPR_COPY=gcp), else plain "cp -R -p" (UPR_COPY=plain;
#   no sparse files, so each copy takes the store's apparent size).
preflight() {
  local writer=$1 missing=() t
  ((BASH_VERSINFO[0] > 3 || (BASH_VERSINFO[0] == 3 && BASH_VERSINFO[1] >= 1))) || missing+=("bash>=3.1 (have $BASH_VERSION)")
  for t in git go python3 tar mktemp find sort sed grep du cut seq uniq tr awk; do
    command -v "$t" >/dev/null 2>&1 || missing+=("$t")
  done
  [[ -n $REPO ]] || missing+=("a git checkout of a scratch/v152-repro-upg-* branch")
  [[ -n $CANDIDATE_SHA ]] || missing+=("scripts/v152-repro/CANDIDATE")
  if command -v sha256sum >/dev/null 2>&1; then SHA256=(sha256sum)
  elif command -v shasum >/dev/null 2>&1; then SHA256=(shasum -a 256)
  else missing+=("sha256sum or shasum"); fi
  if [[ -n $REPO ]] && command -v git >/dev/null 2>&1; then
    git -C "$REPO" cat-file -e "${writer}^{commit}" 2>/dev/null || missing+=("writer commit $writer in this repository (fetch the branch again)")
  fi
  if command -v go >/dev/null 2>&1 && [[ -n $REPO ]]; then
    local have want src
    have=$(go env GOVERSION 2>/dev/null | sed -n 's/^go\([0-9][0-9.]*\).*/\1/p')
    for src in "candidate:$(sed -n 's/^go \([0-9.]*\)$/\1/p' "$REPO/go.mod")" \
               "writer:$(git -C "$REPO" show "${writer}:go.mod" 2>/dev/null | sed -n 's/^go \([0-9.]*\)$/\1/p')"; do
      want=${src#*:}
      [[ -n $want ]] || { missing+=("${src%%:*} go.mod go line"); continue; }
      go_version_at_least "${have:-0}" "$want" || missing+=("go>=$want for the ${src%%:*} (have ${have:-none})")
    done
  fi
  if [[ -z ${UPR_COPY:-} ]]; then
    local probe
    probe=$(mktemp -d 2>/dev/null) && : > "$probe/f"
    if [[ -n $probe ]] && cp -a --sparse=always "$probe/f" "$probe/g" 2>/dev/null; then UPR_COPY=gnu
    elif command -v gcp >/dev/null 2>&1; then UPR_COPY=gcp
    elif command -v cp >/dev/null 2>&1; then UPR_COPY=plain
    else missing+=("cp"); fi
    [[ -n $probe ]] && rm -rf "$probe"
  fi
  case ${UPR_COPY:-} in gnu|gcp|plain) ;; *) missing+=("UPR_COPY=${UPR_COPY:-}: want gnu, gcp or plain") ;; esac
  export UPR_COPY
  if ((${#missing[@]})); then
    local IFS=';'
    harness_fail "preflight: missing ${missing[*]}"
  fi
  echo "REPRO_PREFLIGHT ok bash=$BASH_VERSION go=$(go env GOVERSION) python3=$(python3 -c 'import platform;print(platform.python_version())') sha256=${SHA256[*]} copy=$UPR_COPY os=$(uname -s)"
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
  ROOT="$(mktemp -d "${parent%/}/repro-XXXXXX")" || harness_fail "cannot create a temporary root under $parent"
  if [[ ${REPRO_KEEP:-0} != 1 ]]; then trap 'rm -rf "$ROOT"' EXIT; fi
  say "root=$ROOT"
  say "candidate=$CANDIDATE_LABEL $CANDIDATE_SHA head=$(git -C "$REPO" rev-parse HEAD) go=$(go env GOVERSION)"
}

# prepare_writer <writer-commit>: extracts the writer commit once into $ROOT/writer-src (sets WRITER_SRC).
prepare_writer() {
  local sha=$1
  WRITER_SRC="$ROOT/writer-src"
  mkdir -p "$WRITER_SRC" && git -C "$REPO" archive "$sha" | tar -x -C "$WRITER_SRC" || harness_fail "git archive $sha failed"
  WRITER_SHA=$sha
}

# write_store <instance-root> <build> <scenario>[,<scenario>...]
# Runs the old build's Bolt writer (TestUORBoltWrite) from $WRITER_SRC with UOR_ROOT=<instance-root>/<build>/uor and
# UOR_ONLY=<scenarios>; checks that every scenario has expected.json and data/MANIFEST and prints a fingerprint of
# its journal and snapshot files (the journal of an asynchronous writer differs from run to run).
write_store() {
  local iroot=$1 build=$2 scenarios=$3
  say "writer build=$build commit=$WRITER_SHA scenarios=$scenarios root=$iroot"
  ( cd "$WRITER_SRC" && UOR_ROOT="$iroot/$build/uor" UOR_ONLY="$scenarios" go test ./pkg/bolt -run '^TestUORBoltWrite$' -count=1 -timeout=60m -v ) > "$iroot.writer.log" 2>&1
  local rc=$?
  grep -a -E '^(--- |ok |FAIL|panic)' "$iroot.writer.log" | sed 's/^/REPRO_WRITER /' | tail -n 20
  [[ $rc -eq 0 ]] || { tail -n 40 "$iroot.writer.log"; harness_fail "writer exited $rc"; }
  local sc
  for sc in ${scenarios//,/ }; do
    local d="$iroot/$build/uor/stores/$sc"
    [[ -s $d/expected.json && -f $d/data/MANIFEST ]] || harness_fail "writer produced no complete store $build/$sc ($d)"
    local fp f
    fp=$(cd "$d/data" && find wal snapshots -type f 2>/dev/null | LC_ALL=C sort | while IFS= read -r f; do "${SHA256[@]}" "$f"; done | "${SHA256[@]}" | cut -c1-16)
    say "store $build/$sc written: $(du -sh "$d" | cut -f1) journal+snapshot fingerprint=$fp"
  done
}

# write_store_server <instance-root> <build> <scenario>[,<scenario>...]
# The same as write_store, with the old build's server writer (TestUORWrite in ./pkg/server: the embedded Cypher API,
# Qdrant and asynchronous scenarios) in place of the Bolt writer. Both writers keep their stores under
# <instance-root>/<build>/uor/stores/<scenario>, so the two can write into one instance. Its log is
# <instance-root>.writer-server.log.
write_store_server() {
  local iroot=$1 build=$2 scenarios=$3
  say "server writer build=$build commit=$WRITER_SHA scenarios=$scenarios root=$iroot"
  ( cd "$WRITER_SRC" && UOR_ROOT="$iroot/$build/uor" UOR_ONLY="$scenarios" go test ./pkg/server -run '^TestUORWrite$' -count=1 -timeout=60m -v ) > "$iroot.writer-server.log" 2>&1
  local rc=$?
  grep -a -E '^(--- |ok |FAIL|panic)' "$iroot.writer-server.log" | sed 's/^/REPRO_WRITER /' | tail -n 20
  [[ $rc -eq 0 ]] || { tail -n 40 "$iroot.writer-server.log"; harness_fail "server writer exited $rc"; }
  local sc
  for sc in ${scenarios//,/ }; do
    local d="$iroot/$build/uor/stores/$sc"
    [[ -s $d/expected.json && -f $d/data/MANIFEST ]] || harness_fail "server writer produced no complete store $build/$sc ($d)"
    local fp f
    fp=$(cd "$d/data" && find wal snapshots -type f 2>/dev/null | LC_ALL=C sort | while IFS= read -r f; do "${SHA256[@]}" "$f"; done | "${SHA256[@]}" | cut -c1-16)
    say "store $build/$sc written: $(du -sh "$d" | cut -f1) journal+snapshot fingerprint=$fp"
  done
}

# norm_msg: a reader message with the temporary root replaced by <root> and node IDs by <id> (stdin to stdout).
norm_msg() {
  sed -E -e "s#${ROOT:-/nonexistent-root}#<root>#g" -e 's/nornic:[0-9a-f-]{8,}/<id>/g' -e 's/\\"/"/g'
}

# run_reader <log> <go test args...>: runs a reader of this checkout (UPR_COPY exported); returns its exit status.
run_reader() {
  local log=$1; shift
  say "reader: go test $*"
  ( cd "$REPO" && go test "$@" ) > "$log" 2>&1
  local rc=$?
  say "reader exit=$rc"
  return $rc
}
