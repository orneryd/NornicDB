#!/usr/bin/env bash
#
# The CI test suite: every package under test, split into named groups, each
# run with coverage through scripts/generate-coverage.sh (which retries a
# failing package up to three times). Used by .github/workflows/ci.yml
# ("Test and Coverage") and by `make verify`, so both run exactly the same
# groups.
#
# usage: scripts/ci/test-groups.sh [raw_out] [out]
#   raw_out  accumulated raw profile   (default: coverage.raw.out)
#   out      filtered coverage profile (default: coverage.out)
#
# CI stops at the first failing group. With TEST_GROUPS_KEEP_GOING=1 every
# group runs and the failing ones are listed at the end (the exit status is
# still non-zero). TEST_GROUPS_ONLY="cypher search" runs just those groups.
set -uo pipefail

raw_out="${1:-coverage.raw.out}"
out="${2:-coverage.out}"
keep_going="${TEST_GROUPS_KEEP_GOING:-0}"
only=" ${TEST_GROUPS_ONLY:-} "
failed=()

: > "${raw_out}"

begin_group() {
  if [[ -n "${GITHUB_ACTIONS:-}" ]]; then echo "::group::coverage $1"; else printf '\n=== coverage %s\n' "$1"; fi
}
end_group() {
  if [[ -n "${GITHUB_ACTIONS:-}" ]]; then echo "::endgroup::"; fi
}

run_cov_group() {
  local name="$1"; shift
  if [[ "${only}" != "  " && "${only}" != *" ${name} "* ]]; then
    return 0
  fi
  begin_group "${name}"
  if ! bash scripts/generate-coverage.sh --append "${raw_out}" "${name}" "$@"; then
    if [[ -n "${GITHUB_ACTIONS:-}" ]]; then echo "::error title=coverage failed::group ${name} failed"; fi
    echo "group ${name} failed" >&2
    end_group
    failed+=("${name}")
    [[ "${keep_going}" == "1" ]] && return 0
    return 1
  fi
  end_group
}

# The pkg/server split. The catch-all covers any new test prefix not handled
# by a named bucket, so coverage stays complete without per-file maintenance.
SERVER_BUCKETS_HANDLED='^(TestAuth|TestGDPR|TestExplicit|TestCommit|TestRollback|TestTransaction|TestDatabase|TestExecute|TestDbConfig|TestDB|TestDecay|TestMultiDatabase|TestDatabaseIsolation|TestDefaultDatabaseIsolation|TestComposite|TestCompositeRoot|TestBifrost|TestUI|TestInstrumented|TestCORS|TestSession|TestServe|TestStart|TestNew|TestAddr|TestRoute|TestBaseURL|TestBackup|TestCluster|TestGetCookie|TestGraph|TestSearch|TestCollectLatest|TestCollectSnapshot|TestDiffGraph|TestEdges|TestRetention|TestGPU|TestBuildEmbed|TestBasePathAndRBAC|TestQdrant|TestServerQdrantGRPC|TestMCP|TestSecurity|TestRBAC|TestBasicAuth|TestEmbed|TestNornic)'

run_cov_server_group() {
  local name="$1"; local pattern="$2"
  run_cov_group "server-${name}" -run "${pattern}" ./pkg/server/...
}

# The pkg/storage split. If you add a bucket, add its prefixes to
# STORAGE_BUCKETS_HANDLED too; the catch-all -skip keeps coverage complete.
STORAGE_BUCKETS_HANDLED='^(TestBadgerEngine|TestBadgerTransaction|TestBadgerCache|TestBadgerSchemaHelpers|TestBadgerConstraintValidationHelpers|TestAsyncEngine|TestAsyncEngineConfig|TestNewAsyncEngine|TestBug|TestWAL|TestAtomicWAL|TestRecoverFromWAL|TestReplayAndUndoBulkWALEntries|TestReplayAndUndoWALEntry|TestRecoveryResultSummary|TestReplayResultSummary|TestReplayResultTracking|TestReplayResultHasCriticalErrors|TestReplayWALEntry|TestNewWAL|TestNewWALSyncsDirectory|TestDefaultWALConfig|TestSaveSnapshot|TestSaveSnapshotSyncsDirectory|TestPruneOldSnapshotFiles|TestSnapshot_|TestSnapshotAndSaveSnapshot|TestCheckAtomicWALIntegrity|TestCheckWALIntegrity|TestReadWALEntriesForTruncation|TestReadWALEntriesFromDir|TestSyncDirFunction|TestSyncDirInvalidPath|TestRecoverAbortedTransaction|TestRecoverCommittedTransaction|TestRecoverIncompleteTransaction|TestRecoverMixedTransactions|TestRecoverWithTransactions|TestBuildAtomicRecordV2|TestWriteAtomicRecordV2|TestCompositeEngine|TestCompositeIndex|TestCompositeKey|TestNamespacedEngine|TestTransaction|TestBatch|TestTx|TestNewTransaction|TestUndo|TestBeginTransaction|TestSnapshotIsolationConflict)'

run_cov_storage_group() {
  local name="$1"; local pattern="$2"
  run_cov_group "storage-${name}" -run "${pattern}" ./pkg/storage
}

groups() {
  run_cov_group util-small \
    ./apoc/community/... \
    ./pkg/audit/... ./pkg/auth/... ./pkg/buildinfo/... \
    ./pkg/cache/... ./pkg/compliance/... ./pkg/config/... \
    ./pkg/convert/... ./pkg/embed/... ./pkg/embeddingutil/... \
    ./pkg/encryption/... ./pkg/envutil/... ./pkg/errors/... \
    ./pkg/eval/... ./pkg/fabric/... ./pkg/filter/... \
    ./pkg/gpu/... ./pkg/heimdall/... ./pkg/indexing/... \
    ./pkg/inference/... ./pkg/kms/... ./pkg/knowledgepolicy/... \
    ./pkg/lifecycle/... ./pkg/linkpredict/... ./pkg/localllm/... ./pkg/louvain/... \
    ./pkg/math/... ./pkg/mcp/... ./pkg/multidb/... \
    ./pkg/nornicgrpc/... ./pkg/observability/... ./pkg/pool/... \
    ./pkg/qdrantgrpc/... ./pkg/retention/... ./pkg/security/... \
    ./pkg/simd/... ./pkg/storage/lifecycle/... ./pkg/temporal/... \
    ./pkg/textchunk/... ./pkg/txsession/... ./pkg/util/... \
    ./pkg/vectorspace/... || return 1
  run_cov_group graphql ./pkg/graphql/... || return 1
  run_cov_group cypher ./pkg/cypher/... || return 1
  run_cov_group search ./pkg/search/... || return 1
  run_cov_group bolt ./pkg/bolt/... || return 1

  run_cov_storage_group badger        '^(TestBadgerEngine|TestBadgerTransaction|TestBadgerCache|TestBadgerSchemaHelpers|TestBadgerConstraintValidationHelpers)' || return 1
  run_cov_storage_group async         '^(TestAsyncEngine|TestAsyncEngineConfig|TestNewAsyncEngine|TestBug)' || return 1
  run_cov_storage_group wal-recovery  '^(TestWAL|TestAtomicWAL|TestRecoverFromWAL|TestReplayAndUndoBulkWALEntries|TestReplayAndUndoWALEntry|TestRecoveryResultSummary|TestReplayResultSummary|TestReplayResultTracking|TestReplayResultHasCriticalErrors|TestReplayWALEntry|TestNewWAL|TestNewWALSyncsDirectory|TestDefaultWALConfig|TestSaveSnapshot|TestSaveSnapshotSyncsDirectory|TestPruneOldSnapshotFiles|TestSnapshot_|TestSnapshotAndSaveSnapshot|TestCheckAtomicWALIntegrity|TestCheckWALIntegrity|TestReadWALEntriesForTruncation|TestReadWALEntriesFromDir|TestSyncDirFunction|TestSyncDirInvalidPath|TestRecoverAbortedTransaction|TestRecoverCommittedTransaction|TestRecoverIncompleteTransaction|TestRecoverMixedTransactions|TestRecoverWithTransactions|TestBuildAtomicRecordV2|TestWriteAtomicRecordV2)' || return 1
  run_cov_storage_group composite-tx  '^(TestCompositeEngine|TestCompositeIndex|TestCompositeKey|TestNamespacedEngine|TestTransaction|TestBatch|TestTx|TestNewTransaction|TestUndo|TestBeginTransaction|TestSnapshotIsolationConflict)' || return 1
  run_cov_group storage-rest -skip "${STORAGE_BUCKETS_HANDLED}" ./pkg/storage || return 1

  run_cov_group nornicdb ./pkg/nornicdb/... || return 1

  run_cov_server_group core-tx       '^(TestAuth|TestGDPR|TestExplicit|TestCommit|TestRollback|TestTransaction)' || return 1
  run_cov_server_group db            '^(TestDatabase(Adapter|Empty|Info|Unknown)|TestExecute|TestDbConfig|TestDB|TestDecay)' || return 1
  run_cov_server_group multidb       '^(TestMultiDatabase|TestDatabaseIsolation|TestDefaultDatabaseIsolation|TestComposite|TestCompositeRoot)' || return 1
  run_cov_server_group wiring        '^(TestBifrost|TestUI|TestInstrumented|TestCORS|TestSession|TestServe|TestStart|TestNew|TestAddr|TestRoute|TestBaseURL|TestBackup|TestCluster|TestGetCookie)' || return 1
  run_cov_server_group graph-search  '^(TestGraph|TestSearch|TestCollectLatest|TestCollectSnapshot|TestDiffGraph|TestEdges|TestRetention|TestGPU|TestBuildEmbed|TestBasePathAndRBAC)' || return 1
  run_cov_server_group integrations  '^(TestQdrant|TestServerQdrantGRPC|TestMCP|TestSecurity|TestRBAC|TestBasicAuth|TestEmbed|TestNornic)' || return 1
  run_cov_group server-rest -skip "${SERVER_BUCKETS_HANDLED}" ./pkg/server/... || return 1

  run_cov_group replication ./pkg/replication/... || return 1
}

if [[ "${1:-}" == "--list" ]]; then
  # Print the group names, for `make verify` and anyone choosing TEST_GROUPS_ONLY.
  grep -oE 'run_cov_(server_|storage_)?group +[a-z0-9-]+' "$0" | grep -v 'name' |
    awk '{ p = ($1 == "run_cov_server_group") ? "server-" : ($1 == "run_cov_storage_group") ? "storage-" : ""; print p $2 }'
  exit 0
fi

groups
status=$?
if (( ${#failed[@]} > 0 )); then
  echo "failed groups: ${failed[*]}" >&2
  exit 1
fi
if (( status != 0 )); then
  exit "${status}"
fi
bash scripts/generate-coverage.sh --filter "${raw_out}" "${out}"
