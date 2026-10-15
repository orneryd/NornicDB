#!/usr/bin/env bash
# Compare disposable new-writer fixtures against an actual pre-upgrade reader.
# Run locally; never opens existing database files or triggers GitHub Actions.
set -euo pipefail
repo=$(git rev-parse --show-toplevel)
old_revision=${1:-e0ba9119}
scratch=$(mktemp -d "${TMPDIR:-/tmp}/nornicdb-recovery-compat.XXXXXX")
reader="$scratch/reader"
export NORNICDB_RECOVERY_FIXTURE_DIR="$scratch/fixtures"
export GOMAXPROCS=${GOMAXPROCS:-2}
cleanup() {
 git -C "$repo" worktree remove --force "$reader" >/dev/null 2>&1 || true
 rm -rf "$scratch"
}
trap cleanup EXIT
cd "$repo"
go test -p=1 -tags nolocalllm ./pkg/storage -run '^TestRecoveryGenerateOldReaderFixtures$' -count=1
git worktree add --detach "$reader" "$old_revision"
cp pkg/storage/testdata/old_reader_compatibility_test.go.txt "$reader/pkg/storage/zz_old_reader_compatibility_test.go"
printf 'Old reader revision: %s\n' "$(git -C "$reader" rev-parse HEAD)"
cd "$reader"
go test -p=1 -tags nolocalllm ./pkg/storage -run '^TestOldReaderCompatibilityFixture$' -count=1
