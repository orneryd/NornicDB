#!/usr/bin/env bash
# G-wal-replay-rebuild (ledger UPG-RB4, R9-1): a v1.5.0 asynchronous store with nine 2 MiB documents and a
# snapshot (scenario asyncdocs-snap-clean), whose MANIFEST is damaged, must be rebuilt at start from snapshot
# plus journal with every acknowledged write, in both orders of TestV152SnapshotRebuild:
#   A  open with storage upgrade allowed, close, damage, start again;
#   B  damage the old store, then the first start with storage upgrade allowed.
# Writer: v1.5.0 c99e77131 with the upgrade-lane writer, commit 87311bbd2 (scratch/v152-upgrade-writers-v150).
# Reader: TestV152SnapshotRebuild of this checkout, REPRO_COUNT repetitions (default 4) of each order.
# PASS: every repetition of both orders rebuilt with missing=0 changed=0 extra=0 at the rebuild and at a second
# open. FAIL: any repetition refused or differed.
# Usage (from a checkout of the candidate branch): bash scripts/v152-repro/wal-replay-rebuild.sh
ITEM=wal-replay-rebuild
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
SCENARIO=asyncdocs-snap-clean
COUNT="${REPRO_COUNT:-4}"

repro_root
WRITER="$(git -C "$REPO" rev-parse --verify -q "87311bbd23df966296ebfae50eeae3ba9ab5be51^{commit}")" || harness_fail "writer commit 87311bbd2 is not in this repository"
write_store v150 "$WRITER" "$SCENARIO"
log="$ROOT/reader.log"
UPR_ROOT="$ROOT" UPR_READ_DIR=read UPR_ONLY="v150/$SCENARIO" \
  run_reader "$log" ./pkg/bolt -run '^TestV152SnapshotRebuild$' -count="$COUNT" -timeout=45m -v
rc=$?
grep -E 'UPR_REBUILD_RESULT|^\s*--- (FAIL|PASS|SKIP): TestV152SnapshotRebuild' "$log" | sed 's/^/REPRO_READER /'
want=$((COUNT * 2))
got=$(grep -c 'UPR_REBUILD_RESULT v150/'"$SCENARIO" "$log")
[[ $got -eq $want ]] || { tail -n 40 "$log"; harness_fail "expected $want rebuild results, found $got (reader exit $rc)"; }
summary=""
bad=0
for order in A-upgrade-then-damage B-damage-then-upgrade; do
  ok=$(grep 'UPR_REBUILD_RESULT v150/'"$SCENARIO $order"' ' "$log" | grep -c 'rebuilt=true missing=0 changed=0 extra=0 second_missing=0 second_changed=0 second_extra=0')
  summary+=" $order=rebuilt-intact-$ok-of-$COUNT"
  [[ $ok -eq $COUNT ]] || bad=1
done
errs=$(grep -o 'incomplete or empty replay: [^)]*' "$log" | sort | uniq -c | sed 's/^ *//' | tr '\n' ';')
if [[ $bad -eq 0 && $rc -eq 0 ]]; then
  repro_result PASS "store=v150/$SCENARIO$summary"
fi
repro_result FAIL "store=v150/$SCENARIO$summary refusals: ${errs:-none} (reader exit $rc)"
