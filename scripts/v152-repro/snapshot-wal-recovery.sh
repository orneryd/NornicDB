#!/usr/bin/env bash
# G-snapshot-wal-recovery (ledger R9-1): the same v1.5.0 store (asyncdocs-snap-clean) is opened with storage
# upgrade allowed, closed, its MANIFEST damaged, and started again (start-up auto-recovery from snapshot plus
# journal) REPRO_COUNT times (default 12), each time on a fresh copy:
#   REAL           nornicdb.Open, the start-up path;
#   REPLICA        the recovery steps with Open's options (write-behind on), printing each failed journal entry;
#   REPLICA-WBOFF  the same with write-behind off.
# Writer: v1.5.0 c99e77131 with the upgrade-lane writer, commit 87311bbd2 (scratch/v152-upgrade-writers-v150).
# Reader: TestR9RecoverErrorsV150AsyncdocsSnapClean (pkg/nornicdb) of this checkout.
# PASS: no REAL start refused. FAIL: any refused (the line names the failed journal entries and their errors).
# Usage (from a checkout of the candidate branch): bash scripts/v152-repro/snapshot-wal-recovery.sh
ITEM=snapshot-wal-recovery
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
SCENARIO=asyncdocs-snap-clean
COUNT="${REPRO_COUNT:-12}"

repro_root
WRITER="$(git -C "$REPO" rev-parse --verify -q "87311bbd23df966296ebfae50eeae3ba9ab5be51^{commit}")" || harness_fail "writer commit 87311bbd2 is not in this repository"
write_store v150 "$WRITER" "$SCENARIO"
log="$ROOT/reader.log"
R9_ROOT="$ROOT" R9_DIAG_STORE="v150/uor/stores/$SCENARIO" R9_DIAG_AREA=r9diag R9_DIAG_N="$COUNT" \
  run_reader "$log" ./pkg/nornicdb -run '^TestR9RecoverErrorsV150AsyncdocsSnapClean$' -count=1 -timeout=60m -v
rc=$?
grep -E 'R9REC (store|SUMMARY|iter=[0-9]+ (REAL|REPLICA|REPLICA-WBOFF) )|^\s*--- (FAIL|PASS|SKIP): TestR9' "$log" | grep -v ENTRY | sed 's/^/REPRO_READER /'
sum=$(grep -o 'R9REC SUMMARY iterations=[0-9]* real_failed=[0-9]* replica_failed=[0-9]* replica_wboff_failed=[0-9]*' "$log" | tail -n1)
[[ -n $sum ]] || { tail -n 40 "$log"; harness_fail "no R9REC SUMMARY line (reader exit $rc)"; }
iters=$(sed -n 's/.*iterations=\([0-9]*\).*/\1/p' <<<"$sum")
real=$(sed -n 's/.*real_failed=\([0-9]*\).*/\1/p' <<<"$sum")
[[ $iters -eq $COUNT ]] || harness_fail "ran $iters of $COUNT iterations"
started=$(grep -cE 'R9REC iter=[0-9]+ REAL (ok|FAILED)' "$log")
[[ $started -eq $COUNT ]] || harness_fail "only $started of $COUNT starts reached the REAL step (upgrade open refused?)"
errs=$(grep -oE 'REPLICA(-WBOFF)?-ERROR seq=[0-9]+ op=[a-z_]+ err=[^"\\]*' "$log" | sed -E 's/nornic:[0-9a-f-]+/<id>/g' | sort | uniq -c | sed 's/^ *//' | tr '\n' ';')
detail="store=v150/$SCENARIO starts=$COUNT ${sum#R9REC SUMMARY } entries: ${errs:-none}"
if [[ $real -eq 0 && $rc -eq 0 ]]; then repro_result PASS "$detail"; fi
repro_result FAIL "$detail (reader exit $rc)"
