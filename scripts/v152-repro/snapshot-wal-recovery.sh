#!/usr/bin/env bash
# G-snapshot-wal-recovery (ledger R9-1): the v1.5.0 store asyncdocs-snap-clean is opened with storage upgrade
# allowed, closed, its MANIFEST damaged, and started again (start-up auto-recovery from snapshot plus journal),
# REPRO_COUNT times per instance (default 12), each on a fresh copy:
#   REAL           nornicdb.Open, the start-up path;
#   REPLICA        the recovery steps with Open's options (write-behind on), printing each failed journal entry;
#   REPLICA-WBOFF  the same with write-behind off.
# Writer: v1.5.0 c99e77131 with the upgrade-lane writer, commit 87311bbd2 (scratch/v152-upgrade-writers-v150).
# The writer is asynchronous and its journal differs from run to run; whether a store triggers the refusal depends
# on the instance. So the script writes REPRO_STORES independent instances (default 3) and reads each.
# Reader: TestR9RecoverErrorsV150AsyncdocsSnapClean (pkg/nornicdb) of this checkout.
# PASS: no REAL start refused on any instance. FAIL: any refused (the line names the failed journal entries).
# Usage (from a checkout of the candidate branch): bash scripts/v152-repro/snapshot-wal-recovery.sh
ITEM=snapshot-wal-recovery
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
SCENARIO=asyncdocs-snap-clean
COUNT="${REPRO_COUNT:-12}"
STORES="${REPRO_STORES:-3}"

repro_root
prepare_writer 87311bbd23df966296ebfae50eeae3ba9ab5be51
detail="store=v150/$SCENARIO instances=$STORES starts_per_instance=$COUNT"
total=0
for k in $(seq 1 "$STORES"); do
  iroot="$ROOT/i$k"
  write_store "$iroot" v150 "$SCENARIO"
  log="$iroot.reader.log"
  R9_ROOT="$iroot" R9_DIAG_STORE="v150/uor/stores/$SCENARIO" R9_DIAG_AREA=r9diag R9_DIAG_N="$COUNT" \
    run_reader "$log" ./pkg/nornicdb -run '^TestR9RecoverErrorsV150AsyncdocsSnapClean$' -count=1 -timeout=60m -v
  rc=$?
  grep -a -E 'R9REC (SUMMARY|iter=[0-9]+ (REAL|REPLICA|REPLICA-WBOFF) )' "$log" | grep -a -v ENTRY | cut -c1-400 | sed "s/^/REPRO_READER i$k /"
  sum=$(grep -a -o 'R9REC SUMMARY iterations=[0-9]* real_failed=[0-9]* replica_failed=[0-9]* replica_wboff_failed=[0-9]*' "$log" | tail -n1)
  [[ -n $sum ]] || { tail -n 40 "$log" | cut -c1-400; harness_fail "instance $k: no R9REC SUMMARY line (reader exit $rc)"; }
  iters=$(sed -n 's/.*iterations=\([0-9]*\).*/\1/p' <<<"$sum")
  real=$(sed -n 's/.*real_failed=\([0-9]*\).*/\1/p' <<<"$sum")
  [[ $iters -eq $COUNT ]] || harness_fail "instance $k: ran $iters of $COUNT iterations"
  started=$(grep -a -c -E 'R9REC iter=[0-9]+ REAL (ok|FAILED)' "$log")
  [[ $started -eq $COUNT ]] || harness_fail "instance $k: only $started of $COUNT starts reached the REAL step (upgrade open refused?)"
  errs=$(grep -a -o -E 'REPLICA(-WBOFF)?-ERROR seq=[0-9]+ op=[a-z_]+ err=[^"\\]*' "$log" | sed -E 's/nornic:[0-9a-f-]+/<id>/g' | sort | uniq -c | sed 's/^ *//' | tr '\n' ';')
  detail+=" i$k:[${sum#R9REC SUMMARY } entries: ${errs:-none}]"
  total=$((total + real))
done
[[ $total -eq 0 ]] && repro_result PASS "$detail"
repro_result FAIL "refused_starts=$total $detail"
