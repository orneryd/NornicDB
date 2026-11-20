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
COUNT="${REPRO_COUNT-12}"
STORES="${REPRO_STORES-3}"

WRITER=87311bbd23df966296ebfae50eeae3ba9ab5be51
about \
  "item=$ITEM store=v150/$SCENARIO writer=$WRITER (v1.5.0 c99e77131 + upgrade-lane writer) reader=TestR9RecoverErrorsV150AsyncdocsSnapClean instances=REPRO_STORES($STORES) starts_per_instance=REPRO_COUNT($COUNT)" \
  "i<k>: one independently written store instance; each iteration of the reader opens a fresh copy with upgrade allowed, closes it, damages its MANIFEST, and recovers three identical copies: REAL (nornicdb.Open), REPLICA (the steps of recoverBadgerFromSnapshotAndWAL with Open's options, write-behind on) and REPLICA-WBOFF (the same, write-behind off)." \
  "iterations, real_failed, replica_failed, replica_wboff_failed: copied from the reader's 'R9REC SUMMARY' line. real_failed = iterations whose REAL nornicdb.Open returned an error; replica_failed / replica_wboff_failed = iterations whose replica recovery reported Failed > 0 (an iteration counts once however many entries failed; a replica can report failed=1 with err=<nil>)." \
  "entries: <k> REPLICA[-WBOFF]-ERROR seq=<s> op=<o> err=<e>;...: k = the number of the reader's error lines with that sequence, operation and error (one line per failed journal entry per replica recovery), node IDs replaced by <id>." \
  "refused_starts = the sum of real_failed over the instances. PASS only if refused_starts = 0. HARNESS-ERROR also if an instance has no SUMMARY line, iterations != count, fewer than count iterations reached the REAL step, or fewer instances were read than REPRO_STORES."
require_count REPRO_STORES "$STORES"
require_count REPRO_COUNT "$COUNT"
preflight "$WRITER"
repro_root
prepare_writer "$WRITER"
read_instances=0
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
  read_instances=$((read_instances + 1))
done
[[ $read_instances -eq $STORES ]] || harness_fail "read $read_instances of $STORES instances"
[[ $total -eq 0 ]] && repro_result PASS "$detail"
repro_result FAIL "refused_starts=$total $detail"
