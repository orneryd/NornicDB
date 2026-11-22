#!/usr/bin/env bash
# G-snapshot-wal-recovery (ledger R9-1), v1.5.0 store asyncdocs-snap-clean.
# Writer: the v1.5.0 writer test TestUORBoltWrite at commit 87311bbd2 (v1.5.0 c99e77131 plus that test;
# branch scratch/v152-upgrade-writers-v150), run once per instance.
# Default mode reader: TestR9RecoverErrorsV150AsyncdocsSnapClean (pkg/nornicdb) of this checkout.
# Overlap-inducing mode reader: TestR9OverlapInducingV150AsyncdocsSnapClean (pkg/storage) of this checkout.
# The steps of each mode are printed by the script (REPRO_ABOUT lines).
# Usage (from a checkout of the candidate branch):
#   bash scripts/v152-repro/snapshot-wal-recovery.sh                                       # default mode
#   bash scripts/v152-repro/snapshot-wal-recovery.sh --overlap-inducing                    # write-behind on
#   bash scripts/v152-repro/snapshot-wal-recovery.sh --overlap-inducing --write-behind=off # its control
# --forced-overlap is accepted as another name for --overlap-inducing.
OVERLAP=0
OVERLAP_WB=on
for a in "$@"; do
  case $a in
    --overlap-inducing|--forced-overlap) OVERLAP=1 ;;
    --write-behind=on) OVERLAP_WB=on ;;
    --write-behind=off) OVERLAP_WB=off ;;
    *) echo "REPRO snapshot-wal-recovery RESULT=HARNESS-ERROR unknown argument: $a"; exit 2 ;;
  esac
done
ITEM=snapshot-wal-recovery
if [[ $OVERLAP == 1 ]]; then
  ITEM=snapshot-wal-recovery-overlap-inducing
  [[ $OVERLAP_WB == off ]] && ITEM=snapshot-wal-recovery-overlap-inducing-wboff
elif [[ $OVERLAP_WB == off ]]; then
  echo "REPRO snapshot-wal-recovery RESULT=HARNESS-ERROR --write-behind applies only with --overlap-inducing"; exit 2
fi
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
SCENARIO=asyncdocs-snap-clean
WRITER=87311bbd23df966296ebfae50eeae3ba9ab5be51
if [[ $OVERLAP == 1 ]]; then
  source "$(dirname "${BASH_SOURCE[0]}")/overlap-inducing.sh"
  exit 2
fi
COUNT="${REPRO_COUNT-12}"
STORES="${REPRO_STORES-8}"

about \
  "item=$ITEM store=v150/$SCENARIO writer=$WRITER (v1.5.0 c99e77131 with the v1.5.0 writer test TestUORBoltWrite) reader=TestR9RecoverErrorsV150AsyncdocsSnapClean instances=REPRO_STORES($STORES) iterations=REPRO_COUNT($COUNT)" \
  "steps, for each of the REPRO_STORES instances: the writer test writes the store into a new instance directory; the reader runs once with REPRO_COUNT iterations." \
  "steps, each iteration, on a new copy of the store's data directory: (1) nornicdb.Open with storage upgrade allowed; on an error the reader prints 'upgrade-open-failed' and the iteration ends; (2) close; on an error the reader prints CLOSE-ERROR upgrade and stops; (3) write the bytes 'damaged' over MANIFEST; (4) copy the directory three times; (5) REAL: nornicdb.Open with storage upgrade allowed on copy 1; an error prints 'REAL FAILED', else 'REAL ok' and a close (an error prints CLOSE-ERROR REAL); (6) REPLICA: on copy 2, the engine options resolveDurabilityOptions returns for the copy with the catalog upgrade and the memory policy of step 1's database, the directory renamed to <copy>.corrupted, a new engine in an empty directory, RecoverIntoEngine from the latest snapshot and the journal of <copy>.corrupted; it prints applied, skipped, failed and the returned error, one ERROR line per refused journal entry, then closes the engine (an error prints CLOSE-ERROR REPLICA); (7) REPLICA-WBOFF: step 6 on copy 3 with write-behind off." \
  "iterations, real_failed, replica_failed, replica_wboff_failed, replica_replay_errors, close_errors: copied from the reader's 'R9REC SUMMARY' line. real_failed = iterations with 'REAL FAILED'; replica_failed / replica_wboff_failed = iterations whose step 6 / step 7 reported failed > 0; replica_replay_errors = steps 6 and 7 whose RecoverIntoEngine returned an error; close_errors = CLOSE-ERROR lines of steps 5 to 7." \
  "paths=[<path> x<k>: <message>; ...]: 'upgrade refusal' = the reader's 'upgrade-open-failed' lines, 'real open error' = its 'REAL FAILED' lines, grouped by path and message; k counts iterations." \
  "entries: <k> REPLICA[-WBOFF]-ERROR seq=<s> op=<o> err=<e>;...: k = the number of the reader's ERROR lines with that sequence, operation and error, node IDs replaced by <id>. The verdict does not use them." \
  "reader_exit=<rc>: the reader's exit status for that instance." \
  "refused_starts = the number of iterations with a path, over all instances." \
  "Verdict: HARNESS-ERROR if an instance has no SUMMARY line, iterations != REPRO_COUNT, the REAL lines and upgrade refusals do not add up to REPRO_COUNT, replica_replay_errors > 0, close_errors > 0 or a CLOSE-ERROR line is printed, fewer instances were read than REPRO_STORES, or reader_exit != 0 on an instance without a path. Otherwise FAIL if refused_starts > 0, and PASS if refused_starts = 0; PASS is printed as 'not observed in this run'."
require_count REPRO_STORES "$STORES"
require_count REPRO_COUNT "$COUNT"
preflight "$WRITER"
repro_root
prepare_writer "$WRITER"
read_instances=0
detail="store=v150/$SCENARIO instances=$STORES iterations=$COUNT"
total=0
for k in $(seq 1 "$STORES"); do
  iroot="$ROOT/i$k"
  write_store "$iroot" v150 "$SCENARIO"
  log="$iroot.reader.log"
  R9_ROOT="$iroot" R9_DIAG_STORE="v150/uor/stores/$SCENARIO" R9_DIAG_AREA=r9diag R9_DIAG_N="$COUNT" \
    run_reader "$log" ./pkg/nornicdb -run '^TestR9RecoverErrorsV150AsyncdocsSnapClean$' -count=1 -timeout=60m -v
  rc=$?
  grep -a -E '^R9REC (SUMMARY|iter=[0-9]+ (REAL|REPLICA|REPLICA-WBOFF|upgrade-open-failed|CLOSE-ERROR) )' "$log" | grep -a -v ENTRY | cut -c1-400 | norm_msg | sed "s/^/REPRO_READER i$k /"
  closes=$(grep -a -c -E '^R9REC iter=[0-9]+ CLOSE-ERROR ' "$log")
  [[ $closes -eq 0 ]] || harness_fail "instance $k: $closes close errors (CLOSE-ERROR lines)"
  sum=$(grep -a -o 'R9REC SUMMARY iterations=[0-9]* real_failed=[0-9]* replica_failed=[0-9]* replica_wboff_failed=[0-9]* replica_replay_errors=[0-9]* close_errors=[0-9]*' "$log" | tail -n1)
  [[ -n $sum ]] || { tail -n 40 "$log" | cut -c1-400 | norm_msg; harness_fail "instance $k: no R9REC SUMMARY line (reader exit $rc)"; }
  iters=$(sed -n 's/.*iterations=\([0-9]*\).*/\1/p' <<<"$sum")
  [[ $iters -eq $COUNT ]] || harness_fail "instance $k: ran $iters of $COUNT iterations"
  rerrs=$(sed -n 's/.*replica_replay_errors=\([0-9]*\).*/\1/p' <<<"$sum")
  cerrs=$(sed -n 's/.*close_errors=\([0-9]*\).*/\1/p' <<<"$sum")
  [[ $rerrs -eq 0 && $cerrs -eq 0 ]] || harness_fail "instance $k: replica_replay_errors=$rerrs close_errors=$cerrs"
  real_lines=$(grep -a -c -E '^R9REC iter=[0-9]+ REAL (ok|FAILED)' "$log")
  upgrade_refused=$(grep -a -c -E '^R9REC iter=[0-9]+ upgrade-open-failed ' "$log")
  [[ $((real_lines + upgrade_refused)) -eq $COUNT ]] || harness_fail "instance $k: $real_lines REAL results and $upgrade_refused upgrade refusals for $COUNT iterations"
  paths=$( { grep -a -E '^R9REC iter=[0-9]+ upgrade-open-failed err=' "$log" | sed -E 's/^R9REC iter=[0-9]+ upgrade-open-failed err=/upgrade refusal: /'
             grep -a -E '^R9REC iter=[0-9]+ REAL FAILED err=' "$log" | sed -E 's/^R9REC iter=[0-9]+ REAL FAILED err=/real open error: /'; } \
           | norm_msg | LC_ALL=C sort | uniq -c | sed -E 's/^ *([0-9]+) ([^:]+): (.*)$/\2 x\1: \3/' | tr '\n' ';' | sed 's/;$//')
  refused=$(( $(grep -a -c -E '^R9REC iter=[0-9]+ REAL FAILED ' "$log") + upgrade_refused ))
  errs=$(grep -a -o -E 'REPLICA(-WBOFF)?-ERROR seq=[0-9]+ op=[a-z_]+ err=[^"\\]*' "$log" | norm_msg | LC_ALL=C sort | uniq -c | sed 's/^ *//' | tr '\n' ';' | sed 's/;$//')
  part=" i$k:[${sum#R9REC SUMMARY } reader_exit=$rc"
  if [[ $refused -gt 0 ]]; then
    part+=" paths=[$paths]"
  elif [[ $rc -ne 0 ]]; then
    tail -n 40 "$log" | cut -c1-400 | norm_msg
    harness_fail "instance $k: the reader exited $rc although no start was refused"
  fi
  detail+="$part entries: ${errs:-none}]"
  total=$((total + refused))
  read_instances=$((read_instances + 1))
done
[[ $read_instances -eq $STORES ]] || harness_fail "read $read_instances of $STORES instances"
[[ $total -eq 0 ]] && repro_result PASS "not observed in this run $detail"
repro_result FAIL "refused_starts=$total $detail"
