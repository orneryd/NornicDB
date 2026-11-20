#!/usr/bin/env bash
# G-snapshot-wal-recovery (ledger R9-1): the v1.5.0 store asyncdocs-snap-clean is opened with storage upgrade
# allowed, closed, its MANIFEST damaged, and started again (start-up auto-recovery from snapshot plus journal),
# REPRO_COUNT times per instance (default 12), each on a fresh copy:
#   REAL           nornicdb.Open, the start-up path;
#   REPLICA        the recovery steps with Open's options (write-behind on), printing each failed journal entry;
#   REPLICA-WBOFF  the same with write-behind off.
# Writer: the v1.5.0 writer test TestUORBoltWrite at commit 87311bbd2 (v1.5.0 c99e77131 plus that test;
# branch scratch/v152-upgrade-writers-v150). The writer is asynchronous and its journal differs from run to run, so
# the script writes REPRO_STORES independent instances (default 8) and reads each.
# Reader: TestR9RecoverErrorsV150AsyncdocsSnapClean (pkg/nornicdb) of this checkout.
# Usage (from a checkout of the candidate branch): bash scripts/v152-repro/snapshot-wal-recovery.sh
# Forced mode (deterministic trigger of the replay path, not the real start-up):
#   bash scripts/v152-repro/snapshot-wal-recovery.sh --forced-overlap                    # write-behind on
#   bash scripts/v152-repro/snapshot-wal-recovery.sh --forced-overlap --write-behind=off  # its control
FORCED=0
FORCED_WB=on
for a in "$@"; do
  case $a in
    --forced-overlap) FORCED=1 ;;
    --write-behind=on) FORCED_WB=on ;;
    --write-behind=off) FORCED_WB=off ;;
    *) echo "REPRO snapshot-wal-recovery RESULT=HARNESS-ERROR unknown argument: $a"; exit 2 ;;
  esac
done
ITEM=snapshot-wal-recovery
if [[ $FORCED == 1 ]]; then
  ITEM=snapshot-wal-recovery-forced-overlap
  [[ $FORCED_WB == off ]] && ITEM=snapshot-wal-recovery-forced-overlap-wboff
elif [[ $FORCED_WB == off ]]; then
  echo "REPRO snapshot-wal-recovery RESULT=HARNESS-ERROR --write-behind applies only with --forced-overlap"; exit 2
fi
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
SCENARIO=asyncdocs-snap-clean
WRITER=87311bbd23df966296ebfae50eeae3ba9ab5be51
if [[ $FORCED == 1 ]]; then
  source "$(dirname "${BASH_SOURCE[0]}")/forced-overlap.sh"
  exit 2
fi
COUNT="${REPRO_COUNT-12}"
STORES="${REPRO_STORES-8}"

about \
  "item=$ITEM store=v150/$SCENARIO writer=$WRITER (v1.5.0 c99e77131 with the v1.5.0 writer test TestUORBoltWrite) reader=TestR9RecoverErrorsV150AsyncdocsSnapClean instances=REPRO_STORES($STORES) starts_per_instance=REPRO_COUNT($COUNT)" \
  "instances: REPRO_STORES, default 8. The writer is asynchronous and its journal differs from run to run, and the failure of the real start-up is intermittent: the same build can show it in one run and not in another. PASS therefore means the failure was not observed in this run, not that it is absent. For a deterministic trigger of the replay path use --forced-overlap." \
  "i<k>: one independently written store instance; each iteration of the reader opens a fresh copy with upgrade allowed, closes it, damages its MANIFEST, and recovers three identical copies: REAL (nornicdb.Open), REPLICA (the steps of recoverBadgerFromSnapshotAndWAL with Open's options, write-behind on) and REPLICA-WBOFF (the same, write-behind off)." \
  "iterations, real_failed, replica_failed, replica_wboff_failed: copied from the reader's 'R9REC SUMMARY' line. real_failed = iterations whose REAL nornicdb.Open returned an error; replica_failed / replica_wboff_failed = iterations whose replica recovery reported Failed > 0 (an iteration counts once however many entries failed; a replica can report failed=1 with err=<nil>)." \
  "paths=[<path> x<k>: <message>; ...]: the failure paths of the REAL start, grouped by path and message: 'upgrade refusal' (the open with upgrade allowed before the damage failed; the reader's 'upgrade-open-failed' line), 'real open error' (the start on the damaged store returned an error; the reader's 'REAL FAILED' line). k counts iterations." \
  "entries: <k> REPLICA[-WBOFF]-ERROR seq=<s> op=<o> err=<e>;...: k = the number of the reader's error lines with that sequence, operation and error (one line per failed journal entry per replica recovery), node IDs replaced by <id>. Shown for diagnosis; the verdict does not use them." \
  "reader_exit=<rc>: the reader's exit status for that instance. An instance with a failure path is FAIL whatever its exit status; an instance without one is PASS only with reader_exit=0, else HARNESS-ERROR." \
  "refused_starts = the number of iterations with a failure path, over all instances. HARNESS-ERROR also if an instance has no SUMMARY line, iterations != count, the REAL lines and upgrade refusals do not add up to count, or fewer instances were read than REPRO_STORES."
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
  grep -a -E '^R9REC (SUMMARY|iter=[0-9]+ (REAL|REPLICA|REPLICA-WBOFF|upgrade-open-failed) )' "$log" | grep -a -v ENTRY | cut -c1-400 | norm_msg | sed "s/^/REPRO_READER i$k /"
  sum=$(grep -a -o 'R9REC SUMMARY iterations=[0-9]* real_failed=[0-9]* replica_failed=[0-9]* replica_wboff_failed=[0-9]*' "$log" | tail -n1)
  [[ -n $sum ]] || { tail -n 40 "$log" | cut -c1-400 | norm_msg; harness_fail "instance $k: no R9REC SUMMARY line (reader exit $rc)"; }
  iters=$(sed -n 's/.*iterations=\([0-9]*\).*/\1/p' <<<"$sum")
  [[ $iters -eq $COUNT ]] || harness_fail "instance $k: ran $iters of $COUNT iterations"
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
[[ $total -eq 0 ]] && repro_result PASS "not observed in this run (intermittent) $detail"
repro_result FAIL "refused_starts=$total $detail"
