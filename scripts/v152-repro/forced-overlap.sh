# Forced mode of snapshot-wal-recovery.sh (sourced by it; do not run directly). Deterministic trigger of the
# replay-path failure: a test-side delay in the wrapped write-behind generation apply forces it to overlap the
# direct replay of a buffered_commit journal entry. Covers the replay path (a copy of RecoverIntoEngine with an
# entry hook), not the real nornicdb.Open start-up.
# Reader: TestR9ForcedOverlapV150AsyncdocsSnapClean (pkg/storage) of this checkout, R9_FORCED_WB=$FORCED_WB.
STORES="${REPRO_STORES-1}"
OFFSETS="${REPRO_OFFSETS_MS-0,1,2,3,4,5,6,8,10,12,15,20,25,30,40,50}"
about \
  "item=$ITEM store=v150/$SCENARIO writer=$WRITER (v1.5.0 c99e77131 with the v1.5.0 writer test TestUORBoltWrite) reader=TestR9ForcedOverlapV150AsyncdocsSnapClean write_behind=$FORCED_WB instances=REPRO_STORES($STORES) offsets_ms=REPRO_OFFSETS_MS($OFFSETS)" \
  "scope: the replay path only. The reader restores the store's latest snapshot and replays its journal into a fresh engine through a copy of RecoverIntoEngine with an entry hook; it does not run the real nornicdb.Open start-up." \
  "trigger: with write_behind=on, a test-side wrapper around the background write-behind generation apply holds every apply that starts during journal replay, until the replay of the first buffered_commit entry visited while an apply is held (the target); it then sleeps offset_ms and runs the original apply, so the apply overlaps the target's direct replay. One replay per offset, each on a fresh copy and engine. With write_behind=off (the control) the engine has no write-behind buffer and the same replay runs without any generation apply." \
  "run=<i> lines: the reader's R9FORCED lines: failed = journal entries the replay refused; target_seq = the target entry (none if no apply was held at a buffered_commit); applies_held = applies the gate held; gate_timeouts = held applies released after R9_FORCED_HOLD_MAX_MS (default 10000) without a target; target_err = the target's replay error." \
  "failed_runs, runs_with_target, runs_with_gate_timeout: copied from the reader's 'R9FORCED SUMMARY' line; failed_runs counts replays with failed > 0." \
  "Verdict: FAIL if any replay refused a journal entry (failed_runs > 0), whatever the reader's exit status. Otherwise PASS only with reader_exit=0 and, for write_behind=on, runs_with_target equal to the number of offsets (the overlap was forced in every replay); else HARNESS-ERROR."
require_count REPRO_STORES "$STORES"
[[ $OFFSETS =~ ^[0-9]+(,[0-9]+)*$ ]] || harness_fail "REPRO_OFFSETS_MS=$OFFSETS: want comma-separated integers >= 0"
NOFF=$(tr ',' '\n' <<<"$OFFSETS" | grep -c .)
preflight "$WRITER"
repro_root
prepare_writer "$WRITER"
detail="mode=forced-overlap write_behind=$FORCED_WB store=v150/$SCENARIO instances=$STORES offsets=$NOFF"
total=0
read_instances=0
for k in $(seq 1 "$STORES"); do
  iroot="$ROOT/i$k"
  write_store "$iroot" v150 "$SCENARIO"
  log="$iroot.reader.log"
  R9_ROOT="$iroot" R9_DIAG_STORE="v150/uor/stores/$SCENARIO" R9_FORCED_WB="$FORCED_WB" R9_FORCED_OFFSETS_MS="$OFFSETS" \
    run_reader "$log" ./pkg/storage -run '^TestR9ForcedOverlapV150AsyncdocsSnapClean$' -count=1 -timeout=60m -v
  rc=$?
  grep -a -E '^R9FORCED ' "$log" | cut -c1-400 | norm_msg | sed "s/^/REPRO_READER i$k /"
  sum=$(grep -a -o 'R9FORCED SUMMARY wb=[a-z]* runs=[0-9]* failed_runs=[0-9]* runs_with_target=[0-9]* runs_with_gate_timeout=[0-9]*' "$log" | tail -n1)
  [[ -n $sum ]] || { tail -n 40 "$log" | cut -c1-400 | norm_msg; harness_fail "instance $k: no R9FORCED SUMMARY line (reader exit $rc)"; }
  runs=$(sed -n 's/.* runs=\([0-9]*\).*/\1/p' <<<"$sum")
  fr=$(sed -n 's/.*failed_runs=\([0-9]*\).*/\1/p' <<<"$sum")
  wt=$(sed -n 's/.*runs_with_target=\([0-9]*\).*/\1/p' <<<"$sum")
  [[ $runs -eq $NOFF ]] || harness_fail "instance $k: ran $runs of $NOFF offsets"
  errs=$(grep -a -E '^R9FORCED run=[0-9]+ error ' "$log" | sed -E 's/^R9FORCED run=[0-9]+ error //' | norm_msg | LC_ALL=C sort | uniq -c | sed 's/^ *//' | tr '\n' ';' | sed 's/;$//')
  part=" i$k:[${sum#R9FORCED SUMMARY } reader_exit=$rc"
  if [[ $fr -gt 0 ]]; then
    part+=" refused=[$errs]"
  elif [[ $rc -ne 0 ]]; then
    tail -n 40 "$log" | cut -c1-400 | norm_msg
    harness_fail "instance $k: the reader exited $rc although no replay refused an entry"
  elif [[ $FORCED_WB == on && $wt -ne $NOFF ]]; then
    harness_fail "instance $k: the overlap was forced in $wt of $NOFF replays (no apply held at a buffered_commit entry)"
  fi
  detail+="$part]"
  total=$((total + fr))
  read_instances=$((read_instances + 1))
done
[[ $read_instances -eq $STORES ]] || harness_fail "read $read_instances of $STORES instances"
[[ $total -eq 0 ]] && repro_result PASS "no replay refused a journal entry $detail"
repro_result FAIL "failed_runs=$total $detail"
