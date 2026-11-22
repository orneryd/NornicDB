# Overlap-inducing mode of snapshot-wal-recovery.sh (sourced by it; do not run directly).
# Reader: TestR9OverlapInducingV150AsyncdocsSnapClean (pkg/storage) of this checkout, R9_OVERLAP_WB=$OVERLAP_WB.
# The steps are printed by the script (REPRO_ABOUT lines).
STORES="${REPRO_STORES-1}"
OFFSETS="${REPRO_OFFSETS_MS-0,1,2,3,4,5,6,8,10,12,15,20,25,30,40,50}"
about \
  "item=$ITEM store=v150/$SCENARIO writer=$WRITER (v1.5.0 c99e77131 with the v1.5.0 writer test TestUORBoltWrite) reader=TestR9OverlapInducingV150AsyncdocsSnapClean write_behind=$OVERLAP_WB instances=REPRO_STORES($STORES) offsets_ms=REPRO_OFFSETS_MS($OFFSETS)" \
  "scope: the journal replay through a copy of RecoverIntoEngine with an entry hook; the real nornicdb.Open start-up is not run. Each job writes its own store instances, so the write_behind=on job and its write_behind=off control replay different stores." \
  "steps, for each of the REPRO_STORES instances: the writer test writes the store into a new instance directory; the reader runs once." \
  "steps, for each offset, on a new copy of the store's data directory: (1) a new engine with write-behind as write_behind; (2) with write-behind on, a wrapper around the background write-behind apply; (3) the latest snapshot restored and the journal replayed through the copy of RecoverIntoEngine; (4) the engine closed. The wrapper is armed when the hook sees the first journal entry and holds every apply that starts while it is armed. When the hook sees the first buffered_commit entry while at least one apply is held (the target), it disarms the wrapper and releases the held applies before that entry is replayed; each released apply sleeps offset_ms, then runs. A held apply not released within 10000 ms runs and is counted as a gate timeout. With write-behind off there is no wrapper and no apply." \
  "run=<i> lines: the reader's R9OVERLAP lines: failed = journal entries the replay refused; target_seq = the target entry (none if the hook found none); held_at_target = applies held when the target was found; applies_held = applies the wrapper held; gate_timeouts = gate timeouts; target_err = the target's replay error; err = the error the replay returned; close_err = the error the close returned." \
  "runs, failed_runs, runs_with_target, runs_with_gate_timeout, runs_with_replay_error, runs_with_close_error: copied from the reader's 'R9OVERLAP SUMMARY' line; each counts offsets (failed > 0, a target, a gate timeout, err not nil, close_err not nil)." \
  "Verdict: HARNESS-ERROR if an instance has no SUMMARY line, runs != the number of offsets, runs_with_replay_error > 0, runs_with_close_error > 0, runs_with_gate_timeout > 0, with write_behind=on runs_with_target != runs, with write_behind=off runs_with_target != 0, fewer instances were read than REPRO_STORES, or reader_exit != 0 with failed_runs = 0. Otherwise FAIL if failed_runs > 0 on an instance, else PASS."
require_count REPRO_STORES "$STORES"
[[ $OFFSETS =~ ^[0-9]+(,[0-9]+)*$ ]] || harness_fail "REPRO_OFFSETS_MS=$OFFSETS: want comma-separated integers >= 0"
NOFF=$(tr ',' '\n' <<<"$OFFSETS" | grep -c .)
preflight "$WRITER"
repro_root
prepare_writer "$WRITER"
detail="mode=overlap-inducing write_behind=$OVERLAP_WB store=v150/$SCENARIO instances=$STORES offsets=$NOFF"
total=0
read_instances=0
field() { sed -n "s/.* $1=\([0-9]*\).*/\1/p" <<<"$2"; }
for k in $(seq 1 "$STORES"); do
  iroot="$ROOT/i$k"
  write_store "$iroot" v150 "$SCENARIO"
  log="$iroot.reader.log"
  R9_ROOT="$iroot" R9_DIAG_STORE="v150/uor/stores/$SCENARIO" R9_OVERLAP_WB="$OVERLAP_WB" R9_OVERLAP_OFFSETS_MS="$OFFSETS" \
    run_reader "$log" ./pkg/storage -run '^TestR9OverlapInducingV150AsyncdocsSnapClean$' -count=1 -timeout=60m -v
  rc=$?
  grep -a -E '^R9OVERLAP ' "$log" | cut -c1-400 | norm_msg | sed "s/^/REPRO_READER i$k /"
  sum=$(grep -a -o 'R9OVERLAP SUMMARY wb=[a-z]* runs=[0-9]* failed_runs=[0-9]* runs_with_target=[0-9]* runs_with_gate_timeout=[0-9]* runs_with_replay_error=[0-9]* runs_with_close_error=[0-9]*' "$log" | tail -n1)
  [[ -n $sum ]] || { tail -n 40 "$log" | cut -c1-400 | norm_msg; harness_fail "instance $k: no R9OVERLAP SUMMARY line (reader exit $rc)"; }
  runs=$(field runs "$sum"); fr=$(field failed_runs "$sum"); wt=$(field runs_with_target "$sum")
  gto=$(field runs_with_gate_timeout "$sum"); rer=$(field runs_with_replay_error "$sum"); cer=$(field runs_with_close_error "$sum")
  [[ $runs -eq $NOFF ]] || harness_fail "instance $k: ran $runs of $NOFF offsets"
  [[ $rer -eq 0 && $cer -eq 0 ]] || harness_fail "instance $k: runs_with_replay_error=$rer runs_with_close_error=$cer"
  [[ $gto -eq 0 ]] || harness_fail "instance $k: runs_with_gate_timeout=$gto"
  if [[ $OVERLAP_WB == on ]]; then
    [[ $wt -eq $NOFF ]] || harness_fail "instance $k: runs_with_target=$wt of $NOFF"
  else
    [[ $wt -eq 0 ]] || harness_fail "instance $k: runs_with_target=$wt with write-behind off"
  fi
  errs=$(grep -a -E '^R9OVERLAP run=[0-9]+ error ' "$log" | sed -E 's/^R9OVERLAP run=[0-9]+ error //' | norm_msg | LC_ALL=C sort | uniq -c | sed 's/^ *//' | tr '\n' ';' | sed 's/;$//')
  part=" i$k:[${sum#R9OVERLAP SUMMARY } reader_exit=$rc"
  if [[ $fr -gt 0 ]]; then
    part+=" refused=[$errs]"
  elif [[ $rc -ne 0 ]]; then
    tail -n 40 "$log" | cut -c1-400 | norm_msg
    harness_fail "instance $k: the reader exited $rc with failed_runs=0"
  fi
  detail+="$part]"
  total=$((total + fr))
  read_instances=$((read_instances + 1))
done
[[ $read_instances -eq $STORES ]] || harness_fail "read $read_instances of $STORES instances"
[[ $total -eq 0 ]] && repro_result PASS "failed_runs=0 $detail"
repro_result FAIL "failed_runs=$total $detail"
