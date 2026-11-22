#!/usr/bin/env bash
# G-wal-replay-rebuild (ledger UPG-RB4, R9-1), v1.5.0 store asyncdocs-snap-clean, orders A and B of
# TestV152SnapshotRebuild. The steps are printed by the script (REPRO_ABOUT lines).
# Writer: the v1.5.0 writer test TestUORBoltWrite at commit 87311bbd2 (v1.5.0 c99e77131 plus that test;
# branch scratch/v152-upgrade-writers-v150), run once per instance (REPRO_STORES, default 8).
# Reader: TestV152SnapshotRebuild of this checkout, REPRO_COUNT repetitions (default 4) of each order per instance.
# Usage (from a checkout of the candidate branch): bash scripts/v152-repro/wal-replay-rebuild.sh
ITEM=wal-replay-rebuild
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
SCENARIO=asyncdocs-snap-clean
COUNT="${REPRO_COUNT-4}"
STORES="${REPRO_STORES-8}"

WRITER=87311bbd23df966296ebfae50eeae3ba9ab5be51
about \
  "item=$ITEM store=v150/$SCENARIO writer=$WRITER (v1.5.0 c99e77131 with the v1.5.0 writer test TestUORBoltWrite) reader=TestV152SnapshotRebuild instances=REPRO_STORES($STORES) repetitions=REPRO_COUNT($COUNT)" \
  "covers: the start-up refusal of the v1.5.0 store asyncdocs-snap-clean after its MANIFEST is overwritten. It does not cover the lost writes after a rebuild in the UPG-RB4 report, nor the other stores of the writer matrix." \
  "steps, for each of the REPRO_STORES instances: the writer test writes the store into a new instance directory; the reader runs with go test -count=REPRO_COUNT; each repetition runs order A, then order B, each on a new copy of the store's data directory." \
  "order A, before the common steps: nornicdb.Open with storage upgrade allowed; on an error the result is 'step=upgrade REFUSED' and the order ends; else close." \
  "common steps (A after its first open, B from the start): (1) write the bytes 'damaged' over MANIFEST; (2) nornicdb.Open with storage upgrade allowed; on an error the result is 'step=rebuild FAILED' and the order ends; (3) rebuilt = a directory <data>.corrupted-* exists; (4) missing, changed, extra = the comparison of the open store's nodes, properties and vectors with the writer's expected.json; (5) close; (6) nornicdb.Open with storage upgrade allowed; on an error the result is 'step=second FAILED' and the order ends; (7) second_missing, second_changed, second_extra = the same comparison; (8) close; (9) the result line. A close that returns an error prints UPR_CLOSE_ERROR and stops the reader." \
  "A=intact-<n>-of-<count>, B=intact-<n>-of-<count>: n = the number of the reader's UPR_REBUILD_RESULT lines for that order that read exactly 'rebuilt=true missing=0 changed=0 extra=0 second_missing=0 second_changed=0 second_extra=0'." \
  "paths=[<order> <path> x<k>: <message>; ...]: every other UPR_REBUILD_RESULT line, by its form: 'upgrade refusal' (step=upgrade REFUSED), 'damaged-start refusal' (step=rebuild FAILED), 'second-open error' (step=second FAILED), 'rebuilt=false' (rebuilt=false), 'state difference' (rebuilt=true with a count above 0), 'unrecognised result' (any other form); grouped by order, path and message; k counts lines." \
  "reader_exit=<rc>: the reader's exit status for that instance." \
  "Verdict: HARNESS-ERROR if an instance prints UPR_CLOSE_ERROR, its log does not hold exactly 2 x REPRO_COUNT UPR_REBUILD_RESULT lines, reader_exit != 0 on an instance without a path, or fewer instances were read than REPRO_STORES. Otherwise FAIL if an instance has a path, and PASS if none has; PASS is printed as 'not observed in this run'."
require_count REPRO_STORES "$STORES"
require_count REPRO_COUNT "$COUNT"
preflight "$WRITER"
repro_root
prepare_writer "$WRITER"
read_instances=0
detail="store=v150/$SCENARIO instances=$STORES repetitions=$COUNT"
failed=0
for k in $(seq 1 "$STORES"); do
  iroot="$ROOT/i$k"
  write_store "$iroot" v150 "$SCENARIO"
  log="$iroot.reader.log"
  UPR_ROOT="$iroot" UPR_READ_DIR=read UPR_ONLY="v150/$SCENARIO" \
    run_reader "$log" ./pkg/bolt -run '^TestV152SnapshotRebuild$' -count="$COUNT" -timeout=45m -v
  rc=$?
  closes=$(grep -a -E '^UPR_CLOSE_ERROR ' "$log" | cut -c1-400 | norm_msg | tr '\n' ';' | sed 's/;$//')
  [[ -z $closes ]] || harness_fail "instance $k: close error: $closes"
  lines=$(grep -a -E "^UPR_REBUILD_RESULT v150/$SCENARIO " "$log")
  printf '%s\n' "$lines" | cut -c1-400 | norm_msg | sed "s/^/REPRO_READER i$k /"
  got=$(printf '%s\n' "$lines" | grep -a -c 'UPR_REBUILD_RESULT')
  [[ $got -eq $((COUNT * 2)) ]] || { tail -n 40 "$log" | cut -c1-400; harness_fail "instance $k: expected $((COUNT * 2)) rebuild results, found $got (reader exit $rc)"; }
  part=" i$k:"
  for order in A-upgrade-then-damage B-damage-then-upgrade; do
    ok=$(printf '%s\n' "$lines" | grep -a " $order " | grep -a -c 'rebuilt=true missing=0 changed=0 extra=0 second_missing=0 second_changed=0 second_extra=0')
    part+=" ${order%%-*}=intact-$ok-of-$COUNT"
  done
  # Classify every non-intact result line by its failure path (order, path, message), counted.
  paths=$(printf '%s\n' "$lines" | awk -v sc="v150/$SCENARIO" '
    { order = ($3 ~ /^A-/) ? "A" : "B"; rest = $0; sub(/^UPR_REBUILD_RESULT [^ ]+ [^ ]+ /, "", rest) }
    rest ~ /^step=upgrade REFUSED err=/ { sub(/^step=upgrade REFUSED err=/, "", rest); print order " upgrade refusal: " rest; next }
    rest ~ /^step=rebuild FAILED err=/  { sub(/^step=rebuild FAILED err=/, "", rest);  print order " damaged-start refusal: " rest; next }
    rest ~ /^step=second FAILED err=/   { sub(/^step=second FAILED err=/, "", rest);   print order " second-open error: " rest; next }
    rest ~ /^rebuilt=false/             { print order " rebuilt=false: " rest; next }
    rest ~ /^rebuilt=true missing=0 changed=0 extra=0 second_missing=0 second_changed=0 second_extra=0$/ { next }
    rest ~ /^rebuilt=true/              { print order " state difference: " rest; next }
    { print order " unrecognised result: " rest }' | norm_msg | LC_ALL=C sort | uniq -c | sed -E 's/^ *([0-9]+) ([AB]) ([^:]+): (.*)$/\2 \3 x\1: \4/' | tr '\n' ';' | sed 's/;$//')
  part+=" reader_exit=$rc"
  if [[ -n $paths ]]; then
    failed=1
    part+=" paths=[$paths]"
  elif [[ $rc -ne 0 ]]; then
    tail -n 40 "$log" | cut -c1-400 | norm_msg
    harness_fail "instance $k: the reader exited $rc although every result is intact"
  fi
  detail+="$part"
  read_instances=$((read_instances + 1))
done
[[ $read_instances -eq $STORES ]] || harness_fail "read $read_instances of $STORES instances"
[[ $failed -eq 0 ]] && repro_result PASS "not observed in this run $detail"
repro_result FAIL "$detail"
