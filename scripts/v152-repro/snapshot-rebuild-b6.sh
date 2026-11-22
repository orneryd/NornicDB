#!/usr/bin/env bash
# The b6d8e762a snapshot stores (report entries S05, S06 and S07): every "-snap-" store of the b6d8e762a writer matrix,
# orders A and B of TestV152SnapshotRebuild. The steps are printed by the script (REPRO_ABOUT lines).
# Writers, at commit 068646518 (b6d8e762a plus the writer tests; branch scratch/v152-upgrade-writers-b6), run once per
# instance (REPRO_STORES, default 1): TestUORWrite in ./pkg/server for api-, qdrant- and async-snap-{clean,kill};
# TestUORBoltWrite in ./pkg/bolt for asyncdocs-snap-{clean,kill}.
# Reader: TestV152SnapshotRebuild of this checkout, REPRO_COUNT repetitions (default 1) of each order per store.
# Fail-closed: a store that is missing, has no snapshot, or has fewer or more result lines than expected is a
# HARNESS-ERROR (exit 2), never a PASS.
# Usage (from a checkout of the candidate branch): bash scripts/v152-repro/snapshot-rebuild-b6.sh
ITEM=snapshot-rebuild-b6
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
SERVER_SCENARIOS=api-snap-clean,api-snap-kill,qdrant-snap-clean,qdrant-snap-kill,async-snap-clean,async-snap-kill
BOLT_SCENARIOS=asyncdocs-snap-clean,asyncdocs-snap-kill
ALL_SCENARIOS="$SERVER_SCENARIOS,$BOLT_SCENARIOS"
COUNT="${REPRO_COUNT-1}"
STORES="${REPRO_STORES-1}"
INTACT='rebuilt=true missing=0 changed=0 extra=0 second_missing=0 second_changed=0 second_extra=0'

WRITER=068646518ee813cffc4accfed3b767a01c95fe4e
about \
  "item=$ITEM stores=b6/{$ALL_SCENARIOS} writer=$WRITER (b6d8e762a with the writer tests TestUORWrite and TestUORBoltWrite) reader=TestV152SnapshotRebuild instances=REPRO_STORES($STORES) repetitions=REPRO_COUNT($COUNT)" \
  "covers: the eight snapshot stores of the b6d8e762a writer matrix (embedded Cypher API, Qdrant, asynchronous, and asynchronous large documents over Bolt; each written with a clean stop and with a kill), damaged and rebuilt from snapshot plus journal by this build, in both orders. It does not cover the stores without a snapshot." \
  "steps, for each of the REPRO_STORES instances: the server writer writes the six server stores and the Bolt writer the two Bolt stores into one new instance directory; every store must have expected.json, data/MANIFEST and at least one snapshot; the reader runs with go test -count=REPRO_COUNT and UPR_ONLY naming all eight stores (it stops if any is missing); each repetition runs order A, then order B, each on a new copy of the store's data directory." \
  "order A, before the common steps: nornicdb.Open with storage upgrade allowed; on an error the result is 'step=upgrade REFUSED' and the order ends; else close." \
  "common steps (A after its first open, B from the start): (1) write the bytes 'damaged' over MANIFEST; (2) nornicdb.Open with storage upgrade allowed; on an error the result is 'step=rebuild FAILED' and the order ends; (3) rebuilt = a directory <data>.corrupted-* exists; (4) missing, changed, extra = the comparison of the open store's nodes, properties and vectors with the writer's expected.json; (5) close; (6) nornicdb.Open with storage upgrade allowed; on an error the result is 'step=second FAILED' and the order ends; (7) second_missing, second_changed, second_extra = the same comparison; (8) close; (9) the result line. A close that returns an error prints UPR_CLOSE_ERROR and stops the reader." \
  "<store>: A=intact-<n>-of-<count> B=intact-<n>-of-<count>: n = the number of the reader's UPR_REBUILD_RESULT lines for that store and order that read exactly '$INTACT'." \
  "paths=[<order> <path> x<k>: <message>; ...]: every other UPR_REBUILD_RESULT line of that store, by its form: 'upgrade refusal' (step=upgrade REFUSED), 'damaged-start refusal' (step=rebuild FAILED), 'second-open error' (step=second FAILED), 'rebuilt=false' (rebuilt=false), 'state difference' (rebuilt=true with a count above 0), 'unrecognised result' (any other form); grouped by order, path and message; k counts lines." \
  "reader_exit=<rc>: the reader's exit status for that instance." \
  "Verdict: HARNESS-ERROR if a writer fails or leaves a store incomplete, a store has no snapshot, an instance prints UPR_CLOSE_ERROR, a store's log does not hold exactly 2 x REPRO_COUNT UPR_REBUILD_RESULT lines, reader_exit != 0 on an instance without a path, or fewer instances were read than REPRO_STORES. Otherwise FAIL if a store has a path, and PASS if none has; PASS is printed as 'not observed in this run'."
require_count REPRO_STORES "$STORES"
require_count REPRO_COUNT "$COUNT"
preflight "$WRITER"
repro_root
prepare_writer "$WRITER"
read_instances=0
detail="stores=b6/{$ALL_SCENARIOS} instances=$STORES repetitions=$COUNT"
failed=0
only=""
for sc in ${ALL_SCENARIOS//,/ }; do only="${only:+$only,}b6/$sc"; done
for k in $(seq 1 "$STORES"); do
  iroot="$ROOT/i$k"
  write_store_server "$iroot" b6 "$SERVER_SCENARIOS"
  write_store "$iroot" b6 "$BOLT_SCENARIOS"
  for sc in ${ALL_SCENARIOS//,/ }; do
    n=$(find "$iroot/b6/uor/stores/$sc/data/snapshots" -maxdepth 1 -name 'snapshot-*' 2>/dev/null | wc -l | tr -d ' ')
    [[ $n -ge 1 ]] || harness_fail "instance $k: store b6/$sc has no snapshot (a snapshot store without a snapshot does not test the rebuild)"
    say "store b6/$sc snapshots=$n"
  done
  log="$iroot.reader.log"
  UPR_ROOT="$iroot" UPR_READ_DIR=read UPR_ONLY="$only" \
    run_reader "$log" ./pkg/bolt -run '^TestV152SnapshotRebuild$' -count="$COUNT" -timeout=120m -v
  rc=$?
  closes=$(grep -a -E '^UPR_CLOSE_ERROR ' "$log" | cut -c1-400 | norm_msg | tr '\n' ';' | sed 's/;$//')
  [[ -z $closes ]] || harness_fail "instance $k: close error: $closes"
  part=" i$k:"
  inst_failed=0
  for sc in ${ALL_SCENARIOS//,/ }; do
    lines=$(grep -a -E "^UPR_REBUILD_RESULT b6/$sc " "$log")
    printf '%s\n' "$lines" | grep -a . | cut -c1-400 | norm_msg | sed "s/^/REPRO_READER i$k /"
    got=$(printf '%s\n' "$lines" | grep -a -c 'UPR_REBUILD_RESULT')
    [[ $got -eq $((COUNT * 2)) ]] || { tail -n 40 "$log" | cut -c1-400 | norm_msg; harness_fail "instance $k: store b6/$sc: expected $((COUNT * 2)) rebuild results, found $got (reader exit $rc)"; }
    part+=" b6/$sc:"
    for order in A-upgrade-then-damage B-damage-then-upgrade; do
      ok=$(printf '%s\n' "$lines" | grep -a " $order " | grep -a -c "$INTACT")
      part+=" ${order%%-*}=intact-$ok-of-$COUNT"
    done
    # Classify every non-intact result line of this store by its failure path (order, path, message), counted.
    paths=$(printf '%s\n' "$lines" | awk '
      { order = ($3 ~ /^A-/) ? "A" : "B"; rest = $0; sub(/^UPR_REBUILD_RESULT [^ ]+ [^ ]+ /, "", rest) }
      rest ~ /^step=upgrade REFUSED err=/ { sub(/^step=upgrade REFUSED err=/, "", rest); print order " upgrade refusal: " rest; next }
      rest ~ /^step=rebuild FAILED err=/  { sub(/^step=rebuild FAILED err=/, "", rest);  print order " damaged-start refusal: " rest; next }
      rest ~ /^step=second FAILED err=/   { sub(/^step=second FAILED err=/, "", rest);   print order " second-open error: " rest; next }
      rest ~ /^rebuilt=false/             { print order " rebuilt=false: " rest; next }
      rest ~ /^rebuilt=true missing=0 changed=0 extra=0 second_missing=0 second_changed=0 second_extra=0$/ { next }
      rest ~ /^rebuilt=true/              { print order " state difference: " rest; next }
      { print order " unrecognised result: " rest }' | cut -c1-600 | norm_msg | LC_ALL=C sort | uniq -c | sed -E 's/^ *([0-9]+) ([AB]) ([^:]+): (.*)$/\2 \3 x\1: \4/' | tr '\n' ';' | sed 's/;$//')
    if [[ -n $paths ]]; then
      failed=1
      inst_failed=1
      part+=" paths=[$paths]"
    fi
  done
  part+=" reader_exit=$rc"
  if [[ $inst_failed -eq 0 && $rc -ne 0 ]]; then
    tail -n 40 "$log" | cut -c1-400 | norm_msg
    harness_fail "instance $k: the reader exited $rc although every result is intact"
  fi
  detail+="$part"
  read_instances=$((read_instances + 1))
done
[[ $read_instances -eq $STORES ]] || harness_fail "read $read_instances of $STORES instances"
[[ $failed -eq 0 ]] && repro_result PASS "not observed in this run $detail"
repro_result FAIL "$detail"
