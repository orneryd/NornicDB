#!/usr/bin/env bash
# G-wal-replay-rebuild (ledger UPG-RB4, R9-1): a v1.5.0 asynchronous store with nine 2 MiB documents and a
# snapshot (scenario asyncdocs-snap-clean), whose MANIFEST is damaged, must be rebuilt at start from snapshot
# plus journal with every acknowledged write, in both orders of TestV152SnapshotRebuild:
#   A  open with storage upgrade allowed, close, damage, start again;
#   B  damage the old store, then the first start with storage upgrade allowed.
# Writer: the v1.5.0 writer test TestUORBoltWrite at commit 87311bbd2 (v1.5.0 c99e77131 plus that test;
# branch scratch/v152-upgrade-writers-v150). The writer is asynchronous and its journal differs from run to run, so
# the script writes REPRO_STORES independent instances (default 3) and reads each.
# Reader: TestV152SnapshotRebuild of this checkout, REPRO_COUNT repetitions (default 4) of each order per instance.
# Usage (from a checkout of the candidate branch): bash scripts/v152-repro/wal-replay-rebuild.sh
ITEM=wal-replay-rebuild
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
SCENARIO=asyncdocs-snap-clean
COUNT="${REPRO_COUNT-4}"
STORES="${REPRO_STORES-3}"

WRITER=87311bbd23df966296ebfae50eeae3ba9ab5be51
about \
  "item=$ITEM store=v150/$SCENARIO writer=$WRITER (v1.5.0 c99e77131 with the v1.5.0 writer test TestUORBoltWrite) reader=TestV152SnapshotRebuild instances=REPRO_STORES($STORES) repetitions=REPRO_COUNT($COUNT)" \
  "i<k>: one independently written store instance; each is written and read once." \
  "A=intact-<n>-of-<count>, B=intact-<n>-of-<count>: n = the number of the reader's UPR_REBUILD_RESULT lines for that order that read exactly 'rebuilt=true missing=0 changed=0 extra=0 second_missing=0 second_changed=0 second_extra=0'." \
  "paths=[<order> <path> x<k>: <message>; ...]: every other UPR_REBUILD_RESULT line, classified by the failure path it reports and grouped by order, path and message: 'upgrade refusal' (step=upgrade REFUSED, order A's first open with upgrade allowed), 'damaged-start refusal' (step=rebuild FAILED, the start on the damaged store), 'second-open error' (step=second FAILED), 'rebuilt=false' (the start did not rebuild), 'state difference' (rebuilt=true with missing/changed/extra > 0 at the rebuild or the second open). k counts repetitions." \
  "reader_exit=<rc>: the reader's exit status for that instance. An instance with a failure path is FAIL whatever its exit status; an instance without one is PASS only with reader_exit=0, else HARNESS-ERROR." \
  "HARNESS-ERROR also if an instance's log does not hold exactly 2 x count UPR_REBUILD_RESULT lines, or fewer instances were read than REPRO_STORES."
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
[[ $failed -eq 0 ]] && repro_result PASS "$detail"
repro_result FAIL "$detail"
