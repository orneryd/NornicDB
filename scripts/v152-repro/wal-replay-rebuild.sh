#!/usr/bin/env bash
# G-wal-replay-rebuild (ledger UPG-RB4, R9-1): a v1.5.0 asynchronous store with nine 2 MiB documents and a
# snapshot (scenario asyncdocs-snap-clean), whose MANIFEST is damaged, must be rebuilt at start from snapshot
# plus journal with every acknowledged write, in both orders of TestV152SnapshotRebuild:
#   A  open with storage upgrade allowed, close, damage, start again;
#   B  damage the old store, then the first start with storage upgrade allowed.
# Writer: v1.5.0 c99e77131 with the upgrade-lane writer, commit 87311bbd2 (scratch/v152-upgrade-writers-v150).
# The writer is asynchronous and its journal differs from run to run; whether a store triggers the refusal depends
# on the instance. So the script writes REPRO_STORES independent instances (default 3) and reads each.
# Reader: TestV152SnapshotRebuild of this checkout, REPRO_COUNT repetitions (default 4) of each order per instance.
# PASS: every repetition of both orders on every instance rebuilt with missing=0 changed=0 extra=0 at the rebuild
# and at a second open. FAIL: any repetition refused or differed.
# Usage (from a checkout of the candidate branch): bash scripts/v152-repro/wal-replay-rebuild.sh
ITEM=wal-replay-rebuild
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
SCENARIO=asyncdocs-snap-clean
COUNT="${REPRO_COUNT:-4}"
STORES="${REPRO_STORES:-3}"

repro_root
prepare_writer 87311bbd23df966296ebfae50eeae3ba9ab5be51
detail="store=v150/$SCENARIO instances=$STORES repetitions=$COUNT"
bad=0
for k in $(seq 1 "$STORES"); do
  iroot="$ROOT/i$k"
  write_store "$iroot" v150 "$SCENARIO"
  log="$iroot.reader.log"
  UPR_ROOT="$iroot" UPR_READ_DIR=read UPR_ONLY="v150/$SCENARIO" \
    run_reader "$log" ./pkg/bolt -run '^TestV152SnapshotRebuild$' -count="$COUNT" -timeout=45m -v
  rc=$?
  grep -a -E 'UPR_REBUILD_RESULT' "$log" | cut -c1-400 | sed "s/^/REPRO_READER i$k /"
  got=$(grep -a -c 'UPR_REBUILD_RESULT v150/'"$SCENARIO" "$log")
  [[ $got -eq $((COUNT * 2)) ]] || { tail -n 40 "$log" | cut -c1-400; harness_fail "instance $k: expected $((COUNT * 2)) rebuild results, found $got (reader exit $rc)"; }
  part=" i$k:"
  for order in A-upgrade-then-damage B-damage-then-upgrade; do
    ok=$(grep -a 'UPR_REBUILD_RESULT v150/'"$SCENARIO $order"' ' "$log" | grep -a -c 'rebuilt=true missing=0 changed=0 extra=0 second_missing=0 second_changed=0 second_extra=0')
    part+=" ${order%%-*}=intact-$ok-of-$COUNT"
    [[ $ok -eq $COUNT ]] || bad=1
  done
  [[ $rc -eq 0 ]] || bad=1
  errs=$(grep -a -o 'incomplete or empty replay: [^)]*' "$log" | sort | uniq -c | sed 's/^ *//' | tr '\n' ';')
  [[ -n $errs ]] && part+=" refusals=[$errs]"
  detail+="$part"
done
[[ $bad -eq 0 ]] && repro_result PASS "$detail"
repro_result FAIL "$detail"
