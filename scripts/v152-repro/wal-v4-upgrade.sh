#!/usr/bin/env bash
# G-wal-v4-upgrade (ledger E4): a b6d8e762a store of an asynchronous Bolt import with vectors (scenario
# REPRO_SCENARIO, default asyncbolt-nosnap-clean; the item covers asyncbolt-{nosnap,snap}-{clean,kill}) must be
# upgraded in place with storage upgrade allowed and read back with every node, property and vector.
# Writer: b6d8e762a with the upgrade-lane writer, commit 068646518 (scratch/v152-upgrade-writers-b6).
# Reader: TestV152UpgradeRead of this checkout (first open without upgrade, then with it, then a second open),
# on REPRO_STORES independent instances (default 1; the refusal has reproduced on every instance so far).
# PASS: the upgrade succeeds and both opens read back the writer's acknowledged state, on every instance.
# FAIL: the upgrade is refused (the line quotes the refusal) or the state differs.
# Usage (from a checkout of the candidate branch): bash scripts/v152-repro/wal-v4-upgrade.sh
ITEM=wal-v4-upgrade
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
SCENARIO="${REPRO_SCENARIO:-asyncbolt-nosnap-clean}"
STORES="${REPRO_STORES:-1}"

repro_root
prepare_writer 068646518ee813cffc4accfed3b767a01c95fe4e
detail="store=b6/$SCENARIO instances=$STORES"
bad=0
for k in $(seq 1 "$STORES"); do
  iroot="$ROOT/i$k"
  write_store "$iroot" b6 "$SCENARIO"
  log="$iroot.reader.log"
  UPR_ROOT="$iroot" UPR_READ_DIR=read UPR_ONLY="b6/$SCENARIO" \
    run_reader "$log" ./pkg/bolt -run '^TestV152UpgradeRead$' -count=1 -timeout=45m -v
  rc=$?
  grep -a -E 'UPR_(OPEN|RESULT|REFUSED_NOFLAG|UNCHANGED_AFTER_UPGRADE_REFUSAL) ' "$log" | cut -c1-600 | sed "s/^/REPRO_READER i$k /"
  res=$(grep -a -E "UPR_RESULT b6/$SCENARIO " "$log" | tail -n1)
  [[ -n $res ]] || { tail -n 40 "$log" | cut -c1-400; harness_fail "instance $k: no UPR_RESULT line for b6/$SCENARIO (reader exit $rc)"; }
  if [[ $res == *"upgrade=REFUSED"* ]]; then
    bad=1
    unchanged=$(grep -a -o "UPR_UNCHANGED_AFTER_UPGRADE_REFUSAL b6/$SCENARIO [a-z]*" "$log" | awk '{print $3}')
    detail+=" i$k:[in-place upgrade refused, files unchanged=$unchanged: $(cut -c1-420 <<<"${res#*err=}")]"
  elif [[ $res == *"second=ok failed=false"* && $rc -eq 0 ]]; then
    detail+=" i$k:[$(sed 's/.*UPR_RESULT [^ ]* //' <<<"$res")]"
  else
    bad=1
    detail+=" i$k:[$(sed 's/.*UPR_RESULT [^ ]* //' <<<"$res" | cut -c1-300) reader exit $rc]"
  fi
done
[[ $bad -eq 0 ]] && repro_result PASS "$detail"
repro_result FAIL "$detail"
