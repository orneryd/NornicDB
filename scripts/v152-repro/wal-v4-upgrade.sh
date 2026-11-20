#!/usr/bin/env bash
# G-wal-v4-upgrade (ledger E4): a b6d8e762a store of an asynchronous Bolt import with vectors (scenario
# REPRO_SCENARIO, default asyncbolt-nosnap-clean; the item covers asyncbolt-{nosnap,snap}-{clean,kill}) must be
# upgraded in place with storage upgrade allowed and read back with every node, property and vector.
# Writer: the b6d8e762a writer test TestUORBoltWrite at commit 068646518 (b6d8e762a plus that test;
# branch scratch/v152-upgrade-writers-b6).
# Reader: TestV152UpgradeRead of this checkout (first open without upgrade, then with it, then a second open),
# on REPRO_STORES independent instances (default 1).
# Usage (from a checkout of the candidate branch): bash scripts/v152-repro/wal-v4-upgrade.sh
ITEM=wal-v4-upgrade
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
SCENARIO="${REPRO_SCENARIO:-asyncbolt-nosnap-clean}"
STORES="${REPRO_STORES-1}"

WRITER=068646518ee813cffc4accfed3b767a01c95fe4e
about \
  "item=$ITEM store=b6/$SCENARIO writer=$WRITER (b6d8e762a with the b6d8e762a writer test TestUORBoltWrite) reader=TestV152UpgradeRead instances=REPRO_STORES($STORES)" \
  "i<k>: one independently written store instance, read once: first open without storage upgrade, then with it, then a second open; every node, property and vector compared with the writer's expected.json." \
  "path=<path>: <message>: the reader's UPR_RESULT line for the instance, classified: 'upgrade refusal' (upgrade=REFUSED; the message is the refusal, and files_unchanged is the reader's UPR_UNCHANGED_AFTER_UPGRADE_REFUSAL value: store files identical before and after the refused open), 'second-open error' (second=FAILED), 'state difference or other reader error' (second=ok failed=true)." \
  "upgraded=<bool> second=ok failed=false: the instance opened and read back as expected." \
  "reader_exit=<rc>: the reader's exit status for that instance. An instance with a failure path is FAIL whatever its exit status; an instance without one is PASS only with reader_exit=0, else HARNESS-ERROR." \
  "HARNESS-ERROR also if an instance has no UPR_RESULT line, or fewer instances were read than REPRO_STORES."
require_count REPRO_STORES "$STORES"
preflight "$WRITER"
repro_root
prepare_writer "$WRITER"
read_instances=0
detail="store=b6/$SCENARIO instances=$STORES"
failed=0
for k in $(seq 1 "$STORES"); do
  iroot="$ROOT/i$k"
  write_store "$iroot" b6 "$SCENARIO"
  log="$iroot.reader.log"
  UPR_ROOT="$iroot" UPR_READ_DIR=read UPR_ONLY="b6/$SCENARIO" \
    run_reader "$log" ./pkg/bolt -run '^TestV152UpgradeRead$' -count=1 -timeout=45m -v
  rc=$?
  grep -a -E '^UPR_(OPEN|RESULT|REFUSED_NOFLAG|UNCHANGED_AFTER_UPGRADE_REFUSAL) ' "$log" | cut -c1-600 | norm_msg | sed "s/^/REPRO_READER i$k /"
  res=$(grep -a -E "^UPR_RESULT b6/$SCENARIO " "$log" | tail -n1)
  [[ -n $res ]] || { tail -n 40 "$log" | cut -c1-400 | norm_msg; harness_fail "instance $k: no UPR_RESULT line for b6/$SCENARIO (reader exit $rc)"; }
  body=${res#UPR_RESULT b6/$SCENARIO }
  if [[ $body == "upgrade=REFUSED err="* ]]; then
    failed=1
    unchanged=$(grep -a -o -E "^UPR_UNCHANGED_AFTER_UPGRADE_REFUSAL b6/$SCENARIO [a-z]+" "$log" | awk '{print $3}')
    detail+=" i$k:[reader_exit=$rc path=upgrade refusal (files_unchanged=${unchanged:-not printed}): $(norm_msg <<<"${body#upgrade=REFUSED err=}" | cut -c1-420)]"
  elif [[ $body == *" second=FAILED err="* ]]; then
    failed=1
    detail+=" i$k:[reader_exit=$rc path=second-open error: $(norm_msg <<<"${body#* second=FAILED err=}" | cut -c1-420)]"
  elif [[ $body == *" second=ok failed=true" ]]; then
    failed=1
    detail+=" i$k:[reader_exit=$rc path=state difference or other reader error: $(grep -a -E '_test\.go:[0-9]+: ' "$log" | head -n 3 | norm_msg | cut -c1-300 | tr '\n' ' ')]"
  elif [[ $body == *" second=ok failed=false" ]]; then
    if [[ $rc -ne 0 ]]; then
      tail -n 40 "$log" | cut -c1-400 | norm_msg
      harness_fail "instance $k: the reader exited $rc although its result is '$body'"
    fi
    detail+=" i$k:[reader_exit=$rc $body]"
  else
    harness_fail "instance $k: unrecognised reader result '$body'"
  fi
  read_instances=$((read_instances + 1))
done
[[ $read_instances -eq $STORES ]] || harness_fail "read $read_instances of $STORES instances"
[[ $failed -eq 0 ]] && repro_result PASS "$detail"
repro_result FAIL "$detail"
