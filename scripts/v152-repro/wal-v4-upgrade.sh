#!/usr/bin/env bash
# G-wal-v4-upgrade (ledger E4): a b6d8e762a store of an asynchronous Bolt import with vectors (scenario
# REPRO_SCENARIO, default asyncbolt-nosnap-clean; the item covers asyncbolt-{nosnap,snap}-{clean,kill}).
# The steps are printed by the script (REPRO_ABOUT lines).
# Writer: the b6d8e762a writer test TestUORBoltWrite at commit 068646518 (b6d8e762a plus that test;
# branch scratch/v152-upgrade-writers-b6).
# Reader: TestV152UpgradeRead of this checkout, on REPRO_STORES instances (default 1).
# Usage (from a checkout of the candidate branch): bash scripts/v152-repro/wal-v4-upgrade.sh
ITEM=wal-v4-upgrade
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
SCENARIO="${REPRO_SCENARIO:-asyncbolt-nosnap-clean}"
STORES="${REPRO_STORES-1}"

WRITER=068646518ee813cffc4accfed3b767a01c95fe4e
about \
  "item=$ITEM store=b6/$SCENARIO writer=$WRITER (b6d8e762a with the b6d8e762a writer test TestUORBoltWrite) reader=TestV152UpgradeRead instances=REPRO_STORES($STORES)" \
  "steps, for each of the REPRO_STORES instances: the writer test writes the store into a new instance directory; the reader runs once, on a new copy of the store's data directory." \
  "reader steps: (1) fingerprint = the sorted list of path, size and the first 16 hex digits of the SHA-256 of every file in the data directory; (2) nornicdb.Open without storage upgrade; (3) only if that open returns an error: UPR_REFUSED_NOFLAG with unchanged = the fingerprint now equals step 1's; then nornicdb.Open with storage upgrade allowed; if that open also returns an error: UPR_UNCHANGED_AFTER_UPGRADE_REFUSAL = the fingerprint now equals step 1's, the result 'upgrade=REFUSED', and the reader ends; (4) the open store's nodes, properties and vectors compared with the writer's expected.json; close; (5) a second nornicdb.Open, with storage upgrade allowed only if the open with upgrade of step 3 succeeded; on an error the result 'second=FAILED' and the reader ends; (6) the same comparison; close; (7) the result 'upgraded=<bool> second=ok failed=<bool>', failed = any check of the reader failed. A close that returns an error prints UPR_CLOSE_ERROR and stops the reader." \
  "path=<path>: <message>: the reader's UPR_RESULT line, by its form: 'upgrade refusal' (upgrade=REFUSED; files_unchanged = the UPR_UNCHANGED_AFTER_UPGRADE_REFUSAL value), 'second-open error' (second=FAILED), 'state difference or other reader error' (second=ok failed=true)." \
  "upgraded=<bool> second=ok failed=false: the result line, when it has this form." \
  "reader_exit=<rc>: the reader's exit status for that instance." \
  "Verdict: HARNESS-ERROR if an instance prints UPR_CLOSE_ERROR, has no UPR_RESULT line or an unrecognised one, reader_exit != 0 on an instance without a path, or fewer instances were read than REPRO_STORES. Otherwise FAIL if an instance has a path, and PASS if none has."
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
  closes=$(grep -a -E '^UPR_CLOSE_ERROR ' "$log" | cut -c1-400 | norm_msg | tr '\n' ';' | sed 's/;$//')
  [[ -z $closes ]] || harness_fail "instance $k: close error: $closes"
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
