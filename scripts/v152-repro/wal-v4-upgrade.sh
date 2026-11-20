#!/usr/bin/env bash
# G-wal-v4-upgrade (ledger E4): a b6d8e762a store of an asynchronous Bolt import with vectors (scenario
# REPRO_SCENARIO, default asyncbolt-nosnap-clean; the item covers asyncbolt-{nosnap,snap}-{clean,kill}) must be
# upgraded in place with storage upgrade allowed and read back with every node, property and vector.
# Writer: b6d8e762a with the upgrade-lane writer, commit 068646518 (scratch/v152-upgrade-writers-b6).
# Reader: TestV152UpgradeRead of this checkout (first open without upgrade, then with it, then a second open).
# PASS: the upgrade succeeds and both opens read back the writer's acknowledged state.
# FAIL: the upgrade is refused (the line quotes the refusal) or the state differs.
# Usage (from a checkout of the candidate branch): bash scripts/v152-repro/wal-v4-upgrade.sh
ITEM=wal-v4-upgrade
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
SCENARIO="${REPRO_SCENARIO:-asyncbolt-nosnap-clean}"

repro_root
WRITER="$(git -C "$REPO" rev-parse --verify -q "068646518ee813cffc4accfed3b767a01c95fe4e^{commit}")" || harness_fail "writer commit 068646518 is not in this repository"
write_store b6 "$WRITER" "$SCENARIO"
log="$ROOT/reader.log"
UPR_ROOT="$ROOT" UPR_READ_DIR=read UPR_ONLY="b6/$SCENARIO" \
  run_reader "$log" ./pkg/bolt -run '^TestV152UpgradeRead$' -count=1 -timeout=45m -v
rc=$?
grep -E 'UPR_(OPEN|RESULT|REFUSED_NOFLAG|UNCHANGED_AFTER_UPGRADE_REFUSAL) |^\s*--- (FAIL|PASS|SKIP): TestV152UpgradeRead' "$log" | cut -c1-600 | sed 's/^/REPRO_READER /'
res=$(grep -E "UPR_RESULT b6/$SCENARIO " "$log" | tail -n1)
[[ -n $res ]] || { tail -n 40 "$log"; harness_fail "no UPR_RESULT line for b6/$SCENARIO (reader exit $rc)"; }
if [[ $res == *"upgrade=REFUSED"* ]]; then
  repro_result FAIL "store=b6/$SCENARIO in-place upgrade refused: $(cut -c1-500 <<<"${res#*err=}") unchanged=$(grep -o "UPR_UNCHANGED_AFTER_UPGRADE_REFUSAL b6/$SCENARIO [a-z]*" "$log" | awk '{print $3}')"
fi
if [[ $res == *"second=ok failed=false"* && $rc -eq 0 ]]; then
  repro_result PASS "store=b6/$SCENARIO $(sed 's/.*UPR_RESULT [^ ]* //' <<<"$res")"
fi
repro_result FAIL "store=b6/$SCENARIO $(sed 's/.*UPR_RESULT [^ ]* //' <<<"$res" | cut -c1-400) (reader exit $rc)"
