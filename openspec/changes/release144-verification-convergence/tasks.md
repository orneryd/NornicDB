# Release v1.4.4 verification and convergence

Scope: all remaining failures from full `make verify` on `release/v1.4.4`, plus the entire nine-finding duplicate investigation from PR #1638. This includes baseline and newly exposed integrated regressions; allocation and Northwind work do not limit the scope.
- [x] Fetch and freeze release `0c0495b6db64453ed966923e0c8d114956ccdc50`.
- [x] Preserve the nine-finding PR #1638 inventory and original evidence.
- [x] Make the installed duplicate check advisory without changing its analyzer.
- [x] Complete full baseline `make verify ALL=1` against released main (allocation gate failed; all other applicable steps passed).
- [x] Refresh the remote code graph from the same immutable release commit.
- [x] Obtain approval for the runtime metadata Go API migration with unchanged stored/wire formats.
- [x] Integrate runtime metadata migration and prove immutable-copy and frozen native/transport payload compatibility (focused tests pass; full integrated verification remains pending).
- [x] Correct verification failures and verify affected public contracts at checkpoint `89e2ecb8e`; owner-guard follow-up qualification is tracked below.
- [x] Resolve or justify event-subscription adapters (inventory 1).
- [x] Converge database and DBMS identity projection (inventory 2).
- [x] Verify retirement of the previous mutation receiver (inventory 3).
- [x] Resolve vector-index/index construction ownership (inventory 4).
- [x] Resolve independent batch ownership (inventory 5).
- [x] Resolve transaction/engine refresh preparation ownership (inventory 6).
- [x] Record graph importer/prediction consumer coupling evidence (inventory 7).
- [x] Resolve Fabric dispatch/preparation ownership (inventory 8).
- [x] Resolve running-statement registration/transaction observation ownership (inventory 9).
- [x] Inventory the invalid cache-capacity sweep: inherited cache settings were stripped; exclude its capacity comparison.
- [x] Reproduce the query discrepancy with explicit config and effective decay/cache settings; correct benchmark isolation. Controlled result hashes match; warmed decay-off reads recover expected latency.
- [x] Profile the verified 100,000-entry configuration and test promising follow-on creation optimizations. See the portable Northwind report, including the GOGC=400 RSS tradeoff and separate default-GC diagnostic.
- [x] Verify frozen native/transport payloads and rerun allocation ceilings after the runtime API migration; release and original-main gates pass at `89e2ecb8e`.
- [x] Rebase onto fetched release `f0c932b902b4112114eaeb1fd8a160a6b2bb05cc` without a merge commit; working changes restored intact. Final rebased verification remains required.
- [x] Run full integrated checks on committed checkpoint `89e2ecb8e`; all selected applicable stages pass.
- [x] Open PR #1696 and Linear NOR-313 with the complete catch-up scope.
- [ ] Publish the final PR result with exact results, skips, inventory dispositions
  and specific correctness/performance/memory reasons for retained separation.

The checker is informative, as explicitly requested. An advisory finding or an
incomplete analysis is never recorded as a clean check. No analysis thresholds,
exclusions or ownership proof rules are changed by this policy decision.

## Integrated regression corrections in progress

- [x] Preserve historical V4 replication snapshot metadata through the approved runtime API migration; focused historical snapshot tests pass.
- [x] Preserve the frozen auth-record oracle through typed metadata reads; focused stored compatibility test passes without rewriting goldens.
- [x] Remove the per-candidate namespace metadata record allocation; original UNWIND/MATCH allocation assertion passes unchanged. Runtime copy, bounded reuse and race tests pass at focused scope; workload timing remains pending.
- [x] Route the composite label fixture through the existing fixture owner and add the interactive-read suite boundary.
- [x] Finish the native entity provenance guard migration, retaining planted negative coverage (focused pass).
- [x] Restore internal/public scalar execution allocation ceilings in focused runs: about 995 B/14 and 2,113 B/32; unchanged final ratchet remains required.
- [x] Repeat full integrated `make test` after source freeze and rebase; final command passes with no failures, including cached unchanged packages. The earlier failed runs remain recorded as failures.

- [x] Rebase exposed a newly merged raw Badger fixture constructor in the checkpoint release test; register it with the existing fixture owner while preserving explicit close/reopen behavior. Focused and integrated checks recorded separately.

## Final advisory follow-up

- [x] Review every row of the completed 25-candidate advisory inventory against current source; preserve original nine dispositions.
- [x] Converge continuation type membership, raw label-transition reads and publication identity cleanup; focused tests pass.
- [x] Verify native codec error propagation and identity preservation (encoder 100%, decoder 92.9% statement coverage at focused scope).
- [x] Complete follow-up full `make test` (exit 0; unchanged packages may use the Go test cache).
- [x] Complete committed-head `make verify ALL=1` at checkpoint `89e2ecb8e` (exit 0).

Evidence: [advisory dispositions](followup-advisory-dispositions.md), [Northwind measurements](../../../docs/performance/release144-verification-convergence.md).

## Owner-binding timing correction

- [x] Preserve full verification pass at `89e2ecb8e`, plus original-main allocation pass.
- [x] Reproduce the ~6.5% cache-hit timing regression with six isolated two-second samples per binary and CPU profile.
- [x] Avoid redundant complete runtime-state copy/comparison for an already-bound owner/ID; focused metadata/identity tests pass and repeated timing matches release within variation.
- [x] Re-run full `make test` after correcting nested-module fixture scanning and refreshing source-position-only census artifacts (exit 0).
- [x] Repeat matched Northwind pairs after the guard; all counts/hashes match and ingestion differs by less than 1%.
- [ ] Re-run full verification on the follow-up committed head; final receipt/status lives on PR #1696.

These check boxes capture implementation state before this commit. The final checked head/base and readiness status are recorded in [PR #1696](https://github.com/orneryd/NornicDB/pull/1696) and NOR-313.

The fixture-scanner correction retains active-module and hidden non-module leak controls and error propagation; only nested Go modules leave its population. Registration rules and the duplicate analyzer/hooks are unchanged. Census normalization proves its regenerated difference is source positions alone, without rule, write-population or violation changes.

- [x] Extend the fresh workload confirmation to three samples per binary/mode: every count/hash matches, medians within 5%, cache-hit timing restored with lower B/op. All initial measurements retained; no stable large ingest gain or universal memory claim.
