# NOR-300 / combined PR1695 readiness

- [x] Verify Linear v1.4.4 scope and assign NOR-300 to Tim.
- [x] Preserve original owner and consumer heads; transfer PR1699 AC and close only its delivery PR.
- [x] Fetch release and confirm PR1695 reproduction is based on f45b1a51a.
- [x] Reproduce current-release EXPLAIN/PROFILE constraint metadata failures and registered WRITE procedure effect discrepancies.
- [x] Inventory current canonical statement/effect, permission, cache and Fabric consumers; retained historical heads are reference-only.
- [ ] Fix demonstrated owner/consumer defects without restoring obsolete stacks or adding compatibility wrappers.
- [ ] Verify public outcomes, rejection/no-effect controls, registry replacement, cancellation and unknown-effect behavior on both parsers.
- [ ] Review every duplicate finding and disposition with source/test/measurement evidence.
- [ ] Measure baseline/head representative memory, allocations and throughput after tests are idle.
- [ ] Push candidate and qualify final make test CI, then cleanup task-owned resources.

## Integrated checks and current work

- Schema command EXPLAIN/PROFILE metadata correction and public no-effect controls pass on both parsers.
- Schema permission dispatch now reuses isOrdinarySchemaDDL; both-parser permitted/denied catalog controls pass (2.934 s combined schema run).
- Temporary reproduction probe removed after preserving fail-first evidence; permanent tests retain unchanged-catalog assertions.
- Approved QueryAnalyzer Analyze error-result / Writes / ReadOnly migration is in DeepSeek worktree, not integrated yet. Parent review notes include warm-cache cancellation and registry refresh checks.
- SHOW CONSTRAINTS mode/header failure is assigned to a separate isolated DeepSeek worker; not yet qualified.
- No current full-suite or performance qualification; no merge-readiness claim.

## Later independent findings

- Warm-cache cancellation fail-first reproduced; corrected and focused checks pass.
- Catalog DROP DATABASE / DROP ALIAS false-read reproduced; stateChange fact and canonical statement-effect decision preserve existing data-write permission and transaction flags. Focused controls pass (3.254 s).
- Schema-mode procedure effect fail-first reproduced after adding the mode to live registry tests; corrected using ProcedureSpec.writes without promoting schema procedures to data-write transactions. Focused integrated controls pass (1.056 s).
- Fabric and storage routing still have independent write classifiers; exported contract proposal is pending maintainer choice. Public fail-first evidence is retained, not waived.
- SHOW CONSTRAINTS plain-header proposal is incomplete: 12 public EXPLAIN/PROFILE YIELD/RETURN/alias controls fail on both actual parsers. Follow-up worker must propose retained-tail composition in the canonical owner; no individual text-tail workaround.
- Worker reported QueryCache_TTL's 1 ms immediate-hit timing failure. Independent original test passed 1,000 repetitions; no runtime cache defect established. Test fixture now verifies stored TTL/timestamp bounds, a pre-expiration hit, and explicit elapsed-age eviction without sleeps. Stabilization checks pending on final test source; no production clock or cache behavior changed.

## Integrated migration and verification
- [x] Rebase local work onto pushed dd298f1ca, preserving node-key rejection/no-effect, conservative dynamic Fabric admission, CALL-effect and suppression-cache controls. Remote regression names now describe behavior instead of ticket IDs.
- [x] Complete QueryAnalyzer error-result/Writes/ReadOnly migration, including warm-cache cancellation and procedure-generation refresh.
- [x] Consume canonical effects in CALL subquery evaluation, CALL-tail projection, UNWIND read rewrites and Fabric fragments; retire unreachable correlated lookup unit and keyword classifiers.
- [x] Retained SHOW declarations/tails share canonical binding completion; public schema/mode/header controls pass (2.909s). The integrated checks caught empty node-key EXPLAIN acceptance; shared requirement validation now rejects it. The old EXPLAIN fallback is retired.
- [x] Historical catalog/descriptor/UNWIND diagnostic and authorization checks pass (5.898s). Integrated effect controls pass (1.875s).
- [x] Repository-wide compilation with noui,nolocalllm passes (zero tests executed). Default compilation is blocked by absent local libllama_darwin_arm64; this is not a runtime qualification.
- [x] Approved required planner effect callback and caller-resolved storage analysis migration integrated; public routing/canonical analysis checks pass (5.294s) and unchanged lexical census passes (3.259s). Default storage test linking is blocked by the same missing llama library; the supported nolocalllm affected check is tracked separately.
- [ ] Finish affected-package checks on integrated source, cost comparison, final duplicate dispositions and full make test CI.

- [x] Integrated full Fabric package passes (2.011s); storage routing checks pass with supported nolocalllm (1.002s); repository-wide final API compilation passes with noui,nolocalllm (zero tests executed).
