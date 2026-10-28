# PR #1525 / NOR-7 readiness

Target: updated release/v1.4.4 after merged #1541, baseline a9265090a5d7bd8e34932cf8f504765c01aa147e.
Owner: Tim Sweet; Urgent ticket reassigned from iBurn/mm0nst3r. No PR size limit.

- [x] Verify highest-priority oldest eligible open PR and assign NOR-7 to Tim.
- [x] Isolate original 34-file own layer (5637d0ad5 over dad30a7a1), excluding obsolete parent stack.
- [x] Read-only DeepSeek AC/security inventory; root independently confirms concrete native/ANTLR divergence. Static review is not runtime qualification.
- [x] Reingest immutable current release in background and independently verify its repository marker before final duplicate analysis.
- [ ] Preserve fail-first evidence for supported remote CREATE/ALTER/DROP parity, exact quoted catalog identities, complete input rejection and credential-safe diagnostics through public protocols.
- [ ] Port required behavior into current scannerless recursive-descent and grammar/command/credential owners; preserve current Go APIs, typed source stages and approved storage contract. Regenerate ANTLR output with the pinned generator; never patch the ANTLR runtime.
- [ ] Verify the complete integrated public-path matrix, EXPLAIN metadata/effects, malformed/trailing input, quoted names and credential role boundaries, with original failing evidence retained.
- [ ] Profile and compare focused native parsing/command workloads with the current immutable baseline; correct regressions without sacrificing correctness. ANTLR correctness cost is accepted, but report measured limits.
- [ ] Resolve every unchanged duplicate-checker finding against the refreshed release graph. No analyzer/hook/rule changes or ignored findings.
- [ ] Refresh derived source inventories/census from actual current source without weakening guards; independent DeepSeek review of defensive security changes.
- [ ] Run fresh make test on the final integrated source and focused acceptance checks; fix all regressions. Another full make verify sweep is explicitly deferred by the maintainer to the next PR; report it as deferred.
- [ ] Commit and push to existing PR, publish exact head/base command/results/evidence, update Linear and mark ready for review.

No existing Badger key or payload layout changes, storage-version bump or migration arm. No Dot notifications. Preserve user CONTRIBUTORS.md in primary. Private security triage stays unstaged. Future-source architecture and final acceptance remain root-owned; mechanical work delegates to DeepSeek, tiny exact tasks to local Qwen. Remote Qwen is unused.

## Integrated correction evidence

- Release graph marker independently verified at a9265090a5d7bd8e34932cf8f504765c01aa147e (63,810 nodes; 2,399,820 relationships).
- Pinned ANTLR 4.13.1 generator under existing JDK 21 reproduced all eight generated outputs byte-for-byte. ANTLR runtime unchanged.
- Preserved failing release-baseline evidence for remote grammar admission, USE argument products, quoted DROP identity, and invalid-tail catalog mutation.
- Focused candidate complete-input/quoted-name/USE product checks passed; imported public matrix and credential-output checks exposed further failures being corrected. No full verification pass claimed.
- Imported legacy expectations that compact IF NOT EXISTS into a different alias name or silently drop despite unsupported trailing options are replaced with rejection/state-preservation assertions, consistent with the current release owner and NOR-7 complete-input AC. Original malformed inputs remain.

- Complete public DDL matrix now passes both parsers over HTTP, Bolt and explicit Bolt transactions. Exact catalog state remains compared, and every captured reply/catalog still enters the full credential gate. Parser-specific rejected-command wording is diagnostic; failure code and data remain strict parity assertions.
- Numeric dotted catalog names now use a contextual FLOAT-suffix predicate in qualified-name grammar, preserving the accepted native name extent without changing the shared lexer/runtime.
- Shared log/trace redactor now consumes retained grammar credential roles before preserving identifiers; focused normalization/Unicode/credential tests pass. Private triage recorded, never staged.
- Unchanged staged checker round 1 analyzed 50 functions and reported one shared cached-storage fixture finding. Resolving that owner; no findings ignored. Initial system-Python import failure was a runtime selection error; actual analysis used the installed hook's existing Python runtime.
- Existing USE benchmark ABBA (20,000 fixed iterations, both sides noui,nolocalllm, M2 Max) retained static allocation counts; dynamic exact-argument products add 32 B/op and one allocation. Timing varied widely during concurrent work; no speed/no-regression claim is made from those samples. CPU profiles and actual command logs preserved.

- Independent DeepSeek security review (207 s) found no additional reproduced leak across exercised error/log/trace/source-replay paths. Its concrete DROP localized-identity failure was corrected and focused-verified. Longer-than-8-KiB redaction remains conservative; hypothetical new prose-error paths are not claimed covered.
- Full remote credential wire test, with NORNICDB_PARSER=antlr and actual JSON test events, passes including native/ANTLR restart, persisted remote aliases, GraphQL rejection checks and formerly tracked NOR-211 EXPLAIN checks.
- Second checker round analyzed 57 functions and required broader cached default/named-storage test-owner convergence; both prior standalone owners are now combined with every non-setup assertion retained.

## Full verification and ingest regression follow-up

- First integrated `make verify BASE=origin/release/v1.4.4` at be44a0c7 against a9265090 completed in 3,857 seconds and failed; not merge-ready. Runtime tests passed except the reviewed URL fixture inventory; allocation ratchet passed. Conformance exposed three explicit-transaction identity regressions, differential exposed GH530 query-statistics retrieval, and docs exposed one stale link. No failures waived.
- [x] Correct the stale docs link; focused docs verification passed.
- [x] Classify six exact synthetic fixture expressions in the existing URL non-egress inventory; unchanged guard passes.
- [x] Complete cached-storage rejection coverage; focused race check passes and all 13 helper statements execute.
- [ ] Verify integrated explicit-transaction relationship provenance correction through the original three TCK scenarios and full conformance.
- [ ] Complete credential-URL role redaction correction and resolve GH530 statistics behavior without exposing credentials.
- [x] Reproduce default Northwind ingest slowdown on isolated release binary and capture CPU/allocation profiles; canonical JSON string projection introduced by #1515 copies the entire batch for each non-ASCII codepoint.
- [x] Correct bounded codepoint decoding, preserve exact Unicode/output/edit mapping and storage format, demonstrate fail-first allocation regression and focused before/after benchmarks.
- [ ] Complete sequential default-workload comparison against pre-#1515 d663ce8f and release plus only the correction, including graph-count verification.
- [ ] Re-run integrated checker and verification after all corrections, then commit/push readiness evidence.

## Maintainer verification and delivery direction

The maintainer explicitly deferred another full `make verify` sweep to the next PR. This PR still requires the performance regressions and remaining AC to be resolved, a passing `make test`, focused AC evidence, and all unchanged duplicate-checker findings addressed. After that, push and merge this PR into `release/v1.4.4`. Report the deferred sweep as deferred, not passed. Benchmark configuration uses the existing script controls `GRAPH_ONLY=1` and `GRAPH_ONLY=0`; temporary harness aliases have been removed. Retain verified work/allocation reductions even when wall time is unchanged.


## Current focused follow-up evidence

- Fresh standard graph-mode seed/count checks: GRAPH_ONLY=1 ingest 12,379.364 ms;
  GRAPH_ONLY=0 ingest 15,274.694 ms; both verified 97,440 nodes and 312,050
  relationships. Five-second target remains unmet. These seed-only runs do not
  establish the subsequent read-query suite.
- Fresh internal/jsontext and pkg/config/dbconfig tests passed. Their work and
  allocation reductions remain integrated; no new production switches.
- Quoted-target/backtick-URI redaction reproduced in an in-tree regression test.
  Grammar-role-based URL projection now passes focused redaction, slow-log and
  remote-credential tests; final race verification follows the latest source.
- Buffered prepared publication reproduced a repeated stored-image read (two
  instead of one). Private prepared-image reuse is under focused race validation;
  public image access remains defensive and closed transactions clear retention.
- No merge-ready claim, push or merge yet. Performance and GH530 diagnostic
  contract reconciliation remain open, followed by make test and the unchanged
  duplicate checker on the final staged tree.
