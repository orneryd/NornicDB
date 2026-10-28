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
- [ ] Run make verify-list and fresh make verify BASE=origin/release/v1.4.4 locally on the integrated source; fix all regressions, including baseline failures, and report blocked/skipped/unexecuted checks truthfully.
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
