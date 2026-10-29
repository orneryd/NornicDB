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
- [ ] Resolve every duplicate-checker finding against the refreshed release graph. Ask before any further analyzer, hook, or rule changes; do not ignore findings.
- [ ] Refresh derived source inventories/census from actual current source without weakening guards; independent DeepSeek review of defensive security changes.
- [ ] Run fresh make test on the final integrated source and focused acceptance checks; fix all regressions. Another full make verify sweep is explicitly deferred by the maintainer to the next PR; report it as deferred.
- [ ] Commit and push to existing PR, publish exact head/base command/results/evidence, update Linear and mark ready for review.

No existing Badger key or payload layout changes, storage-version bump, or new migration arm. The existing V4 arm also converts historical WAL framing. No Dot notifications. Preserve user CONTRIBUTORS.md in primary. Private security triage stays unstaged. Future-source architecture and final acceptance remain root-owned; mechanical work delegates to DeepSeek, tiny exact tasks to local Qwen. Remote Qwen is unused.

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

The maintainer explicitly deferred another full `make verify` sweep to the next PR. This PR still requires the performance regressions and remaining AC to be resolved, a passing `make test`, focused AC evidence, and all duplicate-checker findings addressed. After that, push and merge this PR into `release/v1.4.4`. Report the deferred sweep as deferred, not passed. Benchmark configuration uses the existing script controls `GRAPH_ONLY=1` and `GRAPH_ONLY=0`; temporary harness aliases have been removed. Retain verified work/allocation reductions even when wall time is unchanged.


## Current focused follow-up evidence

- Earlier fixed4 seed/count checks took 12,379.364 ms with GRAPH_ONLY=1 and
  15,274.694 ms with GRAPH_ONLY=0. Later work reduced these timings; they are
  historical results, not the latest measurements.
- Fresh internal/jsontext and pkg/config/dbconfig tests passed. Their work and
  allocation reductions remain integrated; no new production switches.
- Quoted-target/backtick-URI redaction reproduced in an in-tree regression test.
  Grammar-role-based URL projection now passes focused redaction, slow-log and
  remote-credential tests; final race verification follows the latest source.
- Buffered prepared publication reproduced a repeated stored-image read (two
  instead of one). Private prepared-image reuse is under focused race validation;
  public image access remains defensive and closed transactions clear retention.
- Checkpoints `4a7fbc03e` and `31bfb2bdd` were pushed to the draft PR. They are
  not a merge-ready claim: performance, acceptance evidence, final `make test`,
  and the duplicate checker on the final staged tree remain open.

## Northwind and recovery follow-up

- [ ] Reach the approximately five-second Northwind seed target using the
  existing graph-only and search-enabled configurations; compare repeatable
  runs with the same payload and verify all node and relationship counts.
- [ ] Complete integrated review and validation of the search-index and
  endpoint-read allocation reductions; retain only correct, non-degrading work.
- [ ] Preserve arbitrary bytes in buffered WAL keys and verify recovery,
  compression, foreign numeric-identity replay, and V4 conversion of supported
  older WAL formats without changing existing Badger keys or snapshot compatibility.
- [ ] Verify recycled numeric IDs remain distinct across aborted and concurrent
  transactions, pending-name adoption, write-behind rejection and reopening.
- [ ] Verify database startup rejects corrupt, unknown, or unauthorized WALs
  without losing existing data; exercise interrupted upgrade and retry.
- [ ] Profile Badger and first-party encoding under the same isolated Northwind
  workload; keep compression enabled and document offline WAL inspection.
- [ ] Resolve the prepared-authority test failure and the WAL ownership guard
  timeout without weakening either test or its assertions.
- [ ] Re-run the plugin tests on frozen, integrated source after the observed
  package-version mismatch from concurrent compilation.

The earlier frozen `make test` run failed and is retained as diagnostic
evidence, not a pass. It exposed missing fixture ownership, stale source and
storage censuses, current-format WAL fixture gaps, and a 30-minute storage
ownership-analysis timeout; that snapshot also lacked ignored UI build inputs.
The focused recycled-ID tests now cover both orders of winner/adopter
commit and rejection, and pass under the race detector. The public historical
WAL fixture exposed a storage-version change before rejection; a fail-first
regression now verifies all persisted Badger entries and WAL bytes are
unchanged after a rejected open. Final integrated suite and performance runs
remain outstanding.

The V4 migration now converts supported historical WAL framing before replay,
preserves the original files in a durable archive, and keeps historical payload
bytes and existing Badger keys intact. Focused checks cover interrupted
publication and retry, original headerless checksums, property-epoch bounds,
corrupt records, and torn active tails. A public-open regression exposed an
empty-store authorization issue and a collision between logical receipts and
the old deindex queue; both now pass focused direct and buffered recovery
tests. Final staged checker and full `make test` evidence are still pending.

The checker changes were reviewed with the maintainer: nonblocking
classifications for related code were removed. The remaining narrow wrapper
detection only adds blocking findings; tests also verify that related semantic
matches continue to be reported.

The conservative checker on staged tree `3e2aef2a80807e70f21d8c8ac6c17fe6379876a4`
reported 11 findings, down from 28 on the prior staged tree. This was not a
pass; the integrated source subsequently changed and requires a fresh run.
Focused MVCC adjacency checks and the consolidated search consumer tests passed.
The formerly skipped unsuppress/delete case now waits for both writes and checks
the resulting storage and search state; the observed run did not force an
out-of-order interleaving. A fail-first malformed logical-receipt regression was
fixed and passed with the race detector, including preservation of the raw key.

The BM25 V1 implementation, selector, and V1 snapshot conversion have been
removed at the maintainer's direction. The current full-text format remains
the single index owner; vector and HNSW conversion paths are retained. Focused
full-text, configuration, persisted-generation, and GraphQL tests passed,
including race runs for the affected search and database paths. An initial
full-text test run exposed obsolete V1 prefix and snapshot assumptions; the
current implementation's explicit prefix setting and snapshot shape now have
passing coverage. A storage-census run encountered WAL source edits in flight
and failed to type-check; the subsequent census regeneration passed on the
integrated source. Its mutation checks remain part of the final full suite.

Subsequent focused WAL payload, V4 migration, snapshot-management and search
transaction-refresh checks passed under the race detector. WAL payload codec
benchmarks retained the previous allocation counts on 32, 4,096 and 65,536-byte
fixtures; the measured timing differences were small. Search refresh A/B/B/A
samples also retained the same allocation counts. These focused results do not
substitute for the final full suite or duplicate check.

A later paired Northwind seed comparison used the same 2,000-row batches, four
sessions, and verified 97,440 nodes and 312,050 relationships. The current tree
seeded in 7,079 and 8,049 ms with GRAPH_ONLY=1, versus 8,026 and 7,227 ms for
the preserved earlier binary; with GRAPH_ONLY=0 it took 10,619 and 10,898 ms,
versus 10,605 and 9,778 ms. The local Qwen service was loaded but idle during
those samples. Two later current-tree samples with Qwen stopped and no Docker
containers running took 7,664 and 7,206 ms with GRAPH_ONLY=1, and 9,975 and
11,376 ms with GRAPH_ONLY=0, again with exact graph counts. These are seed-only
measurements; the approximately five-second target, read-query suite, and
larger-payload profiling remain open.

The checker on staged tree `270b2341200f51809ffc962f24cca61b028c12e1`
completed without candidate-cap skips and reported 83 reuse candidates and two
refactor targets. All 85 are recorded in [checker-followup.md](checker-followup.md).
The maintainer authorized pushing a fully tested checkpoint before resolving
that inventory; the checker has not passed and later source changes are not
covered by that report.

The uncached `make test` on that tree failed five ownership/inventory checks.
The storage census completed successfully. Focused checks now pass for the
fixture, search-lifetime, serving-state, security-inventory, and storage-mutator
corrections. A fail-first cycle test also exposed and corrected an order-
dependent reachability error in the mutator guard. The complete suite must pass
again on the resulting tree before the checkpoint is pushed.

Focused public admin-config tests reproduced credential-bearing invalid values
being echoed to the authenticated submitting administrator. Both normalization
and unregistered-stemmer diagnostics now use the existing text redactor; the
focused race tests pass. Cross-user disclosure was not established.

The uncached `GOFLAGS=-count=1 make test` subsequently passed 127 packages on
tree `c91566569c23c7a08ee34674595c2c645a2c9c98`, committed and pushed as
`7de84c4c20260311e68e69e2340cc8964a18fe04` to the draft PR. The checker
inventory above remains open; it does not cover that exact tree. `make verify`
was deferred by the maintainer.

Post-checkpoint work is in progress. A real replication WAL regression test
reproduced a reader silently skipping an unreadable or missing command; the
local correction fails the requested window instead and passes focused and race
checks. The vector strategy and filter test pairs were consolidated; the
continuation and counting fixture forwarding helpers were removed. Their
retained scenarios pass focused race checks. These changes have not yet passed
the integrated gates or a fresh duplicate check.

A larger isolated Northwind profile used 96,000 products, 96,000 orders and
4,000-row batches. Both modes preserved 193,440 nodes and 624,475 relationships.
The pushed baseline took 16.8 seconds graph-only and 21.6 seconds with search;
peak server RSS was approximately 10.2 and 12.0 GiB, respectively. These are
single profiled samples, not default-workload results. Lock-request reuse is
under evaluation with paired runs; its memory and final integrated correctness
remain open. MessagePack conversion of semantic WAL payloads and migration of
existing WALs also remain open.

A representation-only 2,000-relationship benchmark compared the current gob
path (median 4.96 ms, 48,062 allocs, 551,007 wire bytes), nested MessagePack
(1.69 ms, 14,024 allocs, 544,753 bytes), and direct typed operations (1.49 ms,
8,002 allocs, 799,825 bytes). This does not prove end-to-end WAL performance.
A differential probe showed the direct typed path changes integer and typed-list
representations relative to the existing semantic transaction decoder. It must
not be adopted until the existing property contract is preserved and tested.
The fail-first V4 WAL test also confirms that the pushed migration does not yet
transcode the old semantic envelope and nested gob payload.

The local follow-up now uses a MessagePack semantic transaction and converts
historical semantic records during the V4 WAL upgrade. Focused race tests cover
the file conversion, current records, corrupt historical records, already-V4
startup, interrupted retry, and the authorized replication-WAL startup path.
The V4 store's existing keys and payloads are not rewritten for a WAL-only
conversion; a separate temporary recovery marker makes retry durable. The
replication direct-command and snapshot formats now use MessagePack as well.
The V4 conversion covers the historical semantic, direct, bulk and prefix
commands; an explicit converter handles caller-owned replication snapshots.
Focused race checks cover the migration, typed properties and public adapter
restore. The integrated suite and current-tree duplicate check remain open.

Quota sizing now measures canonical logical entities with MessagePack. On a
small-node codec benchmark the median was 5.11 µs, 4,410 B and 39 allocations
with gob versus 0.68 µs, 864 B and 9 allocations with MessagePack. This changes
reported logical byte usage and is documented for operators. The rebuildable
link-prediction graph cache uses MessagePack; a 1,000-node, eight-neighbor cache
read benchmark measured 494 µs, 727,668 B and 13,205 allocations before versus
387 µs, 449,107 B and 11,020 allocations after direct graph decoding. These are
scoped microbenchmarks, not Northwind ingest results. Focused tests pass; neither
change has received the full-suite or checker gate on the integrated tree.

For the production semantic transaction encoder, five isolated samples on an
Apple M2 Max with `GOMAXPROCS=2` and 2,000 Northwind-shaped relationships had
median 4.92 ms, 8,791,225 B and 48,060 allocations for the historical gob
representation versus 1.27 ms, 2,509,249 B and 24 allocations for MessagePack.
The encoded recovery payloads were 561,022 and 536,753 bytes respectively.
This measures encoding only, not disk, replay, replication, or total ingest.
CPU profiles were recorded for both runs. The default Northwind ingest target
of five seconds remains unmet.

Replication rejects malformed and excessively nested property messages before
publishing them. A leader's unaccepted invalid write is rejected before the
log; an already accepted transaction that cannot apply locally remains recorded
for recovery. Focused race checks cover both paths. Relationship-search vector
files still use gob and need a separate lossless conversion; they are not
rebuildable without an available embedder.

The first uncached integrated `make test` on staged tree
`07322d8336459c577298043ba24483318a03e22e` failed: three database test files
still referenced the retired gob command types; the quota matrix measured gob
instead of MessagePack; two malformed-property tests lost their specific error
messages; and the lexical and stored-entity census inventories needed review.
The test consumers now use the current codec (with an explicit historical gob
fixture only for migration), the quota matrix measures the current quota
format, and bounded property-map preallocation preserves the original decoder
diagnostics. The new WAL recovery marker and semantic wire field have explicit
census classifications, including a negative test for unreviewed keys. Focused
replication/search, quota-matrix, malformed-property, lexical-inventory and
fixture-inventory tests passed. Full-suite verification and the duplicate
checker on the corrected integrated tree remain pending.
