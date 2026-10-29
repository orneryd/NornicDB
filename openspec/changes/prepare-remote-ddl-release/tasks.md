# PR #1525 / NOR-7 readiness

Target: updated release/v1.4.4 after merged #1541, baseline a9265090a5d7bd8e34932cf8f504765c01aa147e.
Owner: Tim Sweet; Urgent ticket reassigned from iBurn/mm0nst3r. No PR size limit.

- [x] Verify highest-priority oldest eligible open PR and assign NOR-7 to Tim.
- [x] Isolate original 34-file own layer (5637d0ad5 over dad30a7a1), excluding obsolete parent stack.
- [x] Read-only DeepSeek AC/security inventory; root independently confirms concrete native/ANTLR divergence. Static review is not runtime qualification.
- [x] Reingest immutable current release in background and independently verify its repository marker before final duplicate analysis.
- [x] Preserve fail-first evidence for supported remote CREATE/ALTER/DROP parity, exact quoted catalog identities, complete input rejection and credential-safe diagnostics through public protocols.
- [x] Port required behavior into current scannerless recursive-descent and grammar/command/credential owners; preserve current Go APIs, typed source stages and approved storage contract. Regenerate ANTLR output with the pinned generator; never patch the ANTLR runtime.
- [x] Verify the complete integrated public-path matrix, EXPLAIN metadata/effects, malformed/trailing input, quoted names and credential role boundaries, with original failing evidence retained.
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

The corrected MessagePack checkpoint was committed and pushed as
`35ed741ebb0c213e8f1a3278c495a784d6d631e1` after an uncached
`GOFLAGS=-count=1 make test` passed 127 packages on tree
`d0405685eedbba78b1c186273e25711854fa9763`. The unchanged checker reported
seven reuse candidates on that staged delta; they are inventoried in
[checker-msgpack-followup.md](checker-msgpack-followup.md). Its graph was at
release `a9265090a5d7bd8e34932cf8f504765c01aa147e`, not the comparison head,
and the SIMD header was partially extracted. The maintainer authorized this
checkpoint with findings inventoried; neither report establishes a checker pass.

Four later isolated seed-only samples on the pushed commit, with local Qwen
stopped and no Docker containers running, preserved all expected graph counts.
At 2,000-row batches the 48,000-product/48,000-order payload took 6.891 seconds
and 5.70 GiB peak RSS graph-only, or 9.278 seconds and 7.40 GiB with search.
At 4,000-row batches the 96,000-product/96,000-order payload took 15.463 seconds
and 10.80 GiB, or 20.145 seconds and 12.68 GiB with search. These are single
samples, with the larger runs profiled, not a statistically established gain;
the five-second target remains open.

Follow-up changes consolidate several checker candidates and retain focused
race coverage. Historical WAL and snapshot conversion now reject trailing zero
bytes that the previous EOF check accepted, while stored-body gob decoding
keeps its existing permissive framing. The shared reader also preserves the
stream position on its permissive path. These changes still require an
integrated suite. The checker passed the subsequent 31-function staged delta
on tree `8c1ab3d5865e39b105fe16e9d6b0a4c9c8d01118`; it did not reanalyze the
unchanged findings in the earlier inventories, and the graph/header extraction
limitations remain.

After the `366387bd2e233c1554acec1cd7b5d9f95b672b2e` checkpoint, a local
property-encoder experiment reuses a separate MessagePack value buffer. It
resets the encoder between properties so custom encoders cannot change the
following value's wire format. Focused byte-comparison and race tests cover
supported scalar, collection and temporal shapes; a deliberately missing reset
makes the state-isolation test fail. A public storage regression also showed
that direct writes into the containing body let a custom typed-property encoder
reset and corrupt it; that direct-write experiment was rejected. The separate
buffer preserves the prior value-writer contract and Badger body format.

For the rejected direct-write experiment, on an Apple M2 Max with
`GOMAXPROCS=4`, local Qwen stopped, no Docker containers, and five one-second
samples per case, median Northwind-shaped
order-line encoding changed from 243.6 ns, 288 B and 5 allocations per op to
180.0 ns, 112 B and 2 allocations; product encoding changed from 1,095 ns,
1,478 B and 20 allocations to 700.2 ns, 502 B and 4 allocations. CPU profiles
were captured. These results do not apply to the corrected separate-buffer
implementation and are not end-to-end results.

The rejected experiment's two isolated 48,000-product/48,000-order seeds per side used 2,000-row batches
and four sessions with exact 97,440-node/312,050-relationship counts. Graph-only
samples were 6.804 and 8.209 seconds before versus 6.573 and 7.696 after;
search-enabled samples were 10.628 and 9.376 seconds before versus 10.495 and
9.352 after. The sample count and spread do not establish an end-to-end gain.
The corrected separate-buffer version was then measured in the same isolated
five-sample microbenchmark: order-line properties took a median 196.4 ns,
176 B and 3 allocations, versus the same 243.6 ns, 288 B and 5-allocation
baseline; product properties took 768.7 ns, 694 B and 6 allocations, versus
1,095 ns, 1,478 B and 20 allocations. Its two alternating default Northwind
samples per side were 7.883/6.997 seconds before and 7.619/6.697 after in
graph-only mode, and 9.445/9.282 before and 9.074/10.208 after with search.
All eight counts matched; the spread does not establish an end-to-end gain.
The corrected experiment is included in this checkpoint; the approximately
five-second ingest target remains open.

A scratch-only Badger v4.9.6 experiment computed log-entry CRC over the bytes
already appended to the buffer. A focused byte-oracle/replay probe passed for
plain and encrypted entries, prefixed buffers, varied offsets and invalid
keys; a checksum mutant failed that probe. On the same isolated M2 Max, five
one-second samples gave median plaintext encoding of 114.3 ns, 96 B and four
allocations before versus 42.4 ns, 0 B and no allocations in the experiment;
AES-256 medians were 482.0 ns, 1,632 B and 12 allocations versus 420.9 ns,
1,536 B and eight allocations. Two alternating default Northwind seed samples
per side were 6.797/6.756 seconds before and 6.646/6.612 with the experiment
graph-only, and 9.388/10.318 before versus 9.088/10.075 with search. Counts
matched in every run. The whole-ingest samples do not establish a clear gain;
the patched dependency has not been adopted or fully qualified.

In a further single-sample, profiled 96,000-product/96,000-order comparison
with 4,000-row batches, graph-only seed time was 15.087 seconds and peak RSS
10.79 GiB before versus 14.643 seconds and 10.42 GiB with the scratch patch;
with search it was 20.436 seconds and 12.82 GiB versus 19.924 seconds and
12.25 GiB. Each run preserved 193,440 nodes and 624,475 relationships. The
allocation profile attributed about 13.9 million sampled graph-only objects
to the old Badger encoder and no samples to the patched encoder. These single
profiled samples establish the observed attribution, not a reliable overall
ingest speedup; the dependency experiment remains outside the PR.

An uncached `GOFLAGS=-count=1 make test` passed 128 packages on staged tree
`e033f6deb8c2478280a0b183f1eb1b2b7ddf8ddf`. After that tree was frozen, a
direct semantic-transaction codec regression was reproduced: a custom typed
property encoder could reset the enclosing MessagePack buffer and corrupt the
transaction. The public write-and-WAL-recovery control passed before this fix
because commit preparation normalized that particular value; recovery loss was
not reproduced. The correction isolates custom property encoders, including
registered extensions and nested values, validates their output and preserves
the existing property-depth boundary. Focused tests and race checks passed.
The full-suite pass above predates this codec correction.

For the codec correction, three alternating one-second samples on the M2 Max
with `GOMAXPROCS=4`, local Qwen stopped and no Docker containers gave median
2,000-relationship transaction encoding of 1,199,980 ns and 19 allocations
before versus 1,193,998 ns and 19 allocations after; both used about 2.43 MB
per operation. CPU profiles were captured. This is a focused codec measurement,
not a claim about whole-ingest performance.

A subsequent public Badger node probe found the same writer-ownership problem
inside a property list: on the pre-fix staged implementation a list containing
17 and a custom value of 42 read back as the scalar 42. The shared property
writer now isolates custom leaves in supported nested lists and maps for both
stored properties and semantic transactions, without changing Badger keys or
the property layout. Focused node, relationship, malformed-value, depth and WAL
recovery tests passed under the race detector. The full run on staged tree
`fb2ea7068b86d378320b0f6e976d26bd712af0dc` was canceled as superseded by this
additional correction; its partial log is not a suite pass.

Three alternating one-second samples of the shared-writer correction on the
isolated M2 Max gave median order-line property encoding of 200.6 ns before
and 201.3 ns after (176 B and 3 allocations each), product properties of
803.3 ns before and 811.2 ns after (about 694 B and 6 allocations each), and
2,000-relationship transaction encoding of 1,219,119 ns before and
1,232,633 ns after (about 2.43 MB and 19 allocations each). CPU profiles were
captured. These focused results do not establish whole-ingest performance.

The uncached full suite on staged tree `44ee1521773219a4bf9775af8b0ec274f77921c2`
failed `TestStoredEntityCensusIsCurrent`; the other 127 packages passed.
Regenerating the census changed only four source-line positions in each of its
JSON and Markdown artifacts; the number of recorded field writes stayed 331,
and `TestStoredEntityWritesAreOrdered` passed with zero census violations.
The guards themselves were not changed.

The corrected tree `565b0794eaab447122066dc5d468c65ba2a80a6c` passed an
uncached `GOFLAGS=-count=1 make test` across 128 packages and was pushed as
`f083c7ae6688092cd524bce0248cf47a48fdfbe2`. The duplicate checker timed out
connecting to the graph before analyzing that tree; the two historical
inventories remain open.

The large graph-only profile after that checkpoint showed substantial memory
work and schema-token registration contention. A follow-up registers a
homogeneous relationship batch's type once, while mixed batches retain their
first-seen token order. Focused race checks cover the schema owner and both
namespaced and transaction bulk writes, including invalid batches and stored
edge IDs, endpoints and types.

On an Apple M2 Max with `GOMAXPROCS=4`, Qwen stopped and no Docker containers,
four interleaved 250 ms samples per side of token registration gave the
following median times and allocations:

| Batch size / types | Before ns/op | After ns/op | Before B/op / allocs | After B/op / allocs |
| --- | ---: | ---: | ---: | ---: |
| 1 / 1 | 14.8 | 14.6 | 0 / 0 | 0 / 0 |
| 100 / 1 | 1,235.5 | 166.9 | 1,792 / 1 | 0 / 0 |
| 100 / 8 | 1,467.5 | 1,489.5 | 1,792 / 1 | 1,792 / 1 |
| 100 / 100 | 1,856.5 | 1,761.5 | 1,792 / 1 | 1,792 / 1 |
| 4,000 / 1 | 47,297.5 | 5,987.5 | 65,536 / 1 | 0 / 0 |
| 4,000 / 8 | 55,597.5 | 54,640.5 | 65,536 / 1 | 65,536 / 1 |
| 4,000 / 100 | 69,113 | 68,630.5 | 65,536 / 1 | 65,536 / 1 |

The 48,000-product/48,000-order Northwind seed used 2,000-row batches and
four sessions. Two alternating samples per side gave graph-only ingest times
of 6.851/8.080 seconds before and 6.827/7.990 after; with search enabled they
were 10.592/9.309 before and 9.157/10.545 after. Every run contained 97,440
nodes and 312,050 relationships. The sample spread does not establish a
whole-ingest gain, and the approximately five-second target remains open.

One profiled sample per side of the doubled workload used 96,000 products,
96,000 orders and 4,000-row batches. Graph-only ingest was 15.362 seconds and
10,389 MiB peak RSS before versus 15.251 seconds and 10,129 MiB after; with
search it was 21.137 seconds and 12,867 MiB before versus 20.222 seconds and
12,435 MiB after. All four runs contained 193,440 nodes and 624,475
relationships. These single profiled samples are diagnostic, not a reliable
performance claim. CPU, allocation and lock profiles are retained locally.
The follow-up still requires a fresh integrated suite and checker analysis.

The first uncached full suite for this follow-up, on tree
`03353d0853352671ee71c12d7f57ccf89af8a53b`, failed the lexical population
and fixture-ownership guards; the remaining packages passed. Discovery found
one new lexical candidate: `SchemaManager.RegisterEdgeTokens`, whose tokens
are graph relationship type names. The reviewed population now records it,
and the new test's memory engine is registered with the existing fixture
owner. Both previously failing guards passed focused reruns; neither guard or
its assertions was changed. A fresh full suite is required on the corrected
tree.

## Relationship snapshot migration follow-up

- [x] Replace the relationship-search snapshot's gob writer with a packed
  MessagePack writer and upgrade existing active snapshots in the authorized V4
  startup flow, including already-V4 stores. Preserve the old Badger node body
  in the tested upgrade and do not introduce another storage-version bump.
- [x] Reject malformed, truncated, unknown and unauthorized snapshot input
  without overwriting the source. A failed post-rename directory sync is retried
  before an already-converted file is accepted. Direct search services report
  an unconverted or unreadable file instead of rebuilding over it.
- [x] Replace predictable snapshot temporary paths with exclusive temporary
  files. The fail-first test reproduced a relative-symlink overwrite of a sibling
  file; the corrected writer preserves that sibling and the migration source.
- [x] Run focused race tests for search conversion, persistence and restart, and
  for both an existing V4 store and the genuine historical-WAL V4 fixture. The
  tests retain exact float bits, source hashes and chunk spans, verify no
  re-embedding after conversion, and compare a Badger node body before and
  after the search upgrade. The lexical inventory discovered one new generation
  token reader; it was reviewed as search-state data and the unchanged lexical
  and fixture-ownership guards passed.
- [ ] Run the complete uncached `make test` and attempt the duplicate checker on
  the integrated staged tree. If the test suite passes but the checker cannot
  pass, inventory its findings for follow-up and push the tested checkpoint.
- [ ] Continue the remaining acceptance and Northwind ingest performance work.

On an isolated Apple M2 Max with `GOMAXPROCS=4`, local Qwen stopped and no
Docker containers, six interleaved two-iteration samples per side measured the
same snapshot encoding and file-sync fixture. For 128 vectors of 384 dimensions,
gob's median was 5,054,979.5 ns/op, 1,541,172 B/op and 176 allocs/op; packed
MessagePack was 4,873,812 ns/op, 70,056 B/op and 8 allocs/op. For 2,048 vectors,
gob was 10,835,708 ns/op, 26,541,108 B/op and 2,108.5 allocs/op; packed
MessagePack was 5,748,146 ns/op, 70,056 B/op and 8 allocs/op. These measurements
cover snapshot encoding and file sync, not complete service startup or
Northwind ingest. CPU profiles and the individual samples are retained locally.

## Final AC and merge direction — 2026-10-29

The maintainer is holding performance tuning at the current results so NOR-7's
acceptance criteria can be completed first. The approximately five-second
Northwind and 0.13 ms query targets carry forward to subsequent work; they are
not achieved by this checkpoint. The completed isolated graph-only run on tree
`3c18a75f7909f25cbb05b4bf801804b3b92ada62` ingested in 6.662 seconds and averaged
0.176 ms over 14 queries with 30 measured iterations each; graph counts and
query fingerprints passed. The search-enabled query run was canceled when
performance work was held; its partial results are not a suite result.

The uncached full `make test` passed 128 packages on that tree. A subsequent
fail-first startup test showed that a directory-sync ENOENT could be suppressed
as an absent snapshot. Only the initial source-open absence is now optional;
post-open errors propagate through startup. Focused race checks passed, but the
corrected final tree requires another complete suite and full PR-delta duplicate
analysis before push and merge. The maintainer authorized merging after the AC
and both gates pass, then pulling and reingesting the merged release head.


## Integrated readiness checkpoint — 2026-10-29, current convergence wave

- [x] Re-run the public administration/credential/remote-DDL/EXPLAIN/statistics matrix after the statistics correction (`public-ac-integrated-wave2.log`, uncached race run, 107.281s). Ordinary collected query text is preserved; administrative/system history is excluded. This does not claim parameter collection parity or full conformance.
- [x] Compare exact GH530 with Neo4j 5.26.30: both parsers × HTTP/Bolt × auto/explicit, eight comparisons, no skips (`gh530-reference-receipt.json`).
- [x] Re-run the original three relationship TCK regressions in auto/explicit modes: six focused scenario passes (`original-three-tck-regressions.log`). Broader conformance remains unexecuted in this wave.
- [x] Complete the unchanged full-delta checker against release graph a9265090: frozen tree 8f124cf7 analyzed 1,611 functions, 22 findings, no cap skips or graph-baseline warnings; down from 71 at 7edf6dee. The evidence runner's console-summary serialization failed after analysis, but the complete report is preserved. This is a findings report, not a gate pass. Unchanged SIMD header extraction limitation persists.
- [x] Preserve the original index-only, unnamespaced 6,000-node IVF fixture and the stored/namespaced 303-vector exact-stage fixture when sharing construction/indexing. No fixture population is substituted for another.
- [x] Obtain maintainer approval for typed entity-preparation/publication migration. The actual request carries the existing Operations slice directly, avoiding a new staging callback or copy. No compatibility forwarding methods remain.
- [ ] Finish and independently review the typed transaction, transport-image and configuration binding consolidations; refresh source populations and stored-entity census without weakening guards.
- [ ] Run the unchanged checker on the final frozen tree and resolve every remaining finding.
- [ ] Run uncached make test on that same integrated source; correct every failure, including release-baseline regressions.
- [ ] Commit/push exact verified source, post evidence on PR #1525, update NOR-7 and mark ready for review.

Performance tuning remains held at the maintainer's latest direction. The historical approximately-five-second ingest target is future work, not a completed claim. `make verify` remains explicitly deferred to the next PR; no full-sweep pass is claimed. No checker/hook/rule edits or Dot notifications are authorized. New transport consolidation must preserve the current wire bytes and all existing Badger layouts.

## Integrated transaction convergence follow-up

- Typed `EntityCommitRequest`, `CommitPublication` and prepared encoding requests replace the overlapping preparation/publication/encoding routes; callers and Go documentation migrated. Receipt observation constructs its key once, enrolls the native conflict read, and preserves acknowledged-generation visibility through physical fallback. No Badger key/payload or storage-version change.
- Preserved fail-first proof for a detached-client publication handoff, public diagnostic before-image aliasing, physical-only receipt observation, and diagnostic typed-list aliasing. Focused race checks pass for the corrected ownership boundaries and commit/discard outcomes. The private prepared encoder retains 13 allocations for scalar and typed-list fixtures; the diagnostic copy alone allocates independent list containers. This is focused allocation evidence, not renewed Northwind timing qualification.
- The uncached complete `make test` on tree `303d4268b438ab2ff595f74ec8edfdcf6d22eecc` finished with two source-guard failures (lexical inventory and relocated receipt metadata lookup); all other tested packages passed. Current source corrects the reviewed inventory and restores generation lookup to the buffer receiver. Both unchanged guards, diagnostic-container tests and receipt visibility tests pass focused checks; the complete suite must pass again before publication.
- The unchanged full release-delta checker on that same tree analyzed 1,672 functions with five reuse findings, zero refactor findings and zero cap skips. The five concern consolidated owner calls and the shared unit/benchmark setup/execution owners. A concrete ownership-proof proposal is awaiting explicit maintainer authorization; no checker/hook/rule change or finding waiver has been applied. Later diagnostic-copy changes require another complete current-tree check. The existing SIMD header extraction warning remains reported.
- `make verify`, broad performance tuning, commit/push and final readiness remain pending or explicitly deferred as described above. No full-suite or checker pass is claimed by this checkpoint.
