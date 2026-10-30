# Local Graphify convergence candidates

This is a reproducible structural candidate list, not a replacement for
`DIVERGENCE_REPORT.md`. That report analyzed eight separately extracted graphs
at `994b3a68` with a generator absent from this checkout; the local graph was
extracted from the full repository. Its counts are not directly comparable.

Input: `graphify-out/graph.json` (ignored), Graphify 0.9.69, SHA-256
`54fa1bd04cf13751c7dd50ef469fdc57cbe299813e4ac3605a5cfd80898c4eb2`.
Regenerate after each graph refresh; this hash identifies only the measured
snapshot, not a checked-in baseline. With the isolated Graphify environment:

```sh
"$(uv tool dir)/graphifyy/bin/python" scripts/graphify_divergence.py \
  --component pkg/cypher --component pkg/storage --component pkg/search \
  --component pkg/nornicdb --component pkg/server --component pkg/bolt \
  --component pkg/multidb --component pkg/embed
```

| Component | Non-test Go graph nodes | Outgoing call edges |
| --- | ---: | ---: |
| pkg/bolt | 369 | 420 |
| pkg/cypher | 3,914 | 7,700 |
| pkg/embed | 167 | 99 |
| pkg/multidb | 201 | 193 |
| pkg/nornicdb | 398 | 580 |
| pkg/search | 1,044 | 1,236 |
| pkg/server | 424 | 674 |
| pkg/storage | 2,535 | 3,205 |

## Historical comparison

The historical report used eight separately extracted component graphs at
`994b3a68`; this snapshot filters one full-repository graph. The raw counts
below are useful for auditing inputs, **not** for calculating added/removed
symbols or changes in divergence.

| Component | Historical nodes | Current nodes | Historical calls | Current calls |
| --- | ---: | ---: | ---: | ---: |
| pkg/bolt | 386 | 369 | 278 | 420 |
| pkg/cypher | 2,993 | 3,914 | 3,970 | 7,700 |
| pkg/embed | 166 | 167 | 91 | 99 |
| pkg/multidb | 206 | 201 | 103 | 193 |
| pkg/nornicdb | 454 | 398 | 357 | 580 |
| pkg/search | 1,092 | 1,044 | 986 | 1,236 |
| pkg/server | 473 | 424 | 323 | 674 |
| pkg/storage | 2,266 | 2,535 | 2,562 | 3,205 |

### Same-Method Revision Comparison

To compare candidate group keys with the *same* Graphify 0.9.69 extractor and
`scripts/graphify_divergence.py` filter, extract clean source worktrees at
`994b3a685d291b89c1276f20fe5e1f5e72687b6b` and
`23d7666f027e913a7025b1ae58d367f07c62fda0`. These exclude uncommitted
changes and are distinct from the ignored local graph above. From this repo:

```sh
git worktree add --detach /tmp/nornicdb-521-old-994b3a68 994b3a685d291b89c1276f20fe5e1f5e72687b6b
git worktree add --detach /tmp/nornicdb-521-current-head 23d7666f027e913a7025b1ae58d367f07c62fda0
"$(uv tool dir --bin)/graphify" extract /tmp/nornicdb-521-old-994b3a68 --out /tmp/nornicdb-521-old-graphify --code-only --no-cluster
"$(uv tool dir --bin)/graphify" extract /tmp/nornicdb-521-current-head --out /tmp/nornicdb-521-current-graphify --code-only --no-cluster
"$(uv tool dir)/graphifyy/bin/python" scripts/graphify_divergence.py \
  --graph /tmp/nornicdb-521-current-graphify/graphify-out/graph.json \
  --compare-graph /tmp/nornicdb-521-old-graphify/graphify-out/graph.json \
  --component pkg/cypher --component pkg/storage --component pkg/search \
  --component pkg/nornicdb --component pkg/server --component pkg/bolt \
  --component pkg/multidb --component pkg/embed
```

| Clean graph | SHA-256 |
| --- | --- |
| Historical | `f992ae498a4ee6cdb7a2662221ac39c69de4572eb51cd0711f44a255e0e23085` |
| Current HEAD | `701ba335db2b7039ef349db45b0643435060509a7784bb88928307c697e35b71` |

| Component | Historical nodes | Current nodes | Historical calls | Current calls |
| --- | ---: | ---: | ---: | ---: |
| pkg/bolt | 351 | 369 | 370 | 420 |
| pkg/cypher | 2,699 | 3,914 | 5,072 | 7,701 |
| pkg/embed | 167 | 167 | 98 | 99 |
| pkg/multidb | 196 | 201 | 190 | 193 |
| pkg/nornicdb | 390 | 398 | 564 | 580 |
| pkg/search | 1,024 | 1,044 | 1,219 | 1,236 |
| pkg/server | 397 | 424 | 636 | 674 |
| pkg/storage | 2,166 | 2,535 | 2,895 | 3,205 |

The identical filter reports 42 historical groups, 38 current groups and 33
retained component/label keys. Historical-only keys: `pkg/search|.candidateSearchGraphGroup()`;
`pkg/storage|.CreateEdge()`, `.GetNode()`, `.GetNodeWithoutEmbeddings()`,
`.RefreshPendingEmbeddingsIndex()`, `.StreamNodesByPrefixProjected()`,
`.StreamNodesByPrefixWithoutEmbeddings()`, `.StreamNodesWithoutEmbeddings()`,
`.checkUniqueConstraint()`. Current-only keys: `pkg/search|.Rerank()`;
`pkg/storage|.BulkDeleteEdges()`, `.GetNodeProjected()`,
`.StreamNodesByLabelProjected()`, `.StreamNodesWithOptions()`.
The key comparison does not prove whether a method was removed, renamed,
converged or replaced; shared-callee candidates require source review and
contract tests. Graphify reported the same 18 partially parsed native-library
files outside these Go components on both runs. This comparison does not
recreate the old report's source-similarity ranking.

The old analyzer also ranked source-similarity candidates and partially
forwarded wrapper capabilities. The current analyzer reports only cross-file,
same-label symbols with shared call targets: neither an unmatched old item nor
a new group is evidence that the implementation changed. The old component
graphs and generator are absent from the pinned commit (and no analyzer path
is present in tracked history), so their candidate IDs cannot be
replayed against this snapshot. The old router and wrapper claims have separate
current-source contract evidence in OpenSpec sections 4.1 and 3.3; source-
similarity candidates still need an independently reproducible comparison.

The analyzer found 38 same-label, cross-file symbol groups with at least one
pair sharing a `calls` target. Its JSON lists the exact files, locations, IDs
and shared callee IDs for every pair in stable order. These are review leads:
shared conversions, interface implementations and calls into an already-shared
kernel do not imply divergent behavior. For example, the three search
`.CandidateSearchGraph()` implementations call the shared GPU build kernel.
The scan took 2.30 seconds with 31.4 MB maximum resident memory on an Apple
M2 Max (`/usr/bin/time -l`); no earlier analyzer baseline exists for an A/B
comparison.

The old report's router, wrapper and source-similarity claims still require a
separate current-source comparison and disposition; this graph-only filter
cannot establish semantic equivalence or measure runtime performance.

## Reviewed historical-only candidate

`pkg/storage|.StreamNodesByPrefixProjected()` is a retained public
compatibility entry point, not a second scan kernel. Badger, WAL, Async and
Namespaced forward a non-nil projection to their `StreamNodesWithOptions`
kernel; nil properties forward to `StreamNodesByPrefix` to preserve full-node
semantics. `TestStreamNodesOptionsParityAcrossStacks` now compares projected
IDs and selected properties from the legacy and options routes on every stack
that exposes the capability, including pending Async and namespace translation.
The broader options parity battery also covers Memory and Composite. No
production consolidation is needed for this group. The parity test passes
three times under `-race`. M2 Max `BenchmarkStreamNodes_ByPrefixProjected`
(`-benchtime=300ms -cpu=1 -count=3`) measured 1.193–1.208 ms/op,
1,053,605–1,053,606 B/op and 13,767 allocs/op; this candidate changed only
tests and documentation, so no runtime improvement is claimed.

`pkg/storage|.checkUniqueConstraint()` is a historical-only group. PR #747
removed Async's partial checker; constrained writes now pass through the
storage engine before acknowledgement. SchemaManager owns registered values,
while BadgerTransaction checks transaction-local writes. Its pending scan used
raw interface equality, so a pending `age: int(1)` followed by `age: float64(1)`
was incorrectly accepted. It now uses `compareValues`, matching the indexed
and persisted validation paths and avoiding a panic for non-comparable values.
`TestBadgerTransactionUniqueConstraintPendingMatchesNumericValueSemantics`
reproduces and pins rejection before the duplicate is staged;
`TestBadgerTransactionUniqueConstraintPendingNonComparableValueDoesNotPanic`
pins safe handling of distinct pending list values. These tests and the
committed-index numeric parity test pass three times under `-race`.

After PR #747, M2 Max `BenchmarkBadgerUniqueConstraintCommit_DisjointFunctionBatches`
(`-benchtime=300ms -cpu=1 -count=3`) measured clean `63349321` at
12.583–13.187 ms/op, 6,676,183–6,676,333 B/op, 135,138 allocs/op;
the fixed worktree measured 12.651–13.356 ms/op,
6,676,131–6,676,182 B/op, 135,137–135,139 allocs/op. Timing bands overlap;
allocation shape is unchanged. A separate profiled run attributes 11.8%
cumulative CPU to `checkUniqueConstraint`, including its pending comparison;
this is not a speedup claim.

`pkg/storage|.RefreshPendingEmbeddingsIndex()` has a distinct native-index
rebuild (Badger), forwarding adapters (WAL/Async), and namespace overlay.
Badger returns newly added entries, not the number removed. Namespaced used
to return removal attempts even for embedded nodes that were never pending,
then re-added and recounted already-indexed nodes after Badger refreshed them.
Direct Badger and WAL-over-Badger now return the underlying complete-refresh
count without a duplicate namespace scan. A capability-only inner retains its
namespace scan and returns the delegated count plus locally queued candidates,
not removal attempts. Two Badger-backed tests cover no-op, repeat no-op and
repair of a missing entry; the existing capability-helper test pins prefix
translation. Async can hold unflushed nodes that the underlying Badger rebuild
cannot see. The native complete-refresh count now also applies through Async
and Async-over-WAL: staged nodes remain visible in Async's cache without
counting phantom pending-index additions. A direct/Async-over-WAL regression
pins repeated no-op refresh before flush and repair of a missing entry after
flush, three times under `-race`. Other Async queue/count behavior remains open.
M2 Max `BenchmarkNamespacedRefreshPendingEmbeddingsIndex` on an already
complete 16-node index (`-benchtime=200ms -cpu=1 -count=3`): direct Badger
66.306–66.883 us/op, 62,896 B/op, 906 allocs/op; Namespaced
66.411–66.694 us/op, 62,896–62,897 B/op, 906 allocs/op. These overlapping
bands show wrapper parity, not a speedup. The profiled run's largest flat CPU
sample was `runtime.madvise` (24.8%); MsgPack decoding and allocation are
also material. A matched staged-node benchmark
(`-benchtime=200ms -cpu=1 -count=3`) measured direct Async at
66.573–67.094 us/op and Namespaced-over-Async
at 66.842–67.097 us/op, both 62,896 B/op and 906 allocs/op. Bands overlap;
the CPU profile is dominated by runtime memory work (`runtime.madvise` 26.8%
flat) rather than the wrapper. No speedup is claimed. Write-side performance
and full qualification remain unmeasured.

The adjacent Async pending-count contract exposed a user-visible gap:
`WaitForEmbeddings` could observe zero while an eligible node was still staged.
`PendingEmbeddingsCount` now adds eligible unflushed creates to the durable
count, holding `flushMu` across both views; explicit requeue remains supported
when automatic indexing is disabled. Direct `FlushWithResult` now shares the
flush boundary so counts cannot observe half-published writes. Tests cover
staged update/embedding/delete/flush transitions, dynamic Badger enable and
label policy, persisted update deduplication and a blocked direct flush.
Async-over-Namespaced now evaluates Badger policy against the ID that its
inner namespace wrapper persists; excluded and allowed nodes are both pinned,
including no phantom index key when requeueing an eligible staged create.
Storage passes the full `-race` suite. M2 Max 16-staged-node count benchmark
(`-benchtime=250ms -cpu=1 -count=5`): previous delegated Badger scan
496.6–509.1 ns/op vs Async count 951.6–978.8 ns/op, both 408 B/op and
9 allocs/op. The added ~0.45 us is a recorded correctness cost; CPU profiling
shows runtime memory work and Badger iteration dominate. A profiled empty
public flush measured 51.9–52.6 ns/op with unconditional locking vs
39.0–40.1 ns/op for its internal body (zero allocations). Reusing the existing
no-work guard measured 30.3–30.6 ns/op (zero allocations), below that body.
The matched prefix-aware 16-node count was 1014–1023 ns/op, 408 B/op and
9 allocs/op; the policy correction does not change allocation shape.
Other report candidates, remaining queue semantics and final gates remain open.

## Reviewed current-only candidate

The historical-only `pkg/storage|.GetNodeWithoutEmbeddings()` group includes
optional reader adapters, not just the Badger cache/miss kernel. Async and WAL
used to fall back to full `GetNode` results when the underlying `Engine` did
not expose `NodeWithoutEmbeddingsReader`, returning inline and named vectors.
Their fallback now returns a copy without embeddings; the optional-reader
fast path remains unchanged. The Namespaced fallback had the same leak; it now
clears vectors on its user-facing struct copy while preserving `EmbedMeta` and
leaving the inner node untouched. `TestEmbeddingFreeSingleReadFallbackStripsEmbeddings`
covers Async/WAL with a capability-hidden Badger store, including preserved
properties, unchanged source vectors and not-found propagation.
`TestNamespacedEmbeddingFreeReadFallbackStripsEmbeddings` pins Namespaced ID and
property mapping, vector stripping, metadata preservation and source vectors.
These tests and the existing stacked fast-path tests pass three times under
`-race`. All four current production implementations (Badger, Async, WAL and
Namespaced) are dispositioned; their native read, overlay and namespace roles
remain distinct.

M2 Max `BenchmarkEmbeddingFreeSingleReadFallback/namespaced-*`
(`-benchtime=500ms -cpu=1 -count=5`) compared the old full-return path at
585.9–611.5 ns/op with the corrected fallback at 584.2–599.7 ns/op; both use
1,112 B/op and 10 allocs/op, within run-to-run noise. The isolated final CPU
profile attributes 58.5% cumulative time to `copyNode` in the inner full read;
the Namespaced fallback adds no extra metadata-copy cost. Avoiding the vector
read itself requires the inner engine to expose the optional reader.

`pkg/storage|.StreamNodesWithOptions()` pairs the Badger and RemoteEngine
implementations. Their local-transaction and network-backed reads cannot be
merged into one iterator. Source review did find a Cypher-facing cancellation
gap: RemoteEngine used `AllNodes()`, which queried with a fresh context before
checking the stream caller's context. It now passes that context to the shared
node-query decoder; the public `GetNodesByLabel` API retains its default
context. `TestRemoteStreamNodesWithOptionsCancellation` covers both a
pre-cancelled stream (zero transport calls) and cancellation inside the remote
query. The test and the existing local-stack stream parity battery pass under
`-race`. A separate fake-transport contract test verifies prefix scoping,
projection, and early stop with one query and one callback. The stream now
honors the remote `ApplyDecayFilter` option: enabled uses normal MATCH behavior;
disabled returns `reveal(n)` so the server does not suppress decayed nodes.
`reveal` is registered in the canonical function catalog, and the query-scoped
reveal lifecycle now wraps the shared router for both autocommit and explicit
transactions. `TestRemoteStreamNodesWithOptionsDecayFilterMode`,
`TestRevealExpressionBypassesDecayDuringQuery` and the execution-state scope
concurrency test pass three times under `-race`.

Remote `WithEmbeddings` parity remains open. Bolt normalization and the HTTP
transaction API expose node element ID, labels and user properties; they do not
carry NornicDB's separately stored chunk/named embedding vectors, so
`RemoteEngine` cannot populate `Node.ChunkEmbeddings` or `Node.NamedEmbeddings`
from the current node result shape. A vector-capable remote API contract is
needed before claiming this option is implemented.

M2 Max `BenchmarkStatementRouting` (`-cpu=1 -count=5`) after the allocation-free
reveal detector measured autocommit at 17.596–19.622 us/op, 11,215 B/op,
162–163 allocs, and explicit transactions at 11.260–11.748 us/op, 5,594 B/op,
89 allocs. The paired pre-detector run measured 17.795–19.428 us/op,
11,255 B/op, 163–164 allocs and 11.333–11.648 us/op, 5,642 B/op, 90 allocs,
respectively. A CPU-profiled autocommit run measured 17.172 us/op, 11,210 B/op
and 163 allocs; timing varies between runs, so this is not a speedup claim.

`pkg/storage|.GetNodeProjected()` pairs Composite's multi-constituent read
with WAL and Async capability adapters. These responsibilities are distinct:
WAL forwards or projects its underlying read, Async first checks staged
updates/deletes, and Composite searches readable constituents until it finds
the node. The production-wrapper projection test covers persisted, staged
update and staged delete cases; `TestCompositeCapabilityParity_MVCCAndVisibilityReads`
now also proves that a node found in a later constituent returns only requested
properties. Both focused tests pass twice under `-race`. No shared projected
read kernel is warranted by this same-label group; other candidate groups
still require disposition.

`pkg/storage|.BulkDeleteEdges()` pairs Async's pending-delete overlay with
Badger's atomic on-disk batch. Badger rejects mixed-namespace batches before
mutation, but Async had returned success and staged both deletes, leaving
reads inconsistent until a later flush failed. Async now validates with the
same `namespaceForEdgeIDs` contract before staging, reuses the validated
namespace for version updates, and skips empty IDs in both its tombstones and
pending-write count. A deterministic two-namespace regression pins immediate
`ErrCrossNamespaceTransaction`, unchanged pre/post-flush visibility, and a
valid same-namespace delete with an empty ID. Related tests pass twice under
`-race`. M2 Max `BenchmarkAsyncEngine_BulkDeleteEdges` (same benchmark, guard
temporarily removed for the baseline) measured 41.60–42.50 vs 40.56–44.93
ns/op for one ID, and 439.9–448.3 vs 467.4–471.3 ns/op for 16 IDs; all
runs used 0 B/op and 0 allocs/op. The 16-ID validation cost is measurable,
not a claimed speedup. The benchmark repeatedly stages a fixed set of IDs
without disk-edge setup; it isolates Async preflight/overlay work, not
end-to-end deletion throughput. Async and Badger retain their distinct
overlay/disk responsibilities; this candidate's contract mismatch is resolved.

`pkg/storage|.StreamNodesByLabelProjected()` pairs Badger's indexed scan,
Async's pending-write overlay, and BadgerTransaction's snapshot overlay. These
remain distinct storage views. Source review found that a transaction with
pending writes invoked a callback for an unchanged committed node while
holding its mutex, unlike its other callback branches. Such a callback could
not read through the transaction. The branch now uses the shared `invokeVisit`
path; `TestTxReads_StreamNodesByLabelProjected_CallbackCanReadWithPendingWrites`
checks reentrant `GetNode` on both initial scan and cached replay. Focused
transaction and wrapper tests pass twice under `-race`. The same wrapper race
run also exposed Badger Close clearing callback fields without `callbackMu`
while async deletion notifications read them under that lock; Close now clears
them under `callbackMu`, and the reproducing wrapper tests pass five times
under `-race`. A controlled cached pending-write scan benchmark on M2 Max
measured 482.0–497.7 ns/op before vs 489.8–494.5 ns/op after, both 440 B/op
and 13 allocs/op (overlapping bands). Other candidate groups remain open.

`pkg/nornicdb|.GetNode()`, `.UpdateNode()` and `.DeleteNode()` pair the
`APOCStorageAdapter`'s numeric-ID plugin storage interface with the public
`DB` API's string-ID, lifecycle and (for reads) property-decryption contract.
They are not duplicate Cypher executor paths and should remain separate
adapters; consolidation would conflate plugin and public API behavior. The
APOC CRUD/error and DB GetNode tests pass (10 focused cases). This is an
adapter-boundary deferral, not a claim of parity for all APOC procedures or
encryption paths; those contracts retain their own ownership.

`pkg/search|.Rerank()` pairs the optional local-scorer and LLM-backed
providers. The former scores documents individually; the latter parses an
ordered model response. Their different candidate budgets, fallback behavior
and score sources are provider contracts, not competing implementations of
one Cypher executor. Four focused ordering/error-fallback tests pass. Keep
the providers separate; retrieval-quality metrics remain a final verification
gate, so these tests do not establish ranking-quality parity.

`pkg/search|.LexicalSeedHints()` pairs full-text v1's map postings with v2's
compressed numeric-ID postings. Both select terms/documents and call the
shared `lexicalSeedHints` builder. `TestLexicalSeedHintsV1V2Parity` asserts
identical ordered hints on the same four-document corpus with normal,
single-result and zero limits; the existing rank/signature and metadata-only
tests pass alongside it twice under `-race`. The separate posting formats
remain necessary, while the emitted-hint contract agrees on this corpus.
This does not substitute for the final retrieval-quality gate.

`pkg/search|.SearchCandidates()` is the selectable ANN candidate-generation
interface across brute-force, KMeans, GPU KMeans, IVF-HNSW and IVF-PQ backends.
Their prerequisites and fallback paths differ by index strategy; same-label
calls are not duplicate Cypher query execution. Six focused strategy tests
(12 cases) pass. Keep these backend implementations distinct; the final
recall/latency gate is still required for any retrieval changes.

`pkg/search|.Search()` pairs the live ANN mutation overlay (fresh vectors and
tombstones) with the persistent base vector index; their return contracts and
roles differ. Source review found cancellation could be lost at their IVF-PQ
candidate merge: an empty/untrained base index returned success before checking
the context, and the overlay scan returns no candidates rather than an error
when cancelled. `IVFPQIndex.SearchApprox` now checks cancellation before its
empty-index exits, and the candidate generator checks the context after the
overlay scan. Focused empty-index and deterministic mid-overlay regressions
pass alongside neighboring IVF-PQ tests twice under `-race`. The 10k-vector
candidate benchmark measured 33,536–34,116 ns/op before vs 32,939–34,200
ns/op after (608 B, 7 allocs); the live-overlay benchmark measured 306.0–357.4
ns/op before vs 297.3–302.6 ns/op after (104 B, 5 allocs). These bands do not
justify a speedup claim. The separate index/overlay implementations remain;
the cancellation contract at their composition is now tested.

## Final disposition

Every same-label candidate group is dispositioned: merged into a shared kernel
where the responsibilities coincided, kept separate where the views
(Badger/WAL/Async/Namespaced/Composite overlays, remote transport, plugin vs
public API, index backends) genuinely differ, or documented as an
adapter-boundary deferral. One item remains intentionally open: remote
`WithEmbeddings` parity requires a vector-capable remote API contract (Bolt
normalization and the HTTP transaction API do not carry NornicDB's separately
stored chunk/named embedding vectors), which is an API-design decision, not a
Cypher executor fork. The retained legacy interface wrappers (the five
StreamNodes* methods, the computed-row evaluator facade) are public API
adapters over the converged kernels and are documented at their definitions.