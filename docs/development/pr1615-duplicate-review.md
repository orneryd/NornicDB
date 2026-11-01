# PR 1615 ownership review

This review accompanies the context-first Go API migration for NOR-20. It is
not a declaration that all checker candidates are duplicates, or that an old
finding can be ignored. Each candidate is compared with its actual consumers.
The unchanged checker is informative; unresolved investigations remain part of
the PR acceptance criteria.

The latest completed discovery snapshot examined tree
`fdf4d7138028d243a14ccc5de81f61a4bffc335c` against indexed release
`35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6`: 174 candidates (52 refactor,
122 reuse), 2,364 functions and no cap skips. It predates the additional
consolidation below and is **not a current-source clean result**. Earlier table
IDs refer to the preceding 187-entry inventory; they are not the new numbering.
The maintainer requested no further duplicate-check run this round. Current
source is reconciled against this complete known inventory; the latest attempt
hit its 300-second comparison deadline and remains incomplete. No clean detector
result is claimed. The final full `make test` result is recorded below.

## Consolidated owners

| Candidate family | Correction | Preserved contract / proof |
| --- | --- | --- |
| Namespace node/edge projections | One typed transaction projection owner; removed old forwarding helpers | Admission, each 256 records, final publication; partial underlying error identity and record ownership |
| Search scans and equality | One fused iterator and result writer; canonical primitive classifier | Examined-record cancellation, nil/list/map comparisons, original order and matching semantics |
| MCP, HTTP and Heimdall related nodes | One generic BFS engine with typed adapters | Original depth conventions, 50/100 caps, missing nodes, paths, ordinary partial errors and caller cancellation |
| Native and transactional relationship cardinality | Shared counter, constraint error and directional endpoint selection | Candidate exclusion and >= limit versus existing-population > limit; namespace boundary matches old prefix expression |
| Node / relationship property rename | One property transition owner | Missing keys, overwrite, same-key behavior and successful persistence count |
| Recorder snapshots (53,55) | One locked snapshot kernel; four private methods retired and all callers migrated | Same producer/reset mutex, slice header read under lock, detached shallow copy and nil empty result |
| Primitive comparisons (111) | `convert.NativeScalar` reused directly | Exact built-in primitive set; index misses still exclude nil and named types |
| Benchmark search fake (56) | Retired duplicate type and five methods; construct existing coverage fake | Identical healthy node/edge maps, 20/100 populations; no extra timed callback |
| Directional graph fake (58,59) | One filter kernel behind required interface methods | Nil omission, insertion order, nonnil result and exact direction |
| Discover fixtures (72) | One node/edge seed and invocation owner | Original two-node keyword fixture and distinct three-node/two-hop identity fixture |
| Property rename tests (117,120) | One execution/assertion kernel | Original values, count 2, key removal and surviving properties |
| Projected errors (119) | One error/result assertion owner | Distinct fake method sets, zero fallback lookup, exact error identity and nil result |
| Bundled byte loader fixture (142) | Existing catalog admission and plugin fixture, selected names | Only binary/stream installed, real compiled handlers reused, lookup restored on cleanup |
| Snapshot-label read aliases (89,90) | Retired both private forwarding methods; direct canonical callers | Explicit `visibleRead`, same pinned snapshot, scope, projection and resume ID |
| Louvain wrappers (182,184) | Retired unused allocating `modularity` wrapper | Existing scratch-buffer owner remains; complete Louvain race suite passes |

Additional test families share table kernels for role-owned mutation failures,
entity ID extraction, composite database identity, loader empties, snapshot
visibility, decay configuration, invalid UNWIND values, restore populations,
reopen label indexes, metrics observations, MVCC capability adapters and pinned
transaction label reads. Original case populations and assertions are preserved;
combining setup must not substitute a different workload.

## Required boundaries and actual shared portions

| Candidate family | Why its entry points remain | Shared behavior and limits |
| --- | --- | --- |
| Size-tracking and WAL indexed reads (1–4,31,76–79) | Required indexed-visibility interface methods select distinct underlying capabilities and typed results | Direct canonical read delegation. Common code is the capability assertion / unavailable error, not another scan. A new provider-forwarder would add an alias while preserving every interface method. |
| Traced reads (5,6) | Span lifetime encloses the specific operation and error | `startOperation` already owns span construction; graph reads directly reuse the wrapped engine. |
| Visible / stored adjacency (45–50) | Visible query reads and complete integrity/backup reads have distinct visibility policies | `readAdjacency` owns the traversal. Named interface methods only choose direction/visibility and the corresponding result. |
| Backup export (64,65) | Node and edge outputs have different types; stored capability preferred, export-only interface remains supported | StoredReader and CollectStoredNodes/Edges already own capability resolution and enumeration. |
| Identity views (84,91,92) | Full versus indexed scans and incoming versus outgoing adjacency cannot substitute their populations | Shared `nodes` / edge projection owners transform identity once. |
| Latest node / edge visitors (88) | Required visitor interfaces differ in entity type and context availability | AllNodes / AllEdges own physical reads. The remaining loop invokes the typed visitor and preserves its error; no second physical scan algorithm. |
| Creation constraint checks (126) | Uniqueness tracks repeated nonnull values; existence rejects the first null | Shared stored-label population and localized scan-error owner. Different validation algorithms remain; replacing one with the other changes accepted data. |
| Buffer locked/public pairs (151,159,163,176) | Public access owns locking and tracking; internal calls already hold the lock and carry caller context | The locked method is the only overlay/index algorithm. Calling the public method under its existing owner lock deadlocks; skipping the public lock races. |
| Node constraint admission (156) | Native transaction versus staged transaction supply different authorities, namespace pinning and context | Both construct the same `nodeConstraintValidation` owner; no second constraint algorithm. |
| Transaction total/type counts (157) | Total reads can use unchanged counters; type reads also apply a typed pending delta | Existing counter and scan owners reused; total count cannot substitute a per-type delta. |
| Namespace transaction adjacency (164) | Required namespace adapter must project IDs after transaction-owned adjacency | Transaction owner handles overlay/locking, `projectTransactionRead` handles projection/cancellation. Bypassing either loses isolation or staged visibility. |
| Collector / visitor (165) | Collection allocates a result; visitors can stop before full materialization | Collector directly invokes the canonical visitor; no second traversal. |
| UUID generation (153,183) | UUIDv7 time/random entropy and UUIDv5 SHA1(namespace+name) are different algorithms | Shared argument/error utilities and `uuidWithVersion` bit stamping; no duplicated stamping or legacy generator retained. |
| Read-view capture / scoped read (177) | Long-lived owners retain a lease; scoped reads must close it after execution | `withReadView` directly uses `captureReadView` then `readAndClose`; combining their lifetimes would prematurely close retained views. |
| Traversal cancellation (old 186, new 173) | Shared `cancelprobe.Ready` owns the nonblocking channel probe | Consumer latching and expression-error publication remain in their respective owners; the common probe is inlined into both. |
| I/O / memory policy (187) | URL/file access capabilities and query memory budgets have different state and enforcement | No common decision or algorithm beyond a constructor name. |

Other reviewed semantic boundaries include snapshot-file versus collection
creation, index metadata versus executor-owned physical backfill/rollback, map
argument parsing versus recursive removal, syntax recognition versus bound graph
execution, named projection publication versus ephemeral link prediction, and
privilege enumeration versus grant mutation. Each actual reusable lower-level
owner remains canonical; a name or similarity score is not the rationale.

## Measurement limits

The search iterator was compared against independent loops with the **same**
cancellation and safe equality contract. Allocations match that correct baseline;
51 of 52 three-round operation medians were within 5% or faster. The one +5.2%
case varied around parity in longer paired runs. Against the original incomplete
code, some small scans remain 6–13% slower: that is disclosed correctness cost,
not a claim of universal speedup. Compiler output confirms iterator fusion.
Related-node workloads reduce measured allocations across all three consumers.
Cardinality namespace selection matches original allocation counts after removing
an allocating prefix construction. See the corresponding performance documents
and PR evidence for exact workloads, raw variation and remaining qualification.

## Additional integrated consolidation

| Current inventory family | Canonical owner and preserved boundary | Integrated proof |
| --- | --- | --- |
| Actual cancellation probes | `internal/cancelprobe.Ready`; related-walk latch and Cypher expression error remain consumer-owned | Six alternating paired rounds: DFS +1.06% with overlapping variation, identical 5,176 allocations; 100K examined-record BFS within variation or faster, identical five allocations. Inlining and public-consumer races pass. |
| Louvain / Cypher checkpoint contexts | `testfixture.NewCheckpointContext` | Actual CancelFunc, Done, Nth live Err checkpoint, callback once and atomic concurrent counter; original thresholds/cases retained. |
| Read-failure fixtures | `testfixture.ReadWithFailure` with lazy provider closures | An injected fault preempts nil backend access; partial result/error pairs and context identity remain unchanged. A selected nil first lookup uses the distinct canonical `singleShotLookup` owner, preserving its counter and second successful read. |
| Directional CALL / GDS failures | One `directionalReadFaultEngine` | Exact optional method sets, endpoint selection and per-node injection remain; no merging of unrelated receiver state or capabilities. |
| Native/MVCC directional fixture assertions | `assertUpdatedEdgeIndex`, `mvccAdjacencyHistory.edgesAt` and directional tables | Same old/new index values, snapshot versions, incoming/outgoing populations, errors and cleanup. |
| Stream fixtures | `streamFixtureItems` and `streamStoredEntities` | Preserve early stop, caller context versus deliberately ignored context, error injection, prefix policy including nil behavior and per-method counters. |
| Composite fixture construction | Existing `compositePairFixture` | Independent AST comparison of all 52 bodies removes only whitelisted constructor assignments; all queries, operations and assertions are otherwise identical. |
| Memory/Badger directional tests | Shared seed/assertion kernels | Original IDs, five Memory cases, distinct incoming/outgoing Badger edge counts and invalid-ID assertions retained. |
| Cypher CREATE/DELETE and Graphiti tests | Shared execution/assertion and incomplete-index surgery owners | Original six statement executions and all counters preserved; benchmark setup remains outside the timer. |
| MCP / HTTP related benchmark fanout | `internal/testfixture/relatedgraph.Fanout` | Original root/branch/leaf IDs, properties, edges and timed loops retained; one-iteration checks are functional evidence only. |
| Go metadata sampling stub | Retired `GraphSample` and registry/classifier entry | The ignored limit was not a sampling implementation. Maintainer approved retirement; direct `meta.New(backend).Graph()` is the full-metadata owner. No new root-Call plan or Cypher procedure is added. |

The mandatory and optional self-relationship tests retain their different
original producers: the mandatory test's unlabeled `node` / `relationship`
records and the optional test's public `CREATE (a:B)` statement. A proposed
shared fixture changed those inputs and was rejected; only the common executor
factory is reused. Direct function argument-contract tests also remain: they
assert null/config/error precedence at a different consumer boundary from
Execute literal/variable/SET tests, using the same canonical production owner.

The two added test candidates in the 174-entry inventory retain distinct real
consumers. Bundled binary/stream policy denial verifies zero transport requests
at the scalar plugin boundary; native JSON/CSV/import/export denial verifies
its separate formats and read/write error families through the shared I/O
capability. MCP discovery with an indexed two-node database verifies keyword
results, while its no-database control verifies default arguments and missing
query failure. Both use the canonical production dispatch; replacing either
population with the other would remove those assertions.

The catalog-list generic kernel was rejected after measured category-lookup
regressions (including roughly 13.2 to 16 us and 14.1 to 15.2 us in subsequent
variants). Pointer-predicate and fused iteration alternatives were examined;
allocations stayed equal. Loaded-host variability and the raw batch ordering
limit the measurements; this is a workload-scoped decision, not universal
zero-overhead proof. The useful private invocation forwarding wrapper was
retired independently, with its argument-policy regression test retained.

## Complete known-inventory disposition

The [174-entry ledger](pr1615-duplicate-dispositions.json) records original
locations, current canonical owners, decisions, concrete reasons, evidence and
Go-AST-resolved receiver/name/body hashes. It supersedes worker proposals that
incorrectly matched stale line offsets or assumed different test populations.
Counts: 84 consolidated, 14 already reusing owners and 76 retained with explicit
contract or measured separation rationale; no known entry is left unresolved.
This is disposition of the existing inventory, not a claim of zero similarity
findings or a completed final comparison.

The last real defect exposed during this review was latest-visible iteration:
a callback could cancel its caller yet receive a successful enumeration. The
shared typed `yieldLatestVisible` owner now observes the existing 256-record
bound and final publication. Native first/last-callback cancellation reproduces
the failure; healthy population, pending-drop precedence, ordinary read/visitor
errors and the context-free edge API stay intact. The helper allocates zero
times over a prebuilt 4,096-record population; this isolated allocation proof
does not establish native scan timing neutrality. The point-read test double
also has one owner: the APOC consumer now uses existing `getNodeErrEngine`,
with no artificial context or forwarding alias.

## Integrated verification

Final `make test` passed on tree
`e258ab6b433f6784b7bcf0aa2abf9b5bece827b9` against release
`35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6`: 140 passing packages,
136 cached/four fresh, exit 0 in 608.65 seconds. The preceding full code
run passed 139 packages and found one generated allocation-inventory failure.
Official regeneration adds only `relatedgraph.Fanout` and its single
`storage.NewMemoryEngine` call; no old path or assertion is removed. Normal
assertions pass in the final run. The final commit's later changes are Markdown
qualification only; executable/test inputs match the tested tree.

No additional duplicate scan is run by explicit maintainer direction. The
last attempted comparison timed out; this document and the complete known
inventory are the guide, not a fabricated clean-check result. Broader
`make verify`, fresh full coverage, full-suite race and external Neo4j-reference
qualification are not claimed. The separate #1684 terminal-outcome contract
remains open and is not delivered here.
