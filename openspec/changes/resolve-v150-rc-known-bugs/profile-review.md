# PROFILE source and ownership review

## Integrated correction and limits

The PROFILE prototype is rebased onto the pushed RC corrections. Public
projection, CASE, map, range, comprehension, scalar-argument and predicate tests
exercise both parsers, cold/warm execution, and text/compiled configurations.
The added WHERE tests require distinct source operators with exact rows and
execution counts; identical logical operands retain the canonical planner's
common-subexpression elimination. No query result or cancellation contract is
weakened to produce those counts.

Parent review corrected the worker's predicate wrapper argument order before
runtime qualification. A new lazy CASE predicate then reproduced an actual
remaining source-attribution failure: the two branches merged into one Union
with six rows. The correction attaches the source base in the canonical
predicate compiler itself, including the mandatory CASE compilation when normal
program caching is disabled. PROFILE compilations never enter the shared
positionless predicate cache. The compiled logical fold remains in use rather
than being routed to an alternate text evaluator.

Evidence: `/tmp/v150-profile-case-predicate-red.log` preserves the runtime
failure. The combined PROFILE/EXPLAIN/expression/cache race population passes
in 15.687 s; after shared cache publication consolidation it passes in 15.726 s.
Focused concurrent-publication, canceled-compilation and predicate controls pass
under race instrumentation in 2.789 s. Full CI and physical-route coverage remain
pending; these are focused results, not a complete gate.

## Canonical owners and dispositions

| Candidate | Behavior and correction | Evidence / limitation |
| --- | --- | --- |
| Text-only versus source-bearing comma splitting | One value iterator owns token/comment/group handling; callers retain text or text plus offsets. No intermediate list or second scanner is retained. | Source-offset controls and 8/9-item ordinary cost measurements in the performance guide. |
| Source-aware value versus predicate cache publication | One typed `profileProgramCache` owns lookup, cancellation and concurrent publication for both program types. Separate copies of mutex/publication logic are retired. | Concurrent readers receive the same accepted program; canceled compilation cannot populate the cache. |
| Predicate and scalar expression compilation | The canonical predicate and value compilers retain their distinct bool/error and value/resolution/error contracts. Predicate operands reuse the value compiler; PROFILE only supplies source extents. | Ordinary predicate modes and public PROFILE matrices pass. No second predicate evaluator is introduced. |
| Borrowed expression offsets | One pointer-window owner serves both runtime rebinding and predicate compilation; no byte-content search can confuse repeated identical parts with copies. | Owned-window, Unicode and copied-string rejection controls; pointers are compared only and kept alive. |
| Retired EXPLAIN label scanners | `extractFirstNodeLabelFromQuery` and `extractLabelFromNodeSection` are removed. `explainNodePattern` calls canonical MATCH/node readers, and existing seek planners decide applicability without executing user expressions. | Both-parser EXPLAIN applicability and no-side-effect controls. The required lexical member is replaced one-for-one with the current owner. |
| Retired row compiler forwarding method | `rowValueCompiler.part` is removed; callers invoke `compile` with retained fragments directly. | Scalar source-boundary and ordinary expression controls. |
| Retained grouping keys | Projection uses the value computed for its group, rather than reevaluating scalar or subquery expressions. | Single-evaluation public tests and explicit ~2% byte cost with unchanged allocations; no memory improvement is claimed. |
| Leading UNWIND preparation | The row pipeline takes the already prepared items when batch execution declines, rather than evaluating again. | Public single-evaluation, parameter, range and correlated-row controls. |

The value and predicate source-root adapters implement different evaluation
interfaces: a value plus resolution/error versus a boolean predicate that records
statement errors. Their common context-base binding is small; a forwarding
adapter would add an entry point without retiring either interface or reducing
the evaluation code. Program publication, source-offset arithmetic and operand
compilation—the substantial common behavior—already have shared owners.

## Inventory review

The unchanged lexical extractor initially rejects the retired EXPLAIN minimum
member. The factual minimum manifest now requires `explainNodePattern` instead.
The first reviewed delta has eleven new, three retired and two changed members;
continuing facts and their prior dispositions remain intact. The new access-key
candidates are typed runtime ledger keys, not configured endpoint credentials.
Source-backed reasons and existing test references accompany every added entry.
That population assertion passes in 1.913 s. Final discovery after predicate
integration adds one source-constructor member and updates the existing
embedding handler to its reviewed credential-output role after the main rebase.
The final 4,034-member population assertion passes in 1.915 s and the unchanged
endpoint structure assertion passes in 1.165 s. The final predicate/publication
race selection, including known zero source origins, passes in 2.800 s.
No extractor rule or assertion is changed.

The remote Graphify transport still returns HTTP 400 (plain HTTP to HTTPS port).
No clean remote duplicate result or exhaustive convergence claim is made.

## Physical count-store reductions

A public both-parser regression reproduces the complete counter-read ledger but
an incorrect generic "outside clause pipeline" completeness note. The canonical
count reducer now returns typed provenance: declined, native counter, visibility
fold, or traversal. A scan-backed undirected count cannot acquire counter-backed
completeness from its shared router. The private fused-plan callers migrate to the
existing dispatch outcome; no forwarding adapters or exported API changes remain.
One shared count-result constructor owns the scalar row and completeness state.

A counter emits one count row and records one actual counter request, including
when its value is zero. An internal-label fold emits one zero-count row with no
storage access. Neither substitutes the graph population for actual operator rows.
Counter declines, decay/temporal gates and scan-backed undirected counts keep their
existing authoritative row paths; incomplete observations remain explicit.

Integrated public/reader/count/visibility/correlated/property-admission and error
controls pass under race instrumentation (10.221 s). The unchanged lexical and
normalization population assertions pass (2.149 s). The public 150-document /
450-relationship uncached ordinary-count benchmark retains 83/84 allocations and
unchanged byte medians; latency medians differ by less than 1%. Its existing
corpus and runner are reused; no second fixture/query loop or production counter
reader is introduced. Full metrics and commands are in the performance guide.

The operator confirms built-in execution routes require coverage, while custom
callbacks may explicitly report unknown metrics. Other physical families still
need qualification; this count-store correction does not claim them complete.

## Physical reads and fused mutations

The canonical physical dispatch now reports bounded projections, indexed OR/IN
reads, filtered counts and scan-backed undirected counts at their actual owners.
Consumed empty sources report known zero; unexecuted sources and unsupported
operators remain explicitly unknown. CREATE/SET/UNWIND phases enter their source
frame before execution and report their own emitted rows. Write-access totals
remain unobserved pending actual mutation accounting; this is not complete
built-in PROFILE coverage. No final result cardinality substitutes for a scan.

Physical read helpers move intact from pipeline_executor.go to
pipeline_physical_reads.go. The reviewed 4,034-member lexical population has six
path changes, without added/retired mechanisms or changed rules/dispositions.
Its assertion passes (1.932 s). Integrated PROFILE/EXPLAIN/read/admission/source
controls pass with race instrumentation (16.693 s). Function-registry package
race checks pass (1.601 s). Full make test and census remain CI-owned.

The existing SeedBareCreate benchmark is reused unchanged except for an untimed
public count postcondition of 500*b.N. Three samples of three operations preserve
the exact population. Ordinary median latency is 7.220 to 7.144 ms; bytes
5,920,437 to 5,957,002 (+0.6%, overlapping sample ranges); allocations 59,678 to
59,668 (overlapping ranges). No systematic performance change is established.
The ordinary filtered-count and bounded-projection controls retain identical
bytes/allocations; latency medians differ +3.9% and +2.6% respectively.

CPU/memory captures include fixture setup and untimed postconditions, so whole
profile percentages are not query-only hotspot evidence. Test-only harnesses
and logs are retained under /tmp/v150-profile-{write,fused-read}-*.

## Scalar GDS and composite catalog observations

The approved registration policy now opts in GDS link-prediction, projection
lookup/property reads and the two composite catalog functions only after their
actual acquisitions report through the existing observation ledger. Self-loops,
parallel entries and type-rejected entries remain visible before neighborhood
deduplication/filtering. Custom replacements remain opaque, including replacements
of reviewed names. No second storage request or observation state is added to an
ordinary statement. These are abstract logical reads, not disk/cache counts or
promised numeric Neo4j parity. GDS procedures and native plugins remain pending.

Both-parser public controls and function-level catalog visibility/replacement
controls pass under race instrumentation (4.061 s / 2.836 s). The old production
implementation fails their access assertions; the baseline overlay adds only the
new constant labels required to compile those tests, no observation behavior.
Two initial worker fixture expectations were corrected from the actual fixture:
the parallel/self-loop union is {a,b,c}, and registry callback names normalize
to lowercase. The first baseline attempt failed compilation and is not runtime
evidence.

The unchanged 5,000-node/20,000-edge GDS property benchmark uses three samples of
1,000 operations. Median ordinary latency is 19,127 to 19,504 ns (+2.0%); bytes
2,696 to 2,695; 48 allocations on both sides. No material performance change or
speedup is established. CPU/memory captures include fixture creation; their
whole-process scheduler/GC percentages are not query-only hotspot evidence.

## Token listings and one population recorder

The three graph-token listings now declare reviewed observation policy at
their registration sites. Handler and policy publish together under the existing
registry lock; aliases retain the declaration of their actual handler. Public
RegisterBuiltIn/RegisterUser admissions remain opaque, even for a reviewed
name. The public built-in entry supplies that fixed admission policy to the
single private validation/storage owner; it is a policy boundary, not a second
implementation or a compatibility fallback. No exported signature changes.

One generic entity-population recorder replaces the proposed separate node and
relationship loops and retires the GDS-specific recorder. Listings, GDS
adjacency and undirected count scans share the non-nil/known-empty rules, with
no ordinary-path population pass or allocation. The existing nil-relationship
fixture reproduces a false one-entry read on the old count path; the same
regression now proves zero real entities were read. The streaming visitor stays
separate because it must observe delivery before its consumer filters rows;
routing it through a bulk slice would allocate and change that contract.

Public listing regressions reproduce opaque metrics on the old dispatcher.
The old db.propertyKeys implementation also discards node/edge scan failures;
the corrected public call propagates either failure instead of returning a
partial catalog. Integrated PROFILE/EXPLAIN/GDS/count/registration controls pass
with race instrumentation (14.424 s Cypher, 2.880 s fn). Strengthened public
built-in and user replacement controls pass (2.424 s) and restore prior registry
state. Lexical discovery reports the same 4,032 reviewed facts, unchanged.

Ordinary token listings reuse perRowDocGraph and runPerRowBench, asserting
exact token counts with result caching disabled. Three samples of 300
operations show label/type/property latency medians 203,454/260,956/469,148 to
201,748/265,440/466,259 ns. Allocation counts remain 3,320/5,408/8,620; byte
medians differ by +4/-1/0 bytes within sample variability. No speedup is claimed.

After consolidation, a short GDS sample showed +6% latency. A matched longer
qualification (three 10,000-operation samples each side) measures 18,504 to
18,273 ns, 2,689 to 2,690 B and 48 median allocations on both sides; each side
has one 49-allocation sample. The evidence does not establish a consistent
latency or allocation regression. Full logs and CPU/memory captures are retained;
setup remains included in process-wide profiles.

Other procedure families and write-access completeness remain pending. Native
bundled callable policy has a separate public both-parser failing test and an
approved compiler-API migration; no blanket built-in coverage is claimed.

## Schema catalogs and registration restoration

The three schema catalog procedures now report actual index/statistics and
constraint acquisitions. Rows count the entries returned by each acquisition;
one hit denotes the catalog request, not physical disk I/O. A vector index
state lookup records one answer from the single resolved search service,
including that service's ONLINE default. With no service, no state read is
claimed. Custom built-in/user replacements remain opaque.

An overlay retaining the old reader bodies reproduced missing observations
through public calls on both parsers (1.517 s). Integrated populated/empty
catalogs, vector-state resolution, custom replacement and token controls pass
under race instrumentation (2.743 s). Catalogs have no ordering contract; the
plain/PROFILE comparison asserts exact row multisets rather than incidental
map iteration order. No query output contract was changed.

The full CI Cypher shard exposed test contamination: the deterministic-cache
registration test restored the handler and public spec but discarded the
registration's private observation policy. Running that test before the public
token controls reproduces the CI failures (1.389 s). Both restoration sites
now use the canonical registry owner with the original complete declaration;
the replacement-under-test still uses the public API and remains opaque. The
paired ordering regression passes under race instrumentation (2.433 s).

No checker rules or runtime admission policies were weakened. Full-suite and
census confirmation remain CI-owned.

## Native callable policy and invocation phase

The approved registry.Compile(function, policy) migration is integrated, with
canonical ReadObservation ownership in internal/queryprofile shared by fn and
apoc/registry. Public registration and plugin consumers migrate without an old
signature or compatibility wrapper. Already-admitted implementation/policy
identity survives direct and descriptor plugin transport; raw functions remain
opaque. The declaration is a Go host contract, not a security attestation.

The complete 981-name registration comparison retains every name, category,
implementation, description and example; only policy arguments are added.
Independent DeepSeek source review inspected backend ownership and all seven
canonical backend-owning methods. Parent review corrected its callback
reachability conclusion: assignable Go function parameters pass BindArgument,
so the public embedded query API can actually invoke caller graph reads.
That both-parser regression fails with a certified zero before the correction
(1.371 s). Fifteen callback-consuming helpers now remain opaque. The final
registration split is 839 canonical / 142 opaque; an all-signature control
rejects canonical declarations on callback consumers. Existing graph-shaped
standalone stubs are not implemented or feature-qualified by this metadata
change; no new functional-correctness claim is made for those declarations.

Two phase errors in the initial worker candidate were also rejected: matching
a nested error cause cannot prove the outer callback never ran, and a panicking
opaque callback still ran. A single sealed invocation receipt owns error phase
and cause separately. The adapter records actual opaque entry on normal and
panic paths, preserving existing fallback, panic recovery and error chaining.
No parallel invocation/reflection owner or duplicate policy enum remains.

Final integrated PROFILE/typed-callable tests pass under race instrumentation
(Cypher 9.056 s, registry 1.911 s, bundled callback inventory 1.563 s). Whole
registry/fn/APOC/math package checks pass (1.402/1.542/2.945/2.794 s);
testing/apocintegers compiles and has no tests. Focused plugin, Bolt and HTTP
consumers pass (27.681/3.149/20.232 s). Additional direct/descriptor/raw plugin
policy transport controls pass (1.993 s). Full-suite/census stay in CI.

A matched three-sample ordinary identity invocation benchmark (one million
operations per sample, M2 Max, Go 1.27.3, noui/nolocalllm) retains 56 B/op and
three allocations on both sides. Median latency is 187.0 to 191.2 ns (+2.2%);
sample ranges overlap. No speedup or allocation improvement is claimed.

Source-only review dispositions: loader-time Functions() and root apoc.Call
have no PROFILE observer and do not promise metrics; their immutable admission
policy matters only when a query dispatcher consumes the handler. Plugin
transport preserves explicit declarations rather than reclassifying them.
Callable-argument helpers remain opaque as proven above; value-returning
loader factories and timer factories do not themselves invoke caller callbacks.
Formal Graphify retrieval was retried and still returns HTTP 400 (plain HTTP
to HTTPS port); current-source review is not a clean duplicate-checker run.

## Definition procedures, property scans and dynamic write images

Four process-definition procedures now declare known zero graph reads:
dbms.components, nornicdb.version, dbms.functions and dbms.procedures. Their
actual handlers only read build metadata/static lists/the process registry.
The two schema-property listings now report their actual node/relationship
population through the existing generic owner and propagate population errors
in both ordinary and PROFILE public CALLs. They no longer return successful
empty metadata on a failed scan. Public both-parser fail-first evidence is
retained (1.400 s); populated controls distinguish three input entities from
two derived property rows, and empty inputs retain observed zeros.

Replacement controls for token, schema and definition procedures share one
asserted population and complete registry restoration, retiring the duplicated
token-only fixture body. Original db.labels column and seeded-graph control
are retained; both public built-in and user registration APIs remain opaque.
No production validation, registration or observation implementation is copied.

The inventory also exposed missing statement metadata on apoc.cypher.doIt and
apoc.cypher.runMany. A public cold-vector regression supplies inner statement
text at runtime and a node parameter. Both parsers previously returned null
in place of the stored component (1.446 s). Both registrations now use the
existing statement-procedure owner, preserving full node images for inner
consumers instead of attempting a newer-snapshot reread. No signature or stored
format changes. Their opaque inner callbacks remain explicitly unobserved.

The integrated race selection passes in 39.179 s, including PROFILE, image
policies, statement contracts, read-failure paths and the complete controlled
registration admission matrix. The matrix's new WRITE-plus-dynamic-read cells
exposed a fixture assumption that WRITE admission always creates a node.
Admission/transaction ownership remains independently asserted; expected
persistence now follows the actual controlled body. Added public write-
procedure read-body checks separately assert the stored node count is unchanged.

The bounded DeepSeek implementation job failed during TLS setup and left a
clean worktree; parent implemented and verified the correction. The separate
fulltext/vector PROFILE investigation remains source review in progress.

CI for the preceding 5bcde39fe candidate failed during workflow startup without
creating any jobs. The run page reports an unexpected GitHub internal error
(request 6f00a84d-de11-11f1-8b6e-3fa63df1acfb); retry also failed to start.
This is neither a passing suite nor a runtime test failure, and no workflow
assertion is weakened to mask it. Final integrated CI remains required.

## One token reader and truthful graph statistics

Public nornicdb.stats calls reproduced successful zero label/type counts after
a population-read failure on both parsers. The expanded fail-first matrix
separately exercises node/relationship failures and ordinary/PROFILE calls;
its current-source failure is retained (1.469 s).

Labels, relationship types, property keys and the two statistics token counts
now use one typed acquisition/insertion owner with a caller-owned map. Parent
review collapsed the worker's remaining repeated acquisition branches too.
Existing listing order and columns stay unchanged. The old private countLabels
and countRelTypes functions are retired; no wrappers remain. Every population
error propagates instead of publishing partial success. Counter requests
report one returned count and one logical API request, explicitly not a claim
about backend count-store I/O or the numeric graph cardinality.

Public count-response/population/empty/custom-replacement/error controls and the
integrated PROFILE population pass under race instrumentation (11.433 s). A
three-sample CPU/memory-profiled comparison uses the unchanged 451-node /
450-relationship benchmark owner with result caching off, plus its stats arm.
Labels/types/properties/statistics allocation medians are unchanged at
3320/5408/8620/8585; byte medians differ by +16/+2/+1/-2 bytes within sample
variation. Latency medians differ by +1.0%/+1.3%/-0.4%/+0.8%, with overlapping
ranges on every arm. No speedup or allocation improvement is claimed.

The owning shapes remain different from schema-property maps (label/type to
property sets) and graph statistics that additionally accumulate per-token
counts. The overlapping token-only enumeration is consolidated; neither richer
result shape is replaced by an allocating token-only intermediary. Formal
duplicate analysis remains unavailable, not passed.

## Shared reader observation owner

The request ledger now hands canonical readers its own two-atomic counter through
`internal/queryprofile.Access`. Cypher and search can share entity population and
stream visitor reporting without a second ledger, retained context or per-entity
observer lookup. The old Cypher-only access/population/visitor helpers and the
separate observer write method are retired; all current callers migrate together.
Known-zero acquisitions remain visible (a worker fake that discarded them was
rejected using a failing control). Ordinary visitors return the original consumer.

Integrated focused race checks pass: Cypher 27.649 s and function registry 1.548 s.
The unchanged lexical assertion passes (1.926 s): four source-reviewed additions
replace one retired function, with every continuing fact and disposition unchanged.
The manifest preserves its original compact formatting; no analyzer rules change.

Three 300-operation token-listing samples against the immediate predecessor retain
allocations (labels 3,320; types 5,408; properties 8,620; statistics 8,585). Median
bytes differ by at most 19 B across 382–757 KB operations; median times are
201,851→197,721 / 264,697→256,919 / 468,048→459,279 / 426,876→425,451 ns, respectively.
These short CPU/memory-profiled samples qualify ordinary-call overhead, not a
statistical speedup. Fulltext/vector observation completeness remains pending.

## Fulltext PROFILE readers

Both public fulltext procedures now report actual acquisition and hydration
through the shared read owner. A streaming record counts each delivered entity
before membership/deduplication; it never counts emitted hits as scanned input.
Ranked node acquisition uses its declared label hint. Ordered wildcard/presence
pages deliberately preserve the existing whole-population release order and
therefore report AllNodesScan. The initial worker expectation of a labelled
ordered scan was corrected to match that source contract; no production reader
route changed to satisfy it. Population-captured images add no point reads; a
retained corpus hydrates emitted rows through its original request snapshot.
Changed-entity reads and schema retention rechecks are observed at their actual
owners. Each fulltext-definition lookup is one logical request with zero/one
returned definitions; these counters are not disk I/O or ANN-visited counts.

Public fail-first controls reproduce missing observations on both parsers
(1.138 s). Integrated PROFILE/fulltext/chunk controls pass under race
instrumentation (Cypher 21.619 s, search 5.503 s), including preserved reader
error causes/status, exact result/score equality, retained reuse and no extra
reader work. Replacements of either procedure stay explicitly opaque through
the existing shared replacement population. The unchanged lexical guard passes
(2.273 s) without a manifest or rule change. The additional DeepSeek review was canceled when rebasing superseded its live
source view; no acceptance is claimed from it. Parent source review follows
the integrated acquisition, incremental refresh, retained image and snapshot
owners, with the public reader probes as independent runtime evidence.

Three longer 1,000-operation paired ordinary samples preserve allocation medians
at 400 and 4,000 generated nodes for top-k/count/eligibility reads. Byte medians
differ by at most 37 B (65–607 KB per operation); latency differences are
−0.9% to +1.8%. The first 100-operation samples showed up to +7.2%, so those
shorter samples do not establish a stable regression or gain. Both profile
captures and the longer results are retained; no speedup is claimed. The
separate cold/first-reuse 100-operation CPU/memory captures pass at both sizes;
allocation variability overlaps the predecessor. No storage, score, ordering,
result-copy, cache-bound or snapshot contract is changed.

## Vector and GDS catalog reader checkpoint

Public both-parser vector tests reproduce the opaque/missing population and
relationship-hydration records (1.176 s). Exact node/relationship routes now
report the actual label/edge population before scoring; service routes report
one logical index query and the candidates that request returned, plus real
point hydration. Returned candidates never stand in for visited ANN entries or
physical I/O. Schema-definition reads, ONLINE waits and state requests are
distinct logical operations. State admission reuses the service actually passed
to it rather than resolving another one. The private eligibility callback and
its callers carry the same context; no exported API or stored format changes.

One shared EntityReadResult owns point outcome reporting across query/search
readers, preserving failed/absent zero rows and one actual request. Search's
existing metadata point/batch readers move intact to a focused file and record
actual calls, without extra reads or vector decoding. Optional unsupported
batches add no read; their existing individual-reader path owns real attempts.
A batch is one logical request with the map it returned, not invented disk hits.
An externally supplied embedding provider remains opaque only when invoked; a
cache hit reports no new invocation. A request joining a configured background
warmup reports its independently owned work as unobserved, never as an invoked
callback or a guessed read count. Empty warmup and subsequent completed/cache
paths remain fully observed in public controls.

GDS lookup/list acquisitions now own their observations. The duplicated scalar
lookup counters and private executor forwarding method are retired; all callers
(including FastRP) migrate to the existing context-bearing lookup. The two
qualified catalog procedures report one scoped request, with missing/empty and
name-filtered population controls. Other algorithm procedure policies stay
opaque until their own readers are qualified. Public catalog fail-first controls
reproduce the gap (1.184 s). Custom replacements stay opaque in the existing
shared replacement population.

Integrated PROFILE/vector/fulltext/GDS race checks pass (Cypher 99.075 s, search
58.516 s). Additional public warmup/provider/cache/service-identity and generic
point-outcome race controls pass (Cypher 3.741 s, shared package 1.296 s). The
unchanged lexical assertion passes (2.281 s), without a manifest/rule change.

Three 1,000-operation CPU/memory-profiled samples preserve every allocation
median for four ordinary vector procedure arms: node exact/service 181/180,
relationship exact/service 218/181. Median bytes are unchanged within 23 B on
13.9–17.0 KB calls; measured latency differences range −0.4% to +3.0%. Existing
GDS integer/float/list property benchmarks retain 48/48/61 allocations and byte
medians within 1 B; latency differences range −0.9% to +1.9%. The frozen
pre-correction test binary provides the unchanged GDS harness baseline. These
small generated workloads qualify observation overhead, not large ANN or private
application throughput. No speedup claim is made.

### Native mutation-request observation checkpoint

Native staging calls now own request counters for node/relationship creates,
updates and deletes. They are logical requests, never claimed as physical KV
writes: one attempt is one hit; successful staging acknowledges one request.
A true relationship batch has its own `RelationshipCreateBatchRequest` kind and
counts once; node batches issue one native call per node. Cascades remain one
node-deletion request rather than invented individual edge-deletion calls.

MERGE fallback population reads converge through one scan owner. Its selective
pinned equality result is named `NodeEqualityLookup`, not a whole-label scan.
Relationship identity point reads reuse `queryprofile.EntityReadResult` and
between-endpoint and deletion-incidence acquisitions are observed at delivery.
The parent found and included the additional labelled MERGE fallback scan omitted
by the initial worker proposal. Physical routes outside the clause pipeline and
opaque custom callbacks retain their explicit unknown markers.

Public both-parser regressions fail on the original blanket-unknown write path
(`/tmp/v150-write-profile-red.log`, 1.116 s). Integrated focused PROFILE, MERGE,
SET/REMOVE and DELETE race controls pass: Cypher 29.959 s and shared observation
package 1.256 s. The unchanged lexical population guard passes in 2.305 s.
No full local `make test` or compiled census was run; those remain CI gates.

Three 500-operation samples per arm retain ordinary allocation medians:

| Ordinary workload | Before ns/op | After ns/op | Before B/op | After B/op | Allocations before/after |
| --- | ---: | ---: | ---: | ---: | ---: |
| Existing relationship MERGE, fanout 2 | 95,071 | 92,571 | 27,870 | 27,871 | 333 / 333 |
| Existing relationship MERGE, fanout 32 | 97,896 | 97,162 | 30,268 | 30,269 | 363 / 363 |
| Existing relationship MERGE, fanout 256 | 149,874 | 150,062 | 48,204 | 48,205 | 587 / 587 |
| Generated 500-node bare CREATE | 8,343,770 | 8,374,194 | 7,102,497 | 7,103,968 | 60,944 / 60,944 |

CPU profiles and raw samples are retained under `/tmp/v150-write-profile-*`.
The shorter 100-operation samples varied more (+8.1% for the largest MERGE and
+5.7% for CREATE); their timing increase did not persist in the longer samples.
These are workload-scoped controls, not a new throughput claim.

### Algorithm and schema-shape reader checkpoint

Reviewed built-in registrations now cover FastRP's single retained-projection
lookup, constant GDS version/ping/client configuration and registered analyzer
metadata, the six whole-graph APOC algorithms, and the three schema-shape
procedures. The algorithm reader records adjacency before type filtering and
reuses its existing wrapper; ordinary unfiltered calls allocate no wrapper.
Supplied node arrays are values, not invented node reads. Static procedures do
not invoke stemmer implementations or read graph state.

Schema shape counts reflect source entities before grouping. Visualization
records its two entity populations and actual per-label catalog requests.
The duplicated index-catalog acquisition/observation in index listing, vector
selection and visualization now has one `readSchemaIndexCatalog` owner; each
consumer keeps its different result filtering. No extra catalog read is added.

Parent review removed duplicate test ledger/registry-restoration helpers in
favor of existing owners, corrected the missing-projection test to inspect the
query's actual collector before independently checking PROFILE's error, and
corrected the visualization total to count catalog requests, not catalog rows.
The individual per-record assertions remain exact. Public fail-first logs:
`/tmp/v150-fastrp-profile-red.log` (1.145 s), static procedures (1.076 s),
whole-graph algorithms (1.115 s), schema shapes (1.137 s).

Qualified focused race checks: FastRP/static/GDS population 78.866 s,
whole-graph algorithm population 27.762 s, corrected schema/vector/catalog
population 7.404 s. The combined `go test -race ./pkg/cypher -run '^TestProfile'
-count=1` passes in 12.264 s. The unchanged lexical population guard passes in
2.256 s. These are focused checks, not a whole-package or full-suite pass.

Three 50-operation ordinary-query samples per arm (32 nodes, 64 relationships)
retain or reduce allocation medians; timing differences are +0.2% to +2.94%.
This is an overhead check, not a speedup claim. One earlier overlapping baseline
sample was discarded and rerun in isolation; only the isolated samples below
were used. CPU profiles/raw samples are `/tmp/v150-wholegraph-profile-*`.

| Algorithm / type filter | Before ns/op | After ns/op | Before B/op | After B/op | Allocations before / after |
| --- | ---: | ---: | ---: | ---: | ---: |
| pageRank/filtered=false | 213,731 | 217,651 | 200,137 | 200,137 | 1,500 / 1,500 |
| pageRank/filtered=true | 201,900 | 204,362 | 200,740 | 200,737 | 1,511 / 1,511 |
| betweenness/filtered=false | 1,836,848 | 1,855,548 | 1,618,375 | 1,618,405 | 19,414 / 19,414 |
| betweenness/filtered=true | 1,780,532 | 1,804,472 | 1,599,042 | 1,599,024 | 20,417 / 20,417 |
| closeness/filtered=false | 2,715,262 | 2,752,045 | 2,131,784 | 2,131,776 | 34,201 / 34,200 |
| closeness/filtered=true | 2,788,364 | 2,836,363 | 2,129,808 | 2,129,806 | 36,996 / 36,996 |
| louvain/filtered=false | 249,730 | 251,713 | 211,501 | 211,501 | 1,522 / 1,522 |
| louvain/filtered=true | 261,610 | 267,828 | 212,166 | 212,165 | 1,579 / 1,579 |
| labelPropagation/filtered=false | 333,519 | 334,191 | 339,849 | 339,849 | 3,996 / 3,996 |
| labelPropagation/filtered=true | 336,469 | 346,378 | 341,674 | 341,678 | 4,199 / 4,199 |
| wcc/filtered=false | 185,282 | 187,697 | 231,836 | 231,836 | 1,960 / 1,960 |
| wcc/filtered=true | 181,632 | 185,630 | 233,153 | 233,150 | 2,035 / 2,035 |

Remaining retrieval/relationship/continuation provider and reader paths are not
certified by these cases. Their existing opaque declarations remain until their
actual read and callback owners are completed and qualified. Final-head full CI
and census equality remain required.

## Search endpoint and continuation observations

Relationship retrieval now uses the admitted query-node reader for endpoint
rows. Native predicates retain full vector-bearing node images; both-parser
public tests assert those vectors reach native consumers. Relationship candidate
and endpoint accesses are counted at the actual read, including missing or
unreadable endpoints without inventing returned rows.

Complete ID-continuation construction records its synchronous scan and constraint
catalog acquisition. Each later pull reports only its own page hydration under
its own context. The optional embedding-free batch capability has one invocation
owner; a declining capability retains each caller's original fallback (service
point reads versus continuation bulk reads). Supported errors remain errors, not
permission to fall back. Both batch fallback contracts, empty catalogs, filtered
scans and independent pull observers have component controls.

The public plan-tree walker and component read recorder each have one shared
owner, replacing duplicate test ledgers/walkers. No analyzer, callback policy,
stored format, cache membership or snapshot lifetime is changed in this slice.

Fail-first evidence: the actual public PROFILE endpoint tests reject baseline
production on both parsers (1.097 s); explicit-YIELD continuation tests reject
baseline production (1.095 s). The initial worker bare parameterized CALL test
could not derive a result schema and was not a valid observation reproduction;
the supported explicit-YIELD form supplies that schema. Public/native-consumer
and component race controls pass, as does the integrated PROFILE race selection
(Cypher 13.142 s, search 2.329 s). Lexical/normalization population assertions
pass (2.105 s). Final-head CI remains required.

The latest prior candidate's runtime shards, capture census and lint all pass.
Its storage census differs only in two reviewed source positions in search.go,
which moved nine lines; every normalized decision and every other report field
is identical. Generated JSON and Markdown are refreshed without rule,
exemption, population or finding changes. CI must confirm the refreshed artifacts.


## Actual opaque provider and filter invocations

The rerank owner reports the provider call only after it has an undecided,
rankable batch. Memo-decided, disabled, absent and empty paths report no call;
provider errors retain the invocation. Heimdall reports actual Generate,
GenerateStream and GenerateWithTools calls, with Chat delegating once. No
provider is declared to have known database-read counts.

Search decay and continuation authorization each use one actual-invocation
owner. Eligibility order, errors, in-place filtering, response images and cursor
lifetimes remain unchanged. Public continuation tests compare actual callback
counts with observations, then use a separate observer for the next pull.
First-party Cypher predicates retain their own expression observations.

The parent retired the two proposed package-local recorders in favor of the
shared fixture. Baseline production fails the rerank public PROFILE controls
on both parsers and the direct provider controls. Integrated provider/filter
race checks pass (search 40.743 s, Heimdall 1.570 s, Cypher 2.418 s); the
expanded PROFILE race selection passes (search 1.851 s, Cypher 13.035 s).
An initial parent test referenced the wrong runtime-metadata type and failed
compilation; the corrected test is included in these passing runs.

The unchanged lexical extractor adds one member, authorizeContinuationNode.
Its source is the typed authorization callback boundary, not a second credential
parser or renderer. The factual manifest records that role and the exact tests;
no extractor, threshold or exemption changes. The population assertions then
pass (1.922 s). Provider-specific ordinary overhead has not been separately
benchmarked; no provider speedup or exhaustive whole-suite pass is claimed.


## Policy metadata observations

The policy procedures report actual schema-catalog acquisitions, entity lookup
attempts, access-metadata point reads and the returned pending-queue scan. These
are logical API requests/populations, not physical Badger gets or entries the
storage implementation filters internally. Acquiring a schema/view handle is
setup, as for other observed logical readers; it is not an invented second
catalog request. The scalar policy/decay readers share the access-metadata owner.
Full-image policy entity reads retain their prior payload contract; their small
point-read adapters share EntityReadResult rather than changing admitted images.

Baseline public tests fail on both parsers (1.109 s). Integrated schema/policy
and custom-registration race controls pass (6.019 s); unchanged lexical checks
pass. Empty reads remain observed zeros, unsupported stores make no metadata
request, and arbitrary custom replacements remain opaque. Existing Go consumers
and stored formats are unchanged; private procedure methods now receive their
caller context. No ordinary-policy benchmark or exhaustive built-in coverage
claim is made from these focused checks.

One shared public statement fixture replaces the duplicate schema/policy
plain-versus-PROFILE runners. Its exact row-multiset comparison retains the
existing unordered catalog contract: there is no ORDER BY in these controls.
An initial parent consolidation incorrectly required incidental row order;
the corrected assertion preserves the original schema test rather than changing
production ordering. Ordered-result contracts have their separate exact checks.


## Graph/path actual-read ownership checkpoint

The identity graph reader owns actual bulk, label and adjacency populations
before identity projection or consumer filtering. Existing algorithm-only
counters and the extra relationship wrapper are retired. Traversal readers
reuse one adjacency recorder; undirected deletion preparation keeps its own
owner, so an incidence read is not counted twice. Native path/neighbor and
link-prediction registrations are reviewed individually; arbitrary replacements
remain opaque in the shared registration control. Full-image semantic scoring
retains vectors and records the point-read outcome.

Public fail-first controls establish missing observations and swallowed native
adjacency errors. PageRank, betweenness, Louvain, path traversal and final
subgraph expansion now propagate the actual read error through existing error
owners. Integrated both-parser graph, algorithm, typed-direction and failure
race controls pass (16.528 s). Invalid worker fixtures were corrected first:
namespaced mutation admission and valid node identities are required; the
existing list-valued allSimplePaths result is compared directly, without
inventing a new path result contract.

The label-seeded scan plan also omitted relationship types from its actual
request. Its public regression fails before correction (1.067 s) and passes
in the integrated race population. Paired allocation and CPU controls are
documented in the performance report, including the variable neighbors sample.
No full built-in completeness or final CI pass is implied by this checkpoint.

The credential URL sink inventory refresh changes exactly three source hashes
for reviewed inference methods. Prompt/tool/stream redaction, locking and
lifetimes are unchanged; actual internal observation callbacks do not emit
provider content. Existing redaction and structure race controls pass
(observability 10.333 s, Heimdall 1.839 s). No census/checker rules, exemptions
or security assertions change.


## Retrieval candidate requests and CDC/temporal/log populations

Generic node/relationship retrieval reports the actual prefix returned by each
issued index request, before metadata/decay admission and the final output
limit. Widening retries and completion requests accumulate their own returned
prefixes; vector units are distinct scored node candidates, not ANN visits,
raw vector comparisons or storage hydration. A named vector-query wrapper
retains its existing final-hit/logical-query contract and passes no subordinate
generic counter; a nested independent search resolves its own request counter.
Only exact first-party candidate/scorer implementations are certified; a custom
implementation remains opaque even if it embeds an owned type or reports an
observed inner read. Ordinary requests return before profile formatting.

Public both-parser retrieval, graph/relationship, named-vector, continuation
and provider controls pass. Component controls cover widening, pre-filter
prefixes, completion, provider failure, custom embedded scorers and a real
result-cache hit that performs no repeated index request. Integrated race
checks pass (search 2.337 s, Cypher 10.783 s, nornicdb 2.284 s). Shared custom
replacement assertions retain all earlier graph registrations and add eight
retrieval plus seven CDC/temporal/log entries; the separate copied replacement
fixture is not retained.

CDC bounds count one actual response/request; paged change scans count the
events returned before selector/limit filtering. Temporal reads distinguish
current-label populations, historical visible populations and the actual
optional temporal lookup, including misses before fallback. WAL diagnostics
count entries handed to the visitor before database/transaction filters, with
early limits counting only visited entries. They claim logical API work, not
physical storage-internal setup, and do not alter snapshot/retention or WAL
formats. Valid public baseline tests fail on absent observations. Recovered
worker tests first needed field-name corrections and a non-overlapping healthy
interval; those invalid fixtures are not presented as product defect evidence.

The existing three-mode temporal/CDC/log test owner compares plain, explicitly
observed and PROFILE results with parameters and checks both the collector and
rendered plan. It is shared by all three families; it does not duplicate the
two-mode unordered-catalog contract or replace ordered vector score checks.

An additional public Go API regression establishes that GetEdgesForNode
previously swallowed either directional read error and could return partial
success. It now propagates the original cause and publishes no partial list.
Individual, listed and adjacency edge results share one stored-image
projection, preserving fields and ordering. No API signature, stored format
or transaction-conflict semantics change.


CI e5f8 found the numeric-input inventory still pinned the old apocNodes
body. Complete old/current source review confirms that the graph/list carrier
switch and numeric rejection are unchanged; only the string carrier's same
full-image lookup now uses the existing query-node owner. The factual hash and
review rationale are refreshed for that declaration, retaining all 69 frozen
nonconsumer entries and every scanner assertion. Public argument/path controls
and the unchanged numeric ownership test remain required before this checkpoint.


## Operational metadata/provider boundary checkpoint

Database identity and statistics share readGraphCounts for the actual two
scalar requests. Named index management receives the caller context and reuses
the schema-index catalog acquisition, after argument validation. awaitIndexes
and cache clearing perform no graph acquisition in their existing bodies;
cache-clearing effects and void-row continuation stay unchanged. Private
unused context-discarding config/connection forwarding methods are retired.

Optional identity, system-name, settings, connection and permission providers
report their actual invocation as opaque. The private permission result also
identifies injected versus reviewed role-derived checks without another
closure. Absent providers invent no read. Public tests cover custom callbacks
with observed inner work, actual invocation counts, connection ownership
filtering, scalar counts, catalog populations and rejected arguments. All
registration replacements keep their opaque policy.

The initial parent identity fixture omitted ServerIdentity and therefore did
not satisfy the complete optional capability; it was corrected and then failed
validly on the missing observation. Initial PROFILE SHOW SETTINGS expectations
conflicted with the existing explicit result-column rejection test. The
qualified tests preserve that supported boundary: ordinary observed SHOW uses
the same resolver owner, CALL listConfig supports PROFILE, and rejected
PROFILE SHOW performs no resolver call. This does not mark the deferred
command-metadata portion of NOR-335 complete.

Focused operational race checks pass (8.097 s). Integrated all-PROFILE,
argument, void, numeric-input and undeclared-SHOW controls pass (20.459 s).
The integrated security structure test then identifies only executeShowSettings'
changed source hash. Complete review confirms unchanged selection and all
DisplayValue redaction projections; the internal callback emits no content.
Factual source-pin refresh, original SHOW redaction/input-ownership tests,
path argument controls, numeric inventory and lexical field population pass
under race instrumentation (Cypher 6.313 s, observability 10.470 s). No
analyzer rules, exclusions or frozen populations change.

Transaction metadata remains opaque pending its arbitrary Go fmt callback
contract; these eight declarations do not silently certify that ninth family.
The remaining native procedure inventory is 39 names after this batch;
optional plugin exports are a distinct admission surface. Original fulltext
postings, relationship-index cost qualification, public vector cache evidence
and the earlier unclassified HNSW miss remain separately tracked.


## GDS projection mutation owners

Project/drop now join reviewed native read-observation coverage. Their population
is the complete delivered storage stream, not just projected entities. A single
lock-held catalog-read owner serves lookup, project admission, drop preflight and
delete-loop reads. It accepts the already computed key to avoid reconstructing
long catalog keys on ordinary calls. The lock-taking lookup still owns its
not-found error contract; it is not a compatibility forwarding alias.

Two public fail-first procedures now pass both-parser exact row/count tests;
empty streams are known zero, and repeated drop names preserve one output row.
Preflight remains all-or-nothing. The shared authorization owner reports only
actual custom resolver invocation; an observed inner read does not certify the
entire callback. Custom built-in/user replacements still receive opaque policy.
Existing cancellation, catalog and broader permission race controls pass in
78.170 s. No analyzer/checker rule, exemption or source-pin update was needed;
Foundation lexical guard passes unchanged. Remaining native inventory: 37 names.


## Persisted custom-procedure catalog owners

Five native administration/listing names are now reviewed: `apoc.custom.installProcedure`,
`declareProcedure`, `dropProcedure`, `list` and `show`. Installation compiles a
stored statement; it does not invoke it. Subsequent user-procedure invocation
retains opaque policy. Private registration now uses the canonical typed spec
owner directly, preserving signatures, arity, descriptions and administrator
requirements; the unused registration forwarding helper is retired.

Identity lookup and reload/list populations share their acquisition owners.
Every actual point attempt is counted, including missing keys and repeated exact
reads around legacy-key normalization. Listing counts all acquired records before
invalid/noncustom entries are filtered. Direct catalog persistence reports one
actual native mutation request; the canonical storage-membership owner prevents
counting the same request again through a transaction adapter. No native mutation
semantics or persisted format changes.

An administration procedure executing outside the clause pipeline now records
its actual `ProcedureCall` through the existing runtime-invocation owner. A call
already owned by a clause keeps that record. This removes the incorrect generic
"write statement storage accesses" label from a fully observed reviewed call;
opaque handlers still report their actual callback and cannot become certified
by an inner read. It does not certify arbitrary nested DDL; that boundary remains
in the pending dynamic/periodic review.

Fail-first public both-parser list/management tests failed with the opaque/generic
write labels. They now assert exact ordinary/PROFILE results, actual identity
attempts and create/update/delete requests, persisted outcomes, legacy migration
and installation-versus-invocation behavior. All PROFILE plus custom/catalog
regression race controls pass in 17.600 seconds. Shared public replacement-policy
controls include all five names. The lexical inventory removes exactly the retired
helper's factual row; no extractor/rule/exemption change is made.

Ordinary public listing controls, M2 Max / Go 1.27.3 / native in-memory defaults,
query result cache disabled, three fixed 1,000-call samples with CPU/memory capture:

| Catalog records | Baseline median ns/op | Candidate median ns/op | B/op ranges (before / after) | Allocations (both) |
| --- | ---: | ---: | --- | ---: |
| 0 | 26,770 | 26,270 | 9,320–9,322 / 9,321–9,322 | 113 |
| 128 | 280,940 | 280,897 | 302,663–302,669 / 302,661–302,669 | 4,494 |

This supports unchanged ordinary allocation counts and overlapping byte/timing
samples for this listing workload, not every catalog mutation's performance.
Evidence: `/tmp/v150-profile-custom-catalog-{before,after}.{txt,cpu,mem}`,
`/tmp/v150-profile-custom-catalog-failfirst.log`, and
`/tmp/v150-profile-catalog-all-profile-race.log`.
Remaining native names: **32**, plus explicit optional-plugin admission inventory.


Catalog integration controls additionally exercise dropping one procedure while
reloading two survivors, including a legacy catalog key: four point attempts,
two delivered catalog records, one create and two deletes (nine total hits).
A pipeline installation preserves its incoming row and counts the same three
identity attempts plus one native create, without counting the transaction
wrapper twice. Both surviving procedures remain callable after reload.


## Query statistics and shared schema acquisitions

Six statistics registrations now share reviewed read observations. The collector
state is one lock-held acquisition (one row/one hit), and query retrieval also
counts its complete record population before maxInvocations limits details.
Graph-statistics sections report delivered nodes and relationships, actual token
ordering requests and the index/constraint/property-type catalogs they acquire.
The schema helpers are reused by db.constraints, SHOW CONSTRAINTS and ordinary
token listings; token ordering adds one logical metadata request independently
of the graph population. Empty catalogs remain observed empty requests. Both
implicit lookup indexes are actual index catalog entries on an empty database.

Public fail-first controls reject the former opaque/missing observations.
Both-parser lifecycle controls compare separate equivalent ordinary/PROFILE
fixtures; stopped query retrieval covers maxInvocations:0 without changing the
record population. Aggregate alias controls preserve its existing system-view
acquisitions: direct system retrieve returns early, while the aggregate alias
still reads the empty system graph and three schema catalogs, with no query
collector request. Exported replacements remain opaque and claim none of the new
metadata records. Integrated all-PROFILE/R2/HNSW race checks pass (Cypher 18.011 s,
search 5.654 s). No checker or census rules/exemptions change.

Remaining native procedure inventory: **26 names**, plus effective optional-plugin
admission inventory. Fulltext preparation cost, relationship-index cold/retained
qualification and final exact-head CI remain independent gates.


## Final native procedure dispositions

The remaining 18-name batch is resolved in scope: eight I/O handlers and seven
dynamic/periodic handlers now report their canonical executed readers; two
suppression handlers report actual suppression requests and canonical service
refreshes. Their public controls include both parsers, success/refusal, exact
populations, custom replacement opacity, transaction commit and rollback.
Schema DDL keeps the reference SCHEMA runtime's explicitly unknown access total;
a data-statement wrapper may not execute schema writes. Schema-mode procedures
retain their existing capability.

The operator explicitly approved retaining tx.setMetaData's unobserved access
metrics on 2026-11-12. Its supported caller values may invoke fmt.Stringer,
fmt.Formatter, error.Error and CallerCopy. The stdlib formatter exposes no hook
for observing those nested dispatches. A worker proposal to declare them known
zero was rejected: a caller method can perform storage work. The existing opaque
registration is retained, with no narrowed values, changed size/copy semantics,
new formatter implementation or Go API migration. This is a documented metric
limit, not a known-zero assertion or a required unimplemented product correction.

The optional 814-export admission inventory is completed registration-policy
evidence; it is not an additional pending 814-behavior test suite. Final-head
CI/censuses remain the integrated verification gate.
