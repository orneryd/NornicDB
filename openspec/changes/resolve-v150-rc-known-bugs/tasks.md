# v1.5.0 RC known-bug corrections

## Current checkpoint

- [x] Review authentication/embedding endpoint inventory drift after the main
  rebase; preserve scanner rules and pass the full observability race package
  (193.546 s). See [source review](url-inventory-review.md).

- [x] Qualify actual public fulltext cache hits, detached caller mutations and
  metadata/node result images on both parsers (race: 7.044 s).
- [x] Consolidate candidate hydration and retire twelve test-only collectors;
  reject the initial extra-allocation form and retain baseline allocation counts.
- [x] Pass integrated public traversal/cache race checks (12.696 s) and review
  the exact twelve-member lexical retirement delta (assertion: 2.184 s).
- [x] Integrate and qualify PROFILE expression/predicate source identity, lazy
  CASE, single-evaluation grouping/UNWIND and shared program publication.
- [x] Qualify physical count/read/fused row, GDS scalar/catalog and token-listing
  observations, with public custom-registration and allocation controls.
- [x] Qualify definition/property-listing procedures and runtime-statement node
  images, including native/Fabric transaction admission (race: 39.179 s).
- [x] Consolidate token reads, propagate graph-statistics population errors,
  and preserve measured ordinary allocation counts (race: 11.433 s).
- [x] Finish remaining built-in procedure/native-callback and actual write-access
  scope; custom callbacks, SCHEMA runtime and approved tx.setMetaData limit
  retain explicit unknown metrics.
- [x] Qualify the schema catalog and vector-state observations and reproduce/fix
  the CI registration-restoration contamination (race: 2.743 s / 2.433 s).
- [x] Complete the approved native callable compiler policy migration and its
  public pure-bundled/custom-replacement, invocation-phase and callback controls.
- [x] Clear the original two census findings with the approved decision-flow
  correction; CI confirms their retirement.
- [x] Correct and publicly qualify embedding-reset masked failures, namespace
  isolation, partial progress and pending-index publication (race: 5.503/2.583 s).
- [x] Review and refresh the complete CI-generated census artifacts without
  changing rules, exemptions or writer populations.
- [ ] Verify final-head census regeneration and full test-only CI.
- [x] Propagate embedding-regeneration failures through GraphQL and verify its
  successful and refused public paths (race: resolver package 18.765 s, HTTP
  controls 11.692 s).
- [x] Complete checkpoint CI at `5b02a5038`: all seven actual test shards,
  both censuses, lint and aggregate make test pass. Later local source is not
  covered by that pass; final-head CI remains required.

Named reuse/consolidation decisions: [candidate review](candidate-review.md).

## Scope and evidence

User-supplied lab report dated 2026-11-10 23:20Z targets `896fa3a1f`, with comparison build `b6d8e762a`. Its original evidence is referenced as `sig/handoff/v150-regressions-20261110.md`, `sig/handoff/known-bugs-v150.md`, and `lanes-20261107/bm25reg/uncached-cost.md`; those external lab artifacts have not been retrieved in this checkout. Measurements in that report are not fresh local qualification.

This branch starts at `acf7426b8604d9640783150c97138b82794eadc5`, which includes #1787 and #1788. Both are merged, despite the older report statuses. Reproduce remaining behavior before editing and verify existing fixes on the integrated candidate. No active remote release branch exists at creation; this draft targets main with milestone v1.5.0, following the current release-fix PRs.

This initial commit records scope only; it contains no runtime fixes and claims no verification pass. Keep existing Linear issues and their reproduction comments as tracking owners; do not create duplicate tickets. Do not reset the dogfood database or weaken cache isolation, transaction semantics, or memory limits to improve measurements.

Excluded features: NOR-286 graph continuation, collection access control #935/#936/NOR-12, and NOR-401 parity extras.

## Acceptance inventory

| Report item | Tracking | Required correction | Verification | Current state |
| --- | --- | --- | --- | --- |
| 1 | NOR-414 | Residual traversal/link-read cost after #1788 | Exact result sets and paired collection/document/link benchmarks through 16K target links; record ns/op, B/op, allocations and adjacency reads | Partial: typed estimates and the existing relationship-key owner pass native public controls. The exact 1K/16K stored-link lookup measures about 0.14/0.16 ms on both parsers. Paired 172K-node collection/document workloads reduce bytes/allocations and latency against frozen main; later MATCH-local statistics and deferred hydration measurements supersede the earlier ~0.31 s result. The recorded historical generated-corpus comparison improves all eight arms versus b6; it is scoped evidence at its named source revision, not a private application replay or a fresh full workload at this head. See the workload table in the performance guide. |
| 2 | NOR-68 | APOC JSON backspace/form-feed byte compatibility | Exact output bytes for backspace/form feed and mixed escaped strings | Integrated focused pass: exact short-control and HTML JSON output tests pass independently with Nornic and ANTLR parsers (0.988s / 1.232s). |
| 3 | NOR-386 | Implicit default vector index across transaction/embedding publication | Open read transaction before embedding save; verify intended empty/retryable outcome rather than missing schema error | Pass in focused public tests on both parsers: pre-publication snapshot returns empty, fresh transaction returns hit; full CI pending. |
| 4 | NOR-407 | db.retrieve after schema statements and indexed where | Paired default/ANTLR benchmarks after schema mutation and indexed filters at 3 and 300 results; exact rows | Partial: namespace-scoped schema publication retains entity caches; public 12.5K-source after-DDL allocation medians fall 768→540 / 794→542 with exact same three rows. Hybrid branches/widening now evaluate each candidate predicate once per request; the generated 12.5K-source correlated-predicate benchmark falls from 165.1→119.9 ms, 123.3→89.4 MB and 1.513→1.063 million allocations. Unfiltered allocation control is unchanged within observed variability. The published disk-backed sequential-write fixture now qualifies both parsers at n3/n300, with and without the document index: bytes and allocations improve in all eight arms. The default cutoff returns about 167–170 results for n300. Cold/warm admission is measured separately in the performance guide; workload, measurements and limits are in the performance guide. |
| 5 | NOR-413 | Procedure-containing CALL reads avoid unused stored vectors | One admitted statement image policy; native defaults full, reviewed bundled metadata handlers opt in; scoped native function inputs reuse canonical type facts | **Focused pass**: automatic/explicit/Fabric, native callback vectors, cached registration replacement, mixed vector-search rows, poisoned chunks and allocation bounds. Full CI pending. |
| 6 | NOR-307 | Result-cache hit copy amplification | Cached fulltext/vector/list results; preserve mutation isolation while measuring returned payload and allocation costs | Partial: one canonical copy owner reuses tracked slice headers. The 21,748-row cache hit drops 109,032→87,283 allocations and 12.23→11.70 MB; 25 metadata/vector controls and ordinary hits reduce allocations too. Public cache/overflow/alias/cycle/nil race controls pass. Requested vector payloads remain detached. Private application-query qualification is unavailable; metadata hydration has separate public coverage. |
| 7 | NOR-27 | Indexed property of indexed list element returns null | Assert page[i].ends[0] returns the exact nested value on both parsers | Focused pass, both parsers and normalization modes; CI pending |
| 8 | NOR-27 | Index/slice after collect(DISTINCT x) rejected | Assert aggregate indexing and slicing results on both parsers | Focused pass, both parsers and normalization modes; CI pending |
| 9 | NOR-27 | Empty FOREACH touches previously deleted entity | Assert empty loop is a no-op; nonempty loop retains required errors | Focused pass using the canonical per-clause deleted-entity owner; CI pending |
| 10 | NOR-112 | CDC earliest/query pruning race | Controlled prune between earliest and query; establish and test identifier lifecycle contract | Focused pass against reproduced Neo4j 5.26.30 Enterprise behavior: earliest marker survives pruning, ordinary event cursors still expire. Both-parser HTTP/router tests reopen the persistent database and reuse the pre-restart marker; TCP Bolt covers pruning and fresh explicit transactions. Exact retained events, expired ordinary cursors and foreign markers are checked. Focused race checks pass; integrated CI pending. |
| 11 | NOR-399 | Ranked retrieval continuation ends at rerank window | Exhaust cursor beyond rerank window; exact complete hit set without duplicates | Integrated focused pass: progressive post-fusion windows, stable first admission, once-per-query provider decisions; CI pending |
| 12 | NOR-401 | Unbudgeted rerank exceeds provider limit | Bounded fake provider reproduces refusal; verify accepted batching/budget and ranking behavior | Focused Voyage package pass: API/model-aware request budgets, global index/order merging, Unicode and refusal controls; CI pending; parity extras excluded |
| 13 | NOR-335 | PROFILE/EXPLAIN metrics and plans do not reflect execution | Known operator populations and actual access counters; assert executable-plan provenance and actual versus estimated distinction | Partial: observed pipeline/read counts and source identities pass focused race controls. Physical routes outside the clause pipeline remain explicitly unobserved; complete coverage and final CI remain pending. |
| 14 | NOR-160 | Typed adjacency expands all relationships | Canonical typed adjacent-edge request; generated high-degree traversal with snapshot and policy controls | Focused pass: both parsers/transaction modes, direction/type-union/self-loop, historical and buffered reads; explicit-step medians 789,770→319,492 ns, 434,721→170,790 B and 6,924→1,947 allocations. Full CI pending. |
| 15 | NOR-42 | UNIQUE relationship-property lookup uses its index | Constraint-owned bounded equality index, existing snapshot-cut proof and authoritative historical/pending readers | **Focused pass**: public auto/explicit, update/delete/recreate snapshots, durability wrappers and allocation ladder; isolated benchmark and retained-memory limits documented. CI pending. |
| 16 | NOR-111 | IF NOT EXISTS constraint rebuilds existing constraint | Repeat existing constraint creation; assert no redundant validation/rebuild and preserve conflict behavior | Focused pass: canonical admission precedes stored-data validation; repeated named/unnamed unique, existence and type declarations retain indexes and avoid population allocations. CI pending. |
| 17 | NOR-410 | Deprecated memory-limit overhead per transaction | Isolate configured compatibility behavior and profile transaction admission; compare with setting absent | Focused pass: repeated configuration loads share immutable localization catalogs; config package independently passes. Warm allocation drops from ~69.6 MB to 8–10 KB per legacy load. Correlated aggregate profiling also removes unused memory estimation (326.10ms→17.62ms on the generated workload); The fresh-executor/read benchmark reproduces the old warning cost (336.64 ms, 348.25 MB, 4.628 million allocations) and measures 194.11 µs, 126,553 B and 694 allocations after correction. Canonical-setting timing overlaps; full wire throughput is not claimed. |
| A2a | NOR-413 | Uncached BM25 whole-graph and chunk-vector hydration | Assert postings/label-scoped acquisition and no unused vector decoding with unchanged top results | Partial: ranked acquisition streams declared labels instead of the whole graph and metadata queries avoid vector decoding. Native-consumer full images now pass focused integrated controls; dialect-compatible postings qualification remains pending. |
| A2b | NOR-413 | Vector-hit and subquery repeated full hydration | Assert only required payload hydration across explicit and auto-commit execution; unchanged ranked results | Focused pass: query-node counters, stored-vector negative controls, native callbacks and sidecar/inline/pending/buffered entity identity. Large-workload timing remains pending. |
| A1 | NOR-413 | Bounded node-cache eviction repeats vector decoding | Repeated distinct query-cache misses with explicit node-cache eviction; retain the memory bound | Focused pass: both parsers, three query misses after eviction, metadata reads and zero full-image reads; generated four-node proof establishes the route, not the reported 172K-node timing. |
| T1 | Test defect | Embedding-free wrapper tests depend on leaked settings | Run each affected test alone and together; expose required engine capabilities and restore settings | Focused pass: spy exposes its underlying native engine; unsupported-engine case restores its own WAL setting. |
| T2 | Test defect | Prune progress assertion confuses time-budget expiry with no progress | Repeat layout test and assert actual progress independently of budget outcome | Already corrected by #1785: repeated standalone and contended runs pass; no redundant edit retained. |
| T3 | Test defect | Write-behind filtered scan fixture races flush | Deterministically control fixture publication/flush; verify overlay and flushed paths | Focused pass: fixture uses existing manual-drain owner; exact overlay/flushed assertions retained. |

## Unconfirmed observation

First vector search after index build: reported single-sample 2.2s versus 39.5s. Determine overlap with A1/A2b before treating this as a separate defect. No independent fix or performance claim yet.

## TODO

- [x] Fetch current main and reconcile #1787/#1788 merge state.
- [x] Preserve every supplied report item and test defect in this inventory.
- [ ] Retrieve linked Linear reproduction comments and available lab fixtures.
- [ ] Reproduce each remaining product defect against the frozen base; retain failing evidence.
- [ ] Implement canonical-owner corrections; preserve detached result ownership and bounded memory.
- [ ] Verify each row through its public path, including both parsers where applicable.
- [ ] Profile performance workloads separately from full test runs; compare identical fixtures and exact revisions.
- [ ] Review duplication and document every finding; unavailable remote retrieval is not a pass.
- [ ] Run affected-package checks and full make test / applicable release verification on integrated head.
- [ ] Reconcile Linear statuses and publish exact-head evidence before marking ready.

New findings belong in a new addendum rather than silently changing the historical report. A merged fix, historical measurement or pending worker report is not proof of current-head qualification.

## First correction: fulltext query-node hydration

Reproduction `TestFulltextQueryLeavesStoredVectorsUnread` failed on the base in both auto-commit and explicit transactions with an embedding-chunk decode error, despite needing only text and node metadata. The fix selects the existing query-node storage policy for the fulltext scope, including population, incremental updates and certified retained snapshots. The full-read snapshot API remains available and the test proves it still reports the poisoned vector. Existing population test observers now instrument the options reader too; all original assertions remain.

Focused results on the integrated working source: Cypher `TestFulltext|TestQueryNodeScans|TestNOR413` passed (5.822s); search fulltext tests passed (1.707s); storage ReadSnapshot tests passed (2.000s), all with `-tags=noui,nolocalllm -count=1`. Heavy suite/census deferred to CI as requested. No throughput/latency qualification yet. Remote graph retrieval still fails HTTP/HTTPS transport; duplicate review is source-based and incomplete, not a checker pass.

Cleanup: removed 31 obsolete binary artifacts, 95 prior-work archive entries and shared Go build cache before new test work started. Dirty/unmerged historical worktrees retained for preservation review; active source, current evidence and running checker relay preserved. Docker has no containers and only the Go toolchain plus an untagged Neo4j image; no task-owned image identified for removal.

## Managed-vector snapshot admission

`TestManagedVectorAdmissionPreservesPinnedSnapshot` reproduced the missing-default-index error after pinning a transaction before embedding publication. The corrected admission checks current committed storage only when the pinned snapshot has no managed vectors. If current storage has the population, the old snapshot returns empty without querying newer vectors. Missing data is still a missing-index error; stale service entries are not authoritative. Both parsers pass the public pinned/fresh transaction regression and the selected vector procedure/input checks (1.717s). No storage-format or public API changes.

## Deprecated setting warning cost

Integrated DeepSeek commit `be9795c4f684f7f0d8ce8e3d891ccc00b8dc8b88` after source review. Immutable localization catalogs are initialized once; each warning still resolves its own language and renders independently, and mutable configuration is reloaded as before. The worker reproduced ~69.6 MB per legacy load before the fix and ~8–10 KB afterward; its config/race/vet checks passed. Parent independently ran the whole config package on integrated source (0.570s, pass). These are configuration-load allocations, not end-to-end transaction throughput. The correlated-subquery accounting observation in NOR-410 comments is a separate pending performance defect and must not be considered fixed by this warning correction.

## Test isolation corrections

Integrated worker commit `697c6e07e25d7656f338a3fa781280627b21ff29`; parent independently passed the selected embedding-free, overlay and prune-layout tests (6.964s, noui/nolocalllm). Four wrapper tests previously failed alone because their spy hid the native write authority. Overlay assertions raced the adaptive flusher after an explicit flush; the existing manual-drain test owner now controls it. The reported prune assertion is already corrected by #1785 and was not edited. A further test-state leak was found (62 EnableWAL/defer DisableWAL pairs); correction is in progress, so test-isolation work is not fully closed.

## Repeated constraint creation

Parent took over after the bounded worker failed on a JSON tool-response parse. Its initial observer fixture hid native transaction capability, so that proposal was not accepted as product evidence. A native public regression instead reproduced 12,751 allocations for a repeated existing constraint (256 nodes) versus 39 for an ordinary read on ANTLR. Both parsers now use 161–191 allocations across the tested named/unnamed uniqueness, existence and type declarations and retain the original index object.

Constraint creation reuses the schema owner's existing admission policy before stored-data validation. The actual publication still rechecks admission; the preflight is not a schema-name reservation. Common validation and scalar uniqueness refresh moved to the canonical creation owner. Property-type creation uses its existing admission policy too. Selected Cypher schema/constraint checks passed (4.719s) and affected storage schema tests passed (2.371s), with noui/nolocalllm. No ns/op or throughput claim is made from these allocation tests.

## CDC retention contract: observed reference, then correction

User explicitly approved the pinned local Enterprise evaluation. Ran image `neo4j:5.26.30-enterprise@sha256:8befcd4e0f48ebd34c58ae071c3e960ca0479675d1046ec35580f8a1dd19891c` with synthetic data in disposable tmpfs, 128KiB rotation, keep_none retention, preallocation off, loopback API and bounded memory/CPU. An HTTP LOAD CSV barrier paused the query between earliest and query; checkpoints pruned the old files while it waited. The chained query and a fresh read using its original earliest token both returned the currently retained events. In five additional controls, a real event token from a removed log returned InvalidIdentifier while that same earliest marker continued to return successive retained transaction pairs. This establishes a live-start marker, not a snapshot-pinning promise.

NornicDB's deterministic public chained-call test failed before the fix on both parsers. The corrected earliest procedure emits a database-bound reserved marker, resolved against the query's own feed bounds; event/current-position tokens retain expiry and future/database checks. Focused CDC checks pass (2.566s). Existing identifier tests now assert semantic populations rather than equating distinct opaque token kinds. No Badger key/payload, WAL format or storage-version change. The tested Enterprise container was removed after evidence capture. Local oracle scripts/logs are retained under the task's cdc-oracle evidence directory. HTTP/Bolt/restart broad qualification remains pending; this is not full Enterprise parity.

## WAL test-state restoration

Reviewed and applied the test worker's 62 setting-restoration replacements and explicit unjournaled-mode selection for wrapper-delegation fixtures; journaling behavior remains covered by its separate native WAL tests. Focused wrapper/WAL/embedding-free tests passed independently (5.779s). The worker began an unintended repository-wide Go test command, which was interrupted and is not counted as a pass; the parent took over focused acceptance. Heavy suite and census runs remain CI-owned.

## Postfix, aggregate and empty-loop corrections

Integrated the NOR-27 worker correction and removed its redundant per-item FOREACH validation: the existing clause executor already owns that validation. The three public regressions pass on both parsers with normalization enabled/disabled. Integrated NOR-27/CDC/constraint checks pass (1.787s). The worker's isolated full-package pass is not a current integrated-head suite pass.

## Rerank request budget

Voyage rerank windows now share the existing provider batch owner with embedding calls. Requests respect the API's 1,000-document cap and the documented decimal model limits; UTF-8 byte accounting conservatively bounds the aggregate query/document request cost without truncating documents. Unknown models retain the API count cap and provider-authoritative token limits. Global response indices and ordering survive batching, and later request failures do not publish a partial result. The Voyage package passes (1.896s), including dense Unicode/numeric documents; selected embedding batching checks independently pass (0.406s). NOR-401 parity extras are excluded; cache behavior during reranker installation is a separate pending investigation.

## Cache copy optimization: bounded measured scope

CPU/allocation profiles of the existing 100-row cache-hit benchmark identified reflective row headers and immutable scalar handling. One typed slice-copy owner now handles lists and matrices, preserving aliases/cycles before recursion; existing scalar policy avoids redundant reflection. Caller-copy and public cache ownership regressions pass (1.829s for selected Cypher tests). Three paired benchmark samples on the same M2 Max / Go1.27.3 setup give median query-cache scalar results of 22,937 to 19,412 ns/op, about 23,298 to 18,517 B/op and 715 to 516 allocations; entity rows 40,990 to 37,324 ns/op, 71,329 to 66,550 B/op and 1,115 to 916 allocations. Equivalent smart-cache paths also improve. These are 100-row workloads only, not qualification of the supplied 21,748-row/vector cases. Baseline is 31a4fff52 with the original copier. An earlier profile overlapping a short worker test was discarded.

## New CI and test findings addendum

Run 42946363014 at 7b83fb7f104c9da71e48c4a886d6da1b29894c6e found a context-census Contract A failure in constraint preflight: the new request path reached a background-only private index scan. Private admission now receives and forwards the caller's context; existing accepted context-free schema mutations retain their completion lifetime. Selected storage schema/copy checks pass (1.310s); the blocking census remains for CI to verify.

The ANTLR concurrency test also depended on timer scheduling and repeated token shapes: it failed 18/20 local repetitions. Test-only gated token streams now hold real parses across retirement and prove distinct live generations, state accounting and reader release. Twenty single-CPU repetitions pass, plus 20 race-enabled repetitions across CPU1/4 (8.000s). No production parser hook or third-party runtime change was added. Other CI failures are still being collected; none are waived.

## Fulltext label acquisition and CI fixture corrections

The new public label-scope regression reproduced whole-graph population on both parsers (128 nodes read for 4 matching documents). Ranked fulltext population now streams the declared label union using existing native storage options, deduplicates multi-label membership and preserves dialect admission. Label selection is part of retained-view identity; wildcard ordered traversal retains its shipped order. Focused fulltext and public cancellation/cardinality checks pass (5.604s); search fulltext/cache-installation checks pass (0.940s). A2a remains partial: dialect substring/field/phrase semantics cannot be replaced by ordinary token-posting search without equivalence proof.

The Cypher failures in run42946363014 were reproduced locally before fixture corrections. The stream observer now instruments the actual options-bearing read through the same cancellation owner; all exact stop/publication assertions remain. Cardinality cases now each own a fresh graph, preventing an existing sibling constraint from masking the intended population rejection; rejected creation is also asserted not to publish schema state.

NOR-401 cache-installation regression: the same public search returned a cached unranked response after installing a provider. SetReranker now advances the shared index revision after replacement, invalidating both service and Cypher result keys while unchanged providers keep useful cached results. Install, cached reuse and removal controls pass. The separate CrossEncoder setter consolidation is now approved; its canonical migration is included in the follow-up correction below.

## Progressive continuation windows

Integrated the reviewed NOR-399 worker commits. Reranking runs once after chunk fusion, keyed by the original query; TopK bounds a provider window rather than the complete cursor. First-admitted membership and provider order remain stable across deeper pulls, including filtered, omitted and failed outcomes. One-shot caps remain intact. Parent independently passed selected continuation/rerank/chunk tests on the integrated source (7.172s).

Review exposed a nil dereference for bare callback hits lacking the service-private hydrated node; the new public regression failed with that panic before correction. The content owner now uses the existing metadata reader for those hits. The new phase also checks and propagates caller cancellation while constructing windows, so cancellation after the first provider call publishes no partial page and never invokes the next window. Focused parent controls pass; the worker's isolated 675 passing cases are supplementary evidence, not a full integrated suite result. Two cursors using different retrieval/page milestones can form different first-admission windows; global reranking of all candidates before page one is intentionally excluded by the approved progressive contract.

## Correlated estimator qualification

The worker's public scaling probe confirmed the unused row estimate. Parent retained the narrow aggregate guard and removed the extra predicate/refactoring of already-correct sites. Its wall-clock unit assertion was removed: timing belongs in benchmarks, while exact public-result and enabled-budget rejection tests remain. Focused memory-policy checks pass (5.144s).

An isolated three-sample CPU-profiled comparison of 128 outer rows and a 40,000-entry unused list/map gives medians 326.10ms to 17.62ms, 11,579,000 to 11,574,345 B/op and 95,191 to 95,123 allocations. No overlapping Go work was observed in accepted samples; three earlier overlapping captures are excluded. Expanded cache-copy benchmarks also show lower allocations at 21,748 rows and 25-node metadata/vector controls. Scoped numbers and commands are in docs/performance/v150-rc-corrections.md. This does not close residual traversal, transport or application-workload criteria.

## Additional acceptance findings

Latest pushed449b969 CI passes the complete Cypher shard (22m39s), storage, server and command. Capturecensus exposes one further private application-owner context substitution; applyConstraintLocked now forwards its caller's accepted completion lifetime. Local source corrected, new CI pending.

A new native fulltext callback regression fails on both parsers: metadata optimization removes vectors required by the native consumer. The typed statement image-policy proposal is approved and implementation is pending; the failing test is local and this item remains unresolved. Relationship-index integration is also not accepted: source review found current postings used for a pinned snapshot and a new process-global test hook. The parent stopped that worker and preserved its unaccepted proposal locally for correction and independent proof. Neither is a merge-ready claim.

## Exact property identity and CI inventory follow-up

Public native transaction reproduction confirmed a new integrity defect while reviewing relationship-index reuse: distinct WTF-8 surrogate property names collapsed under encoding/json into the same derived-index key. The second property's duplicate value was acknowledged. The canonical runtime key now uses existing jsontext.Marshal; the public transaction rejects both duplicate directions, permits distinct values and publishes no rejected edge. Exact property-list identity controls cover surrogate units, literal replacement characters, embedded NUL, comma and tuple boundaries. Focused relationship uniqueness/cancellation, schema cache-scope and accepted count-tail controls pass (2.181s). No stored key, payload, WAL encoding or version changes.

The encoder's lexical dependency adds 17 storage callers to the conservative foundation population, alongside the aggregate-input and node-rerank-content owners already missing from the earlier CI inventory. Every added/changed member was inspected; stored identifier encoding remains with jsontext and aggregate parsing remains with the canonical function reader. Passage selection's graphPassage identifier triggers the conservative credential-name candidate, but it operates on selected node text. Per-member reasons and existing behavioral evidence are recorded in the inventory; focused population validation passes (2.102s). No extraction rule, threshold or exclusion changed.

Latest CI's count-tail occurrence shifted six lines because of the schema namespace dependency declaration. The existing successful durable-commit boundary, retained count lock and completion consumers are unchanged; the exact reviewed occurrence is repinned, without adding a lifetime exemption. Complete census verification remains CI-owned. Storage-census stale artifacts now print their regenerated content on failure so they can be reviewed from CI; no stale artifact is accepted as a pass. Artifact refresh is still pending those diagnostics.

## Canonical reranker installation

Approved retirement of Service.SetCrossEncoder is complete; its Go caller and examples use SetReranker with no forwarding alias. Public installation/reuse/removal cache tests pass. Review also reproduced a panic when a valid value-type Reranker contains a slice; the equality fast path now checks dynamic comparability, retaining same-pointer reuse without rejecting valid interface implementations. Focused rerank-installation, plumbing, matched-passage and progressive-window checks pass (0.725s). The typed node-image and adjacent-read API proposals are approved; their implementation and remaining criteria are not claimed complete.

## Admitted node images and relationship equality

The approved ProcedureSpec.NodeImages field defaults to full images for native registrations. Reviewed bundled metadata consumers opt into body-only reads. One statement-owned policy replaces the executor flag and blanket CALL exclusions. Retained grammar sites plus the canonical scoped type walk prove scalar-only native arguments; unknown and graph-valued arguments retain vectors. The fulltext native callback and no-CALL scalar-native repros fail on the old policy and pass on the integrated correction. Public tests retain strict stored-vector, same-constituent, mixed-row equality and existing allocation bounds. Cached registration replacement is tested in both directions. Selected integrated storage/Cypher/ANTLR checks pass (2.160s / 3.914s / 1.602s), with additional reader/search/numeric checks passing (6.783s / 8.254s / 3.009s). The internal/cyphertext package in that latter selection ran zero tests and is not evidence of its population gate.

Relationship lookup reuses the existing bounded constraint index, build, update and value-key owners. The first proposal lost pinned results after a peer update; current-only postings now require the existing namespace snapshot proof before and after read-back, otherwise the historical reader remains authoritative. Public update/delete/recreate, pending-write, durability-wrapper and unrelated-population controls pass in the integrated source. Eager declaration-time preparation retains derived index memory; no unchanged-retained-memory claim is made.

The two removed lexical members are retired CALL/native scanners, replaced by the grammar/type owners. Three new transitive consumers were reviewed individually and recorded without extraction-rule changes. CI b672 passed runtime shards and capturecensus; its only Other failure is the obsolete numeric-wire exception, removed after the helper moved to jsontext. Its generated stored-entity census differed only in three source locations; reviewed CI artifacts are refreshed. These records are not a full verification pass for this new source.

Race-enabled public scoped-alias, registration-replacement, native fulltext and pinned relationship controls pass in Cypher (4.188s). The first paired storage selection matched no tests and is excluded as evidence; corrected storage relationship-property selection passes with race instrumentation (2.356s). The current lexical population gate passes (2.764s), without discovery mode or rule changes.

## Repeated vector-cache misses

TestVectorQueryMissesUseMetadataAfterNodeCacheEviction fails on b672's original image policy under both parsers (one full-image read on the first query). On the integrated correction it passes all three distinct query vectors per parser (1.208s), with the matching document evicted from a one-entry node cache before each request and result caching disabled. Service index construction remains outside the read counters; every measured request reaches the metadata owner and no full-image owner. The four-node deterministic reproduction proves the hydration route, not timings or retained-memory behavior of the unavailable 172K-node lab dataset.

NOR-335's PROFILE defect is reproduced publicly on b672: constant projection reports 100 fabricated hits, a five-node scan reports the final single row, and EXPLAIN reports index seeks without an applicable index. These need actual executor/operator instrumentation, not zeroed counters or another estimate formula. The bounded EXPLAIN applicability correction is delegated; runtime metrics remain pending.

## Typed-adjacency integration and grammar follow-up

Integrated the worker's context-first AdjacentEdgeRead migration, including all affected Go test doubles. The unchanged public transaction allocation control remained red: 764 to 1,788 allocations for fanout 8 to 64, a 1,024 delta for 112 unrelated edges. CPU/allocation profiling locates the added work in per-edge MVCC head lookup. Parent's first namespace-proof experiment passed the assertion but still collected all MVCC adjacency IDs. The retained correction instead reuses the current-header owner through the transaction's same physical view, certified by the existing namespace-cut predicate before/after acquisition. It is faster and allocates less than the first experiment; historical/uncertain cuts retain their authoritative head reads. The dead second directional enum and its uncalled header collector are retired under the approved API consolidation. Focused integrated traversal, historical, cancellation, type/direction/self-loop and visibility controls pass; complete current-head CI remains pending.

CI on 1bca aborts the Cypher shard on special TRIM ... FROM syntax: that grammar alternative has no invocationName. The retained function-site product now holds only named invocation sites, while still walking nested argument functions. The same review exposed local quantifier/reduce shadowing: an outer scalar name incorrectly authorized metadata for an inner graph binding. Public any/reduce tests failed on both parsers/transaction modes before correction. Canonical grammar binder nodes now prevent that inappropriate proof. Focused function-admission/native-input/ANTLR controls pass (1.815s / 0.614s); assertions and checker rules are unchanged.

## Node-image review dispositions

The read-only worker review identified one concrete ownership question and several explicitly unverified dependencies. Parent checked the actual consumers rather than accepting its speculative impact:

- Scoring reads `AllNodes`/`GetNodesByLabel` into caller-owned nodes, and metadata projection acts on that same original image after scoring, without a newer lookup. Public service/exact membership and reread-of-stored-vector controls pass. Storage `TestEngineGetQueryNodeMatchesGetNode` defines the separately stored-vector projection. The additional inline/pending/write-behind review reproduced an actual public membership failure: the bundled Go collection handler compared node read provenance instead of entity identity. Cypher collection names now retain their existing canonical value owner; third-party overrides remain eligible. Both parsers pass exact sidecar/inline/pending/buffered membership and native full-vector controls, with no extra hydration or storage changes. This does not redefine every standalone Go collection helper's representation contract.
- Outer policy resolution precedes execution routing in both entry branches. The anonymous CALL returning a node to a later native function, native nested MATCH and same-ID Fabric controls pass with complete source vectors. No ordering failure was reproduced.
- Exact-source native scalar tests reuse one executor across comments, whitespace and distinct literal variants and retain metadata reads. Missing/unknown proof remains conservative. QueryNodeReader is an optional capability, so an engine lacking it may still read a full image; no unsupported-reader optimization guarantee is claimed.
- Fulltext QueryNodes selects image hydration, not node population. The native fulltext callback test verifies all four expected rows and vectors. Bundled declaration and alias tests verify the registration delegation; retained function ordering is covered at the grammar product.

Graph retrieval was retried and still returns HTTP400 because the configured client uses plain HTTP against an HTTPS port. Source review is current, but there is no clean remote duplicate-check claim.

## Header visibility regression correction

DeepSeek authored a public native-transaction policy fixture without running Go during profiling. Parent executed it against the integrated migration: all three exact record/header comparisons failed (pinned snapshot, pending overlay, historical cut). The fixture's two otherwise-identical relationships differ only in a property used by a documented promotion policy; full records preserve the promoted relationship while metadata headers lose the necessary input or skip filtering. The migration had removed the earlier decline-to-record boundary. Native transaction admission and the explicit visible-at reader now keep record inputs whenever the existing visibility owner requires them. The unchanged regression, direction/type-union/self-loop, write-behind pre/post-flush and historical type/endpoint controls pass (1.148s). This corrects the new local migration before its first push. The independent type predicate is now called directly; its forwarding request method and the unused duplicate endpoint helper are removed.

## Integrated focused follow-up

The affected-package run found one additional migrated test-double defect: server induced-snapshot collection inherited the unimplemented typed adjacency method because its fixture still declared the retired directional methods. The fixture now implements the current and visible-at typed contracts using its one fixed-snapshot kernel, and the failure-injecting wrapper inherits it. Its original cancellation/deadline/closed-storage/read-error assertions remain unchanged; graph collector/endpoint/snapshot controls pass (7.688s).

CI on 1bca passed capturecensus, storage, server, command and lint; Cypher aborted at special TRIM syntax, the Bolt assertion retained the superseded full-image contract, and stored-entity artifacts differ only at one NamespacedEngine source location. The first two are corrected locally with the original failure controls and native full-image negative control. The much larger adjacency API migration will require fresh exact-head generated artifact review from CI; historical artifact output is not current validation.

Current-source focused race qualification: native-image/function/grammar, vector identity, typed-adjacency, relationship-index and server/transport controls completed. The first combined command failed only the newly added parser-matrix test's cleanup, which used t.Context after testing had canceled it; rollback now runs in a defer before that lifetime ends. Its unchanged parser/row contract passes under race (2.488s). The same combined run passed storage (4.277s), server (5.666s), Bolt (2.498s) and ANTLR (1.594s); Cypher's other selected cases had no failures. No race report was emitted. Explicit lexical discovery matches all 4,033 reviewed members after reviewing the two remote typed-request additions, retired old endpoint and shared-call classification; extraction/checker rules are unchanged. This is focused qualification, not a complete package/full-suite pass.

## CI follow-up, CDC lifecycle and repeated predicates

The complete c793 make-test shards found a zero-value semantic validator cache panic, an obsolete directional error-injection fixture, a historical reader incorrectly included in the current-overlay-only structural guard, and a between-request fixture missing its typed dispatch. Parent retains uncached semantic validation when its optional cache is absent, migrates error injection to the actual AdjacentBoth request with exact request assertions, and corrects the between fixture without changing its expected endpoints. The historical reader remains registered through beginMVCCHistoryRead; a new public buffered-update control proves its old cut returns the old type while the current view returns the new one. It must not capture the newer overlay. Focused race results are recorded in the PR update; no failure assertion was removed.

The two new storage-census violations are independently reproduced as analyzer control-flow defects in isolated SSA tests supplied through a temporary Go overlay (0.310s red), without editing checker files. Joined errors are returned unchanged; a later type guard cannot govern the preceding deletion in the same iteration. Proposal and negative controls are awaiting the user's required permission, so those CI violations remain blocking. Generated artifacts are not refreshed as a substitute.

DeepSeek CDC wire/restart tests were independently reviewed and integrated. Parent strengthened HTTP status checks and explicit manager cleanup. Closing the manager first closes the storage before the database can mark a clean shutdown; the fixture now follows database-owned shutdown first and then releases the manager. Both-parser HTTP/router restart plus transaction-snapshot race controls pass (14.525s); Bolt/CDC/semantic focused race checks also passed. This proves the same pre-restart marker reads the exact retained events after actual persistent reopen, and ordinary expired/foreign markers remain errors; no external CDC feature parity claim.

A newer NOR-407 comment supplied the duplicate predicate reproduction. Parent reproduces two/four evaluations per candidate via public hybrid Search. Request-scoped first decisions now converge lexical/vector branches and widening rounds without retaining graph images or cross-request decisions. Cancellation/error/database identity and fresh-request controls pass under race (1.847s). The existing generated 12.5K-source public retrieval benchmark shows fewer allocations/bytes and lower time; the unfiltered control stays within observed allocation variability. The exact original 3/300 lab fixture has now been retrieved from its Linear comment and still needs independent qualification.

The count-statistics DeepSeek investigation failed with ConnectionResetError after producing an unfinished history-budget prototype. Its proposal is preserved in its isolated worktree and is not integrated: limits, complete caller migration and fallback orientation have not been established. A bounded budget alone could restore the very expensive end-seed route; no benefit or correctness claim is accepted from that proposal. The parent retains this remaining investigation.

## Predicate convergence acceptance review

DeepSeek reviewed c2ca's committed decision owner and found no established runtime regression, but could not read beyond its five approved paths and graph retrieval was incomplete. Parent completed the questioned boundaries directly in current source. `rrfHybridSearch` resolves `vectorPipelineForQuery` before starting the BM25 goroutine, not after it; its returned context comes from `acquireClusterSnapshot`'s WithValue, with no branch-local cancellation. Graph dispatch calls Search again for its node phase; relationship admission uses its distinct RelationshipWhere owner and unique ordered relationship candidates. Query chunks deliberately invoke fresh Search requests. The new verdict owner is for a node Search invocation, not a cross-chunk/relationship cache.

Cancellation remains discoverable through errors.Is(context.Canceled/DeadlineExceeded), and an undecidable/canceled filter remains final through the existing filterEvaluationFailed owner instead of triggering a replacement candidate set. The joining path also retains an independent predicate failure. No error-message or transport mapping branch was added. The nil guard is enforced by admitsNode before its private where call; it is not a second verdict owner. EntityIdentity includes database, as inspected in storage and proven by the two-database control.

The existing public test already invokes Search twice with the same context/options and resets its call counts between them; stale cross-request verdicts therefore fail NotEmpty/call-count assertions. Parent expanded that same fixture to both lexical-entry HNSW and parallel CPU brute-force, asserting the actual selected strategy. Both all-admitted and selective widening cases pass under race together with cancellation/error controls (1.910s). This exercises the public concurrent branch without a new production injection API. Component tests separately control an in-flight waiter, since ordinary scheduling alone does not prove the waiting state.

The verdict map retains O(examined candidates) identities/decisions for one call, never images or vectors. Generated paired B/op/allocations improve materially; peak retained memory and the original lab's distinct workload remain unqualified. The benchmark port's first proposal substituted in-memory storage, chunk vectors, bulk setup and a lower score cutoff; parent rejected those as an exact lab comparison and requested restoration of the original Badger/named-vector/sequential-write/default-cutoff/query-cycle shape before measurement. No performance claim from that unexecuted port is accepted.

## Second c2ca CI correction

The Cypher shard reached two more old expectations before a migrated fake panicked. The db.labels case still asserted that every procedure forces full images; the approved typed contract explicitly declares this bundled metadata reader metadata-only. Its exact title/embedding-metadata row assertion remains, now with positive query-node and zero full-node assertions. Independent native scalar and fulltext-native procedure tests still assert complete vectors and pass with this case. The relationship-MERGE branch fake still implemented retired GetEdgesBetween, so its nil embedded Engine supplied the new ReadAdjacentEdges method and panicked. The fake now handles the actual AdjacentBetween request, records it and asserts exact endpoints/mode on the error path; unused point-lookup fake API and initializers are retired. Original merge/error/collision assertions remain. Focused combined race checks pass (2.891s). The c2ca full run is still not a pass; census remains unchanged and blocking pending the existing source-flow correction decision.


## Completed c5d CI inventory and focused corrections

Run 42980480178 completed every shard. Storage, server, command, other, lint and capturecensus passed. Cypher failed six root tests (including 1,152 generated USE leaves), without a panic. Storagecensus retained the same two pending control-flow findings; their generated output is byte-identical to the preceding run. Build/coverage were skipped by the selected test-only dispatch. The aggregate is not a passing test result.

The endpoint fixtures now count both full and metadata reads in their existing shared beforeRead observer. Metadata reads delegate to the actual QueryNodeReader rather than secretly taking the full-image path. Their cancellation, retry, exact context and result assertions remain. The canonical transaction read exit preserves successful nil collections after its existing error/cancellation guards, retaining nil-versus-empty behavior without a second reader. Selected integrated race controls pass (7.510 s). The USE fixture comparison proposal is still being corrected and independently qualified; no generated population or assertion is waived.

The narrow seed-cost correction passes the parsed type set to both existing endpoint statistics requests. Public exact-result/request tests reproduce the wrong seed choice with the old requests and pass after correction. Untyped, type-union and pinned controls remain. Three-sample 351-document/800-unrelated-edge measurements reduce 73 allocations and 4,578 median bytes per read, with overlapping latency ranges; complete methodology and limits are in docs/performance/v150-rc-corrections.md. The abandoned history-budget prototype is not retained.


## Published retrieval workload qualification

The corrected lab port preserves disk Badger, named vectors, sequential write history, default cutoff and rotating queries. Parent reviewed the complete final fixture and independently measured all four index/page-size shapes under both parsers, baseline versus only the request-verdict correction. All pages pass exact membership/accounting/order controls. Allocations and bytes improve in every arm; actual n300 pages contain approximately 167–170 results under the unchanged cutoff. The final fixture body also passes an integrated-head smoke execution. Methodology, per-arm medians and cross-process HNSW/CPU-attribution limits are recorded in docs/performance/v150-rc-corrections.md. This closes the warm generated/published WHERE qualification slice, not cold admission or full application replay.


## USE retained-contract fixture correction

- [x] Compare all execution/effect/image/routing facts instead of identity-bearing parse pointers, preserving each source's callable and executable-scope products.
- [x] Preserve literal/comment/pre-existing USE bytes through declared insertion windows; compare graph-reference callable names and complete arguments, not names alone.
- [x] Retain 1,155 generated statements and every consumer, plus 25 fact-mutation rejection controls. Selected spelling/Unicode/comparator race checks pass (2.605 s); the full generated population and independent negative consumers pass under race (8.677 s). No runtime semantic assertion, analyzer rule or population is weakened.
- [ ] Re-run integrated CI on the next pushed candidate; these focused results do not certify a full-suite pass.


## Reviewed transaction projection source pin

The b85 Cypher run exposed one additional frozen numeric-input inventory mismatch for projectTransactionRead. Its only production change is the successful-nil guard after the unchanged raw-error and cancellation checks. The generic constraint and typed Node/Edge slice dispatch are unchanged; no raw scalar/list argument reader was added. Parent reviewed the full owner and its callers and repinned this existing boundary with the specific nil-result rationale. No analyzer/test rule, discovery population or new exclusion changed. Focused source-pin and transaction projection/error/cancellation controls pass under race (3.571 s).


## Deprecated-setting public admission qualification

The existing fresh-executor/read benchmark reproduces 336.64 ms / 348.25 MB / 4,627,890 allocations with only the original per-warning catalog call restored. On the committed shared owner it measures 194.11 microseconds / 126,553 bytes / 694 allocations; the equivalent canonical-byte setting measures 184.42 microseconds / 122,587 bytes / 574 allocations. Three ten-operation samples per arm are isolated from other Go work, and the temporary config overlay is restored with a checked hash. This qualifies executor construction plus one successful read, not full HTTP/Bolt or COMMIT throughput; details and commands are in the performance guide. Existing warning observability, locale and concurrency assertions remain.


## Native stored-link qualification

The published 1K/16K target-degree workload passes exact stored-claim assertions on the native candidate and no longer reproduces the older 11.7 ms residual. The initial test wrapper hid the optional native relationship equality interface; parent preserved native capabilities and kept benchmark execution directly on the namespace store. The functional meter has a positive three-entry expansion control and a zero-entry keyed lookup on both parsers at both sizes; selected race validation passes (20.526 s). Existing NOR-42's pinned relationship owner supplies the improvement, so no redundant production patch is added. Candidate medians and baseline/CPU limits are in the performance guide. The larger collection/document shapes are still pending.

Duplicate/ownership review: the specific high-degree claim corpus differs from the existing same-endpoint equality fixture in endpoint distribution, sequential history, after-load constraints and six projected properties. One new corpus builder is shared by its test and benchmark; lifetime tracking, parser selection, namespace storage and adjacency accounting reuse existing owners. The native capability-preserving probe adds metering only and delegates its count logic to adjacencyReadSpy. No second query/equality reader or production forwarding alias is introduced. Remote graph retry still did not complete, so a clean duplicate-check result is not claimed.


## Traversal qualification and copy-owner review

The frozen-main/native A/B workloads now have three complete samples per parser and statement, plus an integrated post-copy-correction smoke. All expected rows pass. The remaining collection-read planner work is reproduced by a public 12-input test: four identical count requests are repeated 48 times. Its correction remains a separate unaccepted proposal until its focused and paired performance controls pass.

The C workload now preserves the original native physical/logical relationship identity separation and unrelated kind/statement fields. Its 1K/16K current-head timings are qualified in the performance guide. It requires no additional production relationship lookup owner. Race controls cover the raw native object and its transparent probe in explicit transactions, with positive handled-index requests and zero expansion/type streams.

DeepSeek's fixture review dispositions:

| Finding | Disposition and evidence |
| --- | --- |
| F1: missing C population certification | Added exact node/relationship/type/degree totals and complete declared-constraint verification. |
| F2: timed native object differs from probe | Both exact object graphs now execute the complete claim under fresh explicit transactions. Timing remains native-only; probe counters are structural evidence, not a timed-path measurement. |
| F3: zero adjacency does not prove index use | Live probe forwards/counts equality-index calls and requires a handled result; type streams remain zero. Explicit dispatch uses BadgerTransaction directly, so its public result and same-transaction handled-lookup control cover that owner separately. An attempted live-probe-call assertion on explicit reads failed as expected for that different dispatch and was replaced with the actual owner control. The unkeyed control still records three adjacency entries. |
| F4: repeated fixture and timer machinery | C reuses the A/B native fixture primitives and constraint owner. All three use one timed-read/transaction loop. Payload generators remain separate for the materially different populations. |
| F5: calibration retains old engines | Not established: Go 1.27 testing.B.runN explicitly defers runCleanup after every calibration invocation (benchmark.go:197–204). No engines accumulate between calibrations. The C benchmark documents rebuilds; the earlier header's once-per-population wording applied to its test and is not used as a benchmark guarantee. |
| F6: missing C run recipe | Added the manual-only benchmark command, degree/parser selectors and timeout guidance. |
| F7: key-name provenance | Aligned C with the native harness's pd_graph_node_id; its text reproduction calls the same key stored_node_id. Preserved distinct physical/logical edge identities and full noise payload. |
| F8: A label count / C list representation | Corrected A header to 171,999 PDGraphNode among 172,000 nodes. The C result intentionally checks the public native stored-value representation (int64 lists); no runtime conversion or assertion weakening. |
| F9: C is an always-on regression | Retained the deterministic native 1K/16K public regression; A/B's 172K populations remain optional benchmarks only. Its race run including setup passes in 21.560 s. |

Copy-owner review reused the existing typed slice kernel, with no second mutable-copy implementation. Integrated race controls pass (storage 2.370 s, Cypher 2.437 s). All five measured result shapes reduce allocations/bytes; vector timing ranges overlap and are not claimed faster. No requested payload was removed.

CI cc7d7534b completed all shards: runtime/capture/lint passed; storage census retained the two pending analyzer findings. The pending aggregate was canceled after preserving the complete inventory. Build/coverage skipped; no full aggregate pass claimed. Remote graph retrieval is incomplete (HTTP-to-HTTPS response); no clean duplicate-checker result.


## Planner-statistics correction acceptance

The original public four-arm reproduction failed with 48 repeated requests for four identities. The integrated MATCH-local correction now reads each identity once, reinitializes for every invocation, preserves every row/write, and returns cancellation/errors. Counter truth and visibility remain outside this heuristic memo. Streaming initialization is lazy; single-row materialized reads do not allocate a memo. The existing generic count owner remains the miss owner; no alternate storage implementation or compatibility adapter was added.

Parent consolidated duplicate test-row generation and used independent structural request comparison in assertions. The private traversal API now takes its existing physical hint as one value; all callers migrated without wrappers. Focused prototype race controls pass in 12.445 s; integrated traversal/link controls pass in 31.446 s. Paired large collection reads improve 264→84 ms and halve allocations; the small link control retains its allocation counts. Full historical b6 comparison is in progress using a temporary unchanged harness, never product code.

The existing schema-publication benchmark now separates unique-source cold admission from warm reads, before/after DDL. The matched frozen-main/current captures completed all eight arms with exact rows and alias columns. Cold-after-DDL allocation medians fall 1,773→1,566 (Nornic) and 1,772→1,569 (ANTLR); ordinary cold allocations are approximately unchanged. Warm-after-DDL byte medians increase but observed ranges overlap, so no systematic byte-regression or precise timing claim follows from the short samples. Full medians and limits are recorded in the performance guide.

## Payload qualification and historical traversal follow-up

The native CALL/fulltext benchmark source now shares the existing statement-read lifecycle and parser population. The isolated 512-source qualification passes all 24 parser/statement/mode arms at both 32 MiB and 512 MiB source payloads; it does not demonstrate flat allocations across that increase. The integrated 64-source smoke passes every CALL/fulltext arm (3.058 s). Large HNSW/private application timing is not qualified by these benchmarks. No second fixture writer, transaction benchmark loop, production decoder or query owner was introduced.

The same temporary native harness completes exact result checks on historical b6d8e762a and current e81f32e06. Forward and keyed reads improve, but the inverse read still increases from about 2.54 to 3.17 MB per operation. A separate 100-read profile confirms this residual. A public two-result traversal reproduces 131 body reads for 128 unused indexed candidates on both parsers. DeepSeek is implementing deferred candidate hydration in an isolated checkout; it is not yet integrated or accepted. The report's one-collection population is preserved; the start side is actually cheaper, so the unselected end bodies are discarded.

CI on e81f32e06 passes all six runtime/capture shards and lint. The same two census findings remain blocking; build and coverage were skipped by test-only dispatch and the aggregate is not a passing result. The prepared census correction and PROFILE counter completeness policy still require explicit user decisions. The separate PROFILE prototype's identical-source-occurrence regression remains unaccepted and unpushed.


## Deferred hydration integration and admission proof

The original public unselected-index test passes after one canonical prepare/read split. Integrated traversal/pending/cancellation/identity/visibility controls pass (4.934 s), then the expanded race population passes (39.670 s). The first isolated 100-read inverse profile returns every expected row at 4.002 ms, 1,926,813 B and 20,973 allocations, compared with the unchanged prior harness at 5.809 ms, 3,164,185 B and 40,700 allocations; these are single-run figures pending repeated both-parser qualification.

The first implementation added one allocation to ordinary selected seeks. It is not accepted as-is: a private inline/overflow ID result now removes that unnecessary allocation without self-referential by-value slices. The six new test-only per-route forwarding aliases are retired; white-box assertions compose production resolution and reading through one shared fixture helper. Public list-property matching exposes a real identity-reader discrepancy: the identity-only and property-only controls succeed, but their combination returns no row before correction. Equality and IN now share canonical property comparison; expanded controls pass (4.981 s).

Three longer 1,000-read samples preserve ordinary allocations and bytes (within 1 B rounding) across inline/equality/IN controls and remove one allocation on OR controls. Measured median latency differences are +0.4% to +4.5%; no ordinary-read speed gain is claimed. The repeated both-parser historical qualification is complete: every result passes, inverse-read medians fall below historical latency, bytes and allocations, and all forward/keyed reads improve. Full workload figures are in the performance guide. Lexical population review remains pending.

The seed-admission public control fails before correction on both parsers. A single canonical MATCH admission call replaces the duplicated partial start predicate. Follow-up unindexed/streamed endpoint controls pass: those paths already use relationshipEndpointReader, so the proposed additional endpoint rewrite was canceled and no speculative hardening was retained. System-namespace privilege is checked through the private owner because public graph statements on system are rejected.

Fulltext source review confirms that the corpus dialect and generic token-postings owner do not provide interchangeable substring matching, field population or scores; existing public rows/scores pin that distinction. The source claim is limited to inspected owners, with no token-index substitution accepted. A public wildcard/presence page additionally reproduces 64 node reads for a five-row result with skip two. Its existing relationship counterpart already stops through appendFulltextOptionedRow. The DeepSeek page implementation request failed before editing. The parent implemented one shared typed row-window owner; no ordering or dialect change is proposed. The public page control now passes, and its performance/race qualification is in progress.

CI f223f270f passes all runtime/capture shards and lint; the same two census failures remain blocking. PROFILE worker timeouts left a preserved, unaccepted partial patch; source review rejected a global profiling switch and extra intermediate splitter allocations before retrying the implementation. The census correction and counter-completeness decisions remain pending.

The by-value node-ID result now has an explicit builder-reuse/copy/inline-overflow lifetime regression (0/1/8/9/17 IDs), independently passing in 1.046 s. Follow-up indexed, unindexed and streamed endpoint controls retain existing admission behavior without an additional runtime rewrite.

The bounded fulltext node/relationship owner is integrated. All 24 paired benchmark arms pass exact results, bounded pages reduce time/bytes/allocations, and longer unlimited controls retain allocation counts. Focused lifecycle/release-oracle race checks pass in 4.072 s. The lexical population was re-extracted (4,037 members): ten prepared-plan declarations replace six retired hydration declarations, with no structural changes to existing rows. Every delta has a source/test disposition; the unchanged population assertion passes in 1.927 s. No extractor/analyzer/checker rules changed.

The combined primary candidate passes focused race verification in 22.882 s, including 1K/16K native stored-link identity/index checks, prepared-plan routes, admission, cancellation/retry, by-value ID lifetime and fulltext release/page controls. This is integrated focused evidence, not a full make-test/census pass.

CI 43025810737 exposed obsolete unranked node acquisition expectations: limited windows expected a full scan, and zero-limit windows expected reads. They now require the exact bounded prefix/no acquisition on both parsers; result identities, complete-following-page populations, ranked corpus results, error gates, wrappers and write-behind controls remain asserted. Focused acquisition/release/page race checks pass in 4.822 s. This changes test expectations for the approved bounded-work optimization, without changing production code or weakening complete-corpus coverage.


## Current PROFILE integration checkpoint

Parent review corrects predicate-source attribution through the canonical
compiler, including lazy CASE with caches disabled; compiled logical folds stay
in use. Two copies of source-program publication are consolidated into one typed
cache with cancellation and concurrent-publication regressions. Combined focused
PROFILE/EXPLAIN/expression/cache race checks pass (15.726 s); the final targeted
predicate/publication controls pass (2.800 s). Both parsers and text/compiled
configurations are exercised. [Owner review](profile-review.md).

The existing per-row public benchmark helper incorrectly reused result-cache
entries between repeated samples. Its explicit zero-capacity cache policy now
keeps all samples uncached. Paired 450-row property and 200-key predicate samples
preserve allocations and bytes within 0.01%, with median time +3.4%/+3.2%.
The 4,034-member lexical review and endpoint structure check pass; no analyzer
rules are changed. Costs and rejected evidence are in the performance guide.

Remaining work is not waived: complete physical-route metrics, resolve the two
blocking census decision-flow findings with the approved correction, finish
current-source performance qualification where still pending, and pass integrated
CI. Unavailable private application replay is a stated evidence limit; the live
Linear descriptions do not make obtaining those unavailable artifacts a new
acceptance gate. Generated evidence must still match each claimed behavior.


## Exact-population and fulltext-score checkpoint

The retrieval fixture now separately qualifies exact three/300-result requests
through the existing zero-RRF-cutoff option on both parsers. All generated
population assertions pass in paired captures; medians and limitations are in the
performance guide. Default-cutoff arms remain unchanged. Fulltext exact rows,
order and score bits reuse the canonical release oracle, including label unions,
relationship types and default-field scope. Integrated race controls pass in
6.687 s.

CI's numeric-input ownership failure was a stale source-pinned `compileValue`
digest after PROFILE source-fragment integration. Parent review confirms the
entire native literal specialization branch is byte-identical to the predecessor.
Only the existing reviewed digest/rationale changes; scanner rules and its
69-member nonconsumer population remain unchanged. Numeric/literal/row-adapter
and PROFILE boundary race controls pass (4.402 s).


## Approved census correction

The scoped decision-flow correction is applied after operator approval. Final
fail-first controls reproduce the joined error/aggregate return and initial loop
backedge defects against the predecessor; isolated corrected race controls pass
in 1.471 s. Actual mutation, substituted values, side effects, defer rewrites and
same-iteration hidden-state body writes remain rejected. The initial load-only
proposal was insufficient because SSA emits an identity self-copy for the named
aggregate return; the proof now recognizes that exact copy only. See
[review and commands](storage-census-decision-flow-review.md). The accidentally
selected full-repository local control was canceled and is not a pass. Full census,
mutants and generated reports remain CI-owned.

## Fused count-route checkpoint

Public both-parser counter routes now report one scalar row/one counter request;
visibility folds report one row/zero reads. Typed reduction provenance prevents
undirected scans from claiming counter completeness. The existing private fused
plans return canonical dispatch outcomes and share scalar result construction,
with all callers migrated and no old-signature adapters. Integrated focused race
controls pass (10.221 s), lexical/normalization population controls pass (2.149 s),
and paired ordinary benchmarks preserve allocations/byte medians with latency
within 1%. Other built-in physical families and final CI remain pending.


## Shared read observation checkpoint

- [x] Retire duplicate Cypher-only reader reporting and migrate canonical callers.
- [x] Preserve known-zero, parent attribution and ordinary no-wrapper behavior.
- [x] Integrated focused race checks, lexical population and paired ordinary token
      benchmarks pass without added allocations.
- [x] Complete fulltext/vector physical-reader observations (later qualified checkpoint below); final-head CI remains one shared gate.


## Fulltext observation checkpoint

- [x] Reproduce public fulltext PROFILE omissions on both parsers.
- [x] Observe actual populations, schema checks and snapshot point reads with
      the shared canonical reader owner; captured images add no invented reads.
- [x] Preserve rows, score bits, bounded reader work and error causes under race
      checks; ordinary allocation medians retained in longer paired samples.
- [x] Complete parent source review and independent public runtime proof; the canceled additional DeepSeek review is not counted. Final-head CI remains open.


## Main NOR-213 integration

Rebased onto main `620595a5f`, preserving latest-committed adjacency and endpoint
resolution for write-owned nodes. The old new-main directional/header test calls
are migrated to the canonical typed request; the added stored-incidence caller
uses the same capture owner, with each self-loop contributing to both positions.
No retired directional API or forwarding adapter is restored.

- [x] Integrated focused race checks: storage 3.211 s, Cypher 22.517 s,
      Bolt 24.734 s, including new-main locked-node consumers and PROFILE/fulltext.
- [x] The additional positional-count/rewrite/delete/order tests and stored read
      cancellation/capture controls pass under race instrumentation (4.552 s).
- [x] Unchanged lexical population assertion passes after rebase (2.210 s).
- [ ] Final-head full census/generated artifacts and all test shards in CI.

The initial post-rebase compile exposed the newly added main incidence helper's
retired call; it was migrated and then retired into the existing caller. That
incomplete compile is not recorded as a test pass.


## Rebased CI cascade validation fixture

The current storage shard fails the validation-window test's old assertion that
the peer waits on the relationship key first. Ten focused race repetitions
reproduce it. NOR-213 now includes cascaded relationships' endpoints in commit
authority; the existing canonical order makes the peer's shared endpoint request
wait on the primary's exclusive node identity before its relationship key.
The test now pins that actual authority and still independently requires the
primary's relationship lock, no peer acknowledgement in the validation window,
the peer's exact relationship-conflict identity and zero final relationship
counts. No production lock ordering or conflict policy changes.
Twenty focused race repetitions of both validation-window/order controls pass
(8.865 s). This is a test-contract correction after integration, not a claimed
new runtime fix; current-head CI must still be rerun.


## Vector and GDS catalog checkpoint

- [x] Reproduce and correct vector exact-population/service-hydration observation
      gaps through source-owned logical operations on both parsers.
- [x] Preserve request-owned provider/cache and independent warmup coverage;
      no background read count or ANN visited-entry estimate is invented.
- [x] Consolidate GDS catalog reads and retire its private forwarding method;
      preserve exact catalog/function/property/replacement behavior.
- [x] Integrated race, boundary controls and unchanged lexical guard pass.
- [x] Ordinary vector/GDS paired profiled samples retain allocation medians.
- [x] Complete native mutation request accounting (later qualified checkpoint); remaining built-in routes stay open.
- [ ] Final-head CI (full tests, context/storage census and generated artifacts).

## Capture census source review after main integration

CI exposes stale PublishCommit phase offsets and a changed holdsEntities digest.
The former still contains exactly two Background sites, now +262/+494 after the
eight added pre-acceptance lines; both durable completion consumer bodies are
byte-identical. The latter now enumerates relabelled as well as deleted nodes
under its existing caller-context lock and creates no fresh context. The factual
review records are refreshed with this source evidence. No analyzer logic,
exception population, phase approval or acceptance rule changes. Static source
checks establish these facts; the full context census remains CI-owned.

### Rebased census artifact review

CI run 43109340385 failed artifact equality only in storage census. Its generated
report retains the same 279 writes and 395 field writes after source-location
normalization. Rules, exemptions, dispositions, pending items, violations and
unordered writes are byte-equivalent as parsed values. The extra decision follows
`PublishCommit` through its existing replay-aware rebase and snapshot conflict
classification into the existing suppression owners. This is a newly reachable
validation path from main, not a new writer or exemption. The unchanged runtime
owners were reviewed at the integrated source. Both generated artifacts are
refreshed from that CI output; equality must still pass on the final head.

- [x] Observe native mutation requests and MERGE/deletion preparation reads;
  distinguish actual relationship batches from single-entity staging calls.
  Both-parser focused/race controls and longer paired ordinary-write benchmarks
  pass without added allocations. Remaining builtin algorithm/procedure coverage
  and final-head CI stay pending; see PROFILE review.

- [x] Qualify FastRP/version, static metadata, six shared whole-graph algorithms
  and schema-shape PROFILE observations with both-parser fail-first controls.
  Retire duplicate catalog-acquisition and test restoration/ledger logic.
- [x] Pass the combined focused PROFILE race population (12.264 s), unchanged
  lexical guard (2.256 s) and 12 ordinary algorithm benchmark arms without added
  allocation medians; no full-suite pass is implied.
- [x] Complete the named retrieval/relationship/continuation read and provider
  observations in the later integrated checkpoints; other opaque routes remain unqualified.


## Search observation checkpoint

- [x] Reproduce actual public relationship endpoint and complete-ID-continuation
  observation gaps on both parsers, using explicit result schemas.
- [x] Qualify admitted endpoint images, native-consumer vectors, batch fallback
  contracts, filtered scan populations and request-owned page counters.
- [x] Consolidate the optional batch capability, schema-constraint acquisition,
  test observation recorder and plan-tree walker without forwarding wrappers.
- [x] Pass integrated focused race and unchanged lexical population checks;
  ordinary endpoint/continuation workload allocations remain within baseline.
- [x] Refresh exactly two stale census source positions; normalized decisions,
  other populations, findings and all rules are unchanged.
- [x] Integrate the qualified provider, graph/path, retrieval, CDC and operational proposals documented below. Unqualified built-in routes remain open.
- [ ] Obtain final integrated CI pass; runtime shards on the previous head pass,
  but its generated census artifact equality fails and is corrected here.


- [x] Qualify actual rerank/inference/decay/authorization callback observations,
  preserving silent non-invocation and request-owned continuation pulls.
- [x] Retire two duplicate test recorders; review the one new factual lexical
  population member and pass the unchanged extractor's assertions.
- [ ] Final provider/source integration CI and remaining builtin read coverage.


- [x] Qualify policy metadata procedures and scalar readers on both parsers;
  source-baseline failures, empty/actual populations and custom replacement
  controls pass after integration (race 6.019 s).
- [x] Retire duplicate public statement-equivalence fixtures while retaining the
  unordered catalog row-multiset contract and unchanged lexical assertions.
- [x] Complete graph/path, CDC/temporal and retrieval-index observation review, as qualified below.


- [x] Converge graph/path/algorithm actual-read observations and propagate
  reproduced adjacency read failures; combined both-parser race controls pass.
- [x] Correct the label-seeded typed-adjacency route and qualify its exact
  populations and paired ordinary allocation/CPU controls.
- [x] Review and refresh three stale credential sink source pins; focused
  security structure/redaction race controls pass without analyzer changes.
- [x] Qualify and integrate retrieval-index and CDC/temporal proposals, as documented below.
- [ ] Finish remaining native procedure ownership families and explicitly
  inventory optional plugin adapters without assuming names prove coverage.
- [ ] Obtain final-head CI: ffb's six runtime/census shards and lint pass, but
  the other shard fails only the source-pin equality corrected above; aggregate
  is not approved and the failed head is not a full pass.


- [x] Qualify and integrate eight retrieval and seven CDC/temporal/log
  observation declarations through canonical read/callback owners.
- [x] Preserve named-vector request units, exact ordinary/PROFILE results,
  continuation isolation, provider opacity, real cache-hit non-invocation and
  pre-filter scan populations under focused integrated race checks.
- [x] Reproduce and fix public Go adjacency partial success on reader errors;
  share stored-edge result projection.
- [x] Match ordinary retrieval CPU/byte controls with unchanged allocation
  counts; preserve the unpinned preliminary sample as non-qualification.
- [x] Review and qualify the operational metadata/provider/index-management batch.
- [ ] Complete remaining native, dynamic/batch and input/output procedure owners.
- [ ] Complete original fulltext postings qualification; label-scoped scan
  equivalence alone is not indexed-postings proof.


- [x] Qualify eight operational metadata/provider/index-management procedures
  with actual shared count/catalog owners and provider opacity.
- [x] Retire unused context-discarding private forwarding methods; preserve
  connection filtering, invalid argument ordering and supported SHOW boundary.
- [x] Pass integrated all-PROFILE, numeric/path/void/schema argument and
  credential structure/redaction focused race populations.
- [x] Retain ordinary allocation counts; targeted longer configuration repeat
  resolves the short-sample timing concern without changing production code.
- [ ] Complete remaining 37 native procedure dispositions and actual-read
  coverage; do not certify transaction metadata's arbitrary formatter path.
- [ ] Resolve original fulltext postings and non-PROFILE evidence gaps identified
  in the acceptance audit; investigate original HNSW publication miss.
- [ ] Obtain final integrated CI after factual source-pin corrections.


- [x] Extend shared public cache-image qualification to named vector queries,
  both parsers, actual hits, mutation isolation and zero rehydration controls.
- [x] Qualify 32/512 MiB vector payload scaling with identical allocation counts;
  distinguish candidate-only scaling from unavailable private replay.
- [x] Remove exactly two factual lexical-manifest records for retired forwarding
  declarations; unchanged Foundation lexical guard passes in 2.201 s.
- [x] Preserve the HNSW first-miss assertion and add diagnostic-only canonical
  exact/direct ANN/durable-vector comparisons. The focused first-build race
  rerun passes in 317.49 s; this does not establish the intermittent root cause.
- [ ] Finish remaining native PROFILE owners, fulltext candidate indexing,
  relationship-index cost qualification and HNSW failure classification.
- [ ] Obtain final integrated CI on the resulting head; fa9's other shard failed
  only the two retired manifest records corrected in this checkpoint.


- [x] Qualify GDS project/drop at actual stream and shared catalog-read owners;
  preserve preflight atomicity, duplicate names, cancellation and result values.
- [x] Preserve resolver opacity on its actual invocation, including a real
  observed inner read in the same collector; qualify denied owner invocation.
- [x] Integrate public both-parser empty/populated/filtered controls and custom
  replacement guards; integrated race pass 78.170 s, unchanged lexical pass.
- [x] Measure ordinary 5K-node/20K-edge projection controls, report overlapping
  allocation ranges honestly, and prove zero allocation in the long-key reader.
- [ ] Complete remaining 37 native procedure names and final integrated CI.


## Current remaining gates after reconciliation

The chronological CI checkboxes above are one continuing exact-head gate, not
separate implementation tasks. Historical failures remain evidence for their
own heads. At `6df70b830`, the genuinely open work is:

- [ ] Finish 37 native procedure dispositions/observations and explicitly
  inventory optional plugin admission; arbitrary custom callbacks stay unknown.
- [ ] Qualify and integrate dialect-compatible fulltext candidate selection.
  The isolated prototype is not accepted or pushed; cold-start, mutation,
  retention and public-equivalence qualification is still in progress.
- [ ] Measure relationship equality-index cold construction and retained bytes.
- [ ] Classify and correct the captured intermittent HNSW publication miss;
  passing reruns do not establish its cause.
- [ ] Obtain final integrated exact-head CI/census and reconcile Linear/evidence.

Public vector-cache mutation/zero-rehydration and 32/512 MiB scaling are already
qualified. Generated workload proof is accepted within its named scope;
unavailable private application replay remains an evidence limit, not a newly
imposed gate. The read-only inventory audit cross-checked each completed slice
against the later committed source-review/performance evidence before changing
its stale checkbox.


- [x] Integrate maintained conservative literal fulltext candidates through the
  existing projection, exact matcher and frequency owners; public both-parser
  node/relationship equivalence, cancellation, replacement/churn and active-lease
  retirement controls pass under integrated race checks (4.852/14.956 s).
- [x] Qualify fixed 1,000-query warm controls: 11.837→3.349 ms, 3,038,670→3,029,141
  bytes and 17,606→17,549 allocations, three samples; preserve short-run differences.
- [ ] Resolve/qualify cold construction cost: latest distinct-body sample is
  +6.6% time, +1.6% bytes and +82 allocations, despite the warm improvement.
- [ ] Obtain next-head CI/census; the prior `6df70b830` complete test-only pass
  remains historical evidence, not verification of these later source changes.


- [x] Complete five native persisted-procedure catalog dispositions through
  shared point/label readers, direct native request accounting and actual
  out-of-pipeline invocation records; preserve administrator/result contracts.
- [x] Qualify public both-parser identity/migration/installation/listing controls,
  opaque replacements and all PROFILE race tests (17.600 s); ordinary 0/128-record
  listing allocations remain 113/4,494 with overlapping byte ranges.
- [ ] Complete remaining **32** native procedure names and optional-plugin
  inventory; dynamic/nested DDL still requires its own scope proof.


## Current local catalog and fulltext checkpoint

- [x] Qualify all five custom-procedure catalog registrations, legacy replacement,
  nonempty surviving-catalog reload and pipeline installation on both parsers.
  Combined PROFILE/catalog/fulltext race checks pass (search 1.918 s,
  Cypher 24.564 s). Custom callback invocations remain explicitly opaque.
- [x] Remove intermediate fulltext selector trees: compile the existing syntax
  directly through one candidate reader. Prepared literal and compound selection
  use zero allocations in normal and race builds. Cleared bounded plan scratch
  remains isolated for nested calls and is included in retention accounting.
- [x] Qualify compiler-owned cancellation for skipped children and propagate
  syntax-reader errors without publishing candidates.
- [ ] Record final-source fulltext cold/warm paired costs, then push and run CI.


## Current source checkpoint after approved coordinate contract

This current list supersedes historical remaining-count notes above.

- [x] Complete HNSW generation coordinate ownership and deterministic public ANN
  regression; qualify ready/build/replay costs and preserve first-build controls.
- [x] Apply the approved exact coordinate-domain contract with direct and aliased
  node-ID rejection controls; review the one factual token-order lexical entry.
- [x] Qualify eight index creation/drop/vector-setter names through shared actual
  readers, mutation requests and native temporal decisions/populations.
- [x] Correct post-commit refresh shutdown ownership while preserving accepted
  caller-independent completion and immutable PROFILE attribution.
- [x] Inventory actual optional APOC admission: 814 exports, 702 canonical and
  112 opaque; this is admission-policy evidence, not 814 behavior tests.
- [x] Qualify ordinary refresh costs and commit the index/lifetime source batch.
- [ ] Pass final integrated exact-head CI after the remaining source batches.
- [x] Qualify I/O 8 and dynamic/periodic 7 through public both-parser controls;
  preserve unknown SCHEMA metrics and correct nested schema admission.
- [ ] Commit/push integrated candidate and qualify final-head CI.
- [x] Qualify suppression 2, commit/rollback and custom opacity through public
  paths; retain a compact deferred value and canonical refresh owner.
- [x] Resolve transaction metadata observation scope: operator approved explicit
  unobserved metrics; reject inaccurate zero-read certification of custom values.
- [x] Qualify final direct-reader cold/warm costs: +2.71% cold time, +1.60%
  construction bytes, -71.16% warm time; no gate exception needed.
- [x] Complete integrated direct-reader result/race checks (search 1.962s,
  Cypher 12.585s; final CI remains required).
- [x] Qualify relationship equality-index cold construction and retained heap
  through 32,768 unrelated edges, four properties and the 16/17-entry cap;
  document absolute costs and their limits, without a false unchanged-cost claim.
- [ ] Complete final integrated exact-head CI and AC/Linear reconciliation.
