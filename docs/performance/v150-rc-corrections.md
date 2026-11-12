# v1.5.0 RC correction measurements

Apple M2 Max, darwin/arm64, Go 1.27.3, `noui,nolocalllm`. Results below are local generated workloads, not a replay of unavailable private lab data. Process monitoring found no overlapping Go test/build process during the accepted samples. Earlier overlapping captures were excluded. No full release-performance qualification is claimed.

## Correlated aggregate without a memory budget

128 outer rows execute a correlated CALL containing count(). An unused import contains a 40,000-entry list of maps and a 40,000-entry map. The canonical aggregate owner previously walked those values before its memory account discarded the estimate. It now evaluates the estimate only while a budget is active. Exact result and enabled-budget rejection controls pass.

Three 1-second benchmark samples, medians:

| Operation | Before | Corrected |
| --- | ---: | ---: |
| ns/op | 326,102,364 | 17,616,113 |
| ops/sec | 3.07 | 56.77 |
| B/op | 11,579,000 | 11,574,345 |
| allocations/op | 95,191 | 95,123 |

The measured operation is about 18.5 times faster with essentially unchanged allocation volume. CPU profiles identify recursive value sizing as the removed work. Baseline is `31a4fff52`, with the same new benchmark overlay; corrected aggregate source SHA256 is `2ceb772853c9f7bb065c4e3262043e14ee714df13e49b0443666be91872b43ae`. This is an in-memory, auto-commit workload; transport, real application data and concurrent load are outside this measurement.

```sh
go test -tags=noui,nolocalllm ./pkg/cypher -run '^$' \
  -bench '^BenchmarkCorrelatedCallAggregateWithoutBudget$/unused-import-40000/aggregate$' \
  -benchmem -benchtime=1s -count=3 -cpuprofile=aggregate.cpu
```

## Result-cache ownership copies

The existing cache fixture now includes a 21,748-row scalar/list result and 25-node metadata/vector controls. The vector control deliberately returns 128 chunks of 1,024 floats per node: those mutable returned bytes still require detached ownership. The optimization shares one typed slice-copy kernel and skips immutable scalar reflection; aliases and cycles remain preserved.

Three samples of three iterations each, median query-cache hit results:

| Result shape | ns/op before / after | B/op before / after | allocations before / after |
| --- | ---: | ---: | ---: |
| 21,748 scalar/list rows | 7,513,305 / 7,157,736 | 13,269,648 / 12,225,205 | 152,528 / 109,032 |
| 25 metadata nodes | 15,111 / 12,805 | 18,216 / 17,040 | 290 / 241 |
| 25 vector-bearing nodes | 1,584,222 / 1,235,722 | 13,206,016 / 13,204,840 | 3,515 / 3,466 |

These short samples establish allocation reductions; the timing ranges, especially the vector control, are too wide for a precise latency claim. They do not establish the full reported application-query improvement. Vector-hydration work remains separately tracked. Baseline is `31a4fff52` with the same expanded benchmark overlay; corrected copier landed in `3c5989eec` and has SHA256 `f75e47d61b31103df97d4b4359799b4dc47f034da89a2f80255cb41b4a44adb4`.

```sh
go test -tags=noui,nolocalllm ./pkg/cypher -run '^$' \
  -bench '^BenchmarkNOR307QueryResultCache$/^(list-21748|metadata-25|vectors-25)$/^query-cache$/^hit$' \
  -benchmem -benchtime=3x -count=3 -cpuprofile=cache.cpu
```

## Retrieval after schema publication

The public `db.retrieve` fixture contains 12,500 sources, 356 groups and 1,024-dimensional vectors. The query returns three identical rows before and after DDL; result caches are disabled and query admission is warmed. A new index declaration runs outside each measured operation. Schema-only transaction publication now declares its namespace, matching live schema persistence, instead of revoking all durable entity caches.

Three isolated single-operation samples per parser, median after-DDL values:

| Parser | ns/op before / after | B/op before / after | allocations before / after |
| --- | ---: | ---: | ---: |
| Nornic | 2,081,084 / 1,756,416 | 700,960 / 579,208 | 768 / 540 |
| ANTLR | 2,070,500 / 1,681,209 | 721,416 / 579,448 | 794 / 542 |

The corresponding allocation ranges are 761–803 to 504–541, and 794–804 to 511–543. Bytes vary substantially across samples, so these medians are descriptive rather than a precise byte-reduction guarantee. Warm-query controls remain about 545–548 allocations, with one 510-allocation outlier. CPU captures include fixture/index construction outside benchmark timing and cannot be interpreted as query-only profiles. Baseline is `31a4fff52` with the same benchmark overlay; corrected candidate is `ba02c1be5`. Cold grammar admission and indexed-WHERE costs are separate remaining criteria.

```sh
go test -tags=noui,nolocalllm ./pkg/cypher -run '^$' \
  -bench '^BenchmarkRetrieveAfterSchemaPublication$' -benchmem \
  -benchtime=1x -count=3 -cpuprofile=retrieve-schema.cpu
```

## Unique relationship-property lookup

The generated public query selects one relationship by a declared UNIQUE property from 128 or 4,096 unrelated relationships of the same type. The fixture, constraint preparation and warmed parser admission precede timing; result caches are disabled. Exact endpoint/value rows are asserted on every iteration. The corrected path reuses the constraint's existing bounded equality index, with current-image read-back. Pinned historical cuts, pending changes, write-behind and unsupported values retain their existing authoritative read behavior.

Three isolated 100 ms samples, medians for 4,096 unrelated relationships:

| Parser / transaction | ns/op before / corrected | B/op before / corrected | allocations before / corrected |
| --- | ---: | ---: | ---: |
| Nornic / auto-commit | 906,358 / 58,767 | 1,674,029 / 14,151 | 8,369 / 171 |
| Nornic / explicit | 17,669,930 / 58,367 | 15,121,430 / 14,014 | 233,791 / 172 |
| ANTLR / auto-commit | 839,155 / 58,446 | 1,674,093 / 14,278 | 8,370 / 172 |
| ANTLR / explicit | 17,273,993 / 58,755 | 15,121,754 / 14,145 | 233,793 / 173 |

These captures qualify the isolated lookup prototype `867a4cced`, based on `b672cebda`. The comparison overlays only the original `b672cebda` traversal on the same fixture and declaration-time preparation, then restores the corrected traversal. It measures warm lookup cost, not original cold DDL/build cost. CPU samples tagged `phase=relationship-read` exclude fixture work; the old path spends samples materializing/copying unrelated edges. At 128 unrelated relationships the corrected path also uses 171–173 allocations, versus 417–418 auto-commit and 7,555–7,556 explicit before correction. No overlapping Go test/build process was observed in these captures.

The integrated branch now also changes statement image policy, so these prototype timing numbers are not a fresh combined-head qualification. Integrated public allocation-ladder, snapshot update/delete/recreate and durability-wrapper tests pass. Declaration-time preparation eagerly retains derived index state under the existing 16-entry limit; retained-memory and cold-build deltas have not been measured. No unchanged-retained-memory claim is made.

```sh
go test -tags=noui,nolocalllm ./pkg/cypher -run '^$' \
  -bench '^BenchmarkRelationshipPropertyEquality$' -benchmem \
  -benchtime=100ms -count=3 -cpuprofile=relationship.cpu
```

## Typed adjacency inside a new explicit transaction

Eight documents each own one `MEMBER_OF` edge and 64 unrelated relationships. Each operation runs BEGIN, the public typed traversal and ROLLBACK; result caching is disabled. Three isolated 100 ms samples include transaction admission and cleanup, with exact result cardinality checked on each operation.

| Metric | Migrated reader before correction | Canonical current-header reuse |
| --- | ---: | ---: |
| ns/op (median) | 789,770 | 319,492 |
| ns/op range | 781,785–804,029 | 310,393–322,011 |
| B/op (median) | 434,721 | 170,790 |
| allocations/op (median) | 6,924 | 1,947 |

The initial migrated snapshot reader loaded an MVCC head for every unrelated edge before rejecting its type. Allocation profiles locate that cost under `adjacentSnapshotHeadersInTxn` and `loadEdgeMVCCHeadByNumRawWithPhysicalVersionInTxn`. A first experiment skipped heads under a namespace-cut proof but still collected every MVCC adjacency ID; its medians were 433,447 ns, 255,751 bytes and 2,244 allocations. Reusing the current-header kernel through the transaction's same physical view removes that second acquisition too, and is the retained candidate.

Both sides include the typed migration through `b57414549` and the same benchmark harness. The retained correction uses the existing namespace-current predicate before and after acquisition; a changed or uncertain cut keeps historical head validation. It never opens a newer snapshot to repair an old one. Visibility-filtered requests use their record-based owner; property-dependent promotion is verified separately. No storage key, payload or format change is involved. Whole-capture CPU/allocation profiles include fixture work, so their percentages are not query-only attribution. These measurements do not replay the private application or qualify NOR-414's separate 16K-link shape.

```sh
go test -tags=noui,nolocalllm ./pkg/cypher -run '^$' \
  -bench '^BenchmarkNOR160TypedAdjacencyStep$/unrelated-64/explicit-true$' \
  -benchmem -benchtime=100ms -count=3 \
  -cpuprofile=typed-adjacency.cpu -memprofile=typed-adjacency.mem
```

## Hybrid predicate decisions within one request

The public Search reproduction evaluates an unchanged candidate twice across lexical/vector branches and four times during selective widening. One request-local decision set now shares each candidate's first WHERE verdict across those branches and rounds. Identities include database and local ID. Later searches and continuation pulls start a fresh set; canceled waiters stop independently, and errors remain errors rather than rejected candidates. Only identities/decisions are retained, not node bodies or vectors. Searches without WHERE create no decision set.

The existing `BenchmarkSearchSourceRetrieval/12.5K/one-shot/retrieve/exists/concurrency1` fixture supplies 12,500 sources, 356 groups, 1,024-dimensional vectors and the existing correlated three-hop EXISTS predicate. Every operation verifies its 50 returned sources are admissible and unique. This is the repository's generated workload, not the lab's separate 3/300-result fixture. Query/result caches are disabled. Setup and index construction precede timing.

Six three-operation samples per arm, run before/after/after/before in isolated processes, give:

| Metric | Before | Corrected |
| --- | ---: | ---: |
| ns/op median | 165,075,785 | 119,948,403 |
| ns/op range | 162,185,069–172,312,875 | 117,802,083–122,505,458 |
| B/op median | 123,343,616 | 89,409,400 |
| allocations/op median | 1,512,914 | 1,062,675 |

The matching unfiltered control (three samples) stays at 1,236,053→1,235,973 median B and 6,654→6,653 allocations; timing ranges overlap (2.98–3.40 ms before, 2.93–3.37 ms after). No general latency improvement is claimed for that control. CPU captures include fixture construction and have sparse query-only filter samples; they are not a precise percentage attribution. The public call-count reproduction and allocation comparison establish the repeated predicate work and the scoped reduction.

Baseline is c7930df01; the measured isolated candidate overlays only the search entry/filter correction and its private decision state on that base. No overlapping Go test/build process was observed before accepted captures. The production error path additionally preserves a predicate error joined with simultaneous cancellation; race controls cover it. These successful-path captures do not quantify that exceptional path, retained-memory peaks, external providers or the unavailable application workload. Indexed-WHERE 3/300-result and cold admission criteria remain separate.

```sh
go test -tags=noui,nolocalllm ./pkg/cypher -run '^$' \
  -bench '^BenchmarkSearchSourceRetrieval$/^12.5K$/^one-shot$/^retrieve$/^exists$/^concurrency1$' \
  -benchmem -benchtime=3x -count=3 -cpuprofile=predicate.cpu -memprofile=predicate.mem
```


## Type-scoped traversal seed statistics

The canonical adjacency reader already takes the relationship types from the parsed match. Seed selection still asked its count owner for every relationship type, so unrelated edges could reverse the endpoint choice. Both endpoint estimates now pass that same parsed type set to the existing count-store reader. Untyped patterns keep their existing all-type request; no budget, new cache, index or stored representation is introduced.

Public generated controls prove exact rows and typed count requests for a single type, a type union, a cheaper target endpoint, many collections and a pinned element-ID seed. The new typed controls fail with the original count requests; untyped and pinned controls retain their contract. Integrated selected traversal, endpoint cancellation and transaction-read race controls pass (7.510 s).

The existing single-collection fixture has 351 documents with 800 unrelated edges each. Its public auto-commit query returns 351 rows; result caching is disabled, fixture setup precedes timing, and three one-second samples per arm use the same candidate based on c2ca670a8 with only the type-statistics correction overlaid.

| Metric | Original all-type statistics | Type-scoped statistics |
| --- | ---: | ---: |
| ns/op median | 2,457,981 | 2,459,798 |
| ns/op range | 2,444,611–2,480,183 | 2,458,929–2,477,973 |
| B/op median | 1,551,157 | 1,546,579 |
| allocations/op | 11,419 | 11,346 |

Timing ranges overlap; this fixture establishes a small work/allocation reduction, not a latency improvement. The corrected estimator is not the dominant remaining cost in this already-refactored workload. Its CPU capture includes setup; the original `chooseTraversalEndSeeds` samples were principally under `collectNodeIndexSeek`, not the count call. The lab's distinct 16K-link residual and full application qualification remain separate criteria. No concurrent Go command was observed during either accepted capture.

```sh
go test -tags=noui,nolocalllm ./pkg/cypher -run '^$' \
  -bench '^BenchmarkTraversalSeedCost$/^single_collection_indexed_end$/^autocommit$' \
  -benchmem -benchtime=1s -count=3 \
  -cpuprofile=seed-statistics.cpu -memprofile=seed-statistics.mem
```


## Published NOR-407 WHERE workload

`BenchmarkWhereFilteredSourceRetrieval` carries the lab's published default corpus into typed current APIs: disk-backed Badger, sequential writes of 12,500 nodes, 356 sources/documents and memberships, 1,024-dimensional named vectors, seed-7 text/vector generation, the original correlated predicate and rotating distinct queries. Each document-ID-index arm owns a fresh corpus. Search index construction precedes timing; page validation and cursor disposal are excluded. An independent plain MATCH establishes exactly 356 eligible source IDs, and every page checks membership, uniqueness, accounting and non-increasing fusion scores.

Three three-operation samples per parser/index/page-size arm give these medians. The baseline is fixture commit 4208ef9b0 on 1c4203e8f. The candidate overlays only `search.go`, `filter_evaluation.go` and `filter_verdict.go` from b85a2e619; their content hashes were recorded before measurement. No other Go command overlapped the captures.

| Parser / shape | ms/op before → corrected | B/op before → corrected | allocations/op before → corrected | returned rows/op before / corrected |
| --- | ---: | ---: | ---: | ---: |
| nornic / document-id-index-absent/n3 | 139.94 → 99.78 | 99,851,584 → 64,859,536 | 845,248 → 531,409 | 3 / 3 |
| nornic / document-id-index-absent/n300 | 466.12 → 174.00 | 357,076,981 → 120,906,925 | 3,013,275 → 960,304 | 169 / 169 |
| nornic / document-id-index-present/n3 | 28.73 → 24.84 | 7,581,592 → 5,919,864 | 56,217 → 38,460 | 3 / 3 |
| nornic / document-id-index-present/n300 | 81.91 → 47.80 | 30,920,394 → 18,866,386 | 234,758 → 111,195 | 169 / 169 |
| antlr / document-id-index-absent/n3 | 139.94 → 98.73 | 98,982,322 → 66,477,984 | 827,857 → 560,154 | 3 / 3 |
| antlr / document-id-index-absent/n300 | 451.28 → 175.14 | 353,836,205 → 123,274,666 | 2,952,088 → 1,009,800 | 169 / 169 |
| antlr / document-id-index-present/n3 | 28.11 → 24.21 | 7,509,626 → 5,761,877 | 56,226 → 38,476 | 3 / 3 |
| antlr / document-id-index-present/n300 | 81.41 → 48.71 | 30,983,754 → 18,685,560 | 234,763 → 111,186 | 169 / 169 |

`n300` requests 300 results, but the unchanged default score cutoff returns approximately 167–170 results. The table reports actual returned rows instead of assuming a full page. Different HNSW construction order can change candidate membership slightly between processes; these are same-shape cost comparisons, not cross-process exact-rank identity proof. Query texts rotate without cache hits. The original identity provider is represented by the existing controlled pass-through fixture, and cursors are promptly released rather than left to expire. Setup uses the original sequential write history instead of bulk loading.

The reduction agrees with the independently reproduced repeated-predicate call counts. Profiles include fixture construction and index warmup, so no query-only CPU percentage is inferred from the whole captures. This qualifies warm first-page WHERE cost on both parsers with and without the document index; cold admission, later cursor pages, external providers, concurrency and the original lab's hardware timings remain separate. An additional smoke execution of the final fixture body on integrated b85a2e619 passes with three exact eligible results (10.418 s including setup).

```sh
# Repeat with NORNICDB_PARSER=antlr for the second parser.
go test -tags=noui,nolocalllm ./pkg/cypher -run '^$' \
  -bench '^BenchmarkWhereFilteredSourceRetrieval$' -benchmem \
  -benchtime=3x -count=3 -cpuprofile=where.cpu -memprofile=where.mem
```


## Fresh-executor admission with the deprecated memory setting

The existing `BenchmarkProtocolTransactionStatementCaches` constructs a fresh executor, shares the database's statement caches and executes one read against an empty graph. Setup and initial warming precede timing; every measured call checks its error. This is executor construction plus a read, not complete HTTP/Bolt transport or COMMIT throughput.

Three ten-operation samples compare the same 1 GiB setting through the deprecated MiB variable and canonical byte variable. The baseline restores only `localization.NewManager(nil, nil)` at the warning call on c2ca670a8; the corrected arm uses the committed shared catalog owner. The unused typed-seed overlay in the measurement checkout is not reached by this empty-graph workload. The warning-call overlay was restored and its content hash checked after capture. No other Go command overlapped these runs.

| Configuration / owner | ns/op median (range) | B/op median | allocations/op |
| --- | ---: | ---: | ---: |
| Deprecated setting, original warning owner | 336,639,950 (335,276,779–337,845,379) | 348,252,161 | 4,627,890 |
| Deprecated setting, shared catalog owner | 194,108 (191,904–209,962) | 126,553 | 694 |
| Canonical byte setting, shared catalog owner | 184,417 (163,271–202,058) | 122,587 | 574 |

Warnings still render per configuration load, including the configured language. Only immutable catalog construction is shared. The corrected legacy and canonical timing ranges overlap; the compatibility warning still has measurable rendering allocations. The original warning behavior accounts for the large fresh-executor regression in this workload. CPU/allocation captures include initial fixture/configuration work; the prior configuration tests separately prove warning observability, language choice, concurrent loads and bounded repeated-load allocations. These measurements do not establish full wire-level transaction throughput or a general query speedup.

```sh
env -u NORNICDB_GO_MEMORY_LIMIT_BYTES NORNICDB_MEMORY_LIMIT=1024 \
  go test -tags=noui,nolocalllm ./pkg/cypher -run '^$' \
  -bench '^BenchmarkProtocolTransactionStatementCaches$' -benchmem \
  -benchtime=10x -count=3 -cpuprofile=admission.cpu -memprofile=admission.mem
```

## Stored claim lookup at 1K and 16K target degree

The published NOR-414 STORED_LINK shape constrains both endpoints and a declared unique relationship property. Its target has 1,000 or 16,000 incoming relationships; the source has three outgoing relationships. Sequential native writes populate in-memory Badger in `personal_documents`; constraints are installed after loading. The six returned property values, including both citation lists, are asserted on every iteration. Each measured read has a fresh explicit transaction, with BEGIN/ROLLBACK and fixture construction excluded from timing. Result caching is disabled.

The integrated native candidate at `82e72b371` with the committed qualification overlay uses the relationship-key owner already corrected for NOR-42. Three 100-operation samples give:

| Parser / target degree | ns/op median | B/op median | allocations/op |
| --- | ---: | ---: | ---: |
| antlr / 1,000 | 145,171 | 22,688 | 242 |
| antlr / 16,000 | 156,803 | 24,497 | 266 |
| nornic / 1,000 | 140,427 | 22,564 | 241 |
| nornic / 16,000 | 159,520 | 24,320 | 265 |

This is current-candidate qualification, not a same-machine b6 baseline comparison. The reported 11.7 ms at 16K on the older partial fix is not reproduced by this native candidate. Public race controls on both parsers additionally preserve every native optional capability: removing the relationship key proves the shared meter sees the source's three adjacent relationships, while the keyed lookup returns the identical claim with zero adjacency entries. Both degree populations pass (21.560 s including setup and race instrumentation), including the native benchmark object and the observer under explicit transactions. The live observer records a positive handled equality-index request. Explicit transactions use BadgerTransaction's separate snapshot-aware lookup, so the public read is followed by an assertion that its transaction-owned equality lookup handles the exact key; a live-store probe cannot count that dispatch. Fixture assertions certify both degree/count populations and the complete declared key set. Physical relationship IDs intentionally differ from their logical id properties, as in the original native generator; unrelated relationships retain its kind/statement payload.

Native timing uses the namespace store itself, not the test decorator; the functional probe preserves its optional native capabilities. Query-labeled CPU samples are sparse and dominated by runtime activity; no precise hotspot percentage is claimed. The separate large collection/document workloads remain under qualification.

```sh
go test -tags=noui,nolocalllm ./pkg/cypher -run '^$' \
  -bench '^BenchmarkStoredLinkLookup$' -benchmem -benchtime=100x -count=3 \
  -cpuprofile=stored-link.cpu -memprofile=stored-link.mem
```


## Reusing caller-copy slice headers

The canonical value-copy walker now delivers the interface header it already retains for alias/cycle tracking. It creates no second boxed header for nested list cells or a matrix root. Returned mutable payloads remain detached; it does not return shared cache entries, omit requested vectors, or add another copy owner.

Paired three-sample cache-hit captures compare only `property_copy.go` at `cc7d7534b` and the corrected owner `cd8ed4c53`, using the same generated fixture and five iterations per sample:

| Shape | ns/op before / after | B/op before / after | allocations before / after |
| --- | ---: | ---: | ---: |
| 21,748 scalar/list rows | 6,884,750 / 6,631,167 | 12,225,048 / 11,703,075 | 109,032 / 87,283 |
| 25 metadata nodes | 10,517 / 9,400 | 17,040 / 16,416 | 241 / 215 |
| 25 vector-bearing nodes | 1,513,233 / 1,538,792 | 13,204,982 / 13,204,339 | 3,467 / 3,441 |

Timing ranges overlap; these captures establish lower allocation cost, not a precise latency improvement. Three additional 100-iteration samples cover ordinary 100-row hits in both cache owners: scalar allocations fall 516→415 (query cache) and 513→412 (smart cache); entity allocations fall 916→815 and 913→812. Their bytes fall too; the smart entity timing changes 38.8→39.8 µs with overlapping ranges. No latency win is claimed for that arm.

Integrated public cache-hit/put mutation isolation, node-copy, overflow/cycle, map metadata and nil/empty-cell controls pass under race instrumentation (storage 2.370 s; Cypher 2.437 s). The 13.2 MB vector control explicitly requests mutable vectors, so those bytes remain required output ownership. These are cache-owner qualification workloads, not the unavailable private application's full text/vector wire benchmark.

```sh
go test -tags=noui,nolocalllm ./pkg/cypher -run '^$' \
  -bench '^BenchmarkNOR307QueryResultCache$/(list-21748|metadata-25|vectors-25)$/query-cache/hit$' \
  -benchmem -benchtime=5x -count=3 -cpuprofile=copy.cpu -memprofile=copy.mem
```

## Large native collection/document traversal

The optional `BenchmarkTraversalLabSeedShapeAControls` and `BenchmarkTraversalLabSeedShapeBKeyedLookups` reproduce the native generator in the NOR-414 laboratory comment: 172,000 nodes, 5,280 documents, sequential native writes into in-memory Badger in `personal_documents`, and constraints installed after loading. Shape A has 351 membership relationships plus 289,649 unrelated relationships; shape B has 5,280 memberships, 284,720 unrelated relationships and one version/original relationship. The full original forward control remains required. Every timed public read runs in a fresh explicit transaction with result caching disabled; construction, BEGIN/ROLLBACK and complete-row assertions are untimed.

Frozen main `acf7426b8` versus candidate `b85a2e619` with the same fixture, three independent samples of three reads per arm, medians:

| Parser / statement | ms/op before / candidate | B/op before / candidate | allocations before / candidate |
| --- | ---: | ---: | ---: |
| nornic / inverse | 7.26 / 5.99 | 3,507,490 / 3,309,746 | 48,028 / 40,937 |
| nornic / forward control | 870.27 / 82.19 | 405,338,610 / 26,399,056 | 6,855,808 / 124,832 |
| antlr / inverse | 7.44 / 6.04 | 3,663,781 / 3,227,096 | 48,362 / 40,760 |
| antlr / forward control | 873.42 / 80.66 | 405,445,989 / 22,831,077 | 6,856,075 / 117,326 |
| nornic / collection documents | 1,302.45 / 306.60 | 404,080,274 / 32,335,597 | 6,846,604 / 206,636 |
| nornic / content document | 3.83 / 1.03 | 1,062,154 / 80,850 | 19,705 / 867 |
| antlr / collection documents | 1,304.26 / 306.49 | 404,047,181 / 32,323,085 | 6,846,460 / 206,640 |
| antlr / content document | 3.87 / 1.04 | 1,076,261 / 67,984 | 19,705 / 867 |

Collection-document candidate timings range 259–311 ms and 259–309 ms. These are improvements over current main, not proof of restored `b6d8e762a` performance. A subsequent integrated capture after the slice-header correction and shared benchmark-loop extraction passes all exact rows (63.849 s including setup); it is a smoke qualification, not another paired sample set. Query-owner CPU frames identify repeated traversal count-store statistics in the remaining collection-read cost. Whole-capture allocation profiles are dominated by fixture loading; CPU labels include timer bookkeeping, so no whole-capture percentages are presented as query cost.

```sh
go test -tags=noui,nolocalllm ./pkg/cypher -run '^$' \
  -bench '^BenchmarkTraversalLabSeedShape' -benchmem -benchtime=3x -count=3 \
  -cpuprofile=traversal.cpu -memprofile=traversal.mem -timeout=30m
```


## MATCH-local planner statistics

The large collection read repeats a keyed MATCH for each UNWIND row. The traversal owner previously reread the same label/type counters on each row. A public 12-row reproduction records 48 requests for four counter identities, under both parsers and transaction modes. One ephemeral MATCH-owned memo now reuses those sampled planning estimates across streaming and materialized rows. Exact request identity stays distinct; errors are not retained and cached reads still observe cancellation. Actual count results and row-visibility decisions do not use this memo.

Three paired samples of three reads compare pushed `2eb67aeaf` with the isolated correction `566cf849d`, on the same native large fixtures:

| Parser / collection documents | ms/op before / after | B/op before / after | allocations before / after |
| --- | ---: | ---: | ---: |
| nornic | 264.35 / 84.70 | 32,039,037 / 22,213,909 | 201,043 / 105,705 |
| antlr | 263.99 / 83.43 | 31,957,829 / 21,508,917 | 201,070 / 103,931 |

The other original statement controls still return their complete expected populations. Their timing/allocation variation is not presented as a gain. The 1K/16K native stored-link control retains identical median allocation counts (241/265 Nornic, 242/266 ANTLR); byte and timing ranges overlap. No cache is allocated for an unseeded streaming continuation or a single materialized row that cannot reuse estimates. No global, transaction-wide or retained graph cache is added.

Focused fail-first, freshness/type-identity, materialized-write, cancellation, seed choice and traversal controls pass under race instrumentation (12.445 s). The integrated correction plus native link controls passes in 31.446 s. These are focused checks; full CI and same-machine historical-build qualification remain separate. The source changes only private traversal inputs and planning work; public Go APIs, storage keys/payloads and WAL are unchanged.

## Cold retrieval admission after schema publication

The existing `BenchmarkRetrieveAfterSchemaPublication` now distinguishes warm statements from unique-source admission, before and after schema mutation. Cold arms use a distinct projection alias for every read and assert that alias and the same three result rows. The generated fixture has 12,500 sources; result caching is disabled. DDL, fixture construction, index warmup and result assertions are outside the timer. Successful admission/text caches can remain warm only in the warm arms; this is not a claim that every search-internal cache is cold.

Three samples of three reads compare frozen main `acf7426b8` with candidate `e81f32e06`, using identical benchmark source. Medians:

| Parser / admission | ms/op before / after | B/op before / after | allocations before / after |
| --- | ---: | ---: | ---: |
| Nornic / warm | 1.494 / 1.436 | 292,080 / 292,056 | 509 / 510 |
| Nornic / warm after DDL | 1.578 / 1.783 | 271,400 / 291,968 | 715 / 507 |
| Nornic / cold | 1.725 / 1.741 | 383,269 / 383,080 | 1,570 / 1,571 |
| Nornic / cold after DDL | 1.843 / 1.692 | 491,837 / 382,192 | 1,773 / 1,566 |
| ANTLR / warm | 1.801 / 1.518 | 292,064 / 292,232 | 509 / 510 |
| ANTLR / warm after DDL | 1.624 / 1.760 | 259,730 / 292,096 | 703 / 508 |
| ANTLR / cold | 1.978 / 1.701 | 383,349 / 383,530 | 1,568 / 1,569 |
| ANTLR / cold after DDL | 1.917 / 1.767 | 490,240 / 385,600 | 1,772 / 1,569 |

Every measured result passes. Warm-after-DDL byte medians increase, but their observed ranges overlap: Nornic 259,658–399,386 before and 152,802–292,229 after; ANTLR 259,688–424,589 before and 291,984–292,205 after. This short sample does not establish a systematic byte regression or a precise latency improvement. It establishes fewer after-DDL allocations and lower cold-after-DDL byte medians on this workload; ordinary cold admission remains approximately unchanged in allocations. Whole-capture profiles include untimed setup and must not be presented as query-only attribution. The private application replay and external-provider/concurrent workloads remain unqualified.

## Chunk-payload pressure on metadata reads

`BenchmarkChunkHydrationCallChainReads` and `BenchmarkChunkHydrationFulltextQueryNodes` reuse the native managed-sidecar fixture with 512 sources and either 8 or 128 chunks per source, each holding 2,048 float32 values. The source payload increases from 32 MiB to 512 MiB while result size remains fixed. Both parsers, automatic and explicit transactions, CALL subqueries, bundled READ procedures and 25-result fulltext queries assert their complete expected rows. The common benchmark lifecycle excludes setup, transaction boundaries and assertions from timing.

Three samples of three reads on isolated candidate `b96a45388` give the following medians. Each pair is **32 MiB / 512 MiB source payload**, not old versus new production code:

| Parser / read | ms/op | B/op | allocations/op |
| --- | ---: | ---: | ---: |
| Nornic / automatic CALL subquery | 25.78 / 29.08 | 13,330,000 / 14,540,000 | 161,910 / 176,682 |
| Nornic / explicit CALL subquery | 33.80 / 44.76 | 17,520,000 / 24,010,000 | 239,294 / 285,766 |
| Nornic / automatic READ procedure | 11.58 / 12.58 | 6,742,000 / 6,742,000 | 78,073 / 78,073 |
| Nornic / explicit READ procedure | 16.77 / 22.54 | 9,800,000 / 14,170,000 | 133,931 / 157,870 |
| Nornic / automatic fulltext, 25 rows | 0.415 / 0.478 | 209,421 / 228,416 | 1,916 / 2,193 |
| Nornic / explicit fulltext, 25 rows | 2.803 / 3.815 | 2,682,669 / 3,199,424 | 32,609 / 39,693 |
| ANTLR / automatic fulltext, 25 rows | 0.404 / 0.488 | 209,549 / 230,144 | 1,917 / 2,200 |
| ANTLR / explicit fulltext, 25 rows | 2.825 / 3.932 | 2,683,101 / 3,201,714 | 32,615 / 39,730 |

CALL byte figures are rounded. These results do **not** show flat allocations across payload sizes. They measure metadata-sized work rather than a 512 MiB per-query decode. The independent poisoned-chunk and full-image-negative public controls establish which read route is used. The 256 MiB node-cache budget remains unchanged; the reason for the residual size-dependent metadata cost is not isolated by this comparison. Result caching is disabled, while retained corpus, admission and search statistics can be warm. This is generated CALL/fulltext qualification, not the private 172K-node application replay or a new large HNSW benchmark.


## Deferred traversal candidate hydration

The planner now prepares one canonical index candidate plan before choosing a traversal orientation. It hydrates the chosen endpoint, reads a singleton needed for binding, and drops larger unchosen ID listings without copying every candidate node. The same production resolution/read owners serve ordinary node MATCH, with no compatibility aliases. Inline ID listings preserve small-query allocation counts without retaining pointers into a copied plan's own array.

The unchanged temporary qualification harness runs on historical `b6d8e762a` and current `6aa745b73`; every expected row passes on both parsers. Native sequential-write fixture populations and untimed setup/transaction boundaries are unchanged. Three samples of three reads give medians:

| Parser / original statement | Historical ms/op / current | Historical B/op / current | Historical allocations / current |
| --- | ---: | ---: | ---: |
| Nornic / inverse | 4.643 / 4.238 | 2,540,034 / 1,913,688 | 37,046 / 20,961 |
| ANTLR / inverse | 4.764 / 4.209 | 2,557,685 / 1,912,069 | 37,089 / 20,956 |
| Nornic / forward control | 954.888 / 77.211 | 398,377,610 / 22,811,165 | 6,859,784 / 116,888 |
| ANTLR / forward control | 961.780 / 76.733 | 398,348,645 / 22,249,970 | 6,859,635 / 115,453 |
| Nornic / collection documents | 936.575 / 78.599 | 389,109,656 / 21,845,301 | 6,712,527 / 102,861 |
| ANTLR / collection documents | 942.374 / 78.274 | 389,085,061 / 21,090,904 | 6,712,555 / 101,000 |
| Nornic / content document | 2.556 / 0.935 | 982,408 / 61,594 | 19,250 / 822 |
| ANTLR / content document | 2.459 / 0.950 | 982,418 / 64,728 | 19,249 / 830 |

The inverse samples vary: ANTLR spans 4.197–4.913 ms and 1.912–2.564 MB, including the colder first sample. These short measurements support the shown workload reductions, not production throughput or a precise latency percentage. Whole profiles include fixture loading; only query-owner frames can support query attribution.

Ordinary selected-route controls use the existing 3,000-node seed corpus, public calls, disabled result caching, and complete expected rows/columns. Three longer 1,000-read samples compare `85e87ad43` with `6aa745b73`. Inline, equality and IN allocation medians remain unchanged on both parsers; OR drops 122→121 allocations (Nornic), 123→122 (ANTLR). Byte medians remain unchanged within 1 B for the former controls and fall 9/16 B for OR. Median time differences range +0.4% to +4.5%; no ordinary-query speedup is claimed. The first prepared-list prototype added one allocation and was corrected before qualification.

The public unselected-candidate probe falls from 131 node reads for two results to its bounded selected walk, while a control still materializes all 128 nodes when that result is actually requested. Pending/snapshot, cancellation/error, identity-value and admission controls remain separate from these benchmarks. No storage format, public API or retained graph cache changes.

## Bounded wildcard and presence pages

Unranked node fulltext acquisition previously read all matching nodes and sliced the completed result. It now shares one typed row-window owner with relationship acquisition, preserving raw release order, exact rows and scores while stopping at `skip + limit` matches. Unranked zero limits acquire no documents; ranked zero-limit calls retain their corpus acquisition behavior. No substring dialect, score, index or persistent-format change is introduced.

The existing managed-chunk benchmark adds wildcard/presence pages (skip 5, limit 25) and unlimited controls. Three samples of three reads compare `6aa745b73` with the corrected source, across both parsers, automatic/explicit transactions and 32/512 MiB source payloads. All 24 arms assert complete expected results. Selected medians:

| Payload / parser / statement mode | ms/op before / after | B/op before / after | allocations before / after |
| --- | ---: | ---: | ---: |
| 8 chunks/source / nornic / wildcard-page / explicit=false | 3.402 / 1.558 | 3,160,954 / 1,311,525 | 48,859 / 20,634 |
| 128 chunks/source / nornic / wildcard-page / explicit=false | 6.411 / 2.210 | 7,463,237 / 1,489,901 | 63,622 / 24,443 |
| 128 chunks/source / nornic / wildcard-page / explicit=true | 6.339 / 2.084 | 6,983,962 / 1,467,312 | 62,559 / 24,415 |
| 128 chunks/source / antlr / presence-page / explicit=false | 6.018 / 2.001 | 6,956,730 / 1,464,800 | 61,394 / 24,328 |

Every bounded arm reduces measured bytes, allocations and time. Short unlimited samples fluctuate by a few allocations; longer 20-read, three-sample 32 MiB controls show no systematic allocation increase: Nornic automatic 50,806→50,785 and explicit 50,807→50,787; ANTLR automatic remains 50,792 and explicit remains 50,794. Unlimited byte differences are below 0.03%. No unlimited speedup or production-throughput claim is made. Profiles include fixture setup and are not query-only CPU attribution.

The fail-first five-row public page probe reads all 64 nodes before correction. Both-parser automatic/explicit row, order, tail, empty, wildcard and presence controls pass after correction. Live-engine acquisition counts are observed only for automatic reads; explicit native transactions bypass that meter and have exact-result coverage. Focused release-oracle and lifecycle race controls pass (4.072 s). Full-suite verification remains CI-owned.


## Cached fulltext node images

`BenchmarkCachedFulltextQueryNodeImages` serves the actual public 25-row fulltext
query through the result cache, separately returning metadata and node objects.
Each calibration invalidates its owned result cache before untimed cold setup;
cache counters prove that every measured invocation is a hit. The existing
managed-chunk fixture holds 512 source nodes with 8 or 128 chunks each (32 or
512 MiB of stored vectors). Twenty reads per sample, three samples, Apple M2 Max:

| Parser / result | 32 MiB source: µs / B / allocations | 512 MiB source: µs / B / allocations |
| --- | ---: | ---: |
| Nornic / metadata | 43.094 / 6,236 / 100 | 43.123 / 6,227 / 100 |
| Nornic / nodes | 54.404 / 27,877 / 276 | 49.962 / 27,875 / 276 |
| ANTLR / metadata | 43.283 / 6,355 / 101 | 43.423 / 6,355 / 101 |
| ANTLR / nodes | 50.660 / 28,005 / 277 | 54.473 / 28,010 / 277 |

These are payload-size controls, not historical speedup or private-application
replay. Metadata-sized results do not carry unused chunk vectors. Public
both-parser mutation tests change received rows, columns, labels and properties,
then verify later hits and native full reads. Public SET changes the returned
source title and verifies fresh results; under this fixture's embedding-text
policy, that write correctly invalidates the old managed vectors. Native reads
still return every stored chunk before that legitimate invalidation.

## Candidate-reader consolidation controls

One typed admission request now feeds the shared ID-to-node read kernel. The
storage engine is a separate argument: embedding it in the request made Go's
escape analysis retain the entire prepared plan, adding one 896-byte allocation
per query. That rejected form is not retained. Twelve obsolete collector entry
points, whose remaining consumers were tests, are removed; their component
checks now compose the canonical planner and reader.

`BenchmarkNodeCandidatePlanning` compares the corrected kernel with `901c4ce8a`
using the same 3,000-node fixture, disabled result cache, exact rows, 1,000 reads
per sample and three samples under CPU/memory profiling. All ten parser/route
arms retain their allocation counts (Nornic inline/equality/combined/IN/OR:
96/89/106/129/121; ANTLR: 97/90/107/130/122). Byte medians vary by at most two
bytes. Median timing differences range +0.25% to +4.59%; no speedup is claimed.
Focused component race controls pass in 9.254 s; integrated public traversal,
prepared-ID and cache-image race controls pass in 12.696 s. The unchanged lexical
population assertion passes after reviewing exactly twelve retired members;
no analyzer rule, threshold or exclusion changes.


## PROFILE source metadata: ordinary expression and grouping cost

Paired local qualification compares the pushed candidate `57c31c273` with
`d295e3f42`, the PROFILE prototype rebased onto that candidate. Apple M2 Max,
Darwin arm64; three serial samples per arm, CPU and allocation profiles captured.
These measurements cover ordinary execution without PROFILE, not overall query
throughput or completeness of PROFILE operator observation.

The existing `perRowDocGraph` fixture supplies the executor. Eight- and nine-item
lists contain repeated `x + 1` expressions with `x = 7`, and every returned item
is checked. Text evaluation disables row-value programs deliberately; this
isolates the fixed-buffer split boundary and bound-identifier lookup. Each sample
executes 100,000 iterations. The temporary harness is
`/tmp/v150-profile-normal-bench_test.go`; paired logs/profiles are
`/tmp/v150-profile-{main,integrated}-text.{log,cpu,mem}`.

| List width | Before ns/op | After ns/op | Before B/op | After B/op | Before allocations | After allocations |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 8 | 4,272 | 4,368 | 280 | 152 | 3 | 2 |
| 9 | 4,860 | 5,012 | 568 | 424 | 4 | 3 |

Medians increase 2.2% and 3.1% in time while removing one allocation and 128/144
bytes. No ordinary-expression latency improvement is claimed. Earlier cold/warm
program controls remain separate evidence; this paired run covers text mode.

Grouping uses the public statement
`UNWIND $rows AS row RETURN row.k AS key, count(*) AS n`, 1,024 input maps,
disabled result caching, and 1/16/1,024 distinct keys. Every returned key and count
is asserted; samples execute 100 complete statements. Retaining each computed
group key avoids reevaluating its expression during projection, preserving
single-evaluation semantics for observable scalar/subquery expressions.

| Groups | Before ns/op | After ns/op | Before B/op | After B/op | Allocations before/after |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 349,291 | 342,695 | 6,491 | 6,642 | 79 / 79 |
| 16 | 358,197 | 350,023 | 30,665 | 31,298 | 272 / 272 |
| 1,024 | 1,087,404 | 1,025,765 | 1,697,957 | 1,730,842 | 12,396 / 12,396 |

Allocations stay unchanged. Bytes increase about 1.9–2.3% because the group keeps
the computed key value; the 1,024-group case retains about 32 KiB more per
operation. This is an explicit correctness cost, not a claimed memory win.
The temporary public harness is `/tmp/v150-aggregate-grouping-bench_test.go`, with
paired evidence `/tmp/v150-profile-{main,integrated}-groups.{log,cpu,mem}`. Whole
CPU captures include fixture setup; they do not establish query-only attribution.
The rebased focused PROFILE/EXPLAIN/expression/cache race selection passed in
15.238 seconds. Full integrated CI and predicate-source attribution remain pending.


### Ordinary public predicate execution with source observation disabled

The existing `BenchmarkPerRowExpression_Rel450` fixture exercises 450 relationship
rows, preserving the expected count of 450 in every invocation. Qualification
found that its unused nonce restarted between repeated benchmark samples, allowing
later samples to return cached results. The retained benchmark-helper correction
uses the existing explicit executor cache policy (`MaxEntries: 0`) and asserts that
the result cache is disabled. The same correction is overlaid onto the baseline;
no fixture writer or second benchmark runner is introduced.

Three serial 100-iteration samples compare `2de922857` with the integrated
PROFILE/predicate correction. Both sides use the corrected helper, default
parser, warm syntax/program caches and no PROFILE collector.

| Predicate | Before ns/op | After ns/op | Before B/op | After B/op | Allocations before/after |
| --- | ---: | ---: | ---: | ---: | ---: |
| Property comparison | 1,375,340 | 1,421,818 | 1,837,198 | 1,837,260 | 14,287 / 14,287 |
| `NOT any` over 200 keys | 10,352,920 | 10,687,816 | 2,398,226 | 2,398,312 | 22,692 / 22,692 |

Median time rises about 3.4%/3.2%; allocations are unchanged and byte differences
are below 0.01%. These are workload controls, not a latency improvement. Evidence:
`/tmp/v150-profile-public-predicates-{before,after}.{log,cpu,mem}`. The rejected
mixed cached/uncached run is preserved as
`/tmp/v150-profile-public-predicates-cache-mixed.log` and excluded from this table.


## Integrated PROFILE follow-up: exact retrieval populations

The existing disk-backed retrieval benchmark now keeps its default-cutoff n3/n300
arms and adds separate `minRRFScore: 0` arms. These use the shipped public option
and assert exactly three or 300 rows, rather than treating a request for 300 as
proof that 300 were returned. The same benchmark source runs on the pre-PROFILE
production source (2de922857-equivalent Go files) and the integrated candidate.
The 12,500-node fixture has 356 predicate-admitted sources, 1,024 named vectors,
a property index, sequential native writes, and the same stub embedding/reranker.
Three three-operation samples per arm, measured serially; medians:

| Parser | Rows | Before ns/op | Current ns/op | Before B/op | Current B/op | Before allocs/op | Current allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Nornic | 3 | 23,448,972 | 23,793,709 | 5,897,706 | 5,897,653 | 38,487 | 38,473 |
| Nornic | 300 | 52,779,000 | 54,044,306 | 23,642,208 | 23,826,522 | 145,948 | 145,966 |
| ANTLR | 3 | 23,550,083 | 23,348,403 | 5,898,802 | 5,796,728 | 38,477 | 38,482 |
| ANTLR | 300 | 52,242,916 | 53,076,153 | 23,703,378 | 23,823,322 | 145,955 | 145,960 |

Every row-count, identity, uniqueness and order assertion passes. These short
samples establish the requested populations; overlapping byte/allocation ranges
do not establish a new per-call allocation, and no timing or memory improvement
is claimed. CPU/memory captures include untimed setup and cannot be interpreted
as query-only whole-capture hotspot percentages. Logs and captures use
`/tmp/v150-exact300-{nornic,antlr}-{before,after}.*`.

Fulltext scope qualification additionally checks exact row order and score bits
against the existing release oracle: single labels, label unions in either order,
multi-label deduplication, changed corpus statistics, relationship types,
wildcards, and the implicit default corpus. The default retains its shipped fixed
field list; it is not all string properties. No second scoring implementation is
introduced. Integrated fulltext and PROFILE publication/predicate race controls
pass (6.687 s). This is focused proof, not full-suite qualification.

## Ordinary count-store execution after physical PROFILE attribution

The public 150-document / 450-relationship fixture and existing uncached query
runner measure label and type counters without PROFILE. Three 10,000-operation
samples per arm, serial CPU/memory capture; medians:

| Shape | Before ns/op | Current ns/op | Before B/op | Current B/op | Before allocs/op | Current allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Label count (150) | 27,101 | 27,330 | 4,264 | 4,264 | 83 | 83 |
| Type count (450) | 29,570 | 29,668 | 4,832 | 4,832 | 84 | 84 |

This is a completeness correction, not a performance improvement claim. It adds
no ordinary allocation or byte-median regression; latency differs by +0.8%/+0.3%.
The same public benchmark asserts exact scalar values on every iteration. PROFILE
uses the same fast reduction; it does not materialize graph entities to obtain
metrics. Captures: `/tmp/v150-profile-count-{before,after}.{log,cpu,mem}`.

```sh
go test -tags=noui,nolocalllm ./pkg/cypher -run '^$' \
  -bench '^BenchmarkPhysicalCountOrdinaryExecution$' -benchtime=10000x \
  -count=3 -benchmem -cpuprofile=/tmp/count.cpu -memprofile=/tmp/count.mem
```

## Ordinary execution after fused route attribution

Apple M2 Max; existing uncached read fixture and SeedBareCreate workload,
pre-attribution candidate versus integrated source. These are ordinary queries,
not PROFILE overhead measurements. Three samples per arm; every write sample
checks the exact 500*b.N stored population outside the timer.

| Workload | Baseline ns/op | Candidate ns/op | Baseline B/op | Candidate B/op | Baseline allocs/op | Candidate allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Filtered count | 97,930 | 101,718 | 119,953 | 119,953 | 975 | 975 |
| Bounded projection | 31,486 | 32,319 | 5,009 | 5,009 | 74 | 74 |
| Create 500 products | 7,219,597 | 7,144,069 | 5,920,437 | 5,957,002 | 59,678 | 59,668 |

Read samples use 1,000 operations; write samples use three operations. Write
bytes and allocations have overlapping ranges; no systematic change or speedup
is established. The normal UNWIND plan retains its prior layout; source state
is installed only for PROFILE. Captures include setup and untimed validation.

## Scalar GDS observation overhead

The existing BenchmarkGDSNodePropertyRead/integer builds 5,000 nodes and 20,000
weighted relationships, then measures ordinary (not PROFILE) property reads.
Three samples of 1,000 operations compare the pre-observation implementation
against the integrated source on Apple M2 Max. Median ns/op is 19,127 to 19,504
(+2.0%); B/op 2,696 to 2,695; allocations remain 48. No material change or speedup
is established. Exact projection/callback/adjacency results have separate public
race assertions. CPU/memory captures include setup, so process-wide scheduler
and GC percentages do not identify a query-only bottleneck.

## Shared population observation: ordinary controls

The new token-listing workload reuses the existing 451-node/450-relationship
fixture and uncached scalar execution runner; it asserts 4 labels, 1 type and
4 property keys on every operation. Three samples of 300 operations, ordinary
execution, Apple M2 Max:

| Listing | Before ns/op | After ns/op | Before B/op | After B/op | Allocs/op both |
| --- | ---: | ---: | ---: | ---: | ---: |
| Labels | 203,454 | 201,748 | 384,483 | 384,487 | 3,320 |
| Relationship types | 260,956 | 265,440 | 382,015 | 382,014 | 5,408 |
| Property keys | 469,148 | 466,259 | 757,410 | 757,410 | 8,620 |

The GDS property control after population-recorder consolidation had +6% median
latency in a short 1,000-operation sample. A matched longer pair of three
10,000-operation samples measures 18,504 to 18,273 ns, 2,689 to 2,690 bytes and
48 median allocations on both sides (one sample per side reports 49). No
consistent regression or speedup is established. Profiles include setup; the
metrics qualify these workloads, not general database throughput.

## Native callable observation policy

Workload: existing BenchmarkPluginHandlerIdentity, pre-admitted opaque identity
function, ordinary non-PROFILE execution, one million operations per sample,
three samples per side. Apple M2 Max, Go 1.27.3, noui/nolocalllm. Baseline
4efdddacedca41872b65586af802209277bd8334; candidate adds the typed callable
policy and sealed invocation-phase receipt. CPU and memory profiles were
captured independently on both sides; no concurrent local Go work ran.

| Measurement | Before | Candidate | Interpretation |
| --- | ---: | ---: | --- |
| Median ns/op | 187.0 | 191.2 | +2.2%; ranges 185.6–194.1 / 191.1–197.3 overlap |
| B/op | 56 | 56 | unchanged |
| allocs/op | 3 | 3 | unchanged |

No speedup or allocation reduction is claimed. PROFILE's deliberate ledger
allocations and other callback signatures are outside this ordinary-call
measurement. Public both-parser controls separately verify correct results,
opaque custom/callback reads, rejected admission and entered-error/panic phases.

```sh
go test -tags=noui,nolocalllm ./pkg/cypher -run '^$' \
  -bench '^BenchmarkPluginHandlerIdentity$' -benchmem \
  -benchtime=1000000x -count=3
```

## Shared graph token acquisition

Baseline ee478a43712fee896f805b4e6fb8c9ca2aaf6792 versus the shared token
reader/error correction. Existing uncached 451-node / 450-relationship fixture,
three samples of 300 operations per arm, M2 Max / Go 1.27.3, noui/nolocalllm.
The statistics arm also asserts its four-label answer. Runs and CPU/memory
captures were serialized; no concurrent Go work overlapped them.

| Query | Before ns/op | After ns/op | Before B/op | After B/op | allocs/op, both |
| --- | ---: | ---: | ---: | ---: | ---: |
| Labels | 199858 | 201851 | 384488 | 384504 | 3320 |
| Relationship types | 261314 | 264697 | 382013 | 382015 | 5408 |
| Property keys | 470102 | 468048 | 757413 | 757414 | 8620 |
| Graph statistics | 423409 | 426876 | 753882 | 753880 | 8585 |

All latency ranges overlap; allocation counts are unchanged and byte changes
are within observed sample variation. No speedup or broad workload guarantee
is claimed. The correction removes masked population errors and duplicate
acquisition/insertion code without an additional measured map/callback allocation.

```sh
go test -tags=noui,nolocalllm ./pkg/cypher -run '^$' \
  -bench '^BenchmarkTokenListingOrdinaryExecution/' -benchmem \
  -benchtime=300x -count=3
```


## Search observation overhead controls

Ordinary uncached endpoint retrieval reuses the existing one-edge/two-node
public query fixture, three samples of 1,000 operations. Median latency is
56,895 -> 57,214 ns (+0.56%); allocation count stays 234; bytes are
20,288 -> 20,302, with overlapping ranges. Native predicates and consumers
retain full vectors; ordinary row endpoints use the admitted image contract.

The unchanged complete-continuation benchmark runs three samples of 20
operations against identical memory fixtures. The before binary overlays only
the four changed production readers with HEAD source; the new private-signature
component test is excluded from that baseline build. Both binaries include the
same existing workload and run alone, with CPU and memory captures.

| Ordinary complete build | Before ns/op | After ns/op | Before B/op | After B/op | Before allocs/op | After allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| ID, 20,000 members | 19,941,562 | 20,232,600 | 23,020,312 | 23,019,983 | 345,479 | 345,452 |
| Grouped ID, 1,000 passages | 1,711,390 | 1,658,804 | 3,294,519 | 3,294,514 | 28,107 | 28,107 |

Latency differs +1.46% / -3.07%, with overlapping sample ranges; no speedup
is established. Variable 20K allocation samples overlap too. No additional
systematic allocation cost is established. Profiles include fixture creation
and test lifecycle, so whole-process percentages are not query-only evidence.
Evidence: `/tmp/v150-continuation-cost-{before,after}.{txt,cpu,mem}` and
`/tmp/v150-rag-relationship-comparison.json`. These are workload controls,
not qualification of the unavailable private application corpus.


## Typed label-seeded traversal and graph observation controls

The public label-seeded one-type traversal previously omitted the type in its
storage request. The regression reads 18 adjacency headers to return two
memberships before the correction, on both parsers. The corrected canonical
scan plan supplies the existing typed-adjacency request; both directions and
type unions retain exact result sets and observed populations. No new index,
storage format, retained cache or alternate execution route is introduced.

Paired serialized benchmarks use the same generated two-document memory
fixture, differing only in the scan-plan source overlay. Each arm has three
samples of 1,000 operations on M2 Max, with CPU and memory profiles.

| Unrelated relationships per document | Before ns/op | After ns/op | Before B/op | After B/op | Before allocs/op | After allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 8 | 65,933 | 62,665 | 22,402 | 16,178 | 276 | 218 |
| 64 | 104,086 | 68,444 | 67,713 | 16,177 | 624 | 218 |

Medians show 4.96% / 34.24% lower latency and 27.78% / 76.11% fewer bytes
for these small generated workloads; these are not application-corpus timings.
The exact observed request population, rather than latency alone, demonstrates
that unrelated relationship headers are no longer acquired by this route.

Graph/path observations reuse the existing identity read view and shared
adjacency recorder. Longer algorithm controls (three samples, 1,000 operations)
retain allocation counts: PageRank unfiltered 1,492 and filtered 1,503; closeness
unfiltered 34,193 and filtered 36,988. Median latency changes range -2.71% to
+3.05%. A profiled neighbors sample varied from 718 to 719 allocations; exact
heap comparisons did not identify a fixed added observer allocation. A separate
matched repeat without CPU profiling reports 718 allocations on both sides,
134,423 B before / 134,423 B after, and 439,564 / 445,305 ns (+1.31%).
Whole-process heap captures include fixture and Badger background work; they
are not presented as isolated query allocations. The ordinary matched repeat
and nil-observer source path establish no systematic added allocation in this
workload, rather than claiming that all heap differences are explained.

Evidence: `/tmp/v150-label-scan-{before,after}.{txt,cpu,mem}`,
`/tmp/v150-graph-read-{before,after}-long.{txt,cpu,mem}`,
`/tmp/v150-neighbors-{before,after}-control.txt` and exact heap comparison
`/tmp/v150-neighbors-exact-allocation-diff.txt`.


## Retrieval candidate observation overhead

Paired serialized ordinary-query controls use the same public fixtures with
query/result caches disabled. The three-node BM25/hybrid fixture explicitly
selects the existing CPU brute-force strategy and disables GPU construction,
avoiding an asynchronous HNSW strategy change during the sample. The baseline
and candidate binaries share the fixture and query text. Each arm has three
1,000-operation samples with CPU and memory profiles on M2 Max / Go 1.27.3.

| Query | Before ns/op | After ns/op | Before B/op | After B/op | allocs/op, both |
| --- | ---: | ---: | ---: | ---: | ---: |
| Relationship endpoints | 58,848 | 59,750 | 20,313 | 20,307 | 234 |
| Node BM25 | 53,264 | 52,222 | 17,115 | 17,121 | 206 |
| Node hybrid | 85,792 | 84,520 | 21,286 | 21,291 | 265 |

Latency changes range -1.96% to +1.53%, and allocation counts are unchanged.
Small byte changes overlap measured sample ranges; no speedup or large-corpus
qualification is claimed. An earlier unpinned fixture allowed HNSW/Metal
construction and is not used for this matched comparison.
Evidence: `/tmp/v150-rag-cpu-{before,after}.{txt,cpu,mem}` and
`/tmp/v150-rag-cpu-comparison.json`. The CDC/temporal/log additions have focused
correctness/race evidence, without a separately measured ordinary latency claim.


## Operational metadata observation controls

Eight reviewed native procedures reuse the count/catalog owners or prove no
graph acquisition in their shipped bodies. Injected identity, settings,
connection and permission providers remain explicitly unobserved at actual
invocation. The private permission owner returns provenance alongside its
existing function, avoiding an extra forwarding closure or resolver allocation.

Paired serialized ordinary controls use identical empty native fixtures,
query caching disabled, three samples of 1,000 operations with CPU/memory
captures. Counts retain their one-response/one-request units.

| Procedure | Before ns/op | After ns/op | Before B/op | After B/op | allocs/op, both |
| --- | ---: | ---: | ---: | ---: | ---: |
| db.info | 16,393 | 16,684 | 5,081 | 5,081 | 66 |
| nornicdb.stats | 19,578 | 18,743 | 6,105 | 6,104 | 87 |
| db.awaitIndexes | 15,893 | 15,860 | 4,080 | 4,080 | 46 |
| dbms.listConfig, version filter | 64,748 | 68,744 | 53,992 | 53,992 | 408 |

The short configuration arm initially reports +6.17% median latency with
overlapping sample ranges. The targeted longer repeat (three 10,000-operation
samples without CPU profiling) is 63,346 -> 63,376 ns (+0.05%), retaining
53,992 B and 408 allocations. No systematic configuration latency regression
or speedup is established; allocation counts are unchanged in every arm.
These workload controls do not qualify arbitrary custom provider cost.
Evidence: `/tmp/v150-operational-{before,after}.{txt,cpu,mem}`,
`/tmp/v150-operational-config-{before,after}-long.txt` and
`/tmp/v150-operational-comparison.json`.


## Public vector-query cache payload scaling

The public vector-query cache regression now shares the fulltext cache fixture
and detached-image assertions. Both parsers execute a cold named-index query,
then two actual cache hits after mutating the previously returned node, row and
column values. A positive cold metadata-read control establishes that the read
probe is connected; both subsequent hits perform zero full or metadata node
reads. Exact top-25 IDs and titles remain unchanged, and native storage retains
all managed chunk vectors. Ranking uses distinct managed first-chunk vectors,
without adding inline named-vector payloads or relying on tied ANN scores.

Focused combined cache-image race checks pass in 11.796 s. The serialized
public vector cache benchmark uses 512 source nodes, 2,048 dimensions, 25 output
rows, three samples of 100 operations, CPU/memory captures, and the default
parser. Index construction and the first query are outside the timer; every
iteration verifies the result and actual cache-hit counters.

| Stored source chunk payload | Median ns/op | Median B/op | allocs/op |
| --- | ---: | ---: | ---: |
| 32 MiB, 8 chunks/source | 86,450 | 185,727 | 2,335 |
| 512 MiB, 128 chunks/source | 87,770 | 185,722 | 2,335 |

These are candidate payload-scaling controls, not a historical-head speedup
comparison or a replay of the private application corpus. Assertions run
outside the query timer. Whole-process profiles include the untimed fixture
and vector-index construction; those samples are not attributed to cache hits.
Earlier qualification attempts had invalid tied-order or inline-vector fixture
expectations and are not product regressions or passing evidence.
Evidence: `/tmp/v150-public-vector-cache-managed-ranking-qualification.log`
and `/tmp/v150-public-vector-cache-scaled.{txt,cpu,mem}`.


## GDS projection observation controls

Projection construction counts each node and relationship delivered by its
existing storage streams before projection filters. Project admission, ordinary
catalog lookups and both drop phases share one lock-held map-read owner; callers
reuse their already computed catalog keys. Atomic multi-name preflight, repeated
name behavior and cancellation-before-publication remain unchanged. Actual
injected permission resolvers remain opaque, including when the same execution
ledger observes a real catalog read made inside the callback.

Fail-first public tests reproduced the missing observations on both parsers.
The final integrated race selection passes in 78.170 s: populated and empty
projection streams, selective outputs, duplicate drop names, atomic failed
preflight, custom replacements, authorization and existing cancellation cases.
Additional zero-allocation long-key catalog-read and public controls pass in
1.092 s; the unchanged lexical population guard passes in 2.317 s. A denied
resolver is also checked at the drop owner, preserving the projection and
performing no catalog read; this component check is distinct from public query
admission, which can reject the statement before its handler runs.

Paired ordinary scalar projection samples use the existing native benchmark,
5,000 nodes / 20,000 relationships, three samples of ten operations and CPU/
memory captures. Catalog deletion is outside timing; no PROFILE observer exists.

| Metric | Before median | After median | Sample ranges |
| --- | ---: | ---: | --- |
| ns/op | 67,634,863 | 68,393,663 | 67,380,254–68,444,158 / 67,604,279–69,305,892 |
| B/op | 68,712,901 | 68,665,587 | 68,707,862–68,782,103 / 68,665,553–68,718,302 |
| allocs/op | 837,817 | 838,199 | 837,737–838,357 / 837,898–838,258 |

Latency changes +1.12%; allocation ranges overlap. These small samples do not
establish a speedup or identical whole-workload allocation totals. The direct
long-key control separately proves zero allocation in the changed ordinary
catalog reader, and `EntityVisitor` returns the original visitor without an
observer. Whole-process profiles include fixture/storage background activity.
Evidence: `/tmp/v150-gds-project-{before,final}.{txt,cpu,mem}`,
`/tmp/v150-gds-final-integrated.log`,
`/tmp/v150-gds-final-allocation-control.log` and
`/tmp/v150-gds-final-lexical.log`.


### Maintained literal candidates for named fulltext indexes

Named fulltext queries now select a conservative document population from a
maintained byte-trigram signature index before running the existing exact
matcher. This is deliberately separate from the stemmed/tokenized BM25 index:
that index cannot exclude candidates safely for the supported literal-substring
and field-phrase dialect. Positive Boolean clauses share the evaluator's
occurrence policy. Fuzzy, wildcard, regex, range, short-term and prohibited-only
queries retain exact matching over every candidate they require. Signatures
never establish a match or supply a score. Document frequencies still count
exact body substrings across the complete scoped corpus, independently for each
term, rather than using the final query's hit population.

The existing projection lease owns documents, signatures and incremental changes.
Deletion clears all old posting bits and reuses slots; replacement prepares its
signature before publication. The retained-cache report charges posting/slot
capacity and releases the entire derived generation under pressure. A detached
active reader can finish but cannot publish that generation again. Small
(<128-document), ordered, dynamically visible and request-local reads preserve
the existing acquisition/matcher contract; these are not indexed-candidate
performance claims for those routes. A first retained population still reads
its declared source corpus once. No storage key, payload, version or persistence
format changes.

Both public parsers agree exactly with the release matcher oracle on node and
relationship IDs, order and scores, including leading field whitespace, Unicode,
Boolean composition, boosts, short substrings and nonliteral patterns, before
and after updates/deletes. Component controls exercise chunk boundaries, arbitrary
bytes, replacement, slot reuse, real cancellation during hashing, unknown
selector rejection, complete-corpus frequencies, recovered broad-query proofs
and memory-pressure retirement of an active 256-document indexed projection.
Prepared selection allocations remain constant as population grows from 1,024
to 4,096 documents; this does not measure public entity hydration. Integrated
search/Cypher race controls passed in 4.852/14.956 seconds at the candidate source.

Paired generated public workload: Apple M2 Max, Go 1.27.3, native in-memory
Badger defaults, query-result caching disabled, four rotating distinct two-term
queries, exact warmup counts checked. Fixture/setup/closure are outside the query
timer. Baseline is `fa9f4014d56a0c4598b6c804e053cbe050da78b0` with the identical
benchmark fixture; candidate production fulltext files are unchanged by the later
GDS-only checkpoint. Three fixed 1,000-query samples, CPU/memory profiles enabled:

| 16,000-node workload (4,000 indexed documents, 400 results) | Baseline median | Candidate median | Change |
| --- | ---: | ---: | ---: |
| Time/query | 11.837 ms | 3.349 ms | -71.7% |
| Bytes/query | 3,038,670 | 3,029,141 | -0.31% |
| Allocations/query | 17,606 | 17,549 | -0.32% |

Baseline allocation range is 17,588–17,627; candidate is 17,540–17,553.
An earlier short 10/50-query comparison appeared to increase allocations.
The longer fixed-count comparison reverses that difference. Source and heap
review also distinguish Badger lookup/compaction allocations from candidate
selection; storage Get counts alone do not establish body-cache misses.
This evidence supports the named repeated-read workload, not a universal
allocation reduction or the unavailable private application replay.

Cold construction has a real cost. For 4,000 generated nodes with distinct
~2 KiB bodies (1,000 indexed documents), three 100-read samples recreating the
service each time measured baseline median 7.560 ms / 10,974,067 bytes /
17,095 allocations versus latest candidate 8.058 ms / 11,149,834 bytes /
17,177 allocations: +6.6% time, +1.6% bytes and +82 allocations to construct the
retained candidate structure. An earlier candidate sample measured +3.0% time;
the latest slower sample is preserved, not replaced with the favorable one.
Cold-cost qualification remains a separate open performance item. The warm
speedup does not prove cold parity. Constructor CPU profiles guided bounded
chunk-signature reuse, preallocation, single-bit tabulation and 4 KiB cancellation
polling; rejected dense-clear/double-bit variants are not retained.

Evidence: `/tmp/v150-fulltext-warm-repeat-{before,after}.{txt,cpu,mem}`,
`/tmp/v150-fulltext-literal-final-cold.{txt,cpu,mem}`,
`/tmp/v150-fulltext-literal-diverse-long-before.txt`, and
`/tmp/v150-fulltext-primary-integration-race.log`. These are local evidence
paths, not downloadable repository artifacts; rerun the committed benchmarks
for independent qualification. Full CI must run on the next pushed head.


#### Cold signature-workspace correction

The latest constructor uses immutable cached-signature references instead of
copying a 256-byte map value for every reused chunk. Admission remains bounded
to 32 constructor-only chunks; no per-document heap object is introduced once
that workspace is full. Exact byte/chunk/field tests remain unchanged.
Three 100-read distinct-body cold samples now measure 7.553 / 7.698 / 7.583 ms,
11,149,337–11,150,182 B/op and 17,177–17,178 allocations. The median is **+0.31%**
versus the original 7.560 ms baseline, compared with the earlier +6.6% variant.
Startup still allocates the new bounded candidate index (+1.6% bytes / about
83 allocations); repeated queries do not reconstruct it. This is the same
workload and baseline, not a substituted favorable operation. The earlier
slower variant remains recorded above. Integrated search/Cypher fulltext and
catalog race controls pass in 4.790/14.381 s.
Evidence: `/tmp/v150-fulltext-pointer-workspace-cold.{txt,cpu,mem}` and
`/tmp/v150-fulltext-pointer-catalog-integration-race.log`.


#### Integrated direct-reader qualification

A fresh serialized comparison includes the direct Cypher syntax reader, bounded
plan scratch, pointer-based constructor workspace and early cached-chunk merge.
The source receipt hashes the five affected production/fixture files before and
after the run; they remained unchanged. Both sides use the identical fixture
(SHA1 `21ce18a4f26e9f9a41b1e7e3884300ca78d8a7c7`), Apple M2 Max,
Go 1.27.3, result caching disabled and CPU profiling. Baseline is
`fa9f4014d56a0c4598b6c804e053cbe050da78b0`. Each side has five fixed
300-operation samples; fixture construction is outside the measured query.

| Workload | Baseline/current median time | Baseline/current median B/op | Allocations/op |
| --- | ---: | ---: | ---: |
| Cold 4K-node corpus, 1K indexed distinct bodies | 7.599 / 7.805 ms | 10,974,106 / 11,149,839 | 17,095 / 17,177 |
| Retained 16K-node corpus, rotating term sets | 11.997 / 3.460 ms | 3,059,417 / 3,047,132 | 17,594 / 17,599 |

Cold time ranges are 7.542–7.701 and 7.763–7.871 ms: the median cost is
+2.71%, within the repository's 5% latency gate. Cold allocation adds 175,733
bytes (+1.60%) and 82 objects to construct the bounded candidate index; it is
not per retained read. Warm time falls 71.16%; allocation ranges overlap
(17,498–17,702 versus 17,563–17,646), so no warm allocation improvement is
claimed. These current-source results supersede the unresolved +6.6%/+8.1%
variant framing without hiding those earlier measurements. They establish the
named generated workload, not unavailable private application data.

Evidence: `/tmp/v150-final-paired-fulltext-{baseline,candidate}.txt/.cpu`,
`/tmp/v150-final-paired-fulltext-candidate-source.json`, and
`/tmp/v150-final-paired-fulltext-summary.json`.


#### Relationship index cold preparation and retained memory

Generated stored populations exercise the existing constraint-owned equality
index at one, two and four properties, namespace/type isolation and the 16/17
retention boundary. The declaration builder, not the public query's warmed
lookup, is measured here. Each stored population includes one selected edge in
addition to the listed unrelated edges. Three fixed five-build samples, native
Badger in-memory, CPU-profiled and serialized on the same Mac:

| Unrelated edges / definitions / arity | Median cold build | B/build | Allocations/build | Retained heap delta |
| --- | ---: | ---: | ---: | ---: |
| 128 / 1 / 1 | 0.159 ms | 198,096 | 2,646 | 31,408 B |
| 4,096 / 1 / 2 | 11.915 ms | 14,525,412 | 209,173 | 2,624,704 B |
| 32,768 / 1 / 1 | 52.034 ms | 52,693,379 | 656,000 | 8,164,960 B |
| 32,768 / 1 / 4 | 180.187 ms | 190,372,216 | 3,049,223 | 35,146,144 B |
| 128 / 16 / 1 | 0.015 ms | 8,544 | 136 | 55,480 B |
| 128 / 17 / 1 | 0.013 ms | 8,544 | 136 | 55,480 B |

The last two rows time the small final definition (five stored relationships),
with preceding declarations outside timing. They measure cap publication and
retirement, not rebuilding the primary 129-edge definition 16 times. The owner
retains at most 16 definitions, not a fixed byte budget; larger/wider keys retain
more memory. Retirement chooses an arbitrary entry. Tests verify the actual
retained set, a retired definition's authoritative rebuild, exact namespace/type
results and unchanged retained-count bound, without assuming eviction order.

Heap deltas use two full GCs and keep the engine live. Baseline is the same
fixture before declaration; the delta includes derived postings/keys and any
entity cache retained by its type read, excludes stored fixture/Badger tables
already live, and includes collection noise. Large-case ranges are 8,164,808–
8,165,456 B (one property) and 35,145,872–35,154,600 B (four). These are absolute
phase costs, not a claim that original complete DDL latency or retained memory
was unchanged. Eager preparation pays this bounded derived-state cost so later
public lookups avoid the measured full-type materialization; warm query byte
and allocation improvements are reported above.

Public 128/4,096/32,768 exact-result tests pass on both parsers; storage cold,
cap-retirement and namespace/type controls pass. Raw evidence:
`/tmp/v150-relationship-index-cold-final.txt/.cpu`,
`/tmp/v150-relationship-index-cold-summary.json`, and
`/tmp/v150-final-suppression-relationship-focused.log`.
