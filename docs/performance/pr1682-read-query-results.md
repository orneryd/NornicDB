# PR #1682 read-query performance checkpoint

The native tables below qualify the frozen retained follow-up to published `2d0362da`: compact cache facts and benchmark acknowledgement/count corrections. The published2d036 source passed full `make test`(147 packages,36 fresh/111 cached). The retained follow-up has completed performance qualification and its fresh full `make test` is running; it is not yet declared merge-ready.

Apple M2 Max, Go1.27.3. Exact native script defaults:97,440 nodes,312,050 relationships,37,124,459 payload bytes; batch2,000,parallel4,seed42. Graph-only, nornic parser, normalization disabled. GC enabled: GOGC=400,GOMEMLIMIT=8GiB (a soft Go limit, not a hard RSS cap). No concurrent Go tests/builds, local-model generation or Docker workloads during the native timings.

## Native ingestion and persistence

| Mode / result cache | Ingestion acknowledgement(s) | Wipe + indexes + ingestion(s) | GC cycles |
| --- | ---: | ---: | ---: |
| durable / default | 8.252 | 8.313 | 21 |
| durable / off | 8.464 | 8.539 | 95 |
| wb / default | 9.742 | 9.801 | 17 |
| wb / off | 9.748 | 9.807 | 72 |
| strict / default | 8.491 | 8.661 | 21 |
| strict / off | 8.576 | 8.738 | 97 |

Synchronous default uses batched WAL sync; strict mode additionally enables immediate WAL sync and Badger synchronous writes. Write-behind acknowledgement is not durable completion. All six cells agree on9 seed counts and all14 row-count/result fingerprints. Graceful reopens are verified separately; neither post-query shutdown nor reopen proves crash recovery. The immediate-postACK controls below measure a complete-operation upper bound, including teardown.

## All14 native queries — result cache default

Milliseconds:median(p95). Five warmups;30 timed iterations. Only the existing result-cache policy differs; exact query text and normal statement/storage caches are unchanged.

| Query | Synchronous default | Adaptive write-behind | Strict durability |
| --- | ---: | ---: | ---: |
| products_per_category | 0.207 (0.231) | 0.240 (0.264) | 0.235 (0.257) |
| customer_category_distinct_orders | 0.176 (0.192) | 0.174 (0.190) | 0.176 (0.193) |
| optional_match_orders_count | 0.241 (0.262) | 0.244 (0.278) | 0.249 (0.278) |
| revenue_by_product | 0.170 (0.189) | 0.168 (0.200) | 0.168 (0.183) |
| products_by_supplier | 0.170 (0.206) | 0.178 (0.200) | 0.169 (0.196) |
| orders_by_customer | 0.154 (0.165) | 0.170 (0.202) | 0.171 (0.192) |
| revenue_by_category | 0.240 (0.260) | 0.201 (0.212) | 0.222 (0.240) |
| revenue_by_supplier | 0.155 (0.161) | 0.160 (0.177) | 0.175 (0.209) |
| revenue_by_customer | 0.196 (0.223) | 0.153 (0.162) | 0.173 (0.194) |
| order_line_sales_by_country | 0.144 (0.152) | 0.149 (0.167) | 0.160 (0.181) |
| low_stock_products | 0.219 (0.241) | 0.226 (0.252) | 0.249 (0.275) |
| products_in_category | 0.211 (0.232) | 0.219 (0.234) | 0.221 (0.231) |
| order_line_quantity_distribution | 0.145 (0.156) | 0.143 (0.159) | 0.165 (0.203) |
| customer_order_details | 0.228 (0.239) | 0.226 (0.242) | 0.240 (0.268) |

## All14 native queries — result cache off

Milliseconds:median(p95). Five warmups;5 timed iterations. Only the existing result-cache policy differs; exact query text and normal statement/storage caches are unchanged.

| Query | Synchronous default | Adaptive write-behind | Strict durability |
| --- | ---: | ---: | ---: |
| products_per_category | 627.721 (648.068) | 634.564 (651.385) | 632.408 (659.901) |
| customer_category_distinct_orders | 5258.662 (5293.184) | 5388.608 (5553.527) | 5301.360 (5406.910) |
| optional_match_orders_count | 3573.922 (3628.025) | 3755.529 (3810.785) | 3550.244 (3574.095) |
| revenue_by_product | 2263.777 (2267.744) | 2390.063 (2401.376) | 2236.317 (2239.259) |
| products_by_supplier | 627.894 (645.455) | 623.291 (636.986) | 640.538 (646.219) |
| orders_by_customer | 398.170 (416.000) | 439.478 (459.969) | 394.979 (410.032) |
| revenue_by_category | 3345.327 (3379.129) | 3577.521 (3655.966) | 3376.258 (3435.102) |
| revenue_by_supplier | 3335.653 (3400.776) | 3555.240 (3563.025) | 3329.987 (3380.781) |
| revenue_by_customer | 3123.390 (3186.756) | 3185.833 (3208.544) | 3146.816 (3173.131) |
| order_line_sales_by_country | 1971.581 (1977.967) | 1957.952 (1959.619) | 1952.451 (1959.903) |
| low_stock_products | 215.443 (219.807) | 215.361 (216.854) | 215.882 (222.964) |
| products_in_category | 2.377 (2.413) | 2.405 (2.543) | 2.341 (2.415) |
| order_line_quantity_distribution | 1975.444 (1991.510) | 2451.247 (2451.948) | 1952.961 (1955.878) |
| customer_order_details | 1.111 (1.268) | 1.139 (1.154) | 1.120 (1.136) |

## Matched-file allocation control

This allocation control was executed on published2d036, not rerun as a fresh retained-follow-up result. The follow-up changes write-time cache facts and seed checks; its read execution owners are unchanged. All three binaries open copies of identical closed Badger files. One timed Execute after five warmups; no transport, seed or fingerprint calculation is timed. This is per-operation diagnostic evidence, not a statistical small-latency-delta claim. CPU and sampled allocation profiles cover all14 queries.

| Query | Release f62 B/op | Follow-up B/op | Release allocs/op | Follow-up allocs/op |
| --- | ---: | ---: | ---: | ---: |
| products_per_category | 403,661,512 | 372,879,752 | 4,882,456 | 4,494,705 |
| customer_category_distinct_orders | 2,725,182,128 | 2,620,737,856 | 33,966,299 | 32,119,981 |
| optional_match_orders_count | 2,923,784,032 | 2,848,165,824 | 30,821,802 | 29,629,372 |
| revenue_by_product | 2,729,349,016 | 2,657,532,704 | 27,742,479 | 26,541,329 |
| products_by_supplier | 409,445,152 | 381,401,416 | 4,896,626 | 4,508,300 |
| orders_by_customer | 381,088,712 | 353,351,192 | 4,579,018 | 4,182,032 |
| revenue_by_category | 2,714,569,080 | 2,636,066,160 | 28,728,091 | 27,409,890 |
| revenue_by_supplier | 2,721,423,200 | 2,629,491,992 | 28,725,760 | 27,365,197 |
| revenue_by_customer | 2,737,720,120 | 2,659,087,264 | 28,469,385 | 27,148,164 |
| order_line_sales_by_country | 2,569,175,296 | 2,502,457,856 | 26,238,230 | 25,054,541 |
| low_stock_products | 160,633,152 | 150,869,712 | 1,937,445 | 1,823,327 |
| products_in_category | 1,783,608 | 1,599,888 | 16,386 | 13,888 |
| order_line_quantity_distribution | 2,619,010,992 | 2,545,596,784 | 26,562,481 | 25,230,923 |
| customer_order_details | 581,000 | 553,472 | 6,003 | 5,421 |

Every query uses fewer bytes and allocations than release f62 in this control. An initial independently seeded1x comparison had six small positive deltas; a three-execution repeat lowered bytes1.90–3.97% and allocations3.52–6.38% for all six. The identical-file control confirms the reductions. Against5a7,13 queries improve; an untouched node-only control varied0.03%B/0.004%alloc in1x, then measured effectively identical bytes and36 fewer allocations in five3-operation repeats. The DFS change is not on that node-only path; no allocation regression is established.

## Canonical traversal result collection

Each traversal now collects results once instead of building/copying recursive result lists. Completed sibling edges are deleted from active-path state instead of retaining false entries. Retained node/edge slices remain owned copies; DFS order, cancellation, limits, cycle semantics and memory accounting are preserved. No wrapper, public API or storage-format change.

The public ORDER BY aggregation benchmark reaches the profiled DFS owner. Five samples, GOGC=100,16/256/4096 children; fixture setup and warmup excluded. An earlier no-ORDER-BY benchmark used the streaming path and is not used as proof.

| Children | Before ns/op | After ns/op | Before B/op | After B/op | Before allocs/op | After allocs/op |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 16 | 84,283 | 83,961 | 46,918 | 43,452 | 457 | 436 |
| 256 | 452,976 | 432,559 | 575,862 | 512,356 | 4,090 | 3,821 |
| 4096 | 6,854,309 | 6,306,594 | 10,028,990 | 8,920,461 | 61,784 | 57,642 |

At4096 children:time−7.99%,bytes−11.05%,allocations−6.70%. At16 children, timing is neutral while bytes/allocations decrease. The fail-first active-edge test fails on both parsers before correction; retained-slice/order/limit/cancellation/repeatable-cycle controls pass afterward. Focused race controls pass3.035s; stored-entity inventory refresh passes14.542s with no artifact/rule change.

## Generated R17 controls

Original24-case application data is unavailable. The maintainer approved the generated10K Reading/2K Record substitute with1KiB documentation text and hourly datetimes; it does not qualify the unavailable original application suite. Both sides use Go1.27.3,GOGC=100,GOMEMLIMIT=8GiB,GOMAXPROCS=12. Statements are distinct text in warmed processes, not process-cold requests.

| Shape | Historical b6 ms/op | Retained follow-up ms/op | Follow-up B/op | Follow-up allocs/op |
| --- | ---: | ---: | ---: | ---: |
| Scoped CALL preparation | 6.369 | 5.883 | 2,786,185 | 26,038 |
| 25-ID lookup | 605.086 | 77.566 | 4,609,198 | 193,624 |
| Unindexed20-CALL | 417.076 | 89.438 | 4,336,381 | 45,837 |
| Indexed20-CALL | 5.774 | 5.995 | 3,682,476 | 41,913 |
| Datetime scan | 39.526 | 36.892 | 30,525,734 | 274,519 |

Historical indexed/datetime baseline is the earlier ten alternating-order process pairs; the retained-source follow-up has three fresh samples per shape and keeps indexed CALL within the5% budget(5.995ms versus5.774ms,+3.8%). Earlier mixed-compiler diagnostics are excluded. Exact results are asserted.

## Current Graphify importer

The retained source is profiled through actual `import_graph` over loopback, with generated11500nodes/30000edges. The three phases are initial import, unchanged refresh and changed/stale refresh(removing100 nodes and2000 edges). Each phase checks exact owned-node/edge hashes and preserves a foreign repository with colliding IDs. Both runs remove their temporary input/source fixtures. The recorded hashes agree with the preserved release-f62 fixture.

| Phase | Release f62 sync s | Retained sync s | Release f62 WB s | Retained WB s |
| --- | ---: | ---: | ---: | ---: |
| initial | 86.535 | 88.249 | 86.123 | 87.789 |
| unchanged | 5.677 | 5.670 | 5.663 | 5.699 |
| changed_sync | 5.769 | 5.785 | 5.724 | 5.748 |

These are single-run diagnostics, not statistical evidence for small timing deltas. The30-second CPU window is partial; allocation deltas include untimed verification. No remote code-graph ingest completion or successful duplicate-checker run is claimed.

## Verification and limits

- Published5a7 full `make test`:PASS147 packages,74 fresh/73 cached.
- Published2d036 follow-up:full `make test` PASS147 packages,36 fresh/111 cached, including the blocking storage context census (710.772s).
- Retained follow-up:integrated focused race checks pass(storage2.462s; benchmark client1.738s). Six native cells,3 reopens, all5 generated R17 controls and both actual Graphify importer modes pass. `make verify-list BASE=origin/release/v1.4.4` passes as a plan-only preview; the fresh full `make test` is running. Broader verification is not implied by the preview.
- Broader `make verify` is deferred to CI under the maintainer gate.
- The remote code graph remains unavailable/incomplete. The maintainer waived the checker this round; current-source reuse/ownership review found no unresolved candidate. No checker, hook, threshold or exclusion changes were made.
- No Badger keys, persisted payloads, storage version or third-party code changes.


## Write-behind overlap and retained allocation reduction

Write-behind still prepares, validates and logs each recovery record before acknowledgement. Background replay adds work; moving Badger application off the foreground does not remove these costs. Both synchronous default and write-behind use non-strict batched WAL in these controls. Their performance difference cannot be explained by avoiding a strict fsync.

The published15-second CPU windows include ingestion and early read warmups. Write-owned cumulative stacks show synchronous `PublishCommit`4.78CPU-seconds, write-behind foreground publication4.37CPU-seconds and replay an additional3.09CPU-seconds. These are sampled stacks, not additive percentages of ingest wall time. Replay physical-cache projection accounts for approximately0.77GiB of allocation traffic in the recorded write-behind run.

The retained change stores key-derived cache facts in one ordered72-byte representation instead of104-byte `AffectedFact` values. Visitation reconstructs the exact same fields. Dictionary resolution, string ownership, malformed/ignored families and insertion order stay unchanged. No fact is skipped or deduplicated. Other producers retain the full public fact representation needed for their own fields; no public API or stored format changes.

### Physical-cache projection benchmarks

Apple M2 Max,Go1.27.3,GOGC=100,GOMAXPROCS=4. Key/fixture setup is excluded; add and ordered emission are timed. Five samples at100K facts:

| Facts / mix | Before ns/op | After ns/op | Before B/op | After B/op | Before allocs/op | After allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 100K node-heavy | 6,013,175 | 4,364,558 | 55,151,473 | 41,678,488 | 100,029 | 100,029 |
| 100K edge-heavy | 7,602,767 | 5,517,658 | 54,151,880 | 40,678,884 | 25,029 | 25,029 |
| 100K mixed | 6,994,058 | 5,007,075 | 54,551,934 | 41,078,886 | 50,030 | 50,030 |

Allocation bytes fall24–25%, with counts unchanged. Five alternating-order process pairs at100K operations per sample qualify small commits. At16 facts, node-heavy medians improve590.2→531.6ns and3552→2560B; edge-heavy717.7→656.5ns and3456→2464B; mixed666.7→608.8ns and3488→2496B. Allocation counts stay21/9/13. At1 fact, bytes fall128→96(node/mixed) and112→80(edge), without extra allocations. Exact ordered-fact controls pass on both old/new representations, including malformed and ignored keys. The earlier split-slice candidate is excluded because it added small-commit allocations.

### Buffer-reuse experiments excluded from this PR

Two additional prototypes reused compressed WAL output and the final encoded frame. They preserved bytes and passed focused correctness/race controls, with strong microbenchmark allocation reductions:

| Prototype workload | Before ns/op | After ns/op | Before B/op | After B/op | Before allocs/op | After allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Compressed append,160 bindings | 18,924 | 19,068 | 4,555 | 2,508 | 12 | 11 |
| Additional frame reuse,160 bindings | 18,238 | 18,464 | 2,509 | 1,612 | 11 | 10 |
| Large frame codec,163840-byte Data | 16,611 | 3,937 | 172,333 | 402 | 4 | 3 |

Append timing ranges overlap. The frame experiment's baseline already includes compression-output reuse, and no unnecessary ownership copy is added to inflate that baseline.

Native memory did not establish a sufficiently stable gain for retaining these buffer prototypes. Three fresh-process comparisons per mode, without profiling, RSS sampling or forced collection during ingestion, recorded these kernel high-water maxima:

| Mode | Published baseline MiB | Cache facts + compression buffer MiB | Plus frame buffer MiB |
| --- | ---: | ---: | ---: |
| Strict | 4061 | 4051 | 4538 |
| Write-behind | 7563 | 8077 | 7518 |

Medians and quiescent heaps often overlap. A separate two-GC diagnostic found live-heap medians around633/632/640MiB respectively. GC traces place the higher peaks after a collection saw a larger temporary in-flight batch, raising its next heap goal. This explains the timing sensitivity but does not waive the unexplained peak cost. Both buffer prototypes are excluded; the existing WAL compression and codec implementations are retained exactly. No new buffer-ownership contract, format, migration or retention policy ships.

### Retained cache-fact candidate — kernel memory and completion controls

Three alternating-order process pairs per mode, unchanged script-default payload, no in-ingest profiling/sampling/forced GC. Fresh controller processes obtain kernel child high-water RSS after reaping the server, and verify it exceeds the already-reaped seed-client peak so the attribution is unambiguous. Completion includes seed/wipe/index work, immediate-postACK handoff and graceful teardown; it is an upper bound, not earliest flush latency or crash-recovery proof.

| Mode | Before ACK s | After ACK s | Before completion s | After completion s | Before peak RSS MiB(range) | After peak RSS MiB(range) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Strict | 8.662 | 8.577 | 9.245 | 9.161 | 3918(3880–4032) | 3912(3862–3922) |
| Write-behind | 8.151 | 8.274 | 11.740 | 11.713 | 7499(7418–7678) | 7615(7551–7721) |

Every reopened population matches all9 counts. Write-behind completed time is neutral; its worst measured RSS is0.6% above this paired baseline maximum, within the control variation, while strict peak decreases. The cache-fact representation introduces no additional retained buffer and structurally reduces its stored element width. No macro ingest-speed breakthrough or5-second result is claimed.

### Benchmark acknowledgement and population correctness

Seed batches now consume terminal summaries before counting success. Serial and parallel workers share that owner, and only typed retriable server statuses authorize CREATE replay. Controlled tests reproduce hidden terminal errors and unsafe message-only retry classification; this is not evidence of data loss in the previously verified benchmark populations.

The shared count verifier checks the exact generated ORDERS population from the existing plan total. NornicDB/Neo4j, Falkor and Ladybug callers use that same verifier. Missing/duplicated order-line tests fail before correction and pass afterward. Optional external-engine runtimes were not exercised. Integrated focused race controls cover the retained storage and seed changes; the final full gate follows frozen-source performance qualification.

Reproduce component measurements with GC enabled:

```sh
GOTOOLCHAIN=go1.27.3 GOGC=100 GOMAXPROCS=4 go test ./pkg/storage \
  -run '^$' -bench '^BenchmarkPhysicalCacheProjectionStore$' \
  -benchmem -benchtime=5x -count=5
```

The repository Northwind runner supplies the unchanged seed/query corpus; Graphify uses the actual importer. The native tables above are refreshed from the completed current-source matrix. Final verification is recorded after its gate finishes.
