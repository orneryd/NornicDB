# PR #1682 read-query performance checkpoint

The reviewed source is checkpoint5a7c16e7 plus the private DFS result accumulator and active-edge cleanup. Full `make test` for this follow-up is running; the earlier5a7 head passed147 packages (74 fresh,73 cached). This is a review checkpoint, not a merge-ready declaration.

Apple M2 Max, Go1.27.3. Exact native script defaults:97,440 nodes,312,050 relationships,37,124,459 payload bytes; batch2,000,parallel4,seed42. Graph-only, nornic parser, normalization disabled. GC enabled: GOGC=400,GOMEMLIMIT=8GiB (a soft Go limit, not a hard RSS cap). No concurrent Go tests/builds, local-model generation or Docker workloads during the native timings.

## Native ingestion and persistence

| Mode / result cache | Ingestion acknowledgement (s) | Wipe + indexes + ingestion (s) | GC cycles |
| --- | ---: | ---: | ---: |
| durable / default | 8.527 | 8.591 | 20 |
| durable / off | 8.349 | 8.420 | 98 |
| wb / default | 8.500 | 8.561 | 17 |
| wb / off | 9.095 | 9.156 | 67 |
| strict / default | 8.553 | 8.719 | 22 |
| strict / off | 8.618 | 8.787 | 97 |

Synchronous default uses batched WAL sync; strict mode additionally enables immediate WAL sync and Badger synchronous writes. Adaptive write-behind acknowledgement is not durable completion. All six cells agree on9 graph counts and all14 fingerprints; all three write modes preserve these after graceful shutdown/reopen. This is not crash-recovery proof. Shutdown after the read suite is not ingest-to-flush latency; an immediate-postACK completion experiment remains pending. No write-behind ingest speedup is claimed.

## All14 native queries — result cache default

Milliseconds:median(p95). Five warmups; 30 timed iterations. Only the existing result-cache policy differs; query text and normal statement/storage caches are unchanged.

| Query | Synchronous default | Adaptive write-behind | Strict durability |
| --- | ---: | ---: | ---: |
| products_per_category | 0.202 (0.238) | 0.231 (0.271) | 0.224 (0.271) |
| customer_category_distinct_orders | 0.142 (0.156) | 0.170 (0.193) | 0.180 (0.209) |
| optional_match_orders_count | 0.197 (0.204) | 0.245 (0.267) | 0.248 (0.263) |
| revenue_by_product | 0.171 (0.191) | 0.163 (0.180) | 0.163 (0.207) |
| products_by_supplier | 0.159 (0.199) | 0.171 (0.207) | 0.180 (0.204) |
| orders_by_customer | 0.169 (0.186) | 0.166 (0.183) | 0.171 (0.193) |
| revenue_by_category | 0.238 (0.254) | 0.231 (0.263) | 0.251 (0.266) |
| revenue_by_supplier | 0.170 (0.201) | 0.184 (0.206) | 0.196 (0.242) |
| revenue_by_customer | 0.185 (0.206) | 0.170 (0.201) | 0.186 (0.208) |
| order_line_sales_by_country | 0.169 (0.193) | 0.154 (0.173) | 0.166 (0.179) |
| low_stock_products | 0.239 (0.277) | 0.248 (0.277) | 0.247 (0.268) |
| products_in_category | 0.248 (0.270) | 0.242 (0.250) | 0.222 (0.248) |
| order_line_quantity_distribution | 0.163 (0.187) | 0.161 (0.190) | 0.169 (0.198) |
| customer_order_details | 0.248 (0.275) | 0.233 (0.266) | 0.245 (0.259) |

## All14 native queries — result cache off

Milliseconds:median(p95). Five warmups; 5 timed iterations. Only the existing result-cache policy differs; query text and normal statement/storage caches are unchanged.

| Query | Synchronous default | Adaptive write-behind | Strict durability |
| --- | ---: | ---: | ---: |
| products_per_category | 636.191 (649.513) | 610.372 (613.746) | 634.521 (650.312) |
| customer_category_distinct_orders | 5353.503 (5380.152) | 5362.997 (5378.543) | 5292.525 (5330.683) |
| optional_match_orders_count | 3589.646 (3620.473) | 3799.565 (3886.387) | 3570.867 (3618.521) |
| revenue_by_product | 2255.118 (2268.981) | 2383.568 (2406.830) | 2254.484 (2262.787) |
| products_by_supplier | 642.113 (660.249) | 619.326 (625.692) | 643.255 (649.133) |
| orders_by_customer | 400.573 (416.833) | 435.953 (454.255) | 398.723 (409.284) |
| revenue_by_category | 3423.121 (3455.926) | 3642.096 (3701.913) | 3423.455 (3490.722) |
| revenue_by_supplier | 3378.884 (3399.424) | 3542.976 (3578.425) | 3385.438 (3396.109) |
| revenue_by_customer | 3186.071 (3230.485) | 3106.287 (3119.088) | 3170.661 (3202.125) |
| order_line_sales_by_country | 1972.486 (1982.004) | 1892.204 (1904.234) | 1954.386 (1990.376) |
| low_stock_products | 216.782 (222.794) | 216.727 (217.705) | 216.055 (217.024) |
| products_in_category | 2.474 (2.499) | 2.411 (2.570) | 2.362 (2.478) |
| order_line_quantity_distribution | 1975.409 (1991.631) | 2371.756 (2385.485) | 1973.138 (2054.466) |
| customer_order_details | 1.122 (1.155) | 1.105 (1.129) | 1.116 (1.136) |

## Matched-file allocation control

All three binaries open copies of identical closed Badger files. One timed Execute after five warmups; no transport, seed or fingerprint calculation is timed. This is per-operation diagnostic evidence, not a statistical small-latency-delta claim. CPU and sampled allocation profiles cover all14 queries.

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

| Shape | Historical b6 ms/op | Follow-up ms/op | Follow-up B/op | Follow-up allocs/op |
| --- | ---: | ---: | ---: | ---: |
| Scoped CALL preparation | 6.369 | 5.665 | 2,786,109 | 26,038 |
| 25-ID lookup | 605.086 | 76.699 | 4,673,601 | 196,919 |
| Unindexed20-CALL | 417.076 | 91.499 | 4,339,645 | 45,840 |
| Indexed20-CALL | 5.774 | 5.804 | 3,682,516 | 41,912 |
| Datetime scan | 39.526 | 36.365 | 30,526,194 | 274,520 |

Historical indexed/datetime baseline is the earlier ten alternating-order process pairs; this frozen follow-up has three samples per shape and keeps indexed CALL within the5% budget. Earlier mixed-compiler diagnostics are excluded. Exact results are asserted.

## Current Graphify importer

The earlier5a7 source was profiled using actual import_graph over loopback with generated11500nodes/30000edges:initial,unchanged,changed/stale refresh, exact owned-node/edge hashes and foreign-repository preservation. Temporary input/source files are removed. Its f62/current synchronous initial wall times are86.535/87.461s and write-behind86.123/86.973s; these single-run diagnostics do not establish a small-delta gain. The private traversal follow-up still needs its completion evidence; no remote ingest completion or successful duplicate-checker result is claimed.

## Verification and limits

- Published5a7 full `make test`:PASS147 packages,74 fresh/73 cached.
- This follow-up:focused/race tests and all native/correctness/performance controls above pass; full `make test` is running.
- Broader `make verify` is deferred to CI under the maintainer gate.
- The remote code graph remains unavailable/incomplete. The maintainer waived the checker this round; current-source reuse/ownership review found no unresolved candidate. No checker, hook, threshold or exclusion changes were made.
- No Badger keys, persisted payloads, storage version or third-party code changes.
