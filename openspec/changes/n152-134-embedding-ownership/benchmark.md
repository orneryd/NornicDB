# Measured vector ownership cost

Apple M2 Max, darwin/arm64, Go1.27.4, GOMAXPROCS2, GOGC100,
GOMEMLIMIT1500MiB; package tags noui,nolocalllm. Same benchmark source and
bounded256-entry cache on both retained binaries. Baseline production is
f77ec7706de099588ac232b32b6908860511d3da; after adds only the ownership correction.
The deterministic provider isolates cache work, not real model/network latency.
Every iteration asserts dimensions and first/last values. Batch size16;
half-miss keeps8 hot keys and generates8 new keys per operation.

Each run used `-test.run='^$' -test.bench='^BenchmarkCachedEmbeddingOwnership$'
-test.benchtime=200ms -test.count=5 -test.benchmem -test.cpuprofile=<side>.cpu`
on `/tmp/n152-134-{baseline,after}.test`. Both pass. Other release Go jobs were
held for both timed windows. Baseline was captured before production edits.

| Workload | Median ns/op before → after | Median B/op before → after | Allocs/op before → after |
| --- | ---: | ---: | ---: |
| dims1024/single_hit | 61.99 → 551.3 | 16 → 4112 | 1 → 2 |
| dims1024/batch16_hit | 1169 → 9880 | 640 → 66176 | 17 → 33 |
| dims1024/batch16_duplicate | 1045 → 9410 | 640 → 66176 | 17 → 33 |
| dims1024/batch16_half_miss | 8830 → 17232 | 35049 → 133350 | 70 → 94 |
| dims4096/single_hit | 61.94 → 1690 | 16 → 16400 | 1 → 2 |
| dims4096/batch16_hit | 1192 → 28984 | 640 → 262784 | 17 → 33 |
| dims4096/batch16_duplicate | 1043 → 29249 | 640 → 262784 | 17 → 33 |
| dims4096/batch16_half_miss | 21045 → 42521 | 133341 → 526558 | 70 → 94 |

This is a necessary ownership cost, not an optimization or a no-regression
claim. A1024-dimensional single hit adds about0.489µs and4KiB;4096 dimensions
add about1.628µs and16KiB. Sixteen-result hit/duplicate batches add16 independently
owned vectors; half-miss batches additionally copy8 retained insertions.
The mutable-vector API requires independent storage to isolate caller writes.
This narrow correction allocates each vector separately; it makes no claim that
the measured cost is an irreducible lower bound. No pooling/borrow API or cache
redesign was introduced. Provider answers, arbitration, LRU, errors and counters remain
unchanged. Results do not qualify real-provider throughput or retained-heap use.

CPU profiles are preserved. Their samples are dominated by runtime scheduling
and allocation machinery on this Mac, so no precise application hotspot or
CPU-percent attribution is claimed. Allocation deltas match one vector per
returned result plus one per insertion. Adaptive iteration counts slightly
change generated miss-key lengths; tiny byte differences beyond vector copies
are not treated as meaningful improvements.
