# Concurrent entity writer qualification

NOR-213 requires same-entity Cypher writers to wait and apply their mutations
on the committed predecessor, rather than both staging stale images and failing
one commit. The correctness regressions exercise node/relationship writes,
rollback, cancellation, timeout and deadlock behavior, both parsers, and public
Bolt/HTTP routes. Refreshed no-op images also participate in collection reads
without staging a write; unrelated entities retain their existing snapshot rules.

## Matched workload

Apple M2 Max, darwin/arm64, Go 1.27.3, `GOMAXPROCS=2`, `GOGC=100`,
`noui,nolocalllm`. Baseline: release `d844abc648a15bc185a42e1f36dc476cc94cd0f0`.
The same `BenchmarkNOR213*` source runs on both arms. Four invocations in ABBA
order, each with three 300 ms samples, give six samples per arm/cell. Figures
below are medians, not statistical significance claims. Task-owned tests and
compilation were stopped before measurement; this is not a claim of complete
machine isolation.

The contended fixture has 64 indexed nodes and 16 concurrent autocommit sessions.
They increment one shared node or their own node. After every sample, the exact
sum of stored counters must equal successful statements (`b.N - conflicts`).
The uncontended fixture repeatedly updates one node from a single session.

Both storage modes use in-memory Badger. “Sync” means synchronous transaction
publication, not disk/fsync durability; write-behind uses the in-memory buffer.
These numbers qualify writer/ledger work, not disk WAL throughput or production
ingest latency. Separate durable public-protocol tests cover those routes.

| Workload | ns/attempt, base → candidate | B/attempt | allocations/attempt | conflicts/attempt |
| --- | ---: | ---: | ---: | ---: |
| Uncontended update | 74,249 → 74,703 | 44,846 → 45,437 | 419 → 422 | none |
| Sync, one node | 40,004 → 37,577 | 48,514 → 30,296 | 422 → 386 | 0.486 → 0 |
| Sync, own nodes | 41,044 → 42,660 | 45,110 → 46,738 | 428.5 → 449.5 | 0 → 0 |
| Write-behind, one node | 34,665 → 36,608 | 31,524 → 29,932 | 326 → 313 | 0.426 → 0 |
| Write-behind, own nodes | 44,093 → 36,974 | 30,876 → 29,588 | 335.5 → 314.5 | 0 → 0 |

Rejected statements count as attempts in the old implementation. Adjusting for
successful writes, the shared-node baseline is approximately 77.8 µs/ACK in sync
mode and 60.4 µs/ACK with write-behind, versus 37.6 and 36.6 µs respectively in the
candidate. Thus the higher write-behind time per attempt does not mean lower
successful-write throughput. Every candidate sample has zero conflicts and its
counter oracle passes.

The required lock has a measurable cost: uncontended updates add 591 B and three
allocations; disjoint sync writers add about 1.6 KiB and 21 allocations (3.6% more
bytes, 4.9% more allocations). This is not an allocation-neutral claim. Shared-node
sync p99 also rises from 7.0 ms to 13.5 ms, while median statement latency falls
from 75 to 68 µs; write-behind p99 is 14.1 → 17.3 ms with a 60 → 53.5 µs median.
The PR does not claim FIFO fairness or tail-latency parity with fast failures.

## Reproduction

```sh
GOMAXPROCS=2 GOGC=100 go test -tags noui,nolocalllm ./pkg/cypher \
  -run '^$' -bench BenchmarkNOR213 -benchmem -benchtime=300ms -count=3
GOMAXPROCS=2 GOGC=100 go test -tags noui,nolocalllm ./pkg/cypher \
  -run '^$' -bench 'BenchmarkNOR213ContendedNodeUpdate/sync/oneNode$' \
  -benchtime=2s -cpuprofile=writer.cpu -memprofile=writer.mem
```

The separate CPU profile contains fixture/setup and Go benchmark calibration as
well as measured execution. It is dominated by runtime scheduling/parking and
memory work; it does not establish that every ledger bottleneck is eliminated.
Raw repeated samples, profiles, binary hashes and per-production-file source
hashes are retained with the PR qualification evidence.
