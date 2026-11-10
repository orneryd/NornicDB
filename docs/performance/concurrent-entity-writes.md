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
below are medians, not statistical significance claims. The candidate is the
reviewed production source in this PR, recorded with per-file source and binary
hashes in the qualification receipt. Task-owned tests and
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
| Uncontended update | 73,857 → 75,192.5 | 44,846 → 45,437.5 | 419 → 422 | none |
| Sync, one node | 39,638.5 → 37,106 | 48,509 → 30,243 | 421 → 386 | 0.486 → 0 |
| Sync, own nodes | 40,522.5 → 42,455.5 | 45,110.5 → 46,712 | 428 → 449 | 0 → 0 |
| Write-behind, one node | 34,431 → 36,232.5 | 31,531.5 → 29,894 | 326 → 312 | 0.424 → 0 |
| Write-behind, own nodes | 45,729.5 → 36,687 | 31,004.5 → 29,337 | 333 → 312 | 0 → 0 |

Rejected statements count as attempts in the old implementation. Adjusting for
successful writes, the shared-node baseline is approximately 77.1 µs/ACK in sync
mode and 59.8 µs/ACK with write-behind, versus 37.1 and 36.2 µs respectively in the
candidate. Thus the higher write-behind time per attempt does not mean lower
successful-write throughput. Every candidate sample has zero conflicts and its
counter oracle passes.

The required lock has a measurable cost: uncontended updates add about 592 B and three
allocations; disjoint sync writers add about 1.6 KiB and 21 allocations (3.6% more
bytes, 4.9% more allocations). This is not an allocation-neutral claim. Shared-node
sync p99 also rises from 6.7 ms to 14.4 ms, while median statement latency falls
from 75 to 68 µs; write-behind p99 is 13.5 → 18.4 ms with a 59 → 54 µs median.
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
