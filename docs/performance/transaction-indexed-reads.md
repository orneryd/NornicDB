# Explicit transaction indexed-read qualification

Baseline: release/v1.5.0 `c4ac624b88a3814c4d12d4014ddfe6cb3a95c0a5`.
Candidate: NOR-160 transaction population visibility plus pinned point-read correction.
Apple M2 Max, darwin/arm64, Go 1.27.3, `GOMAXPROCS=4`, `GOGC=100`.

## Workload and method

`BenchmarkIndexedTransactionRead` generates 512, 12,000 or 24,000 target-document
`:PDGraphNode:PDEntity` nodes and 256 other-document nodes. It creates a unique
`id` constraint and a `document_id` property index through public Cypher.
`MATCH (m:PDGraphNode:PDEntity {document_id:$d})` returns either `count(m)` or
all nodes. The result cache is disabled in both arms. BEGIN/ROLLBACK and setup are outside
timing. Every node result is consumed: count, document, logical id/value and an
allocation-free uniqueness bitset establish the complete expected population.

Separate baseline/candidate binaries use the same benchmark source. Runs are
sequential ABBA, three samples per invocation, 200 ms per sample, giving six
samples per arm/cell. Reported values are medians; native identities are newly
generated for each fixture, so physical layout is not byte-identical. The runs did not overlap this task's local Go tests or builds, and no local-model worker was used. CPU/allocation
profiles were collected separately for the 12K explicit count read; profiles
also include untimed fixture/calibration work and are not query-only CPU totals.

## Explicit transaction results

| Nodes | Result | Baseline ms/op | Candidate ms/op | Baseline MiB/op | Candidate MiB/op | Baseline allocs/op | Candidate allocs/op | Time / bytes / allocations |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 512 | count | 2.075 | 1.627 | 1.684 | 1.459 | 23,345 | 17,433 | -21.6% / -13.3% / -25.3% |
| 512 | nodes | 2.249 | 1.810 | 1.903 | 1.679 | 25,443 | 19,524 | -19.5% / -11.8% / -23.3% |
| 12,000 | count | 65.377 | 52.614 | 43.171 | 37.707 | 665,056 | 520,782 | -19.5% / -12.7% / -21.7% |
| 12,000 | nodes | 69.212 | 56.941 | 48.455 | 42.995 | 713,481 | 569,332 | -17.7% / -11.3% / -20.2% |
| 24,000 | count | 139.782 | 112.342 | 92.586 | 81.701 | 1,559,980 | 1,272,398 | -19.6% / -11.8% / -18.4% |
| 24,000 | nodes | 153.662 | 128.170 | 103.272 | 92.338 | 1,656,192 | 1,367,804 | -16.6% / -10.6% / -17.4% |

Autocommit controls ranged from -1.2% to +3.7% in median time; bytes and
allocations differed by at most 0.1%. In a separate longer isolated 24K
node-result control (ten samples per arm, 500 ms/sample), time changed from
65.459 to 66.314 ms (+1.3%), with 46,990,703 versus 46,999,472 B/op (+0.02%).
These controls do not establish that every autocommit workload is unchanged.

## Owning change and correctness limits

An ordinary native transaction already holds its physical snapshot and its
namespace's registered begin-time version. When those versions still agree,
its node point reads now reuse `readNodeItemInTxn` directly against that pinned
snapshot instead of reading/decoding the logical head and registering another
history reader for every candidate. The body decoder, version-qualified cache,
embedding hydration and read-source provenance remain shared with other reads.
The sampled generic history-read stack is absent from the candidate profile.

Rewound/zero versions, late namespace pins, absent physical snapshots and
write-behind keep the existing logical/read-committed owners. Before/after
owner-equivalence tests cover update/delete/recreate/create after BEGIN,
missing nodes, sidecars/vectors, visible/stored reads, own writes/deletes,
suppression and cancellation. The integrated visibility correction retains
physical label/equality populations and applies live visibility at delivery.
No storage payload, Badger key, WAL format or Go public signature changes.

This is a measured reduction, **not autocommit parity**. At 12K, the candidate
explicit count read is still about 52.6 ms against 10.8 ms in autocommit. Body
reads/copying and embedding metadata checks remain; this PR does not claim a
snapshot-aware batch hydration implementation or resolution of the separate
OPTIONAL MATCH adjacency and relationship-property index reports on NOR-160.
The original private application dataset was not available; this qualification
uses the documented generated shape. Local builds use `noui,nolocalllm` because
the worktree lacks the native llama library; final native qualification is CI.
