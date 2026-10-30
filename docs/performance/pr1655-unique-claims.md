# Concurrent UNIQUE commit validation

PR #1655 corrects false UNIQUE errors, stale key images and claim ownership during
buffered publication and recovery. These results qualify one disjoint-commit
workload; they are not a Northwind result or a database-wide performance claim.

## Paired release comparison

Apple M2 Max, macOS arm64, Go 1.26.9, eight concurrent writers with 100 uniquely
constrained nodes each. Seven alternating baseline/current pairs, 100 iterations
per sample. Local Qwen was stopped and no Docker containers were running.
Baseline: `release/v1.4.4` commit `15f1a5028893195d87b2f72c44d61baa4b1ec390`.

| Median per batch | Release baseline | Corrected implementation | Change |
| --- | ---: | ---: | ---: |
| Time | 5,105,893 ns | 5,153,887 ns | +0.9% |
| Allocated bytes | 7,351,241 B | 6,706,972 B | -8.8% |
| Allocations | 107,061 | 90,477 | -15.5% |

The time increase is reported rather than described as a speedup. These numbers
cannot be compared directly with the earlier TrueNAS `UniqueDisjointBatches`
report: its original harness and artifacts were unavailable.

```sh
go test -tags=noui,nolocalllm ./pkg/storage -run '^$' \
  -bench '^BenchmarkBadgerUniqueConstraintCommit_DisjointFunctionBatches$' \
  -benchtime=100x -benchmem -count=1
```

The fixture excludes transaction construction/staging from its timed region and
measures concurrent commit completion. It asserts every commit succeeds.

## Work removed

CPU/allocation profiles identified repeated current-body projection and constraint
selection work. Validation now opens one canonical current read cut per mutation
batch. It can borrow an immutable body only after matching its physical version;
public query reads remain detached. Cache projection no longer copies an entire
body and then copies its projection again. Temporal consumers select temporal
constraints before copying, and UNIQUE lookups construct their existing key with
string concatenation instead of formatting.

Runtime observation fields are packed beside existing flags: `storage.Operation`
remains 152 bytes, equal to the arm64 release baseline. This size measurement is
separate from the workload allocation results above. No persisted field order,
Badger key, storage version or WAL schema changes are required.

## First-image selection

A batch containing exactly one update for each pending node already carries its
ordered first images. Selection uses those operations directly; repeated or mixed
mutations retain the incarnation-aware fold. Iteration reads operation fields by
index instead of copying each 152-byte Operation. No ordinary-read map is added,
and current-state reads under commit authority are retained.

The isolated selection benchmark is distinct from the commit measurements above.
Five 10,000-iteration samples compare the previous map implementation with the
current selection (same staged inputs):

| Selection workload | Previous median | Current median | Previous/current B/op | Previous/current allocs/op |
| --- | ---: | ---: | ---: | ---: |
| 100 distinct node updates | 2,705 ns | 92.23 ns | 3,544 / 0 | 4 / 0 |
| 100 nodes with one repeated update | 2,720 ns | 2,171 ns | 3,544 / 3,544 | 4 / 4 |
| 1,000 distinct node updates | 26,645 ns | 908.3 ns | 54,656 / 0 | 6 / 0 |
| 1,000 nodes with one repeated update | 26,554 ns | 22,171 ns | 54,656 / 54,656 | 6 / 6 |

The tiny one-node repeated case was additionally sampled for one second three
times: 90.61 ns before, 86.72 ns current; both 256 B and two allocations. Short
10,000-iteration samples of that sub-microsecond case were noisy and are not used
to assert a timing change. These numbers qualify selection only, not database
throughput. The paired commit comparison remains the end-to-end commit result.
