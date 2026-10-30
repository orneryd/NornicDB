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
| Time | 5,120,483 ns | 5,361,720 ns | +4.71% |
| Allocated bytes | 7,334,140 B | 6,919,768 B | -5.65% |
| Allocations | 106,764 | 89,091 | -16.55% |

These are the final follow-up samples; they supersede the earlier published
`97df587a8` sample set (+0.94% time, -8.76% bytes, -15.49% allocations), which
remains preserved in the evidence directory. The final time increase is below
the repository's 5% regression limit and is reported rather than described as a speedup. These numbers
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

## Follow-up convergence control

The shared constraint policy was also compared directly with the two validation
files from published `97df587a8`, using a Go overlay and the same current checkout
for all other files. Three alternating pairs at 200 iterations per sample:

| Median per batch | Before convergence | After convergence | Change |
| --- | ---: | ---: | ---: |
| Time | 5,991,308 ns | 6,016,584 ns | +0.42% |
| Allocated bytes | 9,625,801 B | 9,469,921 B | -1.62% |
| Allocations | 110,206 | 109,430 | -0.70% |

This small control found no material cost from the concrete shared validation
context. It preserves the physical publishing transaction versus pending/current
transaction readers, original error values and first-error ordering. Three pairs
are limited evidence; their small deltas are not claimed as a speedup. The different
iteration count changes workload lifetime and these values must not be compared
numerically with the 100-iteration release table. Final CPU/allocation profiles
were also captured for the commit workload.

## Stored user read allocation regression

Repeated race runs exposed an intermittent eighth allocation against the
unchanged seven-allocation budget. Removing two temporary encoded-byte copies
and one atomic-roles scratch target improved normal reads, but was not sufficient:
the decoder's pooled reflected map key/value temporaries could still be discarded.
The final correction belongs to the existing maintained decoder fork.

[go-json PR #1](https://github.com/orneryd/go-json/pull/1) adds exact
`map[string]string` typed assignment using its existing concrete string decoders.
NornicDB pins published commit `fa0195dc9464133e748373d666c20b1bc718490c`
(`v0.11.3-0.20261030214440-fa0195dc9464`); no local replacement is used.
Named map/key/value types retain generic decoding and their hooks. The complete
fork race suite passes; public API/error/partial-value and sequential stream
checks pass. Differential fuzz covered79,548 executions. The existing total-depth
10001 error-class distinction from encoding/json is preserved: both decoders
reject the input. A reviewed suspected error-path regression was not reproduced
through any of the four public decoding entry points.

The input borrow is synchronous and read-only. The pinned decoder copies its
input; detached output containers and malformed/prefix/atomic-role cases remain
checked. No User layout, persisted fields, Badger keys or storage version changes.

Three sequential500ms samples before the decoder correction measured the initial
user binding improvement:752→635ns/op,717→636B/op,7→4allocations. A subsequent
quiescent ten-sample25-shape comparison of the decoder correction measured the
populated fixture581→546ns/op, with unchanged bytes/allocations across the shape
matrix. Largest256-role timing deltas were+0.2% populated/+1.6% absent metadata;
these small differences are not speedup claims. Twelve unaffected-map controls
(six samples each) retained allocation counts and bytes within measurement
variance; timing medians ranged−3.45% to+0.97%.

These are workload-scoped measurements, not database-wide throughput results.
The fork specialization must be reviewed when upgrading its grammar, scalar
ownership or runtime integration. The original allocation budget remains seven;
final PR results identify the exact pinned source and full make-test gate.
