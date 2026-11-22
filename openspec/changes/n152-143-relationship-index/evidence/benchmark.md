# Matched ordinary relationship index benchmark

Mac arm64, Go 1.27.4, GOMAXPROCS=2, GOMEMLIMIT=1500MiB, noui/nolocalllm.
Both immutable binaries use the same fixture, exact result assertions and cache-off
settings. Baseline was compiled before production edits at f77ec7706de099588ac232b32b6908860511d3da;
after is the current bounded canonical-owner change. Each cell: 100 queries,
5 repetitions, CPU profiling and benchmem, separately coordinated quiet Go windows.
Values below are medians, not a cross-workload or statistical significance claim.

| Parser / unrelated / explicit | ns/op before → after | B/op before → after | allocs/op before → after |
|---|---:|---:|---:|
| nornic/unrelated-8/explicit-false | 67322 → 66302 | 20044 → 18091 | 290 → 290 |
| nornic/unrelated-8/explicit-true | 93274 → 66335 | 43888 → 18111 | 693 → 291 |
| nornic/unrelated-512/explicit-false | 168170 → 67450 | 204281 → 18089 | 1308 → 290 |
| nornic/unrelated-512/explicit-true | 2460422 → 67443 | 2197327 → 18110 | 35249 → 291 |
| antlr/unrelated-8/explicit-false | 68136 → 66040 | 20168 → 18217 | 291 → 291 |
| antlr/unrelated-8/explicit-true | 92273 → 66127 | 44014 → 18238 | 694 → 292 |
| antlr/unrelated-512/explicit-false | 169401 → 67480 | 204409 → 18216 | 1309 → 291 |
| antlr/unrelated-512/explicit-true | 2575167 → 67688 | 2197505 → 18238 | 35250 → 292 |

All eight medians are non-degrading. The 8-edge autocommit difference (1.5–3.1%)
is noise-scale; no small-case speedup is claimed. The 512-edge cases avoid the
scan and allocation growth: around 60% lower autocommit latency and 97% lower
explicit-transaction latency in this fixture. Deterministic counted reads are
2/2 after versus 11/515 before; all return the exact same row.

CPU profiles include setup. Filtering `phase=relationship-read` shows baseline
`relationshipTypeSeedNodes` at 0.62s cumulative and transaction wrapper
`GetEdgesByType` at 0.59s. After has only 0.01s of query samples, too sparse for
precise hotspot comparison. No whole-profile attribution to query execution.

The first after run overlapped parent N138 reproduction and was superseded;
its output and profile remain at `/tmp/n152-143-after-bench-overlap-superseded.log`
and `/tmp/n152-143-after-overlap-superseded.cpu`. Those timings are excluded above.
The after rerun began after parent confirmed completion and N146 held Go work.

Raw final logs/profiles: `/tmp/n152-143-{baseline,after}-bench.log`,
`/tmp/n152-143-{baseline,after}.cpu`, and query-filtered profile summaries at
`/tmp/n152-143-{baseline,after}-query-profile.txt`. Immutable binaries are retained
for parent review/cleanup. This is bounded single-hit evidence, not multi-hit,
unsupported-view, general relationship optimization or the missing 172k fixture.

## Existing constraint-path regression guard

Same retained baseline/after binaries, original `BenchmarkRelationshipPropertyEquality`
body/defaults (128/4096), 100x ×5, benchmem +CPU profiles. Separate final quiet
window with parent, N146 and N138 all explicitly held. Both binaries report PASS.

| Parser / unrelated / explicit | Median ns/op before → after | Change | Median B/op before → after | Allocs/op |
|---|---:|---:|---:|---:|
| nornic/unrelated-128/explicit-false | 66370 → 68492 | +3.2% | 18283 → 18089 | 290 → 290 |
| nornic/unrelated-128/explicit-true | 67668 → 67328 | -0.5% | 18302 → 18111 | 291 → 291 |
| nornic/unrelated-4096/explicit-false | 69146 → 67420 | -2.5% | 18281 → 18089 | 290 → 290 |
| nornic/unrelated-4096/explicit-true | 70335 → 66583 | -5.3% | 18304 → 18112 | 291 → 291 |
| antlr/unrelated-128/explicit-false | 66310 → 67936 | +2.5% | 18408 → 18216 | 291 → 291 |
| antlr/unrelated-128/explicit-true | 66191 → 67266 | +1.6% | 18430 → 18238 | 292 → 292 |
| antlr/unrelated-4096/explicit-false | 66645 → 66036 | -0.9% | 18408 → 18217 | 291 → 291 |
| antlr/unrelated-4096/explicit-true | 70482 → 66576 | -5.5% | 18430 → 18238 | 292 → 292 |

No measured cell exceeds a 5% median regression. ANTLR 128 autocommit is 2.5%
slower (66.31→67.94µs) and its repetition ranges do not overlap; this is retained
measured overhead, not dismissed as noise. Nornic 128 autocommit is 3.2% slower
with overlapping ranges. Other cells vary from 1.6% slower to 5.5% faster.
Every cell retains allocation count and reduces bytes by roughly 192B/op. Parent
accepted this guard with the explicit small-case caveat; no broader optimization
was added. Results establish only this named workload, not universal speedup.
Profiles `/tmp/n152-143-constraint-{baseline,after}.cpu` are retained; no precise
hotspot attribution is claimed for this short guard. Raw logs are copied here.
