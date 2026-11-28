# Projected-read rebase investigation

These measurements compare release `935955a42557bb58460565164fa3f69265beca30`
with PR runtime `40b7951be` after the rebase. They do not establish that the
draft is production ready. The benchmark host and settings match the existing
Linux lab: ordinary-format fixture copies, query-result caching disabled,
`GOGC=400`, 8 GiB Go memory limit, and no concurrent builds or tests during
timing. Four trials alternate server order. The same client and source fixture
are used for both builds, and the harness checks all result fingerprints.
A brief Go coverage-report command overlapped the first mixed-workload trial;
interpret that pair with this limitation.

## Mixed workload, eight clients

Each phase uses 8,000 calls and 64 warm-ups. Values are medians of the four
per-trial p95 values, in milliseconds; they are not pooled percentiles.

| Phase | Release | PR | Change |
| --- | ---: | ---: | ---: |
| Document point | 0.8540 | 0.8131 | -4.8% |
| Product point | 0.8988 | 0.8124 | -9.6% |
| Mixed point | 0.8538 | 0.9024 | +5.7% |
| 16-document batch | 1.8959 | 1.6563 | -12.6% |
| Widen to large fields | 0.7591 | 0.9048 | +19.2% |
| Repeat large fields | 0.7928 | 0.7388 | -6.8% |

The first widening phase follows narrow reads, so the two builds begin that
phase with different cache coverage. The direct cost includes a physical read
and decoding of the previously unretained properties. This is a workload
tradeoff, not evidence that cache widening alone accounts for the entire delta.

## Selected Northwind queries

An otherwise unchanged Northwind client was restricted to the three named
queries. Each server used the same prepared Northwind fixture. Four alternating
trials each used five warm-ups and 30 measured calls per query. These values
are medians of the per-trial mean latencies, in milliseconds. Result hashes and
row counts matched in all paired trials. The first pair had much larger gaps
than subsequent pairs, so the estimates should not be mistaken for confidence
intervals.

| Query | Release | PR | Change |
| --- | ---: | ---: | ---: |
| low_stock_products | 475.486 | 477.544 | +0.4% |
| products_in_category | 4.204 | 4.453 | +5.9% |
| customer_order_details | 1.726 | 1.783 | +3.3% |

## Profiling and rejected experiment

The separate `BenchmarkProjectedReadsWiden` prepares 128 narrow cache entries
outside its timed interval and then widens each. Before any experiment, three
Windows/amd64 samples were 1.715–1.871 ms per batch, approximately 6.35 MB and
7,682 allocations per batch. A CPU profile and allocation profile include the
untimed preparation as well; they point to byte copying, Badger value reads,
decoding and cache fills, but do not apportion the original end-to-end delta.
The labelled CPU profile can isolate the widening phase.

An experimental direct copy for large MessagePack strings reduced one
allocation in an isolated decoder benchmark. Four paired mixed-workload trials
did not demonstrate an end-to-end improvement: widening median p95 was
0.9043 ms before versus 0.9404 ms with the experiment. The experiment was
discarded. The `pr1826-direct-string-mixed` receipts document that experiment;
those results do not describe code retained in the PR. No conclusion about
Northwind memory improvement follows from this experiment.

A second experiment retained the encoded body with a version-matched partial
cache entry, accounting for it within the existing cache budget. A preliminary
storage benchmark reduced timed widening allocations from about 6.35 MB to
4.51 MB per 128-node batch, but excludes the cost of initially retaining the
encoded bodies. Its first four-pair mixed run overlapped a full package suite
and is recorded in `pr1826-raw-cache-mixed`; it is not clean timing evidence.
After stopping that superseded suite, four more alternating pairs in
`pr1826-raw-cache-clean` had matching fingerprints. Widening median p95 was
0.971 ms before versus 0.952 ms after (-2.0%), while mixed-point median p95
was 0.761 versus 0.953 ms (+25.3%). One widening trial reached 3.275 ms p95
with the experiment. These runs do not establish a reliable improvement or
acceptable memory tradeoff, so this experiment was also discarded.

JSON receipts are grouped by run in this directory. The separate Northwind
memory profiles in the parent report were captured before this rebase; their
allocation breakdown is useful for diagnosis but has not been requalified
against these two current binaries.
