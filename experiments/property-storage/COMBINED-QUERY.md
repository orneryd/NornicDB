# Combined Northwind and document projections

## Scope and method

The workload combines 97,440 Northwind nodes and 312,050 relationships with
4,352 document nodes. The document fixture starts with 272 repository Markdown
files, repeats them sixteen times, and adds independent random binary payloads
and low-entropy integer arrays. Repetition is synthetic and must not be used to
predict production deduplication ratios. Both databases were subsequently checked
against every source document field, including body, noise, metrics and status.
Five database-owned system nodes are excluded from the application-node count.

The old binary is release `ddc59dc20` with upstream Badger. The experimental
binary is Nornic `496c3c934` with public fork `4af841ebc969`, flag on. Both use
native parsing, result cache size zero, disabled embeddings/search/decay, GOGC
400 and an 8 GiB Go memory target, inside one isolated 4-CPU/10-GiB container.
These are graph/storage measurements; they do not qualify embeddings or UI.

Each case uses a fresh server and a copied store; three trials alternate engine
order. Numbers below are medians of per-trial measurements, not pooled percentiles.
Each point phase runs 4,000 queries after 64 warmups. This does not establish
physical cold-disk behavior: the OS page cache is uncontrolled. The first document
phase mostly accesses keys for the first time in that server process. Later
phases can benefit from prior reads. Bolt latency includes Run, Collect and
Consume. Result validation, sorting and hashing are outside the latency timer but
inside throughput wall time and share the CPU quota. RSS samples cover the server
only, every 10 ms; they are sampled measurements, not memory bounds.

## Original comparison, eight clients

| Query | Old mean ms | Experimental mean ms | Old p95 ms | Experimental p95 ms |
|---|---:|---:|---:|---:|
| Document indexed point projection | 0.652 | 1.286 | 0.914 | 3.282 |
| Northwind product projection | 0.636 | 0.605 | 0.875 | 0.894 |
| Equal document/product mix | 0.536 | 0.535 | 0.654 | 0.690 |
| Sixteen-document batch projection | 1.473 | 1.213 | 2.407 | 2.046 |
| Document full-property control | 0.769 | 0.730 | 0.791 | 0.807 |
| Full document label projection (4,352 rows/query) | 402.387 | 235.257 | 509.430 | 345.147 |

Label scans use 100 queries after four warmups. Each returns every document;
these are query latencies, not per-row latencies. Single-client scan mean falls
from 121.616 to 72.975 ms; p95 falls from 133.007 to 82.324 ms.

| Footprint | Old | Experimental |
|---|---:|---:|
| Allocated combined-store disk | 437.44 MiB | 369.61 MiB |
| Sampled server RSS, mixed projection, eight clients | 2.39 GiB | 2.09 GiB |
| Median per-process maximum across point phases, eight clients | 3.06 GiB | 2.09 GiB |

The original document-point p95 is unstable: experimental trials are 3.282,
4.741 and 1.168 ms versus old 0.886, 0.914 and 0.978 ms. It is a measured
regression in this run, not proof of a fixed cost in every process.

## Rejected indexed-projection pushdown

The index candidate reader hydrates every user property, whereas the label reader
already carries a proven projection. A candidate reused that analyzer and passed
focused native/ANTLR checks, including filters, whole entities, mutations and
embedding metadata. Its patch and regression fixture are retained in `rejected/`;
it is not active runtime code.

The candidate was compared against the unchanged experimental binary on identical
copies of the experimental combined store, flag on for both. All query fingerprints
matched, but the latency result did not justify adoption:

| Eight-client query | Before mean/p95 ms | Candidate mean/p95 ms |
|---|---:|---:|
| Document point | 0.718 / 1.008 | 0.720 / 1.047 |
| Mixed projection | 0.466 / 0.614 | 0.522 / 0.627 |
| Batch sixteen | 1.027 / 1.442 | 1.172 / 1.667 |

The severe first-run tail did not recur in the unchanged binary either. Do not
attribute that difference to the candidate. Partial projections bypass full-body
cache admission, which is a plausible cost requiring profiling before a redesign.
The candidate was removed because latency/concurrency take priority over avoiding
materialization in isolation.

## Evidence

`profiles/combined-query/`, `profiles/combined-query-scans/`,
`profiles/point-tuned-query/` and `profiles/corpus-verification/` contain receipts.
`verify_projected_queries.py` checks each old/new result fingerprint and prints
medians. `projected-query/` contains the Bolt workload and Linux lab runner.
No full test suite was run for this iteration, as requested.
## Retained payload-skip tuning

The final runtime change is limited to skipping unselected length-framed strings,
binary values and extensions by a checked bytes.Reader seek. Selected properties
retain the canonical strict decoder. Nested containers retain its Skip walk. The
same default-off experimental flag controls this change. No storage format,
write policy, dictionary authority or Badger dependency changed in this iteration.

A five-repeat warmed microbenchmark shows median skipping costs:

| Omitted payload | Decoder copy | Checked seek |
|---|---:|---:|
| 20 KiB extension body | 491.1 ns | 5.93 ns |
| 4 KiB binary | 68.72 ns | 5.28 ns |

Both warmed cases allocate zero bytes/op; the improvement is avoided copying,
not a claim that the old warmed decoder allocated on every call. This isolated
microbenchmark does not predict end-to-end speedup.

The final stock-versus-experimental rerun uses three trials, the same combined
source stores and all five point workloads. All fingerprints match. Here are
medians at eight clients:

| Query | Stock mean/p95 ms | Final experimental mean/p95 ms | Stock/final queries/s |
|---|---:|---:|---:|
| Document point | 0.579 / 0.809 | 0.788 / 1.237 | 13,466 / 9,874 |
| Northwind product | 0.554 / 0.763 | 0.568 / 0.806 | 13,994 / 13,822 |
| Equal document/product mix | 0.508 / 0.616 | 0.519 / 0.617 | 15,307 / 14,985 |
| Sixteen-document batch | 1.214 / 2.287 | 1.089 / 1.520 | 6,400 / 7,118 |
| Full document control | 0.652 / 0.708 | 0.664 / 0.724 | 10,126 / 9,864 |

Document-point p95 remains **53% slower** and throughput **27% lower** than stock.
Batch p95 improves, but its p99 worsens from 13.113 to 16.890 ms. This iteration
has not resolved the indexed document-point regression or established dominance
at every percentile. The query path still requests complete user properties.
Removing copies in projected readers cannot fix hydration elsewhere by itself.

Mixed-phase sampled RSS is 2.34 GiB stock and 2.11 GiB experimental in this rerun.
Median per-process maximum is 2.41 versus 2.11 GiB. The initial memory advantage
was larger; memory/cache state varies across runs. There is no new on-disk format,
so use the separately measured closed-store disk figures above rather than claim
new storage savings from a read-only tuning change.

Receipts: `profiles/point-seek-query/` and
`profiles/point-tuning/skip-microbench.txt`. The final runner metadata's generic
"point projection tuning" label refers to the retained payload-skip source;
the rejected Cypher candidate is absent from `nornicdb-skip-final`.

Focused final storage, native Cypher and ANTLR checks are recorded separately.
The original indexed candidate's passing tests only qualify that rejected
candidate; they are not presented as evidence for an adopted runtime fix.