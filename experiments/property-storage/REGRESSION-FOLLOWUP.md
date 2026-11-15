# Regression profiling and corrections

The experimental path now avoids three measured sources of wasted work: scanning
an unrelated cache when no eviction is needed, copying selected compressed frames
before decoding them, and allocating a fresh decompression buffer for each value.
It also binds a projected cache image's physical identity once at admission.

These changes use the existing default-off
`NORNICDB_EXPERIMENTAL_PROPERTY_STORAGE_ENABLED` flag. Durability, snapshot
selection, returned property types and cache budgets are unchanged. The subsequent
codec correction adds a new experimental property frame, with legacy reads kept.
The public fork dependency is `499a805ba32f`. The allocation/cache corrections
were first profiled with its predecessor, `da80961bb73e`, before the property-codec
change described below.

## Profile first

The previous uncached Northwind comparison was stopped at the user's request.
Its experimental run was incomplete; it is not a completed comparison.

CPU and allocation profiles then exercised these existing queries with the result
cache disabled: category product counts, customer/category distinct orders,
optional-match order counts and supplier revenue. Each server read its own copy of
the prepared store. The lab has four CPUs and 10 GiB of container memory, Go
1.27.3, GOGC 400 and an 8 GiB Go soft limit. No production service was changed.

Before the last two corrections, `putNodeLocked` spent substantial CPU starting a
map iterator on the full-node cache, even though its first loop body immediately
found that no eviction was needed. Starting an iterator over a sparsely populated,
preallocated map can scan empty slots. Query-cache insertions paid this cost for
an unrelated full-node cache.

| Captured profile | Cache admission before | Cache admission after | Full-cache iterator before | After |
| --- | ---: | ---: | ---: | --- |
| Category counts | 530 ms | 120 ms | 340 ms | No samples |
| Supplier revenue | 1,350 ms | 500 ms | 790 ms | No samples |

These are sampled CPU costs in 12-second profiling windows, not query latency.
The three category requests finish within their window; longer workloads need not
execute the same number of requests inside a fixed window. Use these captures to
locate removed work, not as throughput or p95 claims.

The correction checks available room before entering either eviction iterator.
Eviction still uses the existing owner, entry limits, byte limits, source floors
and generation checks. Actual required eviction remains visible in the profiles.

## Compressed property reads

The old extension callback allocated and copied a compressed frame, decompressed
it into another allocation, then copied strings through the generic decoder's
scratch buffer. Selected stored values now borrow the checked frame span while
decoding; strings use the existing MessagePack framing owner and one owned copy.

The fork adds `DecodePayloadInto`: it can reuse caller-provided capacity and bounds
the decoder by the declared output length. Nornic reuses scratch only until typed
values are independently owned. Each pooled scratch buffer retains at most
128 KiB; larger values are decoded normally without retaining their large buffer.
This is a per-buffer limit, not a global pool memory cap.

A cold allocation profile issued 4,000 distinct document reads with eight clients.
Keys came directly from the frozen corpus file paths, avoiding a database scan
before the profile. Both sides used the same experimental store, and neither used
the result cache.

| Sampled allocation | Published experimental build | Corrected build |
| --- | ---: | ---: |
| Payload decoder, flat allocation | 58.98 MB | 8.28 MB |
| Complete HTTP workload | 1,188.06 MB | 1,105.47 MB |

Allocated bytes are cumulative, sampled values reported by pprof; they are not RSS
or retained heap. Large HTTP JSON serialization allocations remain. Compression
still has a CPU cost and Badger still reads and restores complete table blocks.

The first allocation capture queried all document keys through the server, which
also warmed body caches. That capture did not test cold decoding and is excluded
from these figures. Its receipts are retained as `property-allocation-fixed`; the
corrected capture is `property-allocation-cold-fixed`.

## Correctness and scope

### Faster property codec

Allocation removal did not eliminate the remaining first-full-read cost. In the
mixed query-history profile, Zstd property decoding accounted for 180 ms of the
430 ms sampled in projected storage reads. New writes now use S2 with a declared
output length and CRC32C; historical Zstd frames still read through the same typed
decoder. Native int64 columns and SST block codecs are unchanged.

These profiles locate codec work; they do not qualify end-to-end latency.
The separately seeded document records have generated physical IDs. File sizes
are whole-store observations, not a codec-only byte experiment.

The first latency validation after allocation/cache corrections was canceled
when full-read tails remained slow. Its incomplete receipts are preserved as
`regression-first-validation-incomplete`. It is not a passed matrix.

The sequence's first full read follows small projections: stock already decoded
and cached the large fields during those earlier requests, while the experimental
path deliberately deferred that work. Fresh-cold and fully warmed controls are
therefore required alongside the sequence; its two full-read phases alone do not
compare equal cache histories.

### Retained projection analysis

The exact warmed workload's CPU and lock profiles showed no storage-lock wait
increase. It did repeat the same pure text analysis before each indexed point
read. Retaining that analysis with the existing bounded, sharded cache container
reduced its sampled CPU from 320 ms to 90 ms in the 44,352-request profile.
Inline property keys remain per-call inputs, and returned slices are detached.
The cache is constructed only under the experimental flag; it holds at most
4,096 text analyses, not node images, results or row values. The same parser owns
both cached and uncached analysis.

An extra exact-coverage cache-copy path was implemented and tested, then removed.
It had no established end-to-end benefit to justify adding another read branch.

## Comparable physical preparation

The first S2 latency comparisons used unequal compaction states. Stock had 107
tables in L6; the newly seeded S2 store had five overlapping L0 tables. Even a
body-cache hit still performs the index lookup, so those runs did not isolate the
storage-format change. Their latency results are superseded, including
`fast-property-cold-validation`, `fast-property-warm-validation` and
`proof-fixed-warm-validation`. No result from those matrices qualifies the final
performance comparison.

The [fixture tool](fixture-layout/README.md) copies the input stores, configures
each comparison's own Nornic/Badger encoder and consolidates tables. `Flatten`
does not promise L6: the combined pair ends in L6, while both pure Northwind
stores remain in L0 with four tables each, below the compaction threshold.
The runner now records the normalization receipt and checks the selected source
paths and paired level sets. Original input stores remain unchanged.

Matching preparation changes the S2 combined footprint to **378.509 MiB**, versus
**437.195 MiB** stock: about **13.4% smaller**. The earlier 373.618 MiB S2 size was
uncompacted and is superseded for comparison. The prior Zstd-property store is
369.404 MiB in L6; S2 spends about 9.1 MiB more for its faster property decoder.
Sizes sum file lengths after close, including tables, value logs and metadata.
They do not measure allocated filesystem blocks.

Each trial opens a new copy of its prepared source. Query results are uncached
(`--query-cache-size=0`), while normal syntax, index, block and node caches remain.
Cold means process-cold node images, with 64 connection/query warm-ups; it does
not mean cold device caches. Fully warmed controls touch all 4,352 documents first.
Four alternating trials balance old-first and new-first order. Statistics are
medians of per-run statistics, not pooled percentiles or universal bounds.

### Matched-store results

These results use the retained runtime in `nornicdb-proof-fixed`, build ID
`f8bbc120113afec6f5396099a3e91f7b64981d49`, with public fork `499a805ba32f`.
All paired result fingerprints match. The mixed and control rows below are
medians of four alternating trials.

| Workload | Clients | Stock p95 / p99 ms | Experimental p95 / p99 ms | Stock / experimental queries/s |
| --- | ---: | ---: | ---: | ---: |
| Full document, warmed | 1 | 0.4664 / 0.5485 | 0.5108 / 0.5785 | 1,954 / 1,866 |
| Full document, warmed | 8 | 0.8346 / 1.9513 | 0.7724 / 1.4027 | 8,823 / 8,770 |
| Mixed projections | 8 | 0.7925 / 1.3191 | 0.7290 / 1.0733 | 12,822 / 13,146 |
| Mixed projections | 16 | 2.0876 / 21.7035 | 1.9899 / 21.8374 | 13,099 / 13,396 |
| Mixed projections | 32 | 5.0037 / 26.2100 | 5.0500 / 26.2024 | 13,306 / 13,586 |
| Full document, process-cold | 1 | 0.5733 / 0.7273 | 0.5622 / 0.7162 | 1,741 / 1,730 |
| Full document, process-cold | 8 | 0.8739 / 1.4476 | 0.9044 / 1.6483 | 9,035 / 8,661 |

The codec/projection path does not make every query faster. Single-client warmed
full-document p95 is about 9.5% worse; eight-client cold full-document throughput
is about 4.1% lower. Mixed throughput improves about 2.1–2.5%, with better p95 at
8 and 16 clients and a 0.9% p95 cost at 32. The eight-client warmed full-document
p99 improves about 28%. These are workload-specific tradeoffs, not universal gains.

Median sampled peak RSS for mixed reads is 3,271,888 / 3,113,726 KiB stock/new
at eight clients, 2,947,050 / 3,096,208 KiB at 16, and
2,931,586 / 3,116,312 KiB at 32. Memory does not improve at every concurrency.
Cold full-document medians are 2,439,884 / 2,337,248 KiB at one client and
2,448,268 / 2,338,668 KiB at eight. These are process RSS samples, not a memory cap.

The complete uncached Northwind run has 14 queries, three measured calls each,
one warm-up and one reference call. Overall mean is **4,348.14 ms stock versus
3,951.93 ms experimental**, about 9.1% lower. Three calls do not establish stable
tail percentiles. `low_stock_products` remains materially slower: mean
422.60 / 580.64 ms and p95 423.50 / 638.95 ms. It is the next profiling target;
this report does not mark all known regressions resolved. The remaining small
category lookup mean is 4.21 / 4.35 ms. Supplier revenue mean improves
6,926.90 / 6,814.08 ms while its three-sample p95 is worse, 7,000.22 / 7,185.09 ms.

The receipts are `settled-warm-validation`, `settled-scaling-validation`,
`settled-cold-validation` and `settled-northwind-uncached`. They supersede the
unmatched-compaction matrices above. These matrices precede the scan-admission
correction below; they do not measure that later correction's effect.

### Oversized label scan admission

The low-stock CPU profile found 1.81 seconds (13.2% of sampled CPU) in
`cacheStoreNodeBody`, including copies, projection coverage, identity binding and
eviction. The query scans 48,000 products while the cache holds 10,000 entries;
new projected images evicted one another. Stock did not admit those partial
images. This was additional work, not a useful compression tradeoff.

Both live and pinned-snapshot label walks now reuse the existing lazy label-size
decision to stop new projected-body admissions for a label larger than the entry
budget. Small and early-stopped scans keep their existing behavior and pay no new
count reads. Existing version-matched entries still accumulate missing fields;
point reads keep normal admission. No per-projection entity cache was added.

The public scan regression fails on both live and snapshot paths before the fix,
then passes with the integrated correction. Focused storage, Cypher and multidb
checks pass in 5.320, 0.518 and 0.183 seconds. The canonical fixture tool also
builds successfully. Full suites remain deferred.

The corrected 12-second CPU capture has no samples in `cacheStoreNodeBody`.
Total sampled CPU is 12.35 seconds versus 12.63 seconds stock; sampled allocations
over the 24-request capture are 3,667 MB versus 3,695 MB stock. Fixed-window CPU
profiles diagnose work and are not elapsed-latency comparisons.

A separate four-trial HTTP check runs the unchanged Northwind query definitions,
two warm-ups and 30 measured requests per query. It alternates run order, validates
every result fingerprint and disables query-result caching. HTTP totals include
client response parsing and hashing; do not splice them into the Bolt table.

| Focused HTTP workload | Stock mean / p95 ms | Corrected mean / p95 ms |
| --- | ---: | ---: |
| Low-stock products | 447.67 / 533.28 | 454.58 / 606.57 |
| Products in category | 5.22 / 6.38 | 4.94 / 5.39 |

These medians of per-trial statistics show the large mean slowdown has narrowed,
but do not establish tail parity for the low-stock query.
Receipts: `settled-low-stock-cpu`, `settled-scan-fixed-cpu`,
`settled-low-stock-validation`. The current build ID is
`37eae7820cfdc726b0e8c8e576b0ece8d12a1554`.

The subsequent GC diagnostic captures 100 requests per side with `gctrace=1`.
Including every request, stock mean/p95/p99/max are
431.15/471.12/491.25/497.40 ms; corrected experimental values are
439.48/475.22/631.19/708.47 ms. The three slowest experimental requests are a
consecutive early burst (requests 4–6, zero-based). Only one overlaps a reported
GC interval. GC alone therefore does not explain that burst. Both closed stores
have the same five L0 tables and key count after the diagnostic. No competing
builds or tests ran during it.

This longer diagnostic does not reproduce the short check's 14% p95 difference,
but preserves its warning about outliers: it does not prove p99 parity. A new
storage change should not be justified by treating these limited tail samples as
a proven root cause. The main excess scan admission is corrected and measured;
the residual early-read tail cost remains a qualification limit. No historical
format change is enabled while that limit is under review. Complete request
boundaries, GC lines and layouts are in `settled-low-stock-gc-profile`.

The integrated scan/cache race check also passes (4.545 seconds). An initial
invocation used the invalid `-p1` spelling and failed during setup; the actual
check uses `-p 1`. The retained receipt is the successful run. The earlier
projection-proof and codec race/ANTLR receipts remain separately identified;
they are not claimed as fresh checks of this later scan-only change.

### Focused correctness checks

Focused tests cover frame bounds, truncated values, trailing typed values,
caller/scratch ownership, exact native integers, projected-cache widening,
physical source identity, invalidation and shared cache budgets. Codec race checks
pass. Full suites remain deferred.

Both stock and the S2 build also verified every property of all 4,352 records
against the source corpus, including exact numeric lists and independent byte
noise. The source digest is
`2e04ed37939d4d34d4d8695f0ddac448bb8a1933a0c6ff46b14d7e5f6d0f9b64`.

All four profiled Northwind result sets agree at the benchmark driver's existing
12-significant-digit floating-point precision. Three agree byte for byte; supplier
revenue has summation-order differences in the last floating-point digits. No
comparison tolerance was changed in the benchmark driver.

The raw CPU/allocation profiles, line profiles and results are in
[before Northwind](profiles/northwind-regression-cpu),
[corrected Northwind](profiles/northwind-profile-fixed) and
[cold property allocation](profiles/property-allocation-cold-fixed).

## Next experiment: historical versions

MVCC delta storage is queued after these known regression areas are validated.
The [source audit and experiment design](MVCC-DELTA-DESIGN.md) found existing
inverse-diff history with reconstruction chains; the next experiment targets
bounded reconstruction rather than introducing deltas for the first time.
It will use the same flag. The design must preserve bounded historical resolution
and the existing snapshot semantics, rather than walking an unbounded delta chain.
First inspect the canonical version, retention and compaction owners; then compare
checkpoint-relative deltas against whole-version storage before choosing a layout.
A fixed number of KV operations is not a promise of constant elapsed time or
constant work independent of entity size.
