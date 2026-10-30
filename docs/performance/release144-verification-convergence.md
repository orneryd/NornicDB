# Release v1.4.4 verification catch-up: Northwind evidence

Compared release `f0c932b902b4112114eaeb1fd8a160a6b2bb05cc` with correction
`10806a942839aa2fd3b278042b26f671e9a7585a` on an Apple M2 Max with 32 GiB
unified memory, macOS arm64 and Go 1.26.9. Both binaries used `-tags noui`
and the same native llama static library; embeddings were disabled.

The [portable samples](testdata/release144-verification-convergence.json) retain
all 30 measured query latencies per query and all run-level results. Raw profiles,
commands and logs are retained in the local verification evidence directory.

## Matching workload and isolation

Each fresh process ingested 97,440 nodes and 312,050 relationships (48,000 products,
48,000 orders, 96 categories, 144 suppliers, 1,200 customers; seed 42). Batches had
2,000 rows with four seed sessions. The existing native node-cache setting was
100,000 entries, async writes were enabled and memory decay was disabled. A neutral
explicit config prevented the operator's home config from changing the workload.

For each graph mode the run order was release, correction, correction, release,
release, correction. Processes ran serially with no Docker containers, local Qwen,
Go tests or builds running. The model service was restored afterward. Each query
had five unrecorded warmups and 30 measured iterations. Measured cold/outlier
samples remain in means; medians do not replace those means.

## Results at the benchmark's GOGC=400 setting

Three unprofiled samples per binary/mode; seed time includes setup and ingestion.

| Mode | Release seed median (range), s | Correction seed median (range), s | Ingestion-only median, s | Query-suite mean median, ms |
|---|---|---|---|---|
| Graph only | 6.331 (5.480–6.521) | 5.490 (5.396–5.513) | 6.269 → 5.429 | 0.1666 → 0.1709 |
| Search enabled | 6.785 (6.242–6.984) | 6.354 (6.197–7.025) | 6.724 → 6.294 | 0.1734 → 0.1772 |

Ingestion medians improve 13.4% and 6.4%, respectively. Query-suite mean medians
change approximately +2.6% and +2.2%; these small differences are within the
observed run ranges. Every run preserves all query row counts and result hashes.
This does not establish a five-second seed time with search enabled.

### Memory qualification and its limits

The aggressive **unbounded** GOGC=400 configuration has higher sampled peak-RSS
medians: graph-only 6.553 → 8.367 GiB (+27.7%), search-enabled 6.627 → 7.305 GiB
(+10.2%). The respective ranges are 6.182–8.155 versus 8.179–8.451 GiB, and
6.601–7.572 versus 7.153–7.804 GiB. RSS was sampled once per second; it is not
an exact maximum or a live-heap measurement. This resident-memory tradeoff is
retained explicitly rather than described as a memory improvement.

A separate paired graph-only diagnostic at Go's default GOGC=100 measured:

| Metric | Release | Correction |
|---|---|---|
| Seed time | 5.343 s | 5.438 s |
| Ingestion only | 5.282 s | 5.377 s |
| Sampled peak RSS | 3.466 GiB | 3.460 GiB |

That single pair shows no RSS increase at the ordinary GC setting; it is not a
multi-sample timing claim. Dedicated cumulative profiles at GOGC=400 measured
24,273 → 23,487 MB allocated across startup, ingest and queries (approximately
3.2% fewer sampled bytes), 227.33 → 229.40 million sampled allocation objects
(+0.9%), and 906.05 → 906.97 MB live heap after forced GC (approximately +0.1%).
These are whole-process sampled profiles, not exact per-ingest allocations.
The data is consistent with GC/heap-reservation timing rather than retained-heap
growth. It does not erase the higher unbounded-GC resident-memory median.

The correction is retained for restored allocation ceilings and lower allocated
bytes, faster ingestion in the primary comparison, unchanged post-GC live heap,
and comparable ordinary-GC memory. No new product memory or search setting is
introduced. The existing benchmark GC/memory controls remain operator choices.

## Profiling

A separate correction/search-enabled run captured a four-second CPU window
within its 6.801-second seed phase. It is excluded from timing aggregates.
The largest flat samples were runtime memory/scavenging operations (madvise
27.2%, memmove 11.9%, memclr 5.9%) and scheduling/wakeup operations. This is
workload-specific evidence; it does not establish a new Badger bottleneck or
justify a dependency fork.

The original matrix's heap/allocs requests used `seconds=1`, which produces an
idle delta profile after the workload. Those empty profiles are excluded from
allocation conclusions. The dedicated diagnostics use cumulative allocs and
`heap?gc=1` instead.

## Earlier experiments

Earlier cache-capacity comparisons are invalid because their external runner
stripped the requested cache environment setting. They provide no 10k-versus-100k
evidence. A separate controlled experiment explained the seconds-long query gap:
search-enabled runs had inherited `memory.decay_enabled: true` from the home
config. Decay-on and decay-off runs are different workloads. The repository
benchmark now supplies an explicit config; existing explicit environment settings
still select the requested behavior.

The older decay-off products-per-category result retains an 8.6263 ms measured
mean and a 0.1925 ms median. Its first two slow samples were measured values,
not warmups, and are not removed. Current samples above retain their full
query distributions as well.
