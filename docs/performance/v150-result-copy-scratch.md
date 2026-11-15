# Result-copy scratch cost

Result-cache hits return detached mutable values. Changing a delivered map, list, node or vector must not change the cached image or another delivery. This contract requires copying returned mutable payloads; this change does not remove that cost.

The identity index used during a copy previously stored each source identity and reflected destination in a growing Go map. Growth allocated replacement tables containing complete records. The copy owner now appends complete records to bounded blocks and grows a table of eight-byte handles. Lookup, insertion and rehashing share one probe. The four inline identities and 1,024-reference pooling limit are unchanged. Reused indexes clear every retained reference. Larger indexes are discarded.

## Measurements

Apple M2 Max, macOS arm64, Go 1.27.3, `GOMAXPROCS=4`. Baseline is release `6a29b276d6fb4e13802f1044738d93e27a650967`, with the same benchmark fixture added to both binaries. Six interleaved samples (baseline, candidate, candidate, baseline, candidate, baseline), 30 operations per sample; no concurrent Go build or test process was observed during those samples. Values below are medians of three samples per arm, for `BenchmarkNOR307QueryResultCache/.../query-cache/hit`.

| Result shape | Baseline bytes/op | Candidate bytes/op | Baseline allocations/op | Candidate allocations/op | Baseline time | Candidate time |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 21,748 scalar/list rows | 11,702,359 | 7,157,803 | 87,283 | 87,204 | 6.973 ms | 7.311 ms |
| 21,748 maps, 18 fields each | 48,629,452 | 38,132,328 | 261,523 | 261,273 | 30.596 ms | 31.971 ms |
| 100 scalar/list rows | 16,064 | 16,064 | 415 | 415 | 21.000 us | 19.593 us |
| 100 entity rows | 64,064 | 64,064 | 815 | 815 | 38.940 us | 35.947 us |
| 25 metadata rows | 16,416 | 16,416 | 215 | 215 | 10.261 us | 9.922 us |
| 25 vector-bearing rows | 13,203,765 | 13,203,776 | 3,440 | 3,440 | 1.868 ms | 1.905 ms |

Wide-result temporary bytes fall by 38.8% and 21.6%. Wide-result median times increase by 4.8% and 4.5% in this short sample; this is a memory improvement, not a latency improvement. Small-result timing varied, including slower samples through the smart cache; unchanged allocation counts do not establish unchanged latency. Do not interpret these three samples as statistical latency qualification. A separate one-second CPU/allocation profile of the wide-map workload measured 48,626,833 versus 38,133,110 bytes/op and 30.364 versus 31.786 ms/op.

## Scope and verification

Controls cover repeated table growth, aliases, cycles, slice length/type identity, pool clearing, successive-copy independence, both result-cache owners and public cached execution. Cache tests include 256 wide-map rows and 25 vector-bearing rows; the benchmark owns the 21,748-row scale.

This is a partial correction for the cached-read cost report. It does not eliminate the required vector-output copy, reproduce every application query, or restore the old shared-mutable result behavior. Badger payloads, keys, WAL and public Go APIs are unchanged. Full CI and workload-wide latency qualification remain separate gates.
