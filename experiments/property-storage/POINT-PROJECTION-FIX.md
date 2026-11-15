# Indexed document-point projection correction

The indexed document-point regression is fixed in the measured workload. At eight
clients, median per-run p95 is 0.698 ms versus stock's 0.901 ms, and throughput is
13,672 versus 11,801 queries/s. This is not a claim that every experimental query
beats stock: mixed reads and cold full-body reads retain costs shown below.

## Cause

The first-pass CPU profile reached `sizeTrackingEngine.GetQueryNode`, then decoded
all properties although the statement returned only key, path and status. The
managed database wrapper exposed projected label scans but not projected point
reads. The earlier bare-engine candidate therefore added analysis without carrying
projection through the real server boundary. Its lower-level passing tests did
not qualify that routed path.

Three before profiles contain 1.12 seconds in the shared Cypher point-read owner,
including 0.49 seconds in property `DecodePayload`. The corrected profiles contain
0.48 seconds in that owner and no sampled `DecodePayload` calls. SST block reads,
checksums and decompression still apply; this is not partial physical disk I/O.

For the first 4,000-query phase, allocation profiles report 387.61 MB before and
194.59 MB after. The before property-extension decoder alone accounts for about
141 MB cumulatively. These are sampled allocated bytes, not retained heap or RSS.
Two explicit garbage collections flush allocation samples before and after each
cold profile. The older sustained-profile allocation delta is not used because
unflushed startup samples contaminated it.

## Retained implementation

- Indexed candidate reads reuse the existing read-tail projection proof under the
  same default-off feature flag. Predicates and inline lookup properties remain
  included. Whole entities, mutations and unproven reads retain their existing node-image contract and complete user-property hydration.
- The managed wrapper forwards point projection through storage's existing shared
  delegation owner. Its missing capability has a failing-before regression receipt.
- One versioned body-cache entry belongs to each node. A wider projection reuses
  covered values, decodes only missing properties, then atomically merges coverage
  and values into that entry. Confirmed-absent properties are covered too. The
  cache is not keyed by projection; concurrent fills retain each other's fields.
- Partial entries cannot satisfy a complete-body read. Caller results remain
  detached and contain only requested properties. Coverage counts against the
  existing byte budget and eviction policy; oversized fills retain the prior entry.
- Latest point reads can authorize that same entry through the existing read-view,
  generation and durable source-floor admission. Snapshot fills do not authorize
  latest hits. Pinned snapshots keep physical-version checks. Canonical durable
  writes revoke affected entries before publication; pending overlays retain their
  existing priority. Requests for derived `has_embedding` metadata keep snapshot
  hydration rather than using the raw-body current-value shortcut.

The indexed optimization, partial admission and latest-body shortcut use
`NORNICDB_EXPERIMENTAL_PROPERTY_STORAGE_ENABLED`. No storage format or wire API
was added. The wrapper's additive capability uses the same projection/error
contract as existing engine wrappers.

The unused NPR1 directory, adapter and sample segment were removed from the fork
at `dccab80b05b7`. Its directory cost 24 bytes per property plus a 12-byte header,
never entered Nornic's runtime path, and did not address this regression. The
incomplete rejected-patch artifacts were removed from this branch. The active
projection, payload and integer codecs remain; all existing stored frames remain
readable. Nornic pins the public fork by immutable pseudo-version.

## Measurements

The dataset is unchanged: 97,440 Northwind nodes, 312,050 relationships and 4,352
mixed documentation records. Three trials per engine/client count alternate
engine order. Each phase has 4,000 measured calls after 64 warmups. The result
cache is explicitly disabled with `--query-cache-size=0`; native parsing, GOGC 400,
an 8-GiB Go memory target and the isolated 4-CPU/10-GiB container match the earlier
comparison. Search, embeddings and decay are disabled. Every old/new result
fingerprint matches, and all 4,352 records were checked against every source field
again using the final public-module binary.

Values below are medians of per-trial statistics, not pooled percentiles. Bolt
latency includes Run, Collect and Consume. Hashing/validation follows the latency
timer but is included in throughput wall time and shares the CPU quota. RSS is
sampled every 10 ms for the server alone. OS caches are not controlled, so “cold”
means first entity/body accesses in a fresh process, not physical cold-disk I/O.

The last two ordered phases request full document fields, then repeat that same
query. Stock has already hydrated bodies during prior point projections; the
corrected build defers those bodies until requested. A separate fresh-process
full-body control removes that cache-history asymmetry.
### 1 concurrent client(s)

| Query | Stock mean / p95 / p99 ms | Corrected mean / p95 / p99 ms | Stock / corrected queries/s |
|---|---:|---:|---:|
| document_projection | 0.446 / 0.505 / 0.627 | 0.415 / 0.484 / 0.580 | 2,217 / 2,380 |
| northwind_projection | 0.428 / 0.475 / 0.576 | 0.430 / 0.474 / 0.538 | 2,307 / 2,293 |
| mixed_projection | 0.385 / 0.434 / 0.492 | 0.388 / 0.441 / 0.533 | 2,558 / 2,538 |
| document_batch_projection | 0.630 / 0.817 / 0.980 | 0.638 / 0.735 / 0.901 | 1,520 / 1,507 |
| document_full_control | 0.419 / 0.466 / 0.547 | 0.545 / 0.723 / 0.907 | 1,959 / 1,562 |
| document_full_warm_control | 0.419 / 0.471 / 0.572 | 0.434 / 0.485 / 0.559 | 1,960 / 1,903 |

### 8 concurrent client(s)

| Query | Stock mean / p95 / p99 ms | Corrected mean / p95 / p99 ms | Stock / corrected queries/s |
|---|---:|---:|---:|
| document_projection | 0.662 / 0.901 / 4.627 | 0.577 / 0.698 / 1.034 | 11,801 / 13,672 |
| northwind_projection | 0.585 / 0.815 / 1.065 | 0.563 / 0.738 / 0.963 | 13,269 / 13,794 |
| mixed_projection | 0.523 / 0.631 / 0.812 | 0.543 / 0.672 / 0.897 | 14,870 / 14,273 |
| document_batch_projection | 1.226 / 2.301 / 14.936 | 1.211 / 1.898 / 10.934 | 6,326 / 6,421 |
| document_full_control | 0.690 / 0.758 / 1.210 | 0.918 / 1.175 / 1.783 | 8,948 / 7,363 |
| document_full_warm_control | 0.701 / 0.727 / 1.265 | 0.722 / 0.736 / 1.363 | 9,529 / 9,579 |

### Fresh-process full-body control

| Clients | Stock mean / p95 / p99 ms | Corrected mean / p95 / p99 ms |
|---|---:|---:|
| 1 | 0.480 / 0.557 / 0.715 | 0.535 / 0.668 / 0.812 |
| 8 | 0.860 / 0.982 / 1.764 | 0.883 / 1.071 / 5.986 |

Cold full-body p95 remains 20% higher at one client and 9% higher at eight; its eight-client p99 is also worse. Mixed-query throughput is about 4% lower and p95 about 6% higher at eight clients. These tradeoffs are retained explicitly; the result establishes the indexed document-point correction, not a universal performance win.

### Footprints

| Server/store measure | Stock | Corrected |
|---|---:|---:|
| Mixed-phase sampled server RSS | 2.34 GiB | 2.18 GiB |
| Median per-process maximum sampled RSS | 2.50 GiB | 2.18 GiB |
| Earlier closed-store allocated disk, same source stores | 437.44 MiB | 369.61 MiB |

This iteration changes read routing/cache behavior and removes unused code; it does not claim additional on-disk savings.

## Verification and receipts

The published fork is `orneryd/nornic-badger`, commit
`dccab80b05b7934849a2e640b7e4d43a2cf1bc97`; Nornic's module replacement is
`v4.0.0-20261114232838-dccab80b05b7`.

- The managed wrapper regression fails before the correction and passes after it.
- Indexed query checks cover aliases, predicate fields, flag off/on, whole-entity
  reads, mutations and the streamed managed-database route.
- Cache checks cover owned results, known absence, missing-field selection, one
  entry across wider projections, concurrent union fills, byte/entry budgets,
  complete-read exclusion, latest admission, pinned snapshots, updates and deletes.
- Focused storage/durable-generation tests pass with the flag enabled. Focused
  cache/concurrent-write race checks pass. Native and ANTLR query checks pass with
  the flag enabled and with default initialization, including embedding consumers.
- All benchmark drivers compile against the cleaned public fork. Its remaining
  codec packages pass focused tests and vet. Full suites were not run in this
  iteration; this is an experimental-branch correction, not release qualification.
- SHA-256 checks confirm the benchmark binary's changed runtime source matches
  the workspace. Build/module information and binary hashes are retained.

Raw evidence lives under `profiles/`:

| Directory | Meaning |
|---|---|
| `point-cpu` | Initial sustained profile; allocation delta excluded from conclusions |
| `point-cold-cpu` | Before-correction first-access CPU/allocation profiles |
| `point-current-cpu` | Corrected public-module first-access profiles |
| `point-fixed-query` | Intermediate wrapper/projection change without lazy caching |
| `point-final-query` | Intermediate lazy cache, before the latest-read shortcut |
| `point-current-query` | Current six-phase comparison against stock |
| `point-current-full-cold` | Fresh-process full-body control |
| `point-fix` | Failing proof, final checks, source/module and binary provenance |
| `corpus-verification-final` | All source fields verified against both final stores |

`verify_projected_queries.py` checks phase identities and every paired result
fingerprint before printing the per-run medians. Intermediate results are retained
as evidence, not alternate runtime implementations. The earlier
[combined-query report](COMBINED-QUERY.md) records the original regression.