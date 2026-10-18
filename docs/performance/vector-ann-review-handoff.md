# Qdrant shared-index and ANN tuning handoff

This work completes the shared-index correction in [#1372](https://github.com/orneryd/NornicDB/pull/1372)
and the comparison harness in [#1375](https://github.com/orneryd/NornicDB/pull/1375), for
[#1370](https://github.com/orneryd/NornicDB/issues/1370).

## Contracts and preserved reproductions

| Contract | Original failing evidence | Integrated correction |
|---|---|---|
| Lazy first reads and incomplete builds | All three metrics returned no hits without warming; partial builds answered successfully | Optioned/Qdrant reads use the service warming/readiness boundary; existing Cypher scalar reads retain their live-index contract |
| Population versus live updates/deletes | The paused population replaced a completed update (score 0 instead of 1) and restored a deleted point | The existing base-operation mutex orders live node mutations and derived work against population and source clearing |
| Durable Qdrant deletion versus population | A public RPC removed the index entry, then paused storage deletion; population restored it after acknowledgment (2 embeddings instead of 1) | Delete durable storage first, then enter the shared-index mutation boundary, as upserts already do |
| Fixed dimensions | A 2-d explicit embedding on a fixed 4-d index returned success and discarded its previous indexed vector | Validate explicit vectors before replacing index entries; propagate indexing/provider failures to gRPC callers |
| File-backed Dot/Euclid | Dot score for `[3,4,0,0]` against itself was 5 instead of 25 | Resolve original durable vectors for magnitude-sensitive scoring; cosine coordinates remain normalized and single-copy |
| Embedded drop/recreate | Old point 1 remained alongside new point 3 | The constructor's default drop path clears the shared service and removes persisted vector, HNSW, IVF-HNSW and IVF-PQ artifacts |
| Collection dimension adoption | Cluster dimension remained 1024 after its vector source adopted 4, rejecting all clustering backfill | Replace the cluster backend with the adopted dimension while preserving its manager and policy |
| GPU graph completion | A 40-node descent with a one-round cap returned node 1 instead of node 39 | Expand every retained beam node completely; capped unions defer whole adjacency lists |
| IVF-HNSW global candidate budget | Public routed query lost nearest points concentrated in one probed topic cluster | Each cluster can contribute the full requested top-K before the global merge |
| Named/chunk/property lexical identity | Real public HNSW construction stored hints under document IDs; named-vector cluster profiles were empty | Resolve documents with the existing node-ID normalizer and bind construction hints to actual embedding IDs; expand BM25 seed documents through the existing vector metadata for k-means/PQ |
| Lexical route in exact cluster fallback | After restoring profiles, public hybrid results had vector rank 0 for the lexical-selected topic | Exact fallback scores the selected members instead of choosing clusters again by vector proximity |
| GPU candidate setting | The candidate-depth configuration was read but unused | Honor it above the M/efConstruction quality floor |
| Benchmark identity | Inherited settings could change modes; fixed paths and ports could select/delete another run | Private directories, explicit settings, child listener ownership, strategy checks and failure cleanup |
| Benchmark reporting/integrity | Tagged runs vanished; K=5 appeared under recall@10; cached executables bypassed verification | Report the measured population and K; verify every cached archive against the correct platform digest |

The fail-first logs are retained locally as `/tmp/nornic-1372-*-before.log`
and `/tmp/nornic-1375-before.log`. The permanent regressions exercise the public
service and gRPC entry points, including cold database/manager/provider
reconstruction, two-collection isolation, persistent drop/recreate, lazy Search
and Query, and provider failure.

## GPU parity and limits

CUDA and Vulkan retain immutable device shards and share internal-ID ordering,
cosine-distance conversion and global top-K merging. Their existing kernels
score those buffers directly; the graph path remains available. Metal retains
its contiguous resident buffer and MPS scoring. Lexical seeds, signature tie
ordering and graph linking remain in the common builder.

`TestMetalResidentCandidateSearchIsExact`,
`TestCUDAResidentCandidateSearchIsExact` and
`TestVulkanResidentCandidateSearchIsExact` run the same device contract. A skip
means the backend was not verified. The device-independent tests exercise
shard ordering, upload-once behavior, cancellation/errors and lexical/graph
construction recall, but do not establish CUDA/Vulkan native performance.
CUDA native compilation and execution require the toolkit and hardware on the
other machine. The benchmark README on #1375 has exact commands and requires
an observed native construction backend before accepting a GPU HNSW result.

## Tuning boundaries

The lazy GPU brute-force allocation and debounced clustering trigger are
retained. The cluster dimension correction makes the latter usable with a
collection dimension different from the server's embedding default.
`--gpu-backend cpu` disables the manager, including GPU clustering, so a CPU
comparison cannot silently use the GPU. GC-limit and Badger-cache defaults are
unchanged; RSS snapshots are workload measurements, not proof that those
default tradeoffs should change.

The IVF-PQ floor of 200 is a tuning choice, not a corpus-independent recall
guarantee. Compare explicit floors of 200 and 2000 on the same corpus when
assessing the tradeoff. Selective filtering in compressed mode can still miss
matches in unprobed lists; `params.exact=true` evaluates the complete indexed
population when exact results are required.

The HNSW heap change was measured on an Apple M2 Max (12 cores, 32 GiB),
10,000 x 128-d seeded vectors, 200 candidates, three 500 ms repetitions:

| Traversal beam | Before median | After median | Throughput before -> after | Allocations |
|---|---:|---:|---:|---:|
| 200 | 267,153 ns | 261,096 ns | 3,743 -> 3,830 queries/s | 8 -> 8 |
| 800 | 706,815 ns | 686,720 ns | 1,415 -> 1,456 queries/s | 8 -> 8 |

This approximately 2–3% improvement does not establish parity with Qdrant on
other workloads. Real-corpus reports must name their hardware, source/binary
hashes, recall, construction time, latency, throughput, disk and RSS together.
The cached 88,155 x 1,024 Titan corpus has no text payload file; it supports a
vector-only comparison without AWS access. Lexical construction quality is
covered separately by the lexical-hint regression, not inferred from that
corpus's vector-only results.

## Full-corpus follow-up corrections

The first complete 13-mode sweep at `5f7e30163` exposed poor IVF-HNSW recall
(0.6399). Its logs and results are preserved as a baseline. A public query with
32 probed topic clusters reproduced a separate deterministic loss: the generator
split its candidate budget between clusters even though all top results can lie
in one cluster. That quota is removed; nearest-cluster probing remains an
approximation and higher probe counts must still be measured.

Lexical regression fixtures also found a shared identity mismatch. BM25 owns
node IDs while named, property and later-chunk vectors have embedding IDs.
The shared construction boundary now binds cached hints to embedding IDs while
iterators retain their parent-document metadata and document-anchor ordering.
Clustering/PQ seed selection resolves the same indexed document metadata,
preserving a directly indexed document anchor when one exists. This is
shared preparation for CPU, Metal, CUDA and Vulkan, not a new ranking formula.
A public hybrid-search regression proves that an exact fallback for a small
cluster retains lexical selection and contributes the selected results to the
vector branch. Existing persisted graphs/codes remain usable; new construction
uses the corrected hints.

The permanent tests include `TestIVFHNSWPublicQueryPreservesConcentratedNeighbours`,
`TestIVFHNSWPublicHybridSearchPreservesLexicalFallbackRoute`,
`TestANNPublicBuildUsesNamedVectorLexicalSeeds`, and
`TestHNSWLexicalSeedsFollowVectorIdentity`. A controlled overlay of the previous
consumer implementations reproduces the failures; the integrated correction
must pass current verification. The preferred-seed list is consumed during
clustering, so its absence after training is not itself a defect; the regression
inspects resolved seed IDs by reapplying their existing owner.

`TestSharedIndexDeleteOrdersStorageBeforePopulationBoundary` extends the gRPC
population proof to a population that begins during durable deletion, rather
than one already running when the mutation starts. The previous storage/index
order failed (2 indexed embeddings after deleting one of two points); the final
order retains one embedding and returns the surviving point through gRPC.
