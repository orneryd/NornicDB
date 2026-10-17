# Vector ANN benchmark: NornicDB vs Qdrant

This benchmark compares NornicDB's vector search with Qdrant's on recall and speed, across every mode each engine supports. Like the Northwind benchmark, it runs one engine and mode at a time, each on a fresh data directory, and writes Markdown reports.

- **Corpus:** real documentation and code from the git repositories you choose. Each file is cut into chunks and embedded once; the embeddings are cached and reused on later runs.
- **Queries:** 1,000 chunks are held out of the corpus as queries.
- **Ground truth:** the exact nearest neighbours of each query, computed by brute force.
- **Same client for both engines:** NornicDB and Qdrant are driven through the same Qdrant gRPC calls (`Upsert`, `Search`).
- **Where the engines run:** NornicDB is built from this checkout and Qdrant runs as its native release binary. The benchmark **never uses a NornicDB you have installed**.
- **Modes without an equivalent:** a mode with no equivalent on one engine is reported `N/A` for that engine.

## Run it

You need:
- Go;
- Python 3.10 or newer;
- A cached corpus, or AWS credentials that can call Bedrock's `amazon.titan-embed-text-v2:0` to create one. An existing `corpus/meta.json` skips embedding entirely; cached runs need no AWS access.

```sh
cd /path/to/NornicDB

# 1. Which repositories make up the corpus (colon-separated git checkouts).
#    Without this, the corpus is this repository alone.
export VECTOR_CORPUS_ROOTS="$PWD:$HOME/src/other-repo:$HOME/src/another-repo"

# 2. AWS credentials for Titan V2 on Bedrock (first run only).
export AWS_PROFILE=my-profile AWS_REGION=us-east-1

# 3. Run everything: it builds NornicDB, downloads Qdrant (pinned, checksum
#    verified), builds the corpus if missing, runs every mode on both engines
#    and writes the reports.
./scripts/benchmark_vector_vs_qdrant.sh
```

Runtime depends on corpus size, hardware and modes; index construction can dominate. The reports go to `testing/benchmarks/vector_ann/reports/<timestamp>/`:

| File | Contents |
|---|---|
| `comparison.md` | NornicDB vs Qdrant per mode (recall@10, p50/p95 latency, QPS), the `hnsw_ef` sweeps side by side, and load time, index time, disk and memory |
| `nornicdb.md`, `qdrant.md` | every run of each engine in detail |
| `<engine>-<mode>.json` | raw results; `<engine>-<mode>.log` is the server log |
| `environment.json`, `corpus.json` | versions, settings, machine, corpus |

### Common variations

```sh
# Only some modes or one engine
VECTOR_MODES=hnsw-fast,hnsw-balanced ./scripts/benchmark_vector_vs_qdrant.sh
VECTOR_ENGINES=nornicdb ./scripts/benchmark_vector_vs_qdrant.sh

# Leave NornicDB's GPU paths on (Metal on macOS); by default the benchmark compares CPU with CPU
NORNIC_GPU=1 ./scripts/benchmark_vector_vs_qdrant.sh

# A smaller or larger corpus (chunks sampled with a fixed seed; 0 = every chunk)
VECTOR_CORPUS_MAX_CHUNKS=20000 VECTOR_CORPUS_DIR=bench-data/vector_ann/corpus-20k ./scripts/benchmark_vector_vs_qdrant.sh
```

### Running the steps yourself

The script runs three Python programs, which you can also run directly. For example, to try a NornicDB setting on one mode without a full sweep:

```sh
PY=bench-data/vector_ann/.venv/bin/python   # created by the script on its first run

# Prepare binaries for direct runs (the shell driver's private binaries are cleaned on exit).
go build -tags "noui nolocalllm" -o bench-data/vector_ann/nornicdb ./cmd/nornicdb
$PY testing/benchmarks/vector_ann/fetch_qdrant.py --cache bench-data/vector_ann/downloads \
  --destination bench-data/vector_ann/qdrant-v1.19.2/qdrant --asset qdrant-aarch64-apple-darwin.tar.gz
# Choose the matching asset from fetch_qdrant.py on another OS/architecture.

# Build (or extend) the corpus.
$PY testing/benchmarks/vector_ann/corpus.py --roots "$VECTOR_CORPUS_ROOTS" --out bench-data/vector_ann/corpus

# One mode, NornicDB only, with extra NornicDB settings, tagged as a variant.
$PY testing/benchmarks/vector_ann/run.py --corpus bench-data/vector_ann/corpus \
  --out testing/benchmarks/vector_ann/reports/my-try --data-root bench-data/vector_ann/data \
  --nornic-bin bench-data/vector_ann/nornicdb --qdrant-bin bench-data/vector_ann/qdrant-v1.19.2/qdrant \
  --engines nornicdb --modes ivf-pq --tag rerank100 --nornic-env NORNICDB_VECTOR_IVFPQ_RERANK_TOPK=100

# Reports for a results directory.
$PY testing/benchmarks/vector_ann/report.py --results testing/benchmarks/vector_ann/reports/my-try --corpus bench-data/vector_ann/corpus
```

## Modes

| Mode | NornicDB | Qdrant |
|---|---|---|
| `exact` | CPU brute force (`NORNICDB_VECTOR_CPU_BRUTE_MAX_N`) | `exact=true` |
| `hnsw-fast` | `NORNICDB_VECTOR_ANN_QUALITY=fast` (M 16, efConstruction 100, efSearch 50) | HNSW m 16, ef_construct 100 |
| `hnsw-balanced` | `balanced` (16 / 200 / 100) | m 16, ef_construct 200 |
| `hnsw-accurate` | `accurate` (32 / 400 / 200) | m 32, ef_construct 400 |
| `ivf-hnsw` | k-means routing (`NORNICDB_KMEANS_CLUSTERING_ENABLED`); restarts after loading, since clustering runs while the index warms up | N/A |
| `ivf-pq` | `compressed` (IVF-PQ) | N/A |
| `sq-int8` | HNSW fast + scalar int8, rescored; persisted originals | HNSW + scalar int8 quantization, rescored |
| `pq-x4`, `pq-x8`, `pq-x16`, `pq-x32`, `pq-x64` | HNSW fast + product quantization at the named ratio, rescored; persisted originals | HNSW + the same product compression ratio, rescored |
| `bq` | HNSW fast + binary, rescored, 2× oversampling; persisted originals | HNSW + binary quantization, rescored, 2× oversampling |
| `bq-asym4`, `bq-asym8` | Binary storage with a 4/8-bit scalar query, rescored, 2× oversampling | Same stored/query precision and rescoring settings |

Compressed HNSW modes create the same quantization policy and send the same
rescore/oversampling parameters through gRPC on both engines. NornicDB's
collection metadata must round-trip the requested policy, and its strategy must
report an active encoded model; pending or uncompressed traversal is rejected.
`x16` means four float32 dimensions per byte (256 code bytes at 1,024 dimensions),
which differs from the separate IVF-PQ profile's segment count. Coordinate bytes
exclude originals, graph links, and storage caches; compare full RSS and disk as
well as latency and recall. The cached vector-only corpus does not establish a
lexical-seeding gain; lexical behavior has separate regression fixtures.

Use `--quantization-oversampling 4 --tag rescore4` with `run.py` to measure a
larger exact-rescoring pool on both engines without changing their storage
precision. Results retain the explicit parameters and variant tag.

The HNSW modes also sweep `hnsw_ef` (16, 32, 64, 128, 256, 512), the same Search parameter on both engines. The baseline uses the named preset's `efSearch` on both engines (explicitly sent to Qdrant); sweep values are also explicit on both.

## What is measured, and how

- **Recall@1 and recall@10:** each Search's results compared with the exact top-K by cosine.
- **Latency:** one client, 1,000 queries after 100 warm-up queries; mean, p50, p95 and p99.
- **Throughput:** queries per second with 8 concurrent RPCs on one shared channel.
- **Load:** time to upsert the corpus in batches of 500 over gRPC.
- **Index ready:**
  - NornicDB: until its search index reports ready and the first Search has answered.
  - Qdrant: until the collection is green with every vector indexed. `indexing_threshold` is set so every segment gets its HNSW index rather than a plain brute-force one.
- **Disk:** allocated blocks under the data directory. Both engines preallocate sparse files, so apparent file sizes would overstate it.
- **Memory:** the server's RSS snapshot after queries and the final readiness check. This is not peak memory or macOS physical footprint; compare the same measure on the same machine.
- **Strategy:**
  - NornicDB: the strategy it reports in its `Vector search strategy:` log line.
  - Qdrant: its collection configuration.

## Settings

All settings are environment variables read by `scripts/benchmark_vector_vs_qdrant.sh`. The script's header documents each one.

| Variable | Default | |
|---|---|---|
| `VECTOR_CORPUS_ROOTS` | this repository | colon-separated git repositories to chunk |
| `VECTOR_VENV_DIR` | `bench-data/vector_ann/.venv` | Python environment; existing imported `venv` files are preserved |
| `VECTOR_CORPUS_DIR` | `bench-data/vector_ann/corpus` | corpus location; rebuilt only when `meta.json` is missing |
| `VECTOR_CORPUS_MAX_CHUNKS` | 100000 | chunk sample size (0 = all) |
| `VECTOR_QUERIES` | 1000 | held-out queries |
| `VECTOR_K` | 10 | recall@K and Search limit |
| `VECTOR_WARMUP` | 100 | unrecorded warm-up queries |
| `VECTOR_CONCURRENCY` | 8 | clients for the throughput measurement |
| `VECTOR_EF_SWEEP` | 16,32,64,128,256,512 | `hnsw_ef` values on HNSW modes |
| `VECTOR_MODES`, `VECTOR_ENGINES` | all | subsets to run |
| `NORNIC_GPU` | 0 | 1 leaves NornicDB's GPU brute force and GPU HNSW build on |
| `QDRANT_VERSION`, `QDRANT_SHA256` | v1.19.2 | pinned Qdrant release |

Ports are fixed and away from NornicDB's and Qdrant's defaults: NornicDB uses HTTP 18474, Bolt 18687, gRPC 16334 and telemetry 19190; Qdrant uses HTTP 17333 and gRPC 17334. `run.py --help` lists the flags to change them.

## Files

| File | Role |
|---|---|
| `corpus.py` | chunks the repositories, embeds them (cached by chunk content in `embeddings-<model>.sqlite`), holds out queries and computes the exact top-100 |
| `run.py` | starts each engine and mode in turn, loads the corpus and measures it; `MODES` defines the modes |
| `report.py` | renders the reports from a results directory |
| `requirements.txt` | Python dependencies, installed into `bench-data/vector_ann/.venv` by the script |
| `../../../scripts/benchmark_vector_vs_qdrant.sh` | builds, fetches, runs and reports |

`bench-data/` is git-ignored: it holds the corpus, the embedding cache, the binaries and the per-run data directories.

## Review and GPU handoff

The runner rejects occupied ports and proves its launched child owns both API
listeners. It removes inherited `NORNICDB_*` and `QDRANT__*` settings; use
`--nornic-env KEY=VALUE` for an explicit, recorded variant. It validates the
observed NornicDB pipeline against the requested mode. Qdrant collection settings
and request parameters are checked; Qdrant's internal per-query planner is not
exposed by this API. Its minimum full-scan threshold is 10 KB.

Each invocation uses private data and report directories. The shell driver
serializes shared preparation through `VECTOR_WORK_DIR/.benchmark.lock` (a
second invocation fails without touching the first). A direct Python runner
locks its output directory. Archives have platform-specific pinned digests and
are verified even when cached; binaries are extracted anew into the private
work directory. Other Qdrant versions require `QDRANT_SHA256`.

Every result records effective settings, actual construction backend, binary and
corpus hashes, Go build settings (including source revision and dirty status),
and resolved Python dependencies. CPU mode selects `NORNICDB_GPU_BACKEND=cpu`,
so GPU clustering is disabled too. Latency is sequential client latency;
throughput is concurrent RPCs over one channel. RSS and allocated disk are
snapshots after all queries and a final readiness check, not peaks. Tagged
variants appear in both comparison and engine reports. Different K values need
separate report directories.

New corpora include row-aligned `payloads.jsonl` and held-out
`query_chunks.jsonl`. The same text payloads go to both engines; NornicDB can use
them for existing BM25 construction hints. Old cached corpora remain usable
without AWS access. Their reports explicitly record vectors-only construction;
do not describe those results as lexical-hint benchmarks.

Public-path regressions for shared index lifecycle and lexical construction:

```sh
go test ./pkg/search ./pkg/qdrantgrpc -run 'Test(HNSWLexicalSeedsFollowVectorIdentity|ANNPublicBuildUsesNamedVectorLexicalSeeds|IVFHNSWPublic|ModelSpaceExclusion|PersistedExactQueriesRespect|SharedIndexDropRecreate|SharedIndexDeleteOrdersStorage)' -count=1
```

On the CUDA machine, build with the native CUDA toolkit and the `cuda` tag:

```sh
go test -tags 'noui nolocalllm cuda' ./pkg/search -run 'TestCUDAResidentCandidateSearchIsExact|TestResidentBuildVectors|TestHNSW.*RecallMatchesCPU|TestCandidateSearchGraphKernel|TestGPUBuildCandidateDepth|TestGPUFirstPipelineUploadOwnsMutationAdmission' -count=1 -v
VECTOR_GO_TAGS='noui nolocalllm cuda' VECTOR_GPU_BACKEND=cuda NORNIC_GPU=1 \
  VECTOR_MODES=hnsw-balanced VECTOR_ENGINES=nornicdb \
  ./scripts/benchmark_vector_vs_qdrant.sh
```

On the Vulkan machine:

```sh
go test -tags 'noui nolocalllm' ./pkg/search -run 'TestVulkanResidentCandidateSearchIsExact|TestResidentBuildVectors|TestHNSW.*RecallMatchesCPU|TestCandidateSearchGraphKernel|TestGPUBuildCandidateDepth|TestGPUFirstPipelineUploadOwnsMutationAdmission' -count=1 -v
VECTOR_GPU_BACKEND=vulkan NORNIC_GPU=1 VECTOR_MODES=hnsw-balanced \
  VECTOR_ENGINES=nornicdb ./scripts/benchmark_vector_vs_qdrant.sh
```

A hardware test that skips is **unverified**, not a pass. Confirm the JSON
`build_backend` is `Cuda` or `Vulkan`, and no construction fallback occurred.
The runner fails a GPU HNSW run if it observes only a CPU builder. Run CPU and
GPU comparisons on the same corpus, machine, queries and beams, with no other
benchmark or verification workload running. Include recall, construction time,
latency, throughput, disk and RSS together; compare repeated runs before making
performance claims. GC-limit and Badger-cache defaults are unchanged by this
work.

Harness contract checks (no server or AWS required):

```sh
python -m unittest discover -s testing/benchmarks/vector_ann -p test_benchmark.py -v
```
