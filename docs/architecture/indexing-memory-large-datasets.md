# Indexing Memory on Large Datasets

How to keep memory usage bounded when building search indexes (especially the vector index) over large datasets. This doc summarizes how Neo4j and Qdrant do it, what NornicDB has implemented, and what remains.

## Current NornicDB Behavior

- **Without a vector index path**: BuildIndexes streams nodes in batches and keeps one normalized private copy of each unit-length embedding. Raw values are retained only for non-unit vectors that need distinct dot/euclidean semantics. HNSW resolves vectors from this immutable store instead of retaining another full copy, so the normal embedding path is ≈1× vector data plus the graph.
- **With a vector index path** (see Implemented below): BuildIndexes always starts from 0 (removes any existing .vec/.meta). Vectors are written to an append-only file during the run; only id→offset is kept in RAM. HNSW uses a vector lookup (no duplicate copy). We do **not** resume partial builds: BM25 is written only at end, so resume would leave BM25 and vectors out of sync. Each run is a full build; the file is only used to bound memory during that run.

## How Neo4j Handles It

- **Vector index** is Lucene-based and uses **OS memory**, not JVM heap. Formula: `Heap + PageCache + 0.25(Vector Index Size) + OS`.
- Index **lives on disk**; the OS pages it in. No “load all vectors into heap” during build.
- **Batch size** for writes is tuned (e.g. ~20k records per batch) to stay within heap; the index itself is written to disk and paged by the OS.
- So: **vectors are not fully resident in process memory**; they’re in a disk-backed index that the OS caches.

## How Qdrant Handles It

1. **Vectors on disk (mmap)**  
   - Collection can set **`on_disk: true`** for vector storage.  
   - Vectors are stored in **AppendableMemmap** backed by **ChunkedMmapVectors**: append-only chunked mmap files.  
   - Process memory stays bounded: only current chunk(s) and metadata (e.g. id → offset) in RAM, not N full vectors.

2. **Defer HNSW build**  
   - **`indexing_threshold`**: HNSW is built only after N vectors (or when explicitly triggered). During bulk ingest you only append to vector storage.  
   - **`m: 0`**: Disable HNSW during ingestion, then set `m` back (e.g. 16) to build the index once.  
   - So **during bulk**: memory = vector storage (mmap or in-RAM) + no HNSW graph yet.

3. **Streaming upload**  
   - **`upload_collection`** (Python): data comes from an **iterator**; the client never holds the full dataset in memory.  
   - Server still needs to store vectors (on disk via mmap or in RAM); the win is client-side and avoiding a single huge request.

4. **Summary**  
   - **Main lever**: store vectors in **chunked mmap (on disk)** so the process doesn’t hold N vectors in RAM.  
   - **Secondary**: defer building the HNSW index until after bulk ingest.

## Implemented in NornicDB

### File-backed vector store + single vector per id

- **VectorFileStore** (`pkg/search/vector_file_store.go`): Append-only storage. Vectors are written to a **.vec** file (binary: length-prefixed id + float32 vector); only **id→offset** is kept in RAM. A **.meta** file (msgpack) stores dimensions and id→offset for Load/Save.
- **Single vector per id**: Only **normalized** vectors are stored (one per id). No `rawVectors` are needed in the file store; cosine is the primary path. This keeps disk-backed storage aligned with the single-copy normal in-memory path.
- **BuildIndexes**: At start we **resume** if a .vec exists and loads cleanly; we rebuild the id→offset map from .vec and compare the **database last-write time** to the .vec file timestamp. If the DB is newer (or the file is **missing/corrupted**), we remove .vec/.meta and start from 0. When a vector index path is set, `ensureBuildVectorFileStore()` creates a VectorFileStore; new embeddings are appended via `addVectorLocked()`. **No checkpoint persist**: the append-only .vec is authoritative. **After indexing**: we persist **BM25 + vector store** first (base indexes), then build HNSW or k-means+IVF-HNSW. The **.vec/.meta files are the canonical persisted vector store** and are kept on disk for reload/resume (no conversion to a single vectors file).
- **Warmup**: HNSW is built from the file in chunks via **VectorFileStore.IterateChunked** (e.g. 10k vectors per chunk). Cluster backfill and IVF-HNSW also use the file store when present.
- **Load/Save**: On startup, if `vectorPath.vec` exists we open VectorFileStore and Load() .meta (used when we skip iteration because both BM25 and vector index were loaded). **runPersist** Syncs and Saves the vector file store (and BM25/HNSW).
- **Effect**: **Peak RAM during indexing** is bounded by chunk size + id→offset map instead of 2–3× full vector data.

### HNSW with vector lookup (no duplicate copy)

- When using the **file-backed** vector store, HNSW is built and served **without** storing a second vector copy in RAM.
- **Build**: `getOrCreateHNSWIndex` calls `SetVectorLookup(getVectorLookup())` before adding vectors; `Add` stores graph + IDs only (vecOff = -1).
- **Load**: `LoadHNSWIndexWithLookupOnly(hnswPath, vectorLookup)` loads graph-only and sets the lookup (vecOff = -1 for all); vectors are resolved at search time via the file store.
- **Search**: `vectorAtLocked` uses `vectorLookup(internalToID)` when vecOff < 0, so no in-memory vector array is needed.
- **Effect**: Post-warmup RAM is graph + id→internal + file store id→offset (no full vector copy in HNSW). Trade-off: more random I/O on the file store during search; benchmark for your workload.

### Resume (vector store)

- We resume **vector indexing** when a .vec/.meta file already exists by skipping IDs already present in the file store, as long as the DB last-write time is **not newer** than the vector file timestamp.
- BM25 is still written once at end of build; if a run stops mid-build, BM25 will be rebuilt on the next run.

## Remaining (optional improvements)

- **BM25 resume** could be added by checkpointing BM25 in a consistent way (segment-based or periodic snapshots) so a mid-build crash doesn't require rebuilding BM25.

## Recommended order of work

1. **Done**: File-backed vector store + single vector per id + chunked HNSW build; resume vectors via .vec/.meta.
2. **Done**: HNSW vector lookup when using the file store (no duplicate vector copy at runtime).
3. **Optional**: BM25 checkpoint/resume to avoid full rebuild after a crash.

## References

- Neo4j: [Vector index memory configuration](https://neo4j.com/docs/operations-manual/current/performance/vector-index-memory-configuration/), batch sizing for writes.  
- Qdrant: [Optimizing memory for bulk uploads](https://qdrant.tech/articles/indexing-optimization), [Large-scale ingestion](https://qdrant.tech/course/essentials/day-4/large-scale-ingestion/); source: `lib/segment/src/vector_storage/` (ChunkedMmapVectors, AppendableMmapDenseVectorStorage, on_disk config).
