# Qdrant gRPC Compatibility Layer (Go) — Implementation Plan

**Status:** Proposal / execution plan  
**Owner:** NornicDB Core  
**Primary goal:** Reuse existing Qdrant SDKs (multi-language) by implementing a **Qdrant-compatible gRPC API** as a **first-class endpoint inside the core NornicDB service**, optimized for high throughput and low tail latency.

---

## 1) Why Qdrant gRPC (and not Neo4j gRPC)

- Neo4j’s high-performance ecosystem contract is **Bolt**, not gRPC. NornicDB already targets Neo4j drop-in compatibility via Bolt/Cypher.
- For a vector retrieval contract with multi-language SDK reuse over gRPC, **Qdrant provides an established, published gRPC API** with broad client support and an object model that maps cleanly to NornicDB’s vector index + payload approach.

---

## 2) Scope: What “Qdrant-compatible” means here

### Compatibility target
- **Pin an exact Qdrant server tag** (e.g. `v1.16.x`) and vendor the corresponding protobuf definitions used for gRPC.
- “Compatible” means:
  - Qdrant SDKs can connect and successfully call supported RPCs unmodified.
  - Request/response message shapes match the pinned proto definitions.
  - Behavior matches Qdrant semantics where feasible; any intentional deltas are explicitly documented.

### Non-goals (initial)
- Full Qdrant feature parity (distributed consensus, shard placement, quantization on-disk formats, etc.).
- Replacing Bolt/Cypher; this is a **vector API surface**, not a graph query API.
- Exposing a public internet-facing endpoint by default (design supports it, but rollout is internal-first).

---

## 3) NornicDB ↔ Qdrant Mapping (Core Data Model)

### Collections
**Qdrant `collection_name`** → **Nornic vector index identity**
- Backed by:
  - `pkg/storage` schema/index registry (authoritative metadata)
  - `pkg/search` index instance(s) (runtime)
- Recommended mapping formats:
  - Simple: `collection_name = "<indexName>"`
  - Multi-db (if needed): `collection_name = "<db>/<indexName>"` (routing via `pkg/multidb`)

### Points
**Qdrant Point** → **Nornic Node**
- `PointId` (uuid/num) → `storage.NodeID` (canonical string/uuid inside NornicDB; support numeric input as string)
- `payload` → `node.Properties` (typed conversion layer; keep stable typing)
- `vector` / `vectors` → embedding(s) stored on the node + indexed by HNSW

### Search result
**Qdrant ScoredPoint** → computed from:
- HNSW similarity distance/score from `pkg/search`
- Optional payload retrieval by node id (batched)
- Optional returned vectors (configurable; off by default for perf)

---

## 4) API Surface: Minimal RPC Subset (Phase 1)

The objective is to satisfy the most common SDK call paths for ingestion + search.

### Services to implement (Qdrant gRPC)
1. **CollectionsService**
   - `CreateCollection`
   - `GetCollection`
   - `ListCollections`
   - `DeleteCollection`

2. **PointsService**
   - `Upsert` (batch upsert)
   - `Get` (by ids)
   - `Delete` (by ids / filter as supported)
   - `Search` (single vector)
   - `SearchBatch` (if present in pinned version and commonly used)

3. **Operational**
   - gRPC Health Checking Protocol
   - Server reflection (optional but helpful for debugging)

### Explicit limits (must be enforced)
- `max_vector_dim`, `max_batch_points`, `max_payload_bytes`, `max_top_k`, `max_filter_clauses`
- Hard deadline enforcement and cancellation propagation
- Memory safety guards (avoid allocating based on untrusted sizes)

---

## 5) Performance Principles (Non-negotiable)

### Hot path constraints
- **No JSON** on the data plane. Protobuf only.
- **Batch wherever possible**:
  - Upserts: write nodes in batch, then update index in batch
  - Search payload reads: batch node gets by id
- **Minimize allocations**:
  - Reuse buffers, preallocate slices based on bounded sizes
  - Avoid `map[string]interface{}` churn where possible (use typed payload decoding into internal struct, then serialize once)
- **Avoid unnecessary copies**:
  - Keep vectors as `[]float32` end-to-end
  - Avoid converting float32 ↔ float64

### Concurrency model
- Use `grpc-go` with tuned server options:
  - keepalive, max concurrent streams, max message sizes
  - compression disabled by default (enable only if explicitly configured)
- Collection runtime registry:
  - lock strategy optimized for read-heavy workloads
  - writers batch + minimize lock hold time

---

## 6) Proposed Code Layout (Go Native)

### Core integration (no separate service)
This endpoint is hosted by the main NornicDB process (same lifecycle as Bolt/HTTP/MCP). This avoids extra hops, eliminates cross-service marshalling, and enables direct access to hot-path components (storage engine, search index registry, WAL/transactions, metrics).

Recommended integration options:
- **Option A (preferred):** add a gRPC listener to `pkg/server` and wire it from `cmd/nornicdb`.
- **Option B:** expose gRPC via the protocol plugin system once that architecture is implemented (this plan assumes Option A first for fastest delivery and tight coupling).

### Entrypoint changes
- Extend `cmd/nornicdb` startup to optionally start the Qdrant gRPC server alongside existing endpoints.
- Add configuration in the same config mechanism used for other endpoints (flags and/or `nornicdb.yaml`):
  - `grpc.qdrant.enabled`
  - `grpc.qdrant.listen_addr` (e.g. `:6334`)
  - limits (`grpc.qdrant.max_*`)
  - TLS/mTLS settings (recommended for internal use)

### New packages
- `pkg/qdrantgrpc/`
  - `server.go` (wiring, interceptors, limits)
  - `collections_service.go`
  - `points_service.go`
  - `mapping/` (payload + id + vector mapping helpers)
  - `internal/` (hot-path utilities, pooling, validation)

### Protobuf contract (upstream, pinned)
- Use the upstream Qdrant protobuf contract via `github.com/qdrant/go-client/qdrant` (generated types live in the Go module).
- Pin the exact upstream version in `go.mod` and validate compatibility via the Python SDK E2E suite.

---

## 7) Storage/Search Integration (Tight Coupling, Minimal Abstraction)

### Storage
- Use existing `pkg/storage` engine interfaces for node CRUD and schema/index metadata.
- Define a small internal interface for the gRPC layer that matches exactly what we need:
  - `UpsertPoints(ctx, collection, points) error`
  - `GetPoints(ctx, collection, ids) ([]Point, error)`
  - `DeletePoints(ctx, collection, selector) error`
  - `EnsureCollection(ctx, config) error`

### Search
- Use `pkg/search` HNSW index directly for ANN search.
- Ensure dimension + distance compatibility is enforced at collection creation time.
- Consider read-optimized “index handle” cached per collection:
  - dim, distance, pointer to HNSW, runtime config

---

## 8) Filtering Strategy (Phase 2)

Qdrant filters can be complex. A high-performance approach:

1. **Pushdown where possible**
   - If we have indexed fields or schema hints, filter candidates pre-ANN or during payload fetch.

2. **Oversample then filter (fallback)**
   - Request `top_k * oversample_factor` candidates from HNSW
   - Apply filter in Go with strict caps
   - Return first `top_k` matches
   - If not enough matches, repeat with a bounded retry strategy (or return fewer results)

**Documented behavior:** exact semantics where possible; otherwise “best effort” with deterministic caps and clear errors when limits prevent a complete result set.

---

## 9) Compatibility Testing Strategy (SDK Reuse Validation)

### Must-have
- Automated integration tests that use **real Qdrant clients**:
  - Go Qdrant client
  - Python Qdrant client (if it supports gRPC; otherwise use grpc stubs)
- Golden RPC tests for critical methods (Upsert/Search/Get/Delete).

### Performance validation
- Benchmarks:
  - Upsert throughput (points/sec) vs batch sizes
  - Search QPS at target top-k
  - p50/p95/p99 latency under concurrency
- Load tests should run locally and in CI where feasible; keep CI smoke benchmarks bounded.

---

## 10) Security & Operations

- Internal-first exposure:
  - mTLS inside cluster/VPC (recommended)
  - Optional auth token in metadata for service-to-service calls
- Dangerous operations (collection deletion, bulk delete) must be auditable.
- Add request-scoped logging + trace IDs; propagate context cancellations.
- Metrics:
  - per RPC: count, error rate, latency histogram
  - search: candidate count, filter drop rate, time in ANN vs payload fetch
  - ingest: batch size distribution, index update latency

---

## 11) Phased Delivery Plan (Milestones)

### Milestone A — Contract pin + scaffolding (1–2 days)
- Vendor pinned Qdrant protos, generate Go stubs, add build tooling.
- Add Qdrant gRPC server wiring in the core process with health/reflection.
- Stub services compile and respond with “unimplemented” where needed.

### Milestone B — Minimal functional subset (3–7 days)
- Collections: create/list/get/delete (backed by schema/index registry).
- Points: upsert/get/delete/search (HNSW + payload fetch).
- Integration tests using Qdrant SDK(s) for the supported subset.

### Milestone C — Filters + scroll/count (as needed by SDKs) (3–10 days)
- Implement filter subset (match/range + must/should/must_not).
- Add scroll and count if required by client workflows.

### Milestone D — Perf hardening + observability (ongoing)
- Optimize lock strategy and batching.
- Add metrics and profiling guidance.
- Add regression benchmarks and runbooks.

---

## 12) Workstreams for Parallel Agent Execution (Isolation Boundaries)

These are intentionally designed so multiple agents can work independently with minimal merge conflicts. Each workstream owns a directory/file subset and produces clear artifacts.

### Workstream 1 — Contract Version Pinning + SDK E2E
**Owner files:**
- `go.mod`, `go.sum` (pin Qdrant Go client version)
- `scripts/qdrantgrpc_e2e_python.*` (driver-level compatibility)
- `pkg/qdrantgrpc/README.md` / `pkg/qdrantgrpc/COMPAT.md`
**Deliverables:**
- pinned Qdrant version documented (and validated in CI)
- Python `qdrant-client` E2E passes against NornicDB gRPC
- compatibility notes for any unimplemented RPCs

### Workstream 2 — gRPC Server Skeleton (Runtime/Config)
**Owner files:**
- core server wiring (expected touches in `cmd/nornicdb/**` and `pkg/server/**`)
- `pkg/qdrantgrpc/server.go`
**Deliverables:**
- core process starts gRPC endpoint, health checks pass
- config flags/env documented (port, limits, tls)
- interceptors for deadlines, logging, metrics hooks (minimal overhead)

### Workstream 3 — CollectionsService Implementation
**Owner files:**
- `pkg/qdrantgrpc/collections_service.go`
- storage/schema mapping helpers
**Deliverables:**
- create/list/get/delete collections
- validate vector params (dim/distance)
- persist metadata in schema registry

### Workstream 4 — PointsService (Upsert/Get/Delete)
**Owner files:**
- `pkg/qdrantgrpc/points_service.go` (write operations)
- `pkg/qdrantgrpc/mapping/**` (id + payload conversion)
**Deliverables:**
- upsert points into storage + index update
- get/delete by ids
- strict limit enforcement (batch sizes, payload bytes)

### Workstream 5 — Search Path (ANN + Payload Fetch)
**Owner files:**
- `pkg/qdrantgrpc/points_service.go` (search handlers)
- `pkg/qdrantgrpc/internal/**` (batching helpers, pooling)
**Deliverables:**
- search via HNSW + stable scoring
- batch payload fetch and response assembly
- optional “with payload” and “with vectors” behaviors implemented

### Workstream 6 — Filters + Scroll/Count (Optional / Phase 2)
**Owner files:**
- `pkg/qdrantgrpc/filter.go` (or `mapping/filter/**`)
**Deliverables:**
- filter subset with pushdown/oversample strategy
- scroll/count if required by SDK test harness

### Workstream 7 — SDK Compatibility Tests + Benchmarks
**Owner files:**
- `pkg/qdrantgrpc/*_test.go`
- `pkg/qdrantgrpc/bench_test.go` (benchmarks)
**Deliverables:**
- tests using Qdrant clients/stubs proving compatibility
- benchmark suite and baseline results documented

**Merge policy:** Each workstream should avoid editing other workstreams’ owned files without coordination.

---

## 13) Acceptance Criteria (Definition of Done for Phase 1)

- A Qdrant SDK (at least one official client) can:
  - create a collection
  - upsert points
  - search top-k
  - fetch points by id
  - delete points and collection
- `go test ./...` passes.
- Benchmarks exist and produce stable results; no obvious regression vs baseline search throughput in `pkg/search`.
- Limits and deadlines are enforced; requests cannot crash the server via oversized payloads or vectors.
