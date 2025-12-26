# Qdrant gRPC Compatibility Tracker

**Target Version:** Qdrant v1.16.x (via `github.com/qdrant/go-client/qdrant`)  
**Goal:** 100% SDK compatibility using the upstream Qdrant gRPC contract  
**Philosophy:** This layer is translation-only; storage/search semantics live in NornicDB core

## Contract

NornicDB exposes a single gRPC surface: the **official upstream Qdrant gRPC contract** (package `qdrant`).

## Embedding Ownership

- If `NORNICDB_EMBEDDING_ENABLED=true` (NornicDB-managed embeddings): vector mutation RPCs are rejected with `FailedPrecondition` to avoid conflicting sources of truth.
- If `NORNICDB_EMBEDDING_ENABLED=false` (client-managed vectors): Qdrant clients can fully manage stored vectors via gRPC.

## Implemented RPCs (SDK-critical)

### Collections

| RPC | Status | Notes |
|-----|--------|-------|
| `Create` | ✅ | Single-vector and named-vector configs |
| `Get` | ✅ | Returns minimal-but-valid `CollectionInfo` (defaults filled for SDK parsing) |
| `List` | ✅ | |
| `Delete` | ✅ | Deletes collection metadata and points |
| `Update` | ✅ | No-op (validates existence; NornicDB manages params) |
| `CollectionExists` | ✅ | |

### Points

| RPC | Status | Notes |
|-----|--------|-------|
| `Upsert` | ✅ | Dense vectors + named vectors |
| `Get` | ✅ | With payload/vectors selectors |
| `Delete` | ✅ | By ID list or filter |
| `Count` | ✅ | |
| `Search` / `SearchBatch` | ✅ | `vector_name` supported |
| `Query` / `QueryBatch` | ✅ | Supports `VectorInput` (Dense/Id) and `Document` when embeddings are enabled |
| `Scroll` | ✅ | |
| `SetPayload` / `OverwritePayload` / `DeletePayload` / `ClearPayload` | ✅ | |
| `UpdateVectors` / `DeleteVectors` | ✅ | Subject to embedding ownership flag |
| `Recommend` / `RecommendBatch` | ✅ | |
| `SearchGroups` | ✅ | |
| `CreateFieldIndex` / `DeleteFieldIndex` | ✅ | |

### Snapshots

| RPC | Status | Notes |
|-----|--------|-------|
| `Create` | ✅ | Collection snapshot as NornicDB snapshot artifact |
| `List` | ✅ | |
| `Delete` | ✅ | |
| `CreateFull` | ✅ | Uses storage backup when available, otherwise full export snapshot |
| `ListFull` | ✅ | |
| `DeleteFull` | ✅ | |

## Tests / Verification

- Unit tests: `go test ./pkg/qdrantgrpc -v`
- Go integration (server feature-flag): `go test ./pkg/server -run QdrantGRPC -v`
- Python SDK compatibility E2E: `./scripts/qdrantgrpc_e2e_python.sh`
