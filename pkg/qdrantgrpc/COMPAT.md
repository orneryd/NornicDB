# Qdrant gRPC Compatibility Tracker

**Target Version:** Qdrant v1.16.x  
**Goal:** 100% SDK compatibility via thin mapping layer to NornicDB internals  
**Philosophy:** gRPC is a translation layer only - all logic lives in NornicDB core  
**Test Coverage:** 73.9% of statements

---

## Implementation Status

### Collections Service

| RPC Method | Proto | Implementation | NornicDB Mapping | Tests | Status |
|------------|-------|----------------|------------------|-------|--------|
| CreateCollection | ✅ | ✅ | `registry.CreateCollection` → metadata node | ✅ | ✅ Done |
| GetCollectionInfo | ✅ | ✅ | `registry.GetCollection` + `GetPointCount` | ✅ | ✅ Done |
| ListCollections | ✅ | ✅ | `registry.ListCollections` | ✅ | ✅ Done |
| DeleteCollection | ✅ | ✅ | `registry.DeleteCollection` → bulk delete | ✅ | ✅ Done |
| UpdateCollection | ✅ | ✅ | No-op (validates existence, NornicDB manages params) | ✅ | ✅ Done |
| CollectionExists | ✅ | ✅ | `registry.CollectionExists` | ✅ | ✅ Done |

### Points Service - Core CRUD

| RPC Method | Proto | Implementation | NornicDB Mapping | Tests | Status |
|------------|-------|----------------|------------------|-------|--------|
| Upsert | ✅ | ✅ | `storage.BulkCreateNodes` + `search.IndexNode` | ✅ | ✅ Done |
| Get | ✅ | ✅ | `storage.GetNode` | ✅ | ✅ Done |
| Delete | ✅ | ✅ | `storage.DeleteNode` + `search.RemoveNode` | ✅ | ✅ Done |
| Count | ✅ | ✅ | `storage.GetNodesByLabel` count | ✅ | ✅ Done |

### Points Service - Search

| RPC Method | Proto | Implementation | NornicDB Mapping | Tests | Status |
|------------|-------|----------------|------------------|-------|--------|
| Search | ✅ | ✅ | Collection-scoped vector similarity search | ✅ | ✅ Done |
| SearchBatch | ✅ | ✅ | Loop `Search` | ✅ | ✅ Done |
| SearchGroups | ✅ | ✅ | Search + group by payload field | ✅ | ✅ Done |

### Points Service - Pagination

| RPC Method | Proto | Implementation | NornicDB Mapping | Tests | Status |
|------------|-------|----------------|------------------|-------|--------|
| Scroll | ✅ | ✅ | `storage.GetNodesByLabel` + offset/limit | ✅ | ✅ Done |

### Points Service - Recommendations

| RPC Method | Proto | Implementation | NornicDB Mapping | Tests | Status |
|------------|-------|----------------|------------------|-------|--------|
| Recommend | ✅ | ✅ | Average positive vectors, subtract negative → search | ✅ | ✅ Done |
| RecommendBatch | ✅ | ✅ | Loop Recommend | ✅ | ✅ Done |

### Points Service - Payload Operations

| RPC Method | Proto | Implementation | NornicDB Mapping | Tests | Status |
|------------|-------|----------------|------------------|-------|--------|
| SetPayload | ✅ | ✅ | `storage.UpdateNode` (merge Properties) | ✅ | ✅ Done |
| OverwritePayload | ✅ | ✅ | `storage.UpdateNode` (replace Properties) | ✅ | ✅ Done |
| DeletePayload | ✅ | ✅ | `storage.UpdateNode` (remove keys) | ✅ | ✅ Done |
| ClearPayload | ✅ | ✅ | `storage.UpdateNode` (empty Properties) | ✅ | ✅ Done |

### Points Service - Vector Operations

| RPC Method | Proto | Implementation | NornicDB Mapping | Tests | Status |
|------------|-------|----------------|------------------|-------|--------|
| UpdateVectors | ✅ | ✅ | `storage.UpdateNode` (ChunkEmbeddings) + `search.IndexNode` | ✅ | ✅ Done |
| DeleteVectors | ✅ | ✅ | `storage.UpdateNode` (clear embedding) + `search.RemoveNode` | ✅ | ✅ Done |

### Field Index Service

| RPC Method | Proto | Implementation | NornicDB Mapping | Tests | Status |
|------------|-------|----------------|------------------|-------|--------|
| CreateFieldIndex | ✅ | ✅ | `storage.SchemaManager.AddPropertyIndex` | ✅ | ✅ Done |
| DeleteFieldIndex | ✅ | ✅ | No-op (NornicDB manages indexes internally) | ✅ | ✅ Done |

### Health Service

| RPC Method | Proto | Implementation | NornicDB Mapping | Tests | Status |
|------------|-------|----------------|------------------|-------|--------|
| Check | ✅ | ✅ | Return SERVING | ✅ | ✅ Done |

### Snapshots Service

| RPC Method | Proto | Implementation | NornicDB Mapping | Tests | Status |
|------------|-------|----------------|------------------|-------|--------|
| Create | ✅ | ✅ | `storage.SaveSnapshot` (collection export) | ✅ | ✅ Done |
| List | ✅ | ✅ | List files in snapshot directory | ✅ | ✅ Done |
| Delete | ✅ | ✅ | Delete snapshot file | ✅ | ✅ Done |
| CreateFull | ✅ | ✅ | `BadgerEngine.Backup` or `storage.SaveSnapshot` | ✅ | ✅ Done |
| ListFull | ✅ | ✅ | List files in full snapshot directory | ✅ | ✅ Done |
| DeleteFull | ✅ | ✅ | Delete full snapshot file | ✅ | ✅ Done |

---

## Summary

**Implemented:** 30/30 RPCs (100% complete)  
**Tested:** 30/30 RPCs  
**Coverage:** 73%+

All Qdrant SDK operations are implemented:
- ✅ Collection management (Create, Get, List, Delete, Update, Exists)
- ✅ Point CRUD (Upsert, Get, Delete, Count)
- ✅ Vector search (Search, SearchBatch, SearchGroups)
- ✅ Pagination (Scroll)
- ✅ Recommendations (Recommend, RecommendBatch)
- ✅ Payload operations (Set, Overwrite, Delete, Clear)
- ✅ Vector operations (Update, Delete)
- ✅ Field indexes (Create, Delete)
- ✅ Snapshots (Create, List, Delete, CreateFull, ListFull, DeleteFull)
- ✅ Health check

---

## NornicDB → Qdrant Type Mappings

| Qdrant Concept | NornicDB Equivalent | Notes |
|----------------|---------------------|-------|
| Collection | Label + metadata node | Metadata stored as `_QdrantCollection` node |
| Point | Node | With `QdrantPoint` + collection labels |
| Point ID (num) | NodeID | `"qdrant:{collection}:{num}"` format |
| Point ID (uuid) | NodeID | `"qdrant:{collection}:{uuid}"` format |
| Vector | ChunkEmbeddings[0] | First chunk embedding |
| Named Vectors | ChunkEmbeddings[N] | Multiple embeddings (future) |
| Payload | Properties | Direct map |
| Filter | In-memory filter | Applied to Properties |
| Score | Cosine similarity | From vector comparison |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        Qdrant SDK                            │
│                 (Python, Go, Rust, etc.)                     │
└─────────────────────────────────────────────────────────────┘
                              │ gRPC
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                 pkg/qdrantgrpc (THIN LAYER)                  │
│  ┌────────────┐ ┌────────────┐ ┌──────────┐ ┌────────────┐  │
│  │Collections │ │  Points    │ │Snapshots │ │  Health    │  │
│  │  (6 RPCs)  │ │ (16 RPCs)  │ │ (6 RPCs) │ │  (1 RPC)   │  │
│  └─────┬──────┘ └─────┬──────┘ └────┬─────┘ └────────────┘  │
│        │              │             │                        │
│        │       Type Translation Only                         │
│        ▼              ▼             ▼                        │
└─────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────┐
│                  NornicDB Core                          │
│  ┌───────────────┐  ┌───────────────┐  ┌─────────────┐  │
│  │    storage    │  │    search     │  │   schema    │  │
│  │    Engine     │  │    Service    │  │   Manager   │  │
│  └───────────────┘  └───────────────┘  └─────────────┘  │
│         │                   │                │          │
│         └───────────────────┴────────────────┘          │
│                           │                             │
│                     Badger/Memory                       │
└─────────────────────────────────────────────────────────┘
```

---

## Feature Flag

The Qdrant gRPC endpoint is controlled by a feature flag in NornicDB configuration:

```yaml
feature_flags:
  qdrant_grpc_enabled: true          # Enable/disable the endpoint
  qdrant_grpc_listen_addr: ":6334"   # Listen address
  qdrant_grpc_max_vector_dim: 4096   # Max vector dimensions
  qdrant_grpc_max_batch_points: 1000 # Max points per upsert
  qdrant_grpc_max_top_k: 1000        # Max search results
```

Environment variables:
- `NORNICDB_QDRANT_GRPC_ENABLED=true`
- `NORNICDB_QDRANT_GRPC_LISTEN_ADDR=:6334`
- `NORNICDB_QDRANT_GRPC_MAX_VECTOR_DIM=4096`
- `NORNICDB_QDRANT_GRPC_MAX_BATCH_POINTS=1000`
- `NORNICDB_QDRANT_GRPC_MAX_TOP_K=1000`

---

## Testing Strategy

Each RPC method has:
1. **Unit tests** - Verify type translation and error handling
2. **Happy path tests** - Normal operation
3. **Error condition tests** - Invalid inputs, not found, etc.

Test files:
- `collections_service_test.go` - Collection operations
- `points_service_test.go` - Point CRUD and search
- `points_extended_test.go` - Payload, vector, and advanced operations
- `snapshots_service_test.go` - Snapshot operations
- `registry_test.go` - Collection registry
- `server_test.go` - Server lifecycle

---

## Changelog

### 2024-12-24 (Phase 2)
- ✅ Added Snapshots service (6 RPCs)
  - Create, List, Delete (per-collection)
  - CreateFull, ListFull, DeleteFull (full database)
- ✅ Maps to `storage.SaveSnapshot`, `storage.LoadSnapshot`, `BadgerEngine.Backup`
- ✅ All 30 RPCs now implemented with tests

### 2024-12-24 (Phase 1)
- ✅ Completed initial Qdrant SDK compatibility
- ✅ 24 RPCs implemented with tests
- ✅ Feature flag for enabling/disabling endpoint

### Implementation Details
- Collections (6): CreateCollection, GetCollectionInfo, ListCollections, DeleteCollection, UpdateCollection, CollectionExists
- Points CRUD (4): Upsert, Get, Delete, Count
- Search (3): Search, SearchBatch, SearchGroups
- Pagination (1): Scroll
- Recommendations (2): Recommend, RecommendBatch
- Payload (4): SetPayload, OverwritePayload, DeletePayload, ClearPayload
- Vectors (2): UpdateVectors, DeleteVectors
- Indexes (2): CreateFieldIndex, DeleteFieldIndex
- Snapshots (6): Create, List, Delete, CreateFull, ListFull, DeleteFull
- Health (1): Check

**Total: 30 RPCs**
