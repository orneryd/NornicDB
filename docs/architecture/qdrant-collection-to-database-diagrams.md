# Qdrant Collection-to-Database: Architecture Diagrams

## Overview

This document provides detailed visual diagrams comparing the legacy collection-based architecture (removed) with the current database-based architecture (what exists today in code).

## Breakage Policy (Explicit)

- No migration.
- No backwards compatibility.
- Only databases created via the new `CreateCollection` flow (i.e., containing `_collection_meta`) are treated as Qdrant collections.

---

## Legacy Architecture (Before, removed)

### High-Level Component Diagram

```mermaid
graph TB
    subgraph "Client Layer"
        QC[Qdrant Client<br/>gRPC]
        CC[Cypher Client<br/>Bolt/HTTP]
    end

    subgraph "Qdrant gRPC Server"
        CS[CollectionsService]
        PS[PointsService]
        CR[CollectionRegistry<br/>In-Memory Map]
    end

    subgraph "Storage Layer"
        SE[Storage Engine<br/>Shared Namespace: 'nornic']
        CM[Collection Metadata<br/>Label: _QdrantCollection<br/>ID: _qdrant_collection:bench_col]
        PN[Point Nodes<br/>Labels: QdrantPoint, bench_col<br/>Mixed in shared namespace]
    end

    subgraph "Search Layer"
        SS[Search Service<br/>Shared Index<br/>All Collections]
    end

    QC -->|CreateCollection| CS
    QC -->|UpsertPoints| PS
    CC -->|Cypher Queries| SE

    CS --> CR
    CR -->|Load/Save| SE
    PS -->|Create/Update| SE
    PS -->|Index| SS

    SE --> CM
    SE --> PN
    SS -->|Search| PN

    style CR fill:#ffcccc
    style CM fill:#ffcccc
    style PN fill:#ffcccc
    style SE fill:#ffeeee
```

### Data Storage Structure

```
Storage Engine (Namespace: 'nornic')
│
├── Collection Metadata Nodes
│   ├── _qdrant_collection:bench_col
│   │   └── Properties: {name: "bench_col", dimensions: 1024, distance: "Cosine"}
│   ├── _qdrant_collection:documents
│   │   └── Properties: {name: "documents", dimensions: 768, distance: "Cosine"}
│   └── ...
│
└── Point Nodes (All Collections Mixed)
    ├── point-1
    │   ├── Labels: ["QdrantPoint", "bench_col"]
    │   ├── NamedEmbeddings: {"default": [0.1, 0.2, ...]}
    │   └── Properties: {payload data}
    ├── point-2
    │   ├── Labels: ["QdrantPoint", "bench_col"]
    │   └── ...
    ├── point-1000
    │   ├── Labels: ["QdrantPoint", "documents"]
    │   └── ...
    └── ...
```

### Collection Delete Flow (Current - SLOW)

```mermaid
sequenceDiagram
    participant Client
    participant CS as CollectionsService
    participant CR as CollectionRegistry
    participant SE as Storage Engine
    participant SS as Search Service

    Client->>CS: DeleteCollection("bench_col")
    CS->>CR: DeleteCollection()
    CR->>SE: GetNodesByLabel("bench_col")
    Note over SE: ⚠️ LOADS ALL 20K NODES<br/>INTO MEMORY (SLOW)
    SE-->>CR: [20,000 nodes]
    CR->>CR: Filter QdrantPoint nodes
    CR->>SE: BulkDeleteNodes([20k IDs])
    Note over SE: ⚠️ TRIGGERS 20K NOTIFICATIONS<br/>(BLOCKS)
    SE->>SS: notifyNodeDeleted() × 20k
    Note over SS: ⚠️ REMOVES FROM INDEX<br/>(BLOCKS)
    CR->>SE: DeleteNode("_qdrant_collection:bench_col")
    CR->>CR: Remove from cache
    CR-->>CS: Success
    CS-->>Client: Success

    Note over Client,SS: ⏱️ Total Time: 5-30+ seconds<br/>for 20k points
```

### Point Upsert Flow (Current)

```mermaid
sequenceDiagram
    participant Client
    participant PS as PointsService
    participant CR as CollectionRegistry
    participant SE as Storage Engine
    participant SS as Search Service

    Client->>PS: UpsertPoints("bench_col", points)
    PS->>CR: GetCollection("bench_col")
    CR-->>PS: CollectionMeta
    PS->>SE: CreateNode(point)
    Note over SE: Stored in shared namespace<br/>with collection label
    SE->>SS: IndexNode(point)
    Note over SS: Indexed in shared index
    SS-->>PS: Success
    PS-->>Client: Success
```

---

## Current Architecture (After, implemented)

### High-Level Component Diagram

```mermaid
graph TB
    subgraph "Client Layer"
        QC[Qdrant Client<br/>gRPC]
        CC[Cypher Client<br/>Bolt/HTTP]
    end

    subgraph "Qdrant gRPC Server"
        CS[CollectionsService]
        PS[PointsService]
        DM[DatabaseManager<br/>from nornicdb]
    end

    subgraph "Database Layer"
        DB1[Database: bench_col<br/>Namespace Isolation]
        DB2[Database: documents<br/>Namespace Isolation]
        DB3[Database: ...]
    end

    subgraph "Storage Layer"
        SE1[Storage: bench_col<br/>Namespaced Engine]
        SE2[Storage: documents<br/>Namespaced Engine]
    end

    subgraph "Search Layer"
        SS1[Search Service: bench_col<br/>Isolated Index]
        SS2[Search Service: documents<br/>Isolated Index]
    end

    QC -->|CreateCollection| CS
    QC -->|UpsertPoints| PS
    CC -->|USE bench_col| DB1
    CC -->|USE documents| DB2

    CS --> DM
    PS --> DM
    DM -->|Create/Get| DB1
    DM -->|Create/Get| DB2
    DM -->|Create/Get| DB3

    DB1 --> SE1
    DB2 --> SE2

    DB1 --> SS1
    DB2 --> SS2

    style DM fill:#ccffcc
    style DB1 fill:#ccffcc
    style DB2 fill:#ccffcc
    style SE1 fill:#ccffcc
    style SE2 fill:#ccffcc
    style SS1 fill:#ccffcc
    style SS2 fill:#ccffcc
```

### Data Storage Structure

```
Storage Engine (Multi-Database)
│
├── Database: "bench_col" (Collection = Database)
│   ├── _collection_meta (Required)
│   │   ├── Label: _CollectionMeta
│   │   └── Properties: {dimensions: 1024, distance: "Cosine", schema_version: 1}
│   │
│   └── Point Nodes (Isolated in bench_col namespace)
│       ├── point-1
│       │   ├── NamedEmbeddings: {"default": [0.1, 0.2, ...]}
│       │   └── Properties: {payload data}
│       ├── point-2
│       └── ... (20k points, all in bench_col namespace)
│
├── Database: "documents" (Another Collection)
│   ├── _collection_meta
│   └── Point Nodes (Isolated in documents namespace)
│       └── ...
│
└── Database: "nornic" (Default, non-Qdrant)
    └── Regular graph nodes
```

### Node ID Hygiene (Current)

- Namespace isolates by collection; point IDs must not embed the collection name.
- Reserve `_collection_meta` and all IDs starting with `_` for internal metadata nodes.
- Point node IDs use `qdrant:point:<raw-id>` (collection name is not embedded; the database namespace already scopes it).
- `_collection_meta` is reserved for metadata; IDs beginning with `_` are reserved for internal nodes.

### Collection Delete Flow (Current - FAST)

```mermaid
sequenceDiagram
    participant Client
    participant CS as CollectionsService
    participant DM as DatabaseManager
    participant SE as Storage Engine (Badger)

    Client->>CS: DeleteCollection("bench_col")
    CS->>DM: DeleteDatabase("bench_col")
    Note over DM: ✅ Drops namespace prefix "bench_col:"<br/>No per-point deletes
    DM->>SE: DeleteByPrefix("bench_col:")
    Note over SE: ✅ Uses Badger DropPrefix<br/>+ targeted index cleanup
    DM-->>CS: Success
    CS-->>Client: Success

    Note over Client,SE: ⏱️ Total Time: <100ms<br/>regardless of point count
```

### Point Upsert Flow (Proposed)

```mermaid
sequenceDiagram
    participant Client
    participant PS as PointsService
    participant DM as DatabaseManager
    participant DB as Database: bench_col
    participant SE as Storage Engine
    participant SS as Search Service: bench_col

    Client->>PS: UpsertPoints("bench_col", points)
    PS->>DM: GetDatabase("bench_col")
    DM-->>PS: Database: bench_col
    PS->>DB: CreateNode(point)
    Note over DB: Automatically namespaced<br/>to bench_col
    DB->>SE: CreateNode(point)
    Note over SE: Stored in bench_col namespace<br/>automatic isolation
    DB->>SS: IndexNode(point)
    Note over SS: Indexed in bench_col<br/>isolated index
    SS-->>PS: Success
    PS-->>Client: Success
```

---

## Comparison Matrix

| Aspect                    | Before (Collection Registry)   | After (Database-Based)        |
| ------------------------- | ------------------------------ | ----------------------------- |
| **Storage**               | Shared namespace, label-based  | Namespaced per collection     |
| **Isolation**             | Label filtering                | Namespace isolation           |
| **Delete Performance**    | O(n) - scan all nodes          | O(1) - delete namespace       |
| **Collection Management** | Separate registry              | DatabaseManager               |
| **Cypher Access**         | ❌ No direct access            | ✅ `USE collection MATCH (n)` |
| **Metadata Storage**      | Metadata nodes in shared space | Optional metadata node in DB  |
| **Search Index**          | Shared index, label filtering  | Per-database isolated indexes |
| **Scalability**           | Single shared index            | Per-collection indexes        |
| **Complexity**            | Registry + Storage             | Just DatabaseManager          |

---

## Data Flow: Collection Creation

### Before

```
CreateCollection("bench_col")
    ↓
CollectionRegistry.CreateCollection()
    ↓
Storage.CreateNode(metadataNode)
    ├── ID: "_qdrant_collection:bench_col"
    ├── Label: "_QdrantCollection"
    └── Properties: {name, dimensions, distance}
    ↓
Registry.collections["bench_col"] = meta
```

### After

```
CreateCollection("bench_col")
    ↓
DatabaseManager.CreateDatabase(ctx, DatabaseCreateRequest{Name: "bench_col"})
    ↓
Database created with namespace "bench_col"
    ↓
(Optional) Store metadata node in database
    ├── ID: "_collection_meta"
    ├── Label: "_CollectionMeta"
    └── Properties: {dimensions, distance}
```

---

## Data Flow: Point Query

### Before

```
SearchPoints("bench_col", query)
    ↓
SearchService.Search(query, embedding)
    ↓
Search in shared index (all collections)
    ↓
Filter results by label "bench_col"
    ↓
Return filtered results
```

### After

```
SearchPoints("bench_col", query)
    ↓
DatabaseManager.GetDatabase("bench_col")
    ↓
Database.GetSearchService()
    ↓
SearchService.Search(query, embedding)
    ↓
Search in bench_col index (already isolated)
    ↓
Return results (no filtering needed)
```

---

## Benefits Visualization

### Performance Improvement

```
Delete Collection (20,000 points):

Before:  ████████████████████████████████ 30+ seconds
After:   █  <100ms

Improvement: 300x+ faster
```

### Memory Isolation

```
Before:
┌─────────────────────────────────┐
│  Shared Search Index            │
│  - All collections mixed        │
│  - 20k points from bench_col    │
│  - 10k points from documents    │
│  - 5k points from other         │
└─────────────────────────────────┘

After:
┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
│ bench_col Index   │  │ documents Index  │  │ other Index      │
│ - 20k points      │  │ - 10k points    │  │ - 5k points      │
│ Isolated          │  │ Isolated        │  │ Isolated         │
└──────────────────┘  └──────────────────┘  └──────────────────┘
```

### Query Capabilities

```
Before:
┌─────────────────────────────────────┐
│  Qdrant gRPC API                    │
│  ✅ Search within collection        │
│  ❌ No Cypher access                 │
│  ❌ No cross-collection queries      │
└─────────────────────────────────────┘

After:
┌─────────────────────────────────────┐
│  Qdrant gRPC API                    │
│  ✅ Search within collection        │
│  ✅ Cypher access (NEW!)            │
│  ✅ Cross-collection queries (NEW!) │
│  ✅ Graph queries on points (NEW!)  │
└─────────────────────────────────────┘
```

---

## Deployment Strategy

```
Deployment: Clean Break
┌─────────────────────────────────────┐
│  1. Remove CollectionRegistry         │
│  2. Remove PersistentCollectionRegistry│
│  3. Update all services to use       │
│     DatabaseManager                   │
│  4. All collections = databases      │
└─────────────────────────────────────┘

Note: Existing collections will not be migrated.
They will need to be recreated as databases.
```

---

## Example: Querying Collections via Cypher

### Before: Not Possible

```cypher
-- ❌ Can't query collections directly
-- Must use Qdrant gRPC API
```

### After: Full Cypher Support

```cypher
-- ✅ Query points in a collection
USE bench_col
MATCH (n)
RETURN n
LIMIT 10

-- ✅ Vector search in collection
USE bench_col
CALL db.index.vector.queryNodes('embeddings', 10, 'machine learning')
YIELD node, score
RETURN node, score

-- ✅ Filter by properties
USE bench_col
MATCH (n)
WHERE n.category = 'technology'
RETURN n

-- ✅ Create relationships between points
USE bench_col
MATCH (p1:Point), (p2:Point)
WHERE p1.id = 'point-1' AND p2.id = 'point-2'
CREATE (p1)-[:SIMILAR_TO {score: 0.95}]->(p2)

-- ✅ Cross-collection queries (NEW!)
CALL {
  USE bench_col
  MATCH (n) RETURN 'bench_col' AS db, count(n) AS c
  UNION ALL
  USE documents
  MATCH (n) RETURN 'documents' AS db, count(n) AS c
}
RETURN db, c
```

---

## Implementation Sequence

```mermaid
graph LR
    A[1. Update CollectionsService<br/>Use DatabaseManager] --> B[2. Update PointsService<br/>Use Collection Databases]
    B --> C[3. Update DeleteCollection<br/>Use DeleteDatabase]
    C --> D[4. Update Search<br/>Per-Database Indexes]
    D --> E[5. Remove Old Registry<br/>Cleanup]
    E --> F[6. Verify drop performance<br/>and docs]

    style A fill:#e1f5ff
    style B fill:#e1f5ff
    style C fill:#e1f5ff
    style D fill:#fff4e1
    style E fill:#fff4e1
    style F fill:#ffe1e1
```

---

## Risk Assessment

| Risk                              | Impact | Mitigation                                                  |
| --------------------------------- | ------ | ----------------------------------------------------------- |
| Breaking Qdrant API compatibility | High   | Keep API surface unchanged; update mapping tests and docs   |
| Performance regression            | Medium | Benchmark + profile; optimize drops and hot search paths    |
| Unexpected name validation edge   | Medium | Validate collection/database names; reserved names rejected |

---

## Success Metrics

After refactoring, we should see:

1. **Delete Performance**: Collection delete <100ms (vs 5-30+ seconds)
2. **Query Performance**: Same or better (namespace isolation = automatic filtering)
3. **Memory Usage**: Similar or better (better isolation = can free per-collection)
4. **Code Complexity**: Reduced (remove registry, use existing DatabaseManager)
5. **Feature Completeness**: ✅ Cypher access to collections (new capability)

---

## Next Steps

1. **Review this plan** with the team
2. **Prototype** database-based collection creation
3. **Benchmark** delete performance improvement
4. **Implement** Phase 1 (foundation)
5. **Test** thoroughly before full rollout
