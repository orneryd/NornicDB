# Embedding Search Flow Diagrams

**Visual representations of NornicDB's embedding search architecture.**

## Complete Search Flow

```mermaid
flowchart TB
    subgraph API["API Layer"]
        HTTP["HTTP REST<br/>POST /nornicdb/search"]
        MCP["MCP Tool<br/>discover / recall"]
        GRPC["Qdrant gRPC<br/>Points.Search / Points.Query"]
        CYPHER["Cypher<br/>CALL db.index.vector.queryNodes"]
    end

    subgraph SEARCH["Indexed Search Path (search.Service)"]
        SearchService["search.Service<br/>pkg/search/search.go"]
        VectorIndex["Vector Search Pipeline<br/>GPU brute (exact) / CPU brute / global HNSW (ANN) / K-means routing / IVF-HNSW (centroids → per-cluster HNSW)"]
        BM25Index["BM25 Index<br/>Full-text"]
        CypherVector["VectorQueryNodes (Cypher semantics)<br/>index-backed (no storage scan)"]
        CypherMeta["Cypher embedding metadata cache<br/>labels + vector IDs (named/prop/chunk)"]
        SearchService --> VectorIndex
        SearchService --> BM25Index
        SearchService --> CypherVector
        CypherVector --> CypherMeta
    end

    subgraph CYPHERPATH["Cypher Vector Procedure (db.index.vector.queryNodes)"]
        CypherExec["cypher.StorageExecutor<br/>pkg/cypher/call_vector.go"]
        CypherExec --> SearchService
    end

    subgraph STORAGE["Storage"]
        Store[("storage.Engine<br/>(Badger/WAL/Async)")]
        NamedEmb["NamedEmbeddings<br/>map[string][]float32"]
        ChunkEmb["ChunkEmbeddings<br/>[][]float32"]
        PropEmb["Properties vectors<br/>(e.g. n.embedding)"]
        Store --> NamedEmb
        Store --> ChunkEmb
        Store --> PropEmb
    end

    HTTP --> SearchService
    MCP --> SearchService
    GRPC --> SearchService

    CYPHER --> CypherExec

    VectorIndex --> Store
    BM25Index --> Store

    style SearchService fill:#4a90e2,color:#fff
    style CypherExec fill:#ffa500,color:#fff
    style VectorIndex fill:#ff6b6b,color:#fff
    style BM25Index fill:#50c878,color:#fff
```

## Vector Strategy Selection (GPU brute → HNSW)

```mermaid
flowchart TD
    Start["Vector query (embedding)"] --> CallerK["Caller chooses k (overfetch)<br/>to compensate for ID collapse"]
    CallerK --> Select["Select vector pipeline<br/>(search.Service.getOrCreateVectorPipeline)"]

    Select --> GPUAvail{"GPU brute enabled<br/>AND N in [min,max]?"}
    GPUAvail -->|Yes| GPUBrute["GPU brute candidate gen (exact)<br/>gpu.EmbeddingIndex.Search()"]
    GPUAvail -->|No| Clustered{"clusterIndex.IsClustered()?"}

    Clustered -->|Yes| ClusterGPU{"GPU enabled<br/>AND N > max?"}
    ClusterGPU -->|Yes| GPUKMeans["GPU k-means routing<br/>centroids → cluster IDs → ScoreSubset()"]
    ClusterGPU -->|No| ClusterMode{"IVF-HNSW enabled<br/>AND GPU disabled<br/>AND per-cluster HNSW built?"}
    ClusterMode -->|Yes| IVFHNSW["IVF-HNSW candidate gen (CPU)<br/>centroids → per-cluster HNSW"]
    ClusterMode -->|No| KMeans["K-means candidate gen (CPU)<br/>centroids → vectors in nearest clusters"]

    Clustered -->|No| Small{"CPU brute max N > 0<br/>AND N < max?"}
    Small -->|Yes| CPUBrute["CPU brute candidate gen (exact)<br/>VectorIndex.Search()"]
    Small -->|No| HNSW["Global HNSW candidate gen (ANN, CPU)"]

    IVFHNSW --> Pipeline["Pipeline: candidate gen → exact score (CPU SIMD) → top-k"]
    KMeans --> Pipeline
    GPUBrute --> Pipeline
    GPUKMeans --> Pipeline
    CPUBrute --> Pipeline
    HNSW --> Pipeline

    Pipeline --> IDs["Vector IDs + scores"]
    IDs --> Collapse["Collapse IDs to nodeID<br/>chunk/named/prop → nodeID"]
    Collapse --> TopK["Return top-K node IDs"]

    style GPUBrute fill:#4a90e2,color:#fff
    style HNSW fill:#ff6b6b,color:#fff
    style KMeans fill:#50c878,color:#fff
    style IVFHNSW fill:#ffa500,color:#fff
    style GPUKMeans fill:#7b61ff,color:#fff
```

## Embedding Storage Model

```mermaid
flowchart LR
    subgraph NODE["Node (storage.Node)"]
        Node["Node"]
        Node --> ID["ID: NodeID"]
        Node --> Labels["Labels: []string"]
        Node --> Props["Properties: map[string]any"]
        Node --> NamedEmb["NamedEmbeddings<br/>map[string][]float32"]
        Node --> ChunkEmb["ChunkEmbeddings<br/>[][]float32"]
    end

    subgraph NAMED["NamedEmbeddings (client vectors)"]
        NamedEmb --> TitleEmb["title: [..]"]
        NamedEmb --> ContentEmb["content: [..]"]
        NamedEmb --> DefaultEmb["default: [..] (unnamed)"]
    end

    subgraph CHUNKS["ChunkEmbeddings (managed / chunked docs)"]
        ChunkEmb --> Chunk0["[0] main: [..]"]
        ChunkEmb --> Chunk1["[1] chunk: [..]"]
        ChunkEmb --> ChunkN["[N] chunk: [..]"]
    end

    style NamedEmb fill:#4a90e2,color:#fff
    style ChunkEmb fill:#50c878,color:#fff
```

## Search Priority Order

```mermaid
flowchart TD
    Start["Cypher db.index.vector.queryNodes"] --> VECNAME{"Index has property?"}
    VECNAME -->|Yes| Named["Try NamedEmbeddings[property]"]
    VECNAME -->|No| Default["Try NamedEmbeddings[default]"]

    Named --> PropCheck{"Found?"}
    Default --> PropCheck

    PropCheck -->|No & property set| Prop["Try Properties[property] (vector array)"]
    PropCheck -->|Yes| Compare["Compute similarity (best of candidates)"]
    Prop --> Compare

    Compare --> Chunk["Fallback: ChunkEmbeddings[0..N]"]
    Chunk --> Best["Best score"]
    Best --> Results["Return top K"]

    style Named fill:#4a90e2,color:#fff
    style Prop fill:#ffa500,color:#fff
    style Chunk fill:#50c878,color:#fff
    style Best fill:#ff6b6b,color:#fff
```

## Indexing Flow

```mermaid
flowchart TD
    NodeChanged["Node created/updated"] --> IndexNode["search.Service.IndexNode(node)"]

    IndexNode --> Named["Index NamedEmbeddings"]
    IndexNode --> Chunks["Index ChunkEmbeddings"]
    IndexNode --> Props["Index vector-shaped Properties (Cypher)"]

    Named --> NamedIDs["Add one entry per name:<br/>key(node, named, name)"]
    Chunks --> Main["Add main under ID:<br/>nodeID (ChunkEmbeddings[0])"]
    Chunks --> ChunkIDs["Add one entry per later chunk:<br/>key(node, chunk, i)"]
    Props --> PropIDs["Add one entry per property:<br/>key(node, property, key)"]

    NamedIDs --> VectorIndex["Vector Index"]
    Main --> VectorIndex
    ChunkIDs --> VectorIndex
    PropIDs --> VectorIndex

    style IndexNode fill:#4a90e2,color:#fff
    style VectorIndex fill:#ff6b6b,color:#fff
```

## API Request Flow

```mermaid
sequenceDiagram
    participant Client
    participant API as API Layer
    participant Search as Search Service
    participant VIndex as Vector Index
    participant FIndex as BM25 Index
    participant Storage as Storage Engine

    Client->>API: Search Request
    API->>Search: Search(query, embedding, opts)

    alt Hybrid Search
        Search->>VIndex: Vector Search
        VIndex-->>Search: Vector Results
        Search->>FIndex: BM25 Search
        FIndex-->>Search: BM25 Results
        Search->>Search: RRF Fusion
    else Vector Only
        Search->>VIndex: Vector Search
        VIndex-->>Search: Vector Results
    else BM25 Only
        Search->>FIndex: BM25 Search
        FIndex-->>Search: BM25 Results
    end

    Search->>Storage: Load Node Details
    Storage-->>Search: Nodes with NamedEmbeddings + ChunkEmbeddings

    Search->>Search: Combine Results
    Search-->>API: SearchResponse
    API-->>Client: Results
```

## Collection Architecture

```mermaid
flowchart TB
    subgraph MDB["Database Manager (pkg/multidb)"]
        DM["DatabaseManager"]
        DBDocs["Database: documents<br/>(namespace: documents:)"]
        DBImages["Database: images<br/>(namespace: images:)"]
        DM --> DBDocs
        DM --> DBImages
    end

    subgraph PTS["Points (stored as nodes)"]
        Points["Point nodes<br/>Labels: QdrantPoint, Point<br/>ID: qdrant:point:&lt;id&gt;"]
        P1["Point 1<br/>NamedEmbeddings: title, content"]
        P2["Point 2<br/>NamedEmbeddings: default"]
        Points --> P1
        Points --> P2
    end

    subgraph IDX["Vector indexing"]
        Index["search.Service vector index"]
        IndexCache["qdrantgrpc vectorIndexCache<br/>(per-db dims, per-db index)"]
        IndexCache --> Index
    end

    DBDocs --> MetaDocs["_collection_meta<br/>Label: _CollectionMeta"]
    DBImages --> MetaImages["_collection_meta<br/>Label: _CollectionMeta"]
    DBDocs --> Points
    Points --> IndexCache

    style DM fill:#4a90e2,color:#fff
    style Points fill:#50c878,color:#fff
    style Index fill:#ff6b6b,color:#fff
```

## Hybrid Search (RRF) Flow

```mermaid
flowchart LR
    subgraph IN["Input"]
        Query["Query text"]
        Embedding["Query embedding"]
    end

    subgraph PAR["Parallel search"]
        VectorSearch["Vector search"]
        BM25Search["BM25 search"]
    end

    subgraph OUT["Fusion"]
        RRF["RRF fusion"]
        Final["Final ranking"]
    end

    Query --> BM25Search
    Embedding --> VectorSearch
    VectorSearch --> RRF
    BM25Search --> RRF
    RRF --> Final

    style RRF fill:#ff6b6b,color:#fff
    style Final fill:#50c878,color:#fff
```

## Cypher Vector Query Flow

```mermaid
flowchart TD
    Start["Cypher<br/>CALL db.index.vector.queryNodes"] --> Parse["Parse params<br/>indexName, k, queryInput"]

    Parse --> Resolve{Query Type?}

    Resolve -->|Vector Array| UseVector[Use Vector Directly]
    Resolve -->|String| Embed[Embed String Server-Side]
    Resolve -->|Parameter| ResolveParam[Resolve from Parameters]

    Embed --> UseVector
    ResolveParam --> UseVector

    UseVector --> Delegate["Delegate to search.Service.VectorQueryNodes"]

    Delegate --> Candidates["Generate candidates via vector pipeline<br/>(GPU brute / CPU brute / HNSW / K-means / IVF-HNSW)"]
    Candidates --> Collapse["Collapse vector IDs to node IDs<br/>(chunk/named/prop → nodeID)"]
    Collapse --> Filter["Apply label filter (if any)"]
    Filter --> Priority["Apply embedding precedence per node:<br/>1) NamedEmbeddings[name or default]<br/>2) Properties[name] vector<br/>3) ChunkEmbeddings[0..N]"]
    Priority --> Rank["Score + sort nodes by similarity"]
    Rank --> Return[Return top-K nodes + scores]

    style Candidates fill:#ff6b6b,color:#fff
    style Priority fill:#ffa500,color:#fff
    style Rank fill:#50c878,color:#fff
```

## Complete Embedding Search Coverage

```mermaid
flowchart TB
    subgraph APIS["APIs"]
        API1["HTTP /nornicdb/search"]
        API2["Qdrant gRPC"]
        API3["MCP tools"]
        API4["Cypher queryNodes"]
    end

    subgraph INDEXED["Indexed search (search.Service)"]
        Service["search.Service"]
        Method1["Vector index search"]
        Method2["BM25 search"]
        Method3["VectorQueryNodes (Cypher semantics)"]
        Service --> Method1
        Service --> Method2
        Service --> Method3
    end

    subgraph TYPES["Embedding sources"]
        Type1["NamedEmbeddings"]
        Type2["ChunkEmbeddings"]
        Type3["Properties vectors"]
    end

    API1 --> Service
    API2 --> Service
    API3 --> Service
    API4 --> Service

    Service --> Type1
    Service --> Type2
    Service --> Type3

    style Service fill:#4a90e2,color:#fff
```

## NornicDB vs Pure Solutions

```mermaid
flowchart TB
    subgraph H["NornicDB"]
        Nornic["Graph + vector"]
        Nornic --> G["Graph traversal"]
        Nornic --> V["Vector similarity search"]
        Nornic --> HV["Hybrid ranking (RRF)"]
    end

    subgraph Q["Pure Qdrant"]
        Qdrant["Vector only"]
        Qdrant --> QVector["Vector search"]
        Qdrant --> QNoGraph["No graph traversal"]
    end

    subgraph N["Pure Neo4j"]
        Neo4j["Graph only"]
        Neo4j --> NGraph["Graph traversal"]
        Neo4j --> NNoVector["No native vector DB"]
    end

    style Nornic fill:#50c878,color:#fff
    style Qdrant fill:#ffa500,color:#fff
    style Neo4j fill:#ffa500,color:#fff
```

---

## Key Takeaways

1. All vector search entrypoints route through `search.Service` (including Cypher `db.index.vector.queryNodes`)
2. Qdrant gRPC vectors live in `NamedEmbeddings` only; `ChunkEmbeddings` is reserved for chunked/managed embeddings
3. The vector pipeline auto-selects GPU brute / CPU brute / global HNSW / K-means routing / IVF-HNSW depending on runtime + dataset state
4. Cypher `queryNodes` uses indexed candidate generation + in-memory metadata to preserve embedding precedence (no storage scans in steady-state)
5. Hybrid search combines vector similarity with BM25 keyword matching (RRF), then enriches results from storage
