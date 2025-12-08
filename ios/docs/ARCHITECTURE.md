# NornicDB iOS Architecture

## System Overview

NornicDB for iOS is a fully embedded graph database with AI capabilities, designed to run entirely on-device without any cloud dependencies.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          NornicDB iOS App                                │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │                     PRESENTATION LAYER                              │ │
│  │  ┌────────────┐ ┌────────────┐ ┌────────────┐ ┌────────────┐      │ │
│  │  │  SwiftUI   │ │  Widgets   │ │ App Intents│ │ Spotlight  │      │ │
│  │  │   Views    │ │ WidgetKit  │ │   (Siri)   │ │  Index     │      │ │
│  │  └─────┬──────┘ └─────┬──────┘ └─────┬──────┘ └─────┬──────┘      │ │
│  └────────┼──────────────┼──────────────┼──────────────┼─────────────┘ │
│           │              │              │              │                │
│           └──────────────┴──────────────┴──────────────┘                │
│                                    │                                     │
│  ┌─────────────────────────────────▼──────────────────────────────────┐ │
│  │                      SERVICE LAYER (Swift)                          │ │
│  │  ┌────────────┐ ┌────────────┐ ┌────────────┐ ┌────────────┐      │ │
│  │  │  Database  │ │   Model    │ │   Graph    │ │ Background │      │ │
│  │  │  Manager   │ │  Manager   │ │    RAG     │ │   Tasks    │      │ │
│  │  └─────┬──────┘ └─────┬──────┘ └─────┬──────┘ └─────┬──────┘      │ │
│  └────────┼──────────────┼──────────────┼──────────────┼─────────────┘ │
│           │              │              │              │                │
│           └──────────────┴──────────────┴──────────────┘                │
│                                    │                                     │
│  ┌─────────────────────────────────▼──────────────────────────────────┐ │
│  │                   NornicDB.framework (Go via gomobile)              │ │
│  │  ┌────────────┐ ┌────────────┐ ┌────────────┐ ┌────────────┐      │ │
│  │  │   Cypher   │ │  BadgerDB  │ │   Vector   │ │   Memory   │      │ │
│  │  │   Engine   │ │  Storage   │ │   Search   │ │   Decay    │      │ │
│  │  └─────┬──────┘ └─────┬──────┘ └─────┬──────┘ └─────┬──────┘      │ │
│  └────────┼──────────────┼──────────────┼──────────────┼─────────────┘ │
│           │              │              │              │                │
│           └──────────────┴──────────────┴──────────────┘                │
│                                    │                                     │
│  ┌─────────────────────────────────▼──────────────────────────────────┐ │
│  │                      APPLE FRAMEWORKS                               │ │
│  │  ┌────────────┐ ┌────────────┐ ┌────────────┐ ┌────────────┐      │ │
│  │  │   Metal    │ │  Core ML   │ │ BGTasks    │ │  CloudKit  │      │ │
│  │  │    GPU     │ │   Models   │ │ Scheduler  │ │  (opt.)    │      │ │
│  │  └────────────┘ └────────────┘ └────────────┘ └────────────┘      │ │
│  └────────────────────────────────────────────────────────────────────┘ │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

## Layer Responsibilities

### 1. Presentation Layer

**SwiftUI Views**
- Main app interface
- Graph visualization
- Memory browsing
- Settings & configuration

**WidgetKit**
- Home screen widgets
- Lock screen widgets
- Memory of the day
- Quick capture

**App Intents (Siri)**
- Voice queries
- Shortcuts integration
- Focus mode integration
- Interactive responses

**Spotlight Index**
- System-wide search
- NSUserActivity indexing
- Core Spotlight integration

### 2. Service Layer (Swift)

**DatabaseManager**
```swift
@MainActor
class DatabaseManager: ObservableObject {
    private let db: NornicDBMobile
    
    @Published var isReady: Bool = false
    @Published var nodeCount: Int = 0
    @Published var lastSync: Date?
    
    init() {
        let docsURL = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask)[0]
        let dbPath = docsURL.appendingPathComponent("nornicdb").path
        
        db = NornicDBMobile.open(dbPath)
        isReady = true
    }
    
    func query(_ cypher: String) async throws -> QueryResult {
        return try await withCheckedThrowingContinuation { continuation in
            DispatchQueue.global(qos: .userInitiated).async {
                do {
                    let result = try self.db.query(cypher)
                    continuation.resume(returning: result)
                } catch {
                    continuation.resume(throwing: error)
                }
            }
        }
    }
    
    func vectorSearch(query: String, limit: Int) async throws -> [MemoryNode] {
        let embedding = try await ModelManager.shared.embed(query)
        return try await db.vectorSearch(embedding: embedding, limit: limit)
    }
}
```

**ModelManager**
```swift
actor ModelManager {
    static let shared = ModelManager()
    
    private var embedder: MiniLMEmbedder?
    private var llm: LlamaModel?
    
    func embed(_ text: String) async throws -> [Float] {
        if embedder == nil {
            embedder = try loadEmbedder()
        }
        return try embedder!.predict(text: text)
    }
    
    func generate(prompt: String, maxTokens: Int) async throws -> String {
        guard let llm = llm else {
            throw ModelError.llmNotLoaded
        }
        return try llm.generate(prompt: prompt, maxTokens: maxTokens)
    }
    
    private func loadEmbedder() throws -> MiniLMEmbedder {
        let url = Bundle.main.url(forResource: "embedder", withExtension: "mlmodelc")!
        let config = MLModelConfiguration()
        config.computeUnits = .all
        return try MiniLMEmbedder(contentsOf: url, configuration: config)
    }
}
```

**GraphRAG**
```swift
class GraphRAG {
    private let db: DatabaseManager
    private let model: ModelManager
    
    func answer(question: String) async throws -> RAGResponse {
        // 1. Embed the question
        let queryEmbedding = try await model.embed(question)
        
        // 2. Vector search for relevant memories
        let memories = try await db.vectorSearch(
            embedding: queryEmbedding,
            limit: 10
        )
        
        // 3. Expand context via graph traversal
        let context = try await expandContext(memories, depth: 2)
        
        // 4. Generate response (LLM or template)
        let response = try await generateResponse(
            question: question,
            context: context
        )
        
        return RAGResponse(
            answer: response,
            sources: memories,
            confidence: calculateConfidence(memories)
        )
    }
    
    private func expandContext(_ nodes: [MemoryNode], depth: Int) async throws -> GraphContext {
        var visited = Set<String>()
        var context = GraphContext()
        
        for node in nodes {
            try await traverseRelationships(
                from: node,
                depth: depth,
                visited: &visited,
                context: &context
            )
        }
        
        return context
    }
}
```

**BackgroundTaskManager**
```swift
class BackgroundTaskManager {
    static let shared = BackgroundTaskManager()
    
    private let embeddingTaskID = "com.nornicdb.embedding-refresh"
    private let decayTaskID = "com.nornicdb.decay-update"
    private let compactionTaskID = "com.nornicdb.db-compaction"
    
    func registerTasks() {
        BGTaskScheduler.shared.register(
            forTaskWithIdentifier: embeddingTaskID,
            using: nil
        ) { task in
            self.handleEmbeddingRefresh(task as! BGProcessingTask)
        }
        
        BGTaskScheduler.shared.register(
            forTaskWithIdentifier: decayTaskID,
            using: nil
        ) { task in
            self.handleDecayUpdate(task as! BGAppRefreshTask)
        }
        
        BGTaskScheduler.shared.register(
            forTaskWithIdentifier: compactionTaskID,
            using: nil
        ) { task in
            self.handleCompaction(task as! BGProcessingTask)
        }
    }
    
    func scheduleEmbeddingRefresh() {
        let request = BGProcessingTaskRequest(identifier: embeddingTaskID)
        request.requiresExternalPower = true
        request.requiresNetworkConnectivity = false
        request.earliestBeginDate = Date(timeIntervalSinceNow: 3600)
        
        try? BGTaskScheduler.shared.submit(request)
    }
}
```

### 3. NornicDB Framework (Go)

**Mobile Interface**
```go
// pkg/mobile/mobile.go
package mobile

import (
    "github.com/orneryd/NornicDB/pkg/nornicdb"
    "github.com/orneryd/NornicDB/pkg/storage"
)

// MobileDB is the main interface exposed to iOS
type MobileDB struct {
    db *nornicdb.DB
}

// Open creates a new database at the given path
func Open(path string) (*MobileDB, error) {
    config := &nornicdb.Config{
        DataDir:           path,
        EncryptionEnabled: true,
    }
    
    db, err := nornicdb.Open(config)
    if err != nil {
        return nil, err
    }
    
    return &MobileDB{db: db}, nil
}

// Query executes a Cypher query and returns JSON results
func (m *MobileDB) Query(cypher string) (string, error) {
    result, err := m.db.Execute(cypher, nil)
    if err != nil {
        return "", err
    }
    return result.ToJSON()
}

// VectorSearch performs semantic search
func (m *MobileDB) VectorSearch(embedding []float32, limit int) (string, error) {
    results, err := m.db.VectorSearch(embedding, limit)
    if err != nil {
        return "", err
    }
    return results.ToJSON()
}

// Store creates a new node
func (m *MobileDB) Store(nodeType, content string, tags []string) (string, error) {
    node := &storage.Node{
        Labels:     []string{nodeType},
        Properties: map[string]interface{}{
            "content":   content,
            "tags":      tags,
            "timestamp": time.Now().Unix(),
        },
    }
    
    id, err := m.db.CreateNode(node)
    if err != nil {
        return "", err
    }
    return id, nil
}

// Close releases database resources
func (m *MobileDB) Close() error {
    return m.db.Close()
}
```

**Building for iOS**
```bash
# Install gomobile
go install golang.org/x/mobile/cmd/gomobile@latest
gomobile init

# Build iOS framework
gomobile bind -target=ios \
    -o NornicDB.xcframework \
    -iosversion=17 \
    github.com/orneryd/NornicDB/pkg/mobile

# Output: NornicDB.xcframework (~50MB)
```

### 4. Apple Frameworks Integration

**Metal GPU**
- Reuse existing `pkg/gpu/metal/` code
- K-means clustering acceleration
- Vector similarity computation
- Shared shaders with macOS

**Core ML**
- Embedding model inference
- Neural Engine optimization
- Batch processing support

**BGTasks**
- Background processing scheduler
- Charging-aware scheduling
- Task prioritization

**CloudKit (Optional)**
- Cross-device sync
- Backup/restore
- Share graphs

## Data Model

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Graph Schema                                 │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌──────────────┐         RELATES_TO          ┌──────────────┐     │
│  │   Memory     │◄─────────────────────────────│   Memory     │     │
│  │              │                              │              │     │
│  │ - id         │         TAGGED_WITH          │ - id         │     │
│  │ - content    │────────────────────────────► │ - content    │     │
│  │ - embedding  │                              │ - embedding  │     │
│  │ - timestamp  │         MENTIONS             │ - timestamp  │     │
│  │ - decay      │────────────────────────────► │ - decay      │     │
│  │ - source     │                              │ - source     │     │
│  └──────────────┘                              └──────────────┘     │
│         │                                              │            │
│         │ FROM_CONTEXT                    ABOUT_PERSON │            │
│         ▼                                              ▼            │
│  ┌──────────────┐                              ┌──────────────┐     │
│  │   Context    │                              │    Person    │     │
│  │              │                              │              │     │
│  │ - type       │                              │ - name       │     │
│  │ - timestamp  │                              │ - contact_id │     │
│  │ - location   │                              │ - photo      │     │
│  │ - app        │                              │              │     │
│  └──────────────┘                              └──────────────┘     │
│         │                                              │            │
│         │ DURING_FOCUS                      WORKS_AT   │            │
│         ▼                                              ▼            │
│  ┌──────────────┐                              ┌──────────────┐     │
│  │    Focus     │                              │ Organization │     │
│  │              │                              │              │     │
│  │ - mode       │                              │ - name       │     │
│  │ - active     │                              │ - domain     │     │
│  └──────────────┘                              └──────────────┘     │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

## File System Layout

```
~/Documents/NornicDB/
├── database/
│   ├── 000001.vlog          # BadgerDB value log
│   ├── 000002.sst           # BadgerDB SST files
│   ├── MANIFEST             # BadgerDB manifest
│   ├── db.salt              # Encryption salt
│   └── embeddings.idx       # Vector index
├── exports/
│   └── backup_2024_01_15.json
└── config.yaml

~/Library/Caches/NornicDB/
├── models/
│   └── phi-3-mini-q4.gguf   # Optional LLM (downloaded)
└── temp/
    └── processing/          # Temp files during operations

App Bundle/
├── NornicDB.framework/      # Go framework
├── Models/
│   ├── embedder.mlmodelc/   # Core ML embedding model
│   └── tokenizer.json       # Tokenizer
└── Resources/
    └── default_schema.cypher # Initial graph schema
```

## Security Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                      Security Layers                                 │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │  iOS Data Protection (Hardware Encryption)                   │   │
│  │  - Files encrypted at rest with device key                   │   │
│  │  - Decrypted only when device unlocked                      │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                              │                                       │
│                              ▼                                       │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │  App Sandbox                                                 │   │
│  │  - Isolated container                                        │   │
│  │  - No access to other apps' data                            │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                              │                                       │
│                              ▼                                       │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │  BadgerDB Encryption                                         │   │
│  │  - AES-256 encryption at database level                     │   │
│  │  - Key derived from user password + device key              │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                              │                                       │
│                              ▼                                       │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │  Keychain Storage                                            │   │
│  │  - Encryption keys in Secure Enclave                        │   │
│  │  - Biometric protection (Face ID / Touch ID)                │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

## Performance Targets

| Operation | Target | Measurement |
|-----------|--------|-------------|
| App launch | <1s | Cold start to ready |
| Single query | <50ms | Simple MATCH |
| Vector search | <100ms | Top 10 results |
| Embedding | <20ms | Per text (Neural Engine) |
| Graph traversal | <200ms | 3-hop expansion |
| Memory usage | <150MB | Active operation |
| Database size | <500MB | 10,000 memories |

## Next Steps

1. [Apple Intelligence Integration →](APPLE_INTELLIGENCE.md)
2. [Metal ML Implementation →](METAL_ML.md)
3. [Background Processing →](BACKGROUND_PROCESSING.md)
4. [Model Requirements →](MODELS.md)
