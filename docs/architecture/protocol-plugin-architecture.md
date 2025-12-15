# NornicDB Protocol Plugin Architecture Proposal

**Author:** AI Assistant  
**Date:** December 2024  
**Status:** Draft  
**Version:** 1.0

---

## Executive Summary

This proposal outlines a plugin architecture that would allow NornicDB to support multiple database protocols and query languages, enabling users to connect using their existing drivers (Neo4j, MongoDB, PostgreSQL, etc.) while all operations execute against the same underlying NornicDB storage engine.

**Key Benefits:**
- Use existing MongoDB drivers to connect to NornicDB
- MongoDB-style queries with built-in vector search and embeddings
- No vendor lock-in - choose your preferred query language
- Single database, multiple access patterns
- Community-extensible protocol support

---

## Current Architecture

NornicDB currently supports two protocols:

```
┌─────────────────────────────────────────────────────────────┐
│                      Client Applications                     │
│         (Neo4j Browser, Cypher Shell, Custom Apps)          │
└─────────────────────────────────────────────────────────────┘
                    │                    │
                    ▼                    ▼
         ┌──────────────────┐  ┌──────────────────┐
         │   HTTP Server    │  │   Bolt Server    │
         │  (pkg/server)    │  │   (pkg/bolt)     │
         │  Port: 7474      │  │   Port: 7687     │
         └────────┬─────────┘  └────────┬─────────┘
                  │                     │
                  ▼                     ▼
         ┌─────────────────────────────────────────┐
         │           Cypher Executor               │
         │            (pkg/cypher)                 │
         │  - Query parsing                        │
         │  - Execution planning                   │
         │  - Result formatting                    │
         └────────────────┬────────────────────────┘
                          │
                          ▼
         ┌─────────────────────────────────────────┐
         │          Storage Engine                 │
         │           (pkg/storage)                 │
         │  - Node/Edge CRUD                       │
         │  - Indexing                             │
         │  - Transactions                         │
         └─────────────────────────────────────────┘
```

### Key Interfaces

**QueryExecutor** (pkg/bolt/server.go):
```go
type QueryExecutor interface {
    Execute(ctx context.Context, query string, params map[string]any) (*QueryResult, error)
}
```

**Storage Engine** (pkg/storage):
```go
type Engine interface {
    CreateNode(node *Node) error
    GetNode(id NodeID) (*Node, error)
    UpdateNode(node *Node) error
    DeleteNode(id NodeID) error
    CreateEdge(edge *Edge) error
    // ... more methods
}
```

---

## Proposed Plugin Architecture

### High-Level Design

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         Client Applications                              │
│  (Neo4j Browser, MongoDB Compass, psql, Custom Apps, GraphQL Clients)   │
└─────────────────────────────────────────────────────────────────────────┘
          │              │              │              │
          ▼              ▼              ▼              ▼
┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│ Bolt Plugin │  │ MongoDB     │  │ PostgreSQL  │  │ GraphQL     │
│ (built-in)  │  │ Wire Plugin │  │ Wire Plugin │  │ Plugin      │
│ Port: 7687  │  │ Port: 27017 │  │ Port: 5432  │  │ Port: 4000  │
└──────┬──────┘  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘
       │                │                │                │
       ▼                ▼                ▼                ▼
┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│ Cypher      │  │ MQL         │  │ SQL         │  │ GraphQL     │
│ Parser      │  │ Translator  │  │ Translator  │  │ Resolver    │
│ (built-in)  │  │ (plugin)    │  │ (plugin)    │  │ (plugin)    │
└──────┬──────┘  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘
       │                │                │                │
       └────────────────┴────────────────┴────────────────┘
                                │
                                ▼
              ┌─────────────────────────────────────┐
              │     NornicDB AST (pkg/cypher/ast)   │  ← ALL PARSERS TARGET THIS
              │  - ASTClause (Match, Create, etc.)  │
              │  - ASTPattern (Nodes, Relationships)│
              │  - ASTExpression (rich expr tree)   │
              └────────────────┬────────────────────┘
                               │
                               ▼
              ┌─────────────────────────────────────┐
              │     QueryCache (pkg/cache)          │  ← EXISTING INFRASTRUCTURE
              │  - LRU eviction                     │
              │  - TTL expiration                   │
              │  - Thread-safe                      │
              └────────────────┬────────────────────┘
                               │
                               ▼
              ┌─────────────────────────────────────┐
              │     StorageExecutor (pkg/cypher)    │  ← EXISTING EXECUTOR
              │  - AST interpretation               │
              │  - Query optimization               │
              │  - Transaction coordination         │
              └────────────────┬────────────────────┘
                               │
                               ▼
              ┌─────────────────────────────────────┐
              │        Storage Engine               │
              │  - Graph storage (nodes/edges)      │
              │  - Vector embeddings                │
              │  - Full-text search                 │
              └─────────────────────────────────────┘
```

**Key Insight:** All query language plugins translate to the **same existing AST**.
No new intermediate representation needed - we leverage what's already built.

### Core Plugin Interfaces

```go
// pkg/plugin/protocol.go

// ProtocolPlugin defines a wire protocol handler (Bolt, MongoDB Wire, PostgreSQL Wire, etc.)
type ProtocolPlugin interface {
    // Metadata
    Name() string
    Version() string
    Description() string
    
    // Lifecycle
    Init(config PluginConfig) error
    Start(executor QueryExecutor) error
    Stop(ctx context.Context) error
    
    // Configuration
    DefaultPort() int
    ConfigSchema() map[string]ConfigField
}

// QueryLanguagePlugin defines a query language handler.
// Plugins can choose their implementation strategy (see "Plugin Implementation Strategies" below).
type QueryLanguagePlugin interface {
    // Metadata
    Name() string        // "cypher", "mql", "sql", "graphql"
    Version() string
    
    // Strategy returns which implementation approach this plugin uses
    Strategy() PluginStrategy
    
    // Capabilities
    Capabilities() QueryCapabilities
}

// PluginStrategy indicates how the plugin processes queries
type PluginStrategy int

const (
    // StrategyAST - Plugin produces AST, NornicDB executes it
    // Best for: New languages, clean separation, leverages existing optimizer
    StrategyAST PluginStrategy = iota
    
    // StrategyDirect - Plugin handles everything: parse + execute
    // Best for: Maximum control, custom optimizations, streaming execution
    StrategyDirect
)

// ASTProducerPlugin - Strategy A/B: Produce AST for NornicDB to execute
type ASTProducerPlugin interface {
    QueryLanguagePlugin
    
    // Parse produces NornicDB's AST structure
    // Can use ANTLR, hand-rolled parser, or delegate to existing parser
    Parse(query string) (*cypher.AST, error)
}

// DirectExecutorPlugin - Strategy C: Handle everything directly
// Like NornicDB's internal Cypher executor - full control over parse/execute flow
type DirectExecutorPlugin interface {
    QueryLanguagePlugin
    
    // Execute handles the entire query lifecycle
    // Plugin maintains its own parsing, routing, caching, optimization
    Execute(ctx context.Context, query string, params map[string]any, storage storage.Engine) (*ExecuteResult, error)
}
```

---

## Plugin Implementation Strategies

Plugins can choose their parsing/execution approach based on their needs:

### Strategy A: ANTLR-Style Parser → AST

Traditional compiler approach using grammar-based parsing.

> **Existing Work:** [Mimir PR #18](https://github.com/orneryd/Mimir/pull/18) contains an ANTLR-based
> Cypher parser that was not merged due to performance overhead (~5-10ms vs inline's ~0.5ms).
> This could be extracted as an **optional plugin** for users who prefer:
> - Strict grammar validation
> - Better error messages
> - Easier grammar extensions
> - Standard tooling (ANTLR ecosystem)

```go
// pkg/plugins/antlr-cypher/plugin.go
// Based on: https://github.com/orneryd/Mimir/pull/18

type ANTLRCypherPlugin struct {
    // ANTLR-generated parser from PR #18
    lexer  *parser.CypherLexer
    parser *parser.CypherParser
}

func (p *ANTLRCypherPlugin) Name() string { return "antlr-cypher" }
func (p *ANTLRCypherPlugin) Strategy() PluginStrategy { return StrategyAST }

func (p *ANTLRCypherPlugin) Parse(query string) (*cypher.AST, error) {
    // 1. Lex into tokens (from PR #18's generated lexer)
    input := antlr.NewInputStream(query)
    lexer := parser.NewCypherLexer(input)
    tokens := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
    
    // 2. Parse into ANTLR Cypher AST
    cypherParser := parser.NewCypherParser(tokens)
    tree := cypherParser.OC_Cypher()  // Root rule from OpenCypher grammar
    
    // 3. Transform ANTLR AST → NornicDB AST
    visitor := &NornicASTVisitor{}
    return visitor.Visit(tree).(*cypher.AST), nil
}

// Configuration in nornicdb.yaml:
// plugins:
//   cypher:
//     parser: antlr  # Use ANTLR plugin instead of built-in inline parser
//     # parser: inline  # Default - fast inline parser
```

**Why Extract PR #18 as a Plugin:**

| Aspect | Built-in Inline Parser | ANTLR Plugin (PR #18) |
|--------|------------------------|----------------------|
| Speed | ~0.5ms | ~5-10ms |
| Grammar Validation | Loose | Strict OpenCypher |
| Error Messages | Basic | Detailed with line/col |
| Extensibility | Code changes | Grammar file changes |
| Use Case | Production (speed) | Development (validation) |

Users could switch parsers via config:
```yaml
# Development - strict validation, better errors
plugins:
  cypher:
    parser: antlr

# Production - maximum speed
plugins:
  cypher:
    parser: inline  # default
```

```go
// Example: SQL Plugin using ANTLR-generated parser
type SQLPlugin struct {
    lexer  *antlr.SQLLexer
    parser *antlr.SQLParser
}

func (p *SQLPlugin) Strategy() PluginStrategy { return StrategyAST }

func (p *SQLPlugin) Parse(query string) (*cypher.AST, error) {
    // 1. Lex into tokens
    input := antlr.NewInputStream(query)
    lexer := parser.NewSQLLexer(input)
    tokens := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
    
    // 2. Parse into SQL AST
    sqlParser := parser.NewSQLParser(tokens)
    sqlTree := sqlParser.Statement()
    
    // 3. Transform SQL AST → NornicDB AST
    return p.transformToNornicAST(sqlTree)
}
```

**Pros:** Clean grammar, auto-generated parser, good error messages, standard tooling
**Cons:** Slower than hand-rolled (~5-10ms overhead), grammar maintenance

### Strategy B: Reuse/Delegate to Existing Parser

For languages similar to Cypher, or when a fast parser already exists.

```go
// Example: OpenCypher variant that extends NornicDB's parser
type OpenCypherPlugin struct {
    baseBuilder *cypher.ASTBuilder  // Reuse NornicDB's parser
}

func (p *OpenCypherPlugin) Strategy() PluginStrategy { return StrategyAST }

func (p *OpenCypherPlugin) Parse(query string) (*cypher.AST, error) {
    // Preprocess: Handle OpenCypher-specific syntax
    normalized := p.preprocessOpenCypher(query)
    
    // Delegate to NornicDB's fast inline parser
    ast, err := p.baseBuilder.Build(normalized)
    if err != nil {
        return nil, err
    }
    
    // Post-process: Add OpenCypher extensions
    return p.addExtensions(ast)
}

// Pros: Fast (reuses optimized parser), minimal code
// Cons: Limited to languages similar to Cypher
```

### Strategy C: Custom Stream-Parse-Execute (Like NornicDB)

Full control - plugin handles everything. This is how NornicDB's Cypher executor works internally.

```go
// Example: MongoDB plugin with custom inline executor
type MongoDBPlugin struct {
    storage storage.Engine
    cache   *QueryCache  // Plugin's own cache
}

func (p *MongoDBPlugin) Strategy() PluginStrategy { return StrategyDirect }

func (p *MongoDBPlugin) Execute(ctx context.Context, query string, params map[string]any, storage storage.Engine) (*ExecuteResult, error) {
    // Plugin handles EVERYTHING - just like NornicDB's Cypher executor
    
    // 1. Check plugin's own cache
    if cached, ok := p.cache.Get(query, params); ok {
        return cached, nil
    }
    
    // 2. Parse BSON command (fast inline parsing)
    cmd, err := p.parseBSONCommand(query)
    if err != nil {
        return nil, err
    }
    
    // 3. Route to appropriate handler
    var result *ExecuteResult
    switch cmd.Type {
    case "find":
        result, err = p.executeFind(ctx, cmd, storage)
    case "insert":
        result, err = p.executeInsert(ctx, cmd, storage)
    case "aggregate":
        result, err = p.executeAggregate(ctx, cmd, storage)
    // ... more handlers
    }
    
    // 4. Cache result
    if err == nil && cmd.IsCacheable() {
        p.cache.Put(query, params, result, 5*time.Minute)
    }
    
    return result, err
}

// Pros: Maximum performance, custom optimizations, streaming support
// Cons: More code to maintain, must handle caching/routing/optimization
```

### Strategy Comparison

| Aspect | A: ANTLR → AST | B: Reuse Parser | C: Direct Execute |
|--------|----------------|-----------------|-------------------|
| **Parse Speed** | ~5-10ms | ~1ms | ~0.5ms (inline) |
| **Code Size** | Medium (grammar + transform) | Small | Large (full executor) |
| **Flexibility** | High | Limited | Maximum |
| **Optimization** | Uses NornicDB's | Uses NornicDB's | Custom |
| **Caching** | NornicDB's QueryCache | NornicDB's QueryCache | Plugin's own |
| **Best For** | SQL, GraphQL | Cypher variants | MongoDB, Redis |

### Hybrid Approach

Plugins can even mix strategies - use AST for simple queries, direct execution for complex ones:

```go
func (p *HybridPlugin) Execute(ctx context.Context, query string, params map[string]any, storage storage.Engine) (*ExecuteResult, error) {
    // Simple queries → produce AST, let NornicDB execute
    if p.isSimpleQuery(query) {
        ast, err := p.Parse(query)
        if err != nil {
            return nil, err
        }
        return p.executor.ExecuteAST(ctx, ast, params)
    }
    
    // Complex queries → handle directly for custom optimization
    return p.executeComplex(ctx, query, params, storage)
}
```

### Leveraging the Existing AST

NornicDB already has a comprehensive AST in `pkg/cypher/ast_builder.go`:

```go
// EXISTING - pkg/cypher/ast_builder.go

// AST represents a complete parsed query - THIS IS OUR TARGET
type AST struct {
    Clauses    []ASTClause
    RawQuery   string
    QueryType  QueryType
    IsReadOnly bool
    IsCompound bool
}

// ASTClause represents a parsed clause
type ASTClause struct {
    Type     ASTClauseType  // Match, Create, Merge, Delete, Set, Return, etc.
    RawText  string
    StartPos int
    EndPos   int
    
    // Clause-specific parsed content
    Match   *ASTMatch
    Create  *ASTCreate
    Merge   *ASTMerge
    Delete  *ASTDelete
    Set     *ASTSet
    Remove  *ASTRemove
    Return  *ASTReturn
    With    *ASTWith
    Where   *ASTWhere
    Unwind  *ASTUnwind
    OrderBy *ASTOrderBy
    Limit   *int64
    Skip    *int64
    Call    *ASTCall
}

// ASTPattern represents a graph pattern (nodes + relationships)
type ASTPattern struct {
    Nodes         []ASTNode
    Relationships []ASTRelationship
    RawText       string
}

// ASTNode represents a node in a pattern
type ASTNode struct {
    Variable   string
    Labels     []string
    Properties map[string]ASTExpression
}

// ASTExpression - rich expression tree
type ASTExpression struct {
    Type      ASTExprType  // Literal, Variable, Property, Function, Binary, etc.
    RawText   string
    Literal   interface{}
    Variable  string
    Property  *ASTPropertyAccess
    Function  *ASTFunctionCall
    Binary    *ASTBinaryExpr
    // ... more fields
}
```

**Key Insight:** Query language plugins don't need a new intermediate representation.
They translate directly to NornicDB's existing AST, which is then:
1. Cached via `pkg/cache/query_cache.go` (LRU + TTL)
2. Executed by the existing `StorageExecutor`

---

## MongoDB Protocol Plugin Design

### Wire Protocol Implementation

MongoDB uses a binary wire protocol over TCP. The plugin would implement:

```go
// pkg/plugins/mongodb/server.go

type MongoDBPlugin struct {
    config   *Config
    listener net.Listener
    executor QueryExecutor
}

func (p *MongoDBPlugin) Name() string { return "mongodb" }
func (p *MongoDBPlugin) DefaultPort() int { return 27017 }

func (p *MongoDBPlugin) Start(executor QueryExecutor) error {
    p.executor = executor
    listener, err := net.Listen("tcp", fmt.Sprintf(":%d", p.config.Port))
    if err != nil {
        return err
    }
    p.listener = listener
    
    go p.acceptConnections()
    return nil
}

func (p *MongoDBPlugin) handleMessage(conn net.Conn, msg *WireMessage) error {
    switch msg.OpCode {
    case OP_QUERY:
        return p.handleQuery(conn, msg)
    case OP_INSERT:
        return p.handleInsert(conn, msg)
    case OP_UPDATE:
        return p.handleUpdate(conn, msg)
    case OP_DELETE:
        return p.handleDelete(conn, msg)
    case OP_MSG:
        return p.handleOpMsg(conn, msg) // Modern protocol
    }
    return nil
}
```

### MQL to AST Translation

The key insight: **MongoDB documents map naturally to graph nodes**.

```
MongoDB Concept     →  NornicDB AST
─────────────────────────────────────
Collection          →  Label (ASTNode.Labels)
Document            →  Node (ASTNode)
_id field           →  Node ID
Document fields     →  Properties (ASTNode.Properties)
$lookup             →  Relationship traversal (ASTRelationship)
$match              →  ASTWhere clause
$project            →  ASTReturn clause
$sort               →  ASTOrderBy clause
$limit              →  AST.Limit
$skip               →  AST.Skip
```

```go
// pkg/plugins/mongodb/translator.go

type MQLTranslator struct {
    builder *cypher.ASTBuilder
}

// find() → AST with MATCH clause
func (t *MQLTranslator) TranslateFind(collection string, filter bson.M, projection bson.M) (*cypher.AST, error) {
    // db.users.find({age: {$gt: 25}})
    // → AST equivalent to: MATCH (n:users) WHERE n.age > 25 RETURN n
    
    ast := &cypher.AST{
        QueryType:  cypher.QueryMatch,
        IsReadOnly: true,
        Clauses: []cypher.ASTClause{
            {
                Type: cypher.ASTClauseMatch,
                Match: &cypher.ASTMatch{
                    Patterns: []cypher.ASTPattern{{
                        Nodes: []cypher.ASTNode{{
                            Variable: "n",
                            Labels:   []string{collection},
                        }},
                    }},
                },
            },
            {
                Type: cypher.ASTClauseWhere,
                Where: t.translateFilter("n", filter),
            },
            {
                Type: cypher.ASTClauseReturn,
                Return: t.translateProjection("n", projection),
            },
        },
    }
    return ast, nil
}

// insertOne() → AST with CREATE clause
func (t *MQLTranslator) TranslateInsert(collection string, doc bson.M) (*cypher.AST, error) {
    // db.users.insertOne({name: "Alice", age: 30})
    // → AST equivalent to: CREATE (n:users {name: "Alice", age: 30}) RETURN n
    
    return &cypher.AST{
        QueryType:  cypher.QueryCreate,
        IsReadOnly: false,
        Clauses: []cypher.ASTClause{
            {
                Type: cypher.ASTClauseCreate,
                Create: &cypher.ASTCreate{
                    Patterns: []cypher.ASTPattern{{
                        Nodes: []cypher.ASTNode{{
                            Variable:   "n",
                            Labels:     []string{collection},
                            Properties: t.bsonToASTProps(doc),
                        }},
                    }},
                },
            },
        },
    }, nil
}

// aggregate() with $lookup → Graph traversal AST
func (t *MQLTranslator) TranslateAggregate(collection string, pipeline []bson.M) (*cypher.AST, error) {
    // db.orders.aggregate([
    //   {$lookup: {from: "users", localField: "userId", foreignField: "_id", as: "user"}}
    // ])
    // → MATCH (o:orders)-[:BELONGS_TO]->(u:users) RETURN o, u
    
    // This is where NornicDB shines - $lookup becomes native graph traversal!
    return t.translatePipeline(collection, pipeline)
}
```

### Vector Search Integration

MongoDB Atlas has vector search, but NornicDB's is built-in and more powerful:

```go
// MongoDB-style vector search syntax
// db.documents.aggregate([
//   {$vectorSearch: {
//     queryVector: [0.1, 0.2, ...],
//     path: "embedding",
//     numCandidates: 100,
//     limit: 10
//   }}
// ])

func (t *MQLTranslator) TranslateVectorSearch(collection string, spec bson.M) (*cypher.AST, error) {
    queryVector := spec["queryVector"].([]float32)
    field := spec["path"].(string)
    limit := int64(spec["limit"].(int))
    
    // Translate to AST with CALL clause for vector search procedure
    // Equivalent to: CALL db.index.vector.queryNodes('collection_embedding', 10, $vector) 
    //                YIELD node, score RETURN node, score LIMIT 10
    return &cypher.AST{
        QueryType:  cypher.QueryMatch,
        IsReadOnly: true,
        Clauses: []cypher.ASTClause{
            {
                Type: cypher.ASTClauseCall,
                Call: &cypher.ASTCall{
                    Procedure: "db.index.vector.queryNodes",
                    RawArgs:   fmt.Sprintf("'%s_%s', %d, $vector", collection, field, limit),
                    Yield:     []string{"node", "score"},
                },
            },
            {
                Type:  cypher.ASTClauseLimit,
                Limit: &limit,
            },
        },
    }, nil
}
```

### Example: MongoDB Driver Usage

```javascript
// Existing MongoDB driver code works unchanged!
const { MongoClient } = require('mongodb');

// Connect to NornicDB's MongoDB-compatible endpoint
const client = new MongoClient('mongodb://localhost:27017');
await client.connect();

const db = client.db('myapp');
const users = db.collection('users');

// Standard MongoDB operations
await users.insertOne({ name: 'Alice', age: 30 });
const user = await users.findOne({ name: 'Alice' });

// Vector search (NornicDB extension)
const similar = await users.aggregate([
  {
    $vectorSearch: {
      queryVector: await embed("Find users interested in AI"),
      path: "interests_embedding",
      numCandidates: 100,
      limit: 10
    }
  }
]).toArray();

// Graph-style queries via $lookup (native graph traversal!)
const ordersWithUsers = await db.collection('orders').aggregate([
  {
    $lookup: {
      from: 'users',
      localField: 'userId',
      foreignField: '_id',
      as: 'customer'
    }
  }
]).toArray();
```

---

## Plugin Loading Architecture

### Plugin Discovery

```go
// pkg/plugin/loader.go

type PluginLoader struct {
    pluginDir string
    registry  *PluginRegistry
}

// Load plugins from:
// 1. Built-in plugins (Bolt, HTTP)
// 2. Go plugins (.so files)
// 3. External processes (gRPC)

func (l *PluginLoader) LoadAll() error {
    // Built-in plugins
    l.registry.Register(&bolt.BoltPlugin{})
    l.registry.Register(&http.HTTPPlugin{})
    
    // Dynamic plugins from plugin directory
    files, _ := filepath.Glob(filepath.Join(l.pluginDir, "*.so"))
    for _, file := range files {
        plugin, err := l.loadGoPlugin(file)
        if err != nil {
            log.Warn("Failed to load plugin", "file", file, "error", err)
            continue
        }
        l.registry.Register(plugin)
    }
    
    return nil
}

func (l *PluginLoader) loadGoPlugin(path string) (ProtocolPlugin, error) {
    p, err := plugin.Open(path)
    if err != nil {
        return nil, err
    }
    
    sym, err := p.Lookup("Plugin")
    if err != nil {
        return nil, err
    }
    
    return sym.(ProtocolPlugin), nil
}
```

### Configuration

```yaml
# nornicdb.yaml
plugins:
  bolt:
    enabled: true
    port: 7687
    
  mongodb:
    enabled: true
    port: 27017
    # Map MongoDB databases to NornicDB namespaces
    database_mapping:
      myapp: default
      analytics: analytics_ns
    
  postgresql:
    enabled: false
    port: 5432
    
  graphql:
    enabled: true
    port: 4000
    schema_file: ./schema.graphql
```

---

## Implementation Phases

### Phase 1: Plugin Infrastructure 
- [ ] Define `ProtocolPlugin` and `QueryLanguagePlugin` interfaces
- [ ] Implement plugin loader (Go plugins + gRPC for external)
- [ ] Refactor Bolt server (`pkg/bolt`) as a plugin
- [ ] Refactor HTTP server (`pkg/server`) as a plugin
- [ ] Plugin configuration via `nornicdb.yaml`
- [ ] Plugin lifecycle management (init, start, stop, health)

### Phase 2: AST Formalization 
- [ ] Export existing AST types from `pkg/cypher/ast_builder.go` as public API
- [ ] Document AST structure for plugin authors
- [ ] Create AST validation helpers
- [ ] Add AST → Cypher string serialization (for debugging/logging)
- [ ] Ensure QueryCache works with plugin-generated ASTs

### Phase 3: MongoDB Protocol
- [ ] MongoDB Wire Protocol implementation (OP_MSG, OP_QUERY)
- [ ] MQL → AST translator for CRUD operations
  - [ ] `find()` → `ASTClauseMatch` + `ASTClauseReturn`
  - [ ] `insertOne/Many()` → `ASTClauseCreate`
  - [ ] `updateOne/Many()` → `ASTClauseMatch` + `ASTClauseSet`
  - [ ] `deleteOne/Many()` → `ASTClauseMatch` + `ASTClauseDelete`
- [ ] Aggregation pipeline → AST translation
  - [ ] `$match` → `ASTClauseWhere`
  - [ ] `$project` → `ASTClauseReturn`
  - [ ] `$lookup` → `ASTRelationship` traversal (major optimization!)
  - [ ] `$group` → Aggregation functions
- [ ] `$vectorSearch` → `ASTClauseCall` with vector procedures
- [ ] MongoDB driver compatibility tests (Node.js, Python, Go)

### Phase 4: Additional Protocols
- [ ] PostgreSQL Wire Protocol (SQL → AST)
- [ ] GraphQL endpoint (schema-first, resolvers → AST)
- [ ] Redis Protocol (for key-value access patterns)
- [ ] gRPC API (for microservices)

---

## Technical Considerations

### Performance

1. **Query Translation Overhead**: MQL → AST → Execution adds ~1-5ms latency
   - Mitigation: Existing `QueryCache` handles AST caching (LRU + TTL)
   - Same query string = cache hit, no re-translation
   
2. **Wire Protocol Parsing**: Binary protocols are efficient
   - MongoDB Wire Protocol is already optimized for performance
   - BSON parsing is fast and well-documented
   
3. **Graph vs Document Model**: Some queries may be slower
   - Mitigation: `$lookup` becomes **faster** as native graph traversal
   - Simple CRUD is equivalent performance

### AST Considerations

1. **AST Completeness**: Current AST covers Cypher well, may need extensions for:
   - MongoDB-specific aggregation stages (`$bucket`, `$facet`)
   - SQL-specific constructs (JOINs already map to patterns)
   - Solution: Add `ASTClauseExtension` for plugin-specific nodes

2. **AST Stability**: Plugins depend on AST structure
   - Semantic versioning for AST changes
   - Deprecation warnings before breaking changes

3. **AST Validation**: Plugins may generate invalid ASTs
   - `ValidateAST()` function to catch errors early
   - Clear error messages for plugin developers

### Compatibility

1. **MongoDB Features Not Supported**:
   - Sharding (NornicDB has its own replication)
   - Capped collections (use retention policies instead)
   - GridFS (use blob storage extension)
   - Change streams (use NornicDB's event system)
   
2. **NornicDB Features Available via MongoDB**:
   - Vector search (via `$vectorSearch`)
   - Graph traversal (via optimized `$lookup`)
   - Memory decay (via TTL indexes)
   - Full-text search (via `$text`)
   - Auto-embeddings (transparent on insert)

### Security

- Each protocol plugin handles its own authentication
- Unified authorization layer at storage level
- Audit logging across all protocols
- Plugin sandboxing for untrusted plugins

---

## Comparison: MongoDB Atlas vs NornicDB

| Feature | MongoDB Atlas | NornicDB + MongoDB Plugin |
|---------|---------------|---------------------------|
| Document Storage | ✅ Native | ✅ Via graph nodes |
| Vector Search | ✅ Atlas Vector Search | ✅ Built-in, faster |
| Graph Queries | ❌ $graphLookup (limited) | ✅ Native graph engine |
| $lookup Performance | ⚠️ Slow (joins) | ✅ Fast (graph traversal) |
| Auto-embeddings | ❌ External | ✅ Built-in |
| Memory Decay | ❌ No | ✅ Native |
| Self-hosted | ⚠️ Enterprise only | ✅ Always |
| Protocol Flexibility | ❌ MongoDB only | ✅ Multi-protocol |

---

## Conclusion

The plugin architecture would position NornicDB as a **universal database adapter** - users can connect with their preferred tools and query languages while benefiting from NornicDB's graph-native storage, vector search, and memory features.

The MongoDB plugin specifically addresses a large market:
- Millions of MongoDB developers
- Existing applications that could benefit from graph + vector capabilities
- Teams wanting to migrate from MongoDB without rewriting code

**Recommended Next Steps:**
1. Review and approve this proposal
2. Create detailed technical design for plugin interfaces
3. Prototype MongoDB Wire Protocol handler
4. Benchmark MQL translation overhead
5. Community feedback on priority of additional protocols

---

## Appendix A: MongoDB Wire Protocol Reference

```
Message Header (16 bytes):
┌────────────┬────────────┬────────────┬────────────┐
│ messageLen │ requestID  │ responseTo │  opCode    │
│  (int32)   │  (int32)   │  (int32)   │  (int32)   │
└────────────┴────────────┴────────────┴────────────┘

OP_MSG (opCode 2013) - Modern protocol:
┌────────────┬────────────┬────────────────────────┐
│ flagBits   │ sections[] │ checksum (optional)    │
│  (uint32)  │  (varies)  │  (uint32)              │
└────────────┴────────────┴────────────────────────┘
```

## Appendix B: Query Translation Examples

```
MongoDB                          → NornicDB Cypher
─────────────────────────────────────────────────────────────
db.users.find({age: 25})         → MATCH (n:users {age: 25}) RETURN n

db.users.find({age: {$gt: 25}})  → MATCH (n:users) WHERE n.age > 25 RETURN n

db.users.insertOne({             → CREATE (n:users {name: "Alice", age: 30})
  name: "Alice", age: 30
})

db.orders.aggregate([            → MATCH (o:orders)-[:BELONGS_TO]->(u:users)
  {$lookup: {                       RETURN o, u
    from: "users",
    localField: "userId",
    foreignField: "_id",
    as: "user"
  }}
])

db.docs.aggregate([              → CALL db.index.vector.queryNodes(
  {$vectorSearch: {                 'docs_embedding', 10, $vector
    queryVector: [...],           ) YIELD node, score
    path: "embedding",            RETURN node, score
    limit: 10
  }}
])
```
