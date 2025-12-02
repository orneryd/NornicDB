# Architecture Patterns

**Purpose**: System design principles and architectural patterns for NornicDB  
**Audience**: AI coding agents  
**Focus**: Separation of concerns, clean architecture, maintainability

---

## System Architecture

### High-Level Overview

```
┌─────────────────────────────────────────────────────────────┐
│                     Client Applications                      │
│  (Neo4j Drivers, MCP Clients, HTTP Clients)                 │
└────────────────────────┬────────────────────────────────────┘
                         │
┌────────────────────────┴────────────────────────────────────┐
│                      API Layer                               │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │ Bolt Server  │  │  MCP Server  │  │  HTTP Server │     │
│  │  (Neo4j)     │  │   (AI)       │  │   (REST)     │     │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘     │
└─────────┼──────────────────┼──────────────────┼─────────────┘
          │                  │                  │
┌─────────┴──────────────────┴──────────────────┴─────────────┐
│                    Query Layer                                │
│  ┌──────────────────────────────────────────────────────┐   │
│  │              Cypher Executor                          │   │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐          │   │
│  │  │  Parser  │→ │Optimizer │→ │ Executor │          │   │
│  │  └──────────┘  └──────────┘  └──────────┘          │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────┬────────────────────────────────────┘
                          │
┌─────────────────────────┴────────────────────────────────────┐
│                    Storage Layer                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │   Memory     │  │    Badger    │  │   Indexes    │      │
│  │   Engine     │  │   Engine     │  │   (B-Tree)   │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
└───────────────────────────────────────────────────────────────┘
                          │
┌─────────────────────────┴────────────────────────────────────┐
│                Infrastructure Layer                           │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │    Cache     │  │     Pool     │  │   Metrics    │      │
│  │  (LRU/TTL)   │  │ (Connections)│  │ (Prometheus) │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
└───────────────────────────────────────────────────────────────┘
```

---

## Layer Responsibilities

### 1. API Layer

**Purpose**: Protocol translation and request/response handling

**Responsibilities:**
- Accept client connections (Bolt, HTTP, MCP)
- Parse protocol-specific requests
- Authenticate and authorize requests
- Translate to internal format
- Format responses for protocol
- Handle connection lifecycle

**Key Files:**
- `pkg/bolt/server.go` - Neo4j Bolt protocol
- `pkg/mcp/server.go` - Model Context Protocol
- `pkg/server/server.go` - HTTP REST API

**Example:**
```go
// pkg/bolt/server.go
type BoltServer struct {
    executor *cypher.Executor  // ← Depends on Query Layer
    auth     *auth.Manager
    pool     *pool.ConnectionPool
}

func (s *BoltServer) HandleQuery(conn net.Conn, msg *BoltMessage) error {
    // 1. Parse Bolt protocol message
    query, params := parseBoltQuery(msg)
    
    // 2. Authenticate
    if err := s.auth.Authenticate(conn); err != nil {
        return err
    }
    
    // 3. Execute via Query Layer
    result, err := s.executor.Execute(ctx, query, params)
    if err != nil {
        return s.sendError(conn, err)
    }
    
    // 4. Format response for Bolt protocol
    return s.sendBoltResult(conn, result)
}
```

### 2. Query Layer

**Purpose**: Query parsing, optimization, and execution

**Responsibilities:**
- Parse Cypher queries into AST
- Validate syntax and semantics
- Optimize query execution plan
- Execute queries against storage
- Format results
- Cache query results

**Key Files:**
- `pkg/cypher/executor.go` - Query orchestration
- `pkg/cypher/parser.go` - Cypher parsing
- `pkg/cypher/optimizer.go` - Query optimization
- `pkg/cypher/cache.go` - Result caching

**Example:**
```go
// pkg/cypher/executor.go
type Executor struct {
    storage   storage.Engine    // ← Depends on Storage Layer
    parser    Parser
    optimizer Optimizer
    cache     Cache
}

func (e *Executor) Execute(ctx context.Context, query string, 
    params map[string]interface{}) (*Result, error) {
    
    // 1. Parse query
    ast, err := e.parser.Parse(query)
    if err != nil {
        return nil, err
    }
    
    // 2. Optimize
    plan, err := e.optimizer.Optimize(ast)
    if err != nil {
        return nil, err
    }
    
    // 3. Check cache
    if cached, ok := e.cache.Get(query, params); ok {
        return cached, nil
    }
    
    // 4. Execute against storage
    rows, err := e.executeAgainstStorage(ctx, plan, params)
    if err != nil {
        return nil, err
    }
    
    // 5. Format and cache
    result := e.formatResult(rows)
    e.cache.Set(query, params, result)
    
    return result, nil
}
```

### 3. Storage Layer

**Purpose**: Data persistence and retrieval

**Responsibilities:**
- Store and retrieve nodes and edges
- Manage indexes (B-Tree, vector)
- Handle transactions
- Enforce uniqueness constraints
- Provide ACID guarantees
- Manage data files

**Key Files:**
- `pkg/storage/types.go` - Core types and interfaces
- `pkg/storage/memory.go` - In-memory engine (testing)
- `pkg/storage/badger_engine.go` - Persistent engine
- `pkg/storage/schema.go` - Index management

**Example:**
```go
// pkg/storage/types.go
type Engine interface {
    // Node operations
    CreateNode(node *Node) error
    GetNode(id NodeID) (*Node, error)
    UpdateNode(node *Node) error
    DeleteNode(id NodeID) error
    
    // Edge operations
    CreateEdge(edge *Edge) error
    GetEdge(id EdgeID) (*Edge, error)
    DeleteEdge(id EdgeID) error
    
    // Query operations
    FindNodes(labels []string, properties map[string]interface{}) ([]*Node, error)
    GetNeighbors(id NodeID, direction Direction) ([]*Edge, error)
    
    // Index operations
    CreateIndex(label string, property string) error
    DropIndex(label string, property string) error
}

// pkg/storage/badger_engine.go
type BadgerEngine struct {
    db      *badger.DB
    indexes map[string]*BTreeIndex
    mu      sync.RWMutex
}

func (e *BadgerEngine) CreateNode(node *Node) error {
    e.mu.Lock()
    defer e.mu.Unlock()
    
    // 1. Check uniqueness
    if e.nodeExists(node.ID) {
        return ErrAlreadyExists
    }
    
    // 2. Serialize node
    data, err := json.Marshal(node)
    if err != nil {
        return err
    }
    
    // 3. Write to storage
    err = e.db.Update(func(txn *badger.Txn) error {
        key := []byte("node:" + string(node.ID))
        return txn.Set(key, data)
    })
    if err != nil {
        return err
    }
    
    // 4. Update indexes
    for _, label := range node.Labels {
        if idx, ok := e.indexes[label]; ok {
            idx.Insert(node.ID, node.Properties)
        }
    }
    
    return nil
}
```

### 4. Infrastructure Layer

**Purpose**: Cross-cutting concerns

**Responsibilities:**
- Caching (query results, embeddings)
- Connection pooling
- Metrics and monitoring
- Logging and tracing
- Configuration management
- Health checks

**Key Files:**
- `pkg/cache/query_cache.go` - Query result caching
- `pkg/pool/pool.go` - Connection pooling
- `pkg/config/config.go` - Configuration
- `pkg/audit/audit.go` - Audit logging

**Example:**
```go
// pkg/cache/query_cache.go
type QueryCache struct {
    data      map[string]*CacheEntry
    mu        sync.RWMutex
    maxSize   int
    ttl       time.Duration
    eviction  EvictionPolicy
}

func (c *QueryCache) Get(key string) (interface{}, bool) {
    c.mu.RLock()
    defer c.mu.RUnlock()
    
    entry, ok := c.data[key]
    if !ok {
        return nil, false
    }
    
    // Check TTL
    if time.Since(entry.CreatedAt) > c.ttl {
        return nil, false
    }
    
    // Update access time for LRU
    entry.LastAccessed = time.Now()
    entry.AccessCount++
    
    return entry.Value, true
}
```

---

## Design Patterns

### Pattern 1: Dependency Injection

**Use interfaces to decouple layers:**

```go
// Define interface in consumer package
// pkg/cypher/executor.go
type Storage interface {
    CreateNode(node *Node) error
    GetNode(id NodeID) (*Node, error)
    // ... more methods
}

type Executor struct {
    storage Storage  // ← Interface, not concrete type
}

func NewExecutor(storage Storage) *Executor {
    return &Executor{storage: storage}
}

// Implementations in provider package
// pkg/storage/memory.go
type MemoryEngine struct {
    nodes map[NodeID]*Node
}

func (e *MemoryEngine) CreateNode(node *Node) error {
    // Implementation
}

// pkg/storage/badger_engine.go
type BadgerEngine struct {
    db *badger.DB
}

func (e *BadgerEngine) CreateNode(node *Node) error {
    // Implementation
}

// Usage - inject any implementation
memEngine := storage.NewMemoryEngine()
executor1 := cypher.NewExecutor(memEngine)

badgerEngine := storage.NewBadgerEngine("/data")
executor2 := cypher.NewExecutor(badgerEngine)
```

**Benefits:**
- Testability (inject mocks)
- Flexibility (swap implementations)
- Decoupling (no concrete dependencies)

### Pattern 2: Repository Pattern

**Encapsulate data access:**

```go
// pkg/storage/repository.go
type NodeRepository interface {
    Create(node *Node) error
    FindByID(id NodeID) (*Node, error)
    FindByLabel(label string) ([]*Node, error)
    Update(node *Node) error
    Delete(id NodeID) error
}

type nodeRepository struct {
    engine Engine
    cache  Cache
}

func NewNodeRepository(engine Engine, cache Cache) NodeRepository {
    return &nodeRepository{
        engine: engine,
        cache:  cache,
    }
}

func (r *nodeRepository) FindByID(id NodeID) (*Node, error) {
    // Check cache first
    if cached, ok := r.cache.Get(string(id)); ok {
        return cached.(*Node), nil
    }
    
    // Fetch from storage
    node, err := r.engine.GetNode(id)
    if err != nil {
        return nil, err
    }
    
    // Cache result
    r.cache.Set(string(id), node)
    
    return node, nil
}
```

### Pattern 3: Strategy Pattern

**Encapsulate algorithms:**

```go
// pkg/cypher/strategy.go
type ExecutionStrategy interface {
    CanHandle(ast *AST) bool
    Execute(ctx context.Context, ast *AST) (*Result, error)
}

// Simple queries (no joins, no aggregation)
type SimpleStrategy struct {
    storage storage.Engine
}

func (s *SimpleStrategy) CanHandle(ast *AST) bool {
    return ast.IsSimple() && !ast.HasAggregation()
}

func (s *SimpleStrategy) Execute(ctx context.Context, ast *AST) (*Result, error) {
    // Optimized execution for simple queries
}

// Complex queries (joins, aggregation)
type ComplexStrategy struct {
    storage storage.Engine
}

func (s *ComplexStrategy) CanHandle(ast *AST) bool {
    return ast.HasJoins() || ast.HasAggregation()
}

func (s *ComplexStrategy) Execute(ctx context.Context, ast *AST) (*Result, error) {
    // Full execution pipeline
}

// Executor selects strategy
type Executor struct {
    strategies []ExecutionStrategy
}

func (e *Executor) Execute(ctx context.Context, query string) (*Result, error) {
    ast, _ := Parse(query)
    
    for _, strategy := range e.strategies {
        if strategy.CanHandle(ast) {
            return strategy.Execute(ctx, ast)
        }
    }
    
    return nil, errors.New("no strategy found")
}
```

### Pattern 4: Builder Pattern

**Construct complex objects:**

```go
// pkg/cypher/query_builder.go
type QueryBuilder struct {
    match    []string
    where    []string
    with     []string
    returns  []string
    orderBy  []string
    limit    *int
    params   map[string]interface{}
}

func NewQueryBuilder() *QueryBuilder {
    return &QueryBuilder{
        params: make(map[string]interface{}),
    }
}

func (b *QueryBuilder) Match(pattern string) *QueryBuilder {
    b.match = append(b.match, pattern)
    return b
}

func (b *QueryBuilder) Where(condition string) *QueryBuilder {
    b.where = append(b.where, condition)
    return b
}

func (b *QueryBuilder) Return(fields ...string) *QueryBuilder {
    b.returns = append(b.returns, fields...)
    return b
}

func (b *QueryBuilder) OrderBy(field string) *QueryBuilder {
    b.orderBy = append(b.orderBy, field)
    return b
}

func (b *QueryBuilder) Limit(n int) *QueryBuilder {
    b.limit = &n
    return b
}

func (b *QueryBuilder) Build() string {
    var parts []string
    
    if len(b.match) > 0 {
        parts = append(parts, "MATCH "+strings.Join(b.match, ", "))
    }
    
    if len(b.where) > 0 {
        parts = append(parts, "WHERE "+strings.Join(b.where, " AND "))
    }
    
    if len(b.returns) > 0 {
        parts = append(parts, "RETURN "+strings.Join(b.returns, ", "))
    }
    
    if len(b.orderBy) > 0 {
        parts = append(parts, "ORDER BY "+strings.Join(b.orderBy, ", "))
    }
    
    if b.limit != nil {
        parts = append(parts, fmt.Sprintf("LIMIT %d", *b.limit))
    }
    
    return strings.Join(parts, " ")
}

// Usage
query := NewQueryBuilder().
    Match("(p:Person)").
    Where("p.age > 25").
    Return("p.name", "p.age").
    OrderBy("p.age DESC").
    Limit(10).
    Build()
```

### Pattern 5: Observer Pattern

**Event notification:**

```go
// pkg/storage/events.go
type EventType int

const (
    NodeCreated EventType = iota
    NodeUpdated
    NodeDeleted
    EdgeCreated
    EdgeDeleted
)

type Event struct {
    Type      EventType
    NodeID    NodeID
    EdgeID    EdgeID
    Timestamp time.Time
}

type EventListener interface {
    OnEvent(event Event)
}

type EventBus struct {
    listeners []EventListener
    mu        sync.RWMutex
}

func (b *EventBus) Subscribe(listener EventListener) {
    b.mu.Lock()
    defer b.mu.Unlock()
    b.listeners = append(b.listeners, listener)
}

func (b *EventBus) Publish(event Event) {
    b.mu.RLock()
    defer b.mu.RUnlock()
    
    for _, listener := range b.listeners {
        go listener.OnEvent(event)  // Async notification
    }
}

// Usage - cache invalidation listener
type CacheInvalidator struct {
    cache Cache
}

func (c *CacheInvalidator) OnEvent(event Event) {
    switch event.Type {
    case NodeUpdated, NodeDeleted:
        c.cache.Invalidate(string(event.NodeID))
    }
}

// Subscribe to events
eventBus := NewEventBus()
eventBus.Subscribe(&CacheInvalidator{cache: cache})
eventBus.Subscribe(&MetricsCollector{})
eventBus.Subscribe(&AuditLogger{})
```

---

## Architectural Principles

### 1. Separation of Concerns

**Each layer has ONE responsibility:**

✅ **Good:**
```go
// API Layer - Protocol handling ONLY
type BoltServer struct {
    executor *cypher.Executor  // Delegate to Query Layer
}

// Query Layer - Query execution ONLY
type Executor struct {
    storage storage.Engine  // Delegate to Storage Layer
}

// Storage Layer - Data persistence ONLY
type BadgerEngine struct {
    db *badger.DB
}
```

❌ **Bad:**
```go
// API Layer doing query execution (wrong layer!)
type BoltServer struct {
    db *badger.DB  // Should not access storage directly!
}

func (s *BoltServer) HandleQuery(query string) {
    // Parsing query in API layer - wrong!
    ast := parseQuery(query)
    
    // Accessing storage in API layer - wrong!
    nodes := s.db.FindNodes(...)
}
```

### 2. Dependency Inversion

**Depend on abstractions, not concretions:**

✅ **Good:**
```go
// High-level module depends on interface
type Executor struct {
    storage Storage  // ← Interface
}

// Low-level module implements interface
type BadgerEngine struct {
    db *badger.DB
}

func (e *BadgerEngine) CreateNode(node *Node) error {
    // Implementation
}
```

❌ **Bad:**
```go
// High-level module depends on concrete type
type Executor struct {
    storage *BadgerEngine  // ← Concrete type - can't swap!
}
```

### 3. Interface Segregation

**Small, focused interfaces:**

✅ **Good:**
```go
// Small, focused interfaces
type NodeReader interface {
    GetNode(id NodeID) (*Node, error)
    FindNodes(labels []string) ([]*Node, error)
}

type NodeWriter interface {
    CreateNode(node *Node) error
    UpdateNode(node *Node) error
    DeleteNode(id NodeID) error
}

// Compose when needed
type NodeRepository interface {
    NodeReader
    NodeWriter
}
```

❌ **Bad:**
```go
// Giant interface - hard to implement and test
type Storage interface {
    // 50+ methods...
    CreateNode(node *Node) error
    GetNode(id NodeID) (*Node, error)
    CreateEdge(edge *Edge) error
    GetEdge(id EdgeID) (*Edge, error)
    CreateIndex(label, prop string) error
    // ... 45 more methods
}
```

### 4. Single Responsibility

**Each type has ONE reason to change:**

✅ **Good:**
```go
// Parser - ONE responsibility: parsing
type Parser struct {}
func (p *Parser) Parse(query string) (*AST, error)

// Optimizer - ONE responsibility: optimization
type Optimizer struct {}
func (o *Optimizer) Optimize(ast *AST) (*Plan, error)

// Executor - ONE responsibility: execution
type Executor struct {}
func (e *Executor) Execute(plan *Plan) (*Result, error)
```

❌ **Bad:**
```go
// God object - too many responsibilities
type QueryProcessor struct {}
func (p *QueryProcessor) Parse(query string) (*AST, error)
func (p *QueryProcessor) Optimize(ast *AST) (*Plan, error)
func (p *QueryProcessor) Execute(plan *Plan) (*Result, error)
func (p *QueryProcessor) Cache(result *Result)
func (p *QueryProcessor) Log(message string)
// ... 20 more methods
```

### 5. Open/Closed Principle

**Open for extension, closed for modification:**

✅ **Good:**
```go
// Extensible via strategy pattern
type ExecutionStrategy interface {
    CanHandle(ast *AST) bool
    Execute(ctx context.Context, ast *AST) (*Result, error)
}

// Add new strategy without modifying existing code
type NewOptimizedStrategy struct {}
func (s *NewOptimizedStrategy) CanHandle(ast *AST) bool { /* ... */ }
func (s *NewOptimizedStrategy) Execute(ctx context.Context, ast *AST) (*Result, error) { /* ... */ }

// Register new strategy
executor.RegisterStrategy(&NewOptimizedStrategy{})
```

❌ **Bad:**
```go
// Must modify existing code to add new behavior
func (e *Executor) Execute(ast *AST) (*Result, error) {
    if ast.IsSimple() {
        return e.executeSimple(ast)
    } else if ast.IsComplex() {
        return e.executeComplex(ast)
    } else if ast.IsNewType() {  // ← Must modify this function!
        return e.executeNewType(ast)
    }
}
```

---

## Package Organization

### Good Package Structure

```
pkg/
├── bolt/              # Bolt protocol server
│   ├── server.go
│   ├── protocol.go
│   └── server_test.go
│
├── cypher/            # Query execution
│   ├── executor.go    # Core orchestration
│   ├── parser.go      # Query parsing
│   ├── optimizer.go   # Query optimization
│   ├── cache.go       # Result caching
│   └── *_test.go
│
├── storage/           # Data persistence
│   ├── types.go       # Core types & interfaces
│   ├── memory.go      # In-memory engine
│   ├── badger_engine.go  # Persistent engine
│   ├── schema.go      # Index management
│   └── *_test.go
│
├── cache/             # Caching utilities
│   ├── query_cache.go
│   └── lru.go
│
├── pool/              # Connection pooling
│   └── pool.go
│
└── config/            # Configuration
    ├── config.go
    └── feature_flags.go
```

### Package Dependencies

**Allowed dependencies (top to bottom):**

```
API Layer (bolt, mcp, server)
    ↓
Query Layer (cypher)
    ↓
Storage Layer (storage)
    ↓
Infrastructure (cache, pool, config)
```

**Forbidden dependencies:**
- ❌ Storage → Query
- ❌ Storage → API
- ❌ Query → API
- ❌ Infrastructure → any other layer

---

## Quick Reference

### Architecture Checklist

- [ ] Clear layer boundaries
- [ ] Dependency injection via interfaces
- [ ] No circular dependencies
- [ ] Single responsibility per type
- [ ] Small, focused interfaces
- [ ] Testable (can inject mocks)
- [ ] Extensible (open/closed principle)

### Common Anti-Patterns

❌ **God Object** - One type does everything  
❌ **Circular Dependencies** - A imports B, B imports A  
❌ **Tight Coupling** - Depends on concrete types  
❌ **Leaky Abstraction** - Implementation details exposed  
❌ **Feature Envy** - Type uses another type's data too much  

---

**Remember**: Good architecture makes code easier to understand, test, and modify. Invest time in clean boundaries and clear contracts.
