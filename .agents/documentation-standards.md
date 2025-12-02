# Documentation Standards

**Purpose**: Complete guide to documenting NornicDB code  
**Audience**: AI coding agents  
**Requirement**: 100% of public APIs must be documented

---

## Documentation Hierarchy

```
1. Package-level documentation    (What does this package do?)
2. Type documentation             (What does this type represent?)
3. Function/method documentation  (What does this do? When to use it?)
4. Real-world examples            (Show actual usage)
5. ELI12 explanations             (Explain complex concepts simply)
```

---

## Package-Level Documentation

**Every package must have comprehensive package documentation:**

```go
// Package cypher provides Neo4j-compatible Cypher query execution for NornicDB.
//
// This package implements a Cypher query parser and executor that supports
// the core Neo4j Cypher query language features. It enables NornicDB to be
// compatible with existing Neo4j applications and tools.
//
// # Supported Cypher Features
//
//   - MATCH: Pattern matching with node and relationship patterns
//   - CREATE: Creating nodes and relationships
//   - MERGE: Upsert operations with ON CREATE/ON MATCH clauses
//   - DELETE/DETACH DELETE: Removing nodes and relationships
//   - SET: Updating node and relationship properties
//   - REMOVE: Removing properties and labels
//   - RETURN: Returning query results
//   - WHERE: Filtering with conditions
//   - WITH: Passing results between query parts
//   - OPTIONAL MATCH: Left outer joins
//   - CALL: Procedure calls
//   - UNWIND: List expansion
//
// # Example Usage
//
//	// Create executor with storage backend
//	storage := storage.NewMemoryEngine()
//	executor := cypher.NewStorageExecutor(storage)
//
//	// Execute Cypher queries
//	result, err := executor.Execute(ctx, "CREATE (n:Person {name: 'Alice', age: 30})", nil)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Query with parameters
//	params := map[string]interface{}{
//		"name": "Alice",
//		"minAge": 25,
//	}
//	result, err = executor.Execute(ctx,
//		"MATCH (n:Person {name: $name}) WHERE n.age >= $minAge RETURN n", params)
//
//	// Complex query with relationships
//	result, err = executor.Execute(ctx, `
//		MATCH (a:Person)-[r:KNOWS]->(b:Person)
//		WHERE a.age > 25
//		RETURN a.name, r.since, b.name
//		ORDER BY a.age DESC
//		LIMIT 10
//	`, nil)
//
//	// Process results
//	for _, row := range result.Rows {
//		fmt.Printf("Row: %v\n", row)
//	}
//
// # Neo4j Compatibility
//
// The executor aims for high compatibility with Neo4j Cypher:
//   - Same syntax and semantics for core operations
//   - Parameter substitution with $param syntax
//   - Neo4j-style error messages and codes
//   - Compatible result format for drivers
//   - Support for Neo4j built-in functions
//
// # Query Processing Pipeline
//
// 1. **Parsing**: Query is parsed into an AST (Abstract Syntax Tree)
// 2. **Validation**: Syntax and semantic validation
// 3. **Parameter Substitution**: Replace $param with actual values
// 4. **Execution Planning**: Determine optimal execution strategy
// 5. **Execution**: Execute against storage backend
// 6. **Result Formatting**: Format results for Neo4j compatibility
//
// # Performance Considerations
//
//   - Pattern matching is optimized for common cases
//   - Indexes are used automatically when available
//   - Query planning chooses efficient execution paths
//   - Bulk operations are optimized for large datasets
//
// # Limitations
//
// Current limitations compared to full Neo4j:
//   - No user-defined procedures (CALL is limited to built-ins)
//   - No complex path expressions
//   - No graph algorithms (shortest path, etc.)
//   - No schema constraints (handled by storage layer)
//   - No transactions (single-query atomicity only)
//
// # ELI12 (Explain Like I'm 12)
//
// Think of Cypher like asking questions about a social network:
//
//  1. **MATCH**: "Find all people named Alice" - like searching through
//     a phone book for everyone with a specific name.
//
//  2. **CREATE**: "Add a new person named Bob" - like writing a new
//     entry in the phone book.
//
//  3. **Relationships**: "Find who Alice knows" - like following the
//     lines between people on a friendship map.
//
//  4. **WHERE**: "Find people older than 25" - like filtering your
//     search to only show certain results.
//
//  5. **RETURN**: "Show me the results" - like getting the final
//     answer to your question.
//
// The query language lets you ask complex questions about connected data,
// like "Find all friends of friends who like the same music as me and
// live in the same city." It's powerful because it understands relationships
// between things, not just individual items.
package cypher
```

**Package doc requirements:**

✅ **Purpose** - What does this package do?  
✅ **Features** - What capabilities does it provide?  
✅ **Examples** - Show 3-5 real usage examples  
✅ **Architecture** - How does it work internally?  
✅ **Compatibility** - How does it relate to standards?  
✅ **Performance** - What are the performance characteristics?  
✅ **Limitations** - What doesn't it do?  
✅ **ELI12** - Explain complex concepts simply  

---

## Type Documentation

**Every exported type must be documented:**

```go
// Node represents a graph node (vertex) in the labeled property graph.
//
// Nodes follow the Neo4j data model with NornicDB-specific extensions for
// memory decay, semantic search, and access tracking. Nodes are the fundamental
// entities in the graph and can represent people, documents, concepts, or any
// other entity in your domain.
//
// # Core Neo4j Fields
//
//   - ID: Unique identifier (must be unique across all nodes)
//   - Labels: Type tags like ["Person", "User"] (Neo4j :Person:User)
//   - Properties: Key-value data (any JSON-serializable types)
//
// # NornicDB Extensions (not exported to Neo4j)
//
//   - CreatedAt: When the node was first created
//   - UpdatedAt: Last modification timestamp
//   - DecayScore: Memory importance (1.0=fresh, 0.0=decayed)
//   - LastAccessed: Last time node was queried/updated
//   - AccessCount: Total access frequency
//   - Embedding: 1024-dim vector for semantic similarity
//
// # Example 1 - Basic User Node
//
//	node := &storage.Node{
//		ID:     storage.NodeID("user-alice"),
//		Labels: []string{"Person", "User"},
//		Properties: map[string]any{
//			"name":     "Alice Johnson",
//			"age":      30,
//			"email":    "alice@example.com",
//			"verified": true,
//		},
//		CreatedAt:   time.Now(),
//		DecayScore:  1.0, // Fresh memory
//		AccessCount: 0,
//	}
//	engine.CreateNode(node)
//
// # Example 2 - Document Node with Metadata
//
//	doc := &storage.Node{
//		ID:     storage.NodeID("doc-readme"),
//		Labels: []string{"Document", "Markdown"},
//		Properties: map[string]any{
//			"title":    "README.md",
//			"content":  "# Welcome to...",
//			"path":     "./README.md",
//			"size":     4096,
//			"language": "markdown",
//		},
//		CreatedAt: time.Now(),
//		Embedding: generateEmbedding("# Welcome to..."), // For semantic search
//	}
//
// # Example 3 - Concept Node for Knowledge Graph
//
//	concept := &storage.Node{
//		ID:     storage.NodeID("concept-database"),
//		Labels: []string{"Concept", "Technology"},
//		Properties: map[string]any{
//			"name":        "Database Systems",
//			"description": "Software for storing and retrieving data",
//			"category":    "Computer Science",
//		},
//		CreatedAt: time.Now(),
//	}
//
// # ELI12
//
// Think of a Node like a person's profile card:
//   - ID: Their unique username (can't have two people with same ID)
//   - Labels: Tags like "Student", "Athlete" (one person can have many)
//   - Properties: Info about them (name, age, favorite color, etc.)
//
// NornicDB adds cool features:
//   - DecayScore: "How important is this?" (like memory fading over time)
//   - Embedding: "What is this similar to?" (like finding related topics)
//   - AccessCount: "How often do we look at this?" (popularity tracking)
//
// Just like you might have a profile on social media with your info and tags,
// nodes store information about things in your graph database!
//
// # Neo4j Compatibility
//
//   - ID maps to Neo4j node ID (must be unique)
//   - Labels map to Neo4j node labels (e.g., :Person:User)
//   - Properties map to Neo4j node properties
//   - NornicDB extensions are stored but not exported to Neo4j format
//
// # Thread Safety
//
//	Node structs are NOT thread-safe. The storage engine handles concurrency.
type Node struct {
	ID         NodeID         `json:"id"`
	Labels     []string       `json:"labels"`
	Properties map[string]any `json:"properties"`
	
	// NornicDB extensions
	CreatedAt    time.Time `json:"createdAt,omitempty"`
	UpdatedAt    time.Time `json:"updatedAt,omitempty"`
	DecayScore   float64   `json:"decayScore,omitempty"`
	LastAccessed time.Time `json:"lastAccessed,omitempty"`
	AccessCount  int64     `json:"accessCount,omitempty"`
	Embedding    []float32 `json:"embedding,omitempty"`
}
```

**Type doc requirements:**

✅ **Purpose** - What does this type represent?  
✅ **Fields** - Explain each field's purpose  
✅ **Examples** - Show 3 real-world usage examples  
✅ **ELI12** - Explain the concept simply  
✅ **Compatibility** - How it maps to standards  
✅ **Thread Safety** - Concurrency considerations  

---

## Function Documentation

**Every exported function must be documented:**

```go
// Execute executes a Cypher query and returns the results.
//
// This is the main entry point for executing Cypher queries against the
// storage backend. It handles parsing, validation, parameter substitution,
// execution planning, and result formatting.
//
// # Parameters
//
//   - ctx: Context for cancellation and timeouts
//   - query: Cypher query string (Neo4j-compatible syntax)
//   - params: Parameter values for $param substitution (can be nil)
//
// # Returns
//
//   - *Result: Query results with rows and metadata
//   - error: Error if query fails (syntax, execution, etc.)
//
// # Example 1 - Simple Query
//
//	result, err := executor.Execute(ctx, "MATCH (n:Person) RETURN n", nil)
//	if err != nil {
//		log.Fatal(err)
//	}
//	fmt.Printf("Found %d people\n", len(result.Rows))
//
// # Example 2 - Query with Parameters
//
//	params := map[string]interface{}{
//		"name": "Alice",
//		"minAge": 25,
//	}
//	result, err := executor.Execute(ctx,
//		"MATCH (n:Person {name: $name}) WHERE n.age >= $minAge RETURN n",
//		params)
//
// # Example 3 - Complex Query
//
//	result, err := executor.Execute(ctx, `
//		MATCH (a:Person)-[r:KNOWS]->(b:Person)
//		WHERE a.age > 25
//		RETURN a.name, r.since, b.name
//		ORDER BY a.age DESC
//		LIMIT 10
//	`, nil)
//
// # Error Handling
//
// Returns error for:
//   - Syntax errors in query
//   - Invalid parameter references
//   - Type mismatches
//   - Storage backend errors
//   - Context cancellation
//
// # Performance
//
//   - Queries are cached based on query string and parameters
//   - Indexes are used automatically when available
//   - Query planning optimizes execution order
//   - Typical execution: 2,000-10,000 ops/sec depending on complexity
//
// # Thread Safety
//
// Execute is thread-safe and can be called concurrently from multiple
// goroutines. Internal state is protected by appropriate synchronization.
func (e *Executor) Execute(ctx context.Context, query string, params map[string]interface{}) (*Result, error) {
	// Implementation...
}
```

**Function doc requirements:**

✅ **Purpose** - What does this function do?  
✅ **Parameters** - Explain each parameter  
✅ **Returns** - Explain return values  
✅ **Examples** - Show 1-3 usage examples  
✅ **Error Handling** - What errors can occur?  
✅ **Performance** - Performance characteristics  
✅ **Thread Safety** - Concurrency guarantees  

---

## Method Documentation

**Methods follow same pattern as functions:**

```go
// CreateNode creates a new node in the graph.
//
// The node must have a unique ID that doesn't already exist in the graph.
// Labels and properties are optional. If the node already exists, returns
// ErrAlreadyExists.
//
// # Parameters
//
//   - node: Node to create (ID must be unique)
//
// # Returns
//
//   - error: ErrAlreadyExists if node ID already exists, or storage error
//
// # Example 1 - Simple Node
//
//	node := &Node{
//		ID:     NodeID("user-123"),
//		Labels: []string{"User"},
//		Properties: map[string]any{
//			"name": "Alice",
//		},
//	}
//	err := engine.CreateNode(node)
//
// # Example 2 - Node with Multiple Labels
//
//	node := &Node{
//		ID:     NodeID("alice"),
//		Labels: []string{"Person", "User", "Admin"},
//		Properties: map[string]any{
//			"name":  "Alice Johnson",
//			"email": "alice@example.com",
//			"role":  "administrator",
//		},
//	}
//	err := engine.CreateNode(node)
//
// # Example 3 - Node with Embedding
//
//	embedding := generateEmbedding(content)
//	node := &Node{
//		ID:        NodeID("doc-1"),
//		Labels:    []string{"Document"},
//		Properties: map[string]any{"content": content},
//		Embedding: embedding,
//	}
//	err := engine.CreateNode(node)
//
// # Thread Safety
//
// CreateNode is thread-safe and can be called concurrently.
func (e *MemoryEngine) CreateNode(node *Node) error {
	// Implementation...
}
```

---

## Constant Documentation

**Document exported constants:**

```go
// Default configuration values for NornicDB

const (
	// DefaultBoltPort is the default port for Neo4j Bolt protocol.
	// Neo4j uses 7687 by default, so we match for compatibility.
	DefaultBoltPort = 7687
	
	// DefaultHTTPPort is the default port for HTTP API.
	// Neo4j uses 7474 by default, so we match for compatibility.
	DefaultHTTPPort = 7474
	
	// DefaultEmbeddingDimensions is the default vector size for embeddings.
	// 1024 dimensions provides good balance between quality and performance.
	// Common models: mxbai-embed-large (1024), nomic-embed-text (768).
	DefaultEmbeddingDimensions = 1024
	
	// DefaultCacheSize is the default number of query results to cache.
	// Tuned for typical workloads with ~100MB memory usage.
	DefaultCacheSize = 1000
	
	// DefaultDecayHalfLife is the default memory decay half-life in days.
	// After 7 days, memory importance drops to 50% of original value.
	// Based on Ebbinghaus forgetting curve research.
	DefaultDecayHalfLife = 7
)
```

---

## ELI12 Explanations

**Explain complex concepts in simple terms:**

### Good ELI12 Examples

```go
// ELI12: Memory Decay
//
// Imagine your brain remembering things:
//   - Fresh memories (yesterday): Very clear and detailed
//   - Old memories (last year): Fuzzy and less important
//   - Ancient memories (childhood): Very faded
//
// NornicDB does the same thing! When you create a node, it starts with
// a "decay score" of 1.0 (100% fresh). Over time, this score drops:
//   - After 7 days: 0.5 (50% as important)
//   - After 14 days: 0.25 (25% as important)
//   - After 21 days: 0.125 (12.5% as important)
//
// But here's the cool part: If you access a node (read or update it),
// the decay score goes back up! Just like how remembering something
// makes it stronger in your brain.
//
// This helps NornicDB automatically manage what's important:
//   - Recent + frequently accessed = stays important
//   - Old + never accessed = fades away
//   - Old but accessed = becomes important again

// ELI12: Vector Embeddings
//
// Imagine describing your favorite movie to a friend:
//   - "It's an action movie" (one dimension)
//   - "It's funny" (another dimension)
//   - "It has robots" (another dimension)
//   - ... and 1021 more dimensions!
//
// A vector embedding is like a list of 1024 numbers that describe
// something. Each number represents how much that thing has a certain
// quality. For example:
//   - [0.8, 0.2, 0.9, ...] might be "funny action movie with robots"
//   - [0.1, 0.9, 0.1, ...] might be "serious drama with no action"
//
// The cool part: We can compare these lists to find similar things!
// If two lists have similar numbers, the things they describe are similar.
//
// NornicDB uses this to find related nodes:
//   - "Find documents similar to this one"
//   - "Find people with similar interests"
//   - "Find concepts related to this topic"
//
// It's like having a super-smart search that understands meaning,
// not just matching exact words!

// ELI12: Graph Database
//
// Think of a social network like Facebook:
//   - People are "nodes" (circles on a map)
//   - Friendships are "edges" (lines connecting circles)
//
// A graph database stores information this way:
//   - Nodes: Things (people, places, documents, concepts)
//   - Edges: Relationships (knows, likes, contains, similar_to)
//
// Why is this useful? Because you can ask questions like:
//   - "Who are my friends?" (follow edges from me)
//   - "Who are friends of friends?" (follow edges twice)
//   - "What do my friends like?" (follow edges to people, then to things)
//
// Regular databases (like SQL) are bad at this because they store
// data in tables (like spreadsheets). Graph databases are GREAT at
// this because they're designed for connected data!
//
// NornicDB is a graph database that's compatible with Neo4j, which
// means if you know Neo4j, you already know how to use NornicDB!
```

### ELI12 Template

```go
// ELI12: [Concept Name]
//
// [Simple analogy from everyday life]
//
// [Explain the concept using the analogy]
//
// [Show concrete examples]
//
// [Explain why it's useful]
//
// [Connect back to NornicDB]
```

---

## Documentation Checklist

### For Every Package

- [ ] Package-level documentation
- [ ] Purpose and features
- [ ] 3-5 usage examples
- [ ] Architecture overview
- [ ] Compatibility notes
- [ ] Performance characteristics
- [ ] Known limitations
- [ ] ELI12 explanation

### For Every Type

- [ ] Type documentation
- [ ] Purpose and use cases
- [ ] Field explanations
- [ ] 3 real-world examples
- [ ] ELI12 explanation
- [ ] Compatibility notes
- [ ] Thread safety notes

### For Every Function/Method

- [ ] Function documentation
- [ ] Purpose
- [ ] Parameter descriptions
- [ ] Return value descriptions
- [ ] 1-3 usage examples
- [ ] Error conditions
- [ ] Performance notes
- [ ] Thread safety notes

### For Every Constant

- [ ] Constant documentation
- [ ] Purpose and default value
- [ ] Why this value was chosen
- [ ] When to change it

---

## Documentation Tools

### Generate Documentation

```bash
# Generate HTML documentation
go doc -all github.com/orneryd/nornicdb/pkg/cypher > cypher-docs.txt

# View package documentation
go doc github.com/orneryd/nornicdb/pkg/cypher

# View type documentation
go doc github.com/orneryd/nornicdb/pkg/cypher.Executor

# View function documentation
go doc github.com/orneryd/nornicdb/pkg/cypher.Execute
```

### Documentation Linters

```bash
# Check for missing documentation
golangci-lint run --enable=godot,godox

# Check documentation style
go vet ./...
```

---

## Common Documentation Mistakes

### ❌ Bad: Vague Description

```go
// Execute runs a query
func (e *Executor) Execute(ctx context.Context, query string) (*Result, error)
```

### ✅ Good: Detailed Description

```go
// Execute executes a Cypher query and returns the results.
//
// This is the main entry point for executing Cypher queries against the
// storage backend. It handles parsing, validation, parameter substitution,
// execution planning, and result formatting.
//
// Example:
//	result, err := executor.Execute(ctx, "MATCH (n:Person) RETURN n", nil)
func (e *Executor) Execute(ctx context.Context, query string, params map[string]interface{}) (*Result, error)
```

### ❌ Bad: No Examples

```go
// CreateNode creates a new node in the graph.
func (e *MemoryEngine) CreateNode(node *Node) error
```

### ✅ Good: With Examples

```go
// CreateNode creates a new node in the graph.
//
// Example:
//	node := &Node{
//		ID:     NodeID("user-123"),
//		Labels: []string{"User"},
//		Properties: map[string]any{"name": "Alice"},
//	}
//	err := engine.CreateNode(node)
func (e *MemoryEngine) CreateNode(node *Node) error
```

### ❌ Bad: No Error Documentation

```go
// Parse parses a Cypher query.
func Parse(query string) (*AST, error)
```

### ✅ Good: With Error Documentation

```go
// Parse parses a Cypher query into an Abstract Syntax Tree.
//
// Returns error for:
//   - Syntax errors in the query
//   - Unsupported Cypher features
//   - Invalid parameter references
//
// Example:
//	ast, err := Parse("MATCH (n:Person) RETURN n")
//	if err != nil {
//		log.Fatal(err)
//	}
func Parse(query string) (*AST, error)
```

---

## Quick Reference

### Documentation Template

```go
// [FunctionName] [brief one-line description].
//
// [Detailed description of what this does and when to use it]
//
// # Parameters
//
//   - param1: Description of param1
//   - param2: Description of param2
//
// # Returns
//
//   - returnType: Description of return value
//   - error: Description of error conditions
//
// # Example 1 - [Scenario]
//
//	code example
//
// # Example 2 - [Scenario]
//
//	code example
//
// # Example 3 - [Scenario]
//
//	code example
//
// # Error Handling
//
// Returns error for:
//   - Condition 1
//   - Condition 2
//
// # Performance
//
// [Performance characteristics and benchmarks]
//
// # Thread Safety
//
// [Concurrency guarantees]
func FunctionName(param1 Type1, param2 Type2) (ReturnType, error) {
	// Implementation
}
```

---

**Remember**: Documentation is for humans (and AI agents). Write clearly, provide examples, and explain complex concepts simply. Good documentation is as important as good code.
