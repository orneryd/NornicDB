# User Guides

**Comprehensive guides for using NornicDB features and capabilities.**

## 📚 Available Guides

### Core Features
- **[Vector Search](vector-search.md)** - Semantic search with embeddings
- **[Hybrid Search (RRF)](hybrid-search.md)** - Combine vector + BM25 search
- **[Qdrant gRPC Endpoint](qdrant-grpc.md)** - Use Qdrant SDKs against NornicDB
- **[Transactions](transactions.md)** - ACID guarantees and transaction management
- **[Clustering](clustering.md)** - High availability and replication
- **[Heimdall AI Assistant](heimdall-ai-assistant.md)** - Built-in AI for natural language database interaction
- **[Heimdall Context & Tokens](heimdall-context.md)** - Understanding the token budget and context handling
- **[Complete Examples](complete-examples.md)** - Full application examples

### Query & Data Management
- **[GraphQL API](graphql.md)** - GraphQL queries, mutations, and real-time subscriptions
- **[Cypher Queries](cypher-queries.md)** - Complete Cypher language guide
- **[Graph Traversal](graph-traversal.md)** - Path queries and pattern matching
- **[Data Import/Export](data-import-export.md)** - Neo4j compatibility

### Operations
- **[Clustering](clustering.md)** - Hot Standby, Raft, Multi-Region replication
- **[Plugin System](plugin-system.md)** - APOC functions and custom plugins
- **[Heimdall Plugins](heimdall-plugins.md)** - Extend the AI assistant with custom actions

## 🎯 Quick Start Paths

### I want to...

**Search by meaning, not just keywords**
→ [Vector Search Guide](vector-search.md)

**Get the best search results**
→ [Hybrid Search (RRF)](hybrid-search.md)

**Use GraphQL API with real-time subscriptions**
→ [GraphQL Guide](graphql.md)

**Learn Cypher query language**
→ [Cypher Queries Guide](cypher-queries.md)

**Find paths between nodes**
→ [Graph Traversal Guide](graph-traversal.md)

**Ensure data consistency**
→ [Transactions Guide](transactions.md)

**Migrate from Neo4j**
→ [Data Import/Export](data-import-export.md)

**See complete examples**
→ [Complete Examples](complete-examples.md)

**Set up high availability**
→ [Clustering Guide](clustering.md)

**Use natural language to query my database**
→ [Heimdall AI Assistant](heimdall-ai-assistant.md)

**Extend Heimdall with custom actions**
→ [Heimdall Plugins](heimdall-plugins.md)

**Understand Heimdall's token budget**
→ [Heimdall Context & Tokens](heimdall-context.md)

**Build event-driven automation**
→ [Heimdall Plugins - Autonomous Actions](heimdall-plugins.md#autonomous-action-invocation-heimdallinvoker)

## 🤖 Heimdall Plugin Features

Heimdall plugins support advanced capabilities:

| Feature | Description |
|---------|-------------|
| **Lifecycle Hooks** | Modify prompts, validate actions, log results |
| **Database Events** | React to CRUD operations, queries, transactions |
| **Autonomous Actions** | Trigger SLM analysis based on accumulated events |
| **Inline Notifications** | Send ordered messages to the chat interface |
| **Request Cancellation** | Cancel requests with reasons from any hook |

See [Heimdall Plugins Guide](heimdall-plugins.md) for complete documentation.

## 📖 Learning Path

### Beginner
1. [Getting Started](../getting-started/)
2. [First Queries](../getting-started/first-queries.md)
3. [Cypher Queries](cypher-queries.md)
4. [Complete Examples](complete-examples.md)

### Intermediate
1. [GraphQL API](graphql.md)
2. [Vector Search](vector-search.md)
3. [Graph Traversal](graph-traversal.md)
4. [Transactions](transactions.md)
5. [Data Import/Export](data-import-export.md)

### Advanced
1. [Hybrid Search](hybrid-search.md)
2. [Advanced Topics](../advanced/) - K-Means clustering, embeddings
3. [Performance Tuning](../performance/optimization-guide.md)
4. [Custom Functions](../advanced/custom-functions.md)

## 🔍 Search Features

NornicDB provides three search methods:

1. **Vector Search** - Semantic similarity using embeddings
2. **Full-Text Search** - BM25 keyword matching
3. **Hybrid Search** - RRF fusion of both methods

See [Vector Search](vector-search.md) and [Hybrid Search](hybrid-search.md) for details.

## 📊 Common Use Cases

### Knowledge Graphs
- Store and query interconnected information
- Find relationships between concepts
- Semantic search across documents

**Example:** [Personal Knowledge Graph](complete-examples.md#3-personal-knowledge-graph)

### AI Agent Memory
- User preferences and context
- Project decisions and architecture
- Semantic memory with decay

**Example:** [AI Agent Memory System](complete-examples.md#1-ai-agent-memory-system)

### Code Knowledge Bases
- Code structure and dependencies
- Documentation search
- Pattern recognition

**Example:** [Code Knowledge Base](complete-examples.md#2-code-knowledge-base)

### Document Management
- Semantic document search
- Automatic categorization
- Related document discovery

**Example:** [Vector Search Guide](vector-search.md#quick-start)

## 🆘 Common Questions

### How do I search by meaning?
Use [Vector Search](vector-search.md) with automatic embeddings.

### How do I ensure data consistency?
Use [Transactions](transactions.md) for ACID guarantees.

### How do I query relationships?
See [Graph Traversal](graph-traversal.md) for pattern matching.

### How do I migrate from Neo4j?
Check [Data Import/Export](data-import-export.md) for compatibility.

### Where are code examples?
See [Complete Examples](complete-examples.md) for full applications.

## 📚 Related Documentation

- **[API Reference](../api-reference/)** - Function documentation
- **[Features](../features/)** - Feature-specific guides
- **[Advanced Topics](../advanced/)** - K-Means clustering, embeddings, custom functions
- **[Performance](../performance/)** - Optimization and benchmarks

## ⏭️ Next Steps

After mastering the user guides:

- **[Features](../features/)** - Explore GPU acceleration, memory decay
- **[Advanced Topics](../advanced/)** - Deep dive into internals
- **[API Reference](../api-reference/)** - Complete function reference

---

**Ready to dive in?** → **[Vector Search Guide](vector-search.md)**
