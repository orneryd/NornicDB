# NornicDB Browser

A modern web UI for NornicDB - the Neo4j-compatible graph database with GPU-accelerated vector search.

## Features

- **Cypher Query Editor** - Execute Cypher queries with syntax highlighting and history
- **Semantic Search** - Full-text and vector similarity search
- **Node Browser** - View node details, labels, and properties
- **Find Similar** - Discover semantically related nodes using embeddings
- **Live Stats** - Real-time connection status and database metrics
- **Authentication** - Supports none, dev mode, and OAuth

## Quick Start

```bash
# Install dependencies
npm install

# Start development server (port 5174)
npm run dev

# Build for production
npm run build
```

## Configuration

The UI proxies requests to NornicDB server at `localhost:7475`. Configure in `vite.config.ts`:

```typescript
proxy: {
  '/api': { target: 'http://localhost:7475' },
  '/db': { target: 'http://localhost:7475' },
  '/nornicdb': { target: 'http://localhost:7475' },
  '/auth': { target: 'http://localhost:7475' },
}
```

## Theme

The UI uses a Norse-inspired dark theme with NornicDB's signature emerald green accent:

- **Background**: `#0a0e1a` (Norse Night)
- **Cards/Panels**: `#141824` (Norse Shadow)
- **Primary**: `#10b981` (Nornic Green)
- **Accent**: `#4a9eff` (Frost Ice)
- **Highlight**: `#d4af37` (Valhalla Gold)

## Authentication Modes

1. **None** (`--no-auth`): Skip login, direct access to browser
2. **Dev Mode**: Username/password form (configured via server flags)
3. **OAuth**: SSO with external providers (enterprise)

## Usage

### Cypher Queries

Execute Neo4j-compatible Cypher queries:

```cypher
# Count all nodes
MATCH (n) RETURN count(n)

# Find nodes by label
MATCH (n:File) RETURN n LIMIT 10

# Search properties
MATCH (n) WHERE n.title CONTAINS 'typescript' RETURN n
```

### Semantic Search

Search nodes by meaning, not just keywords:

- Enter natural language queries
- Results ranked by BM25 + vector similarity (RRF)
- Click any result to view details
- Use "Find Similar" to discover related content

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/status` | GET | Database stats |
| `/nornicdb/search` | POST | Full-text + vector search |
| `/nornicdb/similar` | POST | Find similar nodes |
| `/db/nornicdb/tx/commit` | POST | Execute Cypher |

## License

MIT
