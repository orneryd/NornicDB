# NornicDB Browser

A modern web UI for NornicDB - the Neo4j-compatible graph database with GPU-accelerated vector search.

## Features

- **Cypher Query Editor** - Execute Cypher queries with syntax highlighting, history, and autocomplete
- **Semantic Search** - Full-text and vector similarity search with RRF-ranked results
- **Graph Explorer** - Interactive visual graph canvas with neighborhood expansion, path finding, temporal queries, and graph diff
- **Node Details** - View and edit node properties and labels inline
- **Find Similar** - Discover semantically related nodes using embeddings
- **Node Deletion** - Delete nodes directly from the browser
- **Multi-Database Support** - Switch between databases from the header
- **MVCC Lifecycle Admin** - Manage pruning schedules, pause/resume lifecycle, and inspect debt
- **Live Stats** - Real-time connection status and database metrics
- **Authentication** - Supports none, dev mode, and OAuth

## Quick Start

```bash
# Install dependencies
npm install

# Start development server (port 5174, proxies to backend on 7474)
npm run dev

# Build for production
npm run build

# Run tests
npm test
```

## Configuration

### Dev Proxy

The Vite dev server proxies all API requests to the NornicDB backend at `http://localhost:7474`. This is configured in `vite.config.ts`:

```typescript
proxy: {
  '/api':      { target: 'http://localhost:7474', rewrite: path => path.replace(/^\/api/, '') },
  '/db':       { target: 'http://localhost:7474' },
  '/nornicdb': { target: 'http://localhost:7474' },
  '/auth':     { target: 'http://localhost:7474' },
  '/admin':    { target: 'http://localhost:7474' },
  '/health':   { target: 'http://localhost:7474' },
  '/status':   { target: 'http://localhost:7474' },
}
```

### Base Path (Reverse Proxy Deployments)

If NornicDB is served behind a reverse proxy at a sub-path, set `VITE_BASE_PATH` at build time:

```bash
VITE_BASE_PATH=/nornic npm run build
```

The production binary reads and rewrites asset paths automatically when serving the embedded UI.

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

Execute Neo4j-compatible Cypher queries with autocomplete:

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
- Use **Find Similar** to discover related content (requires node to have an embedding)

### Graph Explorer

Visualize and traverse the graph interactively:

- Click **Neighborhood** on any node to load its direct connections
- Use the left-panel controls to expand nodes, find paths between nodes, run temporal queries, or diff two graph states
- Click any node in the canvas to open its details panel
- Multi-select nodes for bulk operations

**Layout modes** — Switch between three deterministic canvas layouts using the **Layout** selector in the Display filters panel:

| Layout | Description |
|---|---|
| Radial | Focus nodes at the center; neighbors arranged in concentric rings (default) |
| Grid | All nodes placed in a degree-sorted grid — useful for large flat graphs |
| Hierarchy | Nodes ranked top-to-bottom by connection degree; hubs at the top, leaves at the bottom |

**Diff and temporal** — When using _As-of snapshot_ or _Diff_ request modes, node and edge status is indicated both by color and by a text symbol in the canvas (`+` added, `−` removed, `~` changed) so the information is accessible without relying on color alone. The Node list and Edge list panels also display a text badge per element.

### Property Editing

Click **Edit** in the Node Details panel to modify properties inline. Read-only system properties (`has_embedding`, `embedding_model`, etc.) are shown but cannot be changed.

### MVCC Lifecycle Admin

Available under **Admin** for users with admin privileges:

- View current prune debt and lifecycle status per database
- Pause/resume the lifecycle worker
- Set custom prune schedules
- Trigger manual prune runs

## API Endpoints

| Endpoint | Method | Description |
|---|---|---|
| `/health` | GET | Health check |
| `/status` | GET | Database stats |
| `/nornicdb/search` | POST | Full-text + vector hybrid search |
| `/nornicdb/similar` | POST | Find similar nodes by embedding |
| `/db/{name}/tx/commit` | POST | Execute Cypher (implicit transaction) |
| `/nornicdb/graph/{db}/neighborhood` | POST | Load node neighborhood |
| `/nornicdb/graph/{db}/expand` | POST | Expand selected nodes |
| `/nornicdb/graph/{db}/path` | POST | Find path between nodes |
| `/nornicdb/graph/{db}/temporal` | POST | Temporal graph queries |
| `/nornicdb/graph/{db}/diff` | POST | Graph diff between states |
| `/admin/databases/{db}/mvcc/status` | GET | MVCC lifecycle status |
| `/admin/databases/{db}/mvcc/debt` | GET | Prune debt |
| `/admin/databases/{db}/mvcc/prune` | POST | Trigger prune |
| `/admin/databases/{db}/mvcc/pause` | POST | Pause lifecycle |
| `/admin/databases/{db}/mvcc/resume` | POST | Resume lifecycle |
| `/admin/databases/{db}/mvcc/schedule` | POST | Set prune schedule |
| `/auth/token` | POST | Authenticate |
| `/auth/me` | GET | Current user |

## Production Build

The compiled `ui/dist/` directory is embedded directly into the NornicDB binary at build time via `//go:embed all:dist`. No separate static file server is needed — the backend serves the SPA and all API routes on the same port (default `7474`).

```bash
# Build UI then binary
cd ui && npm run build && cd ..
go build -o nornicdb ./cmd/nornicdb
```

## License

MIT
