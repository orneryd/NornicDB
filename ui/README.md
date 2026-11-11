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

## Navigation and reproducible views

Workspace, database management, operations, access and demos have persistent labeled navigation. Resource IDs and editable states use paths; tabs, filters, disclosures and grid view state use query parameters. See [interaction architecture](review/interaction-architecture.md) for the intent inventory, route contract and known UI Grid limitations.

## Generated demos

Schema management is under **Manage → Schema**. **Operations → Backup & restore** uses existing server APIs and server filesystem paths; restore affects the entire server. Knowledge policies include optional target/profile/review guidance with curve previews. **Access → Account & tokens** lets every authenticated role create a token with their existing permissions and obtain MCP connection instructions.

Open Demos and choose a capability. Click **Seed new demo database** to create a disposable database; opening the setup page alone creates nothing. Progress is shown during setup. Interrupted setup can resume. Delete the generated database through Databases when finished.

Pivot analysis seeds 25,000 orders across 1,000 accounts. It supports dimension changes, weighted averages, calculated revenue per unit/revenue shares, and independently expandable order detail grids. Formula definitions are visible. The original Galaxy, Fleet and Code graph experiences remain separate pages.

## Verification

Use Node 24 or newer for the source-based unit tests:

```bash
npm ci
npm test
npx playwright install chromium
npm run test:browser
npm run build
npm run inventory
```

Browser tests use API fixtures and synthetic records. CI uses bundled Chromium; local tests use Chrome. Set `CI=true` to use bundled Chromium locally. For a running development server, set `VITE_PROXY_TARGET` to the database origin in an untracked `.env.local` file. Do not place credentials in Vite environment variables or URLs.
