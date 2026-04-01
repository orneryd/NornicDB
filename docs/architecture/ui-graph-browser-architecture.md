# UI Graph Browser Architecture

This document describes the architecture of the Browser UI graph features, including the graph API contract layer, capability model, and Browser shell state conventions.

It serves as the foundational pinning document for current and future graph-related UI work in `ui/`.

## Overview

The Browser UI exposes three tabs:

- **Cypher** — direct query execution
- **Semantic Search** — vector + BM25 hybrid search
- **Graph Explorer** — neighborhood traversal and graph inspection

Graph Explorer data flows from Cypher query results or Semantic Search results via the "Open in Graph" handoff, or directly from a node ID in the URL.

## Source of Truth

Graph API routes and request/response shapes are defined in the backend and documented in:

- `docs/api-reference/openapi.yaml` — OpenAPI spec (see Graph tag)
- `pkg/server/server_graph.go` — Go handler implementations
- `pkg/server/server_router.go` — route registration

> Note: `server_graph.go` and the `/nornicdb/graph/*` routes are introduced in PR #68 and will be present on `main` once that PR merges.

## Graph API Endpoints

All graph endpoints are DB-qualified via path segment:

```
POST /nornicdb/graph/{database}/neighborhood
POST /nornicdb/graph/{database}/expand
POST /nornicdb/graph/{database}/path
POST /nornicdb/graph/{database}/temporal
POST /nornicdb/graph/{database}/diff
```

Authentication uses the same Bearer token / Basic Auth / Cookie approach as the rest of the API.

## Request Contract

All graph request bodies are JSON. The `database` segment is routing-only and is not repeated in the body.

### Neighborhood / Expand

```ts
{
  node_ids: string[]           // required seed nodes
  existing_node_ids?: string[] // advisory: already-visible nodes
  existing_edge_ids?: string[] // advisory: already-visible edges
  depth?: number               // traversal hops (default 1)
  limit?: number               // max results
  labels?: string[]            // node-label filter
  relationship_types?: string[] // edge-type filter
  as_of?: string               // ISO-8601; neighborhood rejects historical use
}
```

`expand` shares this body shape with `neighborhood`.

### Path

```ts
{
  source_node_id: string       // required
  target_node_id: string       // required
  limit?: number
  labels?: string[]
  relationship_types?: string[]
  as_of?: string               // historical path not yet supported server-side
}
```

### Temporal

```ts
{
  node_ids: string[]           // required
  as_of: string                // required ISO-8601 timestamp
  labels?: string[]
  relationship_types?: string[]
}
```

### Diff

```ts
{
  node_ids: string[]           // required
  as_of: string                // required: target state timestamp
  compare_to?: string          // optional: baseline timestamp; omit to compare against current
  labels?: string[]
  relationship_types?: string[]
}
```

## Response Contract

All endpoints return the same `GraphPayload` shape:

```ts
{
  nodes: Array<{
    id: string
    labels: string[]
    properties: Record<string, unknown>
    score?: number
    status?: "added" | "removed" | "changed"
  }>
  edges: Array<{
    id: string
    source: string
    target: string
    type: string
    properties?: Record<string, unknown>
    semantic?: boolean
    status?: "added" | "removed" | "changed"
  }>
  meta: {
    database: string
    generated_from: "neighborhood" | "expand" | "path" | "temporal" | "diff"
    depth?: number
    as_of?: string
    compare_to?: string
    node_count: number
    edge_count: number
    truncated: boolean
  }
}
```

`status` fields are only present in diff payloads. `live` is not emitted.

## UI Layer Structure

```
ui/src/graph/
  types.ts           # typed request/response/meta models
  requests.ts        # DB-qualified request builder functions
  capabilities.ts    # privilege/entitlement-aware capability derivation
  viewModel.ts       # client-side filter/display model helpers

ui/src/utils/
  api.ts             # graph API client methods (neighborhood, expand, path, temporal, diff)
  browserUrlState.ts # URL/query-param helpers for Browser tab, database, and graph handoff params

ui/src/components/browser/
  GraphExplorerPanel.tsx  # Graph Explorer tab panel
```

## Capability Model

Graph capabilities are derived per-database from:

1. Per-database privilege matrix (wins first if a matching entry exists)
2. Global role entitlements as fallback

```ts
deriveGraphCapabilities({
  role,
  database,
  privilegesMatrix,   // from /auth/access/privileges
  roleEntitlements,   // from /auth/role-entitlements
  featureFlags,       // optional overrides
})
```

Default feature flag state:
- `neighborhood` — enabled
- `expand` — enabled
- `path` — enabled
- `temporal` — disabled (requires explicit opt-in)
- `diff` — disabled (requires explicit opt-in; coupled to temporal history)
- `mutate` — disabled

## Browser URL State

The Browser shell syncs state to query params for reproducibility:

| Param | Values | Meaning |
|-------|--------|---------|
| `database` | any DB name | active database |
| `tab` | `query` \| `search` \| `graph` | active Browser tab |
| `graph` | node IDs (comma-separated) | seed nodes for graph handoff |
| `graphSource` | node ID | path source node |
| `graphTarget` | node ID | path target node |
| `graphAsOf` | ISO-8601 | temporal/diff as-of timestamp |
| `graphCompareTo` | ISO-8601 | diff compare-to timestamp |

Deep-link URLs are shareable and restore state on page load.

## Conventions

- Always pass `database` explicitly to graph API methods; do not rely on ambient global state.
- Use `deriveGraphCapabilities` to gate UI features rather than checking raw role strings.
- Keep graph state self-contained in `GraphExplorerPanel`; do not hoist it into the global Zustand store unless cross-tab coordination is genuinely needed.
- Prefer deterministic rendering: given the same graph payload and filter state, the output should always be identical.
- Do not emit color-only diff or warning states; use shape and text alongside color.
