# Graph Contract Foundation

This PR adds a UI-local contract layer for future graph features under `src/graph/`.

## Scope

- Typed request and response models for graph payloads
- Request builders for the checked-in backend graph endpoints under `/nornicdb/graph/{database}/*`
- Capability derivation from per-database privileges first, then global role entitlements
- Pure unit tests for the contract and capability logic

## Current Status

This foundation now aligns to the checked-in backend contract used for PR #68 rather than the older UI plan document.
The contract source of truth for this layer is the backend route/request/response shape already checked into the repository, including:

- `pkg/server/server_graph.go`
- `docs/api-reference/openapi.yaml`
- `pkg/server/server_router.go`

Backend route mapping represented by this foundation layer:

- `POST /nornicdb/graph/{database}/neighborhood`
- `POST /nornicdb/graph/{database}/expand`
- `POST /nornicdb/graph/{database}/path`
- `POST /nornicdb/graph/{database}/temporal`
- `POST /nornicdb/graph/{database}/diff`

## Contract Notes

- `database` is routing-only for the UI builders and is not serialized into the JSON request body.
- `neighborhood` and `expand` share the backend body shape: `node_ids`, `existing_node_ids`, `existing_edge_ids`, `depth`, `limit`, `labels`, `relationship_types`, and optional `as_of`.
- `path` uses backend field names `source_node_id` and `target_node_id`, plus `limit`, `labels`, `relationship_types`, and optional `as_of`.
- `temporal` requires `as_of` and accepts `node_ids`, `labels`, and `relationship_types`.
- `diff` accepts `node_ids`, `labels`, `relationship_types`, `as_of`, and optional `compare_to`.
- Response typing mirrors the backend graph payload shape with top-level `nodes`, `edges`, and `meta`.
- Diff status enums are limited to `added`, `removed`, and `changed`.

## Compatibility Notes

- Existing database-aware Cypher calls in `src/utils/api.ts` are unchanged.
- Existing DB-aware search and similar calls in `src/utils/api.ts` are unchanged.
- This PR does **not** add backend implementations, route registration, or UI rewrites.
- This remains foundation-only work. No browser UI integration is added here.
