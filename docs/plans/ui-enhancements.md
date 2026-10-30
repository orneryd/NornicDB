# UI Enhancements Plan

## Objective

Ship a coherent Browser experience for graph investigation, historical reconstruction, and reliable day-to-day operations.

This plan covers:
1. Neighborhood Graph Explorer
2. Graph reconstruction with `asOf(...)`
3. UI architecture and quality improvements (organization, determinism, tests)

## Success Criteria

Users must be able to:
- Run Cypher or semantic search and open results in Graph Explorer.
- Expand graph neighborhoods interactively with predictable controls.
- Reconstruct graph state at a timestamp via `asOf(...)`.
- Compare current and historical state in one view with deterministic diff markers.
- Use workflows that are stable, test-covered, and regression-resistant.

## UX Model

Browser workspace tabs:
- `Cypher`
- `Semantic Search`
- `Graph Explorer`

Graph Explorer layout:
- Top toolbar: depth, filters, layout, `asOf`, compare, refresh.
- Center: graph canvas.
- Right inspector: selected node/edge/path details.
- Bottom diagnostics: node/edge counts, source, timings, truncation/warnings.

## Workstream A: Neighborhood Graph Explorer

### Required Behavior

- Open graph from query result rows, search result rows, or selected node IDs.
- Expand by hops incrementally (`+1`, `+2`, reset).
- Apply label and relationship-type filters without losing current selection.
- Pin/focus nodes and preserve stable selection across refresh when IDs still exist.
- Support deterministic layout modes (`force`, `radial`, `hierarchy`) with explicit user choice.

### API Contract

Use normalized graph payloads from graph endpoints:
- `POST /nornicdb/graph/neighborhood`
- `POST /nornicdb/graph/expand`
- `POST /nornicdb/graph/path`

```ts
type GraphPayload = {
  nodes: Array<{
    id: string
    labels: string[]
    properties: Record<string, unknown>
    score?: number
    status?: "live" | "added" | "removed" | "changed"
  }>
  edges: Array<{
    id: string
    source: string
    target: string
    type: string
    properties?: Record<string, unknown>
    semantic?: boolean
    status?: "live" | "added" | "removed" | "changed"
  }>
  meta: {
    database: string
    generated_from: "query" | "search" | "node" | "diff"
    depth?: number
    as_of?: string
    compare_to?: string
    node_count: number
    edge_count: number
    truncated?: boolean
  }
}
```

## Workstream B: `asOf(...)` Reconstruction + Diff

### Required Behavior

- Timestamp picker for historical state reconstruction.
- Server-side execution of `asOf(...)` semantics (no client simulation).
- Optional comparison mode (`current` vs `asOf(t)` or `asOf(t1)` vs `asOf(t2)`).
- Deterministic visual markers:
  - `added` (green)
  - `removed` (red)
  - `changed` (amber)

### API Requirements

- `POST /nornicdb/graph/temporal`
- `POST /nornicdb/graph/diff`
- All graph fetch operations accept optional `as_of` and `compare_to`.

## Workstream C: UI Architecture + Reliability

### Code Organization

Keep Browser page orchestration-only. Move graph logic into dedicated modules:
- `ui/src/components/browser/graph/GraphPanel.tsx`
- `ui/src/components/browser/graph/GraphToolbar.tsx`
- `ui/src/components/browser/graph/GraphCanvas.tsx`
- `ui/src/components/browser/graph/GraphInspector.tsx`
- `ui/src/utils/graph/*` for adapters/layout/transform helpers

### State Model

Define explicit, typed graph state:
- `graphData`
- `graphLoading`
- `graphError`
- `graphSelection`
- `graphFilters`
- `graphAsOf`
- `graphCompareTo`

### UX/Accessibility Baseline

- Keyboard support for selection and expansion actions.
- Consistent loading/error/empty states across tabs.
- Deterministic truncation messaging when payload limits are hit.
- Bidirectional table/graph highlighting where IDs are shared.

## Implementation Sequence

1. Add Graph Explorer tab and query/search handoff wiring.
2. Add typed graph API methods in `ui/src/utils/api.ts`.
3. Implement Graph toolbar controls (depth/filter/layout/temporal).
4. Implement graph canvas interactions (expand/focus/pin/select).
5. Implement inspector panels for node/edge/path details.
6. Implement `asOf(...)` and compare workflows + diff rendering.
7. Add diagnostics strip and operational warnings.
8. Refactor Browser into orchestration + focused subcomponents.

## Testing Plan

### Unit Tests

- Graph payload normalization and adapter correctness.
- Filter, selection, and temporal state reducers.
- Diff status assignment and rendering model builders.
- Request builders for `as_of` and compare flows.

### Component Tests

- Toolbar state transitions and callback contracts.
- Graph panel loading/error/empty determinism.
- Inspector detail rendering for node/edge/path.
- Tab transitions without unexpected graph state loss.

### Integration Tests

- Cypher result -> Graph Explorer path.
- Semantic search result -> Graph Explorer path.
- Multi-step neighborhood expansion + filters.
- `asOf(...)` reconstruction and current-vs-historical diff flow.

### Regression Rule

Any graph or temporal bug must include:
1. Failing test reproducer
2. Minimal fix
3. Passing regression test

## Acceptance Criteria

- Graph Explorer is available and usable from both Cypher and Semantic Search.
- Neighborhood expansion is incremental, deterministic, and filter-aware.
- `asOf(...)` reconstruction uses server semantics and returns expected graph state.
- Diff mode shows consistent added/removed/changed markers.
- Browser remains responsive and deterministic for large payload scenarios.
- New handwritten UI logic in these workstreams maintains high coverage targets.

## Out of Scope

- Replacing the graph rendering engine entirely.
- Redesigning server-side graph algorithms.
- Introducing non-`asOf(...)` temporal semantics.

## Deliverables

- Complete Graph Explorer experience (tab + interactions + inspector).
- Temporal reconstruction and diff UX in Browser.
- Refactored UI structure with clearer ownership boundaries.
- Automated unit/component/integration coverage for new behavior.
- Updated user docs with usage examples and visuals.
