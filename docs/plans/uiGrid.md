# UI Grid Integration — Instructions for Claude Code

## What to Build

Replace all hand-rolled `<table>` elements in the NornicDB admin UI with `<UiGrid>` from `@ornery/ui-grid-react`. Use inline cell editing for editable fields and custom `cellRenderer` functions for enum/dropdown/toggle cells where freeform text is inappropriate.

## Prerequisites — Run First

```bash
npm install @ornery/ui-grid-react @ornery/ui-grid-core
```

## Architecture

- **React 19** app with **Vite**, **Tailwind v4**, **Zustand** for state, **react-router-dom** for routing
- Dark theme using Norse color palette (see `tailwind.config.js` for exact hex values)
- All API calls go through `src/utils/api.ts` — do not create new API functions
- Existing page state management (useState/useCallback hooks) stays — just wire the grid into the existing data flow

## Theme Setup — `src/index.css`

Add at the top of the file (after the existing `@import "tailwindcss"` and `@config` lines):

```css
@import '@ornery/ui-grid-react/styles';
```

Add a `.nornic-grid` class block that maps the NornicDB palette to `--ui-grid-*` CSS custom properties:

```css
.nornic-grid {
  --ui-grid-surface: #141824;
  --ui-grid-border-color: #2a3247;
  --ui-grid-header-background: #1e2433;
  --ui-grid-cell-color: #f3f4f6;
  --ui-grid-muted-color: #9ca3af;
  --ui-grid-row-odd: #141824;
  --ui-grid-row-even: #1a1f2e;
  --ui-grid-row-hover: rgba(16,185,129,0.08);
  --ui-grid-accent: #10b981;
  --ui-grid-group-background: #1e2433;
  --ui-grid-radius: 8px;
  --ui-grid-shadow: none;
}
```

Every `<UiGrid>` must be wrapped in `<div className="nornic-grid">`.

## UiGrid React API

```tsx
import { UiGrid } from '@ornery/ui-grid-react';
import type { GridOptions, GridColumnDef, UiGridApi, GridCellTemplateContext } from '@ornery/ui-grid-react';

// Basic usage
<UiGrid
  options={gridOptions}
  onRegisterApi={(api) => { gridApiRef.current = api; }}
  cellRenderer={(ctx: GridCellTemplateContext) => {
    // Return React node for custom cells, or null for default rendering
    // ctx.column.name tells you which column
    // ctx.row is the data record
    // ctx.value is the resolved cell value
    return null;
  }}
/>
```

### Key GridOptions fields
- `id: string` — unique grid ID (required)
- `data: GridRecord[]` — row data array (required)
- `columnDefs: GridColumnDef[]` — column definitions (required)
- `enableSorting: boolean` — default true
- `enableFiltering: boolean` — default true
- `enableCellEdit: boolean` — grid-level edit toggle
- `enableCellEditOnFocus: boolean` — edit on focus
- `enableGrouping: boolean` — enable row grouping
- `grouping: { groupBy?: string[] }` — grouping config
- `viewportHeight: number` — grid height in px (default 560)
- `onRegisterApi: (api: UiGridApi) => void` — get API instance

### Key GridColumnDef fields
- `name: string` — column ID and default data key
- `displayName?: string` — header label
- `field?: string` — dot-path into data record
- `type?: 'string' | 'number' | 'boolean' | 'date'`
- `align?: 'start' | 'center' | 'end'`
- `enableCellEdit?: boolean` — column-level edit toggle
- `enableSorting?: boolean` — column-level sort toggle
- `enableFiltering?: boolean` — column-level filter toggle
- `cellEditableCondition?: boolean | ((ctx) => boolean)` — per-row edit guard
- `formatter?: (value, row) => string` — display formatter
- `width?: string` — CSS grid track (e.g. '200px', 'minmax(10rem, 1fr)')

### Cell Edit API (via onRegisterApi)
- `gridApi.edit.on.afterCellEdit = (row, col, newVal, oldVal) => void` — fires after edit commit

## Files to Modify (in order)

### 1. `src/index.css`
- Add `@import '@ornery/ui-grid-react/styles';` after the tailwind imports
- Add `.nornic-grid` CSS variable block (see above)

### 2. `src/components/browser/QueryResultsTable.tsx`
Replace the hand-rolled `<table>` with `<UiGrid>`. This is **read-only** display.

- Transform `cypherResult.results[0].data` rows into flat `GridRecord[]` keyed by column name
- Dynamic `columnDefs` from `cypherResult.results[0].columns`
- Leading checkbox column via cellRenderer — bind to `selectedNodeIds` Set
- Object-valued cells: use cellRenderer to render `<ExpandableCell data={cell} />`
- Row click → call `onNodeSelect` via cellRenderer click handler
- `enableCellEdit: false`, `enableSorting: true`, `enableFiltering: true`
- Keep the row count `<p>` element below the grid
- Reuse: `ExpandableCell`, `extractNodeFromResult`, `getAllNodeIdsFromQueryResults` from existing imports

### 3. `src/pages/AdminUsers.tsx`
Replace the users `<table>` (the `<div className="bg-norse-shadow...">` block containing the table) with `<UiGrid>`.

- Column defs: username (read-only), email (editable), roles (cellRenderer: multi-checkbox), status (cellRenderer: toggle), last_login (formatted date), actions (cellRenderer: Edit/Delete buttons)
- `gridApi.edit.on.afterCellEdit` → call update API for email changes
- Roles cellRenderer: render checkboxes for each `availableRoles`, on change call `PUT /auth/users/{username}`
- Status cellRenderer: toggle `disabled` boolean, on change call update API
- Actions cellRenderer: render existing `<Button>` components for Edit/Delete
- **Remove the editingUser Modal** — inline editing replaces it
- Keep the create user form section

### 4. `src/pages/DatabaseAccess.tsx`
Replace **both** tables with `<UiGrid>`:

**4a — "Access by role" table:**
- One `role` column + one column per `dbNames` entry
- Each db column cellRenderer: checkbox, checked = `getDatabasesForRole(role)` includes that db
- On toggle: call `toggleDbForRole(role, db)` and `setDirty(true)`

**4b — "Role entitlements" table** (only if `globalEntitlements.length > 0`):
- One `role` column + one column per `globalEntitlements` entry
- Each entitlement column cellRenderer: checkbox, checked = `getEntitlementsForRole(role).includes(ent.id)`
- On toggle: call `toggleEntitlementForRole(role, ent.id)`
- Disabled for admin role

Keep: Save buttons, user-defined roles CRUD section, rename modal

### 5. `src/pages/RetentionAdmin.tsx`
Replace the "Policy catalog" `<table>` with `<UiGrid>`.

- Column defs: name/id/description (cellRenderer for multi-line), category (cellRenderer: `<select>` from `RETENTION_CATEGORIES`), retention (formatter: `formatRetentionPeriod`), archive, active (cellRenderer: toggle pill), actions (cellRenderer: Edit/Delete buttons)
- `enableSorting: true`, `enableFiltering: true`
- Keep: policy create/edit modals, legal holds cards, erasure queue cards, confirmation modals

### 6. `src/pages/LifecycleAdmin.tsx`
Replace **two** tables:

**6a — "Top debt keys" table:**
- Columns: logical_key, namespace, debt_bytes (formatter: `formatBytes`, align: end), tombstone_depth (align: end), versions_to_delete (align: end)
- Read-only, sortable, filterable

**6b — "Active readers" table:**
- Columns: ReaderID, Namespace, StartTime (formatter: `formatTimestamp`)
- Read-only, sortable

### 7. `src/pages/Databases.tsx`
Replace the **config modal body** (the `<div className="space-y-4 max-h-[70vh]...">` inside the config modal) with `<UiGrid>`.

- Map `configKeys` to GridRecord[] with: key (stripped prefix), rawKey, type, category, value, useDefault, effectiveDefault
- Columns: key (read-only), value (editable, cellEditableCondition: `!row.useDefault`), useDefault (cellRenderer: checkbox), effectiveDefault (read-only, muted)
- Boolean-type values: cellRenderer renders checkbox/toggle instead of text input
- `enableGrouping: true`, `grouping: { groupBy: ['category'] }` — groups by config category
- `enableSorting: true`, `enableFiltering: true`
- `viewportHeight: 400` — fits inside modal
- `afterCellEdit`: call `setConfigFormValue(row.rawKey, newValue)`
- Keep: Cancel/Save buttons below the grid

## Patterns to Follow

- Wrap every `<UiGrid>` in `<div className="nornic-grid">` for theming
- All grids need a unique `id` string in their `gridOptions`
- Use `useMemo` for `gridOptions` and `columnDefs` to avoid unnecessary re-renders
- cellRenderer returns `React.ReactNode` — return `null` to use default grid rendering
- The cellRenderer receives a single `GridCellTemplateContext` argument. Use `ctx.column.name` to dispatch to the right renderer per column.
- For action buttons in cellRenderers, use `e.stopPropagation()` to prevent cell focus/edit

## Verification

After all changes:
1. `npm run build` must pass with zero TypeScript errors
2. Visual check: all grids render with dark Norse theme
3. Sorting/filtering works on all grids
4. Inline editing works on AdminUsers (email), Databases config modal (values)
5. Checkbox/toggle cellRenderers work and trigger their API calls
6. No regressions: create user, delete user, save database config, generate tokens all still work
