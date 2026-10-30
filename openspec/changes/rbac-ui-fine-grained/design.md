# Design: fine-grained RBAC administration UI

## Current state

| File | Responsibility |
| --- | --- |
| `ui/src/pages/AdminUsers.tsx` | Users `UiGrid` (username, email, **roles checkboxes**, status, last_login, actions). Role toggles call `handleUpdateUser` immediately; delete uses `window.confirm`. |
| `ui/src/pages/DatabaseAccess.tsx` | Roles list, per-role database allowlist + read/write matrix, role entitlements. Already has `dirty`/`saving` state, an explicit `Save` button, and `Modal`-based create/rename/delete. |
| `ui/src/components/common/Modal.tsx` | Reusable themed modal (norse palette, Escape/backdrop close, size variants). |
| `ui/src/components/modals/DeleteConfirmModal.tsx`, `RegenerateConfirmModal.tsx` | Themed confirm dialogs; the pattern to copy for a save-confirm dialog. |
| `ui/src/utils/api.ts` | HTTP client (`fetch`, `joinBasePath`, credentials); auth helpers only (`checkAuth`, login/logout). No RBAC privilege client. |
| `pkg/auth/*.go` | Backend roles (`roles.go`), allowlist (`allowlist.go`), per-DB read/write (`privileges.go`), entitlements (`entitlements.go`, `role_entitlements.go`). No #935 fine-grained model yet. |

## Target shape

### 1. Users list becomes expandable

`AdminUsers.tsx` keeps compact columns — `username`, `email`, `status`,
`last_login`, `actions` — and drops the `roles` checkbox column. A new
`roles` summary column renders a chip list (role names) and, together with a
row chevron, toggles the expandable detail.

`ui-grid` support (from `@ornery/ui-grid-core`):

- `GridOptions.enableExpandable = true`
- `GridOptions.expandableRowTemplate` / `expandableRowScope`
- `GridExpandableTemplateContext` supplies `row`, `rowIndex`, `expanded`

The React binding accepts `cellRenderers` by column name today; the expandable
template is supplied through `options` (`expandableRowTemplate` as a
`GridTemplateRefLike`, matching how `cellTemplate` is wired). Confirm the
React adapter's exact ref shape during implementation (string ref, component
ref, or `cellRenderers`-style map) and keep the editor component as a plain
render function taking `GridExpandableTemplateContext`.

### 2. Per-user fine-grained editor (the expanded row)

New component `ui/src/components/auth/UserPrivilegesEditor.tsx`, rendered as
the expandable template for `AdminUsers`. It is **self-contained and
dirty-tracked per user** so one open row does not block another.

Sections, driven by the #935 contract (`traverse`/`read`/`match`/`write`,
`deny` beats `grant`, label/rel-type/property scope, property rules):

1. **Roles** — add/remove role membership as removable chips (replaces the
   checkbox column). Role names come from `/auth/roles`.
2. **Graph privileges** — a privilege matrix: one row per grantable privilege
   (`TRAVERSE`, `READ`, `MATCH`, `CREATE`, `DELETE`, `SET LABEL`, `REMOVE
   LABEL`, `SET PROPERTY`, `MERGE`, `WRITE`), columns `GRANT` / `DENY` /
   unset, scoped to a selected database, label/rel-type and property set.
3. **Property-based rules** — per rule: privilege (`TRAVERSE`/`READ`/`MATCH`),
   element pattern `FOR (n:Label)`, operator (`=`, `<>`, `<`, `<=`, `>`, `>=`,
   `IS [NOT] NULL`, `IN`, `value IN n.p`), property and value, `GRANT`/`DENY`.

Until the backend route exists, sections 2 and 3 render from a documented
fixture/contract and are marked "not yet persisted" with the controls disabled
and an explanatory `Alert`; section 1 stays live against `/auth/roles`.

### 3. Dirty state

Per-row editor holds a `dirty` flag that is `true` when any field differs from
the loaded snapshot (deep compare of roles, privileges and rules). Rules:

- The Save button is enabled only when `dirty`; it carries a success/emphasis
  variant when dirty (`variant={dirty ? 'success' : 'secondary'}`).
- A "You have unsaved changes" indicator renders at the top of the open editor.
- Collapsing the row or navigating away with `dirty` true shows a themed
  confirm ("Discard unsaved changes?") — never `window.confirm`.
- The parent page header shows an aggregate unsaved count when any row is
  dirty.

### 4. Save + confirm dialogue

New `ui/src/components/modals/ConfirmSaveModal.tsx` (modeled on
`DeleteConfirmModal`):

- Props: `isOpen`, `summary` (the computed grant/deny delta), `saving`,
  `onConfirm`, `onCancel`.
- Themed norse palette, icon, primary/confirm action in the nornic accent and
  a Cancel action, matching `DeleteConfirmModal`/`RegenerateConfirmModal`.

Save flow:

1. Editor computes the delta (added / changed / removed grants, denies, rules
   and role membership).
2. `ConfirmSaveModal` opens with that summary.
3. On confirm, the editor calls the save endpoint once, updates the loaded
   snapshot on success, clears `dirty`, and shows the existing `Alert` success
   banner; on failure it surfaces the server error and keeps `dirty` true.

### 5. API contract (backend #935)

Add client functions to `ui/src/utils/api.ts` (mirroring existing `fetch`
style) and keep the shapes in one typed module
`ui/src/utils/rbac.ts`:

- `GET /auth/privileges/catalog` → the privilege kinds, scope grammar and
  property-rule operators the UI renders (so the UI does not hardcode the
  privilege model).
- `GET /auth/users/:user/privileges` → the user's effective + directly
  granted privileges and rules.
- `PUT /auth/users/:user/privileges` → persist the edited set
  (role membership, grants/denies, rules).
- `GET /auth/users/:user/roles` / `PUT` (or reuse existing user update).

These are the shapes the plan proposes to the #935 backend work; the UI keys
on `privileges/catalog` so it renders exactly what the backend supports and
falls back to the coarse `/auth/roles` surface when the catalog is absent.

## Deletion targets

- `AdminUsers.tsx`: the `roles` checkbox column and `renderUserCell` roles
  branch; the `availableRoles` checkbox matrix; immediate `handleUpdateUser`
  on role toggle.
- `window.confirm` call sites in `AdminUsers.tsx` (delete) and, where the
  coarse surface is touched, `DatabaseAccess.tsx` role delete — replaced by
  the themed `Modal` confirm.

## Compatibility and risks

- **No legacy dual editor**: the checkbox role UI is removed, not hidden
  behind a flag. The expandable editor is the only role surface on the users
  list.
- **Backend absence**: rendering must not fabricate authority. Sections gated
  on the #935 contract stay disabled with an explicit message until their
  routes persist data; role membership remains functional via the existing
  route.
- **ui-grid expandable ref**: the React adapter's expandable template shape is
  the main integration risk; verify it in `node_modules/@ornery/ui-grid-react`
  before building the editor and add a fixture test asserting the template is
  invoked with the expected `GridExpandableTemplateContext`.
- **Performance**: privilege catalog and per-user privilege payloads are
  small; fetch them lazily on row expansion, not for the whole grid up front.
