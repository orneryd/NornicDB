## Why

[#935](https://github.com/orneryd/NornicDB/issues/935) adds Neo4j's fine-grained
role-based access control: graph privileges (`TRAVERSE`, `READ {props}`,
`MATCH {props}`, the write privileges) scoped to labels, relationship types and
properties, plus property-based read rules. Today the admin UI can only toggle
the coarse built-in roles `admin`/`editor`/`viewer` per user
(`ui/src/pages/AdminUsers.tsx`) and per-role database read/write switches
(`ui/src/pages/DatabaseAccess.tsx`). An operator cannot express "this user may
MATCH `Document` nodes but not read `Document.body`", so the new privilege
model has no usable surface.

## What Changes

- Turn each row of the **users list** into an **expandable row** using
  `ui-grid`'s `enableExpandable` + `expandableRowTemplate`, replacing the
  `roles` checkbox column with a compact roles summary chip that expands the
  detail.
- The expanded detail is a **fine-grained RBAC editor** for that user:
  role membership, per-database graph privileges (traverse / read / match /
  write) scoped to labels, relationship types and properties, and
  property-based rules. The editor renders only what the #935 privilege
  contract exposes.
- The editor is **dirty-tracked**: unsaved changes surface a visual dirty
  state (highlighted Save button, "unsaved changes" indicator, guarded
  navigation) and are not written until saved.
- **Save** is explicit and goes through a themed **confirm dialogue**
  (`ui/src/components/common/Modal.tsx` + a new `ConfirmSaveModal` in
  `ui/src/components/modals/`), not `window.confirm`. The confirm summarizes
  exactly which grants/denies are added, changed and removed.
- Reuse the existing `DatabaseAccess` dirty/save, `Alert`, `Button`, `Modal`
  and norse-theming patterns so the new surface is visually consistent.
- **BREAKING (UI)**: remove the `admin`/`editor`/`viewer` checkbox column and
  the immediate-write behavior on role toggles; a role change is now a pending
  edit that must be saved. No dual (checkbox + fine-grained) role editor is
  retained.

## Capabilities

### New Capabilities

- `rbac-administration-ui`: an expandable, dirty-tracked, confirming
  fine-grained RBAC editor on the users list.

### Modified Capabilities

None in the active capability inventory. `#935` itself defines the backend
privilege capabilities; this change only adds their administration surface.

## Impact

Baseline inspected: `main` at plan time (`ac455353` ancestor with
`feat/composite-index-unification` merged for index work, unrelated here).

Existing building blocks include `ui/src/pages/AdminUsers.tsx` (users grid +
role checkboxes + immediate `handleUpdateUser`), `ui/src/pages/DatabaseAccess.tsx`
(dirty state, `handleSave`, `Modal`-based role create/rename/delete),
`ui/src/components/common/Modal.tsx`, `ui/src/components/modals/*` (themed
confirm dialogs), the `UiGrid` React binding with `cellRenderers` and
`expandableRowTemplate`, and the auth HTTP surface (`/auth/roles`,
`/auth/access/databases`, `/auth/access/privileges`, `/auth/entitlements`,
`/auth/role-entitlements`).

**The fine-grained privilege model and its routes do not exist yet** — #935 is
open. This plan therefore defines the UI against a documented contract and
stages rendering behind that contract. Nothing here is implemented by this
planning change.
