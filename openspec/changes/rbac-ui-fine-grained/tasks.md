## Execution rules

All checkboxes are implementation work, not completed by creating this plan.
UI behavior is verified with the existing Vite/React test setup
(`ui/tests`, `npm`/`pnpm` scripts) and by manual checks against a running
server with auth enabled. Backend route IDs are defined by #935; until they
exist the gated sections render disabled and their tasks stay blocked.

| Phase | Depends on | Deliverable |
| --- | --- | --- |
| 0 | None | Capture ui-grid expandable template contract and current UI baseline |
| 1 | 0 | Privilege catalog client + typed RBAC module (`ui/src/utils/rbac.ts`) |
| 2 | 0, 1 | Expandable users list (chevron + roles summary chip), no editor yet |
| 3 | 2 | Fine-grained editor component with role membership + gated privilege/rule sections |
| 4 | 3 | Dirty-state tracking, unsaved indicator and guarded collapse/navigation |
| 5 | 3, 4 | Save button + themed ConfirmSaveModal, delta summary, success/error banners |
| 6 | 2-5 | Remove checkbox/`window.confirm` legacy, wire full save flow, tests |
| 7 | 1 and #935 canonical privilege routes | Enable the gated privilege/rule sections against real persistence |

Phases 0-6 are UI work that can proceed with a fixture catalog before #935's
backend routes land. Phase 7 is the only phase blocked on #935.

## 0. Baseline and grid contract

- [ ] 0.1 Read `node_modules/@ornery/ui-grid-react` and `@ornery/ui-grid-core`
      `grid.models.d.ts` to pin how `expandableRowTemplate` / `expandableRowScope`
      are supplied from the React adapter; record the exact ref shape in
      `design.md` (correct it if the draft is wrong).
- [ ] 0.2 Add a UI fixture test that mounts `UiGrid` with `enableExpandable` and
      an `expandableRowTemplate`, expands a row, and asserts the template is
      invoked with `GridExpandableTemplateContext` carrying the right `row`/`rowIndex`.
- [ ] 0.3 Record the current `AdminUsers.tsx` role-checkbox interaction and
      `DatabaseAccess.tsx` dirty/save pattern as the visual baseline (screenshots
      or DOM snapshots) so the replacement is judged against it.

## 1. RBAC client and types

- [ ] 1.1 Add `ui/src/utils/rbac.ts` with the privilege/rule/scope types from
      the #935 contract (`PrivilegeKind`, `Grant`, `Deny`, `PropertyRule`,
      `UserPrivileges`) and a pure `computePrivilegeDelta(before, after)`.
- [ ] 1.2 Add `fetchPrivilegeCatalog`, `fetchUserPrivileges`,
      `saveUserPrivileges`, `fetchUserRoles`, `saveUserRoles` to
      `ui/src/utils/api.ts`, matching the existing `fetch`/`joinBasePath` style.
- [ ] 1.3 Add a fixture catalog module used when `GET /auth/privileges/catalog`
      returns 404, so phases 2-5 are testable before #935 routes exist.

## 2. Expandable users list

- [ ] 2.1 In `AdminUsers.tsx`, remove the `roles` checkbox column and add a
      `roles` summary chip column (read-only, opens the row).
- [ ] 2.2 Enable `enableExpandable` and supply `expandableRowTemplate`; add a
      chevron toggle and `rowIdentity`-keyed expanded state.
- [ ] 2.3 Lazy-load the user's privilege snapshot on first expansion; show a
      loading spinner in the expanded area.

## 3. Fine-grained editor

- [ ] 3.1 Add `ui/src/components/auth/UserPrivilegesEditor.tsx` rendering the
      sections in `design.md` §2 (roles, graph privileges, property rules).
- [ ] 3.2 Render the privilege matrix and rule editor only from
      `fetchPrivilegeCatalog`; when the backend has no catalog, render section 1
      (roles) live and sections 2-3 disabled with an explanatory `Alert`.
- [ ] 3.3 Use the existing `FormInput`, `Button`, `Alert`, and norse utility
      classes; no new color/typography tokens.

## 4. Dirty state

- [ ] 4.1 Track per-row `dirty` via deep compare against the loaded snapshot
      (`computePrivilegeDelta` reports empty ⇔ not dirty).
- [ ] 4.2 Show the unsaved-changes indicator and emphasis Save variant when
      dirty; aggregate unsaved count in the page header.
- [ ] 4.3 Guard row collapse and route change with the themed discard-confirm
      (not `window.confirm`).

## 5. Save + confirm

- [ ] 5.1 Add `ui/src/components/modals/ConfirmSaveModal.tsx` modeled on
      `DeleteConfirmModal`; props `isOpen`, `summary`, `saving`, `onConfirm`,
      `onCancel`.
- [ ] 5.2 On Save: compute the delta, open `ConfirmSaveModal`, then persist once
      on confirm; on success refresh the snapshot and clear dirty; on failure
      show the error and keep dirty.
- [ ] 5.3 Disable Save while `saving` and when not dirty.

## 6. Legacy removal and tests

- [ ] 6.1 Remove the `roles` checkbox branch, `availableRoles` checkbox matrix,
      immediate role-write, and `window.confirm` in `AdminUsers.tsx`.
- [ ] 6.2 Replace `window.confirm` in `DatabaseAccess.tsx` role delete with the
      themed `Modal` confirm (only if that surface is touched; otherwise record
      it as a follow-up).
- [ ] 6.3 Add component tests: dirty flag transitions, save opens confirm,
      cancel keeps dirty, confirm persists and clears dirty, server error keeps
      dirty, and disabled gated sections render the unsupported alert.

## 7. Real persistence (blocked on #935)

- [ ] 7.1 Swap the fixture catalog for `GET /auth/privileges/catalog` and enable
      sections 2-3 against `GET/PUT /auth/users/:user/privileges`.
- [ ] 7.2 Verify a saved `GRANT MATCH {*} ON GRAPH docs FOR (n:Document) WHERE
      'household' IN n.collections` round-trips and a `DENY`/`GRANT` pair shows
      the deny-wins summary in the confirm.
- [ ] 7.3 Verify two admins editing the same user see the latest snapshot and
      cannot silently overwrite each other (409/version or reload-on-save).
