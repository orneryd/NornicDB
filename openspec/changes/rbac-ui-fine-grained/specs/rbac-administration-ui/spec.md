# RBAC administration UI

## Purpose

Observable UI requirements for administering fine-grained RBAC from the users
list, per #935. The UI is the surface for role membership, graph privileges
(traverse/read/match/write) scoped to labels, relationship types and
properties, and property-based rules, with dirty state and a confirming save.

## Requirements

### Requirement: Expandable users list

The users list SHALL render each user as an expandable row whose detail hosts
the fine-grained RBAC editor, replacing the coarse role checkboxes.

#### Scenario: Expanding a row shows the editor

- **WHEN** an admin opens the users list and selects the expand control on a user row
- **THEN** the row expands in place and the fine-grained RBAC editor for that
  user renders beneath it, inside the grid's expandable template

#### Scenario: Multiple rows expand independently

- **WHEN** an admin expands user A and then expands user B
- **THEN** A's editor and B's editor are both open and maintain independent
  dirty/save state

#### Scenario: Compact columns remain

- **WHEN** the users list renders
- **THEN** the columns are username, email, roles summary, status, last login
  and actions; there is no `admin`/`editor`/`viewer` checkbox column

### Requirement: Fine-grained editor

The editor SHALL render role membership and the graph-privilege and
property-rule surface described by the privilege catalog.

#### Scenario: Privilege matrix renders from the catalog

- **WHEN** the catalog exposes `TRAVERSE`, `READ`, `MATCH` and the write
  privileges
- **THEN** the editor renders one grant/deny control per catalog privilege,
  scoped to database, label/relationship-type and property set

#### Scenario: Property rule form

- **WHEN** an admin adds a property-based rule
- **THEN** the editor collects privilege, `FOR (n:Label)` pattern, operator,
  property, value and grant/deny, and displays it in the rules list

#### Scenario: Unsupported backend

- **WHEN** `GET /auth/privileges/catalog` is absent (404)
- **THEN** role membership remains editable, and the graph-privilege and
  property-rule sections render disabled with an explicit "not supported by
  this server" alert; no control is presented as authoritative

### Requirement: Dirty state

The editor SHALL track whether any field differs from the last persisted
snapshot and surface that state without writing.

#### Scenario: Unsaved change marks dirty

- **WHEN** an admin changes a role, grant, deny or rule without saving
- **THEN** the editor shows an unsaved-changes indicator and the Save control
  becomes enabled with an emphasis variant

#### Scenario: Reverting clears dirty

- **WHEN** an admin reverts the only unsaved change
- **THEN** the unsaved indicator disappears and Save becomes disabled again

#### Scenario: Leaving with unsaved changes

- **WHEN** an admin collapses the row or navigates away while the editor is dirty
- **THEN** the UI shows a themed discard-confirmation dialogue (never
  `window.confirm`); choosing to stay keeps the unsaved edits

### Requirement: Confirming save

Saving SHALL be explicit and SHALL present a themed confirmation summarizing
the delta before persisting.

#### Scenario: Save opens confirmation

- **WHEN** an admin presses Save with unsaved changes
- **THEN** a themed confirmation dialogue opens and lists the added, changed
  and removed grants, denies, rules and role assignments

#### Scenario: Confirm persists once and clears dirty

- **WHEN** the admin confirms the save and the server succeeds
- **THEN** the editor persists in one request, reloads the snapshot, clears the
  dirty state and shows the success banner

#### Scenario: Cancel keeps edits

- **WHEN** the admin cancels the confirmation
- **THEN** no request is sent and the unsaved edits and dirty state remain

#### Scenario: Failure keeps dirty

- **WHEN** the save request fails
- **THEN** the editor shows the server error and keeps the dirty state so the
  changes are not lost

### Requirement: Themed consistency

All new controls SHALL reuse the existing norse theme tokens and component
patterns.

#### Scenario: Confirm dialogue matches existing modals

- **WHEN** the confirmation dialogue renders
- **THEN** it uses the shared `Modal`/confirm-modal styling (`norse-deep`
  background, `norse-rune` border, accent confirm action, Cancel action) and
  honors Escape/backdrop close

#### Scenario: Buttons and alerts match

- **WHEN** the editor renders Save, Cancel, success and error states
- **THEN** they use the existing `Button` variants and `Alert` component with
  norse color tokens, not new ad-hoc styles
