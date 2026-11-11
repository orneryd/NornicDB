# UI interaction architecture

## Inventory and intent

Run `npm run inventory` in `ui` to regenerate [control-inventory.json](control-inventory.json). It enumerates control declarations in pages, shared components and Bifrost; SDK-generated controls are covered separately below. It is a structural inventory, not an accessibility certification.

| Area and canonical owner | User intent | Interaction decision | NN/g guidance |
| --- | --- | --- | --- |
| AppNavigation | Find a task and current location | Labeled task groups; active location; mobile disclosure; skip link | Recognition rather than recall; consistency |
| Header, DatabaseSummary | Know connection and database scope | Connection stays visible; selected database owns its counts; server status is secondary | Visibility of system status |
| QueryPanel, QueryResultsTable | Execute, inspect and compare results | Explicit Run; preserved query/history; expandable details; selectable records | User control; recognition |
| SearchPanel, GraphExplorerPanel, QueryGraphScene | Find records and relationships | Named controls; URL filters and selections; related-record expansion | Match with the real world; flexibility |
| NodeDetailsPanel, PropertyEditor | Inspect and edit one record | Resource route and /edit; typed property controls; explicit save/delete confirmation | Error prevention; user control |
| StatefulGrid | Sort, filter, pin and inspect tabular data | One URL-state bridge for grid view state; keyboard cell navigation; application palette | Consistency; flexibility |
| AdminUsers | Manage accounts without losing identity | Pinned username/actions; role checkboxes; inline email editing; saving/error status beside actions; retry/discard | Error prevention; error recovery; visibility |
| Databases | Create, inspect, configure or delete databases | Resource routes; details/settings dialogs; database-local counts | Match with the real world; user control |
| Browser maintenance | Regenerate one database's embeddings | Secondary Maintenance disclosure; required target; confirmation names database | Error prevention; minimalist design |
| LifecycleAdmin | Inspect and manage MVCC lifecycle | URL database/limits; explicit action confirmations | Visibility; error prevention |
| RetentionAdmin | Manage policies, holds and erasures | Resource creation/edit routes; URL confirmation kind/target; explicit submission | Error prevention; user control |
| KnowledgePoliciesAdmin and editors | Inspect and edit retrieval policies | Database/tab view state; shared native TypeScript grids | Consistency; recognition |
| Security, DatabaseAccess | Manage own credentials/tokens and authorization | Account disclosures; named role/database checkboxes; canonical access page | Recognition; error prevention |
| Modal, confirmations | Complete or cancel a focused task | Native dialog, keyboard escape and focus handling; safe initial action | User control; error prevention |
| FormInput, Alert, Button, Disclosure, ExpandableCell | Enter values, understand feedback, reveal details | Associated labels/errors; status/alert semantics; URL expansion state where applicable | Visibility; error recovery; progressive disclosure |
| Bifrost assistant | Get contextual assistance | Existing feature retained behind a labeled assistant control | Progressive disclosure |
| Demos, DemoSeedProvider, DemoGate | Try a capability without affecting application data | Explicit Seed; fresh disposable database; progress; resumable setup; management CTA | User control; visibility; error recovery |
| Galaxy | Explore shortest paths | Existing 3D graph retained; named endpoints and latency history | Match with the real world; visibility |
| Fleet | Observe graph writes and queries | Existing simulation retained; explicit start/pause; scoped disposable database | User control; visibility |
| Graphify | Navigate code relationships | Existing graph/import retained; URL controls, filters and root | Recognition; flexibility |
| PivotDemo, PivotDetails | Compare aggregates and inspect contributing orders | 25,000 orders/1,000 accounts; virtualized summary; multiple detail grids; visible formulas | Progressive disclosure; visibility; recognition |

The mapping applies the [NN/g usability heuristics](https://www.nngroup.com/articles/ten-usability-heuristics/) and [progressive disclosure](https://www.nngroup.com/articles/progressive-disclosure/). Keyboard/dialog behavior follows the [WAI tabs](https://www.w3.org/WAI/ARIA/apg/patterns/tabs/) and [dialog](https://www.w3.org/WAI/ARIA/apg/patterns/dialog-modal/) patterns. These references guide decisions; they do not establish certification.

## Route and state ownership

`UrlStateProvider` is the only query-string writer. `useViewState` owns typed defaults and updates. `StatefulGrid` owns SDK view-state serialization. No grid records, credentials, form drafts or result bodies are written to the URL.

| Resource/view | Path | Transient state |
| --- | --- | --- |
| Workspace | `/workspace/:database` | `tab`, `query`, `q`, `selected`, `similar`, graph filters/root, assistant, grid view state |
| Node | `/workspace/:database/nodes/:nodeId[/edit]` | Existing workspace state retained |
| Database | `/databases/:database[/edit]`, `/databases/new` | Dialog context resolves from the path |
| User | `/security/admin/:username/edit`, `/security/admin/new` | Grid filters/sort/pinning |
| Retention | `/security/retention/policies/:policyId/edit`, creation routes | `confirm`, `target` identify confirmation only |
| Generated demo | `/demos/:kind/:demoDatabase` | Pivot dimensions/measure/aggregation, grid expansion; graph/simulation controls |
| Secondary disclosure | Existing resource path | `section.<id>`, `navigation`, maintenance and feature-specific disclosure keys |

Back, Forward and reload restore view state. Resource IDs resolve through existing APIs. Opening a route never creates a demo database or submits a destructive action. Query execution and saves require explicit controls. Legacy routes remain available. This follows [Ember's query-parameter distinction](https://guides.emberjs.com/release/routing/query-params/) between route resources and optional view state.

## Demo data and formulas

Seed plans share one owner. A click creates `nornic_demo_<kind>_<random>` and navigates to its setup progress. Deterministic MERGE batches make interrupted setup resumable. A dataset marker becomes ready only after all steps succeed. Generated nodes skip embeddings so synthetic data does not consume the embedding queue. Existing application databases are not seeded. Fleet motion starts paused. Delete a demo from Databases when finished; the UI does not delete it automatically.

The pivot starts with 25,000 orders and 1,000 distinct account rows in a bounded grid viewport. Account is a row dimension, not a 1,000-column axis. Rows can be expanded independently into order grids; details read up to 100 orders and report the cap. Detail grids have independent URL-owned sort/pinning state.

Revenue is stored as integer cents. Average totals use contributing order counts. Revenue per unit divides total revenue by total units; it never averages child ratios. Revenue share divides group revenue by revenue across the dataset. Zero denominators display an empty value. Formulas are named, read-only measures rather than arbitrary executable expressions.

## Query graph workspace

Cypher nodes, relationships, nested lists and paths feed a bounded 3D graph directly; no neighborhood request is required to view returned data. Relationship-only results create labeled endpoint placeholders. Graphs above 200 nodes report truncation; explicit neighborhood expansion uses the existing server API and its limit. Query results remain available when returning from a neighborhood.

Canvas node/edge selection and keyboard lists share the existing details owner. URL state holds selected resource/relationship IDs, camera, pinned positions, labels, motion, filters and disclosure state. Keyboard buttons provide orbit, zoom, fit and focus alternatives. Node colors identify labels; selection and root also have rings and textual explanations. Renderer failure leaves an explicit error and keyboard lists rather than silently replacing the visualization.

## Dependency limitations accepted for this change

- [UI Grid #7410](https://github.com/orneryd/ui-grid/issues/7410): fixed-height virtualization ignores expanded detail heights. The supported virtualization threshold renders the complete result while any detail is open, then restores virtualization when all details close. This increases DOM size during expansion; dependency source is unchanged.
- [UI Grid #7411](https://github.com/orneryd/ui-grid/issues/7411): the SDK grid lacks required row/cell ARIA ownership and pin-menu item roles. Browser checks attach that exact SDK finding separately. Other accessibility findings fail the suite. Do not describe this as full accessibility compliance.

## Policy guidance and administration

Knowledge policies use contextual explanations and an optional, reopenable three-step setup: target, profile, review. This follows [NN/g's onboarding guidance](https://www.nngroup.com/articles/onboarding-tutorials/): teach in context rather than requiring a first-run tour. Label/type suggestions come from the selected database. Preview reads a bounded sample; applying a profile and binding requires an explicit action. Forward and inverse curve previews use the scorer's mathematics and explain anchors, half-life, floors and visibility thresholds. Advanced editors remain available. Setup progress and selections use query parameters; unsaved form bodies remain local.

Schema has one owner at `/schema/:database`: inspect indexes and constraints, create a reviewed definition, or confirm a drop. Advanced operations link to the existing Cypher workspace. Backup and restore have one owner at `/backups`; both operate on the entire server, and file paths refer to the server filesystem. Restore requires typing `RESTORE`. Tests never restore a live database.

Every authenticated role can generate a personal token and open MCP setup guidance. Tokens retain the caller's identity and roles; submitted identity/role fields cannot grant additional privileges. Administration and data permissions remain enforced independently. Browser coverage includes admin, editor, viewer and a custom role without data access; API tests also check impersonation attempts and denied administrative operations.

All grids share one theme and explicit supported viewport sizing. Expanded detail rows temporarily disable virtualization through the SDK's supported threshold; collapsing them restores virtualization.

## Verification

`npm test` covers URL decoding, pivot totals/formulas and existing graph geometry. `npm run test:browser` covers navigation/history, database scope, typed record edits, grid errors/retry, explicit demo seeding, partial resume, virtualization, nested grids, and section accessibility checks. API fixtures abort external requests and never write to a running database. Screenshots use synthetic identities. A live local pivot seed/query was also exercised against a disposable demo database.

Database-scoped regeneration adds a small explicit API extension: `POST /nornicdb/embed/trigger?regenerate=true&database=<name>`. Focused Go tests cover permissions, unknown/composite targets, namespace isolation, worker state and the legacy all-database path. Embedding worker status remains server-wide and is labeled accordingly.
