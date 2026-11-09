# Catalog visibility ownership review

The correction targets the existing v1.4.4 catalog-access contract (NOR-387).

| Candidate paths | Behavior and decision | Evidence |
| --- | --- | --- |
| HTTP post-result database filter / executor catalog listing | Both decide which database rows are visible. The HTTP filter only handled the exact bare command and assumed the first cell was the name. Retire it; use one executor admission predicate before SHOW projection and aggregation. | Public HTTP projection/alias/count regression fails on release 12c2b2b24 and passes on the candidate. |
| Catalog visibility / data permissions | Metadata visibility honors explicit CanSeeDatabase grants, within a supplied request scope. Without a visibility mode, the existing selection resolver decides. Graph reads retain their separate read permission. Keep data-read enforcement in its existing owner rather than treating metadata as a graph read. | Live resolver and permitted/denied controls. |
| Fabric dispatch / graph resolution | Dispatch previously authorized after lookup. Move that existing call into resolution after evaluating the selection and before catalog lookup; do not add a second authorization implementation. | Existing hidden and absent FragmentExec calls now hit the same authorizer once. |
| HTTP implicit / explicit transaction selection | Explicit routes already check selection first. The common statement runner now does so before its absent-database fallback, using the existing graph-access owner. | Implicit, multi-statement and explicit-open denied-target controls. |

The configured Graphify retrieval returns HTTP 400 (HTTP sent to an HTTPS port),
and no fresh indexed code baseline is available. This is a manual source review,
not a clean remote duplicate-check result. CI advisory review remains pending.

Composite graph-name functions, aliases, composite listings and constituent rows
reuse the same catalog visibility predicate. The function context binds a typed
live permission view rather than a per-row closure or an unused work context;
its actual construction remains covered by the zero-allocation assertion. Existing context-free raw catalog
providers remain trusted metadata APIs; request authorization is applied at their
query consumers.

## Independent review reconciliation

- The implicit single-statement HTTP route reaches the common statement runner
  without the explicit-route access gate. Its absent-target regression fails on
  release and passes after the shared runner checks the existing graph-access
  owner. Explicit handlers retain their earlier gate before acquiring a lease;
  callers reuse one predicate rather than implementing permission rules again.
- Metadata selection needs access, not data-read rights on every listed database.
  A public restricted-scope/partial-read control preserves visible names; the
  server's plugin scope enumerates all allowed databases, not just the URL one.
- Retained row metadata can differ from the execution context. Its reproduced
  catalog-function failure is corrected by extracting a typed live permission
  view from retained metadata; cancellation still uses execution work.
- Public USE graph.byElementId controls prove production predicate wiring;
  unrestricted raw Fabric instances remain trusted Go embedding components.
- Fabric authorization now runs before lookup. Reference recording has its own
  post-resolution callback so an absent graph never creates a cache dependency.
- The current-database SHOW row also uses the same predicate. A denied-only
  context previously returned the system row; the regression now requires no row.
- Unknown legacy constituent cells retain their trusted unconfigured behavior;
  under a configured request policy, an unresolvable alias has no selection grant.

These are source and focused-test dispositions, not an exhaustive graph verdict.

The final context audit found that carrying the whole context through fn.Context
created an unused nullable receiver field at GraphStorage. That field is retired:
auth.CatalogVisibility carries only mode, scope and the existing selection
resolver. No census acceptance or rule was added. The shared WAL correction in #1769 is integrated by rebase for testing.
The unchanged blocking context census passes on that integrated source
(126.031s). No census acceptance or rule change was needed. Final-head
full-suite CI and the advisory review remain release gates.
