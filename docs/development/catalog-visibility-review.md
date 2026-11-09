# Catalog visibility ownership review

The correction targets the existing v1.4.4 catalog-access contract (NOR-387).

| Candidate paths | Behavior and decision | Evidence |
| --- | --- | --- |
| HTTP post-result database filter / executor catalog listing | Both decide which database rows are visible. The HTTP filter only handled the exact bare command and assumed the first cell was the name. Retire it; use one executor admission predicate before SHOW projection and aggregation. | Public HTTP projection/alias/count regression fails on release 12c2b2b24 and passes on the candidate. |
| SHOW DATABASES / named SHOW DATABASE | Both produce the same catalog rows. Named selection consumes the same visible listing rather than assembling another metadata population. | Restricted and unrestricted named/listing controls. |
| Catalog visibility / data permissions | Metadata visibility asks CanSeeDatabase and existing selection access; graph reads ask read permission. Reuse the existing mode, scope and resolver. Keep data-read enforcement in its existing owner rather than treating metadata as a graph read. | Live resolver and permitted/denied controls. |
| Fabric dispatch / graph resolution | Dispatch previously authorized after lookup. Move that existing call into resolution after evaluating the selection and before catalog lookup; do not add a second authorization implementation. | Existing hidden and absent FragmentExec calls now hit the same authorizer once. |
| HTTP implicit / explicit transaction selection | Explicit routes already check selection first. The common statement runner now does so before its absent-database fallback, using the existing graph-access owner. | Implicit, multi-statement and explicit-open denied-target controls. |

The configured Graphify retrieval returns HTTP 400 (HTTP sent to an HTTPS port),
and no fresh indexed code baseline is available. This is a manual source review,
not a clean remote duplicate-check result. CI advisory review remains pending.
