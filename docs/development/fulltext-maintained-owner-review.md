# Maintained full-text ownership review (#1683 / NOR-148)

This change targets `release/v1.4.4` after #1682 (`31208b17f`). The original
#1683 stack was used as a reference; obsolete parser/storage APIs were not restored.
The separate vector checkpoint work belongs to NOR-351/352/353 and is excluded.

## Product contract

The existing full-text procedures retain their query matcher and scoring owner.
A database search service may keep immutable document bodies and the most recent query term set's body
frequencies between calls. Storage event/revision certificates determine whether
those documents describe the current committed view. Query-result caching is
not needed for this reuse. Fresh, transaction, visibility-filtered and unproven
wrapper views use their own storage readers without borrowing a different view's
corpus. Ordered consumers keep streaming and propagate their stop to storage.

Field names are exact and case-sensitive. Only selected fields are searchable.
A phrase must occur within one field; it cannot bridge two properties. The
built-in all-property relationship index orders fields by name for deterministic
body length and scoring. Named indexes retain their explicitly declared order.
These fixes also cover the folded NOR-294 field-identity defect.

Neo4j 5.26.0's `FulltextIndexReader.parseFulltextQuery` constructs a Lucene
`MultiFieldQueryParser` over property names; Lucene constructs phrase queries
separately per field. These sources establish the field boundary, rather than a
live Neo4j A/B test or a claim of identical Lucene ranking:

- [Neo4j source](https://github.com/neo4j/neo4j/blob/5.26.0/community/fulltext-index/src/main/java/org/neo4j/kernel/api/impl/fulltext/FulltextIndexReader.java)
- [Lucene source](https://github.com/apache/lucene/blob/main/lucene/queryparser/src/java/org/apache/lucene/queryparser/classic/MultiFieldQueryParser.java)

## Ownership and retained memory

`search.Service.ReadFulltextDocuments` owns admission, request/service lifetime,
projection leases and document-generation retirement. Cypher supplies its existing
entity-to-document/scoring adapter. Storage owns namespace identity, publication
settlement, native streaming, committed point captures, and hydration policy.
Captures are request-local; retained documents never retain native read transactions.
Capture and hydration I/O errors are returned, not converted to empty results.

`memory.Policy` still owns application admission arithmetic. Optional retained
copies observe that policy and the existing Go memory limit; no new limit is
introduced. Pressure detaches a generation without modifying an active reader.
Recovery uses hysteresis. A materialized corpus is already included in physical
usage and is not charged a second time as future growth. Public service and
procedure tests cover release, storage reads, refill, status and operator events.

The shared search read lifetime polls the request's `Err` as well as the derived
service context. A statement's lazy `Done` relay must not allow continued scans
after real caller cancellation. The existing public cancellation population test
on both parsers and a deliberately delayed-relay component test cover this boundary;
shutdown still cancels and joins active readers before resource disposal.

## Duplicate inventory disposition

The informative checker analyzed 204 candidates against staged tree
`9d41d81a1b83e9580c40464af4a3332eed4dac6d`. Its remote graph baseline
`35e3bddbf` predates the release used here. Ten reuse candidates and two refactor
targets were investigated against current source. Subsequent corrections are
reviewed here; this is not a claim of a fresh clean checker run. No checker rules,
thresholds, exclusions or hook behavior were changed.

| Candidate | Current source comparison and decision | Evidence |
| --- | --- | --- |
| Node/edge document builders (both directions) | Collapsed into `buildFulltextDoc`, then the existing property-assembly kernel. Entity-specific content extraction remains necessary: a node's default text selection differs from the built-in relationship all-property selection. Normalization/field-map assembly has one implementation. | `TestFulltextDocKernel_NodeAndEdgeBuildersAgree`, public field-contract tests |
| Term/phrase field matchers (both directions) | Collapsed into `patText`. Literal substring matching and primary-term selection are shared. Quoted phrases carry the existing one-point score boost as data, preserving scoring. | Existing `fulltext_query_test.go`, procedure/field regressions |
| `Lease.WireLimit` / `WireRequest.WireLimit` | `WireRequest` already calls the sole lease capacity calculation. Its additional mutex and closed-state check protect concurrent `Move`, `Reset` and retained-handler cleanup; replacing the request with a lease would discard that ownership. No second capacity formula remains to merge. | `TestWireRequestScopeOwnershipAndActualSize`, `TestWireRequestRetainedWorkerOwnsUntilReturn`, wire reservation concurrency tests |
| `Lease.GrowWireRequest` / `WireRequest.Grow` | Request owns accumulated wire-component sizes, lazy first reservation and transfer/close state. It calls the existing lease growth owner once a lease exists; the lease alone applies forecast arithmetic and policy accounting. Combining entry points would conflate request transfer with policy-held credit. | Same request tests; `TestWireReservationGrowsOneOwnerAndReleases`, `TestWireReservationConcurrentGrowthAndRelease` |
| Search-service factory preflight / construction | Removed the preflight wrapper and renamed the actual constructor `GetOrCreateSearchService`. Its existing registry retirement and work admission are the canonical lifecycle gate, including storage callback callers that cannot take `db.mu`. | `TestOwnerClassSearchConstruction*`, `TestSearchServices_ClosedDBCannotCreateService` |
| Search / replication `closeResources` | Shared stop/admission/join protocol already belongs to `lifecycle.WorkLifetime`. Search disposal retires native indexes **after** accepted reads finish; transport socket/listener closure is the stop signal **before** joining blocked I/O. The resources and phases differ; putting both closures in one phase can prevent reads/writes from returning or dispose active native resources. No common duplicated resource algorithm was found beyond the existing lifetime owner. | `TestFulltextDocumentsParticipateInServiceLifetime`, `TestOwnerClassSearch*`, `TestClusterRPCCloseReleasesActiveAndQueuedSenders` |
| Relationship vector procedure / `...WithVector` | The graph candidate's old outer function is already retired on this release. Current `callVectorQueryArguments` resolves arguments once and invokes the canonical vector executor. The obsolete function is not restored. | Current-source symbol/caller inspection; existing vector procedure tests remain in `make test` |
| `Policy.Refresh` / `refreshSample` | Refresh calls the single `refreshSample` producer, then releases optional copies and may reclaim/re-sample under existing recovery rules. Generation ordering, sample freshness and publication live only in `refreshSample`; no copied sampler exists. Inlining it twice into orchestration would introduce duplication. | `TestPolicyOverlappingRefreshKeepsNewestObservation`, `TestPolicyRejectsImmediatelyAfterSamplerFailure`, policy recovery tests |
| Test `AllNodes` / `AllEdges` counter overrides | Removed unused bulk counter overrides. Native streaming observers now have one `fulltextCountingStore` used by the reproduction, lifecycle tests and benchmarks; assertions still count actual native visits and repeated scans. | `TestPR1683CurrentRelease_*`, `TestFulltextMaintainedPublicNoRebuild` |

The table records actual composition or different phases where retained separation
is justified. It makes no unmeasured performance claim to excuse duplicate code.

## Blocking audit declarations

The new `notificationUnready` counter records reserved logical publications that
have not become ready. Its only writers are the notification queue's reserve and
ready transitions; its only external reader is `readPublicationSettled`. The
storage census now declares that exact publication-order ownership and regenerates
its source inventory. It is not classified as an unowned counter or excluded from
audit. Logical readiness does not mean every listener has finished delivery.

The lexical population review removes two retired ad-hoc full-text parser helpers,
records the public procedures' delegation change, and reviews five new declarations.
The full-text query adapter still uses `ParseFulltextQuery`; corpus `Tokens` fields
are integer word counts used in BM25 statistics, not credentials. The reviewed
inventory names the concrete public matching/statistics tests for those entries.
Extractor rules and assertions remain unchanged; the population check covers 3,969
current scanner/numeric/output candidates.

## Go integration example

Existing procedure adapters use the service with the same engine view they execute
against. A simple node-only adapter can look like this (setup/teardown omitted):

```go
scope := search.FulltextDocumentScope{
    Name: "nodes:notes", Definition: "Note(text)",
    Candidate: func(node *storage.Node, _ *storage.Edge) bool {
        return node != nil && slices.Contains(node.Labels, "Note")
    },
}
build := func(node *storage.Node, _ *storage.Edge) *search.FulltextDocument {
    if node == nil || !slices.Contains(node.Labels, "Note") { return nil }
    text, _ := node.Properties["text"].(string)
    if text == "" { return nil }
    return &search.FulltextDocument{
        Text: strings.ToLower(text), Tokens: len(strings.Fields(text)),
    }
}
err := service.ReadFulltextDocuments(ctx, engine, scope, build,
    func(readCtx context.Context, documents search.FulltextDocuments) error {
        return documents.Each(readCtx,
            func(id string, document *search.FulltextDocument) error {
                // Evaluate this query against immutable document.Text.
                return nil
            })
    })
```

Use the same ordered definition for the same named projection; changing the
schema must change `Definition`. The builder's `Text` must include the searchable
text its dialect evaluates. Do not retain or modify supplied documents. Set
`EntityRows` and call `Entity(readCtx, id, document)` inside this callback when materializing output rows;
check its error. Use `Fresh` for time-dependent visibility. `Ordered` is a different
consumer contract: call `VisitNodes(readCtx, visit)` or `VisitEdges(readCtx, visit)` and return its error/stop,
rather than requesting corpus statistics or enumerating scoring documents.
Service `Close` cancels and joins admitted callbacks before disposing indexes.

## Persistence and evidence limits

The new maintained corpus is process-local and reconstructible. No Badger key,
stored entity payload, WAL format or storage-version change is required. Existing
persisted BM25/vector owners are not replaced. Restart and restore discard/rebuild
these optional copies from storage.

The original private R14 application dataset and saved question suite are not
available. Generated in-memory fixtures reproduce top-k, count, eligibility and
long correspondence bodies, and assert rows/scores/order and native read work.
They do not certify an unavailable application suite or universal constant query
cost: more indexed documents and more matching results still require work.
Workload sizes, cold/warm state and allocation results are reported separately in
the [full-text API guide](fulltext-index-api.md). Full local readiness still
requires the final integrated `make test` result.


## Search-constructor reconciliation review

The full integrated gate exposed one source-pinned reconciliation failure after
constructor consolidation. `storage_reconciliation.json` still pinned
`DB.GetOrCreateSearchService` as a four-call preflight wrapper that took `db.mu`
and invoked `getOrCreateSearchService`. The canonical constructor now has that
public name; the preflight wrapper is retired.

The current implementation was reviewed through its callers and all construction
phases: it enters DB work and the current constructor epoch under
`searchServicesMu`, releases host locks before resolvers/construction, rechecks
lifetime at publication, adopts exactly one registry service, and closes a losing
candidate before releasing its accepted ownership. Callback callers may already
hold `db.mu`, so restoring the old wrapper would reintroduce the locking problem.
Service lifetime creation stays in `search.NewService`; lazy warming uses
`DB.LifetimeContext`. The constructor itself has no fresh context-creation calls.
The record includes the current 97 named call expressions, including closures.

The re-reviewed function digest is
`1dc0e332b932a9864034ae5598e7166b54758a64aaa92ea121e9d1f925e77b2b`,
replacing the old wrapper digest
`8804f6ddbbca5fe0199cacbd36b4ca2a5c24baa9a7ec4872b66b4d3fb677bf02`.
Only that tombstone's source record changes. It grants no acceptance to replacement
paths; the normal compiled census remains authoritative. Rules, assertions,
retired-source counts, and nil-flow allowances remain unchanged. The unchanged
`TestStorageReconciliationRecords` passes on the isolated current-source proposal;
the main corrected untagged `make test` also passes all 147 packages, including
this control and both blocking census packages.

Evidence includes constructor/retirement reentry, actual-completion and closed-DB
regressions, the full `pkg/nornicdb` runtime pass, and the strict nil-context checks.
No runtime code, persisted format, or benchmark implementation changes are needed
for this record correction.

The operator approved the generated R14-shaped workload as release acceptance on
4 November 2026. Private replay remains unavailable and is documented rather than
required. The final local `make test` passes with both lint gates and all 147
packages (8 fresh, 139 cached); its receipt records 8,317 unchanged
repository/native/UI inputs. Broader
`make verify` is not claimed by this gate.
