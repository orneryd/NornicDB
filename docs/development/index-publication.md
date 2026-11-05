# Equality-index publication

An accepted write publishes each entity's first committed predecessor and final
stored image once at its physical publication boundary. Intermediate operations
remain available to cardinality accounting, MVCC and notifications; they are not
independent changes to live equality membership.

`newCommitStates(ctx, frames...)` selects these endpoints. Request preparation,
MVCC staging, change capture and recovery-body hydration retain their own contexts
and propagate cancellation errors. The publication callbacks run after durability
and complete their derived state independently of request cancellation.

`CompositeIndex.ReplaceNode(ctx, id, beforeProperties, afterProperties)` prepares
both complete key projections before mutating any posting, then changes them under
one index lock. Shared keys remain filed. Single-sided filing/removal and replacement
share posting primitives. Nil old/new properties represent entering/leaving an
index's label. For example:

```go
err := index.ReplaceNode(ctx, node.ID,
    map[string]any{"tenant": "a", "id": "old"},
    map[string]any{"tenant": "a", "id": "new"})
```

Canonical UNIQUE claims also retain an unchanged label/property/value identity.
The comparison uses validation's canonical keys, including numeric-width and list
normalization. Scalar NaN remains non-reflexive under the existing claim contract.
Changed predecessors retire before final claims acquire, allowing key transfers;
recovered claims cannot overwrite a newer holder.

## Accepted-completion source review

PR #1670 ports the previously approved accepted index-publication phase from
`maintainCompositeIndexesOnNodeCreated/Updated` and
`updateRelationshipUniqueIndexes` to `replaceNodeIndexEntries` and
`publishRelationshipIndexMembership`. Both successors are reached by
`publishIndexMembership` from the existing ordered physical-publication callbacks.
The deleted owner names no longer qualify a live source site, so their census
records move to these successors. This is a source-owner migration, not approval
to detach request-driven index construction or reads.

The fold formerly inside `publishUniqueValues` now runs in
`publishCommitOperations` and the suppression rewrite's publication callback.
`commitWriter.runPublication` invokes these callbacks after the physical action
has succeeded; neither callback is invoked by write preparation or on rejection.
Suppression checks, staging and key preparation still use the request context.
The callback's completion context selects only the already accepted images.

`WriteBehindBuffer.AppendCommit` is called inside the existing
`DecidePublication` action after the buffered WAL append succeeds. The completion
context already used to rebuild pending membership also selects that accepted
commit's frame. Rejection/cancellation before this boundary does not append it.
Physical buffered replay reuses its existing `accepted` context when folding
frames, rather than creating extra contexts in its publication callback.

The versioned context census records exact successor sites and offsets. This port
does not change the census analyzer, forbidden acceptance kinds, nil-flow checks,
rollback source pins, duplicate checker or hooks. Revalidation must include the
blocking context census and the existing cancellation/publication regressions;
renaming records alone is not runtime proof.

No Badger keys, stored payloads, WAL format or storage version change.
