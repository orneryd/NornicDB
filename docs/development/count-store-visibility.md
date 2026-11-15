# Count-store visibility

Ordinary MATCH queries hide nodes carrying an underscore-prefixed internal label.
A node with both `Document` and `_internal` must not contribute a visible
`MATCH (n:Document) RETURN count(n)` row. Relationships whose endpoints are hidden
must not contribute relationship rows either. Internal storage reads retain their
existing access to these records.

## Embedded Go count reads in v1.4.4

`storage.CountStoreReadEngine` is an optional capability. Its context-first
`ReadCountStore` method takes a `CountStoreReadRequest` and returns a
`CountStoreRead` plus an error. The returned `Count` is a raw derived count;
use it as a query answer only when `DescribesRows` is true. Otherwise, execute
the visible query path. Engines without the capability remain supported through
that query path.

The request selects a node-label count, relationship-type count, or a positional
relationship count with a start/end label. An empty relationship type list counts
all types. Repeated types contribute once. `HiddenLabelPrefix` specifies the
label prefix excluded by the caller's read policy; NornicDB's ordinary MATCH
owner uses `_`. Empty means no internal-label exclusion. The caller must also
exclude count-store plans for temporal viewports or decay-filtered reads.

```go
reader, ok := engine.(storage.CountStoreReadEngine)
if ok {
    answer, err := reader.ReadCountStore(ctx, storage.CountStoreReadRequest{
        Kind: storage.CountStoreNodesByLabel,
        Name: "Document",
        HiddenLabelPrefix: "_",
    })
    if err != nil {
        return err
    }
    if answer.DescribesRows {
        fmt.Println(answer.Count)
    } else {
        // Use the ordinary visible MATCH execution path.
    }
}
```

A namespaced engine always supplies its own namespace. The native reader takes
the counter value and eligibility decision from one persisted snapshot while
holding the existing count-publication and buffered-read locks. Unrelated
internal metadata does not force a scan of the counted population: existing
internal-label membership and positional counters establish whether it can
change the requested result. If relevant hidden nodes can change the result,
the existing visible-row reduction supplies it. No new stored keys, entity
payload, WAL format, or storage migration is introduced.

Transaction wrappers combine the committed answer with their own staged count
contribution. When staged positional changes or another unsupported view cannot
establish the answer, they decline the fast path. Cancellation propagates through
read admission and staged-delta scanning. Invalid requests and storage failures
return errors rather than a certified count.

### Counts inside an explicit transaction

An explicit transaction reads its rows at its begin boundary, so the current
counters describe them only until a peer commits into its namespace. Reading
them at the transaction's own persisted snapshot keeps such a count in the
counter keyspace: the wrapper plans the read at the transaction
(`storage.BadgerTransaction.ScopeCountStoreRead`), which installs an opaque
snapshot capability in `CountStoreReadRequest.Scope`. The request goes through
the caller's own reader chain, so a decorator or custom reader behaves as it
does for every other read, and the native counter kernel answers it from the
transaction's snapshot: it reads the counter and, under the transaction's own
lock, composes the contribution the transaction's staged writes hold at that
instant. A reader that stages a write and then forwards the request it was
handed therefore contributes that write. The answer is `DescribesRows` only when
that kernel consumed the capability
(`CountStoreReadScope.DescribesSnapshot`); a reader that answers the request
itself, refuses it, or rewrites it leaves the answer unproven, and the wrapper
does not ask it again: the count stays with the rows the transaction reads.

The capability reuses the transaction's managed snapshot reader: each use holds
the transaction's lock for the whole read and asks the transaction's lifecycle
owner whether the transaction may still read, so a retained or reused request
cannot reach a disposed view, and a closed transaction declines. So does a
transaction that handed its native client handle to an accepted commit, and one
whose snapshot reader resource pressure expired: both keep an active status but
no longer read the snapshot the capability was issued at. It also names the cut
it was issued at, so a transaction that continued onto another snapshot declines
rather than certifying the newer one. It declines the read rather than borrowing
a newer view when the transaction cannot prove one: a foreign or empty
namespace, a write-behind transaction (whose reads are read-committed), a read
version that is no longer the namespace's begin boundary, or a read whose answer
needs the positional (label, type) counters while the transaction's cut holds an
unpublished version. Those positional counters are published after their
commit's rows, so a cut proves them complete only when the reservation ledger
copied at the cut is empty. A malformed persisted count is likewise not repaired
at a snapshot, because the family rebuild reads the current state.

```go
// Inside an explicit transaction: plan the read at its snapshot and send it
// through the reader you already read everything else through. The answer
// already includes what the transaction's own writes contribute.
request, ok, err := tx.ScopeCountStoreRead(ctx, storage.CountStoreReadRequest{
    Kind: storage.CountStoreRelationshipsByType,
    Namespace: namespace,
    Types: []string{"KNOWS"},
    HiddenLabelPrefix: "_",
})
if err != nil {
    return err
}
if ok {
    answer, readErr := reader.ReadCountStore(ctx, request)
    if readErr != nil {
        return readErr
    }
    if answer.DescribesRows && request.Scope.DescribesSnapshot() {
        fmt.Println(answer.Count)
    }
}
```

Total node and relationship counts have no snapshot-addressable persisted
counter and remain outside this contract.

The release-branch `WriteBehindBuffer.EdgeTypeLabelCountDelta` and
`EdgeTypeLabelCountDeltaInNamespace` entry points are retired. Embedded consumers
should request the corresponding start/end-label family through the engine's
canonical `ReadCountStore`, which includes buffered writes and an error result.

## Verification scope

Public executor regressions cover indexed and bound MATCH, visible ordering and
pagination, both parser configurations, buffered metadata and relabeling, and
count/row agreement. Native tests cover all count families, repeated types,
namespace isolation, internal-label policy and cancellation. Explicit
transaction counts are covered after peer creates and deletes, for own writes,
hidden labels, a peer commit that lands inside the counter read, a reader that
ignores the snapshot capability, a reader that stages a write before it forwards
the request, a capability retained across the transaction's continuation, a
custom reader's own failure and explicit decline, concurrent scoped reads beside
staged writes, cancellation, namespace and engine identity, retained
capabilities, malformed counters and the positional cut. A capability retained
across the transaction's accepted-commit handoff, and one issued after it,
declines, as does a capability consumed once the transaction's snapshot reader
crossed its graceful or hard resource-pressure threshold. The system database
continues rejecting graph statements; privileged storage and internal point-read
controls are checked separately.

A one-hop relationship-ID predicate also retains its specific relationship as
the traversal candidate. Existing traversal owners still apply direction,
incidence, labels, properties, endpoint visibility and the complete WHERE.
Bound endpoints and composite source identities retain those checks. Unsupported
predicate shapes and temporal views retain their existing execution paths.

## Focused count measurements

Apple M2 Max, Go 1.27.3, in-memory Badger, 5,000 counted nodes, query cache
disabled, two 1-second samples per case. These compare two correct candidate
plans, not the release baseline. No network or write-behind timing is included.

| Population | Rejected population-scan plan | Scoped metadata proof |
| --- | --- | --- |
| No internal metadata | 0.363 ms; about 4.7 KB; 90 allocations | 0.363 ms; about 4.7 KB; 90 allocations |
| One unrelated internal metadata node | 2.23–2.24 ms; about 2.50 MB; 25,192 allocations | 0.367–0.374 ms; about 6.1 KB; 121 allocations |

The additional metadata case has a bounded index-proof cost. It no longer
materializes all 5,000 visible nodes. Relevant hidden populations still use the
existing visible-row path when a raw counter cannot answer them. CPU profiles
were captured for both candidates; they also include benchmark fixture setup.
Buffered correctness is covered separately by the count regressions.
