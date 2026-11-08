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

The release-branch `WriteBehindBuffer.EdgeTypeLabelCountDelta` and
`EdgeTypeLabelCountDeltaInNamespace` entry points are retired. Embedded consumers
should request the corresponding start/end-label family through the engine's
canonical `ReadCountStore`, which includes buffered writes and an error result.

## Verification scope

Public executor regressions cover indexed and bound MATCH, visible ordering and
pagination, both parser configurations, buffered metadata and relabeling, and
count/row agreement. Native tests cover all count families, repeated types,
namespace isolation, internal-label policy and cancellation. The system database
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
