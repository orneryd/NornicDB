# Index reads and cancellation in v1.4.4

Index candidate preparation belongs to the statement that requested it. The
following Go APIs now require a `context.Context` as their first argument and
return an error in addition to their existing result:

| Owner | Operations | Results |
| --- | --- | --- |
| `CompositeIndex` | `LookupFull`, `LookupPrefix`, `LookupWithFilter` | IDs, error |
| `SchemaManager` | `PropertyIndexLookup`, `PropertyIndexLookupAnyLabel`, `PropertyIndexTopK`, `PropertyIndexAllNonNil` | IDs, error |
| `SchemaManager` | `PropertyIndexRange` | IDs, index availability, error |
| `SchemaManager` | `VisitPropertyIndexGroups`, `VisitPropertyIndexGroupsInRange` | index availability, error |
| `SchemaManager` | `LookupUniqueConstraintValue` | holder ID, found, constraint exists, cache complete, error |
| `BadgerTransaction` | `MergePendingPropertyMatches`, `MergePendingCompositeMatches` | IDs, error |
| `TransactionEqualityLookup` | `LookupNodesByEquality` | nodes, complete, error |

Pass the actual statement context and handle its error before deciding whether
to use a scan fallback. Cancellation is an error, not an index miss. For example:

```go
ids, err := schema.PropertyIndexLookup(ctx, "Person", "email", email)
if err != nil {
    return err
}
// Evaluate the full query predicate against these candidates as usual.
```

The old signatures are retired; embedding applications and plugins must rebuild
and migrate their callers. HTTP, Bolt, Cypher syntax, Badger keys, stored payloads
and storage version do not change.

Cancellation applies while waiting for index admission, copying candidates,
merging pending writes, ordering keys and preparing groups. A canceled
materialization returns no partial result. A reusable key or pending-value cache
is published only after its construction succeeds. Group visitors run without
holding index locks; an intentional visitor stop remains successful unless the
request was also canceled.

Accepted writes retain their own integrity-check lifetime. Existing context-free
Go maintenance and integrity boundaries explicitly select `context.Background()`;
statement reads must not substitute that context for their caller's context.

The regression budget is measured from actual cancellation or deadline expiry
until backend work stops, separately from total request time. The release target
for the reproduced indexed-read workload is 100 ms after cancellation. This is a
NornicDB regression target, not a claimed universal Neo4j timing guarantee.
