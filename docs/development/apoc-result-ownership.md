# APOC and query-result ownership

APOC graph and warmup reads return caller-owned node, relationship, path and
collection values. Changing returned labels, properties or nested containers
cannot update the graph or a later warmup read. Ordinary and smart Cypher result
caches likewise retain their own result and deliver an independent result on
cache hits. Values preserve the existing domain type/normalization contract;
this does not change Badger encoding or promise a new storage type system.

Use explicit write methods to change stored values. A read-modify-write that
previously depended on modifying a retained read object must migrate.

## Go property updates

`apoc/storage.Storage.UpdateNode` and `UpdateRelationship`, the in-memory
backend, and `nornicdb.APOCStorageAdapter` now accept `PropertyUpdate`:

```go
import apocstorage "github.com/orneryd/nornicdb/apoc/storage"

err := backend.UpdateNode(ctx, nodeID, apocstorage.PropertyUpdate{
    Set: map[string]interface{}{"name": "Alice"},
})
```

A partial update preserves properties not listed in the patch. Remove properties
explicitly, including when renaming a property:

```go
err := backend.UpdateRelationship(ctx, relationshipID, apocstorage.PropertyUpdate{
    Remove: []string{"oldName"},
    Set: map[string]interface{}{"newName": value},
})
```

Removals apply before sets; removing and setting the same name leaves the set
value. A zero patch changes no property but still checks admission and identity.
Setting a nil value keeps the existing Go property-value contract; use `Remove`
for deletion. Nested set values are copied before retention.

Custom backends can apply an admitted patch through `update.ApplyTo(bag)`.
The bag must be exclusively owned by that backend or its write preparation;
`ApplyTo` mutates it and returns the resulting map. Both bundled backends use
this same patch owner. It is not a transaction or admission substitute.
Accepted native writes retain the existing before/after atomic staging.

Update custom storage implementations, method-typed references, plugins and
callers together and rebuild them. There are no old-signature adapters. This
migration does not change HTTP/Bolt syntax, Badger keys or payload encoding,
WAL records, or the storage version, and needs no database migration.

## Verification and costs

The ownership regressions exercise public getters, warmup, cached execution,
explicit property writes and live streamed rows. Statement-local projections now
use their already-owned scoring snapshot, so a returned property list cannot
mutate the producer's later reads. Streams without those detached views copy
through the same structural value owner, rebinding literal-map metadata and
checking cancellation before publication. The named node/relationship and
budget-path controls do not claim exhaustive arbitrary plugin qualification. See [workload measurements](../performance/retained-result-ownership.md)
for copy and traversal cost controls. Final merge readiness is recorded on the
PR from the exact integrated candidate's tests and CI, not inferred from these
examples.
