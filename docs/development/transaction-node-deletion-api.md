# Transactional node deletion

`BadgerTransaction.DeleteGraphNode(id, mode)` selects the relationship policy for a transactional node deletion. `storage.Transaction`, the type used in `DB.Update` and `DB.View` callbacks, is an alias of `BadgerTransaction` and uses the same API.

| Existing call | Typed policy call |
|---|---|
| `tx.DeleteNode(id)` | `tx.DeleteGraphNode(id, storage.CascadeEdges)` |
| `tx.DeleteConnectedNode(id)` | `tx.DeleteGraphNode(id, storage.RejectConnected)` |

`CascadeEdges` removes the node and its incident relationships, including hidden relationships. `RejectConnected` checks connectivity at commit: later operations in the same transaction may remove relationships first. A surviving relationship causes `NodeStillConnectedError` and rolls back the transaction. During the transaction the pending node deletion stays hidden from scans while relationship endpoint access remains available for later clauses.

```go
// DETACH DELETE behavior.
err := tx.DeleteGraphNode(nodeID, storage.CascadeEdges)
```

```go
// DELETE behavior: connectivity is checked when the transaction commits.
err := tx.DeleteGraphNode(nodeID, storage.RejectConnected)
```

An invalid mode returns `ErrInvalidData` before the transaction stages changes or pins a namespace. Empty IDs, lifecycle failures, namespace fencing and repeated deletion retain their existing errors.

## Go source migration

Replace direct transaction calls and method expressions with the typed policy API. The old transaction methods are removed. This affects clients using `*storage.BadgerTransaction` or its `storage.Transaction` alias, including transaction callbacks. `storage.Engine.DeleteNode(id)` keeps its existing signature and cascading behavior.

The optional `storage.ConnectedNodeDeleter` capability now requires `DeleteGraphNode(id storage.NodeID, mode storage.NodeDeletionMode) error`. Custom stores must implement that method to participate in deferred non-DETACH deletion. An adapter implementing only the old method no longer advertises the deferred capability; structural type assertions may stop matching even when the adapter itself still compiles. Namespace wrappers must pass the selected policy along with the prefixed ID.

Cypher, HTTP and Bolt deletion behavior is preserved by the repository's migrated adapters.

## Observed node and relationship updates

The Go mutation API now takes typed image requests. Replace pointer-only
`Engine.UpdateNode`, `Engine.UpdateEdge` and transaction calls with:

| Mutation owner | Request |
|---|---|
| `engine.UpdateNode` or `tx.UpdateGraphNode` | `storage.NodeUpdateRequest{Before: before, After: after, Policy: policy}` |
| `engine.UpdateEdge` or `tx.UpdateEdge` | `storage.EdgeUpdateRequest{Before: before, After: after}` |

`Before` is the immutable image the caller actually read. Preserve it before
editing fields and keep it unchanged until the transaction terminates. Do not
reread the predecessor after the mutation or retain observations on ordinary
reads. A nil `Before` deliberately selects a blind image write, such as restoring
an authoritative backup image; it does not represent an ordinary read-modify-write.

For example, this update uses the standard-library `maps` package to separate
only the property map that changes. Untouched embedding arrays remain shared:

```go
before, err := tx.GetNode(nodeID)
if err != nil {
    return err
}
after := *before
after.Properties = maps.Clone(before.Properties)
if after.Properties == nil {
    after.Properties = make(map[string]interface{})
}
after.Properties["name"] = "Alice"
err = tx.UpdateGraphNode(storage.NodeUpdateRequest{
    Before: before,
    After: &after,
    Policy: storage.ClientNodeEdits,
})
```

For relationships, preserve the `GetEdge` result in the same way, separate any
property map you edit, and pass the original and edited images to
`tx.UpdateEdge(storage.EdgeUpdateRequest{Before: beforeEdge, After: &afterEdge})`.
Custom engines, wrappers and Go method expressions must migrate to these request
signatures; forwarding wrappers for the removed signatures are not provided.

`ClientNodeEdits` retains managed-embedding reconciliation. Use
`storage.StoredNodeImages` only for a complete authoritative stored image,
including embedding metadata, already normalized by the commit coordinator.
A transaction may stage client edits and stored images in sequence; rollback
and publication remain transaction-wide. These runtime requests change no
Badger keys, stored entity payloads, WAL encoding or storage version. Cypher,
HTTP and Bolt syntax remains unchanged.

## UNIQUE claim retirement

Claim publication has one method,
`SchemaManager.ApplyUniqueValue(ctx, label, property, value, nodeID, action) error`, which
owns the constraint lookup, value-key projection and claim mutation under the
constraint's mutex. The former `RegisterUniqueValue` and `UnregisterUniqueValue`
methods and the `UniqueValueRegistrationPolicy` type are removed; forwarding
wrappers are not provided. Update direct Go callers:

- `RegisterUniqueValue(label, property, value, nodeID, policy)` maps directly to
  `ApplyUniqueValue(ctx, label, property, value, nodeID, action) error` with the same
  arguments, using `ValidatedUniqueClaim` or `RecoveredUniqueClaim`.
- `UnregisterUniqueValue(label, property, value, nodeID)` becomes
  `ApplyUniqueValue(ctx, label, property, value, nodeID, RetiredUniqueClaim)`.

Prepare callers pass their request context and handle the returned error;
canceled key preparation or admission leaves the current claim unchanged.
Accepted native publication and recovery explicitly use their completion
lifetime, so a canceled statement cannot interrupt an accepted frame.

Creation validation and UNIQUE cache rebuild APIs also take `ctx` first:
`ValidateConstraintOnCreationForEngine`,
`ValidatePropertyTypeConstraintOnCreationForEngine`,
`ValidateConstraintContractOnCreationForEngine`,
`RefreshUniqueConstraintValuesForEngine`,
`RefreshUniqueConstraintValuesForConstraintsForEngine`,
`SchemaManager.CheckUniqueConstraint`, and
`BadgerEngine.ValidateRelationshipConstraint`. The two Badger convenience
methods `ValidateConstraintOnCreation` and
`ValidatePropertyTypeConstraintOnCreation` are retired; callers use the
canonical engine validators with their context. A failed refresh preserves
existing caches. Replacing a selected constraint while its scan runs returns
`storage.ErrConflict`; retry against the current schema.

`UniqueValueAction` has three values. `ValidatedUniqueClaim` assigns the value
to the node unconditionally and is required for current validated mutations. The
canonical WAL recovery owner uses `RecoveredUniqueClaim`: historical images may
add a missing claim or retain the same owner, but cannot overwrite a different
loaded current holder. `RetiredUniqueClaim` deletes the value only while the
supplied node is its holder. Pass the retiring node's ID, not a fresh lookup of
the current holder: an older buffered delete or rename can then finish replay
without releasing a value already reused by a different committed node. Atomic
publication retires all predecessor claims before acquiring final claims. Normal
buffered writes publish once at logical ACK; physical drain does not republish
their historical claims. An action outside these constants is a programmer error
and panics rather than being treated as a publication mode.

These actions change only derived in-memory ownership, not persisted payloads,
persistent indexes, WAL formats or storage versions.

## Manual visibility

The optional `storage.ManualSuppressionEngine` capability now requires one method:
`SetManualSuppression(ctx context.Context, kind storage.StoredEntityKind, id string, suppressed bool) error`.
Replace node calls with `storage.StoredEntityNodeKind` and relationship calls with
`storage.StoredEntityEdgeKind`, converting typed IDs to `string` explicitly.
The separate `SetNodeManuallySuppressed` and `SetEdgeManuallySuppressed` methods
are removed; custom capability implementations must migrate too.

```go
err := visibility.SetManualSuppression(ctx, storage.StoredEntityNodeKind, string(nodeID), true)
```

`ctx` is the lifetime that executes the change: the `kp.suppress` statement for
an autocommit call, or the transaction's admitted commit when the change is
deferred to COMMIT. It owns admission, the buffered-order drain and the stored
body read; an admitted suppression writes its flag and publication in the store,
so a canceled context returns its own error and changes nothing.

Namespace wrappers preserve their existing ID qualification rules. WAL wrappers
forward the visibility change without adding a graph-write log entry. Persisted
suppression key bytes remain compatible, and the Cypher `kp.suppress` contract,
commit/rollback behavior and search refresh semantics are preserved.

UNIQUE lookups also have one canonical operation:
`LookupUniqueConstraintValue(label, property, value)` returns
`(nodeID, found, constraintExists, cacheComplete)`. The old three-result view,
`LookupUniqueConstraintValueForPlanning`, and the internal validation adapter
are retired. Consumers may trust a missing value only when `cacheComplete` is
true; a positive claim retains its existing meaning.
