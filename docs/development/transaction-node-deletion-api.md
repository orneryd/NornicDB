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

## Transaction node updates

Replace `tx.UpdateNode(node)` with `tx.UpdateGraphNode(node, storage.ClientNodeEdits)`.
The transaction's canonical update method takes an image policy for each operation.
Use `storage.StoredNodeImages` only for a complete authoritative stored image,
including embedding metadata, already normalized by the commit coordinator.
A transaction may stage client edits and stored images in sequence; rollback
and publication remain transaction-wide. `storage.Engine.UpdateNode` is unchanged.

## Manual visibility

The optional `storage.ManualSuppressionEngine` capability now requires one method:
`SetManualSuppression(kind storage.StoredEntityKind, id string, suppressed bool) error`.
Replace node calls with `storage.StoredEntityNodeKind` and relationship calls with
`storage.StoredEntityEdgeKind`, converting typed IDs to `string` explicitly.
The separate `SetNodeManuallySuppressed` and `SetEdgeManuallySuppressed` methods
are removed; custom capability implementations must migrate too.

```go
err := visibility.SetManualSuppression(storage.StoredEntityNodeKind, string(nodeID), true)
```

Namespace wrappers preserve their existing ID qualification rules. WAL wrappers
forward the visibility change without adding a graph-write log entry. Persisted
suppression key bytes remain compatible, and the Cypher `kp.suppress` contract,
commit/rollback behavior and search refresh semantics are preserved.
