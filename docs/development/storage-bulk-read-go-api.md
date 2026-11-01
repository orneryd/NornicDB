# Bulk storage reads carry the caller context

For v1.4.4, bulk storage reads take `context.Context` as their first argument.
Update implementations, optional capability interfaces, mocks and callers together:
a mock with the old signature may silently stop implementing an optional interface.
`GetAllNodes` is retired; use `AllNodes(ctx)` and handle its error.

| Contract | Affected operations |
| --- | --- |
| `storage.Engine` | `GetNodesByLabel`, `GetOutgoingEdges`, `GetIncomingEdges`, `GetEdgesBetween`, `GetEdgesByType`, `AllNodes`, `AllEdges`, `BatchGetNodes` |
| Adjacency and projected reads | `GetAdjacentEdges`, batch reads without embeddings, projected label/prefix streams, prefix streams without embeddings |
| Latest-visible enumeration | `BatchGetNodesLatestVisible` and `IterateLatestVisibleNodes` |
| Scoped and MVCC reads | Context-first bulk label/type/adjacency reads, including their namespace and snapshot variants |
| Stored entity readers | Stored bulk label/adjacency reads and label/type streams, including scoped variants |
| Optional capability interfaces | `storage.IncidentEdgeLister.IncidentEdges`, the stored-inclusive node incidence read a DELETE uses |

The authoritative signatures are in [types.go](https://github.com/orneryd/NornicDB/blob/release/v1.4.4/pkg/storage/types.go) and
[stored_entity_reader.go](https://github.com/orneryd/NornicDB/blob/release/v1.4.4/pkg/storage/stored_entity_reader.go). Existing
context-bearing streams keep their caller context. Namespace, transaction,
projection, remote and other adapters must forward the actual context instead of
substituting a background context. Remote reads additionally retain their existing
configured operation timeout; an earlier caller deadline or cancellation wins.

```go
func outgoing(ctx context.Context, engine storage.Engine, id storage.NodeID) ([]*storage.Edge, error) {
    edges, err := engine.GetOutgoingEdges(ctx, id)
    if err != nil {
        return nil, err
    }
    return edges, nil
}
```

A DELETE's incidence read (a node's relationships, those the visibility layer
hides included) is a bulk read: `IncidentEdges(ctx, nodeID)` takes the caller's
context in both its native and Cypher transaction-wrapper implementations, so a
canceled request stops the enumeration. The DELETE
check and the DETACH DELETE count run before the statement is accepted, so this
is request-owned work, not an accepted write's context-free read.

Canceled reads return the caller's cancellation/deadline error without publishing
successful partial collections or partially filled caches. Native loops poll at a
bounded record interval, including hot-cache copies, pending overlays and wrapper
copies. Cancellation must stop enumeration itself, not merely suppress delivery.
A transaction bulk read — the incidence read, label and whole-graph populations,
type streams and native adjacency reads — is admitted with the caller's context at
the transaction's own read lock, so a request canceled or expired while it waits
for that lock returns its cancellation/deadline error instead of waiting for the
current holder. The
holder is never preempted, the canceled waiter leaves no hold behind, and the
transaction's own state errors keep their precedence (a released client reports
`ErrTransactionClosed` even for a canceled request). An individual allocation,
opaque library call or acquisition of a lock that is not that admission boundary
is not made preemptible by this API; workload measurements remain necessary.

Point and first-row APIs retain their signatures. A genuinely context-free staging
or maintenance entry explicitly supplies its own lifetime at the read site. This
is different from commit validation before acceptance: that validation carries the
publication caller's context. Accepted commit work retains its existing completion
owner and must not be abandoned because a statement's read context expires.

This is a Go embedding/interface migration and requires rebuilding plugins and
custom storage implementations. It changes no Badger keys, persisted payloads,
WAL format or storage version, and requires no database migration. Cypher syntax,
HTTP and Bolt value contracts are unchanged by the storage API migration.
