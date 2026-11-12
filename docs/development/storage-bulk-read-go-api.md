# Bulk storage reads carry the caller context

For v1.4.4, bulk storage reads take `context.Context` as their first argument.
Update implementations, optional capability interfaces, mocks and callers together:
a mock with the old signature may silently stop implementing an optional interface.
`GetAllNodes` is retired; use `AllNodes(ctx)` and handle its error.

| Contract | Affected operations |
| --- | --- |
| `storage.Engine` | `GetNodesByLabel`, `ReadAdjacentEdges`, `GetEdgesByType`, `AllNodes`, `AllEdges`, `BatchGetNodes` |
| Adjacency and projected reads | `ReadAdjacentEdges` in every direction (`AdjacentEdgeRead`), batch reads without embeddings, projected label/prefix streams, prefix streams without embeddings |
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
    edges, err := engine.ReadAdjacentEdges(ctx, storage.AdjacentEdgeRead{Node: id})
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
`ErrTransactionClosed` even for a canceled request). The label, whole-graph and
type streams deliver each record with that lock released, and the retake after
each callback carries the same caller's context: a request canceled while its
visitor ran ends when the visitor returns instead of waiting for another holder,
and a transaction the client closed or detached while its visitor ran reports
`ErrTransactionClosed` the same way, before that retake waits. A visitor that
panics keeps its own panic and leaves no hold behind: the read releases the lock
only while it owns it. A visitor that intentionally returns `ErrIterationStopped`
keeps the existing early-stop result even if it cancels its own context;
transaction closure still reports an error.
An individual allocation, opaque library call or acquisition of a lock that is
not the transaction's own read lock is not made preemptible by this API;
workload measurements remain necessary.

Point and first-row APIs retain their signatures. A genuinely context-free staging
or maintenance entry explicitly supplies its own lifetime at the read site. This
is different from commit validation before acceptance: that validation carries the
publication caller's context. Accepted commit work retains its existing completion
owner and must not be abandoned because a statement's read context expires.

This is a Go embedding/interface migration and requires rebuilding plugins and
custom storage implementations. It changes no Badger keys, persisted payloads,
WAL format or storage version, and requires no database migration. Cypher syntax,
HTTP and Bolt value contracts are unchanged by the storage API migration.

### Remote native entry lifetime

Remote point reads, scalar counts and entity mutations derive the existing
30-second `RemoteRequestTimeout` context at their entry and release it on
return. Private query/count helpers consume that context directly; they do not
create another timeout. Remote Engine methods that accept a context require it
to be non-nil; reads preserve its values, cancellation and any earlier deadline.
`QueryCypher` and `BeginCypherTx` likewise require a non-nil context. This
ownership migration does not change wire queries, result cardinality, access
mode or stored data.

### Search consumers

Candidate population and IVF-HNSW searches forward the same operation context
through their serving-view callbacks. Local and LLM rerankers also require a
non-nil context; they no longer replace nil with a background context. Admission,
model scoring and inference retain the caller's values and cancellation.

## Adjacent-edge request migration

Directional and header-only reads now share `storage.AdjacentEdgeRead`. Use `ReadAdjacentEdges(ctx, request)` on an engine and `tx.ReadEdges(ctx, request)` on a native transaction. `Direction` selects `AdjacentOutgoing`, `AdjacentIncoming`, `AdjacentBoth` or `AdjacentBetween`; `Types` selects a type union and `Other` supplies the other endpoint for a between request. Empty `Types` means all types. `Headers: true` permits omitting relationship properties; consumers that need properties must request full records. Visibility decisions may still require full records for a header request.

```go
edges, err := engine.ReadAdjacentEdges(ctx, storage.AdjacentEdgeRead{
    Node: id, Direction: storage.AdjacentOutgoing,
    Types: []string{"MEMBER_OF"}, Headers: true,
})
```

Stored/integrity readers use `ReadStoredAdjacentEdges`; versioned readers use `ReadAdjacentEdgesVisibleAt` with the existing version argument. This replaces the old outgoing/incoming/header entry points, their capability interfaces and directional enum. Go consumers/plugins must migrate and rebuild; no forwarding aliases remain. Transaction between requests and write-behind retain their existing view semantics. Badger keys, persisted records, WAL and storage version are unchanged.
