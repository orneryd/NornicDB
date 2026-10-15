# Remote graph entity values

Remote query results retain graph provenance through `storage.RemoteEntityValue`.
The remote storage adapter creates this named map only when the Bolt driver
returns an actual node or relationship, or when an HTTP transaction response
identifies an entity through graph metadata. Ordinary result maps remain ordinary
maps, including maps with keys named `id`, `elementId`, `labels`, or `type`.

When forwarding remote query results into Fabric bindings, preserve the returned
value instead of rebuilding its map:

```go
columns, rows, err := remote.QueryCypher(ctx, "MATCH (n:Doc) RETURN n", nil)
if err != nil {
    return err
}
bindings := map[string]interface{}{columns[0]: rows[0][0]}
_ = bindings // The remote node keeps its transport provenance.
```

Cypher `labels()`, `type()`, `id()`, and `elementId()` distinguish these entities
from user maps. `keys()` reads the entity's property keys. Ordinary maps keep all their keys,
including `_nodeId`, `_edgeId`, and `properties`; these names do not give a map
graph identity. A local node or
relationship continues to use its existing storage type. Converting a remote
value to `map[string]interface{}` intentionally discards its graph provenance;
this representation marker is not a Go authorization boundary.

HTTP and GraphQL JSON output retains the existing remote map shape, including
its remote `elementId`, `labels` or `type`, and nested `properties`. Bolt output
retains the previous map encoding. Remote endpoint functions retain their
existing null result instead of resolving remote IDs against a local database.
