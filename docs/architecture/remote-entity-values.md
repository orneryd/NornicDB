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


HTTP transactions request `row`, `graph`, and `rest` representations. The REST
representation identifies entity positions inside nested results, including empty
property maps; graph data preserves stable relationship endpoint IDs and path
entity metadata. Row data distinguishes ordinary maps from serializer envelopes.
An inconsistent supplied REST representation is rejected rather than guessed.

Peers that omit REST retain the legacy decoder for compatibility. That fallback
cannot reliably distinguish an empty ordinary map from an empty graph entity
when flattened metadata omits container positions. Use a peer that supplies REST
for reliable nested empty-value provenance. Queries are never retried to obtain
another representation, because they may perform writes.

For the checked-in `remote_http_empty_entities.json` Neo4j 5.26.30 fixture,
compact response data increases from 1,504 bytes (`row` + `graph`) to 5,050 bytes
with REST, or 3.36 times the wire size. This fixture-specific correctness cost is
not a heap, latency, or throughput measurement. The decoder reads the REST
identity and data fields without retaining its navigation fields.
