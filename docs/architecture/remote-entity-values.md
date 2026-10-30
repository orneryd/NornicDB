# Remote graph entity values

Remote query results retain graph provenance through `storage.RemoteEntityValue`.
The remote storage adapter creates this named map only when the Bolt driver
returns an actual node or relationship, or when an HTTP transaction response
identifies an entity through graph metadata. Ordinary result maps remain ordinary
maps, including maps with keys named `id`, `elementId`, `labels`, or `type`.

When forwarding remote query results into Fabric bindings, preserve the returned
value instead of rebuilding its map:

```go
columns, rows, err := remote.QueryCypher(ctx, "MATCH (n:Doc) RETURN n", nil, storage.RemoteAccessRead)
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

Remote graph protocol output preserves graph identity: genuine marked nodes,
relationships, and paths use Bolt entity structures and HTTP transaction
row/meta/graph entries. Source element IDs and relationship endpoints remain
unchanged. Numeric source IDs are retained when the transport supplies them;
older hand-built markers without those IDs report the explicit unknown value
`-1`. A property named `id` never substitutes for a numeric source ID. Ordinary
maps and lists keep their container identities.

Go callers forwarding remote values should preserve their named types:

```go
columns, rows, err := remote.QueryCypher(ctx,
    "MATCH p=(a:Doc)-[:LINKS]->(b) RETURN p", nil, storage.RemoteAccessRead)
if err != nil {
    return err
}
path := rows[0][0].(storage.RemotePathValue)
nodes, relationships, valid := path.Parts()
_ = nodes
_ = relationships
_ = valid
bindings := map[string]interface{}{columns[0]: path}
_ = bindings // Forward the marked path into another query unchanged.
```

`Parts()` validates the node/relationship sequence and endpoint connections.
A zero-hop path has one node and no relationships. A nil carrier is null;
an empty carrier has no valid path and is never treated as an empty list.
Cypher `length()`, `nodes()`, and `relationships()` use these same validated
parts; `size()` and list predicates reject a non-null path.

This corrects an observable Go API defect: actual Bolt paths previously escaped
as driver `dbtype.Path` structs, while actual HTTP paths became unnamed lists.
Callers asserting those old concrete types must use `storage.RemotePathValue`
and `Parts()`. Ordinary result lists remain unchanged. Node and relationship
markers retain their existing public map fields.

Transport-produced markers also carry private numeric identity metadata in Go.
Their custom JSON marshaler excludes that metadata while keeping ordinary
property keys intact. Direct `json.Marshal()` on a marked path produces its
ordered public entity envelopes, with no raw driver structs or private IDs.
Preserving the named marker also preserves this JSON filtering; an explicit
conversion to a plain map discards both graph provenance and its custom
marshaling behavior. For plain-map JSON interop, sanitize the sealed implementation metadata first:

```go
public := storage.PublicRemoteMap(map[string]interface{}(entity))
encoded, err := json.Marshal(public)
_ = encoded
_ = err
```

`PublicRemoteMap` preserves all ordinary user map keys, returns ordinary maps without copying, and never gives a plain map graph identity. Direct `json.Marshal(map[string]interface{}(entity))` does not invoke the marker marshaler and can expose implementation metadata. Protocol output uses entity structures, rather than the
former remote-envelope-as-ordinary-map representation.


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

### HTTP positional provenance and runtime kinds

NornicDB transaction responses now include the `rest` representation when requested
through `resultDataContents`, including entity envelopes inside maps and lists and
ordered path descriptors. RemoteEngine requests this alongside `row` and `graph`,
so an ordinary empty map cannot steal an entity's flattened metadata. A supplied
REST column list with a different length from the row is rejected before entity
metadata advances; an absent legacy sidecar retains the legacy fallback.
Relationship graph entries carry both element IDs and legacy numeric endpoint IDs,
including relationship-only results, for forwarding to older Bolt versions.
REST `self` values identify legacy entities; this representation does not add
legacy HTTP CRUD/navigation routes.

Marked remote entities participate in the shared Cypher type, list and ordering
rules as nodes or relationships. Remote paths remain paths: `head`, `last` and
`reverse` reject them rather than treating their Go representation as a list.
Entity ordering follows source identity, never its mutable properties; legacy ID
ties across source elements use element IDs to keep a stable order. Ordinary maps
and lists retain their existing types and behavior. Malformed remote path/REST
errors preserve storage localization message identities.

## Explicit remote access mode

Go callers must pass `storage.RemoteAccessRead` or `storage.RemoteAccessWrite`
to `RemoteEngine.QueryCypher`. The mode selects the Bolt session and managed
transaction; zero or other values are rejected before transport dispatch.
Storage does not parse or guess the query's intent. For example:

```go
_, _, err := remote.QueryCypher(ctx,
    "MATCH (n:Doc {id: $id}) SET n.title = $title",
    map[string]interface{}{"id": "doc-1", "title": "Revised"},
    storage.RemoteAccessWrite)
```

Fabric supplies the exact remote fragment's compiled grammar effects, including
its generated import source. Opaque remote procedures and unsupported remote
dialects use write routing and remain subject to the remote parser. Explicit
remote transactions and batches retain their existing write session lifetime;
`RemoteCypherTx.QueryCypher` keeps its existing arguments. Completed remote
query summaries continue to determine actual distributed write participation.
