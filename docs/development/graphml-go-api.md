# GraphML export Go API

In v1.4.4, `apocexport.GraphML(nodes, relationships)` returns `(string, error)`.
Go consumers must handle the error; there is no string-only compatibility entry
point. Bundled registration and `ToString(..., "graphml")` use this same owner.

```go
output, err := apocexport.GraphML(nodes, relationships)
if err != nil {
    return err
}
// output contains a complete XML document.
```

Property names, property string representations and relationship types preserve
XML-special characters and Unicode after XML decoding. XML whitespace is escaped
so attribute normalization and line-ending normalization do not change values.

Malformed UTF-8, unpaired UTF-16 units and characters outside XML 1.0's Char
production are rejected. They cannot be represented losslessly in that format.
The in-memory API returns an empty result with its error. File export validates
the document before opening its destination: rejected input neither creates a
new file nor changes an existing one. Write errors are returned to the caller;
this is not a guarantee of atomic replacement for an operating-system write
failure.

## File export Go API

The canonical file-writing entry points are context-first: `ToFile`, `JsonAll`,
`Csv`, `CypherAll` and `GraphMLAll` take the
caller's context as their first Go argument. `pkg/apocio` is the one owner of
that admission and destination: `AdmitExport` resolves the named path through the
caller's APOC export capability, and the admitted destination writes the bytes
beneath the configured export root. A context without an export capability
performs no write and reports the owner's error, and a caller that already
stopped writes nothing.

```go
if err := apocexport.GraphMLAll(ctx, nodes, relationships, "/path/to/graph.graphml"); err != nil {
    return err
}
```

The in-memory producers `Json`, `Cypher`, `GraphML` and `ToString` are unchanged
and stay context-free, so `apoc.Call` still invokes them without a context. A
query executor binds the statement context to a context-first file handler; a
standalone Go caller binds one explicitly with `apocio.WithCapability`. Root
`apoc.Call` cannot invoke a context-first file export, because the caller's
lifetime is not part of that surface.

Go callers of the retired forwarding `GraphMLData` function use `GraphMLAll`.
The public APOC names `apoc.export.graphMLAll` and `apoc.export.graphMLData`
both register that canonical implementation directly and remain supported.
An empty destination retains the established no-file behavior: no capability is
needed and nothing is written. The whole-graph file exports create the
destination's parent directories, and destination errors are returned to the
caller.

This change preserves the exporter's existing graph selection and document
shape. It does not introduce GraphML schema/key declarations or claim XSD
validation. No database keys, payloads, WAL formats or storage versions change.

## Measured export cost

Five isolated 200ms samples per workload on Apple M2 Max, Darwin/arm64, Go 1.27.3
compare the release implementation with this correction. Each graph has one
string property per node and relationship, one relationship per node, and either
ordinary text or XML markup/Unicode/whitespace. Medians:

| Graph | Text | Before ns/op | After ns/op | Before → after B/op | Before → after allocs/op |
| --- | --- | ---: | ---: | ---: | ---: |
| 1 node + 1 relationship | ordinary | 2,164 | 2,896 | 1,048 → 920 | 16 → 15 |
| 1 node + 1 relationship | markup | 2,627 | 3,561 | 1,097 → 1,681 | 16 → 16 |
| 100 nodes + 100 relationships | ordinary | 256,306 | 161,859 | 116,261 → 100,269 | 1,215 → 1,016 |
| 100 nodes + 100 relationships | markup | 227,947 | 175,468 | 149,762 → 175,582 | 1,216 → 1,018 |

The old markup result is malformed XML, so this comparison measures correction
cost rather than equivalent valid encoders. Escaping expands the output and
increases bytes in markup-heavy cases; the table does not attribute every extra
byte exclusively to the final output. Single-element validation/encoding adds
sub-microsecond latency. The CPU profile includes standard escaping and the
explicit XML-character validation; no custom competing XML escaper was retained.
These helper workloads do not establish whole-database throughput or every
property-type cost.

Reproduce with `go test -tags='noui nolocalllm' ./apoc/export -run='^$'
-bench='^BenchmarkGraphMLExport$' -benchmem -benchtime=200ms -count=5`.

## Canonical file helpers and aliases

Retired Go forwarding entry points migrate to canonical context-first calls:

| Retired Go helper | Canonical helper |
| --- | --- |
| `JsonData(nodes, rels, path)` | `JsonAll(ctx, nodes, rels, path)` |
| `CypherData(nodes, rels, path)` | `CypherAll(ctx, nodes, rels, path)` |
| `CsvData(nodes, path)` | `Csv(ctx, nodes, path)` |
| `CsvAll(nodes, ignoredRelationships, path)` | `Csv(ctx, nodes, path)` |
| `GraphMLData(nodes, rels, path)` | `GraphMLAll(ctx, nodes, rels, path)` |

Their bundled APOC names bind directly to the canonical functions. The bundled
`apoc.export.csvAll` handler now takes nodes and path; its ignored relationship
argument is removed. Native Cypher `CALL apoc.export.csv.all(...)` and the other
native procedure signatures are unchanged. There are no compatibility wrappers.
