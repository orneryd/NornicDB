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

`GraphMLAll` and `GraphMLData` write the same document. An empty destination
retains the established no-file behavior. JSON, CSV, Cypher and raw file exports
share the existing directory/file-writing owner and report destination errors.

This change preserves the exporter's existing graph selection and document
shape. It does not introduce GraphML schema/key declarations or claim XSD
validation. No database keys, payloads, WAL formats or storage versions change.
