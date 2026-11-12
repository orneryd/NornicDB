# Go procedure node-image contracts

`ProcedureSpec.NodeImages` declares the storage image a native handler needs for node arguments, including nodes nested in lists or maps. It does not change Cypher syntax or stored data.

The zero value, `FullNodeImages`, preserves the native contract: node arguments materialized from storage retain their stored embedding vectors. Existing user and external built-in registrations keep this default. Unkeyed Go struct literals must include the new field or move to keyed literals.

A handler that only reads node identity, labels, properties and settled embedding metadata can declare `MetadataNodeImages`:

```go
spec := cypher.ProcedureSpec{
    Name: "app.nodeTitle",
    Mode: cypher.ProcedureModeRead,
    NodeImages: cypher.MetadataNodeImages,
    MinArgs: 1, MaxArgs: 1,
    Returns: []cypher.ProcedureColumn{{Name: "title", Type: "STRING"}},
}
```

Metadata mode avoids decoding separately stored vectors. It does not authorize reading missing vector payloads. Handlers that inspect those vectors, forward nodes to unknown native code or execute caller-supplied statement text must retain `FullNodeImages`. Unsupported contract values are rejected during registration.

The admitted statement owns the read policy, including requirements of later consumers in nested clauses and database selections. Vector scoring retains its own vector inputs; its returned row follows the statement's image policy. A missing vector is never repaired by reading the same logical node ID from a newer database snapshot or a different constituent.

This additive Go registration field requires rebuilding embedding applications and plugins using the changed type. Badger keys, payload encodings, WAL format and storage version are unchanged.
