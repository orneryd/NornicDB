# Go caller contexts

Storage operations take a non-nil `context.Context` as their first argument. Pass the context that owns the operation through storage-backed helpers. Pure functions that only reshape supplied values keep their existing signatures.

For example, a node degree read returns both its value and its error:

```go
functions := node.New(backend)
degree, err := functions.Degree(ctx, graphNode, "KNOWS")
if err != nil {
    return err
}
```

Backend failures and cancellation travel through the helper's error result. A canceled read does not publish a partial collection or a successful zero value. Ordinary missing entities retain the helper's existing null, empty or zero result where that is its documented behavior. An empty successful collection remains distinct from an error.

The statement adapter supplies the context for registered handlers; Cypher argument lists do not gain a context argument. Direct Go registry callers supply all declared Go arguments, including the context.

A helper that performs several writes does not acquire a new atomicity guarantee. Cancellation stops subsequent preparation and operations, while writes already accepted by the backend keep their existing completion lifetime. An error does not establish that those writes were rolled back.

The in-memory backend follows native node deletion: deleting a node also removes its incident relationships. The admitted deletion finishes that mutation without asking the caller to admit each incident removal again.

`path.Functions.SubgraphAll(ctx, start, config)` returns `(map[string]interface{}, error)`. Its successful map contains `nodes` and `rels`; traversal order and relationship multiplicity are preserved. Registered calls return that same map, with no additional Cypher arguments. Cancellation or a backend failure returns an error without a partial map.
