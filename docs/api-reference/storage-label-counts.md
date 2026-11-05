# Go storage label counts

`LabelStatsEngine` is the optional fast label-count capability. Its context owns metadata admission, pending-write merging and any fallback scan:

```go
count, err := storage.CountNodesWithLabel(ctx, engine, "Person")
if err != nil {
    return err
}
```

Implementations use `NodeCountByLabel(ctx context.Context, label string) (int64, error)`. Namespace-aware providers implement `NamespaceLabelStatsProvider.NodeCountByLabelInNamespace(ctx context.Context, namespace, label string) (int64, error)`. Pass a nonnil request context to both. Cancellation returns the original context error; callers must handle errors before using a count.

Native metadata counters remain the fast path. Namespaced, WAL, database-size and transaction wrappers forward the same context and retain namespace, visibility and pending-write count semantics. Engines without a counter retain their scan path under that context.

Go implementations and callers must migrate the old context-free method signatures; no compatibility methods are retained. This changes Go embedding interfaces, not Cypher syntax, HTTP/Bolt responses or stored key/payload formats. A genuinely context-free embedding operation may select `context.Background()` at its own entry boundary.
