# Go source-stage migration

Fabric selector and planner APIs require the original context and an explicit
source stage. Client text uses the public `fabric.RawSource` enum alias:

```go
clause, remaining, found, err := fabric.ParseUseClause(ctx, query, false, fabric.RawSource)
fragment, err := planner.Plan(ctx, query, "portfolio", fabric.RawSource)
statement, err := planner.SingleGraphStatement(ctx, query, fabric.RawSource)
```

Raw selector results retain original argument spelling, remaining text and byte
coordinates. Names are decoded. Planning projects raw text once and produces
accepted fragment queries. Compiler consumers of existing accepted text choose
`fabric.AcceptedSource`; generated backslash-u sequences must not be admitted as
new client escapes.

`RemoteFragmentExecutor.Execute` and `ExecuteRows` continue to accept raw remote
source with the required explicit remote access mode. Fabric's import owner emits
its completed accepted fragment to raw before the exact-source mode policy and
transport. Import bindings remain parameters. Local accepted import statements
are not emitted at that remote boundary. No stage is inferred from source text.
