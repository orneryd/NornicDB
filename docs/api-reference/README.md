# API Reference

This section is the canonical source for protocol and endpoint definitions.

## Start Here

- HTTP endpoint reference: [openapi.md](openapi.md)
- Machine-readable HTTP spec: [openapi.yaml](openapi.yaml)
- Cypher function index: [cypher-functions/README.md](cypher-functions/README.md)
- Bolt protocol notes: [bolt-protocol.md](bolt-protocol.md)

## Find by Interface

### Cypher

- Function catalog: [cypher-functions/README.md](cypher-functions/README.md)

### HTTP (Neo4j-compatible + Nornic extensions)

- Human-readable reference: [openapi.md](openapi.md)
- OpenAPI source: [openapi.yaml](openapi.yaml)

### Bolt

- Protocol compatibility notes: [bolt-protocol.md](bolt-protocol.md)
- Driver compatibility context: [../neo4j-migration/feature-parity.md](../neo4j-migration/feature-parity.md)

### GraphQL

- Practical usage guide: [../user-guides/graphql.md](../user-guides/graphql.md)

### Embedded Go query readers (v1.4.4 migration)

The query reader APIs take the operation's context directly. Existing Go callers
must add `ctx` to `cypher.QueryPermissionRequirements(ctx, query)`,
`executor.ResultColumns(ctx, query, scope, cypher25)`,
`cypher.TransactionDatabaseError(ctx, databases, txDB, target, query)` and
`analyzer.Analyze(ctx, query)`. This preserves request cancellation through the
same owners used during execution; HTTP, Bolt and CLI protocol behavior is unchanged.
Permission requirements are an intermediate result: use `AuthorizeQuery` for an
actual authorization decision rather than treating canceled classification as a grant.

`RegisteredProceduresForCalls(ctx, query)` replaces `RegisteredProcedureForCall`
and returns all registered declarations from grammar-owned CALL clauses, including
nested calls. Callers selecting one declaration must check the returned slice.
Strings and comments do not add calls. No procedure handler is exposed.

`NewStorageExecutor(store)` retains environment-derived cache defaults. Replace
`NewStorageExecutorWithQueryCachePolicy(store, entries, ttl)` with
`NewStorageExecutor(store, cypher.QueryCachePolicy{MaxEntries: entries, TTL: ttl})`.
Zero entries disables caching; non-positive TTL retains the configured/default
lifetime. For example:

```go
executor := cypher.NewStorageExecutor(store, cypher.QueryCachePolicy{
    MaxEntries: 100,
    TTL: time.Minute,
})
columns, err := executor.ResultColumns(ctx, "RETURN 1 AS value",
    resultmetadata.KnownScope(nil), false)
```

## Common Questions

- "What endpoint should I call?" → [openapi.md](openapi.md)
- "Can I generate clients from this API?" → [openapi.yaml](openapi.yaml)
- "Which Cypher function should I use?" → [cypher-functions/README.md](cypher-functions/README.md)
- "Will my Neo4j driver work?" → [../neo4j-migration/feature-parity.md](../neo4j-migration/feature-parity.md)

## Related Docs

- Usage walkthroughs: [../user-guides/README.md](../user-guides/README.md)
- Troubleshooting by symptom: [../issues-index.md](../issues-index.md)
- Operations and deployment behavior: [../operations/README.md](../operations/README.md)
