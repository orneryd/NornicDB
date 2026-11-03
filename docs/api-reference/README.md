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

### Embedded Go storage and atomic writes

Local Cypher writes through `cypher.NewStorageExecutor(engine)` require a
transaction-capable storage owner before any mutation or WRITE procedure callback
runs. A custom `storage.Engine` exposing only the required interface can serve
read-only statements that need no transaction; writes return the same “engine does not support transactions” error as
`BEGIN`. There is no direct-write fallback. The existing transaction-storage
requirement error for `WITH EMBEDDING` remains unchanged.

Native `MemoryEngine` and `BadgerEngine` provide
`BeginTransaction() (*storage.BadgerTransaction, error)`. A transparent adapter
can retain the existing `storage.EngineUnwrapper` contract so capability and
namespace discovery reach that owner. An adapter that changes mutation semantics
must preserve those semantics in the transaction owner; exposing an inner engine
is not a substitute for making its writes atomic. Custom independent backends
without a compatible transaction owner must not accept local Cypher writes.

For example, a namespaced native engine supports atomic statements directly:

```go
engine := storage.NewMemoryEngine()
defer engine.Close()
scoped := storage.NewNamespacedEngine(engine, "tenant")
executor := cypher.NewStorageExecutor(scoped)
_, err := executor.Execute(ctx, "CREATE (:Document {id: 'one'})", nil)
```

Resolved remote/Fabric statements retain their existing delegated coordinator.
Nested procedure statements reuse their admitted statement transaction; this
requirement does not add a second transaction around a delegated owner.

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

### Embedded Go stored values (v1.4.4 migration)

Stored entity properties use their persisted representation when returned to Go
callers, including immediately after a write. Integer properties fitting a signed
64-bit integer are `int64`; larger `uint64` values retain their unsigned type.
For example, replace `node.Properties["age"].(int)` with
`node.Properties["age"].(int64)`. Homogeneous property lists can be typed slices,
such as `[]int64`, rather than `[]any`. Caller-owned input maps retain their values.

Hidden records still occupy their storage IDs. Recreating a stored node ID fails
with `storage.ErrAlreadyExists`. Retrying the identical hidden relationship is a
no-op that preserves its stored body; reusing its ID for another relationship is
rejected. Deleting a node also removes its hidden relationships. An update to a
node already deleted in the same transaction leaves it deleted.

The Badger cache helper now takes an optional type on the existing API:
replace `engine.InvalidateEdgeTypeCacheForType(typeName)` with
`engine.InvalidateEdgeTypeCache(typeName)`. Calling
`engine.InvalidateEdgeTypeCache()` still invalidates every type; an explicitly
empty type remains a no-op. HTTP and Bolt contracts are unchanged.

### Embedded Go role metadata (v1.4.4 migration)

`auth.ReadRoleCatalog(roles, entitlements)` is the role metadata snapshot owner.
Replace `roles.AllRoles()` with `auth.ReadRoleCatalog(roles, nil).RoleNames`, and
replace `entitlements.All()` with
`auth.ReadRoleCatalog(nil, entitlements).EntitlementOverrides`. Supply both stores
to obtain both views. Each supplied store is read under its own read lock; returned
slices and maps are independent. Overrides contain stored IDs; continue using
`auth.PermissionsForRole` for effective permissions. HTTP response shapes and
permissions remain unchanged.

## Common Questions

- "What endpoint should I call?" → [openapi.md](openapi.md)
- "Can I generate clients from this API?" → [openapi.yaml](openapi.yaml)
- "Which Cypher function should I use?" → [cypher-functions/README.md](cypher-functions/README.md)
- "Will my Neo4j driver work?" → [../neo4j-migration/feature-parity.md](../neo4j-migration/feature-parity.md)

## Related Docs

- Usage walkthroughs: [../user-guides/README.md](../user-guides/README.md)
- Troubleshooting by symptom: [../issues-index.md](../issues-index.md)
- Operations and deployment behavior: [../operations/README.md](../operations/README.md)
