# Catalog mutation contexts

In v1.4.4 the Go database-manager constructor and catalog mutators take an explicit
`context.Context` first. Migrate callers and interface implementations together;
there is no context-free compatibility entry point.

Use the owning database lifetime for server startup:

```go
manager, err := multidb.NewDatabaseManager(db.LifetimeContext(), db.GetBaseStorageForManager(), cfg)
if err != nil {
    return err
}
```

Use the operation's context for a catalog request:

```go
if err := manager.CreateDatabase(request.Context(), multidb.DatabaseCreateRequest{Name: "tenant_a"}); err != nil {
    return err
}
if err := manager.CreateAlias(request.Context(), "current_tenant", "tenant_a"); err != nil {
    return err
}
```

The migrated methods are:

| Operation | Context-first call |
| --- | --- |
| Constructor | `NewDatabaseManager(ctx, inner, config)` |
| Create/drop database | `CreateDatabase(ctx, DatabaseCreateRequest{Name: name, Options: initialOptions})`, `DropDatabase(ctx, name)` |
| Database status | `SetDatabaseStatus(ctx, name, status)` |
| Create/drop alias | `CreateAlias(ctx, alias, database)`, `DropAlias(ctx, alias)` |
| Limits | `SetDatabaseLimits(ctx, database, limits)` |
| Options | `SetDatabaseOption(ctx, database, key, value)` |
| Create/drop composite | `CreateCompositeDatabase(ctx, name, refs)`, `DropCompositeDatabase(ctx, name)` |
| Constituent membership | `AddConstituent(ctx, composite, ref)`, `RemoveConstituent(ctx, composite, alias)` |

Cypher's `DatabaseManagerInterface` and `DatabaseOptionsManager`, their HTTP,
Bolt and GraphQL adapters, and Qdrant's `Config.DropDatabase` callback carry the
same operation context. A custom Qdrant drop callback now has type
`func(context.Context, string) error`. Forward that context through the callback;
do not replace a request context with a background or service-lifetime context.

Read-only manager APIs such as `GetStorage`, `ResolveDatabase`, `ListDatabases`
and `DatabaseOptions` keep their existing signatures.

Catalog publication follows the authoritative storage commit receipt. Cancellation
before acceptance must not publish a candidate. Once storage has accepted an
operation, a canceled waiter does not establish rollback: inspect a typed
`storage.AcceptedCommitError` and its state before deciding how to recover or retry.
A pending DROP retains its durable intent and namespace fence until completion;
retrying that DROP or writable startup recovery completes the same administrative
operation. Do not recreate a namespace while its DROP is pending.

Standard database creation uses `multidb.DatabaseCreateRequest{Name, Options}`.
Pass a Name-only request when there are no initial options. Initial options are
validated and copied into the same detached catalog candidate as the name; one
storage receipt publishes both. Cypher `CREATE DATABASE … OPTIONS` makes one
creation call. `SetDatabaseOption` remains the later ALTER mutation and has its
own publication boundary. There is no name-only method overload or compatibility
wrapper. A failed preacceptance create publishes neither name nor options;
an accepted acknowledgement error retains the applied or pending receipt state.

Other integrations that make multiple manager calls must account for their
separate publication boundaries. This does not establish atomic rollback for
`CREATE OR REPLACE`'s physical DROP and subsequent CREATE.
