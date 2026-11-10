# Embed NornicDB in a Go application

Use `pkg/nornicdb` for a managed in-process database. It owns storage, WAL recovery, configured background work, and shutdown. Use HTTP/Bolt drivers when the application should connect to a separately operated server; no NornicDB listener is required for Go embedding.

## Dependency and build

The module path is `github.com/orneryd/nornicdb`. Select a published tag compatible with the application rather than depending on a moving branch. Use the Go version required by that tag's `go.mod`; the current source requires Go 1.27.3.

```bash
go mod init example.com/graph-app
go get github.com/orneryd/nornicdb@<release-tag>
go build -tags noui .
```

Replace `<release-tag>` with the chosen tag. `noui` excludes bundled UI assets. Optional `localllm` builds require their matching native library prerequisites; an external embedding provider does not require building local inference. See [source build setup](setup.md).

## Open, query, and close

```go
package main

import (
    "context"
    "errors"
    "fmt"
    "log"
    "time"

    "github.com/orneryd/nornicdb/pkg/nornicdb"
)

func run() (err error) {
    cfg := nornicdb.DefaultConfig()
    cfg.Database.StrictDurability = true
    cfg.Memory.EmbeddingEnabled = false
    cfg.Memory.DecayEnabled = false
    cfg.Storage.Mode = "low"

    db, err := nornicdb.Open("./app-data", cfg)
    if err != nil {
        return err
    }
    defer func() { err = errors.Join(err, db.Close()) }()

    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()
    result, err := db.ExecuteCypher(ctx,
        "MERGE (n:Example {id: $id}) RETURN n.id AS id",
        map[string]interface{}{"id": "one"})
    if err != nil {
        return err
    }
    fmt.Println(result.Columns, result.Rows)
    return nil
}

func main() {
    if err := run(); err != nil {
        log.Fatal(err)
    }
}
```

The directory is persistent storage. An empty directory argument selects memory storage; it does not provide crash recovery. A persistent directory has one storage owner: do not open it from two processes or close its underlying engine while the DB still uses it.

`DefaultConfig` returns defaults; it does not load YAML or environment overrides. Use `config.LoadFromFile(path)` from `pkg/config` to load deployment configuration and handle its error, then set application-owned overrides before `Open`. Do not construct a sparse literal config and assume all defaults were supplied. `Open` can validate or migrate storage, replay accepted writes, and start configured workers. Authorize format upgrades explicitly and keep a pre-upgrade backup.

Embedding does not put the HTTP/Bolt authentication or request middleware in front of `ExecuteCypher`. The host application owns authorization, tenant routing, parameter validation, request budgets, and safe exposure of results. Pass parameters separately from query text.

## Canonical API boundaries

| Need | API/owner |
| --- | --- |
| Database lifetime | `nornicdb.DefaultConfig`, `nornicdb.Open`, `DB.Close`. |
| Autocommit Cypher | `DB.ExecuteCypher(ctx, query, params)`; result has `Columns`, `Rows`, `Metadata`. |
| Typed row projection | Package function `nornicdb.ExecuteCypherTyped[T](db, ctx, query, params)`, not a generic DB method. |
| Namespaced storage integration | `DB.GetStorage()`; retain wrappers and namespace. |
| Session-owned Cypher transactions | A dedicated `cypher.NewStorageExecutor` over the managed storage; keep its transaction lifecycle private to one owner. |
| Native read/write integration | The transaction-capable `storage.Engine` and capability interfaces; preserve the [atomic write contract](../api-reference/README.md#embedded-go-storage-and-atomic-writes). |
| Backup/replacement | `DB.Backup(ctx, path)` / `DB.Restore(ctx, path)`; both report errors. |
| DB-owned background task cancellation | `DB.LifetimeContext()` and the DB's lifetime rules. Request contexts must not become the lifetime of accepted recovery work. |
| Application admission | `DB.MemoryPolicy()`; nil when disabled. Runtime GC targets require separate process setup. |

`GetBaseStorageForManager` is for database-manager integration, not a shortcut around namespaces, WAL, or replication. Custom wrappers that alter mutations must preserve their semantics through transaction ownership; exposing an inner engine is not enough to make writes atomic.

## Explicit transaction ownership

The shared executor behind `DB.ExecuteCypher` rejects bare transaction-control statements. Do not send `BEGIN` through it and share the DB across requests as though it were a session.

A local transaction owner can create `cypher.NewStorageExecutor(db.GetStorage())`, run `BEGIN`, its statements, then `COMMIT` or `ROLLBACK` sequentially on that same executor. Do not share the executor across concurrent transactions. Use `cypher.WithTransactionControl(ctx)` for lifecycle commands when the caller has marked ordinary text with `WithClientStatement`. Roll back on every early return; use a separate bounded cleanup context when the request context has been canceled. A statement error can end the underlying transaction before a later commit attempt.

A bare executor is a lower-level API: it does not automatically inherit every DB/server resolver, search service, authentication policy, or custom procedure binding. For complete protocol sessions and multi-database routing, prefer the existing Bolt/HTTP server integration and driver APIs. The [API reference](../api-reference/README.md) records required context, query-reader, storage, catalog, and plugin interfaces.

## Memory and durability

`Open` resolves storage/WAL options but is not the `serve` command's process-runtime bootstrap. For the CLI's GC policy call these before opening, once for the host process:

```go
cfg.Memory.ResolveRuntimeLimit()
cfg.Memory.ApplyRuntimeMemory()
```

GC settings and process transaction defaults are shared by embedded DB instances. A successful open publishes its transaction defaults; a failed open does not replace the defaults of an already-serving instance. Application admission uses physical cgroup readings; do not enable it on a host without usable readings and assume it falls back to RSS.

See [memory accounting](../operations/memory-management.md), [low-memory profiles](../operations/low-memory-mode.md), and [durability boundaries](../operations/durability.md). A Go heap target, a buffer cap, and strict durability solve different problems.

## Extension points and upgrades

Use documented procedure/plugin registrations, `storage.Engine` capabilities, and the existing query-cache/settings resolvers. Consult the [embedded Go API reference](../api-reference/README.md) before upgrading: storage update requests, persisted property types, cancellation-aware readers, and metadata APIs have changed.

Persisted integer properties commonly return `int64`; homogeneous lists can be typed slices. Do not assert that every property is a Go `int` or every list is `[]interface{}`. Handle returned errors, including transient conflicts, capacity rejection, and accepted-but-uncertain outcomes; retry only when the operation is safe to repeat.

## Implementation reference

[DB open/config](../../pkg/nornicdb/db.go), [query/results and backup APIs](../../pkg/nornicdb/db_admin.go), [lifetime](../../pkg/nornicdb/db_lifecycle.go), [executor ownership](../../pkg/cypher/executor.go), and [transaction lifecycle](../../pkg/cypher/transaction.go).
