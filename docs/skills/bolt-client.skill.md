---
name: nornicdb-bolt-client
description: Drive NornicDB from a Bolt client (`neo4j-go-driver/v5` or any other Neo4j Bolt driver). Use when classifying retryable vs permanent errors, sizing UNWIND/MERGE batch writes, configuring connection defaults, or implementing retry logic against NornicDB. NornicDB speaks the same wire as Neo4j 5; this skill calls out the small, stable surface a client must pin.
---

# NornicDB Bolt Client (Cypher API)

NornicDB exposes Bolt on port `7687` and is wire-compatible with Neo4j 5. Any upstream Bolt driver works — `neo4j-go-driver/v5`, `neo4j-driver` (Python), `neo4j-javascript-driver`, etc. This skill is what a client must pin to drive NornicDB safely under concurrency.

## Connection

| Setting | Value |
|---|---|
| URI | `bolt://<host>:7687` |
| Driver | any Neo4j 5 Bolt driver (do not vendor a NornicDB-specific fork) |
| Database name | the literal database name; address it directly via `Session.Run(... database=<name>)` |
| HTTP port (search REST, admin) | `7474` |

Server defaults (parsed from `LoadDefaults()` — these are stable):

| Field | Default | Notes |
|---|---|---|
| `Database.AsyncWritesEnabled` | `true` | Async write-behind is on by default. Set `NORNICDB_ASYNC_WRITES_ENABLED=false` if you need synchronous commit so a queue can drain to a known terminal state. |
| `Database.PersistSearchIndexes` | `false` | Search-index persistence is opt-in. Set `NORNICDB_PERSIST_SEARCH_INDEXES=true` if you want BM25/vector indexes to survive restart. |
| `Auth.Enabled` | `false` | Auth is off by default. Production deployments enable via the YAML `auth:` field. |
| `Server.BoltPort` | `7687` | |
| `Server.HTTPPort` | `7474` | |

## Error classes you will see

NornicDB returns Neo4j-shaped error codes (`Neo.ClientError.*`, `Neo.TransientError.*`) plus a stable message text. **You can classify by either the code or the substring; the wire shape is intentionally consistent across both commit paths.**

### Retry these — transient

| Wire shape | Code class | When |
|---|---|---|
| `commit failed: conflict: node <id> changed after transaction start` | `Neo.TransientError.Transaction.Outdated` | Optimistic write conflict on a node — another transaction committed first. |
| `commit failed: conflict: edge <id> changed after transaction start` | `Neo.TransientError.Transaction.Outdated` | Same, for edges. |
| `commit failed: conflict: node <id> has adjacent edge <eid> changed after transaction start` | `Neo.TransientError.Transaction.Outdated` | Same, propagated through an adjacent edge. |
| `... conflict: edge <id> changed after transaction start` (statement-time, no `commit failed:` prefix) | `Neo.TransientError.Transaction.Outdated` | `MERGE ... SET` on a relationship a peer committed after your transaction began — the edge is live at latest-committed state but invisible to your snapshot. Retry on a fresh transaction converges. |
| `... waiting for transaction lock` | `Neo.TransientError.Transaction.DeadlockDetected` | Lock contention. |
| `commit failed: constraint violation: ... already exists` **AND every statement in the group is `MERGE`-shaped** | `Neo.ClientError.Transaction.TransactionCommitFailed` | Two writers raced on the same `MERGE (n {uid:...})`. See "MERGE under concurrent writers" below. |

Both classifier paths work:

```go
import "errors"
import "github.com/neo4j/neo4j-go-driver/v5/neo4j"

var nErr *neo4j.Neo4jError
if errors.As(err, &nErr) {
    if strings.HasPrefix(nErr.Code, "Neo.TransientError.") { /* retry */ }
}

// or substring
if strings.Contains(err.Error(), "changed after transaction start") { /* retry */ }
if strings.Contains(err.Error(), "commit failed") &&
   strings.Contains(err.Error(), "constraint violation") &&
   strings.Contains(err.Error(), "already exists") {
    /* retry only if every statement in the group is MERGE-shaped */
}
```

### Do not retry — permanent

- `Neo.ClientError.Statement.SyntaxError` / `Neo.ClientError.Schema.*` — fix the query.
- `commit failed: constraint violation: ...` on a `CREATE`/`SET`-only group — re-running double-applies. Surface to the caller.
- Anything not in the transient table above.

## MERGE under concurrent writers

`MERGE (n {uid: ...}) SET ...` against the same `uid` from two writers can produce a commit-time UNIQUE on the loser:

```
commit failed: constraint violation: Constraint violation (UNIQUE on T.[uid]): Node with uid=X already exists (nodeID: <db>:<node-id>)
```

This is the storage-correct outcome. NornicDB pins these guarantees:

1. **Idempotent retry** — re-running the failed `MERGE` matches the now-committed node and applies the loser's `SET` cleanly.
2. **Same wire shape on both commit paths** — implicit autocommit and explicit `BEGIN/COMMIT` produce identical `commit failed: ...` strings, so one classifier handles both.
3. **MERGE-only retry rule** — the consumer should retry the whole group only when **every statement in the group contains `MERGE`**. A mixed group (CREATE/DELETE/SET-only alongside MERGE) re-run after partial success could double-apply effects; surface those instead.

The wire shapes are pinned by regression tests in `pkg/cypher/commit_failure_message_contract_test.go`, `pkg/storage/conflict_message_contract_test.go`, and `pkg/cypher/merge_idempotency_under_concurrent_test.go`.

## Sizing UNWIND/MERGE batches

Bound your batches. The right size depends on label cardinality and downstream-fan-out, but as a starting point:

| Shape | Rows per batch |
|---|---|
| Single-node `UNWIND ... MERGE ... SET +=` | 100–2000 |
| `UNWIND` driving N MATCHes + M CREATEs (bulk seeder) | 100–2000 |
| Fixed-depth chain materialization | 100–500 |
| High-cardinality canonical write (per-label cap) | 15–100 |

The Cypher executor recognizes specialized hot paths (`UnwindSimpleMergeBatch`, `UnwindMergeChainBatch`, `UnwindFixedChainLinkBatch`, `UnwindMultiMatchCreateBatch`, `CompoundQueryFastPath`). To stay on the fast path:

- Keep traversal upper bounds (`*1..6`) as **literals** in query text, not parameters.
- Don't mix `RETURN`/`WITH`/nested `UNWIND` inside a bulk-seeder MATCH/CREATE statement.
- Reuse the exact query text across calls; only parameters change. Trivial whitespace edits defeat plan caching.
- Wrap deletes with `WITH n LIMIT k DETACH DELETE n` and loop client-side rather than one unbounded delete.

For the catalog of hot-path shapes see [`cypher-queries.skill.md`](cypher-queries.skill.md).

## Concurrent fan-out

Disjoint MERGE keys across concurrent transactions commit independently. The only collision you should expect is the documented commit-time UNIQUE shape above when keys actually overlap. If you observe what looks like cross-key serialization, it is likely lock contention from a different cause (large adjacent-edge sets, full-table scan, etc.) — surface the slow query rather than de-tune your concurrency.

## Schema bootstrap

Schema DDL (`CREATE CONSTRAINT ... REQUIRE ... IS UNIQUE`, `CREATE INDEX ...`) is durable across restarts. Run it once on first boot and never re-issue against a populated database — `IF NOT EXISTS` makes the bootstrap step idempotent across replays of the same script:

```cypher
CREATE CONSTRAINT t_uid_unique IF NOT EXISTS FOR (n:T) REQUIRE n.uid IS UNIQUE;
CREATE INDEX t_tenantid_idx  IF NOT EXISTS FOR (n:T) ON (n.tenantId);
```

## Reference retry loop

The full retry/classify loop in production-shape pseudocode:

```go
func runWithRetry(ctx context.Context, sess neo4j.SessionWithContext, q string, params map[string]any, isMergeOnlyGroup bool) error {
    const maxRetries = 5
    for attempt := 0; ; attempt++ {
        _, err := sess.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
            res, err := tx.Run(ctx, q, params)
            if err != nil { return nil, err }
            _, err = res.Consume(ctx)
            return nil, err
        })
        if err == nil { return nil }

        if isTransient(err) && attempt < maxRetries {
            backoff(attempt)
            continue
        }
        if isCommitTimeUnique(err) && isMergeOnlyGroup && attempt < maxRetries {
            backoff(attempt)
            continue
        }
        return err
    }
}

func isTransient(err error) bool {
    var n *neo4j.Neo4jError
    if errors.As(err, &n) && strings.HasPrefix(n.Code, "Neo.TransientError.") {
        return true
    }
    msg := err.Error()
    return strings.Contains(msg, "changed after transaction start") ||
        strings.Contains(msg, "waiting for transaction lock")
}

func isCommitTimeUnique(err error) bool {
    msg := err.Error()
    return strings.Contains(msg, "commit failed") &&
        strings.Contains(msg, "constraint violation") &&
        strings.Contains(msg, "already exists")
}
```

## What stays stable

NornicDB pins the following as a wire contract for Bolt clients:

- The error message strings listed in the table above.
- `errors.Is(err, ErrConflict)` walking the wrap chain on the storage side; on the wire, the equivalent is the substring match.
- `commit failed:` as the single wrapper for both implicit autocommit and explicit `BEGIN/COMMIT` paths.
- The default ports `7687` (Bolt) and `7474` (HTTP).
- `MERGE (n {uid:...}) SET ...` idempotency on retry.
- Schema DDL durability across restarts.
- Direct `Session.Run` against a literal database name without proxying through `system`.

Anything in this list changing is a coordinated breaking change with a CHANGELOG entry. Treat the rest of NornicDB's surface as evolution-friendly — but pin the items above in your client.

## See also

- [`cypher-queries.skill.md`](cypher-queries.skill.md) — hot-path Cypher cookbook.
- [`vector-search.skill.md`](vector-search.skill.md) — vector and full-text indexes addressable through the same Bolt session.
- [`rag-procedures.skill.md`](rag-procedures.skill.md) — `db.retrieve`, `db.rerank`, `db.infer` over Bolt.
- [`api-reference/bolt-protocol.md`](../api-reference/bolt-protocol.md) — Bolt/PackStream wire details for the surfaces this skill names.
