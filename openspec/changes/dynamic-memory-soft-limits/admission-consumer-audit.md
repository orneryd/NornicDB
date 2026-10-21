# Admission consumer audit

## Exact-head extraction and queries

The initial worktree was clean at
`6b9f39d90c38d4158b3d51e8f7e87e9dcc62c517`. Local Graphify extraction:

```sh
graphify extract wt-1618-takeover --code-only --no-cluster \
  --output sig/1618-takeover/graph-pre
```

Output: 4,545 code files, 57,043 nodes, 1,527,771 edges; 76,131 `calls` edges,
26,494 production Go nodes, 30,515 production Go call pairs. Graph SHA-256:
`ef6dad65022421c40155c98cd359112f4d703413899f101a5080d83338a0295b`.
Graphify warned about 18 partially extracted files and 44 files with no symbols.
The graph is evidence for a static inventory, not proof of runtime coverage.

The audit streamed `nodes.item` and `edges.item` with `ijson`, selecting
production `pkg/` and `cmd/` Go files and excluding `_test.go`. It queried direct
callees and reverse `calls` breadth-first to depth four for these exact owners:

- `pkg/memory/policy.go`: `Reserve`, `ReserveWireRequest`, `Refresh`, `Lease.Release`;
- `pkg/cypher/transaction_memory.go`: pool/tracker `reserve`, `reservePools`, `beginTransactionMemory`;
- `pkg/cypher/executor_entry.go`: `Execute`, `execute`;
- `pkg/cypher/show_admin.go`: `withRunningStatement`.

The repeatable query script and raw inventory are retained with the local
handoff. Cross-checks ran only inside the worktree:

```sh
rg -n 'memoryPolicy|MemoryPolicy|ReserveWireRequest|ApplicationSoftMax' pkg cmd --glob '*.go' --glob '!**/*_test.go'
rg -n 'beginTransactionMemory|withRunningStatement|reservePools|ConfigureTransactionMemory' pkg/cypher --glob '*.go' --glob '!**/*_test.go'
rg -n 'NewStorageExecutor|\.Execute\(|executeCypher|Cypher\(' pkg/graphql/resolvers pkg/qdrantgrpc pkg/nornicgrpc pkg/nornicdb pkg/mcp cmd/nornicdb --glob '*.go' --glob '!**/*_test.go'
rg -n 'admitTransactionRequest' pkg/server --glob '*.go' --glob '!**/*_test.go'
```

Graphify confirmed the same-file `ReserveWireRequest -> Reserve`,
`Run -> Refresh`, `Execute -> execute`, and tracker `reserve -> reservePools`
relationships. It omitted field-receiver calls, including
`Server.admitTransactionRequest -> memoryPolicy.ReserveWireRequest`,
`Session.handleRun -> memoryPolicy.ReserveWireRequest`, storage's policy reserve,
and cross-package executor interface calls. Scoped searches plus reading each
caller supplied those edges. Graphify found no additional actual admission
consumer beyond that cross-check. Neither a missing graph edge nor the absence
of a `memoryPolicy` token alone was treated as proof of no admission.

## Entry points and ownership

| Entry point | Application policy path | Transaction pool path |
|---|---|---|
| HTTP `/db/{db}/tx`, `/tx/commit`, `/tx/{id}`, statement-bearing `/tx/{id}/commit` | `handleTransactionEndpoint -> admitTransactionRequest -> ReserveWireRequest -> Reserve`; lease through response assembly | HTTP executor -> `StorageExecutor.Execute/execute -> withRunningStatement -> beginTransactionMemory`; estimated growth -> tracker `reserve -> reservePools -> database/global pool.reserve` |
| Bolt RUN | `Session.handleRun -> ReserveWireRequest -> Reserve`; read-result growth also calls `Reserve`; PULL/DISCARD/RESET/disconnect release ownership | `DBQueryExecutor` / session executor -> same Cypher statement owner and pools |
| GraphQL Cypher query/mutation and CRUD/path resolvers implemented in Cypher | No independent application entry gate; buffered commits below may use the shared policy | resolver `executeCypher -> getCypherExecutor -> StorageExecutor.Execute` -> same pools |
| GraphQL direct search/read/subscription operations | No application or transaction admission in these direct operations | No Cypher tracker; existing coverage boundary |
| Qdrant gRPC points/collections/search | No gRPC request application gate; storage mutations may reach a buffered implicit commit; direct reads/search do not | Direct storage/search, not Cypher; no transaction tracker |
| Native search gRPC (`pkg/nornicgrpc`) | Direct `Searcher.Search`; no application gate | No Cypher tracker |
| Embedded `DB.Cypher`, `DB.ExecuteCypher`, typed Cypher wrappers and `GetCypherExecutor().Execute` | No independent entry application gate; buffered storage commits use the database policy | Same Cypher statement registration and transaction pools |
| Embedded/direct storage APIs, imports and background writers | Only producers that reach `BadgerTransaction.commitBufferedLocked` reserve a buffered commit forecast | No Cypher tracker unless invoked by a Cypher statement |
| MCP Cypher and tools implemented using the executor | No independent application entry gate; buffered storage commits use the policy | `pkg/mcp` query executor -> same Cypher registration and pools |
| HTTP direct REST storage/search, admin/Heimdall/plugin operations | Direct operations have no blanket application gate; a Cypher operation follows the executor, a buffered write follows storage | Cypher-backed operations use the same pools; direct ones do not |
| Replication Cypher commands | No replication entry application gate; buffered writes may reach the shared storage policy | `StorageAdapter.applyCypher -> executor.Execute` uses the same Cypher registration/pools; replication's direct storage commands do not |
| Admin import's Cypher schema files | No import entry application gate; buffered writes may reach storage | `adminimport.applySchema -> NewStorageExecutor.Execute`; JSON schema import uses direct storage instead |
| Fabric/composite local fragments and remote execution | The initiating transport keeps its application lease; remote Bolt servers apply their own policy | Local fragments call `cypherFabricExecutor.executeFragmentQuery -> StorageExecutor.executeInternal`; estimated row growth uses the statement context's memory scope and pools. Remote execution is accounted at the remote process, not as local heap |

An additional broad scoped `rg -l '\.Execute\(' pkg cmd --glob '*.go'
--glob '!**/*_test.go'` cross-check identified the replication and import callers
above. It also matched command-root `Execute` calls, comments in
`pkg/cache/query_cache.go`, the remote Fabric executor, and nested Cypher/txsession
adapters. Those were read and classified rather than counted as separate
application policies. Typed execution (`cypher.TypedExecute`) delegates directly
to `StorageExecutor.Execute`. Lazy Bolt and txsession adapters retain the same
underlying query/pool owner. The final shared-container Graphify `affected`
query on `memory_policy_reserve` with `--relation calls --depth 4` again returned
only `ReserveWireRequest`; its default `path` query reached the HTTP caller via
type-reference/method edges, not a call edge. Scoped source reads supply the
missing executable edges.

Buffered storage: the shared `BadgerTransaction.commitBufferedLocked` owner reserves
before WAL publication, transfers its lease with the retained generation, and
releases it on retirement (or pre-transfer failure). It is the application
consumer beneath multiple entry points, rather than a separate gRPC or embedded
policy. The policy itself belongs to `nornicdb.DB`; `Open` constructs it and its
250 ms sampler, HTTP and Bolt borrow it, and database shutdown joins its worker.

Rollback, a bare explicit HTTP COMMIT, Bolt RESET/DISCARD/PULL and accepted
storage completion keep their existing cleanup paths. This correction changes
recovery in `memory.Policy`, so every existing application consumer receives it
without a per-protocol collector. Transaction pools already release estimated
charges at their statement/transaction owner; lowering their counters is not
evidence that physical pages were returned to the OS. The unprotected direct
operations above are existing soft-limit coverage boundaries, not claims of
whole-process allocator enforcement or newly introduced admission behavior.
