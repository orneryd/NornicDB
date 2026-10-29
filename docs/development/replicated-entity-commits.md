# Replicated entity commits in v1.4.4

Explicit entity write transactions commit on the current leader. A follower returns
`NotALeader`; retry the complete transaction on the leader so its reads and writes
share the correct snapshot. Ordinary direct writes retain follower forwarding.
The leader prepares the mutation and checks its admission before recording it.

Preparation preserves the originating write policy: direct writes retain native
commit durability, implicit bulk writes retain configured write-behind eligibility
even for a single item, and explicit transactions retain explicit commit durability.
Receivers preserve that policy. WAL recovery physically applies already accepted
writes without issuing another buffered acknowledgement or WAL record.

Accepted work belongs to its commit coordinator. A caller cancellation or timeout
does not prove rollback: the write can still apply. Pending work retains its quota
reservation until application or a definite discard. Check the resulting data before
issuing a new, non-idempotent request after an uncertain outcome.

Direct Go writes expose errors after acceptance as `*storage.AcceptedCommitError`.
Use `errors.As` to inspect `State` (`CommitApplied` or `CommitRecoveryPending`)
and `LogicalID` when available. `errors.Is` still reaches the original error.
An error from a failed forwarded reply is recovery-pending because the leader
may have accepted the request. A definite pre-admission rejection remains an
ordinary error. Do not roll back catalog admission on an accepted error.

## Retired low-level replication commands

The internal `CmdCypher`, `CmdSetProperty` and `CmdBatchWrite` entry points are
removed. Their numeric wire slots remain reserved; historical commands in those
slots fail explicitly instead of being interpreted as a different command or
silently replayed. `StorageAdapter.SetExecutor` is removed with raw-query replay.

Use the normal Cypher API over Bolt or HTTP for queries and property changes. For
an atomic group of entity changes, use an explicit transaction through those APIs,
or the Go transaction API and its prepared commit route. Do not submit raw query
text or the retired batch payload directly to a replicator.

Upgrade every cluster member before using the new atomic entity commit envelope.
A node must understand the envelope and accepted semantic WAL record to replay
those writes correctly. Do not downgrade a store containing those records to a
version that cannot read them. Before upgrading an installation with outstanding
retired raw-command records, drain them using the old version and verify its state;
keep a backup. The new version does not automatically translate those records.

## Custom Go storage adapters

`NewDatabaseManager(ctx, storage.Engine, config)` and catalog mutations now require
an explicit operation context; see [catalog API migration](catalog-context-api.md).
An engine used for managed writes must expose `storage.ManagedEntityEngine`: native transactions,
coherent stored-entity observation, commit submission, and the shared
`storage.ManagedEntityRuntimeRegistry`. Native engines provide these capabilities.
The registry keeps one accounting, reservation and limit-policy owner for each
physical namespace across manager views.

Count-store decorators also forward `storage.NamespaceEdgeTypeStatsProvider`.
`NamespaceRelationshipCount` dispatches its typed tier to that provider; a
missing capability is `ErrInvalidData`, never a global count or a scan.

A transparent decorator implements `storage.EngineUnwrapper.GetInnerEngine()` to
expose its actual backing engine. A decorator that applies write policy also
implements `storage.EntityWriteBoundary.WithEntityChange`. It must reject the
operation or invoke the supplied staging continuation exactly once, returning its
result. It must not perform a separate write instead of that continuation.

A read-policy decorator uses
`storage.StoredEntityReadBoundary.WithStoredEntityRead` to wrap the supplied
point-read or scan continuation exactly once. This preserves the transaction's
actual snapshot; it must not substitute a later independent read.

If required managed capabilities are absent, the requested entity mutation fails
with a descriptive `storage.ErrInvalidData` before staging. Existing unlimited,
uninitialized opaque-engine paths remain available; they do not supply the atomic
managed-write contract merely by implementing `storage.Engine`.

Go consumers applying a semantic replication payload now call
`storage.DecodeEntityTransaction(payload)` first and pass its envelope and
operations to `storage.SubmitAcceptedEntityTransaction(ctx, engine, envelope,
operations, settle)`. This replaces the former byte-payload parameter and lets
the replication owner reject malformed data before recording it without decoding
the same transaction twice. Accepted transactions that later fail locally remain
available for recovery.

## Native image interpretation

`BeginEntityTransaction(engine, mode)` binds the originating durability policy.
Node updates use `tx.UpdateGraphNode(node, policy)`, replacing the transaction's
`UpdateNode` entry point. `ClientNodeEdits` retains normal managed-embedding
reconciliation; `StoredNodeImages` applies a complete authoritative stored image.
The decoded accepted semantic coordinator selects the latter. Each operation
supplies its policy, allowing a client edit and an authoritative image to share
one transaction while preserving their different sidecar semantics. Both use
one staging implementation. `Engine.UpdateNode` is unchanged.

```go
tx, err := storage.BeginEntityTransaction(engine, storage.CommitModeExplicit)
if err != nil { return err }
defer tx.Rollback()
if err := tx.UpdateGraphNode(node, storage.ClientNodeEdits); err != nil { return err }
return tx.Commit()
```

The redundant Go batch replay helper `ReplayWALEntries` is removed. Keep the
single-entry `ReplayWALEntry` applier and accumulate its actual outcomes with
`ReplayResult.Record`; see the [WAL migration example](../architecture/wal-property-bindings.md#go-batch-replay).

## Namespace DROP and Restore accounting

A whole-namespace DROP retains a durable cleanup intent until its intrinsic
cleanup is complete. If cleanup fails, error-returning graph reads and writes for that namespace return
`storage.ErrConflict`; global graph scans and counts also fail while they would
include the unfinished namespace. Explicitly scoped reads and writes for other
namespaces remain available. Retry the same DROP to finish cleanup. Opening the
store or restoring a backup with an unfinished intent completes that cleanup before
serving graph operations.

`DatabaseManager.DropDatabase` also records its catalog intent before native
deletion. A rejected intent write leaves the data and live catalog intact.
After admission, an error keeps the database unavailable (`ErrDatabaseOffline`)
until retry or writable manager startup completes removal. Catalog completion is
synced before a successful DROP returns, so same-name recreation cannot inherit
an old pending catalog intent. Read-only managers retain pending state without
applying cleanup. Physical deletion continues through the native prefix owner.

Successful Restore invalidates initialized managed usage in attached managers.
The next managed write reads actual restored usage before admission, preserving
the namespace's configured limits and outstanding reservations. Size reporting
retains its existing last-known-value contract until refreshed; it is not an
admission snapshot.
