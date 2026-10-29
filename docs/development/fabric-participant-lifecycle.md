# Composite participant lifecycle in v1.4.4

A composite statement owns the local transactions it opens. It can read several
constituents, but only one constituent may actually write. An UPDATE matching no
rows does not claim the write database. Later clauses see that statement's pending
writes. Failure before commit rolls back its pending local work.

The coordinator completes readers before committing its one writer. A reader's
completion failure therefore prevents the writer from committing. Successful
reader completion is retained as such; it is not reported as undone. Rollback
failures remain visible in the error chain and participant cleanup state. An
unacknowledged remote commit is reported as unknown, never as confirmed rollback.
This is a one-writer coordinator, not distributed two-phase commit.

Remote readers use READ transactions and finish by rolling them back. Before a
later write on that participant, the coordinator ends its READ transaction and
opens a WRITE transaction after claiming the write database. It does not replay
the earlier read. Definite second-writer requests are rejected before opening
their remote session. A failed remote COMMIT is never retried: one cleanup
ROLLBACK may release the session, but cannot establish that an unacknowledged
write was undone.

## Go integration

`fabric.CypherExecutor` implementations receive the coordinator and participant in
context. Before their first actual mutation, they must claim that participant with
`tx.GetOrOpen(participant.ShardName, true)`. Merely opening a participant or parsing
a write statement does not claim it. The native adapter attaches this decision to
`BadgerTransaction.SetWriteAdmission`; later admitted mutations reuse that decision.
The callback runs with the native transaction mutex held and must not reenter the
native transaction. Read-only work never invokes it.

Remote integrations supply an explicit `storage.RemoteAccessMode` to
`BeginCypherTx(ctx, mode)` and `QueryCypher(ctx, query, params, mode)`. Use
`storage.RemoteAccessRead` for known reads and `storage.RemoteAccessWrite` for
known writes. Fabric resolves uncertain procedure access from its execution
contract and response; integrations must not infer it from query string prefixes.

Bind lifecycle operations and their outcome observer in one operation:

```go
err := tx.BindParticipantCallbacks(shard, fabric.ParticipantCallbacks{
    Commit: commit,
    Rollback: rollback,
    Outcome: outcome,
})
```

This replaces the former positional commit/rollback arguments. There is no separate
outcome-registration API. `Outcome` observes the participant's actual completed
commit state after error or panic; it returns `"committed"`, `"rolledback"`, or
`fabric.StateUnknown`. An implementation without an observer must mark uncertain
commit errors with `fabric.ErrCommitOutcomeUnknown`; an ordinary returned error
means that implementation committed nothing. Cleanup state is separate from the
commit outcome.

A procedure may execute data against the transaction storage it receives. It must
not BEGIN, COMMIT, ROLLBACK or run an independent transaction script on that view.
Rebuilding an executor or changing context does not acquire ownership. Transparent
`storage.EngineUnwrapper` decorations preserve this boundary. Controllers retain
their original engine and end the transaction through its existing lifecycle.
Owned storage is authoritative over context hints, including after its transaction
has ended. Routed local and remote operations join that owner instead of opening
independent transactions.

Native schema consumers now select their purpose explicitly:
`tx.Schema(storage.OrdinarySchemaChanges)` for DDL, followed by
`StageSchemaChanges`, or `tx.Schema(storage.KnowledgePolicyChanges)` for native
policy operations. Both use the same isolated view and persistence owner. A
policy read claims no writer; a persisted change does. The separate
`KnowledgePolicySchema` entry point has been retired.

Native validation settings use
`tx.SetValidationSetting(storage.DeferredConstraintValidation, enabled)` or
`tx.SetValidationSetting(storage.SkipCreateCommitHeadReload, enabled)`. Changing
one setting preserves the other; stored-inclusive create identity checks remain
required. The two former boolean setter entry points have been retired.

Native adjacency reads use `tx.ReadEdges(storage.TransactionEdgeRead{Kind:
storage.TransactionReadOutgoing, Node: id})`, the corresponding incoming kind, or
`TransactionReadBetween` with `Node` and `Other`. Directional snapshot and
write-behind semantics, the dedicated between-node path, namespace checks and
pending overlays are preserved. Engine capability adapters retain their existing
typed outgoing/incoming/between contracts and call this native owner.

Explicit HTTP and Bolt transactions retain their existing session owner and idle
deadline. A recovered statement panic fails the explicit transaction, so a later
COMMIT cannot keep its earlier writes. Session operation locks are released on
panic, terminal observers run outside those locks, and bounded rollback workers
return cleanup failures to the request owner. HTTP rollback responses preserve
both the original statement error and any cleanup error.

These changes alter Go integration contracts and in-memory coordination only.
Badger keys, stored payloads, WAL framing and storage-version migration arms remain
unchanged by this fix.

## View capabilities and completion

Native and composite ownership views preserve storage event subscriptions and
materialized-result access observations. Database identity comes from the existing
engine-chain resolver, including through the composite transaction view.

Admitted independently committed batches hide the outer transaction hint using
the existing batch context. Ordinary reentry still joins the issued owner. This
keeps the child's commit separate while the outer statement retains its own
pending writes and rollback.

Deferred native completion actions have one queue and one completion owner.
Implicit and explicit commits both drain it once after storage commit, preserve
all completion errors, and retain actions as pending work when there are no
entity mutations. Rollback does not run them. Inline embedding completion likewise
uses the existing native participant and leaves commit/rollback with its owner;
embedding failure fails the enclosing statement.

Temporary executor views share their database runtime's atomic search-service
binding. A lazy search continuation registry therefore remains available to later
calls. A database selection owns a fresh runtime binding. Procedure callbacks
receive the typed statement executor view, including fresh-context reentry into
its issued transaction; they must not depend on controller pointer identity.

Remote READ participants release their sessions by rollback even when the caller
successfully commits a read-only statement. Credential workflow checks distinguish
an ID-bearing transaction terminal request from catalog preflight autocommit reads.

Resolved graph authorization runs before reporting an unsupported native-to-
composite transition. The existing Fabric dispatch gate refuses that transition
before participant enlistment, engine opening or fragment execution. Unauthorized
selections therefore retain their permission error and execute no data operation.
