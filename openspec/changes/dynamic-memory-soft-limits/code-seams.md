# Implementation seam review

Reviewed against planning head `9b87b52863df173a6d711c764eeb164627b5004c`
(release base `d854845e9b08e510b56587fddf49694b208e6a3b`). This is a source
review and proposed implementation contract, not proof of implemented protection.
Re-inventory the integrated implementation head after lane A and tracker fixes land.

## Findings that must be resolved before implementation readiness

1. **The cancellation gate alone is insufficient.** HTTP decoding, Bolt unpacking,
   parameter construction and parser allocations precede row execution. Reserve
   request capacity before decoding, retain existing wire-size limits, and transfer
   ownership into execution. Wire bytes do not bound decoded maps/arrays or parser
   working memory. Embedded callers bypass transport admission entirely.
2. **A failed explicit transaction can still own memory.** `failTransaction` records
   the first failure and releases MERGE keys but does not universally discard its
   local pending writes immediately. Do not decrement its charges on statement
   failure. Memory-triggered cleanup must immediately release abortable staged storage
   ownership through the session owner; preserve the failed protocol state/cause
   after storage rollback so later COMMIT/ROLLBACK reports truthfully. Otherwise
   idle failed transactions can prevent recovery indefinitely.
3. **Commit has multiple boundaries.** A buffered commit writes a replayable WAL
   record before publication; a large direct commit writes invisible intermediate
   batches before final publication. A gate just before `cw.finish()` is too late
   to assume nothing has been written. Cancellation must use actual storage state.
4. **Returning a result does not end ownership.** Bolt PULL/backpressure, HTTP
   response assembly, retained cursors and embedded results can outlive execution.
   Define explicit ownership transfer and release; never free budget on Execute
   return while its backing rows remain retained.
5. **A per-transaction cap does not bound aggregate memory.** Simultaneous queries,
   background jobs and commit completion must spend finite shared budgets when
   application protection is enabled. Zero/unlimited transaction defaults remain
   compatible, but are not a protected deployment configuration by themselves.
6. **Resource forecasting must include overlap.** Rebuilding an index or replacing
   a model may retain the old resource while constructing the replacement. Reserve
   for that overlap and active readers; release only after their ownership ends.

## Source seams and required integration

Paths below are repository-relative. Symbols identify current owners rather than
proposed parallel wrappers. Entries are an initial reviewed inventory; implementation
must extract all callers and record each route's coverage on its actual source head.

| Owner / seam | Required change and lifetime boundary |
| --- | --- |
| `pkg/config/config.go`, `pkg/config/cgroup_memory.go`, `pkg/config/dbconfig/settings.go` | Keep GC target distinct; parse/validate app and transaction budgets, finite ancestor constraints and units. Publish limits to one process policy owner, not one independent pool per DB/executor. Settings reload must not reset live reservations. |
| `pkg/server/server_db.go`: `readTransactionRequest`, `decodeTransactionRequest` | Admission before decoding/parameter expansion; retain `MaxRequestSize`. Release on decode/auth/execution errors; transfer request buffers retained by execution. Preserve authorization ordering and avoid revealing database existence through pressure handling. |
| `pkg/bolt/server.go`: `Session.readMessage`; `pkg/bolt/packstream.go` | Account bounded message assembly and decoded input, before RUN. Admission must still allow bounded RESET/ROLLBACK/disconnect cleanup. Do not allocate an unbounded rejection queue. |
| `pkg/cypher/executor_entry.go`: `Execute`, `executeRouted`, `execute`; `pkg/cypher/show_admin.go`: `statementContext` | Attach/reuse one transaction account before parser/planner work, share it across nested execution, read cached pressure at existing checkpoints. Preserve first cause and statement commit outcome; final cancellation checks must not override published writes. Both parser paths need growth boundaries. |
| Develop `pkg/cypher/transaction_memory{,_estimate,_retention}.go` | Port the reviewed charge/retention family; atomically reserve individual/database/process capacity before growth. Failed multi-pool reservations unwind completely. Charge capacity, not only length, where backing allocations stay live; avoid repeated charges for shared values. |
| `pkg/cypher/transaction.go`: `handleBegin`, `executeInTransaction`, `failTransaction`, `handleCommit`, `handleRollback` | One account persists across statements. Memory failure prevents later commit; safe storage cleanup must not erase the protocol failure. Composite local participants share process accounting; remote memory is not represented as locally enforceable. Keep local request/result buffers charged. |
| `pkg/txsession/manager.go`: `ExecuteInSession`, `CommitAndDelete`, `RollbackAndDelete`; `pkg/bolt/transaction_lifecycle.go`, `transaction_cleanup.go`, `transaction_executor_adapter.go` | Serialize pressure cancellation with active RUN and commit through existing lifecycle owners. Include idle/failed sessions, expiry, RESET, disconnect and server shutdown. Releasing a session map entry alone does not establish completed cleanup. Preserve uncertain commit outcomes. |
| `pkg/cypher/result_stream.go`: `emit`, `Next`, `discard`, `Finish`; `pkg/server/server_db.go`: `appendStatementResult` | Transfer result charges to actual consumer and any serialization copies, release when references are dropped. Cover partial consumption, HTTP disconnect and Bolt DISCARD. Once streaming headers are sent, use the existing terminal error protocol; a new HTTP 503 status is no longer possible. |
| `pkg/cypher/transaction_commit.go`: `commitStorageTransaction`; `pkg/multidb/storage_transaction.go`: `CommitTransaction` | Preserve managed database admission/size wrappers and publication callback ordering. Disk quota/entity-size accounting is not transaction RAM accounting. Obtain completion capacity before entering storage work, without holding the policy lock over storage locks. |
| `pkg/storage/badger_transaction.go`: `commit`, `commitCancelledLocked`, `Rollback`; `badger_transaction_publication.go` | Reserve for constraint validation, temporal buffers, deferred edges, MVCC/change capture, staged writes, publication and callbacks. The existing context only polls selected validation loops; inventory the rest. Rollback must retain enough capacity to release resources and complete actual cleanup. |
| `pkg/storage/badger_commit_writer.go`: `commitBatch`, `finish`, `abort`; `pkg/storage/badger_managed.go`: `largeCommit` | Distinguish invisible staged batches, published commit and rollback failure. Preserve existing commit-oracle failure handling if large-commit rollback fails; never mask it as successful OOM rollback. |
| `pkg/storage/badger_write_behind.go`: `commitBufferedLocked`, `replayWriteBehind`; `pkg/storage/commit_buffer.go`: `AppendCommit`, `drainOne`, `removeDrained` | Reserve before WAL acceptance; transfer staged maps/slices from transaction to buffer without releasing/reacquiring a capacity gap. Bound bytes as well as operation counts. Keep charge through draining, replay failures and retained read generations until final references are dropped. |
| `pkg/nornicdb/embed_queue.go`: `processNextResolvedBatch`, `processNextDocumentBatch`, `persistEmbeddedNode`; `pkg/nornicdb/query_embedding.go` | Account queue claims, input expansion, provider concurrency, output and persistence overlap. Pressure deferral must preserve retryable work without misclassifying it as permanent provider failure. Native request charge persists until the call returns. |
| `pkg/search/hnsw_index.go`, `hnsw_lifecycle.go`, `ivfpq_build.go`, `ivfpq_index.go`; `pkg/search/local_rerank.go`: `Rerank` | Reserve construction/scratch/candidate capacity and old/new index overlap; track shared representation once. Rerank batch and local/native provider working sets participate in the same app budget. Enumerate inference/Heimdall provider loading and active-call owners at implementation time. |

MCP, GraphQL, Qdrant gRPC, native embedding/search APIs, direct embedded DB calls,
imports and administrative/index procedures must be traced to these owners or to
an equivalent application-policy consumer. A Cypher-only gate does not protect
routes that call storage/search/provider APIs directly. A reusable route inventory
must list entry point, account creation, growth checks, terminal owner and test.
No claim of complete protection until every supported in-process route is classified.

## Concrete commit transitions

| State | Memory-policy action |
| --- | --- |
| Decode / parse / execute / validation, no accepted storage writes | Deny reservation or record memory failure; release owned work through normal transaction failure/rollback. |
| Buffered path before `WAL.AppendBufferedCommit` | Reserve buffer bytes plus completion workspace. Final abortable check occurs before replayable acceptance. |
| Successful replayable WAL append, before `AppendCommit` / ACK | Finish publication and ownership transfer; do not inject a new memory cancellation that leaves a replayable write reported as rolled back. WAL partial-error/recovery behavior needs a focused fault test. |
| Direct ordinary commit before storage submission | Final abortable check and completion reservation. Avoid releasing that reserve while waiting on disk. |
| Large direct commit has invisible batches | Existing `commitWriter.abort` owns compensating rollback. Do not introduce new mid-batch memory cancellation unless cleanup/restart proof establishes it is safe; proposed initial policy reserves completion before entering materialization and lets accepted storage work finish. |
| Published commit / callbacks / fsync | Preserve actual outcome and complete publication/durability work. Pressure stops subsequent admissions, not an invented rollback of this commit. |

Completion reserve is a finite protected pool, not an exemption that permits
unlimited simultaneous commits. Reserve before acquiring storage publication
locks; bound active completion work. If a transaction cannot obtain the required
capacity while still abortable, fail it with the correct pool reason. Do not wait
for a pool while holding locks needed by its releasing work.

## Proposed accounting contract and remaining policy decisions

For an estimated positive growth `d`, one reservation operation checks all applicable
individual/database/process bounds using overflow-safe arithmetic and either charges
all or none. A zero explicit bound skips only that explicit check; it does not disable
configured application pressure protection. The account follows the logical transaction
through nested calls and statements. Ownership transfer keeps shared usage continuous;
copying into another buffer charges the overlap. Counter release is not physical reclaim.

Use bounded request/background concurrency and queue bytes together with budget
checks. A semaphore alone does not bound memory when each request can be arbitrarily
large. Adaptive baseline changes must not silently enlarge operator-set transaction
maxima. Sample recovery after canceled work actually releases memory; do not admit
replacement work just because a victim was signaled.

Accepted pressure sequence: stop new expensive admissions, pause new optional jobs,
choose the newest eligible pre-acceptance transaction through one serialized policy
decision, then reconcile released ownership and total usage before choosing another.
Exclude commit completion and cleanup. Define a stable tie-break, minimum interval
and starvation response during implementation; do not let each checkpoint cancel
its own transaction on a single shared-pressure observation. Emergency escalation
remains a policy review item.

A deployment recipe must specify finite app/aggregate limits, optionally finite
individual limits, concurrency/batch bounds and headroom below the container limit.
Keep compatibility defaults unlimited and the application policy disabled unless
explicitly configured. Numeric protected-mode sizing, result ownership for embedded
callers and exact bounded completion estimates must be settled and measured before
calling implementation ready. Transaction-only limits must not start whole-process
telemetry rejection; the existing Go GC target does not enable this policy.
No finite percentage or soft transaction cap guarantees immunity to an OS OOM kill.

## Required focused evidence

- Oversized HTTP parameters/Bolt messages and parser growth fail before expensive
  expansion; native API execution cannot bypass the same application pool.
- Multiple individually legal transactions exceed the shared budget; failed reserve
  leaves no partial charges. Nested execution and same-process fabric participants
  cannot independently acquire fresh budgets for the same logical transaction.
- Idle failed explicit transaction releases physical storage ownership safely while
  later COMMIT reports its original failure; active RUN/COMMIT/expiry races serialize.
- Slow PULL/HTTP consumers and retained results stay charged; disconnect/discard
  releases them once. Byte budget remains continuous across buffer ownership transfer.
- Pressure injected before/after WAL append, during large batches and after publication
  proves visible writes/restart outcomes match the reported outcome. No cancellation
  converts a committed write into a retryable memory rollback.
- Saturated completion pool still permits rollback/flush and never deadlocks with
  publication locks. Replay failure retains charges; recovery does not double release.
- Model/index replacement measures old/new overlap and delayed reader retirement.
- Original container reproduction records peak total usage, headroom, cancellation
  delay, overshoot, exit/restart counters and surviving data. Report measured safety
  for that workload rather than an absolute no-OOM guarantee.

## Async write-buffer admission (explicit requirement)

When the application limit is enabled, `WriteBehindBuffer` in
`pkg/storage/commit_buffer.go` is inside that application memory policy. Without
it, the existing async write-buffer behavior remains in force. Its current `AppendCommit` increments operation counts and signals
rotation; rotation is not a byte limit and does not establish freed memory. The
same file exposes `AppendNode`, `AppendEdge` and `AppendDelete`; source search at
this review base found no production call sites for those three methods. Inventory
all actual producers on the implementation head rather than assuming they are
active routes or adding another async engine.

- Reserve estimated retained bytes plus overlay/index growth before accepting a
  write. For `commitBufferedLocked`, this must happen before the replayable WAL
  append, not by returning a new pressure error from `AppendCommit` afterward.
  Carry the reservation across WAL serialization, publication and buffer ownership.
- Atomically enforce the shared application budget and a finite configured/derived
  buffer byte allowance. Count active, queued, draining and reader-retained
  generations together; rotation must not reset the allowance. A single oversized
  entry must fail while still abortable. Operation-count/adaptive flush thresholds
  remain scheduling controls, not memory enforcement.
- Keep original payloads and additional overlay/pending-index allocations accounted
  for without charging shared backing storage twice. Reserve extra replay/encoding
  workspace separately from retained payload ownership.
- Reject new abortable writes promptly with the typed pool/admission error when
  capacity is unavailable. Do not create unbounded waiting producers or silently
  switch to synchronous writes to bypass the limit. Preserve existing public
  protocol mappings and committed outcomes.
- Continue flush/replay using protected completion capacity under pressure. Slow
  disk, retries or replay errors retain the charge; successful application releases
  only objects whose owners/readers have actually let go. Recovery must never drop
  an accepted write merely to satisfy a memory budget.
- Apply the same ownership rules to startup WAL replay and direct/non-Cypher
  producers that reach this path. Recovery batching must fit available working
  capacity without weakening replay/durability semantics.

Required tests: producers outrun a deliberately stalled flusher; one huge write
versus many small writes; concurrent reserve at the byte boundary; generation
rotation and reader retention; WAL failure and replay retry; pressure after WAL
acceptance; flush resumes and admissions recover without lost acknowledged writes,
double release or apparent rollback. Measure both estimated buffer bytes and total
process/container memory; the allowance bounds estimates, not exact physical RSS.
