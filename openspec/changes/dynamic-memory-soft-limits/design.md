# Dynamic memory sizing and cooperative soft limits

## Accepted decisions

1. Memory thresholds are soft, checked cooperatively; they are not OS-enforced
   ceilings and cannot promise that a process will never be OOM-killed.
2. Application thresholds must leave space below a finite container ceiling.
   Do not wait until 99% usage. No universal percentage is approved here.
3. Baseline sizing starts at startup and adapts when persistent resources and
   workloads change, including model loading, caches, indexes and storage work.
4. Reuse execution cancellation checkpoints through one policy owner. Preserve
   caller cancellation/deadline identity and attach typed memory-failure causes.
5. Reject new expensive work under pressure; terminate eligible active
   transactions at safe checkpoints and release their owned resources.
6. Transaction accounting/reservations are estimates and soft limits, not exact
   allocator interception. Reuse and correct develop's accounting rather than
   creating another tracker. Large allocations can overshoot between checkpoints.
7. Preserve storage atomicity/durability. Cancellation is not process killing,
   goroutine killing or a mechanism to interrupt arbitrary native/syscall work.

## Current owners and convergence boundary

- Release `pkg/config/config.go` applies `debug.SetMemoryLimit`; existing
  `pkg/config/cgroup_memory.go` discovers effective constraints. Reuse these
  facilities and their hierarchy/platform tests instead of duplicating discovery.
- `pkg/cypher/show_admin.go` owns statementContext and first-cancellation state;
  `executor_entry.go` and lane A's loop checkpoints are the execution consumers.
  Context propagation is not proof that every allocation checks its budget.
- Develop `transaction_memory.go`, `transaction_memory_estimate.go` and
  `transaction_memory_retention.go` own transaction/database/global estimated
  pools. Inventory all follow-ups, error mappings and consumers before porting.
- `pkg/storage/badger_transaction.go`, Badger write-behind/commit buffers,
  `wal.go` and DB durability configuration own persistence and publication.
  Inventory actual acceptance/commit fences for buffered and direct routes.

One application owner publishes a coherent, versioned budget/pressure snapshot;
transaction owners maintain their estimates and atomic pool reservations.
Adapters consume the same policy; transports do not implement separate limits.
Retire superseded memory/deadline decisions after migrating their consumers.

## Budgets, measurements and dynamic baseline

Use separate quantities with explicit units and scopes:

| Quantity | Meaning |
| --- | --- |
| Go runtime target | Existing soft GC policy; excludes some native/kernel memory |
| Effective container ceiling | Minimum applicable finite ancestor constraint; shared cgroup usage includes other members |
| Application soft maximum A | Operator threshold below that ceiling; not the GC target |
| Baseline B | Attributable persistent engine/native resources, excluding active transaction estimates |
| Headroom H | Working reserve for storage/native bursts, cleanup, responses and sampling uncertainty |
| Transaction budgets | Individual, database aggregate and process aggregate estimates; zero means unlimited for explicit settings |

Conceptually `dynamic transaction capacity = max(0, A - B - H)`, additionally
bounded by any configured aggregate limit. This is a sizing model, not a second
copy of physical usage. Admission also checks actual sampled total pressure.
Do not subtract transaction estimates twice, add RSS to overlapping Go/cgroup
usage, or treat all container usage as exclusively this process's allocation.

Reuse periodic measurement infrastructure where suitable. Execution gates read
cached atomic snapshots; they do not read procfs or walk memory on every row.
Linux should use cgroup total usage plus breakdowns (including dirty/writeback)
and applicable limits; RSS/Go/native readings provide attribution. Other supported
platforms expose their actual observation coverage, not a fictitious Linux total.

Bootstrap with configured component sizes and measured startup stages; mark
baseline provisional until loading settles. Reconcile at quiescent observations
and owned resource transitions. Never learn a runaway query into the baseline.
A model load or index rebuild requires forecast working capacity before starting
where estimable, followed by reconciliation against observed usage. Track known
background working memory separately from persistent baseline. Permit budget
increases only within A and the container ceiling; smoothing/hysteresis prevents
rapid oscillation. A shrinking budget can exceed outstanding reservations:
stop new admissions, then apply safe cancellation; do not erase or reassign debt.

Changing live resource ownership, platform coverage or dynamic container limits
must update this same owner. Invalid settings fail configuration validation.
Unavailable/stale telemetry must be visible; its operational policy is an open
review decision, not an implicit unlimited-mode fallback.

## Shared execution gate and transaction lifetime

The conceptual interfaces are `Check(ctx)` and `Reserve(ctx, estimatedBytes)`;
these are design contracts, not new public Go APIs in this PR. Both delegate to
one budget owner. Preserve existing first-error semantics and typed causes;
never turn cancellation into OOM or a memory error into a timeout. Do not perform
OS measurement or introduce unrelated side effects inside arbitrary callers'
`context.Context.Err()` implementations. Integrate through the established
statement/context owner and all its current consumers, including final validation
and final pre-success checks from lane A.

Coarse reservations precede known materialization/growth; observation checkpoints
catch unaccounted pressure. Neither is exact, and neither alone covers all work.
Count retained rows/collections, explicit transaction writes and queued/streamed
results until the actual owner releases them. Finishing a statement is not proof
that its result or transaction overlay has been freed. Do not charge an entire
process-usage increase to each concurrent transaction. Release accounting exactly
once; garbage collection and native frees may make physical recovery lag behind.

Admission uses HTTP 503 for new expensive requests rejected by application
pressure. Existing sessions and Bolt use their proper resource/transaction error
contracts; they are not arbitrary HTTP responses. Preserve essential health,
cancel, rollback and completion paths within reserved capacity. Background
embedding/index/compaction consumers must participate without preventing the
storage flush/cleanup work needed to recover.

A transaction exceeding its own estimate fails with
`Neo.ClientError.General.TransactionOutOfMemoryError`, verified against pinned
Neo4j 5.26 behavior. Distinguish aggregate pool exhaustion
(`Neo.TransientError.General.MemoryPoolOutOfMemoryError`) and application
admission overload. Preserve caller cancellation/deadline mappings and original
unexpected causes. Bolt failed-transaction/reset and HTTP explicit-transaction
semantics require public tests, including fresh-session checks of side effects.
Do not reuse develop's TransactionMemoryLimit code without compatibility proof.

## Badger, fsync and commit safety

Badger v4.9.6 is the pinned dependency in this planning base. Buffered writes,
memtables, caches, WAL/value-log buffers, compaction and kernel dirty/writeback
pages can coexist. A slow disk prolongs their lifetime. Clean cache can remain
after fsync; fsync is a durability operation, not a memory-release notification.
Cgroup file memory must not be omitted simply because it is outside Go heap.

Reserve completion/rollback/flush capacity and bound admitted write concurrency.
Keep essential flush and compaction progress possible when new work is rejected;
do not hold a policy lock while awaiting disk or native work. Preserve configured
immediate/batch/none durability and existing explicit/autocommit semantics. Do
not add per-row fsync or claim to cancel a blocking fsync through Go context.

The irreversible storage boundary is a required inventory item, not assumed to
be entry into Commit(). Check pressure/cancellation and acquire completion
capacity before that boundary. Before it, an aborted explicit transaction must
not commit its pending writes. After irreversible acceptance/commit, finish and
report the actual storage/durability outcome; do not synthesize an OOM/rollback
error for already committed writes, trigger unsafe retries or bypass publication.
A post-commit fsync failure follows the existing storage outcome contract, not a
new fabricated memory error. Current release code logs a Sync error after commit
and closes the transaction as committed; review this interaction explicitly,
without silently changing durability policy in the memory work.

Native/kernel work and already accepted async writes may continue after query
cancellation. Their memory remains charged/observed until completion. This is
why the application threshold needs headroom and remains soft.

## Required implementation decisions still open

- Application maximum/automatic-sizing setting names and units; relationship to
  the existing MiB-valued NORNICDB_MEMORY_LIMIT without silently reinterpreting it.
- Sampling interval, stale-sample policy, initial/minimum reserve, growth response,
  recovery hysteresis and how container co-tenancy affects auto sizing.
- Deterministic pressure-victim selection and fairness: aborting the transaction
  that observes pressure is not proof it caused the pressure. Avoid canceling all
  transactions merely because they saw one over-budget sample.
- Exact safe commit fences and completion reserves for each storage route; native
  and writeback observation coverage on each supported platform.

The draft does not invent numeric defaults or present these as approved choices.
No implementation readiness claim precedes resolving these decisions and the
integrated acceptance matrix below.

## Evidence and limitations

Reproduce the reported 8 GiB/7168 MiB case with a single oversized query and no
subsequent requests, then with concurrent queries and deliberately slow writes.
Use before/after profiles, disabled/permissive/restrictive limits, both parsers,
all public transport/transaction modes, native/background work and restart checks.
A remaining OS-kill window must be documented with overshoot/response timing;
soft limits are not permission to omit checkpoint coverage or weaken assertions.

Primary references:
- [Go GC memory-limit semantics](https://go.dev/doc/gc-guide#Memory_limit)
- [Linux cgroup v2 memory/writeback accounting](https://www.kernel.org/doc/html/latest/admin-guide/cgroup-v2.html)
- [Badger Sync API](https://pkg.go.dev/github.com/dgraph-io/badger/v4@v4.9.6#DB.Sync)
- [Neo4j status codes](https://neo4j.com/docs/status-codes/current/errors/all-errors/)

The error-code inventory above is a proposed compatibility contract; pinned
5.26 differential tests, not current-documentation wording alone, establish it.
