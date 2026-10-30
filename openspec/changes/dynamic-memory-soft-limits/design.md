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
   allocator interception. Build one release-owned tracker that supersedes older
   attempts; adopt a develop charge site only after its ownership lifetime and
   error behavior pass the approved contract. Large allocations can overshoot
   between checkpoints.
7. Preserve storage atomicity/durability. Cancellation is not process killing,
   goroutine killing or a mechanism to interrupt arbitrary native/syscall work.
8. A memory-failed explicit transaction releases abortable storage ownership
   immediately through its existing session lifecycle. The protocol session
   retains the first failure for its later terminal command.
9. Under process pressure, choose the newest eligible, pre-acceptance
   transaction first. Exclude commit completion and cleanup; stop one owner
   through the canonical policy and reassess before selecting another.
10. When application pressure protection is enabled, stale or unavailable
    memory telemetry rejects new expensive work until a fresh successful
    sample arrives. Continue rollback, flush and accepted commit completion.

## Opt-in scope and disabled-path contract

Application pressure protection is enabled only by an explicitly configured
finite application soft maximum. The existing `NORNICDB_MEMORY_LIMIT` remains a
Go GC target and does not implicitly enable request admission, process-pressure
victims, stale-telemetry rejection, dynamic baseline enforcement or async-buffer
byte gating. A finite individual transaction or aggregate pool setting enables
only that scope's estimate checks and resulting cleanup; it does not implicitly
enable an application-wide limit. Zero/unset transaction settings are unlimited.
The new application limit is unset/disabled by default. No finite container
limit automatically turns this policy on.

With no application, transaction or aggregate limit enabled, the policy's hot
path is a cheap disabled check: no reservation/accounting work, sampler-driven
rejection, additional cancellation, admission errors or behavior changes. Existing
context cancellation, request-size rules, storage durability and Go GC behavior
continue normally. A configured application limit enables shared pressure checks,
including async-buffer admission and stale-telemetry handling. Configuration
transitions must publish one coherent state and retain outstanding ownership until
safe completion; disabling a limit cannot erase a pending commit's obligations.

## Proposed setting names and compatibility migration

The source currently reads `NORNICDB_MEMORY_LIMIT` into
`Memory.RuntimeLimit`; `--memory-limit` does the same, and
`memory.runtime_limit` is its YAML key. `ParseRuntimeLimit` accepts `auto` or
an integer count of MiB (`7168` = 7168 MiB), even though the example YAML
comment shows unsupported `2GB`/`512MB` forms. `ApplyRuntimeMemory` then calls
Go `debug.SetMemoryLimit`. These existing values must never become application
or transaction limits merely because their names look broad.

Proposed canonical configuration (to approve during implementation review):

| Scope | Canonical name | Units and default |
| --- | --- | --- |
| Go runtime GC target | `NORNICDB_GO_MEMORY_LIMIT_BYTES`; CLI `--go-memory-limit-bytes`; YAML `memory.go_runtime_limit_bytes` | Use only a non-negative integer number of bytes. Omission retains automatic resolution. The deprecated legacy alias retains MiB input. This is a soft Go-runtime target, not an opt-in application gate. |
| Application pressure maximum | `NORNICDB_MEMORY_APPLICATION_SOFT_MAX`; YAML `memory.application_soft_max` | Explicit byte-size parser (`k`, `m`, `g` as binary units); unset/`0` disables the application policy. A finite value must leave measured headroom below effective container limit. |
| One transaction | `db.memory.transaction.max` / `NORNICDB_MEMORY_TRANSACTION_MAX` | Estimated bytes, `0` unlimited; use develop's established setting and env spelling. |
| All transactions in one database | `db.memory.transaction.total.max` | Estimated bytes, database-scoped, `0` unlimited. Configure by database setting; no process-wide env alias that ambiguously selects a database. |
| All transactions in the process | `dbms.memory.transaction.total.max` / `NORNICDB_MEMORY_TRANSACTION_TOTAL_MAX` | Estimated bytes, `0` unlimited; use develop's established setting and env spelling. |

Do not add separate user-facing limits for async buffers, index jobs, AI workers,
commit completion or each query phase in this change. When the application
limit is enabled, derive bounded sub-budgets/reserves from it and observe their
usage. An operator can still tune existing feature concurrency and storage
settings. Extra knobs require measured evidence that this single policy cannot
handle an ordinary workload.

Deprecation contract: accept `NORNICDB_MEMORY_LIMIT`, `--memory-limit` and
`memory.runtime_limit` with their exact existing Go-runtime meaning. Emit one
clear deprecation warning naming the new spelling and retained units. Never
silently reinterpret a legacy value as the application maximum. If canonical
and legacy spellings for the same Go target are both explicitly set, reject a
conflicting configuration with a message naming both sources; identical values
may be accepted with a warning. Define normal config-source precedence when
only one spelling is present. Document removal in a later release; this spec
neither removes aliases nor selects a removal date. No warning should imply
that a configured GC target itself provides application OOM protection.

## Current owners and convergence boundary

- Release `pkg/config/config.go` applies `debug.SetMemoryLimit`; existing
  `pkg/config/cgroup_memory.go` discovers effective constraints. Reuse these
  facilities and their hierarchy/platform tests instead of duplicating discovery.
- `pkg/cypher/show_admin.go` owns statementContext and first-cancellation state;
  `executor_entry.go` and lane A's loop checkpoints are the execution consumers.
  Context propagation is not proof that every allocation checks its budget.
- Develop `transaction_memory.go`, `transaction_memory_estimate.go` and
  `transaction_memory_retention.go` are reference implementations only. Their
  statement-end release contract does not account for an explicit transaction's
  retained staged writes, and their transaction error code differs from the
  approved Neo4j contract. The release implementation supersedes these attempts:
  use a single owner for transaction, database and process reservations, and
  bring over only individually verified charge sites or tests.
- `pkg/storage/badger_transaction.go`, Badger write-behind/commit buffers,
  `wal.go` and DB durability configuration own persistence and publication.
  Inventory actual acceptance/commit fences for buffered and direct routes.

One application owner publishes a coherent, versioned budget/pressure snapshot;
transaction owners maintain their estimates and atomic pool reservations.
Adapters consume the same policy; transports do not implement separate limits.
Retire superseded memory/deadline decisions after migrating their consumers.

The [implementation seam review](code-seams.md) identifies the concrete owners,
pre-execution and retained-result gaps, transaction cleanup requirements, and
WAL/large-commit transitions that this implementation must cover. Its proposed
contracts supplement this design; unresolved policy choices remain review items.

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
With application pressure protection enabled, unavailable/stale telemetry is
visible and stops new expensive admissions until a fresh successful sample arrives. Rollback, flush and accepted commit completion
continue from protected capacity. The precise freshness deadline remains to be
measured and selected. Linux memory.events and PSI may wake the sampler on pressure
but do not report every change in memory.current. Periodic reads reconcile actual
usage and cgroup limits; explicit ownership/reservation updates refresh the
policy between reads. Record the last successful sample time, source and
coverage. Sampling delays, read errors, a replaced cgroup path and changing
limits can make a cached observation stale. Event notifications supplement
periodic reconciliation; they cannot be the sole source of current usage.

## Variable feature costs and measurement limits

There is no fixed memory price per feature or reliable exact per-operation
measurement derived from process/container deltas. Concurrent allocation, GC,
native allocators, file cache and asynchronous completion overlap. Correlate
observations with work, but do not present correlation as causal byte accounting.

| Work | Measurements/estimates to reconcile |
| --- | --- |
| Query/explicit transaction | Owned retained rows, groups, paths, writes and output; shared pool estimates |
| Index build and insertion | Persistent index representation plus temporary construction, insertion, rebuild and compression buffers |
| Badger/WAL/flush/compaction | Available storage statistics, queued/in-flight bytes and total container file/dirty/writeback observations |
| Embedding workers | Loaded model baseline, worker concurrency, batch shape, input/output buffers and provider-owned native allocations |
| LLM/Heimdall inference | Shared weights separately from variable context/KV cache, token batches and simultaneous inference |
| Reranking | Shared model baseline separately from candidate batches, input lengths and working/output buffers |
| Remote AI providers | Local request/response queues, serialization and retained outputs; remote model RAM is not local usage |

An fsync duration is measurable, but its byte cost is not a constant: dirty data
may predate the call, other writes can proceed concurrently, and completion may
allow later reclamation without evicting clean cache. Do not create a synthetic
per-fsync allocation charge or sum dirty/writeback subsets into total usage again.
Observe these values over time, retaining scope, timestamp, coverage and sample
age. Use owned estimates and bounded representative peak measurements to adapt
working reserves, then cross-check current total pressure. Past peaks do not
bound the next batch, insertion or inference request.

Pressure policy must cover the whole application: new embedding/inference/
reranking/index jobs cannot continue admitting unbounded work merely because
HTTP commits have stopped. Active work cooperates at its safe boundaries;
non-interruptible native calls remain live until they actually return. Flush,
rollback and accepted commit completion retain their recovery reserve. Sampling
and estimates cannot guarantee a transaction error before every possible OOM.

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
pressure. In particular, `/db/{name}/tx/commit` must return a structured failure
instead of hanging or starting new query work. `/db/{name}/tx/{id}/commit` must
respect its existing transaction state and the irreversible storage boundary;
never unconditionally replace a completed commit outcome with HTTP 503. Existing sessions and Bolt use their proper resource/transaction error
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

- Approve the proposed canonical setting spellings and config precedence above,
  plus the finite application-limit validation and auto-sizing behavior.
- Sampling interval/freshness deadline, initial/minimum reserve, growth response,
  recovery hysteresis and container co-tenancy. Stale/unavailable telemetry
  rejects new expensive work only with the application limit enabled. Event monitoring
  remains an optional Linux accelerator, not the only usage source.
- Newest eligible transaction first is accepted. Define ordering under equal start
  times, bounded selection/reassessment cadence and starvation controls without
  treating the observing transaction as the cause or canceling every observer.
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
A performance gate also applies: run at least three paired baseline/current samples
per representative short read, large read, write/commit and concurrent workload
under the same machine, corpus, build tags and configuration. Report throughput,
p50/p95 latency, allocated bytes/op, retained policy memory per active owner and
CPU/heap profiles. The disabled path must add zero allocations per checkpoint or
row; the enabled path must sample outside execution loops and add zero allocations
per checkpoint or accounted growth event. A repeatable greater-than-5% throughput
or latency regression, or greater-than-10% allocation/retained-memory increase,
blocks readiness unless the maintainer explicitly accepts the measured tradeoff.
Investigate noise with more paired samples rather than treating one run as proof.
A remaining OS-kill window must be documented with overshoot/response timing;
soft limits are not permission to omit checkpoint coverage or weaken assertions.

Primary references:
- [Go GC memory-limit semantics](https://go.dev/doc/gc-guide#Memory_limit)
- [Linux cgroup v2 memory/writeback accounting](https://www.kernel.org/doc/html/latest/admin-guide/cgroup-v2.html)
- [Badger Sync API](https://pkg.go.dev/github.com/dgraph-io/badger/v4@v4.9.6#DB.Sync)
- [Neo4j status codes](https://neo4j.com/docs/status-codes/current/errors/all-errors/)

The error-code inventory above is a proposed compatibility contract; pinned
5.26 differential tests, not current-documentation wording alone, establish it.
