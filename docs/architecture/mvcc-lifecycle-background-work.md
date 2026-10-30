# MVCC Lifecycle and Background Work Architecture

This document explains how NornicDB now treats storage-touching background work so that maintenance and indexing do not compete unnecessarily with query execution.

It focuses on three paths:

- mutation-driven search indexing
- embedding worker wakeups
- MVCC lifecycle maintenance

## Design Goals

The current design aims to keep background work predictable under write-heavy load.

Requirements behind this behavior:

- do not spawn unbounded background fan-out from database mutation callbacks
- coalesce bursts of database events before search or embedding work runs
- allow long-running lifecycle work to be scheduled explicitly instead of always running at a fixed cadence
- preserve manual control so operators or the UI can choose quieter maintenance windows

## Mutation-Driven Search Index Sync

Search-index sync is fed from storage node create, update, and delete callbacks.

Previous failure mode:

- one goroutine could be launched per mutation event
- ready search services could index or remove immediately on each callback
- write-heavy workloads could amplify indexing churn and create avoidable contention

Current behavior:

- storage callbacks enqueue search mutations inline
- per-database search services collect pending operations in a mutation map keyed by logical node id
- a short debounce window coalesces bursts of updates before flush
- the flush worker is single-flight per database service, so concurrent callback bursts do not create unbounded worker fan-out

Operational effect:

- repeated updates to the same node are collapsed before indexing work runs
- search services do not process one immediate mutation per write anymore
- database writes stay lightweight because callbacks only enqueue and schedule flush work

Current debounce constant:

- search mutation flush delay defaults to `250ms`

## Embedding Worker Trigger Path

The embedding worker is already pull-based: it scans and processes nodes that still need embeddings rather than embedding directly inside the mutation path.

Important behavior:

- `Enqueue(nodeID)` wakes the worker instead of embedding that node synchronously
- worker wakeups are debounced through `TriggerDebounceDelay`
- repeated trigger bursts collapse into one wakeup
- worker batch delay and scan interval determine how aggressively background embedding work runs

Recent behavior fix:

- the runtime database initialization path now honors configured embedding worker timing values instead of using hardcoded defaults

That matters because queue debounce only protects the system if the configured runtime actually uses the intended timing.

Relevant controls:

- `NORNICDB_EMBED_TRIGGER_DEBOUNCE`
- `NORNICDB_EMBED_BATCH_DELAY`
- `NORNICDB_EMBED_SCAN_INTERVAL`

## MVCC Lifecycle Manager Scheduling

MVCC lifecycle work is responsible for prune planning, debt tracking, pressure visibility, and enforcing retained-floor semantics.

Previous limitation:

- lifecycle cadence was fixed to the configured interval at startup
- operators could pause or resume, but could not switch to manual-only mode or change the cadence at runtime

Current behavior:

- lifecycle exposes `automatic` and `cycle_interval` in status
- automatic scheduling can be updated at runtime through an optional schedule-control interface
- an interval of `0s` disables automatic runs entirely
- manual prune remains available even when automatic scheduling is disabled

This creates two operating modes:

1. Scheduled mode
   The lifecycle loop runs on a periodic ticker and records debt, pressure, and prune-run summaries.

2. Manual-only mode
   Automatic runs are disabled. Operators or the UI decide when to call prune-now.

This is the main mechanism for keeping long-running storage maintenance from impacting query paths during known busy periods.

## Admin Control Plane

The HTTP control surface for lifecycle now supports runtime schedule changes:

- `GET /admin/databases/{db}/mvcc`
- `POST /admin/databases/{db}/mvcc/pause`
- `POST /admin/databases/{db}/mvcc/resume`
- `POST /admin/databases/{db}/mvcc/prune`
- `POST /admin/databases/{db}/mvcc/schedule`

These routes are protected by admin permission when security is enabled.

The schedule endpoint is intentionally operator-facing: it allows the UI to slow, disable, or re-enable automatic lifecycle work without server restart.

## Query Protection Model

The current protection model is pragmatic rather than fully admission-controlled.

What is protected now:

- search mutation callbacks no longer spawn one goroutine per write event
- embedding wakeups are debounced and remain pull-based
- lifecycle automatic work can be paused or disabled entirely

What still matters operationally:

- manual prune can still be expensive on a heavily churned store
- lifecycle work still touches storage and should be scheduled with awareness of ingest and query peaks
- pinned readers can delay the most aggressive tombstone compaction path

The right operator posture is:

- use scheduled mode for steady-state maintenance
- use manual-only mode for large ingest windows or incident mitigation
- monitor reader age, debt bytes, stale-plan skips, and prune duration before making the cadence more aggressive

## Why Debounce and Scheduling Are Separate Controls

These controls solve different problems.

Debounce solves burst amplification:

- many writes arrive close together
- background work should collapse that burst into fewer units of work

Scheduling solves maintenance placement:

- some work is intrinsically long-running or storage-heavy
- operators need control over when it runs, not just how often it is triggered

NornicDB now uses both:

- debounce for mutation-driven indexing and worker wakeups
- scheduling for MVCC lifecycle maintenance

## Stress Testing Strategy

The current storage test strategy for this area focuses on two failure classes:

1. ghost-history retention
   Repeated update, delete, and recreate churn should not leave an unbounded retained MVCC chain after prune.

2. write-path amplification
   Mutation-triggered background workers should not scale linearly with every event burst.

Real-engine churn tests in the storage package are the preferred guardrail for the first class because they exercise actual Badger MVCC records, not just planner mocks.

Related documents:

- [Historical Reads & MVCC Retention](../user-guides/historical-reads-mvcc-retention.md)
- [MVCC Lifecycle Admin API](../user-guides/mvcc-lifecycle-admin-api.md)
- [MVCC Lifecycle and Compaction](../operations/mvcc-lifecycle-compaction.md)