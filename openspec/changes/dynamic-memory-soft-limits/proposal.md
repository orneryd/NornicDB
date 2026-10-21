## Why

An 8 GiB container reportedly OOM-killed NornicDB despite
`NORNICDB_MEMORY_LIMIT=7168`. That setting controls Go's soft GC target, not a
process ceiling or a query allocation barrier. The requested outcome is to
reject new expensive work and cooperatively terminate an offending transaction
with a useful error while keeping the service available where headroom allows.
This is explicitly a soft-limit contract, not a guarantee against OS OOM kills.

## What Changes

- Specify one application memory-policy owner and one execution gate shared by
  caller cancellation, deadlines, transaction budgets and application pressure.
- Dynamically size the shared transaction budget from configured application
  limits, effective container constraints, measured baseline and reserved headroom.
- Reuse approximate transaction accounting and coarse reservations from develop;
  do not infer individual transaction usage from process-wide memory deltas.
- Explicitly cover indexes/builds/inserts, embedding workers, LLM/Heimdall inference,
  reranking and variable batches; no fixed or exactly measured per-feature cost is assumed.
- Reserve room for Badger/WAL buffers, dirty/writeback pages, flush/compaction,
  native/model work, response completion and transaction cleanup. Fsync does not
  mean this memory has been released.
- Preserve rollback before irreversible commit and truthful outcomes afterward.
- Specify HTTP admission rejection, Neo4j-compatible transaction failures,
  operational measurements, reproduction tests and paired performance evidence.

## Capabilities

### New Capabilities
- `dynamic-memory-protection`: cooperative application and transaction memory
  protection with dynamic sizing, admission and commit-safe cancellation.

### Modified Capabilities
None. This proposal does not silently alter an existing archived specification.

## Status and Dependencies

**Draft plan only; no runtime implementation or production fix is delivered.**
Release-fix planning base: `release/v1.4.4@d854845e9b08e510b56587fddf49694b208e6a3b`.
The working-directory Graphify documentation changes are excluded.

Merged #1368 is a develop implementation input, not a shipped release feature.
At inventory time develop has `pkg/cypher/transaction_memory*.go`, including
follow-up estimation/retention fixes; release registers
`db.memory.transaction.total.max` but lacks that implementation. Port the
reviewed family together rather than cherry-picking only the initial feature.
Cancellation lane A (#1615 release / #1616 develop) precedes memory lane B.
Both cancellation PRs are drafts; reported race/coverage/benchmark results are
not independently reproduced by this planning PR. Preserve its strict deadline
assertions. This plan complements the Cypher convergence plan's shared execution
and transaction/error contracts; it does not introduce a second execution route.

## Impact and Non-goals

Affected contracts: config/settings, execution/context, transaction ownership,
HTTP/Bolt consumers, response lifetime, native/background work and Badger/WAL
commit/flush paths. This is a Nornic operational extension plus versioned Neo4j
error/settings compatibility, not an openCypher language change. No Badger source
patch, durability-policy change, disk-spill engine, unrelated search refactor,
forced-GC-on-every-query policy or automatic unbounded transaction retry is proposed.
Transaction limits remain unlimited by default (`0`) on the release port.
Exact application-limit setting names, automatic sizing constants and pressure
victim selection remain explicit review decisions in design.md.
