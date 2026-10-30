## Context

Baseline: `a427a46815c607d0801331f4975e26cc941d125a`. The router merge already
landed. Remaining divergence is in overlapping executors, evaluator scopes and
storage capabilities. Historical experiment results must be reproduced.

## Goals / Non-Goals

Goals: TCK-first regression control, no silent wrong values or skipped clauses,
shared semantics across modes, snapshot isolation, atomic mutations, and
measured streaming fast paths.

Non-goals: mandatory full-tree parsing on existing hot paths; a new independently
interpreted generic text executor; decay-policy, GPU or search redesign;
deletion of public APIs based only on static reachability.

## Decisions

1. Follow [the implementation plan](../../../docs/plans/cypher-convergence-plan.md)
   for module boundaries, dependencies, test matrix and performance gates.
   [The issue matrix](../../../docs/plans/cypher-convergence-issues.md) defines
   closure evidence for every scoped issue.
2. Establish the pinned TCK and local reproduction baseline before parser edits.
   Preserve all existing regressions and fast-path selection assertions.
3. Share preparation but retain public/internal lifecycle differences. Child
   fragments inherit context, bindings, database/auth and transaction state.
4. Use a lightweight lexical cursor and complete shape recognition for fast
   paths. Uncovered shapes surface as explicit unsupported or syntax errors;
   there is no interpreter fallback. Never redispatch reconstructed query
   strings as an execution path.
5. Separate `Handled`, `NotApplicable`, `ParseRejected` and `Failed` outcomes.
   Unhandled shapes return proper unsupported or syntax errors; there is no
   alternate execution path. Runtime errors roll back and cannot become parser
   retries.
6. Adopt one value/scope contract across assignments, computed rows, predicates
   and compiled optimizations. Undefined symbols are errors; bound null follows
   Cypher three-valued logic. Aggregate expressions use group results before
   enclosing scalar evaluation.
7. Require atomic write publication and consistent capability forwarding through
   the production storage stack. Snapshot reads retain own writes and exclude
   later commits. Projection/streaming optimizations cannot bypass visibility.
8. Preserve successful ALTER DATABASE / ALTER COMPOSITE DATABASE transaction
   behavior with explicit lifecycle tests. Keep knowledge-policy branches out
   of the change.
9. Remove each redundant path only after its shapes pass external conformance,
   route parity and workload-scoped benchmarks. Retained fused paths share the
   same behavior contract.

## Migration Plan

Execute Steps 1–9 in the plan, splitting focused OpenSpec changes along its
named change IDs. Transfer applicable requirements to each focused change
without maintaining conflicting copies. Keep this program's tasks open until
its overall gates pass. Archive verified focused changes into canonical specs;
artifact completion alone is not implementation completion.

Preserve an isolated fixture for every route/mode comparison. Never shadow-run
mutations against the same live data. Keep the last correctness-qualified build
as deployment rollback. If #461 needs existing-data repair, implement a separate
idempotent repair with dry-run and recovery evidence.

## Risks / Trade-offs

- A self-written test comparator can conceal defects. Validate it with negative
  controls, official expected values and an independently pinned Neo4j server.
- Fast-path partial execution makes alternate routes unsafe. Complete eligibility
  checks before effects and stage writes in the atomic statement context.
- The current pipeline has reported semantic discrepancies. Use it as migration
  material, not the standard of correctness.
- Pooling and shared state can leak bindings across requests or alias returned
  results. Give scratch explicit lifetimes and test cancellation and races.
- Snapshot streaming must merge pending writes and preserve tombstones/index
  visibility. Test the full production stack and persistence, not only Memory.
- TCK may reveal additional missing language features. Add each gap to the
  ledger and complete the pinned core suite before claiming conformance.
