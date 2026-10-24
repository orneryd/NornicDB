# PR 1653 / NOR-60 readiness

Local final gate: make test. Broader verification deferred to CI per maintainer instruction.

- [x] Verify linked issue is assigned to current user; read all PR feedback and handoff.
- [x] Rebase onto current release including PR 1674.
- [x] Reproduce current cost guards and capture workload CPU/baseline evidence.
- [x] Implement the shared transaction-overlay correction with isolated correctness regressions.
- [ ] Review public consumers and run focused checks on both parsers.
- [ ] Compare affected workloads and complete make test.
- [ ] Push, publish exact-source results, update NOR-60 and mark ready.

Fail-first on rebased test-only checkpoint f616c90706a25d0c46044d33b3bb3ab0555a5902: both guards fail (committed candidate lookup 3532 allocations). Baseline profiled benchmarks: 5k pending create/delete 13.28s, committed MERGE 2.558s. CPU profile attributes 64% of samples to repeated Badger pending-write iterator sorting during cascade materialization.

Correction: one lazily maintained pending adjacency owner handles create/update/move/delete and serves outgoing/incoming/between plus stored incident cascades. Bounded equality populations use the existing pinned physical label reader and equality-key index; pending writes shadow the snapshot through its existing pending-node index. Read-committed views decline caching; oversized/embedded populations answer their first scan then decline retention. Cache limits reuse existing node/byte/stream budgets and reset with snapshot state. Operation-ordered newly created edge bodies avoid pointless committed-history reads during cascades; existing edges keep their archive/read path. The body map is allocated only for commits that cascade edges.

New regressions cover endpoint moves, self-loops, delete/recreate, copy ownership, pending label/property changes/deletes, numeric equality, namespaces, peer-commit isolation, lifecycle and cache refusal. Bolt/HTTP tests cover pending and committed MERGE followed by deletion, auto-commit and explicit transactions. A test initially treated a map as unkeyable; the existing canonical key owner supports maps, so the refusal case now uses nil. No cost-guard assertion was relaxed.

Graphify MCP/CLI is unavailable in this environment. Mutation/consumer convergence was audited with source searches: pending-edge map writes/removals occur only in the shared overlay owner; all incident read/cascade callers use it. No remote graph refresh was attempted.

Review found a fail-first temporal equality regression: a stored date-time was Cypher-equal but raw comparable keys differed. Optional pruning now accepts only canonical numeric/string/bool keys; temporal/composite predicates remain with Cypher. Regression covers temporal-only and mixed scalar/temporal probes. Superseded full-suite runs were cancelled after source corrections and are not passes.
