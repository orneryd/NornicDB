# Concept: converge auto-commit and explicit-transaction execution

Basis: upstream main at 4ec45a85. Read-only investigation plus one local experiment (worktree `exp-router`, branch `exp/single-router`, image `local/nornic-test:exp-router`). Nothing pushed or posted.

## 1. What actually exists today (three routes, not two)

`StorageExecutor.Execute` decides per statement:

| # | when | storage view | router |
| --- | --- | --- | --- |
| R1 | auto-commit **read**; system commands; Fabric transactions | live engine (Namespaced→Async→WAL→Badger) | **A** `executeWithoutTransaction` (executor_query_routing.go, ~450 lines, fast paths first) |
| R2 | auto-commit **write** (`executeImplicitAsync` → `executeWithImplicitTransaction`) | `transactionStorageWrapper` around an implicit Badger transaction | **A** (executor.go:2255 `txExec.executeWithoutTransaction(txCtx, …)`) |
| R3 | statement inside `BEGIN … COMMIT` (`executeInTransaction`) | `transactionStorageWrapper` around the explicit transaction | **B** `executeQueryAgainstStorage` (transaction.go, 209 lines, its own keyword dispatch) |
| R4 | auto-commit write that is a **pure CREATE** (`isEventualAsyncEligible`: first clause CREATE, no MATCH/MERGE/SET/WITH/UNWIND/…) or matches `tryAsyncCreateNodeBatch` | live AsyncEngine, **no transaction at all** | A, or the batch fast path |

Two consequences:

- Router A already runs against the transactional storage wrapper in R2, in production, for every auto-commit write. So "can the main router work on a transaction view?" is already answered: yes.
- Router B exists only for R3. It is a second, separately maintained copy of the dispatch table with a different rule order, without the fast paths `tryFastPathSimpleMatchReturnLimit` / `tryFastPathCompoundQuery`, without `executeMatchWithCallSubquery`, `executeMultipleMerges`, `executeAlterDatabase`, and with the only call to `setRevealOnEngine` / `hasRevealCall`.

## 2. Experiment: delete router B

Change (1 file, +2 / −204): `executeQueryAgainstStorage` keeps its name and the reveal/decay preamble, and its body becomes `return e.executeWithoutTransaction(ctx, cypher, upperQuery)`.

Results:
- `go build ./...`, `go vet ./pkg/cypher`: clean.
- `go test ./pkg/cypher/... ./pkg/bolt/... ./pkg/server/... ./pkg/nornicdb/...`: everything passes except
  - `TestExecuteQueryAgainstStorage_DispatchBranches` and `TestExecuteQueryAgainstStorage_DispatchCoverage`: they pin router B's own error wording and refusals (`SHOW WHATEVER` → "unsupported SHOW command…" vs router A's "unsupported query type: SHOW …"; `LOAD CSV`, `ALTER COMPOSITE DATABASE`, `DROP INDEX` are refused/no-op in B and handled by A);
  - `TestLoadPluginsFromDir` (fails identically on unmodified main in my environment).
- My three Cypher batteries (131 shapes) in both modes: same failing set as main (13/46, 11/50, 20/35) — no regression, and as expected no fix either, because the defects are in the handlers, not in the router.
- Performance: not measured yet (see §5).

So the router convergence itself is small and low-risk. **Decision needed from the maintainer:** inside an explicit transaction, should `LOAD CSV`, `ALTER [COMPOSITE] DATABASE`, `DROP INDEX` be (a) allowed as in auto-commit, or (b) still refused? If (b), that is a 10-line guard in `executeInTransaction`, not a second router. The two dispatch tests then get rewritten to assert that policy instead of B's wording.

## 3. What the router merge does NOT fix: the storage views still differ

After the experiment the two modes still disagree in exactly the places where the *storage view* differs, which proves those are not routing bugs:

1. **#461 — relationship invisible to transactional reads.** Strong lead: the failing case is a pure `CREATE (a)-[:R]->(b)` in auto-commit, which is precisely route **R4** (no transaction; written straight to the live AsyncEngine). The working cases are `MATCH … CREATE` (R2, implicit transaction) and creation inside `BEGIN` (R3). Hypothesis (code-path evidence, not yet proven by a storage-level test): the non-transactional edge write skips the MVCC head/version bookkeeping that snapshot readers use for edges, while node writes on the same path do get it. Verify with a storage test: write node+edge through AsyncEngine without a transaction, then `GetOutgoingEdgesVisibleAt` from a new snapshot.
2. **#475 — `head(a.tags)` in WHERE: 2 rows in auto-commit, 0 in a transaction**, still true with one router. The transaction wrapper hands property values back in a different Go shape than the live engine (list type), and the function fails on one of them.
3. #462 (`SET h.n = h.n + 1` → `'<nil>1'`) and the ordering-dependent wrong answers differ only through row order of the view.

## 4. Proposed target (in the maintainer's terms: one path, BEGIN only when the client says BEGIN, rollback on any failure)

Step 1 — **one router** (the experiment). Reveal/decay preamble moves into the common entry so auto-commit gets it too.

Step 2 — **one write route**: remove R4 as a separate semantic. Either the pure-CREATE fast path goes through the implicit transaction like every other write, or it keeps its speed but must produce the same MVCC records as a transactional write (contract test below decides). This is where #461 gets fixed, and it is the piece with a real performance question (R4 exists for bulk-insert throughput).

Step 3 — **one read contract**: keep auto-commit reads on the live view (no snapshot cost), but add a storage *contract test suite* that runs the same assertions against (a) the live engine stack and (b) `transactionStorageWrapper`: same value types for properties (lists, maps, temporals), same visibility of nodes and edges written by each write route, same label/edge iteration results. Today only the inner BadgerEngine is tested. #475 and #461 become failing contract tests first, then get fixed.

Step 4 — **both-modes harness for Cypher tests**: a helper that runs a query through auto-commit and through BEGIN/COMMIT and asserts identical results; wrap the existing table-driven Cypher tests with it. This is what stops the next divergence.

Order: 1 → 4 → 3 → 2 (cheap and safe first; the performance-sensitive write route last, with benchmarks).

## 5. Acceptance data the maintainer asked for ("all existing query tests work, nothing removed, at least as fast")

- Tests: full `go test ./...` in CI (only the two B-wording tests change, per the decision in §2).
- Speed: run the repo's existing parser/executor benchmarks (`go test ./pkg/cypher -bench=. -benchmem`) before/after. Expectation: explicit-transaction statements get **faster** for simple `MATCH … RETURN … LIMIT` and compound shapes, because router A's fast paths were never reachable inside a transaction; auto-commit is untouched by step 1.
- Correctness: the 131-shape batteries in both modes, plus the new both-modes harness.

## 6. Risks

- Fast paths in router A have so far only seen the transaction wrapper for writes (R2). Read fast paths on a snapshot view are new territory; the suites pass, but they were not written to probe that. The both-modes harness (step 4) is the mitigation.
- The result cache lookup in `Execute` happens before the "active transaction?" check. Inside an explicit transaction a cached read could be served from before the transaction's own uncommitted writes (the key includes the global graph mutation version, which a not-yet-committed write may not bump). Not verified; worth one test (`BEGIN; CREATE; MATCH same pattern twice`).
