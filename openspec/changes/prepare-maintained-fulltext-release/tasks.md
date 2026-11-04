# PR #1683 / NOR-148: maintained full-text release readiness

Target `release/v1.4.4`; preserve the current refactored owners. Do not merge.

- [x] Fetch/prune remote-tracking refs and pin release `31208b17fe7df60cfab1a6062e91d95711abc9f3`.
- [x] Preserve original head `37a9b4d045b60ffa6cb6777de8da93f6404c27cb` and its three-commit fix against `de70c0e8375e9c74800d5ab6d7da5a6505650cb8`.
- [x] Confirm NOR-148 is Urgent in v1.4.4 and assign it to Tim Sweet.
- [x] Remove unprotected obsolete local branches, worktrees and old release-readiness archives; preserve active/user work and report protected exceptions.
- [x] Reproduce repeated corpus construction and folded NOR-294 field-identity/order defects on the current release through public execution.
- [x] Establish Neo4j field/phrase semantics and distinguish unavailable R14 application data from generated workload qualification.
- [x] Port necessary maintained-document behavior into current search/capture/memory owners; retain source/snapshot/visibility and cancellation contracts.
- [x] Complete create/update/delete, concurrent mutation, restore/restart, namespace and relationship lifecycle acceptance criteria.
- [x] Preserve exact case-sensitive field identity and deterministic property selection/order through shared document construction.
- [x] Profile named multi-size workloads; demonstrate retained reductions in work, allocations and memory without changing eligible results or ranking.
- [x] Complete independent current-source review and duplicate/reuse investigation; do not change checker rules without permission.
- [x] Run focused and affected-package tests, then the agreed full `make test` local gate. Broader `make verify` remains deferred to CI unless directed otherwise.
- [x] Publish the replacement head to the existing PR with an exact lease; keep the final reviewed metadata follow-up on the same PR.
- [x] Perform final workspace cleanup and report remaining evidence limits.

No v1 full-text path, vector-file NOR-351/352/353 work, old parser/storage APIs or compatibility wrappers are restored. Badger keys/payloads and storage version must remain unchanged. Any required breaking Go contract or storage migration needs a concrete decision first.

DeepSeek lanes: read-only original-owner review `1f95e64ebb004309a1aa9f904a912edb`; isolated current-release fail-first tests `dc457f63021d47578cc9ae9fd396102e`. Parent owns architecture, integration and acceptance. A prior review submission with an invalid base was canceled before replacement; it is not evidence.

Evidence: `/Users/timothysweet/.local/share/nornicdb-pr-readiness/pr1683-release/`. PR metadata's historical base SHA is not the current branch tip; the fetched remote release ref above is authoritative. Indexed graph baseline `35e3bddb` is stale and advisory only. Linear field-notes integration is unavailable; the live issue is readable.

Current integration corrections: delayed caller cancellation, storage I/O error propagation, reset certificates, resident-memory double charging, database/replication wrapper view proof, empty canceled projection retirement, and 23 explicit fixture registry entries. The shared search read context preserves immediate caller Err observation and service shutdown join. The retained corpus receives only shutdown observation and reporting, without direct access to vector-serving state.

The second untagged Mac `make test` attempt was superseded after exposing explicit-context census findings and an expensive second-query postings build. It is incomplete, not passing. UI was actually built and the native library matches checked-in revision/headers. Both earlier run logs remain preserved. The follow-up correction removes the secondary postings representation, computes body frequencies in one pass, and retains only the most recent term set. Its new Go read API passes the accepted context explicitly. Focused cancellation/census checks and repeated cold/first-reuse/warm benchmarks must pass before the final integrated `make test`. Private R14 replay remains unavailable and is explicitly distinguished from the generated reproduction.

NOR-351/352/353 are assigned to Tim Sweet, High, Dev In Progress, in v1.4.4. Their recovered vector checkpoint implementation is isolated on `codex/vector-checkpoint-v144` and is not included here.

Final statistics integration: focused public/full-text tests, ANTLR, race checks, 84 cross-head row/order/score comparisons, 25 paired benchmark workloads, and the reviewed lexical population pass. The explicit-context proposal passed both blocking nil-flow census controls with zero in-scope nil flows. Parent integration removes a redundant statistics map allocation and verifies that cached terms cannot pin a large query buffer. Final untagged `make test` is running on frozen Go/test-data inputs; it is not yet a pass. The large-document one-GC and two-GC diagnostic profiles distinguish the optional normalized body from transient existing decoder pools; all observed memory costs are documented.

Integrated attempt completed: `make test` ran 4,029.6 s with 146 passing packages and one failed package, `testing/capturecensus`, solely `TestStorageReconciliationRecords`. The 6,702 recorded Go/test-data inputs remained unchanged. The source-pinned review of the retired search preflight wrapper has now been updated after canonical-constructor/lifetime review and a passing isolated unchanged control. Runtime code and benchmarks remain unchanged. A corrected full `make test` is being run; prior failure evidence remains preserved.

Final local gate passes: untagged `make test`, 147 packages, exit 0, 1,375.58 s; all 8,317 recorded repository/native/UI inputs unchanged during the run. The source-reconciliation record correction is included in that pass. Earlier failures remain preserved. The operator approved generated R14-shaped qualification on 4 November 2026; the unavailable private replay is documented and is not a blocker. Runtime Go source and benchmarks remain identical to pushed checkpoint `812c4391c`.

Cleanup: eight completed helper/reference/baseline worktrees and their no-longer-needed local branches are removed. The active PR checkout, isolated vector follow-up, original reference refs, current evidence, user/shared worktrees and the active checker-relay checkout are preserved. No remote branches were changed. Two previously identified obsolete immutable checker copies remain protected pending the separate flag-removal decision; active checker files are untouched. The shared Go build-cache cleanup follows only after confirming no build/test process remains.
