# PR #1655 / NOR-61 — current release readiness

Baseline: release/v1.4.4 `15f1a5028893195d87b2f72c44d61baa4b1ec390`.
Original PR head: `2a082def193b788ffc6c9220045460cabe86d85c`.
Port required behavior to canonical release owners; rebase updates, never merge.

- [x] Select inactive Urgent issue, assign NOR-61 to Tim, preserve other active lanes.
- [x] Reproduce false UNIQUE classification on release through Cypher and native transactions; unchanged string, numeric, list keys fail; changed/new claim controls pass.
- [x] Complete acceptance coverage matrix against current release, preserving original failures.
- [x] Correct key transitions and atomic UNIQUE publication through current storage owner, including logical ACK and bounded crash replay.
- [x] Focused missing-target staging/commit, snapshot lifetime and deletion-order checks through current owners; full qualification remains below.
- [x] Focused real Execute, Bolt and HTTP commit-read fault checks preserve original cause and database status.
- [x] Migrate approved typed node/relationship update APIs and canonical callers; preserve immutable observed images without ordinary-read maps.
- [x] Fix reproduced prefix/restart stale UNIQUE claims, composite observed routing, Qdrant failed-read acknowledgement, direct timestamp defaults and unadmitted replica request journaling.
- [x] Resolve in-scope duplication without restoring obsolete registries, decoders or forwarding APIs; checker remains informative and unmodified.
- [x] Refresh remote code graph from immutable release head in background; independently verify final repository commit receipt.
- [x] Run focused correctness/race checks and workload-specific paired performance qualification.
- [ ] Rebase onto current release, resolve conflicts, run make verify-list and full make verify; fix regressions.
- [ ] Update PR and Linear AC with evidence and outcome table; push existing PR head and mark ready only after qualification.

Evidence: `/Users/timothysweet/.local/share/nornicdb-pr-readiness/pr1655-current-release`.
