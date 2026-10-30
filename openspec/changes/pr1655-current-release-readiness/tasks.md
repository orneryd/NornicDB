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
- [x] Resolve initial duplicate inventory without obsolete registries, decoders or forwarding APIs; checker remains informative and unmodified.
- [x] Finish fresh inventory convergence and review every retained boundary with concrete source evidence; final integrated qualification remains below.
- [x] Publish stabilized commit 97df587a8, matching the full make test passing tree b616e7f.
- [x] Apply outcome-focused PR description and expanded NOR-61 acceptance criteria/test matrix using the new writing skills.
- [x] Finish the latest29-row source convergence review, preserving original test populations and per-case controls.
- [x] Reproduce stale census failure, regenerate with the existing command and inspect unchanged persisted-field population.
- [x] Resolve intermittent auth allocation-budget failure through the approved maintained decoder owner; published pin passes original race/count20 and canonical JSON race checks.
- [x] Qualify follow-up auth state/Qdrant convergence with focused race checks and full make test on db289.
- [x] Qualify the canonical manual-drain lifetime fixture correction: original3/100 failures and forced schedule preserved; all BEGIN lifetime cases pass race/count30; final full make test passes on5bc2a6d6.
- [x] Refresh remote code graph from immutable release head in background; independently verify final repository commit receipt.
- [x] Run focused correctness/race checks and workload-specific paired performance qualification.
- [x] Fetch current release; base remains15f1a502, no rebase needed.
- [x] Run final full make test on frozen5bc2a6d6 as the maintainer-approved readiness gate; preserve canceled make verify as incomplete.
- [x] Prepare the skill-format PR/Linear evidence, explicit PR-branch push target and exact gate-result comment. Publication and review status are recorded on PR #1655 and NOR-61 after push.

Evidence: `/Users/timothysweet/.local/share/nornicdb-pr-readiness/pr1655-current-release`.
