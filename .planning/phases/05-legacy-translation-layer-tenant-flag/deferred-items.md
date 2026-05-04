# Phase 5 Deferred Items

Issues discovered during Phase 5 plan execution that are outside the
plan scope and would require their own remediation work.


## Plan 05-04: pre-existing flake — `TestEmbedTriggerAdditionalBranches`

**Discovered during:** Plan 05-04 Task 02 full-package regression run
(`go test -tags nolocalllm -count=1 -timeout=300s ./pkg/server/`).

**Symptom:** `TestEmbedTriggerAdditionalBranches` (in `pkg/server/server_extra_test.go`)
fails when run alongside the full pkg/server suite (-count=1, ~17–65s
window). Passes deterministically when run in isolation
(`go test -run 'TestEmbedTriggerAdditionalBranches'` exits 0 in 0.7s).

**Out of scope for Plan 05-04:** the test exercises the embed-worker
`Trigger` API; it has zero overlap with `handleMetrics`, `obsRegistry`,
`RenderLegacy`, or any Plan 05-04 surface. Pre-existing timing/ordering
flake — git blame shows the test predates the Phase 5 work; the
intermittent shape is consistent with embed-worker goroutine scheduling
under heavy parallel test load.

**Recommendation:** investigate as a separate `pkg/server` test-flake
hardening pass (post-M1). Not blocking for the M1 single-PR review since
the rest of the suite (and the Plan 05-04 tests in particular) pass
race-stable under `-race -count=10`.
