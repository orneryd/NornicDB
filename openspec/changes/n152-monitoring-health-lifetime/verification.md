# Monitoring health request lifetime

Base and reviewed release: `f2e264f1f02f615ccbf9be498ed94993448dcb9d`.
Release review context: PR #1857. No Plane issue created for this review follow-up.

## Outcome and causal owner

`ui/src/store/monitoringStore.ts` starts health alongside status/metrics but previously omitted health from the completion promise. A fast successful status released `request` and cleared its timeout while health was pending. This allowed overlapping health requests, prevented unmount from aborting that request, and permitted a stale earlier health success to update connection status during a later poll in the same identity generation.

Add health to the existing `Promise.all`. Keep immediate status publication and existing error/deadline/generation logic. No new polling owner or public API is introduced.

## Evidence

- Original source with new tests: 2 expected failures (premature completion; unmount does not abort health); 2 availability controls pass. `/tmp/n152-monitoring-health-red.log`.
- Fixed focused suite: 4/4 PASS; includes no overlapping refresh, 10-second abort, recovery after cancellation, late old-generation response, and status failure with health success/failure. `/tmp/n152-monitoring-health-green.log`.
- `cd ui && npm test`: 52/52 PASS, no skips. `/tmp/n152-monitoring-health-ui-tests.log`.
- `cd ui && npm run build`: PASS; existing Vite large-chunk warning. `/tmp/n152-monitoring-health-build.log`.
- `cd ui && npx playwright test browser-tests/server-status.spec.mjs --workers=1`: 4/4 PASS in 10.3s using built assets and Chrome. `/tmp/n152-monitoring-health-browser.log`.
- `make verify-list BASE=origin/release/v1.5.2`: PASS; fixture self-tests emit missing temporary CI YAML warnings, exit 0. `/tmp/n152-monitoring-health-verify-list.log`.
- `GOMAXPROCS=2 GOMEMLIMIT=1500MiB GOFLAGS=-p=1 make verify BASE=origin/release/v1.5.2 ONLY=localization FETCH=0`: PASS. Other verifier steps explicitly not selected. `/tmp/n152-monitoring-health-verify.log`.

The regression loads the actual store and API via the existing Vite SSR test pattern. Only fetch, browser globals and timers are substituted. Fetch cancellation follows AbortSignal; an additional deliberately late response validates generation isolation. No live backend/network-timing claim.

## Reuse and qualification limits

The change reuses the existing poll Promise.all, controller, timeout and generation check. `AssistantAvailability` has a separate assistant-status contract; moving health there would split connection monitoring ownership. Existing `monitoring.test.mjs` checks numeric transformations and does not load the polling owner; this new test uses the established Vite loader pattern from `nor182-result-key-preservation.test.mjs` without copying polling logic. No abstraction or duplicate polling implementation added.

Indexed duplicate analysis is pending: no graphify MCP is exposed in this session and no exact-head graph is present in the fresh checkout. Release duplicate analysis is advisory under CONTRIBUTING. No empty/clean indexed result claimed. As a subagent, no further worker delegation was performed (the delegation skill prohibits worker re-delegation).

Full make verify, Go test/coverage, allocation/conformance/differential gates, and full browser suite were not run for this scoped candidate. Parent owns shared release qualification. Existing UI dependencies were cloned from a matching lockfile checkout; no dependency changes. Browser-generated tracked screenshots were archived outside the worktree then restored.
