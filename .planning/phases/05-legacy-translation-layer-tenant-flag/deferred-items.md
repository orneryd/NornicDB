# Phase 5 Deferred Items

Issues discovered during Phase 5 plan execution that are outside the
plan scope and would require their own remediation work.

---

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

---

## Plan 05-05: RFC 9745 cutover (Deprecation header value)

**Source decision:** R-01 (Resolution Addendum, 05-CONTEXT.md, 2026-05-04).
ADR-0001 §2.3.1 amendment records the deliberate non-conformance.

**What:** Migrate `Deprecation: true` literal → `Deprecation: @<unix-timestamp>`
per RFC 9745 §2.1 Structured-Field Date format.

**Why deferred:** RFC 9745 (March 2025) supersedes the original draft
Deprecation HTTP header that REQ MET-20 / ROADMAP SC#2 were written against.
Migrating mid-M1 would have required REQ + ROADMAP + ADR amendments,
expanding the scope of Phase 5 beyond its single-PR review budget. The
non-conformance is a customer-API-contract decision the team accepts for M1.

**Trigger:** Customer report of HTTP-strict gateway rejecting the
`Deprecation: true` value, OR M2 major-version bump (which removes the
legacy `:7474/metrics` surface entirely after Sunset passes).

**Owner:** Future ADR amendment + REQ MET-20 update + Phase-N planning where
N ≥ M2 entry. Estimated impact: ~3 LOC change in
`pkg/observability/legacy_translation.go::LegacyDeprecation` const +
golden-file regenerate + ADR §2.3.1 amendment + REQ MET-20 text update.

**Locked literal until cutover:** `const LegacyDeprecation = "true"` in
`pkg/observability/legacy_translation.go`.

---

## Plan 05-05: Removing unused `s.Stats()` / `s.db.Stats()` / `s.db.EmbedQueueStats()` accessors

**Source decision:** 05-CONTEXT.md "Deferred Ideas"; Plan 05-04 confirmation.

**What:** After Plan 05-04 rewrote `handleMetrics` to delegate to
`observability.RenderLegacy`, the three accessor methods are no longer called
from inside `handleMetrics` itself. They remain consumed by other endpoints:

- `s.Stats()` — called by `handleStatus` (server_public.go:53) and the admin/UI
  request flow.
- `s.db.EmbedQueueStats()` — called by `handleStatus` embed-info block
  (server_public.go:116).
- `s.db.Stats()` — used outside Plan 05-04 scope (admin endpoints, replication
  catch-up, integration tests).

**Why deferred:** Removing the accessor methods themselves is out of Phase 5
scope and would require its own cross-cutting refactor. Other callers in
`pkg/server`, `pkg/admin`, and integration tests would all need to migrate to
reading from the unified registry via the same `RenderLegacy` (or an analogous
`RenderStatus`) shape. Not blocking M1.

**Trigger:** Post-M1 `pkg/server` cleanup phase OR a doc-generator (Phase 11)
discovery that the parallel state path causes a doc/code drift.

**Owner:** Post-M1 cleanup phase or M2 deprecation-removal phase (whichever
lands first).

---

## Plan 05-05: Telemetry on deprecation usage

**Source decision:** 05-CONTEXT.md "Deferred Ideas".

**What:** A self-instrumentation counter that increments on every scrape of
`:7474/metrics` (the deprecated legacy adapter). Allows operators / the
NornicDB team to observe customer migration progress (how many customers
still scrape the legacy surface) before the M2 deprecation-removal decision.

**Why deferred:** Out of Phase 5 scope. The counter would need to live in
`pkg/server` (the legacy `:7474` listener owner), but the metric registration
must go through `pkg/observability` per MET-04 lint-cardinality. Adding it
mid-M1 widens the Phase 5 scope without informing any near-term decision.

**Trigger:** When the M2 deprecation-removal decision is being scoped — at
that point usage data becomes load-bearing for the cutover timeline.

**Owner:** Future "observability self-instrumentation" follow-up plan, post-M1.

**Recommended metric shape:**
```
nornicdb_legacy_metrics_scrapes_total{user_agent_class}  counter
```
(`user_agent_class` would be a closed enum: `prometheus`, `agent`, `unknown`
to bound cardinality.)

---

## Plan 05-05: Migration documentation (`docs/getting-started/migrating-from-legacy-metrics.md`)

**Source decision:** 05-CONTEXT.md "Deferred Ideas".

**What:** Customer-facing migration guide explaining how to move scrape configs
from `:7474/metrics` to `:9090/metrics`, including label mapping table for the
12 legacy metrics → ADR §2.3 unified-registry equivalents.

**Why deferred:** Phase 11 (Metrics Reference Doc Generator) owns
`docs/operations/` documentation. The migration guide is best authored
alongside the metrics reference doc so the cross-references stay consistent.

**Trigger:** Phase 11 entry.

**Owner:** Phase 11.

---

## Plan 05-05: Removing `:7474/metrics` entirely

**Source decision:** 05-CONTEXT.md "Deferred Ideas"; CLAUDE.md "Public API
contract — No removal without major version."

**What:** Drop the legacy `:7474/metrics` HTTP route, drop
`pkg/observability/legacy_translation.go` + `legacy_translation_test.go` +
`legacy_snapshot.golden`, drop the ResolveAndLogTenantLabels helper if no
other consumer.

**Why deferred:** Out of M1 scope. CLAUDE.md mandates removal happens at a
major version (M2 or later) AFTER the Sunset date passes
(`Fri, 31 Dec 2027 23:59:59 GMT`). Removing earlier breaks customer scrape
configs without the documented overlap window.

**Trigger:** M2 (or later) major-version planning, AFTER 2027-12-31.

**Owner:** Future M2+ deprecation-removal phase.
