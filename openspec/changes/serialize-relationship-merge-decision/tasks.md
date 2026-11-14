# Serialize relationship MERGE decisions

- [x] Reproduce duplicate relationship/counter through public Bolt on release.
- [x] Prepare bound MERGE action mutation targets through the canonical SET lock owner before MERGE existence decision; preserve independent bare MERGE staging.
- [x] Verify unlocked, start-locked, keyed and incoming schedules on both parsers.
- [x] Preserve already-locked MERGE control.
- [ ] Extend concurrent opposite-direction/self-loop and rollback/deadlock controls.
- [ ] Resolve report2 absence-guard schedules independently; do not prohibit Neo4j-valid CREATE duplicates.
- [ ] Reproduce/disposition report9 unrelated uniqueness, report16 historical relationship view and report17 schema admission.
- [ ] Performance qualification of the changed lock boundary.
- [ ] Exact-head CI and combined release verification.

## Independent MERGE admission (report9)

- [x] Public Bolt fail-first: six seeded/new explicit/autocommit MERGE cases return Outdated solely on unrelated shared/exclusive gate contention, repeated three times. MATCH waits and persists.
- [x] Submission-route prepared admission waits with the caller context; it no longer treats manager-gate contention as a native data conflict. Native ordinary publication retains its existing inversion guard.
- [x] Public successful-wait and canceled managed-commit/key-release controls pass, with native inversion and quota byte-transition controls preserved.
- [ ] Fresh exact-head CI after this correction; no throughput qualification claimed.

- [x] Reproduce and correct the mirrored ON MATCH bound-target wait; apply the selected action exactly once after the competing relationship publishes.
