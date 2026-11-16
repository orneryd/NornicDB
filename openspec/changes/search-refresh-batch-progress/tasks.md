# Deferred search refresh progress

- [x] Reproduce postponed vector refreshes through committed storage events plus public Cypher write load.
- [x] Replace machine-speed assumptions with a virtual-time regression; confirm it fails on release.
- [x] Keep one canonical first-queued-write timer; later scheduling requests join the armed window.
- [x] Verify the public regression five times, focused search/index controls and race checks.
- [x] Compare scheduling allocations and CPU profiles against the same release baseline; keep the measurement scoped to scheduling.
- [x] Document scheduling behavior without promising a hard refresh-completion deadline.
- [ ] Publish the candidate and run exact-head CI and advisory review.
- [ ] Address findings and record final readiness; combined release verification remains separate.
