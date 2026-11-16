# Native empty procedure results

- [x] Reproduce nil direct results, YIELD failure and HTTP failure.
- [x] Normalize successful nil results at the canonical handler boundary.
- [x] Preserve errors, columns, row results, nested writes and statistics.
- [x] Consolidate scoped fixture cleanup without forwarding aliases.
- [x] Independently check integrated public paths with race detection.
- [ ] Rebased-head checks, CI and advisory dispositions.

- [x] Rebase onto merged #1846; preserve the canonical procedure result correction.
- [x] Fix historical unapplied fixture ownership; three storage race repeats pass.
- [x] Restore actual default WAL rollover pressure independently of vector-image repetition; all four Bolt schedules and exact reopen/replay assertions pass.
- [x] Rebased native CALL and HTTP focused race checks pass.
- [ ] Exact-head replacement CI and final advisory review.
