# Recovered derived-count authority (NOR-419)

Release base: `83b8c5884deaa12bc686ac40512d689a7d44a20f` (v1.4.4).
Follow-up to #1690 / NOR-136: `docs/development/replay-snapshot-completion.md`.

Two recovered-data defects share one cause. Physical replay transports entity
bodies without the destination-local label deltas that justified them, while a
semantic record replayed beside it appends its own deltas. Such a delta set
cannot be folded, and the exported single-entry replay never reached the
recovery boundary that supersedes it.

- [x] Preserve the ticket reproductions on the rebased release head: failing baseline in `/tmp/pr1743-nor419/01-repro-single-entry.log`, `02-repro-both.log`, `13-prefix-final-tests.log`.
- [x] Reproduce snapshot + transported relabel + acknowledged delete through `RecoverIntoEngine`: both records apply, then recovery returns `derived count underflow for test:New` (`Applied:2`, `Failed:0`).
- [x] Reproduce one buffered record through the exported `ReplayWALEntry`: body is `New`, counts stay `Old=1`/`New=0`.
- [x] Name the tests by behavior (`wal_recovery_label_count_agreement_test.go`); retire the ticket-named reproduction file.
- [x] Make the recovery completion owner supersede replayed derived-count deltas in bounded transactions instead of folding them (`discardCountDeltas`), then verify/rebuild base counts from committed bodies.
- [x] Make the exported `ReplayWALEntry` its own recovery boundary for a physical buffered commit; keep batch owners on `replayWALEntry` so reconciliation stays once per batch, not once per record.
- [x] Verify both ticket cases, both affected destination modes (ordinary Badger, write-behind), the following ordinary delete and the close: `12-final-tests-matrix.log`.
- [x] Verify the committed tests fail on unmodified release source with the ticket's exact recovery and `label-count final fold` messages: `13-prefix-final-tests.log`.
- [x] Targeted `-race` run over both cases and the adjacent recovery/replay controls: `10-race-targeted.log`.
- [x] Focused affected-package checks: recovery/replay, label and edge-type counts, restart integrity, WAL and write-behind suites, `pkg/nornicdb` recovery (`04`–`07`), final combined focused sweep (`14`).
- [x] Verify the same causal correction for edge-type counts: single-entry type replay and snapshot + type-change + semantic deletion, ordinary and write-behind destinations, strict close, and fail-before evidence.
- [x] Bound discard key retention to one chunk; exercise three chunks per family, canceled admission and authoritative recount after discard.

- [ ] Exact-head full make test CI, parent independent review and advisory duplicate dispositions remain pending.

No stored key, WAL record, storage-format or public-API shape changed.
`ReplayWALEntry` gained a completion step, not a new argument. Duplicate review
was manual: the correction extends the existing count-delta kernel
(`badger_count_deltas.go`) and the existing recovery completion owner
(`badger_write_behind_recovery.go`) instead of adding a parallel accounting path.
The remote code graph answered HTTP/HTTPS mismatch errors and gave no usable
result, so no graph-based duplicate claim is made.
