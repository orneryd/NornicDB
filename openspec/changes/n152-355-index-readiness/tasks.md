# N152-355 Q09 index readiness

- [x] Confirm the release head has no existing Q09 PR and reproduce the public failure on both parsers.
- [x] Reuse the state reported by `SHOW INDEXES` and preserve the vector query storage fallback.
- [x] Add timeout, cancellation, ready-state, and missing-name regression checks.
- [x] Bring the existing dispatch smoke fixture to ONLINE before asserting a successful await.
- [x] Start the in-memory differential fixture's search build before its await step.
- [x] Use the shared integer procedure argument reader required by the numeric ownership gate.
- [x] Correct the migration guide's immediate-wait claim.
- [ ] Run focused and affected-package checks on the final head.
- [ ] Review duplicate candidates, open the release PR, and update Plane with exact verification.
