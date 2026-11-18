# NOR-180 verification

- [x] Preserve fail-first macOS and Linux evidence on production-identical v1.5.1 source.
- [x] Confirm successful-cleanup control and missing ErrTransactionClosed identity.
- [x] Reuse CombineTerminalOutcomes in the shared transaction-admission defer.
- [x] Verify explicit/implicit setup, namespace and configuration cases on macOS.
- [x] Verify final focused matrix on macOS/Linux, native/ANTLR, and race detector.
- [x] Verify NOR-180 WAL producer/consumer, BEGIN and Fabric cleanup regression matrix.
- [x] Run duplicate checker and document actual reuse analysis and limitations (service unavailable; not a pass).
- [x] Run make verify-list; start make verify, then cancel at maintainer request.
- [ ] Finish affected pkg/cypher package check; maintainer requests focused/package checks locally and CI after the fix.
- [ ] Open the release fix PR with milestone v1.5.2 and evidence-backed status.

Baseline: d6ec7766e, production-identical to v1.5.1 cb0a574f. Failing evidence:
/tmp/nor180-v151-begin-repro-{macos,linux}-v2.log. The v1 probe was invalid: its
wrapper unwrapped before the injection point. Corrected probe uses the supported
Engine and BeginTransaction/GetWAL contracts and asserts injection occurred.

The broad pre-fix macOS Bolt run had a handshake reset before an assertion; the
same leaf passed 20 isolated repeats. Keep the initial failure as evidence and
re-run the complete affected matrix on the final source.

Reuse review: openTransaction already owns both explicit and implicit admission.
Its siblings in transaction.go use CombineTerminalOutcomes for primary/cleanup
composition. Extending that owner preserves classification and avoids a second
join/render implementation. Tests reuse newWALOutcomeExecutor and native storage;
the additional Engine wrapper supplies a fault only at the missing admission seam.
The automatic worker graph baseline was unavailable; local graph/checker result
must be reported separately and cannot be inferred from this source review.

2026-11-18 scope correction: this pass is reproduction triage only. This existing
NOR-180 fix is the sole authorized PR exception. Keep subsequent reproductions
local until all tickets are reviewed; make no further production fixes or pushes.
Full make verify was canceled, not passed. Localization, UI and native builds had
completed; coverage groups were in progress. Existing focused and race evidence
remains valid because the source correction is unchanged. Duplicate checker ran
against the local extracted graph but its configured service refused connection.
