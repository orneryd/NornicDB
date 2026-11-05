# PR 1672 Mac readiness

Scope: legacy Bolt DateTime instant preservation, integrated with current release/v1.4.4. Local merge gate is make test; review the current implementation and complete the pending DateTime encoding comparison.

- [x] Read PR handoff and all review/conversation channels; no unresolved threads.
- [x] Rebase onto release e5b35a220, retaining the release's native dependency versions and both changelog entries.
- [x] Independently review the legacy/UTC codec contract and public consumers.
- [x] Run focused regression/race checks on both parsers.
- [x] Compare DateTime encoding benchmarks and existing row/record controls on identical base/head fixtures.
- [x] Complete make test on the integrated source; correct introduced failures.
- [x] Prepare the verified branch and current PR description/result for publication; publish and mark ready as the final remote steps.

Evidence: exact release test-only overlay reproduces legacy wire and driver failures; UTC controls pass. Independent review checked canonical encoder/decoder consumers and the Bolt legacy contract. Both parsers pass focused tests with race detection against Neo4j 5.26.30. Initial integrated make test passed (104 packages, no cache hits); rerun required after the profiled encoding change.

Performance: Apple M2 Max, Go 1.26.9, GOMAXPROCS=1, identical fixtures, eight 300ms samples and CPU profiles. Skipping unused UTC named-zone offset lookup reduces named UTC native encoding from 17.48 to 14.85 ns/op and typed from 12.495 to 10.040. Relative to the incorrect release encoder, legacy named-zone cases cost 2.96–4.18 ns/op more (31–32%); UTC named-zone cases cost 0.55–1.75 ns/op more (6–13%). Offset cases stay within 3.4%; row/record controls stable or faster. All cases remain 0 B/op and 0 allocs/op. This is a correctness fix, not a speed improvement; no blanket non-regression performance claim. Full verification/allocation/compatibility gates deferred per user instruction; make test is the local gate.

Final make test passed on d0632e25db91efbd40ba4e5bcf6e18128805c80e: 104 passing packages, 95 cached. Log: /tmp/nornic-1672-final-make-test.log. Subsequent changes only update this task record; production/test/build inputs remain identical.
