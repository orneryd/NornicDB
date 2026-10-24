# PR 1674 Mac readiness

Local gate: make test. Broader verification deferred to CI per maintainer instruction.

- [x] Read handoff and all review/conversation channels.
- [x] Rebase onto current release and review integrated correction.
- [ ] Verify focused wire/temporal regressions on both parsers with race detection.
- [ ] Complete pending encoding comparison using identical fixtures.
- [ ] Run make test and fix introduced failures.
- [ ] Push, post current evidence, update linked NOR-133 and mark ready.

Maintainer expanded scope to close the incoming-signature negotiation gap here. New raw-wire regressions fail before the correction while Neo4j controls pass. Negotiated mode now flows through the existing recursive decoder, including nested containers and all structure widths; disallowed DateTime families produce Request.Invalid before execution. Session HELLO, RUN parameters/metadata, BEGIN and streaming-option consumers carry the mode. New tests compare allowed/rejected RUN, BEGIN, PULL and DISCARD cases with Neo4j and distinguish string/byte contents from actual structures. Local regressions run without requiring the optional reference.

Initial make test failed only TestPendingConstraintIndex_LargeTransactionScalesLinearly, in unchanged storage source. Three isolated repetitions pass on the exact release base; no storage assertion was changed. That superseded run is not a pass. Final suite is rerun after the negotiation correction. Receipts: /tmp/nornic-1674-make-test.log, /tmp/nornic-1674-base-storage-timing.log, /tmp/nornic-1674-negotiation-red.log, /tmp/nornic-1674-negotiation-final.log. An initial reference streaming fixture omitted the required n option; fixed the test frame, not the product protocol, before counting its corrected passing result.
