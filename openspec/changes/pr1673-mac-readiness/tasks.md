# PR 1673 Mac readiness

Local gate: make test. Broader verification deferred to CI per maintainer instruction.

- [x] Read handoff, review bodies, inline and conversation comments; no outstanding feedback.
- [x] Rebase onto current release/v1.4.4; preserve all changelog entries.
- [x] Review integrated MERGE admission and public transaction consumers.
- [x] Run focused regression tests with race detection on both parsers.
- [x] Run make test; fix introduced failures and distinguish existing release failures.
- [x] Prepare the verified branch and evidence for final publication and ready-for-review transition.

Mac findings: the initial focused run exposed a new test's incorrect snapshot premise under write-behind (label reads use the established read-committed path). The test now separately asserts read-committed catch-up and conflict after a real pending write. Stress also found an async explicit MERGE commit conflict: an ACK can precede transaction begin while its physical flush follows begin and finishes before admission. TestPrepareMergeNodeForWriteAfterBufferDrained deterministically failed at commit before the correction. Admission now refreshes an untouched write-behind snapshot after acquiring the key even when the buffer has already drained. Existing observed-write conflict behavior is retained. No retries or relaxed assertions were added.

Fail-first replay of the original public embedded, HTTP and Bolt tests on exact release base fa72fc3bdafb6b0c37a88027c13c92fb5169fdfb uses only tests and the wait hook. All reproduce the original UNIQUE commit error. Receipts: /tmp/nornic-1673-red.log; new async regression: /tmp/nornic-1673-drained-red.log.

The initial full suite also failed the mutator inventory because PrepareMergeNodeForWrite was unclassified; focused replay confirms this exact missing member. Added its reasoned publication classification without relaxing the inventory assertion. The superseded rerun was cancelled after the inventory correction; it is incomplete, not a pass. Logs retained in /tmp/nornic-1673-final-make-test.log; final gate uses /tmp/nornic-1673-verified-make-test.log.

Final gate PASS on 52753fa91cc6f1beee5fd23c7dac8f62d7b874dc: make test, 104 passing packages, 93 cached. Both focused parser runs pass with race detection and Neo4j 5.26.30 reference; corrected storage/embedded stress passes five race-detected repetitions. Final documentation commit changes only this record; production, tests, dependency and build inputs remain identical.
