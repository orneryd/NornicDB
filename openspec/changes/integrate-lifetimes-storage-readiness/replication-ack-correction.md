# PR #1693 replication publication/ACK correction

Scope: `pkg/replication` only. Preserve accepted native completion, required
storage wait contexts, existing wire framing and both HA synchronization modes.
Do not extend the original wire test's one-second engine timeout.

## Established defects

- The original `TestHAWireStandbyAcknowledgesOwnPublishedRecord` passed once
  normally and with `-race`, but failed twice in an uncached 100-run normal
  reproduction (6.292s). This is not solely full-suite scheduling pressure.
- Normalized entity acceptance detached to Background and invoked HA Apply with
  timeout zero, replacing the engine deadline with the 250ms HA default.
  Exact-deadline direct, bulk and explicit-receipt regressions failed first.
- Correcting that budget alone still left three original wire-test failures
  in 100 runs (9.113s). The uncorrelated transport admitted multiple requests
  and assigned responses by arbitrary pending-map iteration.
- Deterministic public RPC regressions failed first: a canceled second waiter
  still published a WAL request, and an abandoned in-flight connection remained
  reusable for a later request despite its outstanding uncorrelated reply.

## Required behavior

- Quorum preparation, publication and own-position ACK retain one absolute
  deadline through direct/bulk normalization and explicit accepted receipts.
  Queue-context disposal or caller cancellation cannot discard native work.
- Async retains its nonwaiting WALQueued/native-result contract.
- With no wire request IDs, one complete RPC owns a connection at a time.
  Queue cancellation publishes nothing. In-flight cancellation or a partial
  write failure retires that connection; existing HA reconnection owns recovery.
  Only the expected response type can satisfy the pending exchange.

## Tasks and evidence

- [x] Preserve original failure and reproduce normally / under race detection.
- [x] Establish both causal owners and fail-first public regressions.
- [x] Correct absolute-budget ownership and deterministic wire exchange ownership.
- [x] Complete focused normal/race regressions, error/cancellation controls and
      final full `pkg/replication` normal run on the integrated source.
- [x] Record final source applicability and limitations in the caller handoff.

Final uncached evidence:

- Expanded publication/ACK, typed RPC, cancellation and error controls:
  normal `-count=5` passed (23.967s).
- Affected HA, ClusterTransport/ClusterRPC, adapter publication and accepted
  completion tests: `-race -count=1` passed (20.961s).
- Complete package: `go test ./pkg/replication -count=1 -timeout=300s`
  passed (107.920s).
- Enhanced original wire regression, retaining its original timeout and
  assertions: normal `-count=100` passed (5.780s), and `-race -count=100`
  passed (8.429s).
- Scoped `git diff --check` passed. The toolchain emitted the existing Darwin
  linker warning about duplicate `-lobjc`; it did not block these checks.

The tested source is dirty working-tree HEAD
`6b546791283db0e8b2f1951df1ac530a1829a26f`, not a clean commit. The aggregate
SHA256 of sorted `pkg/replication/*.go` path bytes followed by each file's bytes
(106 files) is
`6e9c8648c99ee6777899048c9783d12f5650bee8f2cd2186c50761a7251317a8`.
No throughput improvement, full-package race pass, full-repository pass or
readiness of the other owners' concurrent changes is claimed.

No full-repository verification, census/proof edits, commit, push or release
qualification belongs to this scoped correction. Graphify was unavailable from
tool discovery; direct source queries established the two accepted-helper
consumers and the single typed-RPC exchange owner.
