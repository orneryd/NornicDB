## Bolt FAILURE poisons connection until RESET (#769, PR #768)

Context: concurrent auto-commit `MERGE … ON CREATE SET` on shared keys caused
clients to hang forever or receive malformed Bolt messages
(`Expected structure, found marker 00`; `BufferError: Existing exports of
data: object cannot be re-sized`). Root cause: `sendRunFailureWithDetail` only
set `failedUntilReset` for explicit transactions, so after an auto-commit RUN
FAILURE the pipelined PULL was answered with an empty SUCCESS instead of the
Bolt-spec IGNORED, desynchronizing drivers; poisoned pooled connections then
corrupted later statements. The existing `dispatchInner` IGNORED gate was
reused — no new code path. The fix is folded into the correctness-sweep PR
#768 as an additional issue.

- [x] Reproduce #769 verbatim over the production server (8 threads × 100
      shared-key MERGE loops): `finished=False` with 2–6/8 threads stuck per
      run and driver-side `BufferError`/`marker 00` captures.
- [x] Trace the wire: every failed auto-commit RUN flushed `FAILURE
      Neo.TransientError.Transaction.Outdated` followed by an empty `SUCCESS`
      answering the pipelined PULL (server log instrumentation, removed after
      root cause was established).
- [x] Fix `sendRunFailureWithDetail` to set `failedUntilReset = true`
      unconditionally; queued messages get IGNORED via the existing
      `dispatchInner` gate until RESET. Pinned by
      `TestGh769_AutocommitRunFailurePoisonsUntilReset` (FAILURE → IGNORED for
      PULL and subsequent RUN → RESET restores service) and
      `TestGh769_SuccessfulAutocommitStillFlowsWithoutIgnored` (RECORD+SUCCESS
      on the success path).
- [x] Harden `TestBoltServerStress`: it used a bare `NewMemoryEngine()` (only
      integration test without the namespaced production chain), so every
      CREATE failed and the test passed only because the old empty-SUCCESS
      divergence made the read loop see SUCCESS. Now uses the
      namespaced chain and asserts exactly RECORD+SUCCESS per connection,
      reporting FAILURE/IGNORED instead of hanging.
- [x] Verify live: 6 fresh-label runs of the issue repro finish
      (`finished=True`, `stuck={}`; retries 3–13 from driver-side transient
      conflict handling), zero captured exceptions, disjoint-keys control
      `retries=0`; persisted graph is complete and correct (10 keys, `n.by`
      set on all).
- [x] Performance: no hot-path impact. M2 Max routing benchmark
      (`-benchtime=300ms -cpu=1 -count=3`): autocommit 16.5–16.7 µs/op,
      161 allocs (pre-fix 17.7–17.8 µs, 161 allocs); explicit tx
      10.7–10.9 µs/op, 88 allocs (pre-fix 11.5–11.6 µs, 88 allocs).
- [x] Suites: `pkg/bolt` 43.1s and `go test -race ./pkg/bolt` 135.5s green,
      `pkg/cypher` 41.7s, `pkg/storage` 47.8s, `pkg/server` 114.5s all green;
      `make cypher-tck-ratchet` passes 7794/7794 (100%) in autocommit and
      explicit-transaction modes with zero gaps/setup blocks/harness errors.
- [x] Tracked in OpenSpec (`openspec/changes/bolt-failure-poisons-until-reset/`)
      and folded into correctness-sweep PR #768.
