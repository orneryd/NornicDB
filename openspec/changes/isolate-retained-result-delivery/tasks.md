# Retained result ownership: NOR-306 / NOR-307 / NOR-310

Base: release/v1.5.0 bea145d518c9ae2c9df7ced456b245321cad9565.

- [x] Read current tickets and source retention boundaries; preserve separate AC.
- [x] Delegate APOC correction, query-cache correction and statement-view audit.
- [x] Identify existing structural caller-value copy as shared owner.
- [x] Reproduce APOC graph/warmup mutation through supported public APIs.
- [x] Reproduce ordinary/smart query-cache mutation through public execution.
- [x] Establish NOR-310 reachability independently of NOR-307.
- [x] Integrate reuse without compatibility wrappers or duplicate copying code.
- [x] Preserve result values/types, ordering, explicit writes, nil and lifecycle.
- [x] Profile and measure representative ownership-boundary workloads.
- [ ] Run affected public, race and integration checks and resolve failures.
- [x] Update Linear AC evidence; open and push release PR.
- [ ] Complete exact-head make-test CI and disposition duplicate findings.
- [ ] Clean completed task resources when merge-ready.

No storage-format, Badger key, WAL or query syntax changes. No blanket
collection census or speculative mutation API. No duplicate-checker changes.

## Current evidence and remaining contract

- Original release bea145d51 fails the APOC point/warmup and public cached
  Execute mutation regressions. The integrated candidate passes affected
  storage/warmup packages with race detection and the expanded Cypher checks.
- NOR-310: the initial scalar probes did not reproduce a separate leak. Extended
  legal property-list probes do reproduce both node/relationship continuation
  mutation with result caching disabled. Projection now uses its owned scoring
  snapshot; no-memo streams detach at delivery, retaining map metadata and
  cancellation. Both decay modes and the over-budget view path are covered.
- [x] Fix absent property-map updates without changing partial-update semantics.
- [x] Reject publication when point-read copying observes caller cancellation.
- [x] Degree and shortest-path walks copy only payloads returned to callers.
- [x] Operator approved typed PropertyUpdate; migrated interfaces, both backends,
  callers and mocks. One ApplyTo owner applies removals before owned sets.
  Existing property rename assertions pass without exposing retained reads.
- [x] Repeat full APOC integration after the write-contract correction.
  Full APOC packages pass locally; native memory/Badger update integration passes
  with race detection. Full census and exact-head CI remain separate gates.
- [ ] Request full make-test CI on the final pushed head; keep this PR draft.

- [x] Reconcile all five candidates and four current-owner entries from the CI
  duplicate advisory. Remove trace-cache forwarding methods; combine ordinary
  and ownership relationship-type tests without dropping missing-type controls.
  PR comment: https://github.com/orneryd/NornicDB/pull/1778#issuecomment-6753995829

- [x] Reproduce copy callbacks running under the warmup mutex; copy immutable
  published snapshots after releasing the mutex. Full warmup race suite passes.
- [x] Review regenerated census artifacts: only caller-copy progress source
  position changed, from property_copy.go:92 to :123. No rules or classifications
  changed. Fresh current-artifact and ordered-write census checks pass.

- [x] Correct the warmup Path/Subgraph/Progress callback lock scopes exposed by
  final review. Keep incremental subgraph progress and short publication locks.
- [x] Correct the CI fixture-lifetime constructor registration, retaining the
  unchanged blocking ownership guard and native adapter assertions.
- [x] Correct live streamed row ownership, reuse one map-order transfer owner,
  and reject cancellation during row copying. Existing stream cancellation
  fixture now buffers under a live context before canceling; assertions remain.
- [x] Record final publication benchmark/profile results: default memo allocation
  counts unchanged; unsafe plain-stream aliasing replaced with per-row owned
  images, with cost documented. Reuse single-column output buffers saves
  8,800 B and 400 allocations per 100-row operation versus first correct copy.
- [ ] Complete new-head make-test CI after integrated continuation fixes.
