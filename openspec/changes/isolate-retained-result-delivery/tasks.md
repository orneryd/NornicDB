# Retained result ownership: NOR-306 / NOR-307 / NOR-310

Base: release/v1.5.0 bea145d518c9ae2c9df7ced456b245321cad9565.

- [x] Read current tickets and source retention boundaries; preserve separate AC.
- [x] Delegate APOC correction, query-cache correction and statement-view audit.
- [x] Identify existing structural caller-value copy as shared owner.
- [x] Reproduce APOC graph/warmup mutation through supported public APIs.
- [x] Reproduce ordinary/smart query-cache mutation through public execution.
- [x] Establish NOR-310 reachability independently of NOR-307.
- [x] Integrate reuse without compatibility wrappers or duplicate copying code.
- [ ] Preserve result values/types, ordering, explicit writes, nil and lifecycle.
- [ ] Profile and measure representative ownership-boundary workloads.
- [ ] Run affected public, race and integration checks and resolve failures.
- [ ] Update Linear AC evidence; open and push release PR.
- [ ] Complete exact-head make-test CI and disposition duplicate findings.
- [ ] Clean completed task resources when merge-ready.

No storage-format, Badger key, WAL or query syntax changes. No blanket
collection census or speculative mutation API. No duplicate-checker changes.

## Current evidence and remaining contract

- Original release bea145d51 fails the APOC point/warmup and public cached
  Execute mutation regressions. The integrated candidate passes affected
  storage/warmup packages with race detection and the expanded Cypher checks.
- NOR-310: nine cache-disabled public consumer/lifecycle probes do not reproduce
  a distinct statement-local mutation leak; no production view copies added.
  This is bounded route evidence, not an exhaustive claim about arbitrary code.
- [x] Fix absent property-map updates without changing partial-update semantics.
- [x] Reject publication when point-read copying observes caller cancellation.
- [x] Degree and shortest-path walks copy only payloads returned to callers.
- [ ] Decide the typed APOC property removal/update contract and migrate callers.
  Existing property-rename assertions remain red; do not weaken them or restore
  retained object exposure. Proposal is awaiting the operator's API decision.
- [ ] Repeat full APOC integration after the write-contract correction.
- [ ] Request full make-test CI on the final pushed head; keep this PR draft.
