# Release integration verification follow-up

- [x] Preserve typed Fabric record imports and retire superseded private rewrite paths.
- [x] Reproduce the HTTP connection-metadata mismatch with an idle accepted peer; preserve all populated-field and non-empty assertions for established test-driver connections.
- [x] Verify the connection case on HTTP and Bolt in both transaction modes, including a missing-agent negative control.
- [x] Give cold-cache tests independent identities; preserve cold/warm and cancellation assertions across repeated runs.
- [x] Review the dispatch return and update only its retired/current source-inventory receipts without changing analysis rules.
- [x] Assert expected fault-injected WAL close errors through the existing strict fixture owner.
- [x] Restore canonical ambiguous-aggregation diagnostics for RETURN and WITH through both parsers.
- [x] Reproduce and correct zero-argument normalize panics through public Execute and Bolt; retain canonical arity diagnosis.
- [x] Correct remaining fault-injection cleanup fixtures without suppressing normal close failures.
- [x] Resolve write-behind stress derived-count underflow at its owning cause; fixed seed passes ten repetitions and reserved-deletion controls pass with race detection.
- [x] Preserve canonical CALL-conclusion diagnostics through Fabric; integrated focused checks pass with both parsers.
- [x] Preserve acknowledged buffered re-creations, own-frame node/relationship create-delete operations, and unchanged stale-index behavior with public transaction controls.
- [x] Reuse the canonical read-and-close owner for current stored-presence projection; avoid entity and embedding hydration.
- [x] Review source ownership, rotation immutability and lifecycle lock ordering; record independent-review findings and corrections in Linear.
- [x] Regenerate the stored-entity census after the source-line changes; retain its rules and ownership gates.
- [x] Record dispositions for all three printed entries in the 2ea32da66 advisory report; stale graph baseline documented.

## Completion receipts

The frozen-source head is qualified by its PR and Linear verification receipts,
rather than a subsequent source commit that would change the checked SHA:

- Full exact-head make test CI, final Linux differential comparison, and local
  interrupted-Cypher-coverage replay must have actual outcomes recorded.
- TCK ratchet/vetted corpus and affected public paths must retain their recorded
  results; superseded or interrupted broad runs are not passes.
- Every final advisory entry needs an evidence-backed disposition; incomplete
  analysis and stale indexing must remain explicit.
- Retire only task-owned worktrees, Docker resources and temporary artifacts;
  preserve other active work and defer shared Go-cache cleanup while needed.
