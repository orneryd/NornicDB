# N152-55 — documented Neo4j migration

- [x] Reproduce generated-node/edge and documented-promotion failure before edits.
- [x] Consolidate destination Cypher in a shared template owner used by Go/Python/Node.
- [x] Use a visible bridge, retain stable identity across completed-workflow reruns, merge relationships and properties together.
- [x] Verify applied target rows before reporting batch progress; distinguish rows from unique graph entities and dry-run reads.
- [x] Document all-label promotion before bookkeeping cleanup and retain bridge/IDs for reruns.
- [x] Verify both parsers, source labels/properties/endpoints, multi-label promotion, rerun updates, missing endpoints, internal visibility and client result handling.
- [x] Run scoped canonical verification and existing staged duplicate checker; record explicit unexecuted checks and scripts exclusion.
- [ ] Complete full release qualification and graph-wide analysis before claiming merge readiness.
