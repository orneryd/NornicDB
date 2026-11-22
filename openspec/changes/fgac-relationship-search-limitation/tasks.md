# FGAC relationship search limitation

- [x] Preserve semantic/hybrid fail-first fixtures and hidden-property controls externally.
- [x] Trace HTTP relationships and Cypher db.retrieveRelationships to the projected relationship search owner.
- [x] Document keyword-only FGAC relationship ranking and distinguish node vector search.
- [x] Reject hash-only vector reuse and per-request corpus embedding within this scoped release correction.
- [x] Run docs verifier and record actual result/skips: make verify BASE=origin/release/v1.5.2 ONLY=docs passed; other verifier steps explicitly skipped.
- [x] Parent reviews documentation and outcome-focused draft body before publication.

Runtime correction remains unresolved: exact embedding-input/provider provenance is not retained with published relationship vectors. A complete safe fix needs an explicit provenance/restart/memory decision. This documentation change does not repair semantic ranking or reduce enforcement.

Evidence: ~/.codex/artifacts/n152-fgac-relationship-20261122. Frozen source f2e264f1f02f615ccbf9be498ed94993448dcb9d; original package RED0.584s, hybrid RED0.638s. No known-failing Go test is included. DeepSeek source-only review8e868abfed18428fb2a130c0a1d158c0 completed140.471s; graph retrieval incomplete (DNS), no worker tests. Parent independently verified source/provider owners.
