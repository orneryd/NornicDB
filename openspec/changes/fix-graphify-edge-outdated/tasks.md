# Graphify repeated relationship conflict

- [x] Inspect NOR-288 and current #1691; preserve reference head 182893c0 and release 8f5835d7.
- [x] Preserve failing ingest logs 42020171895 and 42108551854; second fails at SET r += after 3,060,000 processed edges.
- [x] Compare current statement-time classifier against #1691: immediate failure path differs from post-acceptance settlement; indirect history/concurrency contribution remains unproven. No stack cherry-pick.
- [ ] Reproduce through public query/ingest path on disposable data; retain healthy and genuine-conflict controls.
- [ ] Correct canonical owning cause without weaker conflict semantics.
- [ ] Run focused affected-package checks; report exact evidence and limitations.
- [x] Open separate draft #1737 against release/v1.4.4, distinct from #1736; preserve open AC/evidence and coordinate NOR-288/#1691 without claiming the same root.
- [ ] Propose safe reingest after verified fix and confirm completed commit marker only after actual completion.

Dogfood database and volume must not be deleted/reset or mutated for experiments. Local Docker desktop-linux currently lists no running containers; awaiting host/copy location. Remote graph retrieval is incomplete (configured HTTP endpoint answers HTTPS-port error). Root cause and stored-data corruption are not established.

- [x] Preserve clean controls (native repeated batches, repeated pairs, captured local release batch native/WAL, and 3.1 million sequential updates); all pass and are not claimed as a failing regression. Diagnostic source/logs remain outside the PR until a defect is reproduced.
- [x] Independently validate six supported-history controls (24-iteration matrix, 0.78s), with identical probe-source digest before/after; no production change justified by passing probes.
