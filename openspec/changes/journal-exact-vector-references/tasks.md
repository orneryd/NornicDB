# Exact managed-vector WAL references

- [x] Reproduce full-vector journal amplification through native and public Cypher updates.
- [x] Preserve original journal-only exact-vector recovery controls.
- [x] Add complete source-validated managed anchors using existing WAL framing/recovery owners.
- [x] Support approved multi-frame anchors in standard mode without lifting ordinary record limits.
- [x] Encode WAL-local references with a distinct semantic version; portable replication remains full-image.
- [x] Migrate accepted encoding/staging requests to explicit opaque vector-reference metadata.
- [x] Require an exact predecessor fingerprint; observe receipts before resolving duplicate replay.
- [x] Reuse a bounded fixed-size proof cache, invalidated by canonical embedding-audit append and journal retirement.
- [x] Never anchor an uncommitted staged predecessor; preserve complete replacement vectors.
- [x] Focused public Cypher, journal-only recovery, malformed-reference and above-frame controls pass.
- [x] Rebased onto a492f2acd; focused storage/nornicdb/replication checks pass
  (18.365s / 2.163s / 2.294s), with no-tests-selected results kept separate.
- [x] Staged replacement, snapshot retirement and worker replacement recovery
  controls pass; reference admission preserves complete replay-image validation.
- [x] Focused storage/nornicdb race checks pass (5.265s / 2.640s); vet passes.
- [x] Final admission-focused checks pass4.537s; large-frame checks2.480s;
  final focused race passes5.697s/2.814s. Native full-image admission remains before
  anchor append. Byte measurements are documented separately from runtime cost.
- [ ] Qualify cold first-anchor versus steady-state bytes and runtime allocations/cost.
- [ ] Publish a separate release fix PR and trigger exact-head CI/duplicate review.
