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
- [ ] Rebase onto the latest release and independently repeat affected checks.
- [ ] Complete staged/multiple-update, snapshot-cut and worker-reset interleaving controls.
- [ ] Run affected race checks and vet.
- [ ] Qualify cold first-anchor versus steady-state bytes and runtime allocations/cost.
- [ ] Publish a separate release fix PR and trigger exact-head CI/duplicate review.
