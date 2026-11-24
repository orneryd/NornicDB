# v1.5.2 security triage follow-up

Scope: reconcile the eleven findings in the local v1.5.2 security triage note, correct established defects, and submit one verified PR against `release/v1.5.2-hotfix`. The release base is current `main` at `55ced12a0`. Verify X06 first and keep the advisory draft local for maintainer review.

## Findings

- [x] X02 / N152-269 — maintainer confirmed the no-auth image default and warning are intentional for testing. Explain the default at the entrypoint and document the explicit authenticated setup.
- [x] X04 / N152-270 — verified the documented environment on the published CPU image. The HTTPS certificate validated, anonymous query returned 401, and the authenticated HTTPS write and Bolt TLS read succeeded.
- [ ] X05 / N152-271 — point and population reads score the complete snapshot image; the focused projected point and label scan checks pass. Verify remaining consumers and package coverage.
- [x] X06 / N152-272 — related-node traversal uses the authorized graph. The scoped public Discover test and full MCP package suite pass. The advisory is ready for maintainer review.
- [ ] X07 / N152-273 — uniqueness diagnostics omit holder identities for scoped requests; verify storage, HTTP and Bolt behavior
- [x] X09 / N152-287 — rank readable fields before applying candidate limits in restricted search. Focused node, relationship, and paging checks and the Linux search package suite pass.
- [x] X03 / N152-290 — replace or remove the unsupported settings and document the supported credential input
- [x] X08 / N152-289 — rank restricted keyword searches from the authorized corpus. Tests match the readable-only reference. At 10,000 in-memory documents, the Linux medians were 20.83 to 20.72 µs without FGAC and 152.00 to 142.31 µs with full graph and property grants. The restricted hotfix median was 53.05 ms. Allocation counts were unchanged for both unrestricted cases.
- [x] X12 / N152-288 — MCP recall accepts local and qualified node IDs. Focused public tool test passes.
- [x] X10 / N152-371 — require source-property read access as well as the managed embedding grant. The focused node and relationship checks and the Linux search and GraphQL package suites pass. Existing embeddings lack a complete source-field record, so require access to all properties of the source entity. Route both GraphQL similarity fields through the same authorized graph read.
- [x] X11 / N152-372 — repeated FGAC writes preserve the statement timestamp. The regression fails on the base and passes with the patch for nodes and relationships.

## Integrated gates

- [ ] Record current-head reproduction or already-correct evidence for each finding.
- [ ] Add fail-first public-path regressions for established defects and fix the complete causal units.
- [ ] Run the staged duplicate checker and assess every candidate against current source.
- [ ] Run focused, race, and applicable repository verification on the combined head; report skips and benchmark effects where relevant.
- [ ] Draft one outcome-focused PR with an explicit per-finding AC and verification map.

## Scoped keyword acceptance

X08 and X09 share a scoring boundary. Apply traversal and readable-field selection before term membership, document frequency, length normalization, relevance scores, or relevance explanations can be seen by a restricted request. Keep the shared database indexes for candidate ownership. Preserve visible matches and page boundaries whether unreadable fields are absent or present. Exercise node and relationship searches, retrieval continuation, and the unrestricted control. Measure CPU and allocations for restricted searches against the same indexed corpus.
