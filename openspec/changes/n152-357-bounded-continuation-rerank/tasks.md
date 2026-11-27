# N152-357 Q01 — bounded continuation rerank query

- [x] Reproduce the report's one-shot control and over-limit continuation provider request on the release base.
- [x] Reuse the retrieval chunk for the continuation provider call while preserving original-query memo identity and fused-window behavior.
- [x] Document the long-query ranking limitation in the reranking and continuation guides.
- [x] Run focused and affected package checks on the integrated change; record exact head, base, outcomes, and skips.
- [ ] Open the release/v1.5.3 PR and link focused verification from Plane.
