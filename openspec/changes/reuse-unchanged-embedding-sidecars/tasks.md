# Reuse unchanged physical embedding sidecars

- [x] Reproduce chunk rewrites and metadata removal for an unrelated property update.
- [x] Use the existing source-policy and fresh-sidecar owners to prove reuse.
- [x] Preserve float bits, metadata changes, replacement vectors and source invalidation.
- [x] Public storage controls pass on release7f565720f (2.012s).
- [x] Expanded focused race controls pass5.648s.
- [x] Exact current-release fail-first1.174s proves chunk rewrite and removed metadata.
- [x] Matched current-base CPU/allocation profiles and four isolated samples per arm: ns/op20,685,989→13,642,069; B/op16,073,233→12,032,233; allocs44,454→20,853. Prior overlapping trial excluded.
- [ ] Push candidate and run regular make-test CI.

This correction removes physical sidecar rewrites only. The separate B30/B37
full-vector WAL record/standard-frame refusal remains open in its reproduction
worktree; no reduction in WAL record bytes is claimed.
