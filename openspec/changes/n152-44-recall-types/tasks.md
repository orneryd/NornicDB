# N152-44 recall type union and discover depth prose

- [x] Copy the reviewed public-path regression into `pkg/mcp/nor100_recall_depth_repro_test.go` and reproduce the `nodeTypes[0]`-only failure before production changes.
- [x] Filter recall on the union of every supplied type label before the limit, reusing the canonical `containsLabel` predicate and preserving direct-ID, coercion, database-scope and eligibility order.
- [x] Clarify the discover depth description without changing the 1-3 bounds, default or traversal behavior.
- [x] Verify both parsers over JSON-RPC, REST and default-database-pinned REST, including unknown-first, single, empty, unknown-only, node-ID, exclusion and limit-eligibility controls.
- [ ] Complete release verification and duplicate review.
- [x] Parent: review implementation, extend filter-composition controls, and rebase onto current release.
- [x] Parent: pass full MCP package race run and selected canonical localization checks; see evidence.md for exact scope and skips.
- [ ] Publish candidate PR; full qualification and graph analysis remain pending.
