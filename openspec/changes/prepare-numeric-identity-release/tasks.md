# PR #1665 / NOR-68 readiness

- [x] Select by current Linear v1.4.4 project and Urgent priority; assign NOR-68 to Tim Sweet.
- [x] Rebase original numeric layer onto release df07c94fb, preserving current context/transaction owners.
- [x] Reproduce exact JSON-load rounding through APOC library and public Cypher on current release; retain RED evidence.
- [x] Reproduce CDC equal-number selector omission and native max-uint64 event rounding on current release; preserve existing INTEGER/FLOAT change detection.
- [x] Port numeric domain fixes; reuse canonical Cypher numeric equality for CDC selectors.
- [x] Complete canonical caller migration, reviewed lexical/security/value inventories and stored-entity regeneration after refactor.
- [x] Focused AC verification: backups, nested values, legacy limits and CDC; both parsers with race checks and actual Neo4j-reference comparisons. Requalify the final integrated gate below.
- [x] Preserve original full-gate failures; fix fixture ownership, historical WAL integer lexemes, schema typed metadata bytes and reviewed census records without weakening assertions.
- [x] Correct the JSON dependency's float32 extrema and double rounding in its existing owner; full dependency/race tests pass, pinned fork PR #2.
- [x] Reduce whole-FLOAT text and indented-export allocations with one direct formatted writer and bounded geometric growth; matched release measurements also reduce retained heap. Preserve caller-owned buffers. Required Neo4j/correctness output bytes are accepted; avoidable overhead remains subject to review.
- [x] Best-effort installed duplicate review hit its deadline; analysis is incomplete and informative. No checker/hook/rule changes.
- [x] Full make test passed on fdbf6568c; both current-head parser/reference runs and all seven focused race packages pass. Broader make verify remains CI-owned.
- [x] Prepare the qualified rebased head, final evidence and PR/Linear publication descriptions; maintainer owns merge. Final publication is recorded on GitHub and Linear.
- [x] Retire nine completed task/dependency worker worktrees and preserve source/review evidence; remove temporary reference container and probe binaries. Shared Go cache is explicitly excluded while another agent tests.
