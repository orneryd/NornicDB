# NOR-394 implementation

- [x] Preserve original duplicate-ticket history; reuse NOR-394 for v1.5.0 and assign to maintainer.
- [x] Branch from current main without disturbing active PR worktrees.
- [x] Implement canonical new temporal constraint DDL and deprecated spelling compatibility.
- [x] Implement one historical read scope with current-incarnation, retention and endpoint eligibility.
- [x] Integrate parameterized read-only asOf execution and canonical effect/result metadata.
- [ ] Verify public paths, both parsers, cancellation and unchanged storage formats.
- [x] Update API/deprecation examples and migration guidance.
- [ ] Run focused/package checks and workload-scoped allocation profiles.
- [ ] Review integrated duplicate/reuse candidates; record unavailable checks honestly.
- [ ] Push candidate, open v1.5.0 PR and trigger CI with exact-head evidence.

- [x] Compile block-local temporal.epoch/GROUP BY entries for nodes and relationships; verify grouping isolation, mixed predicates and complete-declaration framing.

- [x] Implement symmetric epoch bounds across shared validators, index readers and deprecated interval consumers without changing persisted layouts.
- [x] Verify direct/indexed/scan/native, restart/write-behind and concurrent open-bound claims; preserve finite-key byte encodings.
- [x] Execute documented ownership, fact-ledger, constraint-block, historical-read and downloadable-script examples through both parsers and HTTP/Bolt.
- [ ] Verify the dedicated docs-site section, generated navigation/search and rendered mobile/desktop guides.
