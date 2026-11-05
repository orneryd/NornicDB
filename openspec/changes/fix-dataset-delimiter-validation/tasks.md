# NOR-203: offline dataset delimiter validation

Investigation base: release/v1.4.4 at
`8f5835d7f2fbb9c1253a046ef4b9b64c2da06af1`.
The maintainer approved v1.4.4 and all three delimiter families on 2026-11-05.
Linear is updated and the issue is assigned to Tim; NOR-406 remains separate.

- [x] Apply cleanup-pr-workspace; retire completed task worktrees and build artifacts while preserving active/unfinished source.
- [x] Compare NOR-203 and NOR-406 issues and comments; keep disjoint validator and runtime retrieval owners in separate PRs.
- [x] Reproduce the actual CLI false rejection of a quoted brace and comment brace on the current release, with a genuinely unbalanced negative control.
- [x] Reproduce the neighboring parentheses/brackets false positives without changing their behavior.
- [x] Inspect deployment and existing reader boundaries; no reusable standalone Python Cypher reader identified in the inspected scripting paths. Go query execution and this offline Python tool are separate products.
- [x] Confirm release destination: maintainer explicitly selected v1.4.4.
- [x] Confirm all three existing delimiter checks share one lexical reader.
- [x] Add deterministic scanner and public CLI tests, including escapes/comments, genuine imbalance and line attribution. Same public CLI test fails on original source with false rejection, then passes on correction.
- [x] Correct the existing validation owner without introducing a full parser, network dependency, compatibility wrappers or changing unrelated CLI checks.
- [x] Independently verify focused Python tests; this is a correctness correction, not a latency optimization claim. No local full Go suite is a push prerequisite.
- [ ] Update Linear and PR using their description skills; push and request make-test/duplicate CI once release scope is decided.

The comparison review was delegated read-only to DeepSeek job
`dfba67a3eab34e478140b44ba1033550`. Its code-graph retrieval is unavailable
(HTTP 400); acceptance rests on current-source and public CLI evidence.
No NOR-406 database performance correction is inferred from older, loaded
end-to-end measurements. Its recorded engine-only timings include stub providers
and do not reproduce the reported 16-way screening latency.

Focused Python verification: 16 tests pass, including the 30-member delimiter/
lexical-context matrix and real CLI controls. Baseline counterfactual invokes
the original script with the same CLI tests and reproduces the false rejection.

DeepSeek implementation job `8ffb9ddd75f3416198a868303828a396` completed in
259.4 seconds. Parent reviewed the actual reader, current Go/browser extent
rules, negative controls and all public CLI cases; extended CR/LF controls and
the backtick/backslash distinction. The reader counts delimiters in one pass
without copying the query. Unterminated lexical extents consume to EOF like
the raw shared readers; this aid adds no full-syntax diagnostic.

The new CI workflow includes the previously reviewed exact make-test dispatch
mode plus the focused Python test step. Its concurrency key includes the
requested candidate, preventing this PR's CI from canceling #1688's run.
