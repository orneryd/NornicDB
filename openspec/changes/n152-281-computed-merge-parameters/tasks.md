# N152-281: computed MERGE parameter reuse

- [x] Reproduce the statement-parameter row conversion in the shared pattern binder before editing.
- [x] Capture baseline CPU and allocation profiles on nested 100, 1,000, and 10,000-row parameters, plus a public MERGE/CASE control.
- [x] Reuse the statement's converted parameter rows for computed pattern bindings while retaining row-local synthetic values and the Cypher 25 mark.
- [x] Check ON CREATE and ON MATCH persisted values in Nornic and ANTLR parser modes, and compare matched post-change benchmarks.
- [x] Run the full affected Cypher package and review the final diff for correctness, performance, and duplicate owners.
- [x] Open a release/v1.5.3 PR with exact head/base verification and update Plane.
