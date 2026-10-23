# PR 1571 release acceptance

Scope: correct the seven native GDS stream ID declarations to STRING on release/v1.4.4; preserve runtime values and utility input behavior. Composite identity parent changes and failures reproduced on the release base are excluded by the maintainer's request.

- [x] Read Linear NOR-29, PR conversation, reviews and inline comments.
- [x] Isolate the two-file metadata correction from the composite identity stack on release eb16d6003afc6b39adefba8ae06f2531eabbeaff.
- [x] Reproduce incorrect metadata on the exact release base with the public regression.
- [ ] Validate all seven streams and utility consumers on default and ANTLR parsers with race detection.
- [ ] Complete make verify-list and make verify against origin/release/v1.4.4; classify failures using exact-base reruns.
- [ ] Review the final diff and record evidence and remaining limitations.
- [ ] Prepare the PR update and verification result for review.

Evidence: /tmp/nornic-1571-red.log reproduces INTEGER vs STRING on exact release base. /tmp/nornic-1571-base-utility.log reproduces unsupported node-object utility inputs on that same base; the imported parent-only assertion was replaced with the released id(n) string-ID contract. The first full verifier was canceled during build after this test correction; its logs are preserved at /tmp/nornic-1571-verify.log and are incomplete.
