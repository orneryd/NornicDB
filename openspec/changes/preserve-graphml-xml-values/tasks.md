# GraphML value preservation

- [x] Reproduce invalid text/attribute XML on the release baseline.
- [x] Reproduce unsupported public file signature and ignored output/destination errors.
- [x] Obtain approval for GraphML's `(string, error)` Go API migration and XML-invalid-character rejection.
- [x] Use standard XML escaping for all property keys/values and relationship types.
- [x] Reject malformed UTF-8 and XML 1.0 forbidden characters before returning/writing a document.
- [x] Migrate canonical callers and registry signature metadata, without a compatibility wrapper.
- [x] Verify actual registry paths, empty graph, exact values and XML whitespace round trips.
- [x] Verify rejected input leaves a pre-existing file unchanged and creates no new output file.
- [x] Fix demonstrated sibling Cypher/raw export output stubs through the shared file owner.
- [x] Integrated focused race checks: export, registry and root APOC pass.
- [x] Profile four representative exports in five isolated samples and document latency/bytes/allocation tradeoffs.
- [x] Independent DeepSeek review: escaping/rejection correct; raw public dispatch gap fixed and tested; XML helper comparison documented.
- [x] Approved context-first file helpers reuse the existing APOC export capability.
- [x] Retire redundant Go aliases and register bundled names directly on canonical writers.
- [x] Shared rooted destination owner preserves native directory and file-result contracts.
- [x] Public scalar and native procedure permission controls pass on both parsers.
- [x] Reproduce and correct CSV header/value misalignment across Go and bundled aliases; reuse the header-backed column view without a second key buffer.
- [ ] Complete current-source duplicate dispositions and exact-head CI make test.
