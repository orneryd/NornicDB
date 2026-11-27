# N152-326: multi-label MERGE index selection

- [x] Reproduce the production label-order gap on the release head with the archived 150,000-node fixture.
- [x] Add a fail-first public MERGE regression for both parser modes and both execution paths.
- [x] Search each pattern label for unique-constraint candidates while preserving ordinary-index fallback and candidate validation.
- [x] Verify indexed hits and preserve incomplete ordinary-index, unique-cache, and scan fallbacks with the existing public regression matrix.
- [x] Compare the recorded workload before and after, with a CPU profile during the benchmark.
- [x] Run the affected Cypher package and review the two MERGE lookup owners against their distinct fallback contracts.
- [x] Prepare the release PR description and exact verification evidence for publication.
