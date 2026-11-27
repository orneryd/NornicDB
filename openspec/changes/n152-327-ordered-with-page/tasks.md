# N152-327: ordered WITH page on two labels

- [x] Reproduce the archived 108,000-read WITH page on the release head and preserve its rows and terminal control.
- [x] Add a fail-first public PROFILE assertion for an ordered page-sized read.
- [x] Pass only a direct single-node WITH order to the existing ordered index window; keep unordered early limiting and the streaming WITH executor separate.
- [x] Check aliases, renamed variables, SKIP, multiple projections, descending null guards, seeded rows, pending transaction writes, missing sort keys, and tied keys in both parser modes.
- [x] Compare matched before/after benchmark and capture CPU profiles; keep the fixture setup included in their interpretation.
- [x] Run the affected Cypher package and review duplicate owners.
- [x] Prepare the release PR description and exact verification evidence.
