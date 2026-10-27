# PR #1515 managed credential lookup measurements

These measurements cover warm `DatabaseManager.GetStorage` lookups only. They
compare release `d663ce8f1320e409e2eaa29560b987ffe6526207` with the integrated,
uncommitted PR #1515 port before the read-only alias security correction. They
are development evidence, not final-head qualification or PR readiness.

On an Apple M2 Max with Go 1.26.9, eight balanced baseline/candidate pairs used
200 ms per case. The same external Go benchmark was overlaid into both source
trees. Setup creates one or 128 remote aliases with explicit synthetic credentials,
warms the last alias, then times repeated public manager lookups while checking
engine identity. A fixture factory checks the resolved username/password and
returns a preowned engine; no network request occurs in the timed loop. The
folded case uppercases the alias portion of the selected name.

| Aliases | Spelling | Baseline ns/op | Candidate ns/op | Change | Baseline B/op / allocs | Candidate B/op / allocs |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 1 | Registered | 222.00 | 58.87 | -73.5% | 128 / 3 | 0 / 0 |
| 1 | Folded | 225.20 | 80.26 | -64.4% | 128 / 3 | 0 / 0 |
| 128 | Registered | 1006.50 | 59.46 | -94.1% | 128 / 3 | 0 / 0 |
| 128 | Folded | 1345.00 | 87.62 | -93.5% | 128 / 3 | 0 / 0 |

Values are medians across eight runs per implementation. CPU profiles show
repeated alias scanning and credential decode/decrypt work on the release path;
the candidate uses the resolved credential/cache owners. The 128-alias case
selects the last alias and does not establish equivalent gains for every lookup
position. These results say nothing about transport latency, startup, persistence
throughput, or encryption throughput.

Local evidence is retained under
`~/.local/share/nornicdb-pr-readiness/post1503/pr1515/`: the exact harness
`lookup-benchmark_test.go`, overlay maps, profiled binaries, CPU profiles,
`lookup-pairs-receipt.json`, per-run logs and `lookup-pairs-summary.json`.
Binary SHA-256 values are recorded in the receipt. Final source applicability
and a repository-reproducible benchmark remain required before publication.
