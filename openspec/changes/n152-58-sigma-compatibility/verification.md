# N152-58 verification

Base `fec41082d2ed01459ff6c9d9f85e34aec528ec39` from `release/v1.5.2`. The native Go checks use `GOMAXPROCS=2 GOMEMLIMIT=1500MiB`, `-p=1` and `-tags=noui,nolocalllm`.

The original 53-row owner fixture failed at the base (package 0.424s). Both public parsers failed 34 cases combined (package 1.106s), and the same parser configurations failed 18 cases combined through the authenticated HTTP transaction endpoint (package 1.228s). The public tests compare the full output and the terminal code point separately. Recorded failures include long runs of combining marks, transform buffer boundaries, and the Java dash and colon rules. The historical fixtures and raw logs are retained separately.

The frozen corpus tests the exact sigma decisions captured from the pinned JDK and compares general lowercase bytes through the existing mapper. It preserves UTF-16 edges, including the difference between physical input start and an ordinary preceding scalar. It does not assert whole-Unicode mapping identity between Go and Java.

On the final classifier, 20,801 frozen input rows, the 53-row case-owner matrix with the updated U+0345-only oracle, and the comparison of all 1,114,112 code points against the external pinned snapshots passed together in 0.548s. The generated delta coalesces equal neighboring values into 83 ranges. The test checks the Go Unicode version on every run. Regeneration uses externally preserved snapshots and their hashes.

The full casing package with the external profile passed normal (0.563s) and race (3.632s). The public selector passed in `pkg/cypher` (normal 1.091s, race 2.567s) and `pkg/server` (normal 1.235s, race 5.260s), including the raw JSON unpaired-surrogate control in both parsers. `go vet -p=1 -tags=noui,nolocalllm ./pkg/cypher ./pkg/server` passed. The public Go selector is `^(TestPublicToLowerFinalSigmaBoundary|TestHTTPToLowerFinalSigmaBoundary|TestCaseFunctionOwnerPolicies|TestStringFunctions|TestToLowerWithParameterInContains|TestStringFunctionsUseUnicodeCodePoints)$` with `-count=1 -v -timeout=8m`.

The duplicate checker was run on the staged production and public test changes. The local graph service returned `AnalysisError: analysis/ingestion failed` before reporting candidates. Source review confirms the lowercase registry shares one implementation, the ordinary and adapted paths use the existing mapper, and the adapter reuses `cyphertext.ReadCodepoint` for UTF-8 and admitted surrogate units. Existing Unicode word segmentation has different punctuation and supplementary rules. The indexed result is pending.

`make verify-list BASE=origin/release/v1.5.2` passed. `make verify BASE=origin/release/v1.5.2 ONLY=localization FETCH=0` passed localization in 7s. Full release verification, coverage groups, allocation ratchet and differential checks were outside that scoped pass.

## Matched casing microbenchmarks

Five 100 ms samples per workload on the same Apple M2 Max and Go 1.27.4. Both binaries used the same benchmark fixture and the same bounded process settings. No other Go build or test was running during the timed samples. The release base is the historical, incorrect result on some sigma inputs.

| Input | Median ns/op, base → adapter | B/op, base → adapter | Allocations, base → adapter |
| --- | ---: | ---: | ---: |
| ASCII 8 | 195 → 201 | 256 → 256 | 1 → 1 |
| ASCII 128 | 900 → 911 | 256 → 256 | 1 → 1 |
| ASCII 4096 | 25263 → 25316 | 256 → 256 | 1 → 1 |
| Short Greek word | 212 → 504 | 272 → 288 | 2 → 3 |
| Sigma after 127 ASCII bytes | 975 → 1596 | 544 → 400 | 3 → 2 |
| 31 combining marks | 458 → 1942 | 336 → 336 | 2 → 2 |
| 1024 combining marks | 10367 → 56320 | 9728 → 9728 | 8 → 8 |
| 1024 capital sigmas | 14334 → 55279 | 9472 → 11522 | 8 → 9 |
| Expanding dotted I and final sigma | 999 → 1706 | 656 → 800 | 3 → 4 |

CPU profiles show added time in classifying Unicode characters for sigma context. These are the measured costs of the approved rule in the tested function, without an end-to-end query throughput claim. Raw samples and profiles are retained in the task archive.
