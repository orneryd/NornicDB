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

## Repeated code point classification

The follow-up reuses the previous code point's classification when an adjacent code point is identical. The cursor and supplementary boundary logic still run for every code point. Classification depends only on the code point, and the cached value belongs to one invocation. The matched experiment used the exact follow-up source, SHA-256 `ba907eb95f9cdb423ed8defdf6169bd2c074333c2024f8902878e8d07df53ebd`.

Five 100 ms samples per workload compared the initial adapter with this source on Apple M2 Max. The 31-mark workload improved from 1,972 to 824 ns, the 1,024-mark workload from 59.8 to 18.6 µs, and 1,024 capital sigmas from 57.1 to 17.8 µs. The short Greek word moved from 514.8 to 516.1 ns and the three ASCII workloads changed by +1.4%, +0.3%, and -0.1%. Allocation counts matched in all nine cells. The matched CPU profiles show the classifier and `unicode.is16` dropping out of the leading sample population. This comparison uses the initial corrected adapter as its control. The earlier table uses the original release implementation as its control.

Separate allocation profiles used 200 iterations of the 1,024-mark workload with `-test.memprofilerate=1`. Both binaries measured 9,728 B/op and 8 allocations/op. Timings for these instrumented runs are excluded from the comparison above.

The follow-up branched from `d85ad3f436bd9f068a4090c87789efd79346d639`, the release merge of #1935. With the cache, the casing package passed with the pinned profile in normal and race runs (0.699s and 3.467s). Both parsers and HTTP passed normal (1.073s and 1.212s) and race (2.587s and 5.323s). The casing scan had 97.6% statement coverage, and the casing package had 88.7%. An initial coverage invocation used an incorrect snapshot directory. The corrected invocation reran the full casing package and profile.

`make verify-list BASE=origin/release/v1.5.2` passed. The full `make verify BASE=origin/release/v1.5.2 FAIL_FAST=1` run started with the reviewed source before commit. It passed localization, UI and native build and entered package coverage. The complete result and any coverage or allocation limitations are recorded on the PR. The staged indexed duplicate check again failed before candidate reporting.
