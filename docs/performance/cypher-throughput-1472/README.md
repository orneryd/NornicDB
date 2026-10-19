# Cypher throughput investigation (#1472)

The reported shared-worker results (9,831 and 11,424 ops/s against a 15,000
floor) have not been reproduced locally. The original PR and its immutable
release base clear the floor on this machine; the original PR also passes its
full Nornic-parser package run without a waiver. Local improvements do not
establish that the original worker failure is resolved. The maintainer directed continuing without the unavailable original job evidence;
conclusions therefore remain limited to reproducible local baselines.

## Retained correction

The CPU and allocation profiles identified redundant work in the canonical
streaming WITH projection owner: even a WITH without WHERE built and filled a
predicate scope, then discarded it. The streaming caller now requests a
reusable predicate scope only when its plan contains WHERE. The shared
projection helper is unchanged: ordered/materialized WITH also consumes its
explicit scope even without WHERE. Projection, parameter carriage and WHERE
evaluation still use the same owner.
No query-specific executor, cache, transaction shortcut or threshold change.

The 1,024-row WITH benchmark asserts exact sums, with and without WHERE, and
changes an unused parameter to prevent result-cache hits. The original mutation
benchmark still asserts one relationship created and deleted per operation.
The public throughput floor remains 15,000 and strict checks are not waived.

## Machine and controls

October 18, 2026; Apple M2 Max, darwin/arm64, 12 logical CPUs, Go 1.26.9. Local
database and embedding workers remain running. Benchmark processes run serially;
no tests or compilation run concurrently with the retained benchmark matrix.

Original PR: 8b9e64eb52463e8fb59e4a8c31a92192e96dcd34. Its original release
base: 24d08776cdb9cb624da1073007da437306518426. The final retained experiment is
against current release/v1.4.4 b1f9ed81189cfdcbe35977dd35e99825e30f3e2d; both
sides use identical benchmark harnesses. The runtime diff changes only
pipeline_row_stream.go. Results on this machine are not shared-worker results.

Both binaries are compiled with `go test -c -tags noui,nolocalllm ./pkg/cypher`.
This machine also requires
`CGO_LDFLAGS=-L/Users/timothysweet/src/NornicDB/lib/llama`.
Execute from pkg/cypher with `-test.run=^$ -test.count=1 -test.benchmem` and
NORNICDB_PARSER=nornic or antlr. Five rounds alternate control/current order.

| Group | -test.bench | -test.benchtime |
| --- | --- | --- |
| mutation | ^BenchmarkPipelineLimitedCartesianCreateDelete$ | 1s |
| with | ^BenchmarkPipelineWithScope$ | 500ms |
| routing | ^BenchmarkStatementRouting$ | 200ms |
| distinct | ^BenchmarkSemanticValidationDistinctText$ | 300ms |

The mutation benchmark has 100 actors and 150 movies, created directly in the
store without properties. The strict floor and profile harness create the same
population through Cypher with name/title properties, so their allocation and
latency figures need not equal the benchmark. The profiles repeat the actual
floor query 20,000 times per test (ten repetitions for useful sampling).
MemoryEngine uses the unified in-memory Badger backend; its name does not mean
the old map-backed engine. Allocation traffic is not retained heap size.

## Confirmed local results

[Five-round confirmation](confirmation.md) repeats the affected workloads after
the unusually variable initial matrix. WITH without WHERE improves from
275.152 to 155.482 µs (Nornic, 43.5% lower latency) and 276.430 to 154.741 µs
(ANTLR, 44.0% lower latency). Mutation is effectively flat: 28.807 to 28.722 µs
and 29.059 to 28.936 µs respectively. It saves 336 B and two allocations per
operation. Predicate-WITH and distinct-text validation are within noise.
Do not interpret the initial matrix's large routing swings as gains from this
change: those paths are unchanged. The confirmation supersedes its affected
workload latency conclusions; all initial samples remain available.

A fresh final-source mutation CPU/allocation profile is included as
[cpu-top.txt](cpu-top.txt) and [allocation-top.txt](allocation-top.txt).
Command: final current binary with `NORNICDB_PARSER=nornic -test.run=^$`
`-test.bench=^BenchmarkPipelineLimitedCartesianCreateDelete$ -test.benchtime=3s`
`-test.cpuprofile=... -test.memprofile=...`. This includes fixture allocation;
it is allocation traffic, not retained heap. Keyword scanning remains about
10% cumulative CPU, allocation about 23%; storage transactions, node copying,
and label streams remain material costs. This is an in-process workload, so
there is no connection overhead to remove.

## Rejected experiment

Extending keyword-position caching to all scan configurations improved repeated
mutation queries and semantic-plan misses. The first implementation also slowed
simple MATCH and literal RETURN, so it was rejected. A compact-key revision
recovered most routing overhead, but a distinct-text control exposed about 10%
more validation time and roughly 1.8 KB more allocation per unique query. That
cache expansion and its tests were fully reverted; keyword_scan.go is unchanged.

The existing SemanticValidationUncached benchmark resets semantic-verdict caches
but repeats the same text: lexical caches can still hit. The added distinct-text
benchmark changes the literal ID every iteration and uses no semantic-verdict
cache, exposing the miss cost that the earlier workload did not measure. This
negative control is retained to avoid repeating that mistaken optimization.

A first version also removed scope construction inside the shared helper.
Full package tests rejected it: ordered WITH uses that scope without WHERE,
including APOC path queries. The helper was restored, and the corrected
streaming-caller-only change was verified. Superseded timing data is not final-source proof.

## Verification and limits

Both final package suites passed fresh with `NORNICDB_STRICT_PERF_ASSERTS=1`:
Nornic in 134.113 s (floor test 26,998.75 ops/s), ANTLR in 222.459 s
(29,442.07 ops/s). Each used the final compiled test binary with
`-test.v -test.count=1 -test.timeout=30m`, built with the tags above.
The original 15,000 floor is unchanged.

The new allocation regression fails against the unmodified release runtime
(4 allocations against a maximum of 3) and passes with the correction.
The public ordered-WITH test verifies an ORDER BY binding omitted from the
projection, while existing WHERE and APOC tests exercise the other consumers.
Focused Nornic race checks passed (2.098 s); ANTLR race checks and package vet also passed. [Package skips](skips.md) are recorded separately.

`make verify-list BASE=origin/release/v1.4.4` and
`make verify BASE=origin/release/v1.4.4` stop in `verify-selftest`.
With the configured environment loaded, the mandatory live commit hook rejects
the verifier's temporary `plan` repository because it has no ingested record.
Path-filter and build-input-gate self-tests pass; downstream verifier checks
are unexecuted. The hook was not disabled or bypassed.

[Full initial paired matrix](results.md) and the JSON samples retain all
observations, including large swings in unchanged workloads. They must not be
read as established gains or regressions without confirmation.


Focused coverage run passed (0.950 s), exercising projection, WHERE, ordered WITH
and APOC consumers. Its package statement coverage is 16.7%; this is a focused
run and does not establish full-package coverage. `go vet -tags noui,nolocalllm
./pkg/cypher` and `git diff --check` passed.

## Publication and live commit check

Source and report are published in draft #1538, linked from #1472. The mandatory
live check passed against `orneryd/NornicDB` / `nornicdbcode` using the maintainer's
new bounded-search policy (limit 1000, RRF floor 0.01; no cursors). Analysis took
21.99 s. Earlier cursor-mode attempts failed with HTTP 410 despite a verified
15-minute live cursor TTL; they were not passes and no hook was bypassed.
A renamed copy of production `pipelineProjectWithRow` was then staged as a
separate probe: the hook blocked it as a 100% similar reuse candidate in 10.81 s.
HEAD stayed unchanged, and the probe was unstaged and removed. Both checks warn
that the ingested baseline is older (`7e90401c6`); they do not establish current
baseline completeness or exhaustive retrieval.
