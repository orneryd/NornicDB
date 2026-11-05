# Quantization verification

Runtime and timed harness: `7c201376d1203a651fc625869fb98df7be746ac4`.
PR base checked locally: `f2ddd27cfae3788076778a0a0d48c827df046a3b`
(`release/v1.4.4`). Through report snapshot `511e33769fb33a02a91ffc454e3cc030e7413fcf`, subsequent
changes affected benchmark source preflight, captions, tests and reports; runtime
source remained identical to the timed head. Later scalar-default and concurrent
IVF-HNSW work is recorded separately in [the follow-up](ivf-parallel/README.md).

## Product proof

The public service/gRPC regressions exercise named/property/chunk identity,
original document anchors, lexical graph entry points, scalar/PQ/binary scoring,
updates, deletion, Clear, checkpoint reload, pending-PQ activation after growth,
thresholds, request ignore/rescore/oversampling, collection policy reconstruction,
unsupported configurations, cancellation and concurrent query/mutation. Native
int8 tests compare exact signed integer sums with the portable implementation,
including -128 values, block tails and accumulation beyond int32.

The complete corpus audit passed **41 cases and 263 curves**, with 88,155 vectors,
1,024 dimensions, K=10 and 1,000 queries per curve. It checks expected populations,
mode/beam membership, corpus hashes, clean embedded binary revisions, active
coordinate counts and CPU construction. [Repeatable audit](audit.py),
[audit result](audit.json). Per-result `.log` files contain selected, unmodified
construction/strategy records; full server logs remain in local benchmark data.

## Commands and results

Commands below ran locally; paths under `bench-data/vector_ann/profiling` are
retained locally, with selected transcripts copied to [checks/](checks/).

```sh
go test ./pkg/search ./pkg/qdrantgrpc ./pkg/simd ./pkg/localization -count=1 \
  -coverprofile=bench-data/vector_ann/profiling/quantization-package-coverage.out
go test ./pkg/qdrantgrpc -count=1 \
  -coverprofile=bench-data/vector_ann/profiling/quantization-qdrant-final-coverage.out
go test -race ./pkg/search ./pkg/qdrantgrpc ./pkg/simd \
  -run 'TestHNSWQuant|TestHNSWBinary|TestQuantized|TestHNSWPQActivates|TestQuantiz|TestConfigureHNSW|TestCollectionQuantization|TestBinaryUnsupported|TestCollectionQuantizationProtocol|TestCPUHNSW|TestDotProductInt8|TestHNSWLexicalSeedsFollow|TestANNPublicBuildUses|TestHNSWBuildIteratorPrioritizes' \
  -count=1
CGO_ENABLED=0 go test ./pkg/simd -count=1
go vet ./pkg/search ./pkg/qdrantgrpc ./pkg/simd ./pkg/localization
python -m unittest discover -s testing/benchmarks/vector_ann -p 'test_*.py'
make test
```

- Fresh package run: search **180.963s**, Qdrant **3.596s**, SIMD **0.836s**,
  localization **3.293s**. After explicit private API parameter wiring, Qdrant's
  fresh package run passed in **2.757s**. The subsequent full suite checked the
  frozen runtime head.
- Go statement coverage: search **90.2%**, Qdrant **90.9%**, SIMD **95.7%**.
  Newly added Go production files: **447/472 statements, 94.7%**. Localization's
  full package remains **21.9%**, including its existing generated/message paths;
  this is a recorded limitation, rather than a package-wide coverage claim.
- Focused races passed: search **4.630s**, Qdrant **1.906s**, SIMD **1.461s**.
  Portable SIMD and vet passed. C++ branches are outside Go statement coverage.
- Python harness checks: **17 passed** after source-preflight/caption corrections.
  Their three original failures and the lock-preflight regression are preserved.
- First `make test`: logging/cardinality gates passed; **93 successful packages**
  (34 fresh, 59 cached), with one failure in storage:
  `TestTemporalCurrentPointerRefresh_ScalesLinearly`. Make's output filter omitted
  the assertion values. No storage source, dependencies or assertion changed.
- The failing storage test then passed once and **10 isolated repetitions** on
  the same runtime source. A clean baseline probe passed with `-tags nolocalllm`.
  The unchanged `make test` retry passed **94 packages** (4 fresh, 90 cached),
  including the fresh complete storage package. Both full-run transcripts are
  preserved. This establishes the recorded retry result; the first run failed.
- The baseline worktree's default-tag storage probe could not link its absent
  local-LLM library. Its `nolocalllm` probe passed. Product benchmark binaries
  consistently use `noui nolocalllm`; no default-tag baseline suite pass is claimed.

## Preserved correction evidence

| Boundary | Failing observation | Current correction |
|---|---|---|
| Ranked encoded embeddings | File/memory sources inserted unseeded vectors ahead of ranked named/property/chunk embeddings | Shared priority selection resolves indexed identity and preserves directly indexed document anchors |
| Existing anchor contract | An intermediate implementation put a document's chunk ahead of the next document anchor | Original anchor assertion retained; common priority owner corrected and complete package rerun passed |
| Product codebooks | Spherical training collapsed magnitude; a 1-D fixture had squared error 19.802498 | One training owner selects cosine for coarse routing and squared distance for product books |
| Property supplements | `rescore=false` returned 0.6332377 instead of approximate score 0 | Public selection and supplements share the same scoring policy |
| First PQ activation | A newly trainable collection's first approximate query returned exact score 1 instead of 0.99987453 | Supplements are scored after candidate discovery activates the model |
| Source/captions | Missing binary source information was accepted; 8× variants retained 2× captions | Binary preflight before report locking; producer requests stamping; runner/renderer share effective-factor formatting |

## Hardware checks and unexecuted work

Native Metal resident scoring passed on this machine. Shared resident/kernel,
lexical construction and CPU/reference recall contracts passed in the focused
native-contract run (**95.630s**). CUDA was unavailable on this platform/build;
Vulkan was unavailable. Those native tests **skipped**, with their reasons in
[the transcript](checks/quantization-native-build-contracts.log).

The int8 C++ fallback also compiled with:

```sh
clang++ -arch x86_64 -O3 -std=c++11 -c pkg/simd/int8_dot.cpp -o bench-data/vector_ann/profiling/int8-amd64.o
```

This is compile evidence for the x86_64 branch, alongside ARM64 execution and
portable Go tests. Native AMD64, CUDA and Vulkan execution/performance remains
for mm0nst3r's machine. No GPU-compressed full-corpus performance claim is made.
The BEIR SciFact baseline was not rerun: its corpus/model cache was unavailable;
these Titan/vector-only results do not establish its retrieval metrics.

Per the maintainer's explicit task instruction, **`make verify` was skipped**.
Earlier `make verify` attempts remain canceled/incomplete. GitHub Actions was not dispatched. The extra full-suite retry addressed the
observed test failure; no assertion was weakened and no storage change was made.

## Excluded measurements

An initial complete float-file control and one canceled retry used baseline
binaries without embedded VCS information, including a worktree build whose
explicit `-buildvcs=true` still omitted it on this toolchain. They were excluded.
The published baseline was rebuilt from a clean regular local clone, checked for
`vcs.revision=239b057e7...` and `vcs.modified=false`, then measured afresh. The
runner now rejects this missing-source condition before engine startup.

An early PQ diagnostic was canceled after discovering its training-metric
mismatch. Intermediate SIMD/query microbenchmarks overlapping other work were
excluded from timing conclusions. Published query-preparation microbenchmarks
ran after tests and compilation stopped. Raw excluded/canceled records remain
locally; they were never relabeled as completed final measurements.
