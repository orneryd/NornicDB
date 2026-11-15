# Projected-read comparison

Compare two server binaries through Bolt on copies of the same ordinary-format
fixtures. Build the client with:

```sh
go build -o /tmp/projected-read-client ./testing/benchmarks/projected_reads
go build -tags noui,nolocalllm -o /tmp/northwind-client ./testing/benchmarks/northwind_power
```

The mixed fixture contains Northwind plus 16 replicas of a frozen Markdown corpus.
Each document has stable identity, source text, independent deterministic 4 KiB
byte noise, a 256-element numeric list and small projected fields. `-seed` appends
these documents to an existing Northwind store; `-verify` checks every field
against the frozen files. Corpus identity hashes relative paths and file contents.
Keep the same frozen source files for seeding, verification and measurement.

`run_lab.py` requires Linux `/proc`, two prebuilt servers, ordinary-format prepared
offline data directories and explicit source revisions. It copies the inputs before each
trial, alternates run order and validates paired result fingerprints. No source
fixture is opened by a server. Query-result caching is explicitly disabled.

The recorded corpus is the 272 Markdown files in `ddc59dc20:docs`, with CRLF line
endings, replicated 16 times. Reproduce that frozen checkout rather than seeding
from changed documentation. The result folder's manifest lists every file hash.

```sh
python3 testing/benchmarks/projected_reads/run_lab.py \
  --old-bin /baseline/nornicdb --new-bin /candidate/nornicdb \
  --old-source RELEASE_COMMIT --new-source CANDIDATE_COMMIT \
  --old-data /fixtures/combined --new-data /fixtures/combined \
  --client /tmp/projected-read-client --docs /frozen-docs \
  --out /receipts/mixed --prepared --clients 1,8 --trials 4 --queries 8000
```

For Northwind use its canonical client, `--workload northwind`, a pure Northwind
fixture, `--clients 1`, and explicit iteration/warm-up counts. The first reference
call consumes one warm-up when available; it is still performed with zero warm-ups.
Northwind RSS is sampled from the server every 10 ms while the driver runs.

Use `--memory-profiles` for a separate diagnostic pass. It captures heap and
allocation profiles before and after the workload, with two explicit GCs at each
boundary. Its latency fields are not primary timing results. Inspect profiles
with `go tool pprof`, using `-base` for cumulative allocation differences. Timing
runs keep pprof disabled and must not overlap builds, tests or profile analysis.

The large-field controls return body, byte noise and numeric-list properties;
they are not whole-entity queries. In the six-phase sequence the first such
control follows narrow projections, so the builds intentionally have different
body-cache coverage. Treat it as the cost of widening a read. Use a standalone
phase for equal cold preparation, or enough large-field calls to warm the measured
document set. Process-cold does not imply cold filesystem/device caches.
