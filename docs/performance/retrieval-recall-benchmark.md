# Retrieval Recall Benchmark

`recall-bench` creates reproducible BEIR retrieval runs for NornicDB. It writes
standard six-column TREC runs, evaluates qrel-backed query sets, and compares
paired runs. It contains no query-expansion or Dice logic.

## Prerequisites

- Go and a running Ollama server for hybrid runs.
- The BEIR dataset files: `corpus.jsonl`, `queries.jsonl`, and the test qrels TSV.
- For the SciFact configuration recorded below: `ollama pull bge-m3:latest`.

Build the tool from the repository root:

```sh
go build -o bin/recall-bench ./cmd/recall-bench
```

## SciFact Protocol

Create a fresh benchmark database and ingest the official corpus. The state file
makes an interrupted import resumable and prevents mixing corpus versions.

```sh
bin/recall-bench ingest \
  --dataset scifact \
  --corpus bench-data/beir/scifact/corpus.jsonl \
  --data-dir bench-data/nornic/scifact \
  --embedding-provider ollama \
  --embedding-model bge-m3:latest \
  --embedding-dim 1024
```

Always create the manifest from qrel-backed queries. This avoids evaluating
unjudged rows from `queries.jsonl`.

```sh
bin/recall-bench manifest \
  --dataset scifact --split test \
  --queries bench-data/beir/scifact/queries.jsonl \
  --qrels bench-data/beir/scifact/qrels/test.tsv \
  --limit 300 --seed 20260810 \
  --output bench-data/runs/scifact-test-manifest.json
```

Run BM25 and default hybrid RRF at depth 100, then evaluate each TREC run:

```sh
bin/recall-bench run --mode bm25 \
  --data-dir bench-data/nornic/scifact \
  --queries bench-data/beir/scifact/queries.jsonl \
  --manifest bench-data/runs/scifact-test-manifest.json \
  --output bench-data/runs/scifact-bm25.trec

bin/recall-bench run --mode rrf \
  --data-dir bench-data/nornic/scifact \
  --queries bench-data/beir/scifact/queries.jsonl \
  --manifest bench-data/runs/scifact-test-manifest.json \
  --embedding-provider ollama --embedding-model bge-m3:latest --embedding-dim 1024 \
  --output bench-data/runs/scifact-rrf.trec

bin/recall-bench evaluate --qrels bench-data/beir/scifact/qrels/test.tsv \
  --run bench-data/runs/scifact-rrf.trec \
  --output bench-data/runs/scifact-rrf.metrics.json
```

For a controlled vector comparison on SciFact's 20,488 indexed passages, force
brute-force CPU retrieval for the command being measured:

```sh
NORNICDB_VECTOR_CPU_BRUTE_MAX_N=25000 bin/recall-bench run --mode rrf ...
```

This is a benchmark determinism setting, not a production recommendation. Fresh
HNSW builds can differ across processes. Production-style ANN profiles are
selected with `NORNICDB_VECTOR_ANN_QUALITY=fast|balanced|accurate|compressed`.

## Tuning Levers

`run --mode rrf` accepts `--rrf-preset`, `--rrf-k`, `--vector-weight`,
`--bm25-weight`, and `--min-rrf-score`. The current production preset and the
default preset both use equal lexical/vector weights of `1.0`; the explicit
flags make alternate runs auditable. A score floor of `0` ensures RRF can fill a
depth-100 run, while production retains its configured default floor.

`NORNICDB_BM25_IDF_MIN_DOC_FREQ` controls the minimum document frequency for a
term to participate in BM25 lexical seed selection. It defaults to `2`, matching
the previous hard-coded behavior; values below `1` are treated as `1`. This is a
clustering seed control, not a BM25 rank-score formula or a query-expansion rule.

`NORNICDB_HNSW_LEXICAL_SEED_ENABLED=false` disables BM25-selected passage
ordering during HNSW construction. It defaults to `true`; changing it invalidates
the persisted HNSW build settings, so compare fresh graph builds rather than two
runs against one persisted graph. GPU-assisted construction can vary between
fresh builds, so use multiple independent pairs before changing a production
default.

### Large-Corpus Construction Result

On a separate corpus with just over 1 million embedding chunks,
BM25 lexical seeding reduced `fast` HNSW construction time from about 27 minutes
to about 10 minutes: a 2.7x speedup. Both builds used `M=16`,
`efConstruction=100`, and up to 2,048 seed passages (`256` high-IDF terms times
`8` passages per term); the test reported no recall or graph-quality loss.

This is a construction-throughput result, not a claim that seeding improves
SciFact retrieval quality. At SciFact's 20,488 vectors, the same setup does not
show a meaningful recall improvement and is too small to reproduce the
large-corpus traversal-work saving. See [the full 1M construction
measurement](https://github.com/orneryd/NornicDB/discussions/22) for its corpus
and timing methodology.

For a CPU-only construction comparison, set both switches on a fresh data
directory and change only the lexical-seeding value between runs:

```sh
NORNICDB_HNSW_BUILD_GPU_ENABLED=false \
NORNICDB_HNSW_LEXICAL_SEED_ENABLED=false \
NORNICDB_VECTOR_ANN_QUALITY=accurate \
bin/recall-bench run --mode rrf ...
```

Use paired bootstrap comparison when choosing a profile:

```sh
bin/recall-bench compare --qrels bench-data/beir/scifact/qrels/test.tsv \
  --baseline bench-data/runs/scifact-baseline.trec \
  --candidate bench-data/runs/scifact-candidate.trec
```

For a leaderboard-comparable document-level run with the local BGE reranker,
rerank all 100 exact hybrid candidates. Rank pooling reads the GGUF classifier
head and the command aborts if the model does not return one relevance logit.

```sh
NORNICDB_VECTOR_CPU_BRUTE_MAX_N=25000 bin/recall-bench run --mode rrf \
  --data-dir bench-data/nornic/scifact \
  --queries bench-data/beir/scifact/queries.jsonl \
  --manifest bench-data/runs/scifact-test-manifest.json \
  --embedding-provider ollama --embedding-model bge-m3:latest --embedding-dim 1024 \
  --rrf-preset default --min-rrf-score 0 \
  --reranker-provider local-gguf \
  --reranker-model models/bge-reranker-v2-m3.gguf \
  --reranker-pooling-type 4 --rerank-top-k 100 \
  --reranker-max-doc-chars 32000 --reranker-timeout 30s \
  --tag nornic-exact-bge-rerank \
  --output bench-data/runs/scifact-exact-bge-rerank.trec
```

The TREC run contains unique official BEIR document IDs. Internal embedding
chunks are collapsed to their parent document before RRF and reranking.

## Recorded SciFact Results

The following corrected measurements use the official 300 test qrels and
`bge-m3:latest` with 1,024 dimensions. They are configuration-specific, not a
claim of leaderboard parity: public BEIR leaderboards commonly report nDCG@10
and differ in models, chunking, and indexing.

| Retrieval profile                               | Recall@100 | nDCG@10 |
| ----------------------------------------------- | ---------: | ------: |
| BM25 V2                                         |    0.88422 | 0.59974 |
| HNSW accurate, equal RRF weights                |    0.93767 | 0.65534 |
| Exact CPU brute force, equal RRF weights        |    0.94433 | 0.67932 |
| Exact CPU brute force, previous adaptive policy |    0.92600 | 0.67321 |

The BM25 profile also measured MRR@10 `0.56161` and MAP@100 `0.55487`.
The equal-weight production default was selected because it improved the
controlled SciFact recall result over the previous word-count heuristic. Repeat
the protocol on additional BEIR datasets before treating it as a universal rule.

### Native BGE Reranker: 174-Query Partial Run

A native `bge-reranker-v2-m3.gguf` Every completed query has exactly 100
unique official BEIR document IDs.

| Profile                         |   Recall@10 |  Recall@100 |     nDCG@10 |      MRR@10 |     MAP@100 |
| ------------------------------- | ----------: | ----------: | ----------: | ----------: | ----------: |
| Exact equal RRF, no reranker    |     0.78793 |     0.93563 |     0.67043 |     0.64404 |     0.63486 |
| Exact equal RRF + native BGE-M3 | **0.82510** | **0.93563** | **0.72292** | **0.69447** | **0.69215** |
| Absolute change                 |    +0.03716 |    +0.00000 |    +0.05248 |    +0.05042 |    +0.05729 |

Reranking does not change Recall@100 because it only reorders the same 100
candidates. It moves more relevant documents into the first 10 results and
substantially improves their ordering.

#### Comparison with published BEIR references

BEIR's official metric for cross-system comparison is nDCG@10. Published
SciFact reference results provide useful context:

| Published system                      | SciFact nDCG@10 |
| ------------------------------------- | --------------: |
| BEIR 2021 BM25                        |           0.665 |
| BEIR 2021 ColBERT                     |           0.671 |
| BEIR 2021 BM25 + MiniLM cross-encoder |           0.688 |
| BEIR 2024 SPLADE reference            |           0.699 |
| NornicDB native BGE-M3,               |           0.723 |
| BEIR 2024 Contriever + SPLADE hybrid  |           0.734 |

On this partial sample, NornicDB is 2.39 nDCG points above the published SPLADE
reference and 0.011 points below the published Contriever + SPLADE hybrid. This
is competitive evidence, not an official leaderboard placement: NornicDB's row
uses BGE-M3 retrieval plus reranking rather than the reference systems' models, and NornicDB's document-chunk aggregation.

Sources: the [original BEIR results](https://arxiv.org/abs/2104.08663) and the
[2024 reproducible reference systems and official leaderboard
paper](https://arxiv.org/abs/2306.07471).

### CPU Fast HNSW Seeding Check

Ten independent paired SciFact builds used the `fast` preset (`M=16`,
`efConstruction=100`, `efSearch=50`) with CPU-only construction, equal RRF
weights, and the same 300-query manifest. The seeded and unseeded graph builds
were recreated from the same embedded corpus for every pair.

| Observed     | Recall@10 | Recall@100 | nDCG@10 |
| ------------ | --------: | ---------: | ------: |
| Seeded run   |   0.78622 |    0.94100 | 0.66995 |
| Unseeded run |   0.75622 |    0.93767 | 0.64659 |
