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

Use paired bootstrap comparison when choosing a profile:

```sh
bin/recall-bench compare --qrels bench-data/beir/scifact/qrels/test.tsv \
  --baseline bench-data/runs/scifact-baseline.trec \
  --candidate bench-data/runs/scifact-candidate.trec
```

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
