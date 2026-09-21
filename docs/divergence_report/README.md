# Divergence report — evidence, generators and runnable reproductions

This directory holds the reports that `docs/plans/cypher-convergence-plan.md` cites, plus everything needed to regenerate or re-run what they claim: the generator scripts, the raw experiment outputs, and the 131-case Cypher battery. Nothing here is compiled into NornicDB.

The reports are kept byte-for-byte as they were written. They refer to their inputs by the paths of the working folder they were produced in; this table maps those paths to this directory:

| path used inside the reports | here |
| --- | --- |
| `graphify/analyze.py`, `graphify/render.py`, `graphify/dead_summary.py` | `tools/` |
| `graphify/report/data.json`, `report/data.json` | `data.json` |
| `graphify/deadcode/` | `deadcode/` |
| `results5/concepts-2-3/fallthrough.tsv`, `strict.log`, `pipeline-first.log` | `experiments/` |
| `results5/after-merge/alter-in-tx.txt` | `experiments/alter-in-tx.txt` |
| `logs/bench2-router-*.txt` | `experiments/bench-statement-routing-*.txt` |
| "my three Cypher batteries (131 shapes)", `repro/cypher_battery*.py` | `battery/` |
| `graphify/out/<component>/graphify-out/graph.json` | not included (8 files, regenerate with the commands below) |

## Contents

| file | what | revision it was produced on |
| --- | --- | --- |
| `DIVERGENCE_REPORT.md`, `data.json` | structural signals: capability forwarding gaps between wrappers, variant families, parallel dispatchers, near-duplicate bodies. `DIVERGENCE_REPORT.md` is rendered from `data.json` by `tools/render.py`. | 994b3a68 |
| `HARD_CONVERGENCE.md` | the ten convergence items that are not mechanical | 994b3a68 |
| `CONCEPT_single_router.md` | item 1; implemented by #488 | 4ec45a85 |
| `CONCEPT_2_execution_entry_points.md`, `CONCEPT_3_expression_evaluation.md` | items 2 and 3 | a427a468 |
| `tools/analyze.py` | deterministic analysis over the per-component graphs and the Go sources → `data.json` | — |
| `tools/render.py` | `data.json` → `DIVERGENCE_REPORT.md` | — |
| `tools/dead_summary.py` | summarises `deadcode` output into `deadcode/summary.json` | — |
| `tools/experiment-probes.patch` | the three env-gated probes used for the concept 2 and 3 experiments (written against a427a468; also applies cleanly to this branch at 91b83566) | a427a468 |
| `deadcode/` | raw `deadcode` output, with and without tests as roots | 994b3a68 |
| `experiments/` | raw outputs of the experiments quoted in the concepts | see below |
| `battery/` | the 131-case battery, the isolated minimal reproductions, and their recorded results | 4ec45a85 |

No LLM was used to produce `data.json`, the dead-code lists, or any experiment output. The "why it is hard" and "approach" paragraphs in `HARD_CONVERGENCE.md` and the concepts are assessments, not tool output.

## Regenerating the report

Requirements: Python 3.10+, `pip install graphifyy==0.9.53 tree-sitter tree-sitter-go`, Go for `deadcode`.

```bash
# 1. stage the non-test, non-generated Go sources of the eight components
for c in cypher storage search nornicdb server bolt multidb embed; do
  mkdir -p work/src/$c
  (cd pkg/$c && find . -name '*.go' ! -name '*_test.go' \
      ! -path './antlr/cypher_parser.go' ! -path './antlr/cypher_lexer.go' ! -name 'cypherparser_*' \
    | cpio -pdm --quiet "$OLDPWD/work/src/$c")
done

# 2. one code-only graph per component (tree-sitter AST, no LLM, no network needed)
for c in cypher storage search nornicdb server bolt multidb embed; do
  graphify extract work/src/$c --code-only --out work/out/$c --max-workers 4
done

# 3. analysis and rendering. analyze.py reads /work/src and /work/out and writes /work/report/data.json;
#    either run it with the work directory mounted at /work, or change ROOT at the top of the script.
python docs/divergence_report/tools/analyze.py
python docs/divergence_report/tools/render.py      # expects data.json in ./report and the cypher graph in ./out
```

`analyze.py` has four passes: (A) exported methods of an inner type that some wrappers of the same chain forward and others do not; (B) functions on one receiver whose names differ only by a variant suffix (`WithContext`, `Locked`, `VisibleAt`, `InTxn`, …); (C) function pairs with ≥6 callees each and callee-set Jaccard ≥0.45, from the graph's `calls` edges; (D) near-duplicate bodies by 12-token shingles with identifiers and literals normalised (≥90 tokens, similarity ≥0.70 or containment ≥0.85). The wrapper families for pass A are listed in `FAMILIES` near the bottom of the script.

Dead code:

```bash
go install golang.org/x/tools/cmd/deadcode@latest
deadcode -tags noui ./...            > dead-notest-noui.txt     # unreachable from the binaries
deadcode -test -tags noui ./...      > dead-test-noui.txt       # unreachable even with tests as roots
python docs/divergence_report/tools/dead_summary.py             # set B and ROOT at the top first
```

Two known false-positive classes, both handled in `dead_summary.py` or noted in the reports: the top-level `apoc/` tree is built as a Go plugin and is reported dead although it is not; functions referenced only from `cuda`/`metal`/`vulkan`/`localllm`/OS-specific build-tagged files are not compiled by the tool, so names that appear in those files are excluded.

## Re-running the experiments (concepts 2 and 3)

```bash
git apply docs/divergence_report/tools/experiment-probes.patch

# concept 3, experiment 1: which expressions are returned as text while the suite passes
NORNIC_FALLTHROUGH_PROBE=/tmp/fallthrough.tsv go test -tags "noui,nolocalllm" ./pkg/cypher -count=1
# concept 3, experiment 2: the main evaluator returns nil instead of the text
NORNIC_STRICT_EVAL=1     go test -tags "noui,nolocalllm" ./pkg/cypher -count=1
# concept 2: the router tries executePipeline before every other rule
NORNIC_PIPELINE_FIRST=1  go test -tags "noui,nolocalllm" ./pkg/cypher -count=1

git apply -R docs/divergence_report/tools/experiment-probes.patch
```

Recorded results on a427a468 (`experiments/`): `fallthrough.tsv` 1,231 lines / 150 distinct expressions (sites: main-evaluator 1,039, values-evaluator 74, set-evaluator 70, where-literal 48); `strict.log` 7 failing tests; `pipeline-first.log` 32 failing tests (14 assert the route taken, 18 differ in result). Without any environment variable set the patched tree behaves exactly like the unpatched one.

`experiments/bench-statement-routing-*.txt`: `BenchmarkStatementRouting` (now in `pkg/cypher/transaction_router_bench_test.go`), `-benchtime=1s -count=5`, golang:1.27.1 container with 4 CPUs on a Ryzen 9 7950X, in-memory engine, 2,000 nodes / 500 relationships, result cache defeated by a changing parameter. One file for main at 4ec45a85, one for the single-router branch that became #488. These are the numbers in #487 and #488.

`experiments/alter-in-tx.txt` + `alter_in_tx.py`: `ALTER COMPOSITE DATABASE … ADD ALIAS` and `ALTER DATABASE … SET LIMIT` inside an explicit Bolt transaction followed by ROLLBACK, against a server built from the #488 branch: both succeed and both are still in effect after the rollback. (The last two lines of the `.txt` are a separate check, `CREATE DATABASE` inside a rolled-back transaction, on the build before #488.)

## The 131-case battery

`battery/cypher_battery.py` (46 cases), `cypher_battery2.py` (50), `cypher_battery3.py` (35). Each case is `(query, params, expected rows)`; every file creates its own small fixture graph and deletes it first, so they are independent of each other and of existing data with other labels. Expected rows were written by hand from the openCypher semantics; row order is compared only when the query has ORDER BY. The three `cypher_isolated*.py` files are minimal one-query reproductions, each group starting from the same clean six-node graph; they are the sources of the reproductions in issues #447–#481.

```bash
pip install neo4j
python battery/cypher_battery.py  localhost auto    # driver auto-commit
python battery/cypher_battery.py  localhost tx      # managed explicit transaction (driver.execute_query)
```

The scripts connect to `bolt://HOST:7687` without authentication; start the server with `serve --no-auth` on an empty data directory and embeddings disabled. Output is one `FAIL:` block per failing case with expected and actual rows, and a final `[mode] N cases, M failed` line.

Recorded results, `battery/baseline-4ec45a85/` (server built from main at 4ec45a85, `noui,nolocalllm`):

| battery | auto-commit | explicit transaction | auto-commit with `NORNICDB_PARSER=antlr` |
| --- | --- | --- | --- |
| 1 (46) | 13 failed | 13 failed | 13 failed |
| 2 (50) | 11 failed | 11 failed | 11 failed |
| 3 (35) | 20 failed | 20 failed | 20 failed |

The failing sets were unchanged by #483–#486 and by #488. They are a snapshot, not a target: the plan's TCK baseline supersedes them, and the battery is meant to be imported as `gh-<number>-<case>` local regressions next to it.
