# Dense-Seeded Query Expansion With Modified Dice

## Goal

Improve hybrid-search recall with no additional online model call by turning a bounded set of existing semantic-search hits into corpus-grounded BM25 expansion terms.

The target is a faster, cheaper experimental alternative to cross-encoder query expansion:

1. Run the existing query embedding and semantic vector search once.
2. Preserve a bounded set of top semantic passages before vector results collapse to nodes.
3. Extract candidate unigrams, bigrams, and trigrams from those passages.
4. Rank candidates using semantic-source strength, corpus distinctiveness, and support across independent passages.
5. Use corrected character and word-bigram Dice only to normalize variants, remove redundancy, and preserve phrase structure.
6. Append a small expansion set to the original BM25 query.
7. Fuse enriched BM25 with the original semantic ranking through existing RRF.
8. Apply existing optional MMR and Stage-2 reranking after fusion without changing them.

The feature succeeds if it produces a measurable recall improvement with no meaningful first-page quality regression and only a small, bounded latency increase. It does not need to equal full Cross-Encoder Query Expansion (CE-QE) to be valuable.

This method is **dense-seeded pseudo-relevance feedback with modified-Dice deduplication**, exposed as provider `dense_prf_dice`. It must not be described as CE-QE. CE-QE uses cross-encoder token attribution and remains an optional quality reference.

The feature must be disabled by default, fail open to current hybrid search, and leave indexes, embeddings, HNSW construction, BM25 statistics, and the original semantic query unchanged.

## Current Anchors

Primary implementation surfaces:

- `pkg/search/search.go`
- `pkg/search/vector_pipeline.go`
- `pkg/search/hybrid_cluster_routing.go`
- `pkg/search/fulltext_index.go`
- `pkg/search/fulltext_index_v2.go`
- `pkg/search/observability.go`
- `pkg/search/rerank.go`
- `pkg/eval/harness.go`
- `cmd/eval/main.go`
- `docs/advanced/search-evaluation.md`
- `pkg/config/config.go`
- `pkg/config/dbconfig/resolver.go`
- `pkg/server/server.go`
- `pkg/nornicdb/embed_queue.go`
- `pkg/embeddingutil/helpers.go`
- `docs/operations/environment-variables.md`
- `docs/user-guides/hybrid-search.md`
- `docs/architecture/embedding-search-flow-diagrams.md`

Relevant existing behavior:

- `Service.rrfHybridSearch(...)` already runs vector retrieval before BM25, providing the insertion point.
- `VectorSearchPipeline.Search(...)` returns raw vector IDs such as `nodeID-chunk-N`, `nodeID-named-name`, and `nodeID-prop-key` before `collapseIndexResultsByNodeID(...)` removes provenance.
- `normalizeVectorResultIDToNodeID(...)` maps managed vector IDs to parent node IDs.
- `embeddingutil.BuildText(...)` and deterministic `ChunkText(...)` define the text and chunks represented by embeddings.
- BM25 V1 and V2 already own documents, tokenization, document frequency, and IDF-related statistics.
- `hybrid_cluster_routing.go` already builds per-cluster lexical profiles.
- `Reranker` remains an optional downstream Stage-2 scorer. Expansion must not invoke it.
- `searchCacheKey(...)` caches complete responses, and index updates invalidate the cache.
- `dbconfig.Resolver` already resolves per-database search settings.
- `pkg/eval` and `cmd/eval` already provide a small-suite quality harness, but they stop at 50 results and do not implement paper-compatible Recall@100 or standard graded nDCG. Reuse metric/reporting concepts, not the current metric implementation unchanged.

## Explicit Product Decisions

### Target Outcome

Target a moderate recall gain at low incremental latency and zero additional inference.

Required experiment variants:

1. Current BM25 plus vector RRF.
2. Dense-seeded term statistics without Dice.
3. Dense-seeded term statistics with modified Dice.
4. Full CE-QE when an attribution-capable reference is available.

Variants 2 and 3 isolate Dice's contribution. Gains shared by both belong to dense pseudo-relevance feedback, not Dice.

### Feature Scope

The feature changes only the query submitted to BM25. It does not:

- replace semantic search or the query embedding;
- run a second vector search;
- mutate BM25 or vector indexes;
- replace RRF with raw-score fusion;
- invoke a cross-encoder or generative LLM;
- generate vocabulary absent from semantic source passages;
- use BM25 results as feedback sources;
- silently enable Stage-2 reranking;
- claim equivalence with CE-QE before benchmarks support it.

### Signal Responsibilities

- Vector retrieval identifies the relevant corpus neighborhood.
- Semantic hit strength and corpus statistics determine candidate relevance.
- Modified Dice normalizes morphology/spelling, removes redundancy, and preserves phrase structure.
- BM25 applies lexical retrieval to the expanded query.
- RRF fuses original semantic and enriched lexical rankings.

Dice is not semantic. Low Dice must never reject an otherwise strong expansion such as `NSAID` for `ibuprofen`.

### Original Query Preservation

Append expansions to the original query; never replace original terms.

The expanded lexical query is request-local and must not overwrite the original query used by vector search, response metadata, Stage-2 reranking, cache lookup, logs, or traces.

### Exact Passage Preference

Prefer the exact passage represented by each semantic vector hit. Do not collapse raw vector IDs before source selection.

For `nodeID-chunk-N`:

1. Load the parent node.
2. Rebuild embedding text with `embeddingutil.BuildText(...)` and effective settings.
3. Use the same tokenizer, chunk size, and overlap.
4. Select the chunk encoded by the vector ID.
5. Cache reconstructed chunks with a content/configuration fingerprint.

Skip unresolved sources. Do not substitute an entire large node for a missing chunk.

### Expansion Gate

Support:

- `always`: run on every eligible query; best for controlled evaluation and one-BM25-search latency.
- `retrieval_disagreement`: run when bounded BM25/vector parent-node overlap is below a threshold.

Use Jaccard overlap:

$$
J(B,V)=\frac{|B\cap V|}{|B\cup V|}
$$

Behavior:

- no semantic results: ineligible;
- no BM25 results but semantic results exist: eligible;
- both empty: ineligible;
- overlap at or above threshold: skip expansion.

`retrieval_disagreement` may require baseline and expanded BM25 searches when expansion applies. Benchmark that tradeoff; do not assume the gate is cheaper. The feature-wide default remains disabled.

### Latency And Privacy

The `always` path performs one semantic search, bounded passage resolution, bounded local extraction/Dice work, and one BM25 search. It makes no model or network call and performs no whole-corpus scan.

Passage and term text must not enter normal logs, errors, or metric labels. Debug text requires explicit debug mode and existing sensitive-query controls.

## Configuration

| Variable                                              |  Type |                  Default | Purpose                                              |
| ----------------------------------------------------- | ----: | -----------------------: | ---------------------------------------------------- |
| `NORNICDB_SEARCH_QUERY_EXPANSION_ENABLED`             |  bool |                  `false` | Master feature flag.                                 |
| `NORNICDB_SEARCH_QUERY_EXPANSION_PROVIDER`            |  enum |         `dense_prf_dice` | Initial provider.                                    |
| `NORNICDB_SEARCH_QUERY_EXPANSION_GATE`                |  enum | `retrieval_disagreement` | `always` or `retrieval_disagreement`.                |
| `NORNICDB_SEARCH_QUERY_EXPANSION_GATE_TOP_K`          |   int |                     `10` | Ranking depth for gate overlap. Range `1..100`.      |
| `NORNICDB_SEARCH_QUERY_EXPANSION_GATE_MAX_OVERLAP`    | float |                   `0.25` | Expand below this Jaccard overlap. Range `0..1`.     |
| `NORNICDB_SEARCH_QUERY_EXPANSION_SOURCE_TOP_K`        |   int |                     `10` | Maximum semantic passages. Range `1..20`.            |
| `NORNICDB_SEARCH_QUERY_EXPANSION_MAX_CANDIDATES`      |   int |                    `256` | Candidates before final filtering. Range `16..2048`. |
| `NORNICDB_SEARCH_QUERY_EXPANSION_MAX_TERMS`           |   int |                     `10` | Maximum appended terms/phrases. Range `1..20`.       |
| `NORNICDB_SEARCH_QUERY_EXPANSION_MAX_PHRASE_WORDS`    |   int |                      `3` | Maximum phrase length. Range `1..3`.                 |
| `NORNICDB_SEARCH_QUERY_EXPANSION_MIN_PASSAGE_SUPPORT` |   int |                      `1` | Minimum distinct source passages. Range `1..20`.     |
| `NORNICDB_SEARCH_QUERY_EXPANSION_MIN_IDF`             | float |                      `0` | Optional minimum corpus IDF.                         |
| `NORNICDB_SEARCH_QUERY_EXPANSION_DICE_THRESHOLD`      | float |                   `0.85` | Variant grouping threshold. Range `0..1`.            |
| `NORNICDB_SEARCH_QUERY_EXPANSION_MAX_PASSAGE_CHARS`   |   int |                   `2048` | UTF-8-safe source bound. Range `128..16384`.         |
| `NORNICDB_SEARCH_QUERY_EXPANSION_PASSAGE_CACHE_SIZE`  |   int |                  `10000` | Bounded reconstructed chunk cache; zero disables.    |
| `NORNICDB_SEARCH_QUERY_EXPANSION_DEBUG`               |  bool |                  `false` | Bounded sanitized diagnostics.                       |

Do not expose fixed positive character/word Dice weights. Each component serves a specific comparison.

```yaml
search_query_expansion:
  enabled: false
  provider: dense_prf_dice
  gate: retrieval_disagreement
  gate_top_k: 10
  gate_max_overlap: 0.25
  source_top_k: 10
  max_candidates: 256
  max_terms: 10
  max_phrase_words: 3
  min_passage_support: 1
  min_idf: 0
  dice_threshold: 0.85
  max_passage_chars: 2048
  passage_cache_size: 10000
  debug: false
```

Environment variables override YAML through existing precedence. Per-database overrides use the existing effective environment mechanism.

## Architecture

```text
Original query + existing embedding
              |
              v
Existing semantic vector search
              |
       preserve raw passage IDs
              |
              +---------------------------+
              | original semantic ranking |
              v                           |
Resolve exact semantic passages           |
              |                           |
              v                           |
Extract unigrams/bigrams/trigrams          |
              |                           |
              v                           |
Rank by semantic evidence                  |
+ corpus IDF + passage support             |
              |                           |
              v                           |
Modified Dice normalization                |
+ redundancy/diversity filtering           |
              |                           |
              v                           |
original query + expansions                |
              |                           |
              v                           |
Existing BM25 search                       |
              |                           |
              +-------------+-------------+
                            v
                     Existing RRF
                            |
                            v
                 Existing MMR/reranker
```

## Implementation Approach

### 1. Add Provider-Neutral Contracts

Create `pkg/search/query_expansion.go`:

```go
type ExpansionSource struct {
    VectorID       string
    NodeID         string
    SemanticRank  int
    SemanticScore float64
    Text           string
}

type ExpansionCandidate struct {
    Text             string
    Tokens           []string
    BestSemanticRank int
    SemanticSupport  float64
    PassageSupport   int
    IDF              float64
    Score            float64
}

type QueryExpansionResult struct {
    Terms      []string
    Candidates int
    Sources    int
}

type QueryExpander interface {
    Name() string
    Expand(context.Context, string, []ExpansionSource) (QueryExpansionResult, error)
}
```

Implement `densePRFDiceExpander`. Output terms must be source-traceable, deterministic, bounded, and cancellation-aware. The interface must not imply attribution.

### 2. Preserve Raw Semantic Hits

Refactor vector retrieval to produce raw scored hits and existing collapsed node results from one search:

```go
type semanticRetrieval struct {
    Raw       []ScoredCandidate
    Collapsed []indexResult
}
```

Select sources in rank order, deduplicate vector IDs, prefer distinct parent nodes, apply authorization/type/property/decay visibility before hydration, and cap work before storage access.

### 3. Resolve Exact Passages

Create an injected `PassageResolver` and implement it in `pkg/nornicdb`, where embedder settings and node access are available.

The resolver must parse typed vector provenance, batch-load parent nodes, reconstruct deterministic chunks, preserve rank/score, truncate on UTF-8 boundaries, and skip invalid sources.

Use a bounded database-scoped LRU keyed by node ID, updated timestamp, embedding model, chunk settings, and embedding-text fingerprint. Reuse existing mutation invalidation chokepoints.

### 4. Extract Bounded Candidates

Reuse the BM25 tokenizer or a narrow shared normalization helper. Extract normalized unigrams and adjacent word bigrams/trigrams.

Rules:

- count passage support once per distinct source passage;
- retain bounded within-passage frequency;
- reject stop words, punctuation-only values, invalid UTF-8, and phrases bounded by stop words;
- stop at a bounded intermediate candidate limit;
- do not remove whitespace and generate character bigrams across word boundaries.

### 5. Rank Candidates Without Dice

Candidate relevance comes from semantic and corpus evidence:

$$
S(t,q)=\operatorname{IDF}(t)
\sum_{d\in D_q}
\frac{\max(\operatorname{sim}(q,d),0)}{\operatorname{rank}(d)}
I(t\in d)
$$

For phrases, derive distinctiveness from constituent token IDF using a documented stable rule. Apply a passage-support bonus:

```text
support_bonus = 1 + log1p(distinct_passage_count - 1)
final_score = base_score * support_bonus
```

Requirements:

- reject negative/non-finite semantic scores;
- duplicate chunks do not fake independent support;
- retain low-Dice candidates with strong semantic/corpus evidence;
- remove exact original-query candidates before Dice;
- sort deterministically;
- retain at most `MAX_CANDIDATES`.

This formula is a testable baseline, not a proven optimum.

### 6. Correct And Apply Modified Dice

Use independently normalized multiset Dice:

$$
D_c(a,b)=\frac{2|B_c(a)\cap B_c(b)|}{|B_c(a)|+|B_c(b)|}
$$

$$
D_w(a,b)=\frac{2|B_w(a)\cap B_w(b)|}{|B_w(a)|+|B_w(b)|}
$$

Both remain in $[0,1]$. Do not preserve the historical mixed numerator/denominator, which can exceed 1.

Select comparison by shape rather than using one global weight:

- single-token variants: character Dice within normalized tokens;
- multiword variants: lightly normalize/stem tokens, then word-bigram Dice;
- morphological phrase ties: aligned-token character Dice;
- missing word bigrams: no-comparison, not an ordinary zero;
- semantic equivalents: no Dice decision.

Character bigrams stay inside token boundaries. Word bigram keys preserve token boundaries. Both use multiset intersections.

Filtering sequence:

1. Process candidates in relevance order.
2. Drop lexical/morphological restatements of the query.
3. Compare with already selected candidates of compatible shape.
4. For variants, retain the candidate with stronger relevance evidence.
5. Keep low-Dice candidates when semantic evidence is strong.
6. Stop at `MAX_TERMS`.

Expected behavior:

```text
access token / access tokens       -> variant
adverse reaction / adverse reactions -> variant
ibuprofen / NSAID                  -> Dice does not reject
adverse reactions / adverse effects -> do not merge by Dice
```

### 7. Integrate Into Hybrid Search

Update `Service.rrfHybridSearch(...)`:

1. Snapshot configuration and dependencies under the existing short lock.
2. Run vector search once and preserve raw hits.
3. Apply source visibility filters.
4. Evaluate the gate.
5. Resolve bounded passages.
6. Extract and statistically rank candidates.
7. Apply Dice filtering.
8. Build a request-local lexical query.
9. Run BM25 according to gate mode.
10. Continue through existing RRF, MMR, reranking, and enrichment.

Applied expansion appends `+dense_prf_dice` to the search method. Skipped/failed expansion leaves the method unchanged and records a bounded reason.

Stage-2 reranking always receives the original query. BM25 alone receives expanded text.

Skip expansion for vector-only, BM25-only, disabled-index, no-source, and retrieval-agreement cases.

### 8. Reuse Cluster Profiles Carefully

Cluster lexical profiles are an optional prior, not a first-implementation dependency. Passage evidence is more query-specific.

If added, use cluster-discriminative weighting and evaluate passage-only against passage-plus-cluster behavior. Do not allow broad cluster vocabulary to overwhelm passage evidence.

### 9. Configuration, Cache, And Lifecycle

Add centralized validation and explicit dependency wiring. Disabled mode allocates no expander or passage cache. Configuration changes invalidate result caches.

Preserve the complete response cache. Node mutations invalidate response and passage caches. Never use expanded query or passage text as cache keys, and do not add an unbounded expansion cache.

### 10. Performance Rules

- Disabled mode is one predictable branch with no expansion allocations.
- No model call, health probe, network call, or retry.
- Never run semantic retrieval twice.
- Cap sources before hydration and batch node reads.
- Preallocate from configured limits.
- Avoid regex in bounded token/Dice loops.
- Avoid all-pairs document comparisons from the JavaScript prototype.
- Compare Dice only inside the bounded candidate set.
- Do not hold service/index/cache locks during reconstruction.
- Propagate cancellation.

Track vector, gate BM25, passage resolution, extraction/scoring, Dice, expanded BM25, fusion, and total timings separately.

Initial targets, requiring retained evidence:

- disabled p50/p95 stays within benchmark noise;
- `always` runs one vector and one BM25 search;
- cache-hit passage resolution p95 is below 1 ms for ten sources;
- bounded extraction/scoring/Dice p95 is below 1 ms;
- enabled end-to-end p95 increase is at most 5% or 5 ms, whichever is larger.

These are targets, not claims. Report hardware, corpus, cache state, and candidate strategy.

## Observability

Add bounded response metrics for total expansion, passage resolution, candidate extraction, Dice filtering, source/candidate/accepted counts, gate decision, applied status, and fallback reason.

Fixed statuses:

```text
disabled
ineligible
retrieval_agreement
no_sources
no_passages
no_candidates
no_terms
canceled
internal_error
applied
```

Do not put dynamic terms, queries, node IDs, or errors in metric labels.

## Negative Cases

### Candidate And Dice Safety

- identical multiword Dice never exceeds 1;
- repeated bigrams use multiset intersection;
- short tokens never divide by zero;
- one-word candidates are not penalized for missing word bigrams;
- no character bigrams cross word boundaries;
- duplicate chunks do not inflate independent support;
- low-Dice semantic expansions survive;
- semantic-but-lexically-distinct phrases are not merged;
- original-query morphological restatements are removed;
- maximum candidate work remains bounded;
- ties are deterministic.

### Passage And Search Safety

- deletion between retrieval and hydration;
- invalid or stale chunk provenance;
- changed embedding configuration;
- missing named/property source text;
- UTF-8-safe truncation;
- engines without batch get;
- empty query remains vector-only;
- missing embedding remains BM25-only;
- request filters apply before hydration;
- original query reaches vector search and optional reranker;
- expanded query reaches BM25 only;
- expansion never enables reranking;
- failures run ordinary hybrid search.

### Gate Safety

- both rankings empty;
- only BM25 empty;
- only vector empty;
- duplicate chunks collapse before overlap;
- exact-threshold behavior;
- `always` does not run baseline BM25 solely for gating;
- disagreement mode records a second BM25 lookup;
- ambiguous-query drift is measured.

## Testing Strategy

### Unit Tests

Create focused tests for:

- corrected character and word Dice;
- historical prototype regression where identical text exceeded 1;
- token-boundary and missing-component behavior;
- stemming/normalization of phrase variants;
- candidate extraction and statistical ranking;
- semantic-rank discount, IDF, and independent passage support;
- deterministic filtering and maximum bounds;
- cancellation and fail-open errors.

### Passage Resolver Tests

Cover main/chunk/named/property resolution, malformed IDs, hyphenated parent IDs, embedding settings, UTF-8 truncation, cache invalidation, batch/fallback reads, and concurrent invalidation.

### Hybrid Integration Tests

Core fixture:

```text
query:       ibuprofen side effects
semantic:    passage containing NSAID nausea dizziness
relevant:    BM25 document containing NSAID nausea, not original terms
noise:       passages containing common include general
```

Assert baseline BM25 misses, discriminative terms survive, generic scaffolding is rejected, expanded BM25 retrieves the document, vector search runs once, BM25 count matches gate mode, no reranker is called for expansion, and failures return baseline behavior.

Exercise HNSW, clustered, IVF-HNSW, IVFPQ, GPU, and CPU candidate strategies through the same expansion stage.

### Recall Benchmark

Add a runnable BEIR benchmark workflow dedicated to this experiment. This is an end-to-end retrieval benchmark, not a Go `BenchmarkXxx` microbenchmark or a set of hand-authored relevance cases.

Suggested surfaces:

- `cmd/recall-bench/` for prepare, index, run, compare, and report commands;
- `pkg/eval/ir/` for standard qrels parsing and metrics shared by in-process and HTTP runners;
- `testing/benchmarks/beir/manifest.yaml` for pinned datasets, checksums, splits, and profiles;
- `testing/benchmarks/beir/published-ce-qe.json` for values explicitly reported by the paper, with figure/table provenance;
- `scripts/benchmark_beir_query_expansion.sh` as the one-command entry point;
- `docs/performance/query-expansion-recall-benchmark.md` for requirements, disk estimates, commands, and interpretation.

Do not commit BEIR corpora, generated embeddings, database files, or query outputs. Store them under an ignored configurable data directory.

#### Paper-Aligned Dataset Matrix

Use the seven BEIR datasets listed in the CE-QE paper (`2608.00452v1.pdf`, Table 1):

| Dataset           | BEIR slug       | Documents | Domain                  | Official test queries |
| ----------------- | --------------- | --------: | ----------------------- | --------------------: |
| MSMARCO           | `msmarco`       |     8.84M | Web / QA                |                 6,980 |
| HotpotQA          | `hotpotqa`      |     5.23M | Wikipedia               |                 7,405 |
| Natural Questions | `nq`            |     2.68M | Wikipedia               |                 3,452 |
| FEVER             | `fever`         |     5.42M | Wikipedia fact checking |                 6,666 |
| Climate-FEVER     | `climate-fever` |     5.42M | Climate claims          |                 1,535 |
| TREC-COVID        | `trec-covid`    |      171K | Biomedical              |                    50 |
| FiQA-2018         | `fiqa`          |       57K | Finance                 |                   648 |

Download official BEIR archives, verify published checksums, and preserve `corpus.jsonl`, `queries.jsonl`, and `qrels/<split>.tsv` semantics. Record source URL, archive checksum, extracted-file fingerprints, license/citation metadata, and retrieval date in the run manifest. Require explicit license acknowledgement because BEIR does not grant rights to underlying datasets.

FEVER and Climate-FEVER share the same 5.42M-document Wikipedia corpus. Detect identical corpus fingerprints and allow prepared index/embedding artifacts to be reused without changing either dataset's queries or qrels.

#### Paper Protocol And Reproducibility Labels

The paper discloses:

- the seven datasets above;
- 1,000 queries per configuration;
- retrieval depth $k=100$;
- Recall@100 and nDCG@10;
- standard flat BM25;
- `granite-embedding-30m-english`;
- IVF-PQ in LanceDB or HNSW in OpenSearch;
- up to 10 semantic source documents and 10 expansion tokens.

Provide two benchmark labels:

- **paper-aligned**: uses every disclosed setting but records NornicDB-specific choices and unresolved details;
- **paper-reproduced**: allowed only when exact author query manifests, split choices, preprocessing, model revision, RRF settings, and index parameters are available and matched.

Do not call the initial result a paper reproduction. The paper does not disclose sampled query IDs/seed, how “1,000 queries” applies to TREC-COVID's 50 or FiQA's 648 test queries, exact RRF constant/weights, BM25 parameters/tokenization, model revision, document truncation/chunking, or full per-dataset tables. These omissions prevent strict reproduction from the PDF alone.

For paper-aligned runs:

- evaluate all official test queries when a dataset has 1,000 or fewer;
- select exactly 1,000 official test queries without replacement when more are available;
- sort query IDs before deterministic sampling;
- use a fixed documented seed;
- write selected IDs and their SHA-256 digest to a manifest;
- reuse the identical query manifest for every variant and repetition;
- never repeat 50 TREC-COVID queries to manufacture 1,000 queries.

Also support `--queries all` to report complete official test sets independently of the paper-aligned profile.

#### Benchmark Profiles

```text
smoke:
    datasets: scifact, fiqa, trec-covid
    corpus: complete
    queries: deterministic 25 per dataset
    purpose: correctness and local iteration, not paper comparison

paper-aligned:
    datasets: msmarco, hotpotqa, nq, fever, climate-fever, trec-covid, fiqa
    corpus: complete
    queries: deterministic min(1000, official test query count)
    top_k: 100
    purpose: quality comparison with disclosed paper protocol
```

Never evaluate a reduced corpus against full qrels and label it comparable. Small runs reduce query count only; they retain the complete corpus. `scifact` is a fast engineering dataset but was not used by the paper and must not enter paper-comparison averages.

#### Corpus Ingestion And Retrieval Identity

Index each BEIR document as one logical retrieval unit and retain its BEIR ID as evaluation identity. Store title and text in deterministic documented fields. If storage IDs cannot equal BEIR IDs, persist a lossless BEIR-ID-to-node-ID map.

Pin and record:

- NornicDB commit and dirty-tree state;
- dataset fingerprints and query-manifest hash;
- embedding model immutable revision/digest, dimensions, normalization, and tokenizer;
- document construction, truncation, and chunking;
- BM25 version and parameters;
- vector strategy and HNSW/IVF-PQ parameters;
- RRF constant, weights, candidate depths, and final depth;
- query-expansion configuration;
- hardware, OS, threads, and cache/warmup state.

Use `granite-embedding-30m-english` for the paper-aligned profile when NornicDB can run the exact pinned model. If unavailable, use another model only under a clearly labeled `nornic-native` profile; do not compare its absolute score directly with the paper as though the retriever were controlled.

Reuse the same prepared corpus and embeddings across all variants. Expansion changes must not trigger re-indexing or re-embedding.

#### Required Variants

Run every query through the same matrix:

1. `bm25`: flat BM25 only.
2. `vector`: semantic only.
3. `rrf`: current hybrid baseline.
4. `dense_prf`: statistical selection with Dice disabled.
5. `dense_prf_dice`: identical settings with corrected Dice enabled.
6. `dense_prf_dice_gate`: Dice plus retrieval-disagreement gate.
7. `ce_qe_reference`: optional, only with faithful attribution.

Disable Stage-2 reranking and MMR in variants 1-6 to isolate retrieval recall.

Rotate or deterministically randomize variant order per repetition so warming and thermal drift do not always favor the final variant. Run a warmup pass and at least three measured repetitions. Quality metrics must be identical across repetitions; latency uses measured repetitions only.

#### Standard Metric Correctness

Extend or replace current harness calculations for this runner:

- retain at least 100 result IDs;
- compute Recall@100 against all positive qrels;
- compute nDCG@10 using gain $2^{rel}-1$ and discount $\log_2(rank+1)$ with ideal grades sorted descending;
- support binary and graded qrels without collapsing grades;
- compute MRR@10 and MAP@100 as supporting metrics;
- macro-average each query exactly once;
- report dataset metrics before any cross-dataset average;
- validate fixtures against `pytrec_eval`, `trec_eval`, or the official BEIR evaluator.

Do not reuse current `cmd/eval` nDCG unchanged: it divides grade by rank and models the ideal list as all grade 1. Do not reuse its fixed result limit of 50 for Recall@100.

#### Paired Comparison Report

For each dataset and variant, emit:

- Recall@100, nDCG@10, MRR@10, and MAP@100;
- absolute and relative delta from `rrf`;
- absolute delta from `dense_prf` for Dice variants;
- improved, unchanged, regressed, and harmed-query counts;
- expansion application rate, selected-term count, and no-term rate;
- p50/p95/p99 end-to-end, extraction, and Dice latency;
- baseline and expanded BM25 execution counts;
- build time, corpus size, embedding time, and disk size separately from query latency.

Use paired bootstrap confidence intervals over query-level deltas with a fixed seed and at least 10,000 resamples. Report 95% intervals for Recall@100 and nDCG@10 deltas.

Write TREC run files, per-query JSONL, aggregate JSON/Markdown, configuration/environment manifests, selected query IDs, and raw latency samples or equivalent histograms. The comparison command must consume saved run files and qrels without rerunning search.

#### Published Paper Anchors

Store only values explicitly stated by the paper and identify scope:

| Comparison                  | Dataset       | Published Recall@100 |
| --------------------------- | ------------- | -------------------: |
| BM25 to CE-QE enriched BM25 | NQ            |       `0.32 -> 0.47` |
| BM25 to CE-QE enriched BM25 | TREC-COVID    |       `0.56 -> 0.67` |
| BM25 to CE-QE enriched BM25 | Climate-FEVER |       `0.19 -> 0.26` |
| BM25 to CE-QE enriched BM25 | FEVER         |       `0.71 -> 0.70` |
| BM25 to RRF                 | MSMARCO       |       `0.66 -> 0.80` |
| semantic to RRF             | NQ            |       `0.75 -> 0.92` |

Record aggregate statements separately: the paper states that “query expansion with cross-encoders” improves `6.6%` over RRF, SESF reports `2.5%` higher Recall@100 than cross-encoder score fusion, and Table 2 reports approximately `153.8 ms` for RRF and `556.9 ms` for SESF on MSMARCO. Do not assign the `6.6%` statement to a narrower variant than the paper does, and do not treat aggregate percentages or chart-read estimates as missing per-dataset ground truth.

Show paper anchors beside, not inside, NornicDB averages. Conclusions must be qualified:

```text
On the paper-aligned NQ workload, dense_prf_dice changed NornicDB RRF
Recall@100 by +X absolute at +Y ms p95. The CE-QE paper reported BM25
to CE-QE-enriched-BM25 changing from 0.32 to 0.47 under a different
retrieval stack. This is directional context, not a reproduced head-to-head result.
```

#### NornicDB-Specific Regression Suite

Retain a separate small suite of exact-match, synonym-gap, identifier/code, strong-BM25, ambiguous, long-query, and drift-prone cases. Do not mix it into BEIR averages.

#### Recall Benchmark Acceptance

The benchmark is complete when:

- one command verifies, prepares, indexes, runs, and reports a smoke dataset;
- all seven paper datasets are supported with resumable ingestion/embedding;
- variants use identical corpus, embeddings, qrels, query manifests, and depth;
- metrics match an independent standard evaluator;
- Recall@100 retrieves 100 results;
- run artifacts are deterministic apart from latency/timestamps;
- `dense_prf` versus `dense_prf_dice` isolates Dice;
- reports distinguish paper-aligned, paper-reproduced, smoke, and Nornic-native profiles;
- licensing and comparability caveats are visible;
- benchmark data and generated indexes stay out of Git.

## Validation Commands

```sh
go test ./pkg/search -run 'TestModifiedDice|TestDensePRF|TestRRFHybridSearch_QueryExpansion' -count=1
go test ./pkg/nornicdb -run 'TestQueryExpansionPassageResolver' -count=1
go test ./pkg/config/... -run 'Test.*QueryExpansion' -count=1
go test ./pkg/search -run '^$' -bench 'BenchmarkModifiedDice|BenchmarkDensePRF|BenchmarkRRFHybridQueryExpansion' -benchmem
go test ./pkg/search ./pkg/nornicdb ./pkg/config/... -race
go test ./...
go build ./cmd/nornicdb
go test ./pkg/eval/... ./cmd/recall-bench/... -count=1
scripts/benchmark_beir_query_expansion.sh smoke
scripts/benchmark_beir_query_expansion.sh paper-aligned --datasets trec-covid,fiqa
```

Retain benchmarks for disabled mode, `always`, both gate outcomes, passage-cache hit/miss, statistics without/with Dice, and maximum candidate bounds.

## Implementation Sequence

### Phase 1: Corrected Dice And Candidate Scoring

Implement pure bounded helpers and tests for normalization, both Dice forms, extraction, statistical ranking, and deterministic deduplication. Benchmark local computation independently.

### Phase 2: Configuration And Contracts

Implement centralized configuration, environment/YAML/per-database resolution, interfaces, and test doubles. Keep disabled behavior allocation-free.

### Phase 3: Semantic Passage Provenance

Preserve raw hits and implement vector-ID parsing, exact passage reconstruction, bounded caching, and invalidation.

### Phase 4: Hybrid Integration

Resolve passages, extract/rank candidates, filter with Dice, run bounded BM25, and continue through existing fusion and optional downstream stages.

### Phase 5: Gate, Observability, And Documentation

Implement disagreement gating, cache generation, metrics, diagnostics, user documentation, and architecture diagrams.

### Phase 6: BEIR Recall Benchmark

Implement the pinned downloader/manifest, resumable importer, standard metrics, variant runner, TREC export, paired comparison report, and smoke profile. Validate metrics independently before using results for product conclusions.

### Phase 7: Recall And Latency Proof

Run smoke validation, the seven-dataset paper-aligned matrix, ablations, latency benchmarks, race tests, and NornicDB-specific cases. Quantify Dice separately and report CE-QE values only as qualified context. Keep the feature experimental and disabled until evidence passes acceptance criteria.

## What Not To Do

- Do not call `dense_prf_dice` CE-QE.
- Do not claim cross-encoder-equivalent quality before evaluation.
- Do not replace semantic search with Dice.
- Do not put Dice in positive relevance scoring.
- Do not reject strong candidates because Dice is low.
- Do not merge merely semantic equivalents using Dice.
- Do not use guessed fixed Dice weights.
- Do not create character bigrams across words.
- Do not preserve the historical score-above-1 formula.
- Do not use BM25 hits as feedback sources.
- Do not generate terms with an LLM.
- Do not append entire passages.
- Do not run vector search twice or re-embed expansions.
- Do not invoke reranking to select terms.
- Do not mutate indexes or log sensitive text.
- Do not add unbounded caches or collections.
- Do not enable by default before evidence exists.

## Acceptance Criteria

The experiment is implementation-complete when:

- disabled mode preserves behavior and performance within benchmark noise;
- enabled mode uses the existing embedding and one semantic retrieval with no additional model call;
- candidates come only from eligible top semantic passages;
- managed chunks resolve to exact deterministic sources;
- semantic strength, IDF, and independent passage support rank candidates;
- Dice runs only after ranking for normalization and redundancy/diversity;
- Dice remains in $[0,1]$, preserves boundaries, and handles missing components;
- low-Dice semantic expansions remain eligible;
- original terms remain and bounded grounded terms are appended only to BM25;
- existing RRF, MMR, reranking, filtering, and enrichment remain downstream;
- failures and no-term outcomes continue through ordinary hybrid search;
- caches do not return stale results after configuration/node changes;
- benchmarks isolate statistics-only from statistics-plus-Dice;
- a runnable BEIR benchmark covers the paper's seven datasets at top-$k=100$ with deterministic query manifests;
- BEIR metric fixtures match an independent standard evaluator;
- evaluation reports Recall@100, nDCG@10, confidence intervals, latency, harmed-query rate, and drift;
- reports distinguish paper-aligned context from strict paper reproduction;
- no first-page regression is hidden by aggregate recall;
- tests, race checks, and production build pass;
- documentation describes a cheap experiment, not CE-QE equivalence.

## Release Decision

Promote beyond experimental status only when retained evidence shows meaningful Recall@100 improvement, no material nDCG@10 regression, bounded harmed-query rate, acceptable p50/p95 latency, and a positive incremental Dice contribution.

If dense feedback helps but Dice does not, ship the simpler dense PRF path without Dice. If the disagreement gate reduces harm without excessive duplicate BM25 cost, use it for enabled deployments. Both are evidence-driven outcomes.
