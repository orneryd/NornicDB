# N152-138: preserve continuation-owned rerank windows

## Outcome and scope

Ranked `db.rretrieve` continuation must return every eligible synthetic note unless the caller sets an output ceiling. Each candidate reaches the provider once, in bounded windows, and each admitted window retains the provider order across pages. Global score monotonicity across different windows is not the contract.

This change is limited to the Cypher adapter and public regression tests. It changes neither ranking algorithms nor stream lifecycle. Parent owns exact-fixture validation, release verification, commit and publication.

## Root cause

At base `f77ec7706de099588ac232b32b6908860511d3da`, `chunkSearchOptions` disables `RerankEnabled` for continuation retrieval because `startTextContinuation` reranks after chunk fusion. The `runSearchRequest` callback in `pkg/cypher/call_rag.go` then unconditionally re-enables it for configured reranking. Consequently `applyStage2Rerank` applies the one-shot TopK population bound and result limit before the continuation producer sees the results. Retrieval exhaustion was computed before that truncation, so the producer can correctly consume its inputs yet terminate with missing rows. Provider memo admissions alone do not establish delivery.

The direct supplied-embedding branch passes `service.Search` without this callback and already preserves the continuation switch. `db.retrieve` does not request configured reranking. Those are controls, not additional owners to modify.

## Correction

Retain the callback's configured-provider lookup only for non-continuation calls. Request-level configuration already enables the continuation producer before it clones per-chunk options. Keep lazy service creation and the one-shot lookup unchanged. No new helper or duplicate ranking implementation is needed.

## Public proof

A small deterministic fixture has 12 synthetic notes with equal retrieval vectors and a reversing provider. It uses existing repository storage, embedder and parser fixtures. Retrieval depth 3, page size 2 and provider TopK 4 force expansions and partial pages. Exercise embedded and multi-chunk requests, supplied embeddings, plain retrieval, and a caller ceiling. Assert unique complete delivery, bounded once-only provider inputs, retained window order, no fallback and truthful completion. Run both parsers. Preserve actual RED and GREEN results separately; do not infer a pass from compilation.

Parent's separate 150-note fixture and its logs remain outside this public regression. This proposal contains no fixture content or private query text.

## Reuse review

`applyContinuationRerank` and `applyStage2Rerank` both reuse `runRerank`, but implement different output contracts: progressive windows versus a one-shot TopK population. The error is the adapter invoking both owners, not missing shared logic. Preserve both existing consumers and remove the incorrect invocation. Existing `rerankTruthfulnessEmbedder`, `forEachParser`, namespaced memory store and service fixtures are reused. The small recording provider adds public-boundary once/order assertions unavailable from the fixed-output `malformedReranker`; the search package's window recorder is package-private and cannot be imported by Cypher tests. Graph/checker status must be reported independently; manual source comparison is not a checker pass.
