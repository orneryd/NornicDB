# Scale fine-grained search without rebuilding its corpus

Restricted search currently projects every indexed entity through FGAC and
constructs a new BM25 index for each keyword query. It also disables several
fast paths and can trigger broad vector scoring when candidate filtering
empties a page. On large databases this makes search time and temporary RAM
scale with the entire corpus.

Keep shared indexes and the existing security contract: unreadable entities
and properties cannot affect visible hits or BM25 scores; visible result pages
remain complete under the configured query limits. Make authorization-aware
scoring consume shared field-aware index data with bounded visibility metadata,
and make vector/relationship candidate completion avoid unnecessary full scans.
The unrestricted search path must retain its current cost profile.

This is a v1.5.3 release fix. It must preserve index restart and live update
behavior, policy and graph mutation invalidation, and cancellation.

## Ownership and limits

The shared BM25 index remains the sole postings and document owner.
`visibleFieldCorpus` holds only authorization masks and visible statistics;
`visibleBM25` remains a correctness fallback for old or noncomposable field
provenance. Both use the same `bm25Ranker` read contract but have different
storage ownership. `searchableFieldTexts` and `searchableText` share
`visitSearchableFields` so they use the same property selection. Indexed
vector candidates use authorized node batches for repeated owners and a
revisioned admission cache for repeated searches. Exact
vector page completion and relationship vector ranking still examine their
indexed population to preserve complete result pages; those paths require
a separate measured change before switching their candidate strategy.
