package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/math/vector"
	"github.com/orneryd/nornicdb/pkg/search"
)

// embedQueryChunked embeds a potentially long string query safely by chunking it
// into embedding-friendly pieces and returning the normalized average embedding.
//
// This is used by Cypher vector procedures that accept string queries, e.g.:
//   - CALL db.index.vector.queryNodes(..., "search text")
//   - CALL db.index.vector.queryRelationships(..., "search text")
//
// The Cypher layer uses a minimal QueryEmbedder interface, so chunking and
// sequential embedding stay provider-specific without importing embed.
func embedQueryChunked(ctx context.Context, embedder QueryEmbedder, text string) ([]float32, error) {
	probe := contextPipelineProbe(ctx)
	if embedder == nil {
		return nil, localizedError(localization.CypherCoreEmbedderNotConfigured(), nil)
	}

	const (
		queryChunkSize    = 512
		queryChunkOverlap = 50
	)

	chunks, err := embedder.ChunkText(text, queryChunkSize, queryChunkOverlap)
	if err != nil {
		return nil, err
	}
	// The rows (node, score) have no place to report a query searched in
	// part, so the whole text is embedded or the call fails: over
	// NORNICDB_SEARCH_QUERY_MAX_CHUNKS chunks whatever
	// NORNICDB_SEARCH_QUERY_CHUNK_OVERFLOW says, and on a chunk whose
	// embedding fails, is empty or has another dimension than the first's.
	if err := search.RequireAllQueryChunks(chunks); err != nil {
		return nil, err
	}
	if len(chunks) <= 1 {
		return embedder.Embed(ctx, text)
	}

	var sum []float32
	for index, c := range chunks {
		emb, err := embedder.Embed(ctx, c)
		if err != nil {
			return nil, err
		}
		if len(emb) == 0 {
			return nil, localizedError(localization.CypherCoreEmbeddingNoOutput(), nil)
		}
		if sum == nil {
			sum = make([]float32, len(emb))
		}
		if len(emb) != len(sum) {
			return nil, localizedError(localization.SearchQueryChunkDimensions(index+1, len(emb), len(sum)), nil)
		}
		for i := range emb {
			if probe.stopped() {
				return nil, probe.failed()
			}
			sum[i] += emb[i]
		}
	}

	inv := float32(1.0 / float32(len(chunks)))
	for i := range sum {
		if probe.stopped() {
			return nil, probe.failed()
		}
		sum[i] *= inv
	}
	vector.NormalizeInPlace(sum)

	return sum, nil
}
