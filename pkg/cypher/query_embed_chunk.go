package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/math/vector"
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
	if embedder == nil {
		return nil, localizedError(localization.CypherCoreEmbedderNotConfigured(), nil)
	}

	const (
		queryChunkSize    = 512
		queryChunkOverlap = 50
		maxQueryChunks    = 32
	)

	chunks, err := embedder.ChunkText(text, queryChunkSize, queryChunkOverlap)
	if err != nil {
		return nil, err
	}
	if len(chunks) > maxQueryChunks {
		chunks = chunks[:maxQueryChunks]
	}
	if len(chunks) <= 1 {
		return embedder.Embed(ctx, text)
	}

	var (
		sum      []float32
		count    int
		firstErr error
	)
	for _, c := range chunks {
		emb, err := embedder.Embed(ctx, c)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if len(emb) == 0 {
			continue
		}
		if sum == nil {
			sum = make([]float32, len(emb))
		}
		if len(emb) != len(sum) {
			continue
		}
		for i := range emb {
			sum[i] += emb[i]
		}
		count++
	}

	if count == 0 {
		if firstErr != nil {
			return nil, firstErr
		}
		return nil, localizedError(localization.CypherCoreEmbeddingNoOutput(), nil)
	}

	inv := float32(1.0 / float32(count))
	for i := range sum {
		sum[i] *= inv
	}
	vector.NormalizeInPlace(sum)

	return sum, nil
}
