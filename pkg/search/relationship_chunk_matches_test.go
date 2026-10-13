package search

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/embeddingutil"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A relationship's chunk matches give the property and span its embedding
// placed each chunk in, as a node's do, also after its vectors are saved and
// loaded; spans are dropped when a chunk lost its vector.
func TestRelationshipChunkMatchesCarryTheirSpans(t *testing.T) {
	ctx := context.Background()
	engine := relationshipTestEngine(t)
	embed := func(_ context.Context, edge *storage.Edge) (RelationshipEmbedding, error) {
		switch edge.ID {
		case "r1":
			return RelationshipEmbedding{
				Vectors: [][]float32{{1, 0, 0}, {0, 1, 0}},
				Spans:   []embeddingutil.ChunkSpan{{Chunk: 0, Property: "reason", Start: 0, End: 10}, {Chunk: 1, Property: "reason", Start: 11, End: 17}},
			}, nil
		case "r2":
			// The second chunk has no vector: its span can't be named.
			return RelationshipEmbedding{
				Vectors: [][]float32{{1, 0, 0}, nil},
				Spans:   []embeddingutil.ChunkSpan{{Chunk: 0, Property: "reason", Start: 0, End: 7}},
			}, nil
		}
		return RelationshipEmbedding{}, nil
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "bm25")
	svc := startRelationshipService(t, engine, embed, func(s *Service) {
		s.SetPersistenceEnabled(true)
		s.SetFulltextIndexPath(path)
	})
	waitRelationshipVectors(t, svc, 2)

	response, err := svc.SearchRelationships(ctx, "nothing", []float32{1, 0, 0}, &SearchOptions{Limit: 5})
	require.NoError(t, err)
	byID := map[string][]SearchMatch{}
	for _, result := range response.Results {
		byID[result.ID] = result.Matches
	}
	require.Equal(t, []SearchMatch{{Kind: SearchMatchChunk, Chunk: 0, Score: 1, Property: "reason", Start: 0, End: 10}}, byID["r1"][:1])
	require.Equal(t, "", byID["r2"][0].Property)

	// Saved and loaded with the vectors.
	_, err = os.Stat(path + relationshipVectorsFileSuffix)
	require.NoError(t, err)
	loaded := svc.loadRelationshipVectors()
	require.Equal(t, []embeddingutil.ChunkSpan{{Chunk: 0, Property: "reason", Start: 0, End: 10}, {Chunk: 1, Property: "reason", Start: 11, End: 17}}, loaded["r1"].Spans)
	require.Nil(t, loaded["r2"].Spans)
}
