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
	waitRelationshipVectors(t, svc, 2, relationshipSnapshotPublished)
	_, err = os.Stat(path + relationshipVectorsFileSuffix)
	require.NoError(t, err)
	loaded, err := svc.loadRelationshipVectors()
	require.NoError(t, err)
	require.Equal(t, []embeddingutil.ChunkSpan{{Chunk: 0, Property: "reason", Start: 0, End: 10}, {Chunk: 1, Property: "reason", Start: 11, End: 17}}, loaded["r1"].Spans)
	require.Nil(t, loaded["r2"].Spans)
}

// The chunk-match cap counts chunks, not spans: a chunk across two
// properties takes one place.
func TestRelationshipChunkMatchCapCountsChunks(t *testing.T) {
	spans := []embeddingutil.ChunkSpan{{Chunk: 0, Property: "a", Start: 0, End: 3}, {Chunk: 0, Property: "b", Start: 0, End: 2}, {Chunk: 1, Property: "b", Start: 3, End: 5}}
	matches := relationshipChunkMatches([]float64{0.9, 0.8, 0.7, 0.6}, spans, 0.5)
	chunks := map[int]bool{}
	for _, match := range matches {
		chunks[match.Chunk] = true
	}
	require.Len(t, chunks, maxChunkMatchesPerHit)
	require.Len(t, matches, maxChunkMatchesPerHit+1)
}

// A vector file of an earlier format (without chunk spans) is dropped, so
// its relationships are embedded again.
func TestRelationshipVectorFileOfAnEarlierFormatIsDropped(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bm25")
	file, err := os.Create(path + relationshipVectorsFileSuffix)
	require.NoError(t, err)
	old := relationshipVectorsFile{Format: 1, Entries: map[string]*relationshipEntry{"r1": {Source: 1, Vectors: [][]float32{{1, 0, 0}}}}}
	require.NoError(t, encodeRelationshipSnapshot(file, &old))
	require.NoError(t, file.Close())
	svc := NewService(relationshipTestEngine(t), 3, nil)
	t.Cleanup(func() { _ = svc.Close() })
	svc.SetPersistenceEnabled(true)
	svc.SetFulltextIndexPath(path)
	loaded, err := svc.loadRelationshipVectors()
	require.NoError(t, err)
	require.Nil(t, loaded)
}
