package search

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The Cypher-style relationship query scores on Neo4j's scale (#907): a
// relationship whose vector isn't valid for cosine (a zero vector) is no
// hit, and the others score max((1 + cos) / 2, 0).
func TestVectorQueryRelationshipsNeo4jScale(t *testing.T) {
	t.Parallel()

	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 2)
	for id, embedding := range map[string][]float32{"zero": {0, 0}, "same": {1, 0}, "opposite": {-1, 0}} {
		require.NoError(t, svc.IndexEdge(&storage.Edge{
			ID: storage.EdgeID(id), StartNode: "a", EndNode: "b", Type: "R",
			Properties: map[string]any{"emb": embedding},
		}))
	}
	hits, err := svc.VectorQueryRelationships(context.Background(), []float32{1, 0}, RelationshipVectorQuerySpec{
		Type: "R", Property: "emb", Similarity: "cosine", Limit: 5,
	})
	require.NoError(t, err)
	scores := map[string]float64{}
	for _, hit := range hits {
		scores[hit.ID] = hit.Score
	}
	require.Equal(t, map[string]float64{"same": 1, "opposite": 0}, scores)
}

// The cheap ranking and the exact float32 scores can disagree on a near-tie:
// the hits come back in the exact scores' order (#907).
func TestVectorQueryRelationshipsExactOrderOnNearTies(t *testing.T) {
	t.Parallel()

	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 3)
	for id, embedding := range map[string][]float32{
		"ranked-first": {-0.23, -0.64, 0.09},
		"scored-first": {-0.3818, -1.0624, 0.1494},
	} {
		require.NoError(t, svc.IndexEdge(&storage.Edge{
			ID: storage.EdgeID(id), StartNode: "a", EndNode: "b", Type: "R",
			Properties: map[string]any{"emb": embedding},
		}))
	}
	query := []float32{0.3, 0.7, 0.2}
	prepared := newCypherVectorQuery("cosine", query)
	require.Greater(t, prepared.rank([]float32{-0.23, -0.64, 0.09}), prepared.rank([]float32{-0.3818, -1.0624, 0.1494}))
	hits, err := svc.VectorQueryRelationships(context.Background(), query, RelationshipVectorQuerySpec{
		Type: "R", Property: "emb", Similarity: "cosine", Limit: 2,
	})
	require.NoError(t, err)
	require.Len(t, hits, 2)
	require.Equal(t, "scored-first", hits[0].ID)
	require.Equal(t, "ranked-first", hits[1].ID)
	require.Greater(t, hits[0].Score, hits[1].Score)
}
