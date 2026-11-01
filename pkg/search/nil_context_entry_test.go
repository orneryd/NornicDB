package search

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// These entry paths already normalized nil before lifecycle admission was
// added. Keep normalization at those boundaries rather than broadening the
// admission owner's contract for every API.
func TestSearchNilContextEntries(t *testing.T) {
	t.Run("clustering", func(t *testing.T) {
		f := newCGFixture(t, 4, false)
		require.NoError(t, f.svc.TriggerClustering(nil))
		assertClusterGenerationCoherent(t, f.svc, cgAllChecks)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		require.ErrorIs(t, f.svc.TriggerClustering(ctx), context.Canceled)
	})
	t.Run("vector queries", func(t *testing.T) {
		svc := newOptionsTestService(t, 2, pointNode("one", []float32{1, 0}, nil))
		require.NoError(t, svc.BuildIndexes(t.Context()))
		spec := VectorQuerySpec{Property: "default", Limit: 1, ScoreScale: VectorScoreSimilarity}
		want, err := svc.VectorQueryNodes(t.Context(), []float32{1, 0}, spec)
		require.NoError(t, err)
		require.Len(t, want, 1)
		got, err := svc.VectorQueryNodes(nil, []float32{1, 0}, spec)
		require.NoError(t, err)
		require.Equal(t, want, got)
		got, err = svc.VectorQueryNodesWithOptions(nil, []float32{1, 0}, spec, nil)
		require.NoError(t, err)
		require.Equal(t, want, got)
		require.NoError(t, svc.IndexEdge(&storage.Edge{ID: "edge", StartNode: "one", EndNode: "one", Type: "R", Properties: map[string]any{"emb": []float32{1, 0}}}))
		rspec := RelationshipVectorQuerySpec{Type: "R", Property: "emb", Limit: 1}
		rwant, err := svc.VectorQueryRelationships(t.Context(), []float32{1, 0}, rspec)
		require.NoError(t, err)
		require.Len(t, rwant, 1)
		rgot, err := svc.VectorQueryRelationships(nil, []float32{1, 0}, rspec)
		require.NoError(t, err)
		require.Equal(t, rwant, rgot)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_, err = svc.VectorQueryNodes(ctx, []float32{1, 0}, spec)
		require.ErrorIs(t, err, context.Canceled)
		_, err = svc.VectorQueryRelationships(ctx, []float32{1, 0}, rspec)
		require.ErrorIs(t, err, context.Canceled)
	})
}
