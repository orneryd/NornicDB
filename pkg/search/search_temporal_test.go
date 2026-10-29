package search

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestSearchService_BuildIndexesSkipsHistoricalTemporalVersions(t *testing.T) {
	base, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, base.Close())
	})
	ns := storage.NewNamespacedEngine(base, "test")
	require.NoError(t, base.GetSchemaForNamespace("test").AddConstraint(storage.Constraint{
		Name:       "fact_temporal",
		Type:       storage.ConstraintTemporal,
		Label:      "FactVersion",
		Properties: []string{"fact_key", "valid_from", "valid_to"},
	}))

	v1Start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	v1End := time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)
	v2Start := time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)
	_, err = ns.CreateNode(&storage.Node{
		ID:     "fact-v1",
		Labels: []string{"FactVersion"},
		Properties: map[string]any{
			"fact_key":   "search-current",
			"valid_from": v1Start,
			"valid_to":   v1End,
			"title":      "historical",
		},
		ChunkEmbeddings: [][]float32{{1, 0, 0}},
	})
	require.NoError(t, err)
	_, err = ns.CreateNode(&storage.Node{
		ID:     "fact-v2",
		Labels: []string{"FactVersion"},
		Properties: map[string]any{
			"fact_key":   "search-current",
			"valid_from": v2Start,
			"valid_to":   nil,
			"title":      "current",
		},
		ChunkEmbeddings: [][]float32{{0, 1, 0}},
	})
	require.NoError(t, err)

	svc := NewService(ns, 3, nil)
	require.NoError(t, svc.BuildIndexes(context.Background()))
	require.Equal(t, 1, svc.EmbeddingCount())
}

func TestSearchService_IndexNodeRemovesClosedTemporalVersion(t *testing.T) {
	base, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, base.Close())
	})
	ns := storage.NewNamespacedEngine(base, "test")
	require.NoError(t, base.GetSchemaForNamespace("test").AddConstraint(storage.Constraint{
		Name:       "fact_temporal",
		Type:       storage.ConstraintTemporal,
		Label:      "FactVersion",
		Properties: []string{"fact_key", "valid_from", "valid_to"},
	}))

	start := time.Now().UTC().Add(-2 * time.Hour)
	node := &storage.Node{
		ID:     "fact-live",
		Labels: []string{"FactVersion"},
		Properties: map[string]any{
			"fact_key":   "search-live",
			"valid_from": start,
			"valid_to":   nil,
		},
		ChunkEmbeddings: [][]float32{{1, 0, 0}},
	}
	_, err = ns.CreateNode(node)
	require.NoError(t, err)

	svc := NewService(ns, 3, nil)
	require.NoError(t, svc.IndexNode(node))
	require.Equal(t, 1, svc.EmbeddingCount())

	closed := storage.CopyNode(node)
	closed.Properties["valid_to"] = time.Now().UTC().Add(-time.Hour)
	require.NoError(t, ns.UpdateNode(closed))
	require.NoError(t, svc.IndexNode(closed))
	require.Equal(t, 0, svc.EmbeddingCount())

	// A historical version remains excluded even when its old model's
	// embedding dimension differs from the current search index.
	svc.FixVectorDimensions()
	closed.ChunkEmbeddings = [][]float32{{1, 0}}
	require.NoError(t, svc.IndexNode(closed))
	require.Zero(t, svc.EmbeddingCount())
}
