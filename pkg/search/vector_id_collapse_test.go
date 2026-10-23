package search

import (
	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestVectorSearchCandidates_CollapsesChunkIDsToNodeID_MaxScore(t *testing.T) {
	ctx := context.Background()
	engine := newTestMemoryEngine(t)
	svc := NewServiceWithDimensions(engine, 3)

	node := &storage.Node{
		ID:     "nornic:doc1",
		Labels: []string{"Doc"},
		Properties: map[string]any{
			"title": "doc1",
		},
		ChunkEmbeddings: [][]float32{
			{1, 0, 0}, // main
			{0, 1, 0}, // chunk 1
		},
	}
	_, err := engine.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, svc.IndexNode(node))

	cands, err := svc.VectorSearchCandidates(ctx, []float32{0, 1, 0}, &SearchOptions{Limit: 1})
	require.NoError(t, err)
	require.Len(t, cands, 1)
	require.Equal(t, "nornic:doc1", cands[0].ID)
	require.InDelta(t, 1.0, cands[0].Score, 1e-6)
}

func TestVectorSearchCandidates_CollapsesNamedIDsToNodeID(t *testing.T) {
	ctx := context.Background()
	engine := newTestMemoryEngine(t)
	svc := NewServiceWithDimensions(engine, 3)

	node := &storage.Node{
		ID:     "nornic:n1",
		Labels: []string{"Vec"},
		Properties: map[string]any{
			"title": "n1",
		},
		NamedEmbeddings: map[string][]float32{
			"default": {0, 1, 0},
		},
	}
	_, err := engine.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, svc.IndexNode(node))

	cands, err := svc.VectorSearchCandidates(ctx, []float32{0, 1, 0}, &SearchOptions{Limit: 1})
	require.NoError(t, err)
	require.Len(t, cands, 1)
	require.Equal(t, "nornic:n1", cands[0].ID)
	require.InDelta(t, 1.0, cands[0].Score, 1e-6)
}

func TestSearch_VectorOnly_ResolvesNamedEmbeddingsToNode(t *testing.T) {
	ctx := context.Background()
	engine := newTestMemoryEngine(t)
	svc := NewServiceWithDimensions(engine, 3)

	node := &storage.Node{
		ID:     "nornic:named-only",
		Labels: []string{"Vec"},
		Properties: map[string]any{
			"title": "named-only",
		},
		NamedEmbeddings: map[string][]float32{
			"default": {0, 1, 0},
		},
	}
	_, err := engine.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, svc.IndexNode(node))

	resp, err := svc.Search(ctx, "", []float32{0, 1, 0}, &SearchOptions{Limit: 1})
	require.NoError(t, err)
	require.Len(t, resp.Results, 1)
	require.Equal(t, "nornic:named-only", resp.Results[0].ID)
	require.Equal(t, storage.NodeID("nornic:named-only"), resp.Results[0].NodeID)
}
