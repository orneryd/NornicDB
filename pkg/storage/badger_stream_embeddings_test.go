package storage_test

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestBadgerEngine_StreamNodes_LoadsSeparatelyStoredEmbeddings(t *testing.T) {
	engine, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })

	// Force separate embedding storage by exceeding the inline node size threshold.
	large := strings.Repeat("x", 60*1024)
	_, err = engine.CreateNode(&storage.Node{
		ID:     storage.NodeID("nornic:big"),
		Labels: []string{"Doc"},
		Properties: map[string]any{
			"content": large,
		},
		ChunkEmbeddings: [][]float32{
			{1, 0, 0},
			{0, 1, 0},
		},
	})
	require.NoError(t, err)

	// Sanity check: GetNode loads embeddings.
	got, err := engine.GetNode(storage.NodeID("nornic:big"))
	require.NoError(t, err)
	require.Len(t, got.ChunkEmbeddings, 2)
	require.Len(t, got.ChunkEmbeddings[0], 3)

	// StreamNodes should also load embeddings (used by index rebuild paths).
	var streamed *storage.Node
	err = engine.StreamNodes(context.Background(), func(n *storage.Node) error {
		if n.ID == storage.NodeID("nornic:big") {
			streamed = n
			return storage.ErrIterationStopped
		}
		return nil
	})
	require.NoError(t, err)
	require.NotNil(t, streamed)
	require.Len(t, streamed.ChunkEmbeddings, 2)
	require.Len(t, streamed.ChunkEmbeddings[0], 3)
}

func TestBadgerEngine_StreamNodeChunks_LoadsSeparatelyStoredEmbeddings(t *testing.T) {
	engine, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })

	large := strings.Repeat("x", 60*1024)
	_, err = engine.CreateNode(&storage.Node{
		ID:     storage.NodeID("nornic:big"),
		Labels: []string{"Doc"},
		Properties: map[string]any{
			"content": large,
		},
		ChunkEmbeddings: [][]float32{
			{1, 0, 0},
			{0, 1, 0},
		},
	})
	require.NoError(t, err)

	var streamed *storage.Node
	err = engine.StreamNodeChunks(context.Background(), 10, func(nodes []*storage.Node) error {
		for _, n := range nodes {
			if n.ID == storage.NodeID("nornic:big") {
				streamed = n
				return storage.ErrIterationStopped
			}
		}
		return nil
	})
	require.NoError(t, err)
	require.NotNil(t, streamed)
	require.Len(t, streamed.ChunkEmbeddings, 2)
	require.Len(t, streamed.ChunkEmbeddings[0], 3)
}
