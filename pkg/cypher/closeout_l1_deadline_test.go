package cypher

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type closeoutDeadlineReader struct {
	storage.Engine
	got []time.Time
}

func (s *closeoutDeadlineReader) GetNode(ctx context.Context, id storage.NodeID) (*storage.Node, error) {
	deadline, _ := ctx.Deadline()
	s.got = append(s.got, deadline)
	return s.Engine.GetNode(ctx, id)
}

func TestCloseoutDirectGDSNodeCarriesDeadline(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "nornic")
	_, err := store.CreateNode(t.Context(), &storage.Node{ID: "x", Properties: map[string]any{"id": "x"}})
	require.NoError(t, err)
	reader := &closeoutDeadlineReader{Engine: store}
	exec := NewStorageExecutor(reader, QueryCachePolicy{})
	const query = "RETURN gds.util.asNode('nornic:x').id"
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	deadline, _ := ctx.Deadline()
	result, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Equal(t, [][]any{{"x"}}, result.Rows)
	require.NotEmpty(t, reader.got)
	for _, got := range reader.got {
		require.Equal(t, deadline, got, "the point read receives the statement deadline")
	}
	reader.got = nil
	result, err = exec.Execute(context.Background(), query, nil)
	require.NoError(t, err)
	require.Equal(t, [][]any{{"x"}}, result.Rows)
	require.NotEmpty(t, reader.got)
	for _, got := range reader.got {
		require.True(t, got.IsZero(), "the control starts without a deadline")
	}
}
