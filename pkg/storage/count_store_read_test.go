package storage

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCountStoreReadIncludesVisibilityInEveryFamily(t *testing.T) {
	ctx := context.Background()
	engine := createTestBadgerEngine(t)
	for _, node := range []*Node{
		{ID: "db:a", Labels: []string{"L"}},
		{ID: "db:b", Labels: []string{"L"}},
		{ID: "db:hidden", Labels: []string{"L", "_internal"}},
	} {
		_, err := engine.CreateNode(ctx, node)
		require.NoError(t, err)
	}
	for _, edge := range []*Edge{
		{ID: "db:e1", StartNode: "db:a", EndNode: "db:b", Type: "FIRST"},
		{ID: "db:e2", StartNode: "db:hidden", EndNode: "db:b", Type: "SECOND"},
	} {
		require.NoError(t, engine.CreateEdge(ctx, edge))
	}
	cases := []struct {
		request CountStoreReadRequest
		count   int64
	}{
		{CountStoreReadRequest{Kind: CountStoreNodesByLabel, Name: "L"}, 3},
		{CountStoreReadRequest{Kind: CountStoreRelationshipsByType}, 2},
		{CountStoreReadRequest{Kind: CountStoreRelationshipsByType, Types: []string{"FIRST", "FIRST"}}, 1},
		{CountStoreReadRequest{Kind: CountStoreRelationshipsByStartLabel, Name: "L", Types: []string{"FIRST", "SECOND"}}, 2},
		{CountStoreReadRequest{Kind: CountStoreRelationshipsByEndLabel, Name: "L", Types: []string{"FIRST", "SECOND"}}, 2},
	}
	for _, tc := range cases {
		request := tc.request
		request.Namespace = "db"
		request.HiddenLabelPrefix = "_"
		answer, err := engine.ReadCountStore(ctx, request)
		require.NoError(t, err)
		require.Equal(t, tc.count, answer.Count)
		require.Equal(t, request.Kind == CountStoreRelationshipsByType && len(request.Types) == 2, answer.DescribesRows)
		request.HiddenLabelPrefix = ""
		answer, err = engine.ReadCountStore(ctx, request)
		require.NoError(t, err)
		require.Equal(t, tc.count, answer.Count)
		require.True(t, answer.DescribesRows)
		request.Namespace = "other"
		request.HiddenLabelPrefix = "_"
		answer, err = engine.ReadCountStore(ctx, request)
		require.NoError(t, err)
		require.Zero(t, answer.Count)
		require.True(t, answer.DescribesRows)
	}
	require.NoError(t, engine.DeleteEdge(ctx, "db:e2"))
	require.NoError(t, engine.DeleteNode(ctx, "db:hidden"))
	answer, err := engine.ReadCountStore(ctx, CountStoreReadRequest{Kind: CountStoreNodesByLabel, Namespace: "db", Name: "L", HiddenLabelPrefix: "_"})
	require.NoError(t, err)
	require.True(t, answer.DescribesRows)
	require.Equal(t, int64(2), answer.Count)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = engine.ReadCountStore(canceled, CountStoreReadRequest{Kind: CountStoreNodesByLabel, Namespace: "db", Name: "L", HiddenLabelPrefix: "_"})
	require.ErrorIs(t, err, context.Canceled)
}
