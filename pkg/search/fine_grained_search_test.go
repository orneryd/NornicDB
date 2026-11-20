package search

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/resultstream"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestFineGrainedSearchUsesProjectedCorpusAndInvalidatesPages(t *testing.T) {
	ctx := t.Context()
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	store := storage.NewNamespacedEngine(base, "work")
	for _, node := range []*storage.Node{
		{ID: "one", Labels: []string{"Article"}, Properties: map[string]any{"content": "albatross", "draft": "forbiddenword"}},
		{ID: "two", Labels: []string{"Article"}, Properties: map[string]any{"content": "albatross"}},
		{ID: "three", Labels: []string{"Article"}, Properties: map[string]any{"content": "albatross"}},
		{ID: "hidden", Labels: []string{"Internal"}, Properties: map[string]any{"content": "forbiddenword"}},
	} {
		_, err := store.CreateNode(ctx, node)
		require.NoError(t, err)
	}
	service := NewService(store, 3, nil)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	service.SetIndexFlags(true, false)
	require.NoError(t, service.BuildIndexes(ctx))

	policy, err := auth.CompileFineGrainedPolicy(auth.GraphFineGrainedActions(), auth.FineGrainedCatalog{
		Definitions: []auth.FineGrainedEntitlement{
			{ID: "work", Action: "database.access", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedDatabase}},
			{ID: "articles", Action: "graph.node.traverse", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode, Types: []string{"Article"}}},
			{ID: "content", Action: "graph.node.property.read", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode, Properties: []string{"content"}}},
		},
		Assignments: map[string][]string{"reader": {"work", "articles", "content"}},
	})
	require.NoError(t, err)
	request := auth.FineGrainedRequest{
		Access: policy.ForRoles([]string{"reader"}), DatabaseID: func(string) string { return "work-id" }, Fingerprint: "policy-one",
	}
	scoped := auth.WithFineGrainedRequest(ctx, &request)
	raw, err := service.Search(ctx, "forbiddenword", nil, nil)
	require.NoError(t, err)
	require.NotEmpty(t, raw.Results)
	masked, err := service.Search(scoped, "forbiddenword", nil, nil)
	require.NoError(t, err)
	require.Empty(t, masked.Results)
	visible, err := service.Search(scoped, "albatross", nil, nil)
	require.NoError(t, err)
	require.Len(t, visible.Results, 3)
	for _, result := range visible.Results {
		require.Equal(t, "albatross", result.Properties["content"])
		require.NotContains(t, result.Properties, "draft")
	}

	initial, err := service.SearchTextContinuation(scoped, "albatross",
		&SearchOptions{Limit: 2}, SearchContinuationRequest{Owner: "reader", Database: "work", N: 1},
		nil, nil, service.Search, ChunkedSearchErrorPolicy{})
	require.NoError(t, err)
	require.NotEmpty(t, initial.QID)
	changed := request
	changed.Fingerprint = "policy-two"
	_, err = service.SearchTextContinuation(auth.WithFineGrainedRequest(ctx, &changed), "", nil,
		SearchContinuationRequest{Owner: "reader", Database: "work", QID: initial.QID, N: 2},
		nil, nil, nil, ChunkedSearchErrorPolicy{})
	require.ErrorIs(t, err, resultstream.ErrInvalidated)
}
