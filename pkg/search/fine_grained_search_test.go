package search

import (
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/resultstream"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestFineGrainedSearchUsesProjectedCorpusAndInvalidatesPages(t *testing.T) {
	ctx := t.Context()
	service, request := newFineGrainedSearchFixture(t)
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

// The complete ranked-then-ID stream is built from a private projected corpus
// too, so its cursor must take the corpus over: the first page and the next
// page both serve the authorized article population and its visible properties,
// and a later page stays bound to the retained policy.
func TestFineGrainedRankedThenIDContinuationDeliversProjectedPages(t *testing.T) {
	ctx := t.Context()
	service, principal := newFineGrainedSearchFixture(t)
	scoped := auth.WithFineGrainedRequest(ctx, &principal)

	first, err := service.SearchTextContinuation(scoped, "albatross", &SearchOptions{Limit: 2},
		SearchContinuationRequest{
			Owner: "reader", Database: "work", Mode: SearchContinuationRankedThenID,
			RankedLimit: 2, N: 2,
		},
		nil, nil, nil, ChunkedSearchErrorPolicy{})
	require.NoError(t, err)
	require.NotEmpty(t, first.QID)
	require.Len(t, first.Results, 2)
	require.True(t, first.HasMore)
	delivered := make(map[storage.NodeID]struct{}, len(first.Results))
	for _, result := range first.Results {
		require.NotEqual(t, storage.NodeID("hidden"), result.NodeID)
		require.Equal(t, "albatross", result.Properties["content"])
		require.NotContains(t, result.Properties, "draft")
		_, repeated := delivered[result.NodeID]
		require.False(t, repeated, "first page repeats a projected result")
		delivered[result.NodeID] = struct{}{}
	}

	next, err := service.SearchTextContinuation(scoped, "", nil,
		SearchContinuationRequest{Owner: "reader", Database: "work", QID: first.QID, N: 2},
		nil, nil, nil, ChunkedSearchErrorPolicy{})
	require.NoError(t, err)
	require.Len(t, next.Results, 1)
	require.False(t, next.HasMore)
	remaining := next.Results[0]
	require.NotEqual(t, storage.NodeID("hidden"), remaining.NodeID)
	_, repeated := delivered[remaining.NodeID]
	require.False(t, repeated, "next page repeats a projected result")
	require.Equal(t, SearchContinuationCatalogPhase, remaining.Phase)
	require.Equal(t, "albatross", remaining.Properties["content"])
	require.NotContains(t, remaining.Properties, "draft")

	released, err := service.SearchTextContinuation(scoped, "", nil,
		SearchContinuationRequest{Owner: "reader", Database: "work", QID: first.QID, Discard: true},
		nil, nil, nil, ChunkedSearchErrorPolicy{})
	require.NoError(t, err)
	require.True(t, released.Released)

	// A later page stays bound to the policy the cursor was started under.
	retained, err := service.SearchTextContinuation(scoped, "albatross", &SearchOptions{Limit: 2},
		SearchContinuationRequest{
			Owner: "reader", Database: "work", Mode: SearchContinuationRankedThenID,
			RankedLimit: 2, N: 2,
		},
		nil, nil, nil, ChunkedSearchErrorPolicy{})
	require.NoError(t, err)
	require.NotEmpty(t, retained.QID)
	changed := principal
	changed.Fingerprint = "policy-two"
	_, err = service.SearchTextContinuation(auth.WithFineGrainedRequest(ctx, &changed), "", nil,
		SearchContinuationRequest{Owner: "reader", Database: "work", QID: retained.QID, N: 1},
		nil, nil, nil, ChunkedSearchErrorPolicy{})
	require.ErrorIs(t, err, resultstream.ErrInvalidated)
}

func newFineGrainedSearchFixture(t *testing.T) (*Service, auth.FineGrainedRequest) {
	t.Helper()
	ctx := t.Context()
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	store := storage.NewNamespacedEngine(base, "work")
	for _, node := range []*storage.Node{
		{ID: "one", Labels: []string{"Article"}, Properties: map[string]any{"content": "albatross", "draft": "forbiddenword"}},
		{ID: "two", Labels: []string{"Article"}, Properties: map[string]any{"content": "albatross"}},
		{ID: "three", Labels: []string{"Article"}, Properties: map[string]any{"content": "albatross"}},
		{ID: "hidden", Labels: []string{"Internal"}, Properties: map[string]any{"content": "albatross forbiddenword"}},
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
	return service, auth.FineGrainedRequest{
		Access: policy.ForRoles([]string{"reader"}), DatabaseID: func(string) string { return "work-id" }, Fingerprint: "policy-one",
	}
}
