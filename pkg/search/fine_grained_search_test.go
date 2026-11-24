package search

import (
	"context"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/resultstream"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestFineGrainedSearchFiltersSharedCorpusAndInvalidatesPages(t *testing.T) {
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
	require.Nil(t, visible.Metrics)
	reference := NewFulltextIndexV2WithAnalyzer(service.bm25Analyzer)
	readable := map[string]any{"content": "albatross"}
	for _, id := range []string{"one", "two", "three"} {
		reference.Index(id, service.extractSearchableText(&storage.Node{Labels: []string{"Article"}, Properties: readable}), nil, nil, false)
	}
	expected := reference.Search("albatross", 3)
	require.Len(t, expected, 3)
	for _, result := range visible.Results {
		require.Equal(t, "albatross", result.Properties["content"])
		require.NotContains(t, result.Properties, "draft")
		require.Equal(t, expected[0].Score, result.Score)
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

// The ranked phase queries the shared index. The catalog phase, which does
// not rely on search scores, can still return any visible article.
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

func TestFineGrainedHNSWUsesOnlyVisibleVectors(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
	t.Setenv("NORNICDB_HNSW_BUILD_GPU_ENABLED", "false")
	t.Setenv("NORNICDB_VECTOR_GPU_ENABLED", "false")
	ctx := t.Context()
	base, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	store := storage.NewNamespacedEngine(base, "work")
	schema := base.GetSchemaForNamespace("work")
	require.NoError(t, schema.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{
		Name: "article-decay", Scope: knowledgepolicy.ScopeNode,
		Function: knowledgepolicy.DecayFunctionExponential, HalfLifeSeconds: 3600,
		VisibilityThreshold: .10, ScoreFrom: knowledgepolicy.ScoreFromCreated,
		Enabled: true, DecayEnabled: true,
	}))
	require.NoError(t, schema.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{
		Name: "articles", ProfileRef: "article-decay", TargetLabels: []string{"Article"},
	}))
	base.SetDecayEnabled(true)
	for _, node := range []*storage.Node{
		{ID: "internal", Labels: []string{"Internal"}, ChunkEmbeddings: [][]float32{{1, 0}}},
		{ID: "no-vector-grant", Labels: []string{"Article"}, ChunkEmbeddings: [][]float32{{1, .1}}},
		{ID: "suppressed", Labels: []string{"Article", "PublicVector"}, ChunkEmbeddings: [][]float32{{1, .05}}, CreatedAt: time.Now().Add(-720 * time.Hour)},
		{ID: "visible", Labels: []string{"Article", "PublicVector"}, ChunkEmbeddings: [][]float32{{1, .5}}},
	} {
		_, err := store.CreateNode(ctx, node)
		require.NoError(t, err)
	}

	service := NewService(store, 2, nil)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	service.SetIndexFlags(false, true)
	require.NoError(t, service.BuildIndexes(ctx))

	policy, err := auth.CompileFineGrainedPolicy(auth.GraphFineGrainedActions(), auth.FineGrainedCatalog{
		Definitions: []auth.FineGrainedEntitlement{
			{ID: "access", Action: "database.access", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedDatabase}},
			{ID: "article", Action: "graph.node.traverse", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode, Types: []string{"Article"}}},
			{ID: "vectors", Action: "graph.node.embedding.read", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode, Types: []string{"PublicVector"}}},
			{ID: "vector-source", Action: "graph.node.property.read", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode, Types: []string{"PublicVector"}}},
		},
		Assignments: map[string][]string{
			"reader":        {"access", "article", "vectors", "vector-source"},
			"source-scoped": {"access", "article", "vectors"},
		},
	})
	require.NoError(t, err)
	request := &auth.FineGrainedRequest{
		Access: policy.ForRoles([]string{"reader"}), DatabaseID: func(string) string { return "work-id" },
	}
	scoped := auth.WithFineGrainedRequest(ctx, request)
	require.Equal(t, strategyModeHNSW.String(), service.CurrentStrategy())
	population := service.EmbeddingCount()
	require.Greater(t, population, 1)

	spec := VectorQuerySpec{Limit: 1}
	vector := []float32{1, 0}
	sourceScoped := *request
	sourceScoped.Access = policy.ForRoles([]string{"source-scoped"})
	sourceHits, err := service.VectorQueryNodesWithOptions(auth.WithFineGrainedRequest(ctx, &sourceScoped), vector, spec, &SearchOptions{HNSWEf: 8})
	require.NoError(t, err)
	require.Empty(t, sourceHits)
	for _, tc := range []struct {
		ctx context.Context
		id  storage.NodeID
	}{{ctx, "internal"}, {scoped, "visible"}, {ctx, "internal"}} {
		hits, err := service.VectorQueryNodesWithOptions(tc.ctx, vector, spec, &SearchOptions{HNSWEf: 8})
		require.NoError(t, err)
		require.Len(t, hits, 1)
		require.Equal(t, string(tc.id), hits[0].ID)
		response, err := service.Search(tc.ctx, "", vector, &SearchOptions{Limit: 1, HNSWEf: 8})
		require.NoError(t, err)
		require.Len(t, response.Results, 1)
		require.Equal(t, string(tc.id), response.Results[0].ID)
	}
	require.Equal(t, population, service.EmbeddingCount())
}
