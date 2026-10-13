package search

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// A graph search whose node side finds nothing by hybrid ranking
// (no_hybrid_results) but whose relationship side matched doesn't report a
// fallback; with no relationship hit either, it does.
func TestGraphSearchFallbackReflectsBothKinds(t *testing.T) {
	ctx := context.Background()
	engine := relationshipTestEngine(t)
	svc := startRelationshipService(t, engine, nil, nil)
	require.NoError(t, svc.BuildIndexes(ctx))

	opts := DefaultSearchOptions()
	opts.Graph, opts.Limit = true, 5
	response, err := svc.Search(ctx, "zephyrine", []float32{1, 0, 0}, opts)
	require.NoError(t, err)
	require.NotEmpty(t, response.Results)
	require.NotNil(t, response.Results[0].Relationship)
	require.False(t, response.FallbackTriggered)
	require.Equal(t, SearchFallbackNone, response.FallbackReason)

	opts = DefaultSearchOptions()
	opts.Graph, opts.Limit = true, 5
	response, err = svc.Search(ctx, "nothingmatchesthis", []float32{1, 0, 0}, opts)
	require.NoError(t, err)
	require.Empty(t, response.Results)
	require.True(t, response.FallbackTriggered)
	require.Equal(t, SearchFallbackNoHybridResults, response.FallbackReason)
}
