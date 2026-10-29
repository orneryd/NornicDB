package search

import (
	"context"

	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestManagedEmbeddingContentIsExcludedFromSearchTextAndResults(t *testing.T) {
	engine := newTestMemoryEngine(t)
	service := NewService(engine, DefaultVectorDimensions, nil)
	service.SetFulltextProperties([]string{"title"})
	node := &storage.Node{
		ID:     "test:visual",
		Labels: []string{"Asset"},
		Properties: map[string]any{
			"title":              "Architecture diagram",
			"_embedding_content": `[{"type":"image_base64","image_base64":"data:image/png;base64,c2VjcmV0LXBheWxvYWQ="}]`,
		},
	}

	text := service.extractSearchableText(node)
	require.Contains(t, text, "Architecture diagram")
	require.NotContains(t, text, "image_base64")
	require.NotContains(t, text, "c2VjcmV0LXBheWxvYWQ")
	require.NotContains(t, service.searchableTextValues(node), "_embedding_content")

	service.SetFulltextProperties(nil)
	node.Properties["_embedding_content"] = "large-image-payload"
	_, err := engine.CreateNode(node)
	require.NoError(t, err)

	opts := &SearchOptions{Limit: 1, ExcludeProperties: []string{"_embedding_content"}}
	results := service.enrichResults(context.Background(), []rrfResult{{ID: string(node.ID), RRFScore: 1}}, opts, nil, hitMatchQuery{})
	require.Len(t, results, 1)
	require.NotContains(t, results[0].Properties, "_embedding_content")
	require.Equal(t, "Architecture diagram", results[0].Properties["title"])

	require.Contains(t, node.Properties, "_embedding_content", "search projection must not mutate stored properties")
}

func TestSearchResultPropertyProjectionCombinesIncludeAndExclude(t *testing.T) {
	properties := map[string]any{"title": "diagram", "summary": "visible", "structured_payload": "opaque-value"}
	projection := newResultPropertyProjection(
		[]string{"title", "summary", "structured_payload"},
		[]string{"structured_payload"},
	)

	require.Equal(t, map[string]any{"title": "diagram", "summary": "visible"}, projection.apply(properties))
	require.Equal(t, properties, newResultPropertyProjection(nil, nil).apply(properties))
}

func TestBM25PropertyAllowlistDoesNotProjectSearchResponses(t *testing.T) {
	properties := map[string]any{"title": "diagram", "structured_payload": "opaque-value"}
	service := NewService(newTestMemoryEngine(t), DefaultVectorDimensions, nil)
	service.SetFulltextProperties([]string{"title"})

	require.NotContains(t, service.extractSearchableText(&storage.Node{Properties: properties}), "opaque-value")
	require.Equal(t, properties, newResultPropertyProjection(nil, nil).apply(properties))
}

func TestContinuationHydrationPreservesCallerPropertyProjection(t *testing.T) {
	engine := newTestMemoryEngine(t)
	node := &storage.Node{ID: "test:visual", Properties: map[string]any{
		"title": "diagram", "structured_payload": "opaque-value",
	}}
	_, err := engine.CreateNode(node)
	require.NoError(t, err)

	projection := newResultPropertyProjection([]string{"title"}, nil)
	results, err := hydrateContinuationResults(
		engine,
		[]SearchResult{{ID: string(node.ID), NodeID: node.ID}},
		nil,
		projection,
	)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, map[string]any{"title": "diagram"}, results[0].Properties)
}

func TestSearchCacheSeparatesResultPropertyProjections(t *testing.T) {
	base := &SearchOptions{Limit: 10, IncludeProperties: []string{"title"}}
	other := *base
	other.IncludeProperties = []string{"title", "summary"}

	require.NotEqual(t, searchCacheKey("query", nil, base), searchCacheKey("query", nil, &other))
}
