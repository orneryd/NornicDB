package search

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A provider's own deadline is an outage while the request remains live. The
// common completion boundary must preserve the real lexical fallback and hits.
func TestSearchTextChunksProviderDeadlineKeepsLexicalFallback(t *testing.T) {
	for _, mode := range []string{"single", "multi"} {
		t.Run(mode, func(t *testing.T) {
			engine := newTestMemoryEngine(t)
			t.Cleanup(func() { _ = engine.Close() })
			store := storage.NewNamespacedEngine(engine, "nornic")
			_, err := store.CreateNode(&storage.Node{ID: "seed", Labels: []string{"Doc"}, Properties: map[string]interface{}{"content": "needle"}})
			require.NoError(t, err)
			service := NewService(store, DefaultVectorDimensions, nil)
			require.NoError(t, service.BuildIndexes(t.Context()))
			t.Cleanup(func() { _ = service.Close() })
			response, err := SearchTextChunks(t.Context(), "needle", DefaultSearchOptions(),
				func(context.Context, string) ([]string, error) {
					if mode == "multi" {
						return []string{"needle", "needle"}, nil
					}
					return []string{"needle"}, nil
				},
				func(context.Context, string) ([]float32, error) { return nil, context.DeadlineExceeded },
				service.Search)
			require.NoError(t, err)
			require.NoError(t, t.Context().Err())
			require.NotNil(t, response)
			require.Equal(t, "success", response.Status)
			require.Len(t, response.Results, 1)
			require.Equal(t, SearchFallbackQueryEmbeddingFailed, response.FallbackReason)
		})
	}
}
