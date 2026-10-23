package search

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Use the real BM25 service. The filter is the deterministic rendezvous for
// a request cancellation arriving during the synchronous lexical search.
func TestSearchTextChunksCancellationDuringLexicalSearch(t *testing.T) {
	for _, mode := range []string{"no_embedder", "single_fallback", "multi_fallback", "hybrid_control"} {
		t.Run(mode, func(t *testing.T) {
			engine := newTestMemoryEngine(t)
			t.Cleanup(func() { _ = engine.Close() })
			store := storage.NewNamespacedEngine(engine, "nornic")
			_, err := store.CreateNode(&storage.Node{ID: "seed", Labels: []string{"Doc"}, Properties: map[string]interface{}{"content": "needle"}})
			require.NoError(t, err)
			service := NewService(store)
			require.NoError(t, service.BuildIndexes(t.Context()))
			t.Cleanup(func() { _ = service.Close() })
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			filtered := false
			opts := DefaultSearchOptions()
			opts.Where = func(*storage.Node) bool { filtered = true; cancel(); return true }
			var embedding EmbedQueryFunc
			if mode != "no_embedder" {
				embedding = func(context.Context, string) ([]float32, error) {
					if mode == "hybrid_control" {
						return []float32{1, 0}, nil
					}
					return nil, errors.New("provider offline")
				}
			}
			chunks := func(context.Context, string) ([]string, error) {
				if mode == "multi_fallback" {
					return []string{"needle", "needle"}, nil
				}
				return []string{"needle"}, nil
			}
			response, err := SearchTextChunks(ctx, "needle", opts, chunks, embedding, service.Search)
			require.True(t, filtered, "the request must cancel inside real search work")
			t.Logf("mode=%s context=%v returned_error=%v response=%+v", mode, ctx.Err(), err, response)
			require.ErrorIs(t, err, context.Canceled)
			require.Nil(t, response)
		})
	}
}

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
			service := NewService(store)
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
