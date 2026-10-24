package nornicgrpc

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"errors"
	"testing"

	gen "github.com/orneryd/nornicdb/pkg/nornicgrpc/gen"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type lexicalCancellationSearcher struct {
	service  *search.Service
	cancel   context.CancelFunc
	filtered bool
}

func (s *lexicalCancellationSearcher) Search(ctx context.Context, query string, embedding []float32, opts *search.SearchOptions) (*search.SearchResponse, error) {
	copy := *opts
	copy.Where = func(*storage.Node) bool { s.filtered = true; s.cancel(); return true }
	return s.service.Search(ctx, query, embedding, &copy)
}

func TestSearchTextCancellationDuringLexicalSearch(t *testing.T) {
	for _, mode := range []string{"no_embedder", "single_fallback", "multi_fallback", "hybrid_control"} {
		t.Run(mode, func(t *testing.T) {
			engine := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
			t.Cleanup(func() { _ = engine.Close() })
			store := storage.NewNamespacedEngine(engine, "nornic")
			_, err := store.CreateNode(&storage.Node{ID: "seed", Labels: []string{"Doc"}, Properties: map[string]interface{}{"content": "needle"}})
			require.NoError(t, err)
			realSearch := search.NewService(store)
			require.NoError(t, realSearch.BuildIndexes(t.Context()))
			t.Cleanup(func() { _ = realSearch.Close() })
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			searcher := &lexicalCancellationSearcher{service: realSearch, cancel: cancel}
			var embedding func(context.Context, string) ([]float32, error)
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
			service, err := NewService(Config{}, embedding, chunks, searcher)
			require.NoError(t, err)
			response, err := service.SearchText(ctx, &gen.SearchTextRequest{Query: "needle"})
			require.True(t, searcher.filtered)
			t.Logf("mode=%s context=%v returned_error=%v response=%+v", mode, ctx.Err(), err, response)
			require.Error(t, err)
			require.Nil(t, response)
		})
	}
}
