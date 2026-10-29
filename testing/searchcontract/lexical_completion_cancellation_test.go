// Package searchcontract holds the external contract tests that exercise the
// search text completion boundary through more than one public transport
// without importing either package's internals as a test owner.
package searchcontract

import (
	"context"
	"errors"
	"testing"

	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/nornicgrpc"
	gen "github.com/orneryd/nornicdb/pkg/nornicgrpc/gen"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const seedContent = "needle"

// lexicalModes are the original four query-completion modes. A nil embedder
// distinguishes no_embedder; provider offline drives both fallback arities and
// a real vector drives the hybrid control.
var lexicalModes = []string{"no_embedder", "single_fallback", "multi_fallback", "hybrid_control"}

// grpcSearcher delegates to the real search service. The proto SearchText
// request has no predicate field, so the gRPC call path injects the shared
// cancellation predicate into a copy of the options it was handed, exactly as
// the previous gRPC-local fixture did.
type grpcSearcher struct {
	service  *search.Service
	observed func(*storage.Node) bool
}

func (s *grpcSearcher) Search(ctx context.Context, query string, embedding []float32, opts *search.SearchOptions) (*search.SearchResponse, error) {
	copied := *opts
	copied.Where = s.observed
	return s.service.Search(ctx, query, embedding, &copied)
}

// lexicalFixture is the one real-memory-storage/node/searchService setup the
// matrix shares: tracked storage engine, namespaced Doc seed, built indexes and
// an explicit ordered cleanup.
type lexicalFixture struct {
	service *search.Service
}

func newLexicalFixture(t *testing.T) lexicalFixture {
	t.Helper()
	engine := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = engine.Close() })
	store := storage.NewNamespacedEngine(engine, "nornic")
	_, err := store.CreateNode(&storage.Node{ID: "seed", Labels: []string{"Doc"}, Properties: map[string]interface{}{"content": seedContent}})
	require.NoError(t, err)
	service := search.NewService(store, search.DefaultVectorDimensions, nil)
	require.NoError(t, service.BuildIndexes(t.Context()))
	t.Cleanup(func() { _ = service.Close() })
	return lexicalFixture{service: service}
}

func lexicalEmbedding(t *testing.T, mode string) search.EmbedQueryFunc {
	t.Helper()
	switch mode {
	case "no_embedder":
		return nil
	case "hybrid_control":
		return func(context.Context, string) ([]float32, error) { return []float32{1, 0}, nil }
	case "single_fallback", "multi_fallback":
		return func(context.Context, string) ([]float32, error) { return nil, errors.New("provider offline") }
	}
	t.Fatalf("unknown lexical completion mode %q", mode)
	return nil
}

func lexicalChunks(t *testing.T, mode string) search.ChunkQueryFunc {
	t.Helper()
	return func(context.Context, string) ([]string, error) {
		if mode == "multi_fallback" {
			return []string{seedContent, seedContent}, nil
		}
		return []string{seedContent}, nil
	}
}

// lexicalCancellationPredicate is the shared rendezvous: it is reached inside
// real search work and cancels the request from the actual Where callback.
func lexicalCancellationPredicate(t *testing.T, cancel context.CancelFunc) (func(*storage.Node) bool, func() bool) {
	t.Helper()
	filtered := false
	return func(*storage.Node) bool { filtered = true; cancel(); return true }, func() bool { return filtered }
}

// TestSearchTextChunksVsSearchTextCancellationDuringLexicalSearch compares the
// canonical direct completion (search.SearchTextChunks) with the gRPC transport
// completion (nornicgrpc.Service.SearchText) across the original four modes.
func TestSearchTextChunksVsSearchTextCancellationDuringLexicalSearch(t *testing.T) {
	for _, mode := range lexicalModes {
		t.Run(mode, func(t *testing.T) {
			fixture := newLexicalFixture(t)
			embedding := lexicalEmbedding(t, mode)
			chunks := lexicalChunks(t, mode)
			require.Equal(t, search.DefaultVectorDimensions, fixture.service.VectorIndexDimensions())

			t.Run("direct", func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				opts := search.DefaultSearchOptions()
				predicate, observed := lexicalCancellationPredicate(t, cancel)
				opts.Where = predicate
				response, err := search.SearchTextChunks(ctx, seedContent, opts, chunks, embedding, fixture.service.Search)
				require.True(t, observed(), "the request must cancel inside real search work")
				t.Logf("mode=%s transport=direct context=%v returned_error=%v response=%+v", mode, ctx.Err(), err, response)
				require.ErrorIs(t, err, context.Canceled)
				require.Nil(t, response)
			})

			t.Run("grpc", func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				observed := new(bool)
				searcher := &grpcSearcher{service: fixture.service, observed: func(*storage.Node) bool { *observed = true; cancel(); return true }}
				service, err := nornicgrpc.NewService(nornicgrpc.Config{}, nornicgrpc.EmbedQueryFunc(embedding), nornicgrpc.ChunkQueryFunc(chunks), searcher)
				require.NoError(t, err)
				response, err := service.SearchText(ctx, &gen.SearchTextRequest{Query: seedContent})
				require.True(t, *observed, "the request must cancel inside real search work")
				t.Logf("mode=%s transport=grpc context=%v returned_error=%v response=%+v", mode, ctx.Err(), err, response)
				require.Error(t, err)
				require.Nil(t, response)
			})
		})
	}
}
