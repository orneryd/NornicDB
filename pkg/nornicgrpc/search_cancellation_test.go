package nornicgrpc

import (
	"context"
	"testing"

	gen "github.com/orneryd/nornicdb/pkg/nornicgrpc/gen"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/stretchr/testify/require"
)

func TestServiceSearchTextCanceledEmbeddingDoesNotFallBack(t *testing.T) {
	searcher := &stubSearcher{resp: &search.SearchResponse{}}
	svc, err := NewService(Config{}, func(context.Context, string) ([]float32, error) {
		return nil, context.Canceled
	}, nil, searcher)
	require.NoError(t, err)
	response, err := svc.SearchText(context.Background(), &gen.SearchTextRequest{Query: "needle"})
	require.Error(t, err)
	require.Nil(t, response)
	require.Empty(t, searcher.lastQuery, "cancellation must not start lexical fallback")
}
