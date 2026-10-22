package nornicgrpc

import (
	"context"
	"math"
	"net"
	"testing"

	gen "github.com/orneryd/nornicdb/pkg/nornicgrpc/gen"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
)

type malformedGRPCReranker struct{ results []search.RerankResult }

func (r malformedGRPCReranker) Name() string                     { return "malformed" }
func (r malformedGRPCReranker) Enabled() bool                    { return true }
func (r malformedGRPCReranker) IsAvailable(context.Context) bool { return true }
func (r malformedGRPCReranker) Rerank(context.Context, string, []search.RerankCandidate) ([]search.RerankResult, error) {
	return append([]search.RerankResult(nil), r.results...), nil
}

func TestSearchTextMalformedRerankTrailerParity(t *testing.T) {
	engine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic")
	service := search.NewServiceWithDimensions(engine, 2)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	for index, id := range []string{"a", "b", "c"} {
		node := &storage.Node{
			ID: storage.NodeID(id), Labels: []string{"Doc"},
			Properties:      map[string]any{"content": "alpha document " + id},
			ChunkEmbeddings: [][]float32{{1, float32(index) * 0.01}},
		}
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
		require.NoError(t, service.IndexNode(node))
	}
	service.SetSearchResultCachePolicy(0, 0)
	api, err := NewService(Config{RerankEnabled: true}, func(context.Context, string) ([]float32, error) {
		return []float32{1, 0}, nil
	}, func(_ context.Context, query string) ([]string, error) { return []string{query}, nil }, service)
	require.NoError(t, err)
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	gen.RegisterNornicSearchServer(server, api)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	connection, err := grpc.NewClient("passthrough:///rerank", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, connection.Close()) })
	client := gen.NewNornicSearchClient(connection)

	for name, results := range map[string][]search.RerankResult{
		"duplicate": {{ID: "a", OriginalRank: 1, CrossScore: 0.9, FinalScore: 0.9}, {ID: "a", OriginalRank: 1, CrossScore: 0.1, FinalScore: 0.1}},
		"negative":  {{OriginalRank: -1, CrossScore: 0.9, FinalScore: 0.9}},
		"nan":       {{ID: "a", OriginalRank: 1, CrossScore: math.NaN(), FinalScore: math.NaN()}},
	} {
		t.Run(name, func(t *testing.T) {
			service.SetReranker(&malformedGRPCReranker{results: results})
			for _, request := range []*gen.SearchTextRequest{
				{Query: "alpha", Limit: 3},
				{Query: "alpha", Mode: "ranked", N: 3},
			} {
				var trailer metadata.MD
				response, err := client.SearchText(context.Background(), request, grpc.Trailer(&trailer))
				require.NoError(t, err)
				require.True(t, response.FallbackTriggered)
				require.Equal(t, string(search.SearchFallbackRerankFailed), response.FallbackReason)
				require.Equal(t, []string{string(search.SearchFallbackRerankFailed)}, trailer.Get("nornicdb-search-fallback-reason"))
				require.NotContains(t, response.SearchMethod, "+rerank")
			}
		})
	}
}
