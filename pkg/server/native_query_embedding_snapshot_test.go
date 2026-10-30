package server

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	nornicConfig "github.com/orneryd/nornicdb/pkg/config"
	nornicpb "github.com/orneryd/nornicdb/pkg/nornicgrpc/gen"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

type nativeRequestEmbedder struct {
	calls   atomic.Int64
	axis    int
	onEmbed func()
	once    sync.Once
}

func (e *nativeRequestEmbedder) Embed(context.Context, string) ([]float32, error) {
	e.calls.Add(1)
	if e.onEmbed != nil {
		e.once.Do(e.onEmbed)
	}
	vector := make([]float32, 1024)
	vector[e.axis] = 1
	return vector, nil
}

func (e *nativeRequestEmbedder) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	vectors := make([][]float32, len(texts))
	for i, text := range texts {
		vectors[i], _ = e.Embed(ctx, text)
	}
	return vectors, nil
}

func (*nativeRequestEmbedder) ChunkText(text string, _, _ int) ([]string, error) {
	if text == "alpha beta" {
		return []string{"alpha", "beta"}, nil
	}
	return []string{text}, nil
}
func (*nativeRequestEmbedder) Dimensions() int { return 1024 }
func (*nativeRequestEmbedder) Model() string   { return "native-snapshot-test" }
func (*nativeRequestEmbedder) Backend() string { return "cpu" }

// Exercise the installed protocol handlers, so independently resolving either
// callback in the REST handler or native gRPC resolver fails this regression.
func TestNativeSearchRequestKeepsProviderSnapshot(t *testing.T) {
	for _, protocol := range []string{"http", "grpc"} {
		t.Run(protocol, func(t *testing.T) {
			server, authenticator := setupTestServer(t)
			server.db.SetEmbedConfigForDB(nil) // Exercise the global query publication without external model creation.
			token := "Bearer " + getAuthToken(t, authenticator, "admin")
			database := server.dbManager.DefaultDatabaseName()
			if protocol == "grpc" {
				server.auth = nil
				server.config.Features = &nornicConfig.FeatureFlagsConfig{QdrantGRPCEnabled: true, QdrantGRPCListenAddr: "127.0.0.1:0"}
				require.NoError(t, server.startQdrantGRPC())
			}
			store, err := server.dbManager.GetStorage(database)
			require.NoError(t, err)
			seed := make([]float32, 1024)
			seed[0] = 1
			_, err = store.CreateNode(&storage.Node{ID: "snapshot-doc", Labels: []string{"Doc"}, Properties: map[string]any{"content": "alpha beta"}, ChunkEmbeddings: [][]float32{seed}})
			require.NoError(t, err)
			require.NoError(t, server.db.BuildSearchIndexes(context.Background()))
			old := &nativeRequestEmbedder{}
			replacement := &nativeRequestEmbedder{axis: 1}
			old.onEmbed = func() { server.db.GetCypherExecutor().SetEmbedder(replacement) }
			server.db.SetEmbedder(old)
			var request func()
			if protocol == "http" {
				request = func() {
					resp := makeRequest(t, server, http.MethodPost, "/nornicdb/search", map[string]any{"query": "alpha beta", "limit": 5}, token)
					require.Equal(t, http.StatusOK, resp.Code, resp.Body.String())
				}
			} else {
				conn, err := grpc.NewClient(server.qdrantGRPCServer.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
				require.NoError(t, err)
				t.Cleanup(func() { _ = conn.Close() })
				client := nornicpb.NewNornicSearchClient(conn)
				request = func() {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", token))
					_, err := client.SearchText(ctx, &nornicpb.SearchTextRequest{Query: "alpha beta", Limit: 5})
					require.NoError(t, err)
				}
			}
			request()
			require.Equal(t, int64(2), old.calls.Load(), "both chunks must use the request's captured provider")
			require.Zero(t, replacement.calls.Load())
			request()
			require.Equal(t, int64(2), old.calls.Load())
			require.Equal(t, int64(2), replacement.calls.Load(), "the next request must see the shared publication")
		})
	}
}
