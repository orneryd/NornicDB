package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	nornicConfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/embed"
	nornicpb "github.com/orneryd/nornicdb/pkg/nornicgrpc/gen"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestIDStreamDoesNotResolveQueryModel(t *testing.T) {
	for _, protocol := range []string{"http", "grpc"} {
		t.Run(protocol, func(t *testing.T) {
			server, authenticator := setupTestServer(t)
			if protocol == "grpc" {
				server.auth = nil
				server.config.Features = &nornicConfig.FeatureFlagsConfig{QdrantGRPCEnabled: true, QdrantGRPCListenAddr: "127.0.0.1:0"}
				require.NoError(t, server.startQdrantGRPC())
			}
			store, err := server.dbManager.GetStorage(server.dbManager.DefaultDatabaseName())
			require.NoError(t, err)
			for i := range 3 {
				_, err = store.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("id-only-doc-%d", i)), Properties: map[string]any{"content": "alpha"}})
				require.NoError(t, err)
			}
			provider := &nativeRequestEmbedder{}
			server.db.GetCypherExecutor().SetEmbedder(provider)
			var resolutions atomic.Int64
			server.db.SetEmbedConfigForDB(func(string) (*embed.Config, error) {
				resolutions.Add(1)
				return nil, nil // No network/model setup is needed to observe this call.
			})
			if protocol == "http" {
				token := "Bearer " + getAuthToken(t, authenticator, "admin")
				resp := makeRequest(t, server, http.MethodPost, "/nornicdb/search", map[string]any{"mode": "id", "n": 1}, token)
				require.Equal(t, http.StatusOK, resp.Code, resp.Body.String())
				var page struct {
					QID string `json:"qid"`
				}
				require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &page))
				require.NotEmpty(t, page.QID)
				resp = makeRequest(t, server, http.MethodPost, "/nornicdb/search", map[string]any{"qid": page.QID, "n": 1}, token)
				require.Equal(t, http.StatusOK, resp.Code, resp.Body.String())
				resp = makeRequest(t, server, http.MethodPost, "/nornicdb/search", map[string]any{"qid": page.QID, "discard": true}, token)
				require.Equal(t, http.StatusOK, resp.Code, resp.Body.String())
			} else {
				conn, err := grpc.NewClient(server.qdrantGRPCServer.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
				require.NoError(t, err)
				t.Cleanup(func() { _ = conn.Close() })
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				client := nornicpb.NewNornicSearchClient(conn)
				page, err := client.SearchText(ctx, &nornicpb.SearchTextRequest{Mode: "id", N: 1})
				require.NoError(t, err)
				require.NotEmpty(t, page.Qid)
				_, err = client.SearchText(ctx, &nornicpb.SearchTextRequest{Qid: page.Qid, N: 1})
				require.NoError(t, err)
				_, err = client.SearchText(ctx, &nornicpb.SearchTextRequest{Qid: page.Qid, Discard: true})
				require.NoError(t, err)
			}
			t.Logf("%s ID-only request: model resolutions=%d, embedding calls=%d", protocol, resolutions.Load(), provider.calls.Load())
			require.Zero(t, provider.calls.Load())
			require.Zero(t, resolutions.Load(), "ID-only enumeration must not resolve or initialize a query model")
		})
	}
}
