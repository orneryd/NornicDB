package server

import (
	"context"
	"testing"
	"time"

	nornicConfig "github.com/orneryd/nornicdb/pkg/config"
	qpb "github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Publish from another goroutine while the public RPC is embedding its first
// example. Every remaining example must stay in that request's embedding space.
func TestQdrantRecommendKeepsProviderSnapshot(t *testing.T) {
	for _, method := range []string{"Recommend", "RecommendNegative", "RecommendBatch", "QueryBatch"} {
		t.Run(method, func(t *testing.T) {
			server, _ := setupTestServer(t)
			server.auth = nil
			server.config.EmbeddingEnabled = true
			server.config.Features = &nornicConfig.FeatureFlagsConfig{QdrantGRPCEnabled: true, QdrantGRPCListenAddr: "127.0.0.1:0"}
			require.NoError(t, server.startQdrantGRPC())
			const collection = "snapshot-recommend"
			require.NoError(t, server.qdrantCollectionStore.Create(context.Background(), collection, 1024, qpb.Distance_Cosine))
			conn, err := grpc.NewClient(server.qdrantGRPCServer.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
			require.NoError(t, err)
			t.Cleanup(func() { _ = conn.Close() })
			client := qpb.NewPointsClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			old := &nativeRequestEmbedder{}
			replacement := &nativeRequestEmbedder{axis: 1}
			base := server.db.GetCypherExecutor()
			started, published, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
			old.onEmbed = func() {
				close(started)
				select {
				case <-published:
				case <-ctx.Done():
				}
			}
			base.SetEmbedder(old)
			go func() {
				defer close(done)
				select {
				case <-started:
					base.SetEmbedder(replacement)
					close(published)
				case <-ctx.Done():
				}
			}()
			vector := func(text string) *qpb.Vector {
				return &qpb.Vector{Vector: &qpb.Vector_Document{Document: &qpb.Document{Text: text}}}
			}
			query := func(text string) *qpb.QueryPoints {
				return &qpb.QueryPoints{CollectionName: collection, Query: &qpb.Query{Variant: &qpb.Query_Nearest{Nearest: &qpb.VectorInput{Variant: &qpb.VectorInput_Document{Document: &qpb.Document{Text: text}}}}}}
			}
			wantCalls := int64(2)
			if method == "RecommendNegative" {
				wantCalls = 3
			}
			request := func() error {
				switch method {
				case "Recommend", "RecommendNegative":
					req := &qpb.RecommendPoints{CollectionName: collection, Limit: 5, PositiveVectors: []*qpb.Vector{vector("alpha"), vector("beta")}}
					if method == "RecommendNegative" {
						req.NegativeVectors = []*qpb.Vector{vector("gamma")}
					}
					_, err := client.Recommend(ctx, req)
					return err
				case "RecommendBatch":
					_, err := client.RecommendBatch(ctx, &qpb.RecommendBatchPoints{CollectionName: collection, RecommendPoints: []*qpb.RecommendPoints{
						{PositiveVectors: []*qpb.Vector{vector("alpha")}}, {PositiveVectors: []*qpb.Vector{vector("beta")}},
					}})
					return err
				default:
					_, err := client.QueryBatch(ctx, &qpb.QueryBatchPoints{QueryPoints: []*qpb.QueryPoints{query("alpha"), query("beta")}})
					return err
				}
			}
			require.NoError(t, request())
			<-done
			require.Equal(t, wantCalls, old.calls.Load(), "one RPC must embed all examples with its captured provider")
			require.Zero(t, replacement.calls.Load())
			require.NoError(t, request())
			require.Equal(t, wantCalls, old.calls.Load())
			require.Equal(t, wantCalls, replacement.calls.Load(), "the next RPC must observe the replacement")
		})
	}
}
