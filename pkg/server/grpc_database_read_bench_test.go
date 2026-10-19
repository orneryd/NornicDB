package server

import (
	"context"
	"testing"

	nornicConfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/multidb"
	qpb "github.com/qdrant/go-client/qdrant"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// Exercise the production callbacks through the authenticated gRPC wire path.
func BenchmarkGRPCDatabaseReadSelections(b *testing.B) {
	server, users := setupTestServer(b)
	server.config.EmbeddingEnabled = false
	server.config.Features = &nornicConfig.FeatureFlagsConfig{
		QdrantGRPCEnabled: true, QdrantGRPCListenAddr: "127.0.0.1:0",
		QdrantGRPCMaxVectorDim: 4096, QdrantGRPCMaxBatchPoints: 1000, QdrantGRPCMaxTopK: 1000,
	}
	if err := server.Start(); err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	if err := server.qdrantGRPCServer.CollectionStore().Create(ctx, "grpc_physical", 4, qpb.Distance_Cosine); err != nil {
		b.Fatal(err)
	}
	if err := server.dbManager.CreateCompositeDatabase("grpc_portfolio", []multidb.ConstituentRef{
		{Alias: "part", DatabaseName: "grpc_physical", Type: "local", AccessMode: "read_write"},
	}); err != nil {
		b.Fatal(err)
	}
	connection, err := grpc.NewClient(server.qdrantGRPCServer.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = connection.Close() })
	points := qpb.NewPointsClient(connection)
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer "+getAuthToken(b, users, "reader")))
	for _, selection := range []string{"grpc_physical", "grpc_portfolio.part", "grpc_portfolio"} {
		b.Run(selection, func(b *testing.B) {
			request := &qpb.CountPoints{CollectionName: selection}
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := points.Count(ctx, request); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
