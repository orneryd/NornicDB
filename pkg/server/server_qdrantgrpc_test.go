package server

import (
	"context"
	"testing"
	"time"

	nornicConfig "github.com/orneryd/nornicdb/pkg/config"
	pb "github.com/orneryd/nornicdb/pkg/qdrantgrpc/gen"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestServer_QdrantGRPCFeatureFlag_StartsAndSharesDefaultDB(t *testing.T) {
	server, _ := setupTestServer(t)

	// Allow Qdrant clients to manage vectors by disabling NornicDB-managed embeddings.
	server.config.EmbeddingEnabled = false

	server.config.Features = &nornicConfig.FeatureFlagsConfig{
		QdrantGRPCEnabled:        true,
		QdrantGRPCListenAddr:     "127.0.0.1:0",
		QdrantGRPCMaxVectorDim:   4096,
		QdrantGRPCMaxBatchPoints: 1000,
		QdrantGRPCMaxTopK:        1000,
	}

	require.NoError(t, server.Start())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Stop(ctx)
	})

	require.NotNil(t, server.qdrantGRPCServer)
	require.True(t, server.qdrantGRPCServer.IsRunning())

	conn, err := grpc.Dial(server.qdrantGRPCServer.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	collections := pb.NewCollectionsClient(conn)
	points := pb.NewPointsClient(conn)

	_, err = collections.CreateCollection(ctx, &pb.CreateCollectionRequest{
		CollectionName: "grpc_col",
		VectorsConfig: &pb.VectorsConfig{
			Config: &pb.VectorsConfig_Params{
				Params: &pb.VectorParams{
					Size:     4,
					Distance: pb.Distance_COSINE,
				},
			},
		},
	})
	require.NoError(t, err)

	_, err = points.Upsert(ctx, &pb.UpsertPointsRequest{
		CollectionName: "grpc_col",
		Points: []*pb.PointStruct{
			{
				Id: &pb.PointId{PointIdOptions: &pb.PointId_Uuid{Uuid: "p1"}},
				Vectors: &pb.Vectors{
					VectorsOptions: &pb.Vectors_Vectors{
						Vectors: &pb.NamedVectors{
							Vectors: map[string]*pb.Vector{
								"a": {Data: []float32{1, 0, 0, 0}},
								"b": {Data: []float32{0, 1, 0, 0}},
							},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	// Verify the point exists in the same default database the HTTP/Bolt/Cypher layer uses.
	exec, err := server.getExecutorForDatabase(server.dbManager.DefaultDatabaseName())
	require.NoError(t, err)

	res, err := exec.Execute(ctx, "MATCH (n:QdrantPoint:grpc_col) RETURN count(n) AS c", nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, int64(1), res.Rows[0][0])
}

func TestServer_QdrantGRPC_ManagedEmbeddings_DisablesVectorMutations(t *testing.T) {
	server, _ := setupTestServer(t)

	// Managed embeddings enabled (default) => Qdrant vector mutation endpoints should be rejected.
	server.config.EmbeddingEnabled = true
	server.config.Features = &nornicConfig.FeatureFlagsConfig{
		QdrantGRPCEnabled:        true,
		QdrantGRPCListenAddr:     "127.0.0.1:0",
		QdrantGRPCMaxVectorDim:   4096,
		QdrantGRPCMaxBatchPoints: 1000,
		QdrantGRPCMaxTopK:        1000,
	}

	require.NoError(t, server.Start())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Stop(ctx)
	})

	conn, err := grpc.Dial(server.qdrantGRPCServer.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	collections := pb.NewCollectionsClient(conn)
	points := pb.NewPointsClient(conn)

	_, err = collections.CreateCollection(ctx, &pb.CreateCollectionRequest{
		CollectionName: "grpc_col",
		VectorsConfig: &pb.VectorsConfig{
			Config: &pb.VectorsConfig_Params{
				Params: &pb.VectorParams{
					Size:     4,
					Distance: pb.Distance_COSINE,
				},
			},
		},
	})
	require.NoError(t, err)

	_, err = points.Upsert(ctx, &pb.UpsertPointsRequest{
		CollectionName: "grpc_col",
		Points: []*pb.PointStruct{
			{
				Id: &pb.PointId{PointIdOptions: &pb.PointId_Uuid{Uuid: "p1"}},
				Vectors: &pb.Vectors{
					VectorsOptions: &pb.Vectors_Vector{
						Vector: &pb.Vector{Data: []float32{1, 0, 0, 0}},
					},
				},
			},
		},
	})
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.FailedPrecondition, st.Code())
}
