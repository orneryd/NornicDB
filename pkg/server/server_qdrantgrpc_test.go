package server

import (
	"context"
	"testing"
	"time"

	nornicConfig "github.com/orneryd/nornicdb/pkg/config"
	qpb "github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestServer_QdrantGRPCFeatureFlag_StartsAndCreatesCollectionDatabases(t *testing.T) {
	server, ctx, collections, points := qdrantGRPCTestClients(t, false)

	require.NotNil(t, server.qdrantGRPCServer)
	require.True(t, server.qdrantGRPCServer.IsRunning())

	_, err := collections.Create(ctx, &qpb.CreateCollection{
		CollectionName: "grpc_col",
		VectorsConfig: &qpb.VectorsConfig{
			Config: &qpb.VectorsConfig_Params{
				Params: &qpb.VectorParams{
					Size:     4,
					Distance: qpb.Distance_Cosine,
				},
			},
		},
	})
	require.NoError(t, err)

	_, err = points.Upsert(ctx, &qpb.UpsertPoints{
		CollectionName: "grpc_col",
		Points: []*qpb.PointStruct{
			{
				Id: &qpb.PointId{PointIdOptions: &qpb.PointId_Uuid{Uuid: "p1"}},
				Vectors: &qpb.Vectors{
					VectorsOptions: &qpb.Vectors_Vectors{
						Vectors: &qpb.NamedVectors{
							Vectors: map[string]*qpb.Vector{
								"a": {Vector: &qpb.Vector_Dense{Dense: &qpb.DenseVector{Data: []float32{1, 0, 0, 0}}}},
								"b": {Vector: &qpb.Vector_Dense{Dense: &qpb.DenseVector{Data: []float32{0, 1, 0, 0}}}},
							},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	// Verify the point exists in the collection database namespace.
	exec, err := server.executorCache.Get("grpc_col")
	require.NoError(t, err)

	res, err := exec.Execute(ctx, "MATCH (n:QdrantPoint) RETURN count(n) AS c", nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, int64(1), res.Rows[0][0])

	// Sanity check: default database should not contain the collection points.
	defaultExec, err := server.executorCache.Get(server.dbManager.DefaultDatabaseName())
	require.NoError(t, err)
	defaultRes, err := defaultExec.Execute(ctx, "MATCH (n:QdrantPoint) RETURN count(n) AS c", nil)
	require.NoError(t, err)
	require.Len(t, defaultRes.Rows, 1)
	require.Equal(t, int64(0), defaultRes.Rows[0][0])
}

func TestServer_QdrantGRPC_ManagedEmbeddings_DisablesVectorMutations(t *testing.T) {
	_, ctx, collections, points := qdrantGRPCTestClients(t, true)
	_, err := collections.Create(ctx, &qpb.CreateCollection{
		CollectionName: "grpc_col",
		VectorsConfig: &qpb.VectorsConfig{
			Config: &qpb.VectorsConfig_Params{
				Params: &qpb.VectorParams{Size: 4, Distance: qpb.Distance_Cosine},
			},
		},
	})
	require.NoError(t, err)

	_, err = points.Upsert(ctx, &qpb.UpsertPoints{
		CollectionName: "grpc_col",
		Points: []*qpb.PointStruct{{
			Id: &qpb.PointId{PointIdOptions: &qpb.PointId_Uuid{Uuid: "p1"}},
			Vectors: &qpb.Vectors{VectorsOptions: &qpb.Vectors_Vector{
				Vector: &qpb.Vector{Vector: &qpb.Vector_Dense{Dense: &qpb.DenseVector{Data: []float32{1, 0, 0, 0}}}},
			}},
		}},
	})
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.FailedPrecondition, st.Code())
}

// qdrantGRPCTestClients owns the identical transport fixture used by both
// managed and client-managed vector tests.
func qdrantGRPCTestClients(t *testing.T, managedEmbeddings bool) (*Server, context.Context, qpb.CollectionsClient, qpb.PointsClient) {
	t.Helper()
	server, authenticator := setupTestServer(t)

	server.config.EmbeddingEnabled = managedEmbeddings
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
	t.Cleanup(cancel)
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer "+getAuthToken(t, authenticator, "admin")))
	return server, ctx, qpb.NewCollectionsClient(conn), qpb.NewPointsClient(conn)
}
