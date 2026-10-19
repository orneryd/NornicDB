package server

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	nornicConfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/multidb"
	qpb "github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestGRPCConstituentReadsUsePhysicalDatabasePrivileges(t *testing.T) {
	server, users := setupTestServer(t)
	server.config.EmbeddingEnabled = false
	server.config.Features = &nornicConfig.FeatureFlagsConfig{
		QdrantGRPCEnabled: true, QdrantGRPCListenAddr: "127.0.0.1:0",
		QdrantGRPCMaxVectorDim: 4096, QdrantGRPCMaxBatchPoints: 1000, QdrantGRPCMaxTopK: 1000,
	}
	require.NoError(t, server.Start())
	ctx := context.Background()
	require.NoError(t, server.qdrantGRPCServer.CollectionStore().Create(ctx, "grpc_physical", 4, qpb.Distance_Cosine))
	require.NoError(t, server.dbManager.CreateCompositeDatabase("grpc_portfolio", []multidb.ConstituentRef{
		{Alias: "part", DatabaseName: "grpc_physical", Type: "local", AccessMode: "read_write"},
	}))
	require.NoError(t, server.privilegesStore.SavePrivilege(ctx, string(auth.RoleViewer), "grpc_portfolio", true, false))
	connection, err := grpc.NewClient(server.qdrantGRPCServer.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = connection.Close() })
	collections := qpb.NewCollectionsClient(connection)
	points := qpb.NewPointsClient(connection)
	reader := metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer "+getAuthToken(t, users, "reader")))
	admin := metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer "+getAuthToken(t, users, "admin")))
	for _, readable := range []bool{false, true} {
		require.NoError(t, server.privilegesStore.SavePrivilege(ctx, string(auth.RoleViewer), "grpc_physical", readable, false))
		phase := "denied"
		if readable { phase = "allowed" }
		for _, selection := range []string{"grpc_physical", "grpc_portfolio.part", "grpc_portfolio"} {
			t.Run(phase+"/"+selection, func(t *testing.T) {
			_, getErr := collections.Get(reader, &qpb.GetCollectionInfoRequest{CollectionName: selection})
			_, countErr := points.Count(reader, &qpb.CountPoints{CollectionName: selection})
			if readable {
				require.NoError(t, getErr, selection)
				require.NoError(t, countErr, selection)
			} else {
				require.Equal(t, codes.PermissionDenied, status.Code(getErr), "read bypass via %s: %v", selection, getErr)
				require.Equal(t, codes.PermissionDenied, status.Code(countErr), "read bypass via %s: %v", selection, countErr)
			}
			_, err := collections.Get(admin, &qpb.GetCollectionInfoRequest{CollectionName: selection})
			require.NoError(t, err, "admin lost supported selection %s", selection)
			})
		}
	}
}
