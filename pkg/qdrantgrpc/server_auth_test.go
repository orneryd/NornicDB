package qdrantgrpc

import (
	"context"
	"encoding/base64"
	"testing"

	qpb "github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestQdrantGRPC_AuthAndRBAC(t *testing.T) {
	t.Parallel()

	store := storage.NewMemoryEngine()
	registry, err := NewPersistentCollectionRegistry(store)
	require.NoError(t, err)
	t.Cleanup(func() { _ = registry.Close() })

	authStore := storage.NewMemoryEngine()
	authCfg := auth.DefaultAuthConfig()
	authCfg.JWTSecret = []byte("0123456789abcdef0123456789abcdef")
	authCfg.SecurityEnabled = true
	authenticator, err := auth.NewAuthenticator(authCfg, authStore)
	require.NoError(t, err)

	_, err = authenticator.CreateUser("admin", "AdminPass123!", []auth.Role{auth.RoleAdmin})
	require.NoError(t, err)
	_, err = authenticator.CreateUser("viewer", "ViewerPass123!", []auth.Role{auth.RoleViewer})
	require.NoError(t, err)

	adminTok, _, err := authenticator.Authenticate("admin", "AdminPass123!", "127.0.0.1", "test")
	require.NoError(t, err)
	viewerTok, _, err := authenticator.Authenticate("viewer", "ViewerPass123!", "127.0.0.1", "test")
	require.NoError(t, err)

	cfg := DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.EnableReflection = false

	srv, err := NewServer(cfg, store, registry, nil, authenticator)
	require.NoError(t, err)
	require.NoError(t, srv.Start())
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(srv.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	collections := qpb.NewCollectionsClient(conn)
	points := qpb.NewPointsClient(conn)

	ctxNoAuth := context.Background()
	_, err = collections.List(ctxNoAuth, &qpb.ListCollectionsRequest{})
	require.Error(t, err)
	require.Equal(t, codes.Unauthenticated, status.Code(err))

	ctxAdmin := metadata.NewOutgoingContext(ctxNoAuth, metadata.Pairs("authorization", "Bearer "+adminTok.AccessToken))
	ctxViewer := metadata.NewOutgoingContext(ctxNoAuth, metadata.Pairs("authorization", "Bearer "+viewerTok.AccessToken))

	// Admin can create.
	_, err = collections.Create(ctxAdmin, &qpb.CreateCollection{
		CollectionName: "test",
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

	// Viewer cannot create.
	_, err = collections.Create(ctxViewer, &qpb.CreateCollection{
		CollectionName: "viewer_forbidden",
		VectorsConfig: &qpb.VectorsConfig{
			Config: &qpb.VectorsConfig_Params{
				Params: &qpb.VectorParams{
					Size:     4,
					Distance: qpb.Distance_Cosine,
				},
			},
		},
	})
	require.Error(t, err)
	require.Equal(t, codes.PermissionDenied, status.Code(err))

	// Viewer can read.
	_, err = collections.Get(ctxViewer, &qpb.GetCollectionInfoRequest{CollectionName: "test"})
	require.NoError(t, err)

	// Viewer cannot upsert.
	_, err = points.Upsert(ctxViewer, &qpb.UpsertPoints{
		CollectionName: "test",
		Points: []*qpb.PointStruct{
			{
				Id: &qpb.PointId{PointIdOptions: &qpb.PointId_Uuid{Uuid: "p1"}},
				Vectors: &qpb.Vectors{
					VectorsOptions: &qpb.Vectors_Vector{
						Vector: &qpb.Vector{
							Vector: &qpb.Vector_Dense{Dense: &qpb.DenseVector{Data: []float32{1, 0, 0, 0}}},
						},
					},
				},
			},
		},
	})
	require.Error(t, err)
	require.Equal(t, codes.PermissionDenied, status.Code(err))

	// Basic auth also works for read ops (Neo4j compatibility).
	basic := "Basic " + base64.StdEncoding.EncodeToString([]byte("viewer:ViewerPass123!"))
	ctxViewerBasic := metadata.NewOutgoingContext(ctxNoAuth, metadata.Pairs("authorization", basic))
	_, err = collections.Get(ctxViewerBasic, &qpb.GetCollectionInfoRequest{CollectionName: "test"})
	require.NoError(t, err)
}
