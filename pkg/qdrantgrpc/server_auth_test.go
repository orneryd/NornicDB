package qdrantgrpc

import (
	"context"
	"encoding/base64"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
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

type testServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *testServerStream) SetHeader(metadata.MD) error  { return nil }
func (s *testServerStream) SendHeader(metadata.MD) error { return nil }
func (s *testServerStream) SetTrailer(metadata.MD)       {}
func (s *testServerStream) Context() context.Context     { return s.ctx }
func (s *testServerStream) SendMsg(any) error            { return nil }
func (s *testServerStream) RecvMsg(any) error            { return nil }

func TestQdrantGRPC_AuthAndRBAC(t *testing.T) {
	t.Parallel()

	base := storage.NewMemoryEngine()
	dbm, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)

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

	srv, err := NewServerWithDatabaseManager(cfg, dbm, base, nil, authenticator)
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

func TestServer_DatabaseAccessHelpers(t *testing.T) {
	t.Parallel()

	base := storage.NewMemoryEngine()
	dbm, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	store, _ := newTestCollectionStore(t)

	cfg := DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"

	srv, err := NewServer(cfg, store, base, nil, nil)
	require.NoError(t, err)
	require.NoError(t, srv.AllowDatabaseAccess(context.Background(), "any", false))

	cfg.DatabaseAccessModeResolver = func(roles []string) auth.DatabaseAccessMode {
		_ = roles
		return auth.DenyAllDatabaseAccessMode
	}
	cfg.ResolvedAccessResolver = func(roles []string, dbName string) auth.ResolvedAccess {
		_ = roles
		_ = dbName
		return auth.ResolvedAccess{Read: true, Write: false}
	}

	srv2, err := NewServerWithDatabaseManager(cfg, dbm, base, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, srv2.CollectionStore())

	err = srv2.AllowDatabaseAccess(context.Background(), "db1", false)
	require.Error(t, err)

	visible, err := srv2.VisibleDatabases(context.Background(), []string{"db1", "db2"})
	require.NoError(t, err)
	require.Empty(t, visible)

	require.NoError(t, srv2.RegisterAdditionalServices(func(g *grpc.Server) {}))

	cfgNilMode := DefaultConfig()
	cfgNilMode.DatabaseAccessModeResolver = func(roles []string) auth.DatabaseAccessMode {
		_ = roles
		return nil
	}
	srvNilMode, err := NewServerWithDatabaseManager(cfgNilMode, dbm, base, nil, nil)
	require.NoError(t, err)
	visible, err = srvNilMode.VisibleDatabases(context.Background(), []string{"db1", "db2"})
	require.NoError(t, err)
	require.Equal(t, []string{"db1", "db2"}, visible)

	cfgAllow := DefaultConfig()
	cfgAllow.DatabaseAccessModeResolver = func(roles []string) auth.DatabaseAccessMode {
		_ = roles
		return auth.FullDatabaseAccessMode
	}
	cfgAllow.ResolvedAccessResolver = func(roles []string, dbName string) auth.ResolvedAccess {
		_ = roles
		_ = dbName
		return auth.ResolvedAccess{Read: true, Write: false}
	}
	srv3, err := NewServerWithDatabaseManager(cfgAllow, dbm, base, nil, nil)
	require.NoError(t, err)
	require.NoError(t, srv3.AllowDatabaseAccess(context.Background(), "db1", false))
	err = srv3.AllowDatabaseAccess(context.Background(), "db1", true)
	require.Error(t, err)
}

func TestServer_IsRunning_StreamAuthAndStreamContext(t *testing.T) {
	t.Parallel()

	base := storage.NewMemoryEngine()
	dbm, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	authStore := storage.NewMemoryEngine()
	authCfg := auth.DefaultAuthConfig()
	authCfg.JWTSecret = []byte("0123456789abcdef0123456789abcdef")
	authCfg.SecurityEnabled = true
	authenticator, err := auth.NewAuthenticator(authCfg, authStore)
	require.NoError(t, err)
	_, err = authenticator.CreateUser("viewer2", "ViewerPass123!", []auth.Role{auth.RoleViewer})
	require.NoError(t, err)

	cfg := DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"

	srv, err := NewServerWithDatabaseManager(cfg, dbm, base, nil, authenticator)
	require.NoError(t, err)
	require.False(t, srv.IsRunning())
	srv.started = true
	require.True(t, srv.IsRunning())
	srv.started = false
	require.False(t, srv.IsRunning())
	require.NotNil(t, srv.CollectionStore())

	// streamAuthInterceptor returns unauthenticated when metadata is missing.
	err = srv.streamAuthInterceptor(nil, &testServerStream{ctx: context.Background()}, &grpc.StreamServerInfo{
		FullMethod: "/qdrant.Points/Search",
	}, func(_ interface{}, _ grpc.ServerStream) error {
		return nil
	})
	require.Error(t, err)
	require.Equal(t, codes.Unauthenticated, status.Code(err))

	// Basic auth path should inject claims and wrap stream context.
	basic := "Basic " + base64.StdEncoding.EncodeToString([]byte("viewer2:ViewerPass123!"))
	mdCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", basic))
	ss := &testServerStream{ctx: mdCtx}

	err = srv.streamAuthInterceptor(nil, ss, &grpc.StreamServerInfo{
		FullMethod: "/qdrant.Points/Search",
	}, func(_ interface{}, stream grpc.ServerStream) error {
		claims, ok := stream.Context().Value(contextKeyClaims{}).(*auth.JWTClaims)
		require.True(t, ok)
		require.Equal(t, "viewer2", claims.Username)
		return nil
	})
	require.NoError(t, err)

	// Bearer auth path for stream interceptor.
	token, _, err := authenticator.Authenticate("viewer2", "ViewerPass123!", "127.0.0.1", "test")
	require.NoError(t, err)
	bearerCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+token.AccessToken))
	err = srv.streamAuthInterceptor(nil, &testServerStream{ctx: bearerCtx}, &grpc.StreamServerInfo{
		FullMethod: "/qdrant.Points/Search",
	}, func(_ interface{}, stream grpc.ServerStream) error {
		claims, ok := stream.Context().Value(contextKeyClaims{}).(*auth.JWTClaims)
		require.True(t, ok)
		require.Equal(t, "viewer2", claims.Username)
		return nil
	})
	require.NoError(t, err)

	custom := context.WithValue(context.Background(), "k", "v")
	wrapped := &authServerStream{ctx: custom}
	require.Equal(t, custom, wrapped.Context())
}

func TestServer_AuthorizeMethodBranches(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	cfg.MethodPermissions = map[string]auth.Permission{
		"Points/Search": auth.PermDelete,
	}

	store, _ := newTestCollectionStore(t)
	srv, err := NewServer(cfg, store, storage.NewMemoryEngine(), nil, nil)
	require.NoError(t, err)

	claimsViewer := &auth.JWTClaims{Roles: []string{string(auth.RoleViewer)}}
	claimsAdmin := &auth.JWTClaims{Roles: []string{string(auth.RoleAdmin)}}

	err = srv.authorizeMethod(context.Background(), "/qdrant.Points/Search", claimsViewer)
	require.Error(t, err)
	err = srv.authorizeMethod(context.Background(), "/qdrant.Points/Search", claimsAdmin)
	require.NoError(t, err)

	err = srv.authorizeMethod(context.Background(), "/qdrant.Unknown/Method", claimsAdmin)
	require.Error(t, err)
}

func TestServer_BasicAuthAndNoAuthInterceptorBranches(t *testing.T) {
	t.Parallel()

	store, _ := newTestCollectionStore(t)
	srvNoAuth, err := NewServer(DefaultConfig(), store, storage.NewMemoryEngine(), nil, nil)
	require.NoError(t, err)

	// Auth disabled paths call handler directly.
	_, err = srvNoAuth.unaryAuthInterceptor(context.Background(), nil, &grpc.UnaryServerInfo{
		FullMethod: "/qdrant.Points/Search",
	}, func(ctx context.Context, req interface{}) (interface{}, error) {
		_ = ctx
		_ = req
		return "ok", nil
	})
	require.NoError(t, err)

	err = srvNoAuth.streamAuthInterceptor(nil, &testServerStream{ctx: context.Background()}, &grpc.StreamServerInfo{
		FullMethod: "/qdrant.Points/Search",
	}, func(srv interface{}, ss grpc.ServerStream) error {
		_ = srv
		_ = ss
		return nil
	})
	require.NoError(t, err)

	// handleBasicAuth input validation branches.
	authStore := storage.NewMemoryEngine()
	authCfg := auth.DefaultAuthConfig()
	authCfg.JWTSecret = []byte("0123456789abcdef0123456789abcdef")
	authCfg.SecurityEnabled = true
	authenticator, err := auth.NewAuthenticator(authCfg, authStore)
	require.NoError(t, err)
	_, err = authenticator.CreateUser("viewer3", "ViewerPass123!", []auth.Role{auth.RoleViewer})
	require.NoError(t, err)

	srvAuth, err := NewServer(DefaultConfig(), store, storage.NewMemoryEngine(), nil, authenticator)
	require.NoError(t, err)

	_, err = srvAuth.handleBasicAuth(context.Background(), "Basic !!!")
	require.Error(t, err)

	badFormat := "Basic " + base64.StdEncoding.EncodeToString([]byte("no-colon"))
	_, err = srvAuth.handleBasicAuth(context.Background(), badFormat)
	require.Error(t, err)

	good := "Basic " + base64.StdEncoding.EncodeToString([]byte("viewer3:ViewerPass123!"))
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-forwarded-for", "1.2.3.4"))
	claims, err := srvAuth.handleBasicAuth(ctx, good)
	require.NoError(t, err)
	require.Equal(t, "viewer3", claims.Username)

	// Cache hit branch.
	claims2, err := srvAuth.handleBasicAuth(context.Background(), good)
	require.NoError(t, err)
	require.Equal(t, "viewer3", claims2.Username)

	// x-real-ip branch (no x-forwarded-for).
	ctxReal := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-real-ip", "5.6.7.8"))
	_, err = srvAuth.handleBasicAuth(ctxReal, good)
	require.NoError(t, err)
}

func TestServer_MiscBranches(t *testing.T) {
	t.Parallel()

	_, err := NewServer(nil, nil, storage.NewMemoryEngine(), nil, nil)
	require.Error(t, err)

	require.Nil(t, (*Server)(nil).CollectionStore())

	store, _ := newTestCollectionStore(t)
	srv, err := NewServer(DefaultConfig(), store, storage.NewMemoryEngine(), nil, nil)
	require.NoError(t, err)

	require.Equal(t, "", srv.Addr())
	srv.Stop() // not started branch

	err = srv.RegisterAdditionalServices(nil)
	require.Error(t, err)

	srv.started = true
	err = srv.RegisterAdditionalServices(func(g *grpc.Server) { _ = g })
	require.Error(t, err)
	err = srv.Start()
	require.Error(t, err) // already started
	srv.started = false

	srv.config.ListenAddr = "bad addr :::"
	err = srv.Start()
	require.Error(t, err)

	// Token extraction helper branches.
	_, err = srv.extractTokenFromMetadata(context.Background())
	require.Error(t, err)
	mdCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-api-key", "k"))
	token, err := srv.extractTokenFromMetadata(mdCtx)
	require.NoError(t, err)
	require.Equal(t, "k", token)

	// Permission/method helpers.
	_, ok := methodKey("")
	require.False(t, ok)
	_, ok = methodKey("bad")
	require.False(t, ok)
	_, ok = requiredPermissionForMethod("/bad")
	require.False(t, ok)

	require.False(t, hasPermissionFromClaims(nil, auth.PermRead))
	require.False(t, hasPermissionFromClaims(&auth.JWTClaims{Roles: []string{"unknown"}}, auth.PermRead))

	// VisibleDatabases with role-aware mode.
	cfg := DefaultConfig()
	cfg.DatabaseAccessModeResolver = func(roles []string) auth.DatabaseAccessMode {
		_ = roles
		return auth.FullDatabaseAccessMode
	}
	srvVis, err := NewServer(cfg, store, storage.NewMemoryEngine(), nil, nil)
	require.NoError(t, err)
	ctxClaims := context.WithValue(context.Background(), contextKeyClaims{}, &auth.JWTClaims{Roles: []string{"viewer"}})
	visible, err := srvVis.VisibleDatabases(ctxClaims, []string{"d1", "d2"})
	require.NoError(t, err)
	require.Equal(t, []string{"d1", "d2"}, visible)

	_, err = NewServerWithDatabaseManager(DefaultConfig(), nil, storage.NewMemoryEngine(), nil, nil)
	require.Error(t, err)
}

func TestServer_AuthInterceptorErrorBranches(t *testing.T) {
	t.Parallel()

	authStore := storage.NewMemoryEngine()
	authCfg := auth.DefaultAuthConfig()
	authCfg.JWTSecret = []byte("0123456789abcdef0123456789abcdef")
	authCfg.SecurityEnabled = true
	authenticator, err := auth.NewAuthenticator(authCfg, authStore)
	require.NoError(t, err)
	_, err = authenticator.CreateUser("viewer4", "ViewerPass123!", []auth.Role{auth.RoleViewer})
	require.NoError(t, err)

	store, _ := newTestCollectionStore(t)
	srv, err := NewServer(DefaultConfig(), store, storage.NewMemoryEngine(), nil, authenticator)
	require.NoError(t, err)

	// Basic malformed then missing bearer token.
	basicMalformed := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Basic !!!"))
	_, err = srv.unaryAuthInterceptor(basicMalformed, nil, &grpc.UnaryServerInfo{
		FullMethod: "/qdrant.Points/Search",
	}, func(ctx context.Context, req interface{}) (interface{}, error) {
		_ = ctx
		_ = req
		return nil, nil
	})
	require.Error(t, err)

	// Invalid bearer token path.
	invalidBearer := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer invalid"))
	_, err = srv.unaryAuthInterceptor(invalidBearer, nil, &grpc.UnaryServerInfo{
		FullMethod: "/qdrant.Points/Search",
	}, func(ctx context.Context, req interface{}) (interface{}, error) {
		_ = ctx
		_ = req
		return nil, nil
	})
	require.Error(t, err)

	// Stream invalid bearer token path.
	err = srv.streamAuthInterceptor(nil, &testServerStream{ctx: invalidBearer}, &grpc.StreamServerInfo{
		FullMethod: "/qdrant.Points/Search",
	}, func(srv interface{}, ss grpc.ServerStream) error {
		_ = srv
		_ = ss
		return nil
	})
	require.Error(t, err)
}
