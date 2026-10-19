package qdrantgrpc

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/storage"
	qpb "github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestUnaryDatabaseAdmissionScopeAndLifetime(t *testing.T) {
	base := storage.NewMemoryEngine()
	defer base.Close()
	config := DefaultConfig()
	config.DatabaseAccessModeResolver = func([]string) auth.DatabaseAccessMode { return auth.FullDatabaseAccessMode }
	allowed := true
	resolutions := 0
	config.ResolvedAccessResolver = func(_ []string, database string) auth.ResolvedAccess {
		resolutions++
		return auth.ResolvedAccess{Read: allowed && database == "selected"}
	}
	store, _ := newTestCollectionStore(t)
	server, err := NewServer(config, store, base, nil, nil)
	require.NoError(t, err)
	claims := &auth.JWTClaims{Roles: []string{"viewer"}}
	ctx := &authenticatedUnaryContext{Context: context.Background(), server: server, claims: claims}
	require.NoError(t, server.authorizeRequestDatabase(ctx, &qpb.CountPoints{CollectionName: "selected"}, "/qdrant.Points/Count"))
	require.Same(t, claims, ctx.Value(contextKeyClaims{}))
	require.NoError(t, server.AllowDatabaseAccess(ctx, "selected", false))
	require.Equal(t, 1, resolutions, "one admission for one unary operation")
	require.Equal(t, codes.PermissionDenied, status.Code(server.AllowDatabaseAccess(ctx, "other", false)))
	require.Equal(t, codes.PermissionDenied, status.Code(server.AllowDatabaseAccess(ctx, "selected", true)))
	otherStore, _ := newTestCollectionStore(t)
	otherServer, err := NewServer(config, otherStore, base, nil, nil)
	require.NoError(t, err)
	allowed = false
	require.Equal(t, codes.PermissionDenied, status.Code(otherServer.AllowDatabaseAccess(ctx, "selected", false)), "another server cannot inherit admission")
	fresh := &authenticatedUnaryContext{Context: context.Background(), server: server, claims: claims}
	require.Equal(t, codes.PermissionDenied, status.Code(server.authorizeRequestDatabase(fresh, &qpb.CountPoints{CollectionName: "selected"}, "/qdrant.Points/Count")), "next RPC observes revocation")
	require.Equal(t, codes.PermissionDenied, status.Code(server.AllowDatabaseAccess(context.Background(), "selected", false)), "direct services still authorize")
}
