package qdrantgrpc

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	qpb "github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGRPCMethodEffectContracts(t *testing.T) {
	expected := map[cypher.PrivilegeClass][]string{
		cypher.PrivilegeDBMS:  {"Collections/Create", "Collections/Update", "Collections/UpdateAliases", "Collections/CreateAlias", "Collections/DeleteAlias", "Collections/RenameAlias", "Collections/Delete"},
		cypher.PrivilegeRead:  {"Collections/Get", "Collections/List", "Collections/GetAliases", "Collections/CollectionExists", "Collections/GetCollectionClusterInfo", "Points/Search", "Points/SearchBatch", "Points/Scroll", "Points/Get", "Points/Count", "Points/Recommend", "Points/RecommendBatch", "Points/Discover", "Points/DiscoverBatch"},
		cypher.PrivilegeWrite: {"Points/Upsert", "Points/UpdateVectors", "Points/DeleteVectors", "Points/SetPayload", "Points/OverwritePayload", "Points/ClearPayload", "Points/Delete"},
		cypher.PrivilegeAdmin: {"ServerReflection/ServerReflectionInfo"},
		cypher.PrivilegeAdmin | cypher.PrivilegeRead | cypher.PrivilegeWrite | cypher.PrivilegeFileIO:   {"Snapshots/Create"},
		cypher.PrivilegeAdmin | cypher.PrivilegeRead | cypher.PrivilegeFileIO:                           {"Snapshots/List"},
		cypher.PrivilegeAdmin | cypher.PrivilegeWrite | cypher.PrivilegeFileIO:                          {"Snapshots/Delete"},
		cypher.PrivilegeAdmin | cypher.PrivilegeWrite | cypher.PrivilegeFileIO | cypher.PrivilegeRemote: {"Snapshots/Recover"},
	}
	count := 0
	for class, names := range expected {
		for _, name := range names {
			contract, ok := defaultMethodPermissions[name]
			require.True(t, ok, name)
			require.Equal(t, class, contract.Class, name)
			require.True(t, contract.Class.Valid(), name)
			count++
		}
	}
	require.Len(t, defaultMethodPermissions, count)
	config := DefaultConfig()
	config.MethodPermissions = map[string]auth.Permission{"Collections/Create": auth.PermWrite}
	s := &Server{config: config}
	// A configured method entitlement cannot lower its database-management
	// effect to a data write, including for custom roles.
	config.EntitlementsResolver = func([]string) []string { return []string{"read", "write"} }
	claims := &auth.JWTClaims{Roles: []string{"custom"}}
	require.Equal(t, codes.PermissionDenied, status.Code(s.authorizeMethod(context.Background(), "/qdrant.Collections/Create", claims)))
	config.EntitlementsResolver = func([]string) []string { return []string{"admin"} }
	require.NoError(t, s.authorizeMethod(context.Background(), "/qdrant.Collections/Create", claims))
	require.Equal(t, codes.PermissionDenied, status.Code(s.authorizeMethod(context.Background(), "/qdrant.Points/Query", claims)), "unadmitted methods remain unavailable")
}

type admissionCollectionStore struct {
	CollectionStore
	calls int
}

func (s *admissionCollectionStore) Open(context.Context, string) (storage.Engine, *CollectionMeta, error) {
	s.calls++
	return nil, nil, errors.New("unexpected resolution")
}
func (s *admissionCollectionStore) GetMeta(context.Context, string) (*CollectionMeta, error) {
	s.calls++
	return nil, errors.New("unexpected resolution")
}
func (s *admissionCollectionStore) Exists(string) bool { s.calls++; return false }
func (s *admissionCollectionStore) PointCount(context.Context, string) (int64, error) {
	s.calls++
	return 0, errors.New("unexpected resolution")
}

func TestGRPCSelectedReadDenialBeforeResolution(t *testing.T) {
	config := DefaultConfig()
	config.EntitlementsResolver = func([]string) []string { return []string{"admin"} }
	s := &Server{config: config,
		databaseAccessModeResolver: func([]string) auth.DatabaseAccessMode { return auth.FullDatabaseAccessMode },
		resolvedAccessResolver:     func([]string, string) auth.ResolvedAccess { return auth.ResolvedAccess{} },
	}
	ctx := context.WithValue(context.Background(), contextKeyClaims{}, &auth.JWTClaims{Roles: []string{"custom"}})
	store := &admissionCollectionStore{}
	points := NewPointsService(config, store, nil, newVectorIndexCache(), s)
	collections := NewCollectionsService(store, newVectorIndexCache(), s)
	snapshots := NewSnapshotsService(config, store, nil, t.TempDir(), s)
	for _, database := range []string{"existing", "missing"} {
		for _, call := range []func() error{
			func() error {
				_, err := collections.Get(ctx, &qpb.GetCollectionInfoRequest{CollectionName: database})
				return err
			},
			func() error {
				_, err := collections.CollectionExists(ctx, &qpb.CollectionExistsRequest{CollectionName: database})
				return err
			},
			func() error { _, err := points.Get(ctx, &qpb.GetPoints{CollectionName: database}); return err },
			func() error { _, err := points.Count(ctx, &qpb.CountPoints{CollectionName: database}); return err },
			func() error { _, err := points.Search(ctx, &qpb.SearchPoints{CollectionName: database}); return err },
			func() error { _, err := points.Scroll(ctx, &qpb.ScrollPoints{CollectionName: database}); return err },
			func() error {
				_, err := snapshots.Create(ctx, &qpb.CreateSnapshotRequest{CollectionName: database})
				return err
			},
		} {
			require.Equal(t, codes.PermissionDenied, status.Code(call()))
			require.Zero(t, store.calls)
		}
	}
}
