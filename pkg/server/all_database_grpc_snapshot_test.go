package server

import (
	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"net/http"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/qdrantgrpc"
	"github.com/orneryd/nornicdb/pkg/storage"
	qpb "github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestAllDatabaseGRPCSnapshotsIncludeSystemAdmission(t *testing.T) {
	s, users := setupTestServer(t)
	ctx := context.Background()
	role := string(auth.RoleAdmin)
	require.NoError(t, s.allowlistStore.SaveRoleDatabases(ctx, role, []string{"nornic", "system"}))
	require.NoError(t, s.privilegesStore.SavePrivilege(ctx, role, "nornic", true, true))
	system, err := s.dbManager.GetStorage("system")
	require.NoError(t, err)
	_, err = system.CreateNode(&storage.Node{ID: "grpc-system-sentinel", Labels: []string{"ReviewPrivate"}, Properties: map[string]any{"value": "review-owned-grpc-system-data"}})
	require.NoError(t, err)
	cfg := qdrantgrpc.DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.SnapshotDir = t.TempDir()
	// Exact production admission callbacks from startQdrantGRPC.
	cfg.DatabaseAccessModeResolver = s.GetDatabaseAccessModeForRoles
	cfg.ResolvedAccessResolver = s.grpcDatabaseAccessForRoles
	cfg.DatabaseOperationAccessResolver = s.databaseAccessForRoles
	service, err := qdrantgrpc.NewServerWithDatabaseManager(cfg, s.dbManager, s.db.GetBaseStorageForManager(), nil, users)
	require.NoError(t, err)
	require.NoError(t, service.Start())
	t.Cleanup(service.Stop)
	conn, err := grpc.NewClient(service.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	token := getAuthToken(t, users, "admin")
	caller := metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer "+token))
	snapshots := qpb.NewSnapshotsClient(conn)
	for _, readable := range []bool{false, true, false} {
		require.NoError(t, s.privilegesStore.SavePrivilege(ctx, role, "system", readable, readable))
		control := makeRequest(t, s, http.MethodGet, "/db/system", nil, "Bearer "+token)
		if !readable {
			require.Equal(t, http.StatusForbidden, control.Code, control.Body.String())
		}
		response, err := snapshots.CreateFull(caller, &qpb.CreateFullSnapshotRequest{})
		t.Logf("system read=%v; /db/system=%d; gRPC CreateFull=%s", readable, control.Code, status.Code(err))
		if readable {
			require.NoError(t, err)
		} else {
			if status.Code(err) != codes.PermissionDenied {
				t.Errorf("unreadable system included by CreateFull: %v", err)
			}
		}
		if readable {
			description := response.GetSnapshotDescription()
			require.NotNil(t, description)
			restored, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(filepath.Join(t.TempDir(), "restore")))
			if restored != nil {
				fixtureowner.Own(t, restored)
			}
			require.NoError(t, err)
			require.NoError(t, restored.Restore(filepath.Join(cfg.SnapshotDir, "full", description.GetName())))
			node, err := restored.GetNode("system:grpc-system-sentinel")
			require.NoError(t, err)
			require.Equal(t, "review-owned-grpc-system-data", node.Properties["value"])
			require.NoError(t, restored.Close())
			t.Logf("public snapshot size=%d; granted artifact contains the system sentinel", description.GetSize())
		}
		_, listErr := snapshots.ListFull(caller, &qpb.ListFullSnapshotsRequest{})
		t.Logf("system read=%v; gRPC ListFull=%s", readable, status.Code(listErr))
		if readable {
			require.NoError(t, listErr)
		} else {
			require.Equal(t, codes.PermissionDenied, status.Code(listErr))
		}
		snapshotName := "refused.snapshot"
		if readable {
			snapshotName = response.GetSnapshotDescription().GetName()
		}
		_, deleteErr := snapshots.DeleteFull(caller, &qpb.DeleteFullSnapshotRequest{SnapshotName: snapshotName})
		if readable {
			require.NoError(t, deleteErr)
		} else {
			require.Equal(t, codes.PermissionDenied, status.Code(deleteErr))
		}
	}
}
