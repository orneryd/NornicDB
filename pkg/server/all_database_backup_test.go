package server

import (
	"context"
	"net/http"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Public HTTP backup with a custom administrative role whose database policy
// explicitly refuses system. Inspect only our own disposable backup artifact.
func TestAllDatabaseBackupIncludesSystemAdmission(t *testing.T) {
	s, users := setupTestServer(t)
	ctx := context.Background()
	const role = "review-backup-operator"
	require.NoError(t, s.roleStore.CreateRole(ctx, role))
	require.NoError(t, s.roleEntitlementsStore.Set(ctx, role, []string{"read", "write", "admin"}))
	require.NoError(t, s.allowlistStore.SaveRoleDatabases(ctx, role, []string{"nornic", "system"}))
	require.NoError(t, s.privilegesStore.SavePrivilege(ctx, role, "nornic", true, true))
	require.NoError(t, s.privilegesStore.SavePrivilege(ctx, role, "system", false, false))
	_, err := users.CreateUser(role, "password123", []auth.Role{auth.Role(role)})
	require.NoError(t, err)
	token := "Bearer " + getAuthToken(t, users, role)
	system, err := s.dbManager.GetStorage("system")
	require.NoError(t, err)
	_, err = system.CreateNode(&storage.Node{ID: "review-system-sentinel", Labels: []string{"ReviewPrivate"}, Properties: map[string]any{"value": "review-owned-system-data"}})
	require.NoError(t, err)
	// This public route is the control for the identical effective principal.
	control := makeRequest(t, s, http.MethodGet, "/db/system", nil, token)
	require.Equal(t, http.StatusForbidden, control.Code, control.Body.String())
	path := filepath.Join(t.TempDir(), "review-backup.bin")
	response := makeRequest(t, s, http.MethodPost, "/admin/backup", map[string]any{"path": path}, token)
	t.Logf("same token: GET /db/system=%d; POST /admin/backup=%d %s", control.Code, response.Code, response.Body.String())
	if response.Code == http.StatusOK {
		restored, err := storage.NewBadgerEngine(filepath.Join(t.TempDir(), "restore"))
		require.NoError(t, err)
		defer restored.Close()
		require.NoError(t, restored.Restore(path))
		node, err := restored.GetNode("system:review-system-sentinel")
		require.NoError(t, err)
		require.Equal(t, "review-owned-system-data", node.Properties["value"])
		t.Log("backup contains the system sentinel despite explicit system read refusal")
	}
	// Grant and revoke system on this same token; permitted backup stays usable.
	for _, readable := range []bool{true, false} {
		require.NoError(t, s.privilegesStore.SavePrivilege(ctx, role, "system", readable, readable))
		next := makeRequest(t, s, http.MethodPost, "/admin/backup", map[string]any{"path": filepath.Join(t.TempDir(), "next.bin")}, token)
		t.Logf("system grant=%v: backup HTTP %d", readable, next.Code)
		if readable {
			require.Equal(t, http.StatusOK, next.Code, next.Body.String())
		} else {
			require.Equal(t, http.StatusForbidden, next.Code, "revoked system access must refuse a backup containing system")
		}
	}
	require.Equal(t, http.StatusForbidden, response.Code, "full backup must admit every included database")
}
