package server

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCompositeAggregateReadsRequirePhysicalDatabaseAccess(t *testing.T) {
	server, users := setupTestServer(t)
	ctx := context.Background()
	require.NoError(t, server.dbManager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "aggregate_child"}))
	engine, err := server.dbManager.GetStorage("aggregate_child")
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{ID: "hidden-count", Labels: []string{"Private"}})
	require.NoError(t, err)
	require.NoError(t, server.dbManager.CreateCompositeDatabase(t.Context(), "aggregate_root", []multidb.ConstituentRef{
		{Alias: "part", DatabaseName: "aggregate_child", Type: "local", AccessMode: "read_write"},
	}))
	const role = "aggregate-reader"
	require.NoError(t, server.roleStore.CreateRole(ctx, role))
	require.NoError(t, server.roleEntitlementsStore.Set(ctx, role, []string{"read", "admin"}))
	require.NoError(t, server.privilegesStore.SavePrivilege(ctx, role, "nornic", true, false))
	require.NoError(t, server.privilegesStore.SavePrivilege(ctx, role, "aggregate_root", true, false))
	_, err = users.CreateUser(role, "password123", []auth.Role{auth.Role(role)})
	require.NoError(t, err)
	token := "Bearer " + getAuthToken(t, users, role)
	for _, phase := range []struct {
		name         string
		read, access bool
	}{
		{"read-refused", false, true},
		{"access-refused", true, false},
		{"allowed", true, true},
	} {
		t.Run(phase.name, func(t *testing.T) {
			names := []string{"nornic", "aggregate_root"}
			if phase.access {
				names = append(names, "aggregate_child")
			}
			require.NoError(t, server.allowlistStore.SaveRoleDatabases(ctx, role, names))
			require.NoError(t, server.privilegesStore.SavePrivilege(ctx, role, "aggregate_child", phase.read, false))
			response := makeRequest(t, server, http.MethodGet, "/admin/stats", nil, token)
			require.Equal(t, http.StatusOK, response.Code)
			var payload struct {
				Database struct {
					NodeCount   int64                       `json:"node_count"`
					PerDatabase map[string]map[string]int64 `json:"per_database"`
				} `json:"database"`
			}
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &payload))
			if phase.read && phase.access {
				require.EqualValues(t, 1, payload.Database.PerDatabase["aggregate_root"]["node_count"])
			} else {
				require.NotContains(t, payload.Database.PerDatabase, "aggregate_root")
				require.Zero(t, payload.Database.NodeCount)
			}
		})
	}
}
