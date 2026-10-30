package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type roleDeleteFailureEngine struct {
	storage.Engine
	deletes  int
	failRole bool
	failure  error
}

func (e *roleDeleteFailureEngine) GetInnerEngine() storage.Engine { return e.Engine }
func (e *roleDeleteFailureEngine) Namespace() string              { return "system" }

type userStatusFailureEngine struct{ storage.Engine }

type roleRenameFailureEngine struct {
	storage.Engine
	fired *bool
}

func (e *roleRenameFailureEngine) GetInnerEngine() storage.Engine { return e.Engine }
func (e *roleRenameFailureEngine) Namespace() string              { return "system" }

func (e *roleRenameFailureEngine) WithStoreTransaction(ctx context.Context, mutate func(storage.Engine) error) error {
	return auth.WithStoreTransaction(ctx, e.Engine, func(view storage.Engine) error {
		return mutate(&roleRenameFailureEngine{Engine: view, fired: e.fired})
	})
}

func (e *roleRenameFailureEngine) DeleteNode(id storage.NodeID) error {
	if id == "role_entitlement:ops" {
		*e.fired = true
		return errors.New("injected role rename failure")
	}
	return e.Engine.DeleteNode(id)
}

func TestRenamedRoleKeepsItsGrantsOverHTTP(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "failure"}[fail], func(t *testing.T) {
			ctx := context.Background()
			server, users := setupTestServer(t)
			admin := getAuthToken(t, users, "admin")
			system, err := server.dbManager.GetStorage("system")
			require.NoError(t, err)
			server.roleStore = auth.NewRoleStore(system)
			require.NoError(t, server.roleStore.CreateRole(ctx, "ops"))
			server.allowlistStore = auth.NewAllowlistStore(system)
			server.privilegesStore = auth.NewPrivilegesStore(system)
			server.roleEntitlementsStore = auth.NewRoleEntitlementsStore(system)
			require.NoError(t, server.allowlistStore.SaveRoleDatabases(ctx, "ops", []string{"nornic"}))
			require.NoError(t, server.privilegesStore.SavePrivilege(ctx, "ops", "nornic", true, true))
			require.NoError(t, server.roleEntitlementsStore.Set(ctx, "ops", []string{"read", "write"}))
			_, err = users.CreateUser("worker", "password123", []auth.Role{"ops"})
			require.NoError(t, err)
			require.Empty(t, txErrorCode(t, server, "system", admin, "GRANT EXECUTE BOOSTED PROCEDURE db.cdc.* ON DBMS TO ops"))
			before, err := system.AllNodes()
			require.NoError(t, err)
			fired := false
			if fail {
				server.roleStore = auth.NewRoleStore(&roleRenameFailureEngine{Engine: system, fired: &fired})
				require.NoError(t, server.roleStore.Load(ctx))
			}
			resp := makeRequest(t, server, "PATCH", "/auth/roles/ops", map[string]any{"name": "renamed"}, "Bearer "+admin)
			name := "renamed"
			if fail {
				require.Equal(t, http.StatusInternalServerError, resp.Code, resp.Body.String())
				require.True(t, fired, resp.Body.String())
				after, err := system.AllNodes()
				require.NoError(t, err)
				require.ElementsMatch(t, before, after)
				name = "ops"
			} else {
				require.Equal(t, http.StatusOK, resp.Code, resp.Body.String())
				require.NoError(t, server.roleStore.CreateRole(ctx, "ops"))
				require.False(t, server.ProcedureGrantChecker([]string{"ops"})("db.cdc.query"))
			}
			require.True(t, server.roleStore.Exists(name))
			require.True(t, server.ProcedureGrantChecker([]string{name})("db.cdc.query"))
			require.Equal(t, []string{"nornic"}, server.allowlistStore.Allowlist()[name])
			if !fail {
				require.NotContains(t, server.allowlistStore.Allowlist(), "ops")
			}
			require.Equal(t, auth.ResolvedAccess{Read: true, Write: true}, grantedAccess(server.privilegesStore, []string{name}, "nornic"))
			require.Equal(t, []string{"read", "write"}, server.roleEntitlementsStore.Get(name))
			worker, err := users.GetUser("worker")
			require.NoError(t, err)
			require.Equal(t, []auth.Role{auth.Role(name)}, worker.Roles)
		})
	}
}

func TestDeletedRoleStartsEmptyAfterRecreationOverHTTP(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "failure"}[fail], func(t *testing.T) {
			ctx := context.Background()
			server, users := setupTestServer(t)
			admin := getAuthToken(t, users, "admin")
			system, err := server.dbManager.GetStorage("system")
			require.NoError(t, err)
			server.roleStore = auth.NewRoleStore(system)
			require.NoError(t, server.roleStore.CreateRole(ctx, "ops"))
			server.allowlistStore = auth.NewAllowlistStore(system)
			server.privilegesStore = auth.NewPrivilegesStore(system)
			server.roleEntitlementsStore = auth.NewRoleEntitlementsStore(system)
			require.NoError(t, server.allowlistStore.SaveRoleDatabases(ctx, "ops", []string{"nornic"}))
			require.NoError(t, server.privilegesStore.SavePrivilege(ctx, "ops", "nornic", true, true))
			require.NoError(t, server.roleEntitlementsStore.Set(ctx, "ops", []string{"read", "write"}))
			_, err = users.CreateUser("worker", "password123", []auth.Role{"ops", auth.RoleViewer})
			require.NoError(t, err)
			require.Empty(t, txErrorCode(t, server, "system", admin, "GRANT EXECUTE BOOSTED PROCEDURE db.cdc.* ON DBMS TO ops"))
			before, err := system.AllNodes()
			require.NoError(t, err)
			fired := false
			if fail {
				server.roleStore = auth.NewRoleStore(&roleRenameFailureEngine{Engine: system, fired: &fired})
				require.NoError(t, server.roleStore.Load(ctx))
			}
			resp := makeRequest(t, server, "DELETE", "/auth/roles/ops", nil, "Bearer "+admin)
			worker, err := users.GetUser("worker")
			require.NoError(t, err)
			if fail {
				require.Equal(t, http.StatusInternalServerError, resp.Code, resp.Body.String())
				require.True(t, fired, resp.Body.String())
				after, err := system.AllNodes()
				require.NoError(t, err)
				require.ElementsMatch(t, before, after)
				require.True(t, server.roleStore.Exists("ops"))
				require.True(t, server.ProcedureGrantChecker([]string{"ops"})("db.cdc.query"))
				require.Equal(t, []string{"nornic"}, server.allowlistStore.Allowlist()["ops"])
				require.Equal(t, auth.ResolvedAccess{Read: true, Write: true}, grantedAccess(server.privilegesStore, []string{"ops"}, "nornic"))
				require.Equal(t, []string{"read", "write"}, server.roleEntitlementsStore.Get("ops"))
				require.Equal(t, []auth.Role{"ops", auth.RoleViewer}, worker.Roles)
			} else {
				require.Equal(t, http.StatusOK, resp.Code, resp.Body.String())
				require.False(t, server.roleStore.Exists("ops"))
				require.NoError(t, server.roleStore.CreateRole(ctx, "ops"))
				require.False(t, server.ProcedureGrantChecker([]string{"ops"})("db.cdc.query"))
				require.NotContains(t, server.allowlistStore.Allowlist(), "ops")
				require.Equal(t, auth.ResolvedAccess{}, grantedAccess(server.privilegesStore, []string{"ops"}, "nornic"))
				require.Empty(t, server.roleEntitlementsStore.Get("ops"))
				require.Equal(t, []auth.Role{auth.RoleViewer}, worker.Roles)
			}
		})
	}
}

func (e *userStatusFailureEngine) WithStoreTransaction(ctx context.Context, mutate func(storage.Engine) error) error {
	return auth.WithStoreTransaction(ctx, e.Engine, func(view storage.Engine) error {
		return mutate(&userStatusFailureEngine{Engine: view})
	})
}

func (e *userStatusFailureEngine) UpdateNode(updateRequest storage.NodeUpdateRequest) error {
	node := updateRequest.After
	if node.Properties["disabled"] == true {
		return errors.New("injected user status failure")
	}
	return e.Engine.UpdateNode(updateRequest)
}

func TestCombinedUserChangeFailureChangesNothingOverHTTP(t *testing.T) {
	server, _ := setupTestServer(t)
	system, err := server.dbManager.GetStorage("system")
	require.NoError(t, err)
	fault := &userStatusFailureEngine{Engine: system}
	config := auth.DefaultAuthConfig()
	config.JWTSecret = []byte("test-secret-key-for-testing-only-32b")
	config.BcryptCost, config.DefaultAdminUsername = 4, ""
	users, err := auth.NewAuthenticator(config, fault)
	require.NoError(t, err)
	server.auth = users
	_, err = users.CreateUser("worker", "password123", []auth.Role{auth.RoleViewer})
	require.NoError(t, err)
	before, err := system.GetNode("user:worker")
	require.NoError(t, err)
	admin := getAuthToken(t, users, "admin")
	resp := makeRequest(t, server, "PUT", "/auth/users/worker", map[string]any{
		"roles": []string{"admin"}, "disabled": true,
	}, "Bearer "+admin)
	assert.Equal(t, http.StatusBadRequest, resp.Code, resp.Body.String())
	after, err := system.GetNode("user:worker")
	require.NoError(t, err)
	assert.Equal(t, before, after)
	user, err := users.GetUser("worker")
	require.NoError(t, err)
	assert.Equal(t, []auth.Role{auth.RoleViewer}, user.Roles)
	assert.False(t, user.Disabled)
}

func (e *roleDeleteFailureEngine) WithStoreTransaction(ctx context.Context, mutate func(storage.Engine) error) error {
	return auth.WithStoreTransaction(ctx, e.Engine, func(view storage.Engine) error {
		staged := *e
		staged.Engine = view
		return mutate(&staged)
	})
}

func (e *roleDeleteFailureEngine) DeleteNode(id storage.NodeID) error {
	if strings.HasPrefix(string(id), "proc_priv:") {
		e.deletes++
		if !e.failRole && e.deletes == 2 {
			return errors.New("injected grant deletion failure")
		}
	}
	if e.failRole && id == "role:ops" {
		if e.failure != nil {
			return e.failure
		}
		return errors.New("injected role deletion failure")
	}
	return e.Engine.DeleteNode(id)
}

func TestDeleteRoleFailureChangesNothingOverHTTP(t *testing.T) {
	for _, scenario := range []struct {
		name     string
		failRole bool
		failure  error
	}{{"second grant", false, nil}, {"role node", true, nil}, {"role-exists backend error", true, auth.ErrRoleExists}} {
		t.Run(scenario.name, func(t *testing.T) {
			ctx := context.Background()
			server, users := setupTestServer(t)
			admin := getAuthToken(t, users, "admin")
			system, err := server.dbManager.GetStorage("system")
			require.NoError(t, err)
			fault := &roleDeleteFailureEngine{Engine: system, failRole: scenario.failRole, failure: scenario.failure}
			server.roleStore = auth.NewRoleStore(fault)
			require.NoError(t, server.roleStore.CreateRole(ctx, "ops"))
			grants := auth.NewProcedurePrivilegeStore(system)
			require.NoError(t, grants.Grant(ctx, "ops", "db.a", true, false))
			require.NoError(t, grants.Grant(ctx, "ops", "db.b", false, true))
			before, err := grants.List(ctx)
			require.NoError(t, err)
			resp := makeRequest(t, server, "DELETE", "/auth/roles/ops", nil, "Bearer "+admin)
			require.Equal(t, http.StatusInternalServerError, resp.Code, resp.Body.String())
			require.True(t, server.roleStore.Exists("ops"))
			_, err = system.GetNode("role:ops")
			require.NoError(t, err)
			after, err := grants.List(ctx)
			require.NoError(t, err)
			require.Equal(t, before, after)
		})
	}
}

func TestDropRoleUnsupportedOverHTTPChangesNothing(t *testing.T) {
	ctx := context.Background()
	server, users := setupTestServer(t)
	admin := getAuthToken(t, users, "admin")
	require.NoError(t, server.roleStore.CreateRole(ctx, "ops"))
	require.Empty(t, txErrorCode(t, server, "system", admin, "GRANT EXECUTE BOOSTED PROCEDURE db.a ON DBMS TO ops"))
	for query, code := range map[string]string{
		"CREATE ROLE new":             "Neo.ClientError.Statement.SemanticError",
		"DROP ROLE ops":               "Neo.ClientError.Statement.UnsupportedAdministrationCommand",
		"RENAME ROLE ops TO new":      "Neo.ClientError.Statement.SyntaxError",
		"GRANT ROLE ops TO reader":    "Neo.ClientError.Statement.UnsupportedAdministrationCommand",
		"REVOKE ROLE ops FROM reader": "Neo.ClientError.Statement.UnsupportedAdministrationCommand",
	} {
		require.Equal(t, code, txErrorCode(t, server, "system", admin, query))
	}
	require.True(t, server.roleStore.Exists("ops"))
	require.True(t, server.ProcedureGrantChecker([]string{"ops"})("db.a"))
}

func TestAdministrationRequestRoutesToSystemAndRollsBackOnRejection(t *testing.T) {
	for _, reject := range []bool{false, true} {
		t.Run(map[bool]string{false: "two grants", true: "procedure after grant"}[reject], func(t *testing.T) {
			ctx := context.Background()
			server, users := setupTestServer(t)
			admin := getAuthToken(t, users, "admin")
			second := "GRANT EXECUTE PROCEDURE db.b ON DBMS TO viewer"
			if reject {
				second = "CALL dbms.components()"
			}
			resp := makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]any{
				"statements": []map[string]any{
					{"statement": "GRANT EXECUTE PROCEDURE db.a ON DBMS TO viewer"},
					{"statement": second},
				},
			}, "Bearer "+admin)
			var body struct {
				Errors []struct {
					Code string `json:"code"`
				} `json:"errors"`
			}
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
			if reject {
				require.Len(t, body.Errors, 1)
				require.Equal(t, "Neo.ClientError.Transaction.ForbiddenDueToTransactionType", body.Errors[0].Code)
			} else {
				require.Empty(t, body.Errors)
			}
			system, err := server.dbManager.GetStorage("system")
			require.NoError(t, err)
			grants, err := auth.NewProcedurePrivilegeStore(system).List(ctx)
			require.NoError(t, err)
			if reject {
				require.Empty(t, grants)
			} else {
				require.Len(t, grants, 2)
			}
		})
	}
}

func TestRoleCatalogHTTPMetadataViews(t *testing.T) {
	server, users := setupTestServer(t)
	admin := getAuthToken(t, users, "admin")
	system, err := server.dbManager.GetStorage("system")
	require.NoError(t, err)
	server.roleStore = auth.NewRoleStore(system)
	server.roleEntitlementsStore = auth.NewRoleEntitlementsStore(system)
	for _, role := range []string{"zeta", "alpha"} {
		require.NoError(t, server.roleStore.CreateRole(context.Background(), role))
	}
	require.NoError(t, server.roleEntitlementsStore.Set(context.Background(), "alpha", []string{"read"}))

	response := makeRequest(t, server, http.MethodGet, "/auth/roles", nil, "Bearer "+admin)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	var names []string
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &names))
	require.Equal(t, []string{"admin", "editor", "viewer", "alpha", "zeta"}, names)

	response = makeRequest(t, server, http.MethodGet, "/auth/role-entitlements", nil, "Bearer "+admin)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	var entries []struct {
		Role         string   `json:"role"`
		Entitlements []string `json:"entitlements"`
	}
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &entries))
	byRole := make(map[string][]string, len(entries))
	returnedNames := make([]string, 0, len(entries))
	for _, entry := range entries {
		returnedNames = append(returnedNames, entry.Role)
		_, duplicate := byRole[entry.Role]
		require.False(t, duplicate)
		byRole[entry.Role] = entry.Entitlements
	}
	require.Len(t, byRole, len(names))
	require.Equal(t, names, returnedNames)
	require.Equal(t, []string{"read"}, byRole["alpha"])
	require.Nil(t, byRole["zeta"])
	require.Contains(t, byRole["admin"], "admin")
	require.Contains(t, byRole["viewer"], "read")
}
