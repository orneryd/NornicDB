package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Authorization asks for admin for a procedure call exactly when the
// procedure's Admin flag (the admin column of SHOW PROCEDURES) is set: the
// flag is the one source of that requirement, for every registered
// procedure.
func TestProcedureAdminFlagIsTheAuthorizationRequirement(t *testing.T) {
	ensureBuiltInProceduresRegistered()
	procedures := globalProcedureRegistry.List()
	if len(procedures) == 0 {
		t.Fatal("no procedures registered")
	}
	for _, procedure := range procedures {
		requirements := QueryPermissionRequirements("CALL " + procedure.Name + "()")
		if requirements.Admin != procedure.Admin {
			t.Errorf("%s: authorization requires admin=%v, Admin flag is %v", procedure.Name, requirements.Admin, procedure.Admin)
		}
	}
}

// A user-defined or plugin procedure declares a mode only: an ADMIN or DBMS
// one is admin-only, any other is not.
func TestUserProcedureModeSetsAdminFlag(t *testing.T) {
	registry := NewProcedureRegistry()
	handler := func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
		return &ExecuteResult{}, nil
	}
	for mode, admin := range map[ProcedureMode]bool{ProcedureModeAdmin: true, ProcedureModeDBMS: true, ProcedureModeRead: false, ProcedureModeWrite: false} {
		name := "custom.mode" + string(mode)
		if err := registry.RegisterUser(ProcedureSpec{Name: name, Mode: mode}, handler); err != nil {
			t.Fatal(err)
		}
		procedure, _ := registry.Get(name)
		if procedure.Spec.Admin != admin {
			t.Errorf("%s: Admin = %v, want %v", mode, procedure.Spec.Admin, admin)
		}
	}
}

// dbms.listConnections lists every connection to a principal holding the
// admin entitlement, whatever its role is called, and only its own to any
// other, as in Neo4j. Its result is never served from the result cache the
// callers of one executor share: a reader calling after an administrator
// still sees only its own connection.
func TestListConnectionsShowsOtherUsersOnlyToAdministrators(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic"))
	connections := func() []ConnectionListing {
		return []ConnectionListing{{ConnectionID: "bolt-1", Username: "reader"}, {ConnectionID: "bolt-2", Username: "other"}}
	}
	for _, c := range []struct {
		name        string
		roles       []string
		permissions []string
		want        []string
	}{
		{"role with the admin entitlement", []string{"ops"}, []string{"read", "admin"}, []string{"bolt-1", "bolt-2"}},
		{"reader after an administrator", []string{"viewer"}, []string{"read"}, []string{"bolt-1"}},
		{"role named admin without the admin entitlement", []string{"admin"}, []string{"read"}, []string{"bolt-1"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			ctx := WithRequestIdentity(context.Background(), &RequestIdentity{
				User:        &AuthenticatedUser{Name: "reader", Roles: c.roles},
				Connections: connections,
			})
			ctx = WithPermissionChecker(ctx, func(permission string) bool {
				for _, held := range c.permissions {
					if held == permission {
						return true
					}
				}
				return false
			})
			result, err := exec.Execute(ctx, "CALL dbms.listConnections() YIELD connectionId RETURN connectionId", nil)
			require.NoError(t, err)
			var listed []string
			for _, row := range result.Rows {
				listed = append(listed, row[0].(string))
			}
			require.Equal(t, c.want, listed)
		})
	}
}
