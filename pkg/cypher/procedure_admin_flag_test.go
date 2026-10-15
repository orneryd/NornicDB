package cypher

import (
	"context"
	"testing"
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
