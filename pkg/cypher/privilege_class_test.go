package cypher

import (
	"context"
	"testing"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEveryBuiltinProcedureHasExpectedPrivilegeClass(t *testing.T) {
	ensureBuiltInProceduresRegistered()
	procedures := globalProcedureRegistry.ListBuiltIns()
	assert.Len(t, procedures, len(expectedBuiltinProcedureClasses))
	for _, procedure := range procedures {
		expected, found := expectedBuiltinProcedureClasses[procedure.Name]
		assert.True(t, found, "unreviewed procedure %q", procedure.Name)
		assert.Equal(t, expected, procedure.Privilege, procedure.Name)
		require.True(t, procedure.Privilege.Valid(), procedure.Name)
		require.Equal(t, requirementsForClass(expected).Admin, procedure.Admin, procedure.Name)
		require.Equal(t, requirementsForClass(expected).UpdatesDBMS, procedure.UpdatesDBMS, procedure.Name)
	}
}

func TestEveryBuiltinFunctionHasExpectedPrivilegeClass(t *testing.T) {
	seen := make(map[string]bool)
	for _, function := range cypherFunctionCatalog {
		expected, found := expectedBuiltinFunctionClasses[function.name]
		assert.True(t, found, "unreviewed function %q", function.name)
		assert.Equal(t, expected, function.privilege, function.name)
		seen[function.name] = true
	}
	implementations := cypherfn.Classes()
	for name, actual := range implementations {
		class, found := functionPrivilegeClass(name)
		require.True(t, found, name)
		assert.Equal(t, class, actual, name)
		var reviewed bool
		for expectedName := range expectedBuiltinFunctionClasses {
			if lowerASCII(expectedName) == name {
				reviewed = true
				seen[expectedName] = true
				break
			}
		}
		assert.True(t, reviewed, "unreviewed implementation %q", name)
	}
	for name := range expectedBuiltinFunctionClasses {
		assert.True(t, seen[name], "stale expected function %q", name)
	}
}

func TestProcedureRegistrationRequiresPrivilegeContract(t *testing.T) {
	handler := func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
		return &ExecuteResult{}, nil
	}
	for _, class := range []PrivilegeClass{0, 1 << 15, PrivilegeNone | PrivilegeWrite} {
		registry := NewProcedureRegistry()
		require.Error(t, registry.RegisterBuiltIn(ProcedureSpec{Name: "test.unclassified", Privilege: class}, handler))
		require.Empty(t, registry.List())
	}
	for _, mode := range []ProcedureMode{"", "unsupported"} {
		registry := NewProcedureRegistry()
		require.Error(t, registry.RegisterUser(ProcedureSpec{Name: "test.unclassified", Mode: mode}, handler))
		require.Empty(t, registry.List())
	}
	registry := NewProcedureRegistry()
	require.NoError(t, registry.RegisterBuiltIn(ProcedureSpec{Name: "test.value", Privilege: PrivilegeNone}, handler))
}

func TestFunctionRegistrationRequiresPrivilegeContract(t *testing.T) {
	handler := func(cypherfn.Context, []string) (interface{}, error) { return nil, nil }
	for _, class := range []PrivilegeClass{0, 1 << 15, PrivilegeNone | PrivilegeWrite} {
		require.Panics(t, func() { cypherfn.Register("test.unclassified", class, handler) })
		_, found := cypherfn.Class("test.unclassified")
		require.False(t, found)
	}
}
