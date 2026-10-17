package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// withPurePluginContracts keeps existing scalar fixtures explicit about their
// value-only contract. The product path never infers a class from a callback.
func withPurePluginContracts(lookup func(string) (interface{}, bool)) func(string) (interface{}, bool) {
	return func(name string) (interface{}, bool) {
		handler, found := lookup(name)
		if !found {
			return nil, false
		}
		return PluginFunctionContract{Handler: handler, Privilege: PrivilegeNone}, true
	}
}

func TestPluginFunctionContractBeforeInvocation(t *testing.T) {
	previous := PluginFunctionLookup
	t.Cleanup(func() { PluginFunctionLookup = previous })
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "plugin_contract"))
	called := 0
	handler := func() int64 { called++; return 1 }
	readOnly := WithPermissionChecker(context.Background(), func(permission string) bool { return permission == "read" })
	for _, query := range []string{"RETURN custom.effect()", "UNWIND [1] AS x RETURN custom.effect()", "WITH 1 AS x WHERE custom.effect() = 1 RETURN x"} {
		for _, contract := range []interface{}{
			handler,
			PluginFunctionContract{Handler: handler},
			PluginFunctionContract{Handler: handler, Privilege: PrivilegeWrite},
		} {
			PluginFunctionLookup = func(name string) (interface{}, bool) { return contract, name == "custom.effect" }
			_, err := executor.Execute(readOnly, query, nil)
			require.Error(t, err, query)
			require.Zero(t, called, query)
		}
	}
	PluginFunctionLookup = func(name string) (interface{}, bool) {
		return PluginFunctionContract{Handler: handler, Privilege: PrivilegeNone}, name == "custom.effect"
	}
	_, err := executor.Execute(readOnly, "RETURN custom.effect()", nil)
	require.NoError(t, err)
	require.Positive(t, called)
}

func TestBoostedProcedureKeepsUnrelatedFunctionAdmission(t *testing.T) {
	previous := PluginFunctionLookup
	t.Cleanup(func() { PluginFunctionLookup = previous })
	for _, class := range []PrivilegeClass{PrivilegeAdmin, PrivilegeSharedState} {
		called, admitted := 0, 0
		PluginFunctionLookup = func(name string) (interface{}, bool) {
			return PluginFunctionContract{
				Privilege: class,
				Handler:   func() int64 { called++; return 1 },
			}, name == "contract.effect"
		}
		ctx := WithPermissionChecker(context.Background(), func(permission string) bool { return permission == "read" })
		ctx = WithProcedureGrantChecker(ctx, func(name string) bool { return name == "dbms.listConfig" })
		query := "CALL dbms.listConfig() YIELD name RETURN contract.effect() AS value"
		require.Error(t, AuthorizeQuery(ctx, query), "a procedure grant covers only that procedure")
		executor := NewStatementExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "boosted_contract"))
		executor.SetFirstAdmission(func() { admitted++ })
		_, err := executor.Execute(ctx, query, nil)
		require.Error(t, err)
		require.Zero(t, called)
		require.Zero(t, admitted, "unrelated effects must be refused before services are initialized")
	}
}
