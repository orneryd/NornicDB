package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestPluginFunctionContractKeepsInvocationAndPrincipal(t *testing.T) {
	prior := PluginFunctionLookup
	t.Cleanup(func() { PluginFunctionLookup = prior })
	writes := int64(0)
	PluginFunctionLookup = func(name string) (interface{}, bool) {
		switch name {
		case "contract.write":
			return PluginFunctionContract{Privilege: PrivilegeWrite, Handler: func() int64 { writes++; return writes }}, true
		case "contract.owner":
			return PluginFunctionContract{Privilege: PrivilegeNone, Handler: func(ctx context.Context) string { user, _ := authenticatedUserFromContext(ctx); return user.Name }}, true
		}
		return nil, false
	}
	executor := NewStatementExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "cache_contract"))
	ctx := WithPermissionChecker(context.Background(), func(permission string) bool { return permission == "write" })
	for i := int64(1); i <= 2; i++ {
		result, err := executor.Execute(ctx, "RETURN contract.write() AS value", nil)
		require.NoError(t, err)
		require.Equal(t, i, result.Rows[0][0])
	}
	for _, name := range []string{"alice", "bob"} {
		as := WithRequestIdentity(ctx, &RequestIdentity{User: &AuthenticatedUser{Name: name}})
		result, err := executor.Execute(as, "RETURN contract.owner() AS owner", nil)
		require.NoError(t, err)
		require.Equal(t, name, result.Rows[0][0], "a shared cache cannot replace a principal-dependent plugin invocation")
	}
}
