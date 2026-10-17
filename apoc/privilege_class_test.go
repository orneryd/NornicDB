package apoc

import (
	"testing"

	"github.com/orneryd/nornicdb/apoc/storage"
	"github.com/stretchr/testify/require"
)

func TestEveryAPOCRegistrationHasExpectedPrivilegeClass(t *testing.T) {
	require.NoError(t, Initialize(storage.NewInMemoryStorage(), nil))
	mu.RLock()
	defer mu.RUnlock()
	require.Len(t, functions, len(expectedBuiltinFunctionClasses))
	for name, function := range functions {
		expected, found := expectedBuiltinFunctionClasses[name]
		require.True(t, found, "unreviewed APOC registration %q", name)
		require.Equal(t, expected, function.Privilege, name)
		require.True(t, function.Privilege.Valid(), name)
	}
}
