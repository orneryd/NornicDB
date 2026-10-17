package registry

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/stretchr/testify/require"
)

func TestRuntimeRegistrationRequiresPrivilegeClass(t *testing.T) {
	registry := NewFunctionRegistry()
	handler := func() int { return 1 }
	for _, class := range []auth.OperationClass{0, 1 << 15, auth.OperationNone | auth.OperationWrite} {
		require.Error(t, registry.RegisterFunction("custom.effect", "custom", handler, "", nil, class))
		require.Empty(t, registry.List())
	}
	require.NoError(t, registry.RegisterFunction("custom.value", "custom", handler, "", nil, auth.OperationNone))
}
