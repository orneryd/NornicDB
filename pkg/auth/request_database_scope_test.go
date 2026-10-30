package auth

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRequestDatabaseScopeResolutionAndDefensiveCopy(t *testing.T) {
	selections := map[string]string{
		"tenant_a": "tenant_a",
		"primary":  "tenant_a",
	}
	scope := NewRequestDatabaseScope("tenant_a", selections)
	selections["tenant_b"] = "tenant_b"
	selections["primary"] = "tenant_b"

	database, ok := scope.Resolve(" PRIMARY ")
	require.True(t, ok)
	require.Equal(t, "tenant_a", database)

	database, ok = scope.Resolve("")
	require.True(t, ok)
	require.Equal(t, "tenant_a", database)

	_, ok = scope.Resolve("tenant_b")
	require.False(t, ok)

	ctx := WithRequestDatabaseScope(context.Background(), scope)
	require.Same(t, scope, RequestDatabaseScopeFromContext(ctx))
}

func TestRequestDatabaseScopeOmittedSelection(t *testing.T) {
	t.Run("sole authorized database", func(t *testing.T) {
		scope := NewRequestDatabaseScope("", map[string]string{
			"tenant_a": "tenant_a",
			"primary":  "tenant_a",
		})

		database, ok := scope.Resolve("")
		require.True(t, ok)
		require.Equal(t, "tenant_a", database)
	})

	t.Run("ambiguous authorized databases", func(t *testing.T) {
		scope := NewRequestDatabaseScope("", map[string]string{
			"tenant_a": "tenant_a",
			"tenant_b": "tenant_b",
		})

		_, ok := scope.Resolve("")
		require.False(t, ok)
	})
}
