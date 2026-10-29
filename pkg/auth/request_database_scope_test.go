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

// Selection spelling is a separate, normalized protocol. Its mapped physical
// database is a decoded catalog identity and must reach the resolver unchanged.
func TestRequestDatabaseScopeCanonicalDatabaseIdentity(t *testing.T) {
	for _, name := range []string{"db", " db ", "DB", " ", "\t"} {
		t.Run(name, func(t *testing.T) {
			var checked []string
			scope := NewRequestDatabaseScopeWithSelectionCheck("primary", map[string]string{"primary": name}, func(selection string) bool {
				checked = append(checked, selection)
				return true
			})
			for _, selection := range []string{"", " PRIMARY "} {
				database, ok := scope.Resolve(selection)
				require.True(t, ok)
				require.Equal(t, name, database)
			}
			require.Equal(t, []string{"primary", "primary"}, checked)
		})
	}
}
