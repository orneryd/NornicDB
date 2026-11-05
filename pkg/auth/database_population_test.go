package auth

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDatabasePopulationAccess(t *testing.T) {
	names := []string{"system", "nornic", "empty", "composite"}
	for _, denied := range names {
		any, all := DatabasePopulationAccess(names, func(database string) bool { return database != denied })
		require.True(t, any)
		require.False(t, all, denied)
	}
	any, all := DatabasePopulationAccess(names, func(string) bool { return false })
	require.False(t, any)
	require.False(t, all)
	any, all = DatabasePopulationAccess(names, func(string) bool { return true })
	require.True(t, any)
	require.True(t, all)
	any, all = DatabasePopulationAccess(nil, func(string) bool { t.Fatal("empty catalog checked a database"); return false })
	require.False(t, any)
	require.True(t, all)
}
