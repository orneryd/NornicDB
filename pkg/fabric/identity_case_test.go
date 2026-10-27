package fabric

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestIdentityCatalogAndScopePreserveCodeUnits(t *testing.T) {
	high, low := "route_\xed\xa0\x80", "route_\xed\xb0\x80"
	catalog := NewCatalog()
	catalog.Register("identity."+high, &LocationLocal{DBName: "first"})
	catalog.Register("identity."+low, &LocationLocal{DBName: "second"})
	for _, tc := range []struct{ name, target string }{{high, "first"}, {low, "second"}, {"ROUTE_\xed\xa0\x80", "first"}, {"ROUTE_\xed\xb0\x80", "second"}} {
		location, err := catalog.Resolve("identity." + tc.name)
		require.NoError(t, err)
		require.Equal(t, tc.target, location.DatabaseName())
	}
	require.False(t, inScope(high+".child", low))
	require.True(t, inScope("ROUTE_\xed\xa0\x80.child", high))
	catalog.Unregister("identity." + high)
	_, err := catalog.Resolve("identity." + high)
	require.Error(t, err)
	location, err := catalog.Resolve("identity." + low)
	require.NoError(t, err)
	require.Equal(t, "second", location.DatabaseName())
}
