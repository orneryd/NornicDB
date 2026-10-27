package auth

import (
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestIdentityRequestDatabaseScopePreservesCodeUnits(t *testing.T) {
	high, low := "route_\xed\xa0\x80", "route_\xed\xb0\x80"
	scope := NewRequestDatabaseScope("", map[string]string{high: "first", low: "second", `route_\ud800`: "literal"})
	for _, tc := range []struct{ selection, target string }{{high, "first"}, {low, "second"}, {`route_\ud800`, "literal"}, {"ROUTE_\xed\xa0\x80", "first"}, {"ROUTE_\xed\xb0\x80", "second"}} {
		target, ok := scope.Resolve(tc.selection)
		require.True(t, ok)
		require.Equal(t, tc.target, target, tc.selection)
	}
}

func TestIdentityProcedureGrantsAndGlobPreserveCodeUnits(t *testing.T) {
	for _, scenario := range []string{"role", "procedure"} {
		t.Run(scenario, func(t *testing.T) {
			engine := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
			grants := NewProcedurePrivilegeStore(engine)
			high, low := "\xed\xa0\x80", "\xed\xb0\x80"
			require.NoError(t, grants.Grant(t.Context(), "reader_"+high, "custom.read_"+high, true, false))
			allowed, err := grants.AllowsBoosted([]string{"READER_" + high}, "CUSTOM.READ_"+high)
			require.NoError(t, err)
			require.True(t, allowed)
			role, procedure := "reader_"+high, "custom.read_"+high
			if scenario == "role" {
				role = "reader_" + low
			} else {
				procedure = "custom.read_" + low
			}
			allowed, err = grants.AllowsBoosted([]string{role}, procedure)
			require.NoError(t, err)
			require.False(t, allowed, "distinct unit identity must not inherit another grant")
			require.True(t, ProcedureGlobMatches("custom.read_?", "custom.read_"+high))
			require.False(t, ProcedureGlobMatches("custom.read_??", "custom.read_"+high))
			require.False(t, ProcedureGlobMatches("custom.read_"+high, "custom.read_"+low))
		})
	}
}
