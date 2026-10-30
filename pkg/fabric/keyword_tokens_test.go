package fabric

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPlannerKeywordTokens(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("comp", &LocationLocal{DBName: "comp"})
	catalog.Register("comp.one", &LocationLocal{DBName: "one"})
	planner := NewFabricPlanner(catalog, testStatementWrites)
	for _, body := range []string{
		"RETURN $union AS k", "RETURN $use AS k", "RETURN $create AS k",
		"RETURN 'UNION USE CALL { } RETURN WITH SET' AS k",
		"RETURN 7 AS `} UNION USE CALL {`",
		"RETURN 7 AS k /* UNION USE CALL { } RETURN WITH SET */",
	} {
		t.Run(body, func(t *testing.T) {
			fragment, err := planner.Plan(t.Context(), "USE comp CALL { USE comp.one "+body+" } RETURN k", "comp", cyphertext.RawSource)
			require.NoError(t, err)
			outer, ok := fragment.(*FragmentApply)
			require.True(t, ok, "%T", fragment)
			call, ok := outer.Input.(*FragmentApply)
			require.True(t, ok, "%T", outer.Input)
			inner, ok := call.Inner.(*FragmentExec)
			require.True(t, ok, "CALL body must be one fragment, got %T", call.Inner)
			require.Equal(t, body, inner.Query)
			require.False(t, inner.IsWrite)
		})
	}
}
