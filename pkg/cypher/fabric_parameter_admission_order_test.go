package cypher

import (
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/stretchr/testify/require"
)

// TestFabricParameterAdmissionOrder checks the retained late parameter boundary.
func TestFabricParameterAdmissionOrder(t *testing.T) {
	for _, tc := range []struct {
		name, prefix, scope, code string
		provided                  bool
		writes                    int
	}{
		{"normal/missing", "", "x", "Neo.ClientError.Statement.ParameterMissing", false, 0},
		{"profile/missing", "PROFILE ", "x", "Neo.ClientError.Statement.ParameterMissing", false, 0},
		{"explain/missing", "EXPLAIN ", "x", "", false, 0},
		{"profile/invalid-scope", "PROFILE ", "$x", "Neo.ClientError.Statement.SyntaxError", false, 0},
		{"explain/invalid-scope", "EXPLAIN ", "$x", "Neo.ClientError.Statement.SyntaxError", false, 0},
		{"normal/provided", "", "x", "", true, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exec := newFabricCallRowsExecutor(t)
			mgr := exec.dbManager.(*testDatabaseManagerAdapter).manager
			store, err := mgr.GetStorage("one")
			require.NoError(t, err)
			var params map[string]interface{}
			if tc.provided {
				params = map[string]interface{}{"missing": int64(7)}
			}
			query := tc.prefix + "USE comp WITH 1 AS x CALL (" + tc.scope + ") { USE comp.one CREATE (:FabricParameterAdmissionOrder {id:'witness'}) RETURN $missing AS y } RETURN x, y"
			result, err := exec.Execute(t.Context(), query, params)
			if tc.code != "" {
				require.Error(t, err)
				code, _ := nornicerrors.Neo4jStatus(err)
				require.Equal(t, tc.code, code)
			} else {
				require.NoError(t, err)
				require.NotNil(t, result)
				if tc.provided {
					require.Equal(t, [][]interface{}{{int64(1), int64(7)}}, result.Rows)
				}
			}
			nodes, readErr := store.GetNodesByLabel("FabricParameterAdmissionOrder")
			require.NoError(t, readErr)
			require.Len(t, nodes, tc.writes, "admission must precede participant writes")
		})
	}
}
