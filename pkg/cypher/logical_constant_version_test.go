package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLogicalConstantFoldsPreserveCypherVersion(t *testing.T) {
	for range []bool{false} {
		mode := "compiled"
		if text {
			mode = "text"
		}
		t.Run(mode, func(t *testing.T) {
			exec, _ := perRowTestExecutor(t)
			exec.cache = nil
			for _, tc := range []struct {
				expression string
				cypher25   bool
			}{
				{"(9007199254740993 <> 9007199254740992.0) AND COUNT { RETURN 1 } > 0", false},
				{"COUNT { RETURN 1 } > 0 AND (9007199254740993 != 9007199254740992.0)", false},
				{"(9007199254740993 <> 9007199254740992.0) OR COUNT { RETURN 1 } = 0", false},
				{"NOT ((9007199254740993 <> 9007199254740992.0) AND COUNT { RETURN 1 } > 0)", true},
			} {
				for _, versions := range [][]string{{"25", "5", "25", "5"}, {"5", "25", "5", "25"}} {
					expression := tc.expression
					if versions[0] == "5" {
						// Distinct text makes each warming order start with a cold plan.
						expression = strings.ReplaceAll(expression, "RETURN 1", "RETURN 2")
					}
					// Reuse the expression caches across alternating statement versions.
					for _, version := range versions {
						t.Run(version+"/"+expression, func(t *testing.T) {
							want := tc.cypher25
							if version == "5" {
								want = !want
							}
							query := "CYPHER " + version + " RETURN " + expression + " AS v"
							result, err := exec.Execute(context.Background(), query, nil)
							require.NoError(t, err)
							require.Equal(t, [][]interface{}{{want}}, result.Rows)
						})
					}
				}
			}
		})
	}
}


func TestLogicalConstantVersionPreservesSubqueryErrors(t *testing.T) {
	for range []bool{false} {
		for _, tc := range []struct {
			expression string
			deciding   string
			want       bool
		}{
			{"(9007199254740993 <> 9007199254740992.0) AND COUNT_X > 0", "25", false},
			{"COUNT_X > 0 AND (9007199254740993 <> 9007199254740992.0)", "25", false},
			{"(9007199254740993 <> 9007199254740992.0) OR COUNT_X > 0", "5", true},
			{"NOT ((9007199254740993 <> 9007199254740992.0) AND COUNT_X > 0)", "25", true},
			{"NOT ((9007199254740993 <> 9007199254740992.0) OR COUNT_X > 0)", "5", false},
		} {
			for _, version := range []string{"5", "25", "5", "25"} {
				t.Run(version+"/"+tc.expression, func(t *testing.T) {
					exec, _ := perRowTestExecutor(t)
					query := "CYPHER " + version + " UNWIND [0,0] AS x RETURN " + strings.ReplaceAll(tc.expression, "COUNT_X", "COUNT { WITH x WHERE 1/x=1 RETURN x }") + " AS v"
					result, err := exec.Execute(context.Background(), query, nil)
					if version != tc.deciding {
						requireRound3Neo4jError(t, err, "/ by zero")
						return
					}
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{tc.want}, {tc.want}}, result.Rows)
				})
			}
		}
	}
}
