package cypher

import (
	"fmt"
	"math"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Expectations were executed against Neo4j Community5.26.30, not inferred
// from ORDER BY's total order. Public RETURN, WITH and nullable-WHERE consumers
// exercise the actual parser/evaluator paths; parameter values avoid relying
// solely on literal construction of the NaN. No equality contract is changed.
func TestNOR87NaNListPredicates(t *testing.T) {
	list := func(v ...any) []any { return v }
	nan := math.NaN()
	unknown := []any{nil, nil, nil, nil}
	less := []any{true, true, false, false}
	greater := []any{false, false, true, true}
	equal := []any{false, true, false, true}
	cases := []struct {
		name, left, right string
		a, b              any
		want              []any
	}{
		{"reported", "[sqrt(-1)]", "[1.0]", list(nan), list(1.0), unknown},
		{"right_nan", "[1.0]", "[sqrt(-1)]", list(1.0), list(nan), unknown},
		{"both_nan", "[sqrt(-1)]", "[sqrt(-1)]", list(nan), list(nan), unknown},
		{"later_nan_left", "[1,sqrt(-1)]", "[1,2]", list(int64(1), nan), list(int64(1), int64(2)), unknown},
		{"later_nan_right", "[1,2]", "[1,sqrt(-1)]", list(int64(1), int64(2)), list(int64(1), nan), unknown},
		{"decisive_prefix_less", "[0,sqrt(-1)]", "[1,2]", list(int64(0), nan), list(int64(1), int64(2)), less},
		{"decisive_prefix_greater", "[2,sqrt(-1)]", "[1,2]", list(int64(2), nan), list(int64(1), int64(2)), greater},
		{"nested_nan", "[[sqrt(-1)]]", "[[1.0]]", list(list(nan)), list(list(1.0)), unknown},
		{"nested_decisive", "[[0,sqrt(-1)]]", "[[1,2]]", list(list(int64(0), nan)), list(list(int64(1), int64(2))), less},
		{"empty_before_nan", "[]", "[sqrt(-1)]", list(), list(nan), less},
		{"nan_before_empty", "[sqrt(-1)]", "[]", list(nan), list(), greater},
		{"null_element", "[null]", "[1]", list(nil), list(int64(1)), unknown},
		{"null_list", "null", "[1]", nil, list(int64(1)), unknown},
		{"finite_less", "[1,2]", "[1,3]", list(int64(1), int64(2)), list(int64(1), int64(3)), less},
		{"finite_equal", "[1,2]", "[1.0,2.0]", list(int64(1), int64(2)), list(1.0, 2.0), equal},
		{"finite_longer", "[1,2]", "[1]", list(int64(1), int64(2)), list(int64(1)), greater},
		{"empty_equal", "[]", "[]", list(), list(), equal},
		{"nested_prefix", "[[1],sqrt(-1)]", "[[2],0]", list(list(int64(1)), nan), list(list(int64(2)), int64(0)), less},
		{"nan_before_null", "[sqrt(-1)]", "[null]", list(nan), list(nil), unknown},
		{"nan_scalar", "sqrt(-1)", "1.0", nan, 1.0, []any{false, false, false, false}},
	}
	previousParser := config.GetParserType()
	previousPrograms := rowValueProgramsDisabled.Load()
	t.Cleanup(func() { config.SetParserType(previousParser); rowValueProgramsDisabled.Store(previousPrograms) })
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		for _, programs := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/programs=%t", parser, programs), func(t *testing.T) {
				config.SetParserType(parser)
				rowValueProgramsDisabled.Store(!programs)
				exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor87"), QueryCachePolicy{MaxEntries: 0, TTL: 0})
				for _, tc := range cases {
					t.Run(tc.name, func(t *testing.T) {
						params := map[string]any{"a": tc.a, "b": tc.b}
						queries := []string{
							fmt.Sprintf("RETURN %s < %s AS lt, %s <= %s AS le, %s > %s AS gt, %s >= %s AS ge", tc.left, tc.right, tc.left, tc.right, tc.left, tc.right, tc.left, tc.right),
							"RETURN $a < $b AS lt, $a <= $b AS le, $a > $b AS gt, $a >= $b AS ge",
							"WITH $a AS a, $b AS b RETURN a < b AS lt, a <= b AS le, a > b AS gt, a >= b AS ge",
						}
						for _, q := range queries {
							result, err := exec.Execute(t.Context(), q, params)
							require.NoError(t, err, q)
							require.Len(t, result.Rows, 1, q)
							assert.Equal(t, tc.want, result.Rows[0], q)
						}
						for i, operator := range []string{"<", "<=", ">", ">="} {
							assert.Equal(t, tc.want[i], compareCypherPredicateValue(tc.a, tc.b, operator), operator)
							q := fmt.Sprintf("WITH $a %s $b AS v WHERE v IS NULL RETURN v", operator)
							result, err := exec.Execute(t.Context(), q, params)
							require.NoError(t, err, q)
							if tc.want[i] == nil {
								assert.Equal(t, [][]any{{nil}}, result.Rows, q)
							} else {
								assert.Empty(t, result.Rows, q)
							}
						}
					})
				}
				result, err := exec.Execute(t.Context(), `UNWIND [{name:'nan',v:[sqrt(-1)]},{name:'one',v:[1.0]},{name:'empty',v:[]},{name:'null',v:[null]},{name:'zero',v:[0.0]},{name:'longer',v:[1.0,0.0]}] AS x RETURN x.name AS name ORDER BY x.v`, nil)
				require.NoError(t, err)
				assert.Equal(t, [][]any{{"empty"}, {"zero"}, {"one"}, {"longer"}, {"nan"}, {"null"}}, result.Rows)
			})
		}
	}
}
