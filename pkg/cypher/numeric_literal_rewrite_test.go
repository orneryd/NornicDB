package cypher

import (
	"context"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Results and errors of Neo4j 5.26.30 for digit-grouping underscores and
// the legacy integer forms (#907).
func TestNumericLiteralDigitGroupingMatchesNeo4j(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "numeric_literals"))
	ctx := context.Background()

	for _, testCase := range []struct {
		query string
		want  interface{}
	}{
		{"RETURN 1_000 AS v", int64(1000)},
		{"RETURN 1_000_000 AS v", int64(1000000)},
		{"RETURN -1_000 AS v", int64(-1000)},
		{"RETURN 1_000.5 AS v", 1000.5},
		{"RETURN 1.0_5 AS v", 1.05},
		{"RETURN .5_0 AS v", 0.5},
		{"RETURN 1e1_0 AS v", 1e10},
		{"RETURN 1E1_0 AS v", 1e10},
		{"RETURN 1e+1_0 AS v", 1e10},
		{"RETURN 1.5e-1_0 AS v", 1.5e-10},
		{"RETURN 1_0e2 AS v", 1000.0},
		{"RETURN 1_000.0_0 AS v", 1000.0},
		{"RETURN 0_0.5 AS v", 0.5},
		{"RETURN 0_0e1 AS v", 0.0},
		{"RETURN 00.5 AS v", 0.5},
		{"RETURN 0x_1F AS v", int64(31)},
		{"RETURN 0x1_F AS v", int64(31)},
		{"RETURN 0x1F_E AS v", int64(510)},
		{"RETURN 0o1_7 AS v", int64(15)},
		// A NornicDB extension kept from before (#907): Neo4j 5 rejects the
		// uppercase prefixes, NornicDB reads them as 0x and 0o.
		{"RETURN 0X1F AS v", int64(31)},
		{"RETURN 0O17 AS v", int64(15)},
		{"RETURN 0o_17 AS v", int64(15)},
		{"RETURN 0x7FFF_FFFF_FFFF_FFFF AS v", int64(9223372036854775807)},
		{"RETURN 9_223_372_036_854_775_807 AS v", int64(9223372036854775807)},
		{"RETURN -9_223_372_036_854_775_808 AS v", int64(-9223372036854775808)},
		{"RETURN 1_000 + 1 AS v", int64(1001)},
		{"RETURN [1_0, -2_0] AS v", []interface{}{int64(10), int64(-20)}},
		{"RETURN {a: 1_0}.a AS v", int64(10)},
		{"RETURN toString(1_000) AS v", "1000"},
		{"WITH 1 AS x RETURN x+1_0 AS v", int64(11)},
		{"UNWIND [1] AS x RETURN x*1_0 AS v", int64(10)},
		{"RETURN '1_000' AS v", "1_000"},
		{"RETURN 1 /* 1_000 */ AS v", int64(1)},
	} {
		t.Run(testCase.query, func(t *testing.T) {
			result, err := exec.Execute(ctx, testCase.query, nil)
			require.NoError(t, err)
			require.Equal(t, []string{"v"}, result.Columns)
			require.Equal(t, [][]interface{}{{testCase.want}}, result.Rows)
		})
	}

	t.Run("columns keep the text written", func(t *testing.T) {
		result, err := exec.Execute(ctx, "RETURN 1_000, 1_0.5e1_0, 2_0 AS `2_0x`", nil)
		require.NoError(t, err)
		require.Equal(t, []string{"1_000", "1_0.5e1_0", "2_0x"}, result.Columns)
		require.Equal(t, [][]interface{}{{int64(1000), 105000000000.0, int64(20)}}, result.Rows)
	})

	t.Run("a parameter name is not a number", func(t *testing.T) {
		result, err := exec.Execute(ctx, "RETURN $1_0 AS v", map[string]interface{}{"1_0": "p"})
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{"p"}}, result.Rows)
	})

	for _, query := range []string{
		"RETURN 1__000 AS v",
		"RETURN 1000_ AS v",
		"RETURN 1.5_ AS v",
		"RETURN .5_ AS v",
		"RETURN 1e_10 AS v",
		"RETURN 1_e10 AS v",
		"RETURN 1e+_10 AS v",
		"RETURN 1e-_10 AS v",
		"RETURN 1e1__0 AS v",
		"RETURN 0x1F_ AS v",
		"RETURN 0x_ AS v",
		"RETURN 0o_ AS v",
		"RETURN 0x1__F AS v",
		"RETURN 0x__1F AS v",
		"RETURN 0o1__7 AS v",
		"RETURN 0_1 AS v",
		"RETURN 0_0 AS v",
		"RETURN 01_0 AS v",
		"RETURN 01 AS v",
		"RETURN 00 AS v",
		"RETURN 0x8000_0000_0000_0000 AS v",
		"RETURN 9_223_372_036_854_775_808 AS v",
		"RETURN [1,2,3][0_1..2] AS v",
	} {
		t.Run(query, func(t *testing.T) {
			_, err := exec.Execute(ctx, query, nil)
			require.Error(t, err)
			code, _ := nornicerrors.Neo4jStatus(err)
			require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code)
		})
	}
}

func TestCanonicalizeNumericLiterals(t *testing.T) {
	for _, testCase := range []struct{ query, want string }{
		{"RETURN 1_000", "RETURN 1000"},
		{"RETURN a1_000, x.p_1, $1_0", "RETURN a1_000, x.p_1, $1_0"},
		{"RETURN '1_0', `1_0`, \"1_0\" // 1_0", "RETURN '1_0', `1_0`, \"1_0\" // 1_0"},
		{"RETURN 1__0, 0_1, 1_e2", "RETURN 1__0, 0_1, 1_e2"},
		{"RETURN 0x_F, 0o_7, 1.0_5e-1_0", "RETURN 0xF, 0o7, 1.05e-10"},
		{"RETURN 1_0..2_0", "RETURN 10..20"},
		{"RETURN user_id", "RETURN user_id"},
		{"1_000", "1000"},
		{"RETURN n_1_a, `a_1`, 'b_2' AS x_3", "RETURN n_1_a, `a_1`, 'b_2' AS x_3"},
	} {
		got, rewrite := canonicalizeNumericLiterals(testCase.query)
		require.Equal(t, testCase.want, got, testCase.query)
		require.Equal(t, testCase.want != testCase.query, rewrite != nil, testCase.query)
	}
}
