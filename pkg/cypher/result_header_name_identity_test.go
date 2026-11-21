package cypher

import (
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// N152-95 (NOR-66): a result column the statement names - an alias or a bare
// variable - is the decoded identity the accepted statement carries, on both
// parsers and both normalization settings. Only an expression column keeps the
// text the client wrote, which is what the source rewrites restore.
func TestResultHeaderNameIdentityVersusExpressionText(t *testing.T) {
	oldParser, oldNormalization := config.GetParserType(), config.IsCypherQueryNormalizationEnabled()
	t.Cleanup(func() {
		config.SetParserType(oldParser)
		config.SetCypherQueryNormalizationEnabled(oldNormalization)
	})
	cases := []struct {
		name    string
		query   string
		columns []string
		rows    int
		// names records that every column is named by the statement, so the
		// strict header view and execution must agree on it.
		names bool
	}{
		{"computed literal keeps its written text", `RETURN 'a\u0062'`, []string{`'a\u0062'`}, 1, false},
		{"grouped literal keeps its written text", `RETURN 1_000 + 1`, []string{"1_000 + 1"}, 1, false},
		{"computed expression keeps its written text", "UNWIND [1] AS `a\\u0062` RETURN `a\\u0062` + 1", []string{"`a\\u0062` + 1"}, 1, false},
		{"grouped identity keeps its written text", `UNWIND [1] AS a\u0062 RETURN (a\u0062), a\u0062`, []string{`(a\u0062)`, "ab"}, 1, false},
		{"wildcard keeps a written expression", `UNWIND [1] AS a\u0062 RETURN *, 'a\u0062'`, []string{"ab", `'a\u0062'`}, 1, false},
		{"union names its columns from its first branch", `RETURN 1 AS a\u0062, 'a\u0062' UNION RETURN 'a\u0062', 2 AS a\u0062`, []string{"ab", `'a\u0062'`}, 2, false},
		{"quoted union preserves first branch order", "RETURN 1 AS `a b\\u0062`, 'a\\u0062' UNION RETURN 'a\\u0062', 2 AS `a b\\u0062`", []string{"a bb", `'a\u0062'`}, 2, false},
		{"map and alias", "RETURN {`a\\u0062`:1} AS m, 1 AS `a\\u0062`", []string{"m", "ab"}, 1, true},
		{"unquoted alias", `RETURN 1 AS a\u0062`, []string{"ab"}, 1, true},
		{"quoted alias with a space", "RETURN 1 AS `a b\\u0062`", []string{"a bb"}, 1, true},
		{"quoted alias with punctuation", "RETURN 1 AS `a.b\\u0062`", []string{"a.bb"}, 1, true},
		{"quoted alias with a doubled backtick", "RETURN 1 AS `a``b\\u0062`", []string{"a`bb"}, 1, true},
		{"quoted alias with a generated literal backslash", "RETURN 1 AS `a\\u005cu0062`", []string{`a\u0062`}, 1, true},
		{"bare variable", "UNWIND ['value'] AS `a\\u0062` RETURN `a\\u0062`", []string{"ab"}, 1, true},
		{"zero rows", "UNWIND [] AS `a\\u0062` RETURN `a\\u0062`", []string{"ab"}, 0, true},
		{"wildcard", "UNWIND [1] AS `a\\u0062` RETURN *", []string{"ab"}, 1, true},
		{"calling subquery wildcard", `CALL { RETURN 1 AS a\u0062 } RETURN *`, []string{"ab"}, 1, true},
		{"union", "RETURN 1 AS `a\\u0062` UNION RETURN 2 AS `a\\u0062`", []string{"ab"}, 2, true},
		{"order by", "UNWIND [1] AS `a\\u0062` RETURN `a\\u0062` ORDER BY `a\\u0062`", []string{"ab"}, 1, true},
	}
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		config.SetParserType(parser)
		for _, normalization := range []bool{true, false} {
			config.SetCypherQueryNormalizationEnabled(normalization)
			for _, tc := range cases {
				t.Run(fmt.Sprintf("%s/normalized=%t/%s", parser, normalization, tc.name), func(t *testing.T) {
					executor := NewStorageExecutor(newTestMemoryEngine(t))
					result, err := executor.Execute(t.Context(), tc.query, nil)
					require.NoError(t, err)
					require.Len(t, result.Rows, tc.rows)
					require.Equal(t, tc.columns, result.Columns)
					if tc.name == "map and alias" {
						require.Equal(t, [][]any{{map[string]any{"ab": int64(1)}, int64(1)}}, result.Rows)
					}
					if !tc.names {
						return
					}
					headers, err := executor.StatementResultColumns(tc.query, RawSource)
					require.NoError(t, err)
					require.Equal(t, tc.columns, headers)
				})
			}
		}
	}
}
