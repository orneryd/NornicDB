package cypher

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestPerRowRound5RetainedValues(t *testing.T) {
	for _, tc := range []struct {
		query string
		want  interface{}
	}{
		{"CREATE (n:B15)-[:R]->(:B15) WITH n,0 AS x RETURN size([(n)-->(m) | 1/x]) AS v", int64(1)},
		{"CREATE (n:B15)-[:R]->(:B15) WITH n,0 AS x RETURN trim(toString(size([(n)-->(m) | 1/x]))) AS v", "1"},
		{"RETURN trim(toString({a:1, b:{c:2}}['b']['c'])) AS v", "2"},
	} {
		for _, text := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/text=%v", tc.query, text), func(t *testing.T) {
				rowValueProgramsDisabled.Store(text)
				defer rowValueProgramsDisabled.Store(false)
				rows, err := runPerRowRound5Statements(t, tc.query)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{tc.want}}, rows)
			})
		}
	}
}
func TestPerRowRound5StaticSliceErrors(t *testing.T) {
	for _, query := range []string{"WITH [1,2,3] AS l RETURN l[1..'a']", "WITH [1,2,3] AS l RETURN trim(toString(l[0..'a']))"} {
		exec, _ := perRowTestExecutor(t)
		_, err := exec.Execute(context.Background(), query, nil)
		requireRound3Neo4jError(t, err, "Type mismatch: expected Integer but was String")
		var semantic *SemanticError
		require.ErrorAs(t, err, &semantic)
		require.Equal(t, "Neo.ClientError.Statement.SyntaxError", semantic.Code)
	}
}
