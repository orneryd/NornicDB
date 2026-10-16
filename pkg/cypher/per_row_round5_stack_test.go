package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A map projection's subscript reads the projection, whose receiver starts
// at its variable (rowSubscriptReceiverStart, release fix 6765c4d7). Expected values are Neo4j
// 5.26.30's.
func TestPerRowRound5MapProjectionSubscript(t *testing.T) {
	for _, tc := range []struct {
		statements string
		want       [][]interface{}
	}{
		{"WITH {a:1} AS n RETURN n{.a}['a'] AS v", [][]interface{}{{int64(1)}}},
		{"WITH {a:1} AS m RETURN m{.*}['a'] AS v", [][]interface{}{{int64(1)}}},
		{"WITH {a:1} AS n RETURN n {.a, b: 2}['b'] AS v", [][]interface{}{{int64(2)}}},
		{"CREATE (:G04 {a:1}) ;; MATCH (n:G04) RETURN n{.a}['a'] AS v", [][]interface{}{{int64(1)}}},
		{"WITH {a:1} AS n RETURN trim(toString(n{.a}['a'])) AS v", [][]interface{}{{"1"}}},
		{"WITH {a:1} AS m RETURN trim(toString(m{.*}['a'])) AS v", [][]interface{}{{"1"}}},
		{"CREATE (:G05 {a:1}) ;; MATCH (n:G05) RETURN trim(toString(n{.a}['a'])) AS v", [][]interface{}{{"1"}}},
		{"RETURN {a:'b'}['a'] AS v", [][]interface{}{{"b"}}},
		{"RETURN trim({a:'b'}['a']) AS v", [][]interface{}{{"b"}}},
	} {
		for _, text := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/text=%v", tc.statements, text), func(t *testing.T) {
				rowValueProgramsDisabled.Store(text)
				defer rowValueProgramsDisabled.Store(false)
				exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "r5map"))
				var result *ExecuteResult
				for _, statement := range strings.Split(tc.statements, ";;") {
					var err error
					result, err = exec.Execute(context.Background(), strings.TrimSpace(statement), nil)
					require.NoError(t, err)
				}
				require.Equal(t, tc.want, result.Rows)
			})
		}
	}
}
