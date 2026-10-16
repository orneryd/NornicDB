package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// These expected values come from Neo4j 5.26.30, not another NornicDB path.
func TestPerRowRound4Values(t *testing.T) {
	for _, tc := range []struct {
		name, query string
		want        [][]interface{}
	}{
		{"nested_subquery_function", "UNWIND [0,0] AS x RETURN abs(CASE WHEN coalesce(EXISTS { WITH x WHERE 1/x=1 RETURN x },false) OR x=0 THEN 1 ELSE 0 END) AS v", [][]interface{}{{int64(1)}, {int64(1)}}},
		{"pattern_and", "CREATE (n:N)-[:R]->(:N) WITH n,0 AS x RETURN abs(CASE WHEN size([(n)-->(m) | 1/x])=0 AND x<>0 THEN 1 ELSE 0 END) AS v", [][]interface{}{{int64(0)}}},
		{"chain_false", "UNWIND [0,0] AS x RETURN 1/x < x < -1 AS v", [][]interface{}{{false}, {false}}},
		{"chain_where", "UNWIND [0,0] AS x WITH x WHERE 1/x < x < -1 RETURN x", [][]interface{}{}},
		{"chain_case", "UNWIND [0,0] AS x RETURN CASE WHEN 1/x < x < -1 THEN 1 ELSE 0 END AS v", [][]interface{}{{int64(0)}, {int64(0)}}},
		{"chain_reverse", "UNWIND [0,0] AS x RETURN -1 > x > 1/x AS v", [][]interface{}{{false}, {false}}},
		{"predicate_or", "UNWIND ['a','a'] AS x WITH x WHERE toLower(x)='a' OR toLower(x)='b' RETURN x", [][]interface{}{{"a"}, {"a"}}},
	} {
		for _, text := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/text=%v", tc.name, text), func(t *testing.T) {
				rowValueProgramsDisabled.Store(text)
				defer rowValueProgramsDisabled.Store(false)
				exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "r4"))
				result, err := exec.Execute(context.Background(), tc.query, nil)
				require.NoError(t, err)
				require.Equal(t, tc.want, result.Rows)
			})
		}
	}
}

func TestPerRowRound4CompiledChain(t *testing.T) {
	exec, _ := perRowTestExecutor(t)
	root, _ := (&rowValueCompiler{}).compile("1/x < x < -1")
	value, ok, err := root.eval(exec, map[string]interface{}{"x": int64(0)}, nil)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, false, value)
}

func TestPerRowRound4RequiredErrors(t *testing.T) {
	for _, query := range []string{
		"UNWIND [0,0] AS x RETURN 1/x < x AS v",
		"UNWIND [0,0] AS x RETURN EXISTS { WITH x WHERE 1/x=1 RETURN x } AND x<>0 AS v",
	} {
		for _, text := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/text=%v", query, text), func(t *testing.T) {
				rowValueProgramsDisabled.Store(text)
				defer rowValueProgramsDisabled.Store(false)
				exec, _ := perRowTestExecutor(t)
				_, err := exec.Execute(context.Background(), query, nil)
				requireRound3Neo4jError(t, err, "/ by zero")
			})
		}
	}
}
