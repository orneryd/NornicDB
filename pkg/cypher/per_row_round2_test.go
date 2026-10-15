package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Literal expectations are checked independently with Neo4j 5.26.30.
func TestPerRowRound2Reproductions(t *testing.T) {
	tests := []struct {
		name, query string
		want        [][]interface{}
	}{
		{"nested_guard", "UNWIND [0,0] AS x WITH x WHERE (x=0 OR CASE WHEN 1/x=1 THEN true END) AND x=0 RETURN x", [][]interface{}{{int64(0)}, {int64(0)}}},
		{"coalesce_case", "UNWIND [0,0] AS x WITH x WHERE coalesce(true, CASE WHEN 1/x=1 THEN true END) RETURN x", [][]interface{}{{int64(0)}, {int64(0)}}},
		{"nested_and", "UNWIND [0,0] AS x WITH x WHERE (x<>0 AND CASE WHEN 1/x=1 THEN true END) OR x=0 RETURN x", [][]interface{}{{int64(0)}, {int64(0)}}},
		{"exists_or", "UNWIND [0,0] AS x RETURN EXISTS { WITH x WHERE 1/x=1 RETURN x } OR x=0 AS v", [][]interface{}{{true}, {true}}},
		{"count_or", "UNWIND [0,0] AS x RETURN COUNT { WITH x WHERE 1/x=1 RETURN x } > 0 OR x=0 AS v", [][]interface{}{{true}, {true}}},
		{"filtered_integer", "UNWIND [1,{}] AS x RETURN [k IN [x] WHERE k=1 | toInteger(k)] AS v", [][]interface{}{{[]interface{}{int64(1)}}, {[]interface{}{}}}},
		{"filtered_float", "UNWIND [1,{}] AS x RETURN [k IN [x] WHERE k=1 | toFloat(k)] AS v", [][]interface{}{{[]interface{}{float64(1)}}, {[]interface{}{}}}},
		{"filtered_false", "UNWIND [1,{}] AS x RETURN [k IN [x] WHERE false | toInteger(k)] AS v", [][]interface{}{{[]interface{}{}}, {[]interface{}{}}}},
		{"lazy_integer", "UNWIND [1,{}] AS x RETURN coalesce(1,toInteger(x)) AS v", [][]interface{}{{int64(1)}, {int64(1)}}},
		{"filtered_labels", "CREATE (n:N) WITH n, [n,1] AS xs RETURN [k IN xs WHERE k=n | labels(k)] AS v", [][]interface{}{{[]interface{}{[]interface{}{"N"}}}}},
		{"filtered_type", "CREATE (a:N)-[r:R]->(b:N) WITH [r,1] AS xs RETURN [k IN xs WHERE k<>1 | type(k)] AS v", [][]interface{}{{[]interface{}{"R"}}}},
		{"skipped_comprehension", "UNWIND [0,0] AS x RETURN coalesce(1, [k IN [1/x] | k]) AS v", [][]interface{}{{int64(1)}, {int64(1)}}},
		{"skipped_conversion_comprehension", "UNWIND [0,0] AS x RETURN coalesce(1, [k IN [1/x] | toInteger(k)]) AS v", [][]interface{}{{int64(1)}, {int64(1)}}},
		{"null_predicates", "UNWIND [null,1] AS x RETURN x IS NULL AS n,x IS NOT NULL AS p", [][]interface{}{{true, false}, {false, true}}},
		{"integer_order", "UNWIND [0,1] AS x WITH x WHERE x+1<2 RETURN x", [][]interface{}{{int64(0)}}},
		{"count_order", "UNWIND [0,1] AS x WITH x WHERE COUNT { WITH x WHERE x=1 RETURN x } < 1 RETURN x", [][]interface{}{{int64(0)}}},
	}
	for _, operator := range []string{"STARTS WITH", "ENDS WITH", "CONTAINS"} {
		tests = append(tests, struct {
			name, query string
			want        [][]interface{}
		}{"context_" + operator, "RETURN abs(CASE WHEN NOT (null " + operator + " 'a') THEN 1 ELSE head([0,0]) END) AS v", [][]interface{}{{int64(0)}}})
		tests = append(tests, struct {
			name, query string
			want        [][]interface{}
		}{"context_list_" + operator, "RETURN abs(CASE WHEN head([null " + operator + " 'a',null]) IS NULL THEN 1 ELSE 0 END) AS v", [][]interface{}{{int64(1)}}})
		tests = append(tests, struct {
			name, query string
			want        [][]interface{}
		}{"context_nonstring_" + operator, "RETURN abs(CASE WHEN NOT (head([1]) " + operator + " 'a') THEN 1 ELSE head([0,0]) END) AS v", [][]interface{}{{int64(0)}}})
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for _, text := range []bool{false, true} {
				t.Run(fmt.Sprintf("text=%v", text), func(t *testing.T) {
					rowValueProgramsDisabled.Store(text)
					defer rowValueProgramsDisabled.Store(false)
					exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "r2"))
					result, err := exec.Execute(context.Background(), tc.query, nil)
					require.NoError(t, err)
					require.Equal(t, tc.want, result.Rows)
				})
			}
		})
	}
}

// These errors are independently checked on Neo4j; reached arguments still
// validate even though filtered projections and skipped lazy arguments do not.
func TestRowFunctionArgumentsMatchNeo4j(t *testing.T) {
	for _, query := range []string{
		"UNWIND [1,{}] AS x RETURN coalesce(null,toInteger(x)) AS v",
		"UNWIND [{}] AS x RETURN labels(x) AS v",
		"UNWIND [{labels:['N']}] AS x RETURN labels(x) AS v",
		"UNWIND [{type:'R'}] AS x RETURN type(x) AS v",
		"CREATE (n:N) WITH n, [n,1] AS xs RETURN [k IN xs WHERE true | labels(k)] AS v",
		"CREATE (a:N)-[r:R]->(b:N) WITH [r,1] AS xs RETURN [k IN xs WHERE true | type(k)] AS v",
		"UNWIND [0,0] AS x RETURN coalesce(null, [k IN [1/x] | toInteger(k)]) AS v",
		"UNWIND [0,0] AS x WITH x WHERE (x<>0 OR CASE WHEN 1/x=1 THEN true END) AND x=0 RETURN x",
		"UNWIND [0,0] AS x RETURN EXISTS { WITH x WHERE 1/x=1 RETURN x } OR x<>0 AS v",
		"UNWIND [0,0] AS x RETURN EXISTS { WITH x WHERE 1/x=1 RETURN x } AND x<>0 AS v",
	} {
		t.Run(query, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "r2errors"))
			_, err := exec.Execute(context.Background(), query, nil)
			require.Error(t, err)
		})
	}
}
