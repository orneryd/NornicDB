package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// The projected complex expression is a grouping key as a whole, not a
// license to reuse its components alongside an aggregate in ORDER BY.
func TestAggregationOrderDiagnosticPublicExecute(t *testing.T) {
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			for _, horizon := range []string{"RETURN me.age + you.age, count(*) AS cnt", "WITH me.age + you.age AS ages, count(*) AS cnt"} {
				t.Run(horizon, func(t *testing.T) {
					exec := setupTestExecutor(t)
					suffix := ""
					if horizon[:4] == "WITH" {
						suffix = " RETURN *"
					}
					prefix := "MATCH (me:Person)--(you:Person) " + horizon
					_, err := exec.Execute(t.Context(), prefix+" ORDER BY me.age + you.age + count(*)"+suffix, nil)
					requireSemanticDetail(t, err, "AmbiguousAggregationExpression")
					_, err = exec.Execute(t.Context(), "CREATE (:Person {age: 20})-[:KNOWS]->(:Person {age: 30})", nil)
					require.NoError(t, err)
					// An unaggregated ORDER BY can reuse the complete projected key.
					result, err := exec.Execute(t.Context(), prefix+" ORDER BY me.age + you.age"+suffix, nil)
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{int64(50), int64(2)}}, result.Rows)
				})
			}
			for _, query := range []string{
				"MATCH (me:Person) RETURN count(*) AS cnt ORDER BY me.age",
				"MATCH (me:Person) WITH count(*) AS cnt WHERE me.age > 1 RETURN *",
			} {
				_, err := setupTestExecutor(t).Execute(t.Context(), query, nil)
				var diagnostic interface{ BoltErrorDetail() string }
				require.ErrorAs(t, err, &diagnostic)
				require.Equal(t, "UndefinedVariable", diagnostic.BoltErrorDetail(), query)
			}
			for _, query := range []string{
				"MATCH (me:Person)--(you:Person) RETURN me.age, count(*) AS cnt ORDER BY me.age + count(*)",
				"MATCH (me:Person)--(you:Person) WITH me.age AS age, count(*) AS cnt ORDER BY age + count(*) RETURN *",
			} {
				exec := setupTestExecutor(t)
				_, err := exec.Execute(t.Context(), "CREATE (:Person {age: 20})-[:KNOWS]->(:Person {age: 30})", nil)
				require.NoError(t, err)
				result, err := exec.Execute(t.Context(), query, nil)
				require.NoError(t, err, query)
				require.Equal(t, [][]interface{}{{int64(20), int64(1)}, {int64(30), int64(1)}}, result.Rows)
			}
		})
	}
}
