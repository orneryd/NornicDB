package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestFinishAsNameMatchesNeo4j pins `finish` as a variable and alias name in
// both parsers (#958): FINISH is a clause keyword but not a reserved word in
// Neo4j 5.26, so a trailing `finish` where a name or an expression is read is
// the variable. Expected rows are Neo4j 5.26.30's.
func TestFinishAsNameMatchesNeo4j(t *testing.T) {
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
			ctx := context.Background()
			for _, step := range []struct {
				query string
				want  [][]interface{}
			}{
				{"CREATE (finish:Q {id: 1}) RETURN finish.id AS v", [][]interface{}{{int64(1)}}},
				{"MATCH (finish:Q) RETURN finish.id AS v", [][]interface{}{{int64(1)}}},
				{"WITH 1 AS finish RETURN finish", [][]interface{}{{int64(1)}}},
				{"UNWIND [1] AS finish RETURN finish", [][]interface{}{{int64(1)}}},
				{"WITH 1 AS finish RETURN DISTINCT finish", [][]interface{}{{int64(1)}}},
				{"WITH 1 AS finish, 2 AS b RETURN b, finish", [][]interface{}{{int64(2), int64(1)}}},
				{"WITH 1 AS finish RETURN 1 + finish", [][]interface{}{{int64(2)}}},
				{"WITH {finish: 1} AS m RETURN m.finish", [][]interface{}{{int64(1)}}},
				{"WITH 1 AS finish RETURN finish AS finish", [][]interface{}{{int64(1)}}},
				{"WITH 1 AS finish RETURN finish ORDER BY finish", [][]interface{}{{int64(1)}}},
				{"WITH true AS finish RETURN NOT finish", [][]interface{}{{false}}},
				{"WITH 1 AS FINISH RETURN FINISH", [][]interface{}{{int64(1)}}},
				{"MATCH (finish:Q) DELETE finish", nil},
				{"MATCH (n:Q) RETURN count(n) AS c", [][]interface{}{{int64(0)}}},
			} {
				result, err := exec.Execute(ctx, step.query, nil)
				require.NoError(t, err, step.query)
				if step.want == nil {
					require.Empty(t, result.Rows, step.query)
					continue
				}
				require.Equal(t, step.want, result.Rows, step.query)
			}

			// FINISH is still the clause after a complete clause, and still
			// invalid after RETURN.
			result, err := exec.Execute(ctx, "CREATE (:Q {id: 2}) FINISH", nil)
			require.NoError(t, err)
			require.Empty(t, result.Rows)
			_, err = exec.Execute(ctx, "MATCH (n:Q) RETURN n.id AS v FINISH", nil)
			require.Error(t, err)
			require.Contains(t, err.Error(), "Neo.ClientError.Statement.SyntaxError")
		})
	}
}
