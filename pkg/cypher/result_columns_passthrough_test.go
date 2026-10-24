package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestProjectionPassThroughUsesSyntax(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "neo4j"))
	for _, sample := range []struct {
		query string
		input []string
		want  bool
	}{
		{"WITH x AS x", []string{"x"}, true},
		{"WITH x AS `x`", []string{"x"}, true},
		{"WITH x AS y", []string{"x"}, false},
		{"WITH ``", []string{""}, true},
		{"WITH `` AS ``", []string{""}, true},
		{"WITH 1 AS ``", nil, false},
		{"WITH (x) AS x", []string{"x"}, false},
		{"WITH x + 0 AS x", []string{"x"}, false},
		{"WITH DISTINCT x ORDER BY x", []string{"x"}, true},
		{"WITH true AS true", nil, false},
		{"WITH false AS false", nil, false},
		{"WITH null AS null", nil, false},
		{"WITH `true`", []string{"true"}, true},
		{"WITH `false`", []string{"false"}, true},
		{"WITH `null`", []string{"null"}, true},
		{"WITH *", []string{"true", "false", "null", "x"}, true},
		{"WITH *, 1 AS x", []string{"true", "x"}, false},
		{"WITH *, x AS x", []string{"true", "x"}, true},
		{"WITH *", nil, false},
	} {
		t.Run(sample.query, func(t *testing.T) {
			var projection *resultmetadata.Projection
			executor.StatementFacts(sample.query+" RETURN 1 AS sentinel", resultmetadata.KnownScope(sample.input), false).Visit(func(statement resultmetadata.Statement) {
				require.NotEmpty(t, statement.Clauses)
				require.Equal(t, resultmetadata.ClauseWith, statement.Clauses[0].Kind)
				projection = statement.Clauses[0].Projection
			}, func() { t.Fatal("statement facts declined") }, func(reason resultmetadata.Unresolved) { t.Fatalf("statement facts unresolved: %v", reason.Err) })
			require.NotNil(t, projection)
			require.Equal(t, sample.want, projection.PassThrough)
			if sample.query == "WITH DISTINCT x ORDER BY x" {
				require.True(t, projection.Distinct)
				require.Equal(t, "ORDER BY x", projection.Modifiers)
			}
		})
	}
}
