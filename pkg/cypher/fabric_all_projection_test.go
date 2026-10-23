package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
	"github.com/stretchr/testify/require"
)

func TestFabricAllProjectionClauseBoundaries(t *testing.T) {
	for _, query := range []string{
		"USE comp WITH 7 AS all WITH all CALL { WITH all USE comp.one RETURN all AS k } RETURN k",
		"WITH 7 AS all WITH all RETURN all AS k",
		"WITH 7 AS all WITH all USE comp.one RETURN all AS k",
		"WITH 7 AS all WITH all OPTIONAL CALL { RETURN 1 AS k } RETURN all",
		"WITH 7 AS all WITH all MATCH (n) RETURN all",
		"WITH 7 AS all WITH all OPTIONAL MATCH (n) RETURN all",
		"WITH 7 AS all WITH all UNWIND range(1, 1) AS n RETURN all",
		"WITH 7 AS all WITH all CREATE (:N {v:all}) RETURN all",
	} {
		for _, cypher25 := range []bool{false, true} {
			actual, edits, err := desugarCypher25Expressions(query, cypher25)
			require.NoError(t, err, query)
			require.Equal(t, query, actual, "a bound name before a clause is not a projection modifier")
			require.Empty(t, edits, query)
		}
	}
	for _, statement := range []string{"RETURN ALL call AS k", "RETURN ALL use AS k", "WITH ALL match AS k RETURN k"} {
		actual, edits, err := desugarCypher25Expressions(statement, true)
		require.NoError(t, err)
		require.NotEqual(t, statement, actual, "the same keyword can still be the projected expression")
		require.NotEmpty(t, edits)
	}
}

func TestFabricImportProjectionFacts(t *testing.T) {
	exec := newFabricCallRowsExecutor(t)
	for _, tc := range []struct {
		projection string
		identity   bool
	}{
		{"use", true}, {"use AS use", true}, {"DISTINCT use", true}, {"*", true},
		{"`true`", true}, {"`false`", true}, {"`null`", true},
		{"true", false}, {"false", false}, {"null", false},
		{"use AS other", false}, {"use + 0 AS use", false}, {"$use AS use", false}, {"*, use + 1 AS other", false},
	} {
		query := "WITH " + tc.projection + " RETURN 1 AS k"
		exec.StatementFacts(query, resultmetadata.KnownScope([]string{"use", "true", "false", "null"}), false).Visit(func(statement resultmetadata.Statement) {
			require.Equal(t, tc.identity, statement.Clauses[0].Projection.PassThrough, tc.projection)
		}, func() { t.Fatal("facts declined") }, func(u resultmetadata.Unresolved) { t.Fatal(u.Err) })
	}
	for _, projection := range []string{"use", "DISTINCT use", "*"} {
		query := "WITH " + projection + " ORDER BY use LIMIT 1 WHERE use = 7 RETURN 1 AS k"
		exec.StatementFacts(query, resultmetadata.KnownScope([]string{"use"}), false).Visit(func(statement resultmetadata.Statement) {
			facts := statement.Clauses[0].Projection
			require.True(t, facts.PassThrough)
			require.Equal(t, "use = 7", facts.Predicate)
			require.Equal(t, "ORDER BY use LIMIT 1", facts.Modifiers)
		}, func() { t.Fatal("facts declined") }, func(u resultmetadata.Unresolved) { t.Fatal(u.Err) })
	}
}
