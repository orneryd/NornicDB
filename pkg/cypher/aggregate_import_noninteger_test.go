package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// An import that is known not to be an Integer (n.v * 1.0 is Float or
// Duration) keeps that fact through a global aggregate in a CALL body, so a
// list index or slice bound using it is rejected at compile time, as Neo4j
// 5.26.30 does, even when the outer input is empty.
func TestAggregateImportKeepsNonIntegerConstraint(t *testing.T) {
	for _, outer := range []struct{ name, setup string }{
		{"empty", ""},
		{"populated", "CREATE (:AggregateImportNonInteger {v: 1})"},
	} {
		for _, call := range []string{"CALL (x)", "CALL (*)"} {
			for _, use := range []string{"[1,2][x]", "[1,2][x..]", "[1,2][..x]"} {
				query := "MATCH (n:AggregateImportNonInteger) WITH n.v * 1.0 AS x " + call +
					" { WITH count(*) AS c RETURN " + use + " AS a } RETURN a"
				t.Run(outer.name+"/"+call+"/"+use, func(t *testing.T) {
					executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "aggregate_import_noninteger"))
					ctx := context.Background()
					if outer.setup != "" {
						_, err := executor.Execute(ctx, outer.setup, nil)
						require.NoError(t, err)
					}
					_, err := executor.Execute(ctx, query, nil)
					require.Error(t, err, query)
					var classified interface{ BoltErrorCode() string }
					require.True(t, errors.As(err, &classified), "unclassified error: %v", err)
					require.Equal(t, "Neo.ClientError.Statement.SyntaxError", classified.BoltErrorCode(), "%s: %v", query, err)
				})
			}
		}
	}
}

func TestJoinedOperandKeepsAgreedConstraints(t *testing.T) {
	constrained := signatureOperand([]string{"Float", "Duration"})
	require.Equal(t, constrained, joinedOperand(constrained, constrained))
	require.True(t, joinedOperand(constrained, constrained).excludes(isIntegerOrNumberTypeName))
	// An unknown alternative keeps no type, so nothing is excluded.
	require.False(t, joinedOperand(constrained, staticOperand{}).excludes(isIntegerOrNumberTypeName))
	// Typed alternatives keep every one of their types.
	joined := joinedOperand(constrained, signatureOperand([]string{"Float", "Integer"}))
	require.False(t, joined.excludes(isIntegerOrNumberTypeName))
	require.ElementsMatch(t, []string{"Duration", "Float", "Integer"}, staticTypeChoices(joined.typeName()))
	nested := knownOperand("Map")
	nested.members = map[string]staticOperand{"a": constrained}
	require.True(t, joinedOperand(nested, nested).members["a"].excludes(isIntegerOrNumberTypeName))
}
