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

func TestCommonStaticOperandKeepsAgreedConstraints(t *testing.T) {
	constrained := staticOperand{display: "Float or Duration", nonInteger: true, nonBoolean: true}
	require.Equal(t, constrained, commonStaticOperand(constrained, constrained))
	require.False(t, commonStaticOperand(constrained, staticOperand{}).nonInteger)
	deferred := staticOperand{kind: "Integer", display: "Integer", predicateDeferred: true}
	require.True(t, commonStaticOperand(deferred, knownOperand("Integer")).predicateDeferred)
	nested := staticOperand{kind: "Map", display: "Map", members: map[string]staticOperand{"a": constrained}}
	require.True(t, commonStaticOperand(nested, nested).members["a"].nonInteger)
}
