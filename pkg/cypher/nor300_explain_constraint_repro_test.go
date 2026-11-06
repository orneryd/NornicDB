package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// NOR-300 / PR #1695 reproduction: EXPLAIN and PROFILE of a supported native
// constraint declaration must return its empty result header, and EXPLAIN must
// not create the constraint.
//
// STATUS: fails on release/v1.4.4 (69115e8a2) with "Cannot establish the
// statement scope and result columns" (localization.CypherResultMetadataUnresolved,
// raised from pkg/cypher/result_columns.go:33 and pkg/cypher/resultmetadata/facts.go:244).
// SUSPECTED CAUSE: the result-metadata owner has no zero-column contract for
// native schema commands, so EXPLAIN/PROFILE cannot resolve them. PR #1695
// splits native schema argument preparation from execution so metadata can read
// the existing empty-header contract without executing. Handoff: see the PR comment.
func TestNOR300ExplainProfileNativeConstraintDeclarations(t *testing.T) {
	declarations := []string{
		"CREATE CONSTRAINT example_key FOR (n:Example) REQUIRE (n.a, n.b) IS NODE KEY",
		"CREATE CONSTRAINT unit_rel_key FOR ()-[r:UnitRel]-() REQUIRE (r.a, r.b) IS RELATIONSHIP KEY",
		"CREATE CONSTRAINT unit_unique FOR (n:UnitUnique) REQUIRE (n.a, n.b) IS UNIQUE",
		"CREATE CONSTRAINT unit_temporal FOR (n:UnitTime) REQUIRE (n.a, n.from, n.to) IS TEMPORAL NO OVERLAP",
	}
	withCypherParsers(t, func(t *testing.T) {
		for _, prefix := range []string{"EXPLAIN ", "PROFILE ", "CYPHER 5 EXPLAIN ", "/* c */ EXPLAIN "} {
			for _, decl := range declarations {
				t.Run(prefix+decl, func(t *testing.T) {
					executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor300"))
					result, err := executor.Execute(t.Context(), prefix+decl, nil)
					require.NoError(t, err)
					require.Empty(t, result.Columns)
				})
			}
		}
	})
}
