package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A MATCH clause, its WHERE and what that WHERE nests may not mix the colon
// conjunction (:A:B) with label expression syntax (|, &, !, %, IS), across
// its patterns as within one chain; another clause, a CALL body, a UNION
// branch or a RETURN expression may. A relationship type's legacy | is not
// label expression syntax. Verdicts are Neo4j 5.26.30's and 2026.09's.
func TestLabelSyntaxMixedAcrossAMatchClause(t *testing.T) {
	ctx := context.Background()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	for _, tc := range []struct {
		query string
		mixed bool
	}{
		{"MATCH (n:A:B), (m:C|D) RETURN 1", true},
		{"MATCH (n:A:B)-[:!R]->(m) RETURN 1", true},
		{"MATCH (n:A:B)-[:R&S]->(m) RETURN 1", true},
		{"MATCH (n:A:B) WHERE n:C|D RETURN 1", true},
		{"MATCH (n:A:B) WHERE EXISTS { MATCH (m:C|D) } RETURN 1", true},
		{"MATCH (n:A:B), (m:%) RETURN 1", true},
		{"MATCH (n:A|B) WHERE n:C:D RETURN 1", true},
		{"MATCH (n IS A), (m:B:C) RETURN 1", true},
		{"MATCH (n:A:B)-[:R|S]->(m) RETURN 1", false},
		{"MATCH (n:A:B) RETURN n:C|D AS x", false},
		{"MATCH (n:A:B) WITH n MATCH (m:C|D) RETURN 1", false},
		{"MATCH (n:A:B) CALL () { MATCH (m:C|D) RETURN m } RETURN 1", false},
		{"MATCH (n:A:B) RETURN 1 AS x UNION MATCH (m:C|D) RETURN 1 AS x", false},
		{"MATCH (n:A), (m:C|D) RETURN 1", false},
		{"MATCH (n:A:B) RETURN COUNT { (m:C&D) } AS c", false},
		{"MATCH (n:A:B) WHERE n:C:D RETURN 1", false},
		{"CREATE (n:A:B) WITH n MATCH (m:C|D) RETURN 1", false},
		{"MATCH (m:C|D) CREATE (n:A:B) RETURN 1", false},
	} {
		_, err := exec.Execute(ctx, tc.query, nil)
		if tc.mixed {
			require.ErrorContains(t, err, "Mixing label expression symbols", tc.query)
		} else {
			require.NoError(t, err, tc.query)
		}
	}
}
