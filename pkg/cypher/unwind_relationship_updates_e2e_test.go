package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIssue1885UnwindRelationshipMergeUpdates(t *testing.T) {
	for _, tc := range []struct{ name, clause, expected string }{
		{"trailing_foreach", "FOREACH (_ IN CASE WHEN coalesce(e.y, '') <> 'n' THEN [1] ELSE [] END | SET e.y = 'n')", "n"},
		{"on_create_set", "ON CREATE SET e.y = 'created'", "created"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exec := NewStorageExecutor(newTestBadgerNamespacedStore(t))
			_, err := exec.Execute(t.Context(), "MERGE (:R {id: 'r1'})", nil)
			require.NoError(t, err)
			q := `UNWIND [{p: 'a'}] AS row
MATCH (r:R {id: 'r1'})
MERGE (g:G {id: 'g1'})
FOREACH (_ IN CASE WHEN coalesce(g.x, -1) <> 1 THEN [1] ELSE [] END | SET g.x = 1)
MERGE (r)-[e:T {p: row.p}]->(g)
` + tc.clause
			for range 2 {
				_, err := exec.Execute(t.Context(), q, nil)
				require.NoError(t, err)
			}
			result, err := exec.Execute(t.Context(), "MATCH (:R {id: 'r1'})-[e:T {p: 'a'}]->(g:G {id: 'g1'}) RETURN g.x, e.y", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(1), tc.expected}}, result.Rows)
		})
	}
}
