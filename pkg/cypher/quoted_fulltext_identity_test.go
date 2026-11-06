package cypher

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestQuotedFulltextRelationshipTypesAndPropertiesStayExact(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		exec := NewStorageExecutor(newTestBadgerNamespacedStore(t), QueryCachePolicy{MaxEntries: 0})
		for _, statement := range []string{
			"CREATE ()-[:`T|U ` {` text `:'zephyrine', name:'wanted'}]->()",
			"CREATE ()-[:`T|U` {` text `:'zephyrine', name:'trimmed'}]->()",
			"CREATE ()-[:T {` text `:'zephyrine', name:'plain'}]->()",
			"CREATE ()-[:`a``b` {` text `:'zephyrine', name:'backtick'}]->()",
			"CREATE FULLTEXT INDEX exact_relationships FOR ()-[r:`T|U `]-() ON EACH [r.` text `]",
		} {
			_, err := exec.Execute(t.Context(), statement, nil)
			require.NoError(t, err, statement)
		}
		result, err := exec.Execute(t.Context(), "CALL db.index.fulltext.queryRelationships('exact_relationships','zephyrine') YIELD relationship RETURN relationship.name AS name", nil)
		require.NoError(t, err)
		require.Equal(t, [][]any{{"wanted"}}, result.Rows)

		for _, tc := range []struct {
			index, types string
			want         [][]any
		}{
			{"exact_backtick", "`a``b`", [][]any{{"backtick"}}},
			{"exact_alternatives", "`T|U `|`a``b`", [][]any{{"wanted"}, {"backtick"}}},
		} {
			_, err = exec.Execute(t.Context(), "CREATE FULLTEXT INDEX "+tc.index+" FOR ()-[r:"+tc.types+"]-() ON EACH [r.` text `]", nil)
			require.NoError(t, err)
			result, err = exec.Execute(t.Context(), "CALL db.index.fulltext.queryRelationships('"+tc.index+"','zephyrine') YIELD relationship RETURN relationship.name AS name", nil)
			require.NoError(t, err)
			require.ElementsMatch(t, tc.want, result.Rows)
		}
		for _, statement := range []string{
			"CREATE (:`Doc|Q ` {` text `:'zephyrine', name:'node-wanted'})",
			"CREATE (:Doc {` text `:'zephyrine', name:'node-plain'})",
			"CREATE FULLTEXT INDEX exact_nodes FOR (n:`Doc|Q `) ON EACH [n.` text `]",
		} {
			_, err = exec.Execute(t.Context(), statement, nil)
			require.NoError(t, err, statement)
		}
		result, err = exec.Execute(t.Context(), "CALL db.index.fulltext.queryNodes('exact_nodes','zephyrine') YIELD node RETURN node.name AS name", nil)
		require.NoError(t, err)
		require.Equal(t, [][]any{{"node-wanted"}}, result.Rows)
	})
}
