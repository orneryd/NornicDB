package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// TestDeleteWithWithLimitMembershipMixedTypesPublic pins the canonical
// pipeline outcome for `MATCH ... WHERE ... WITH n LIMIT 1 DETACH DELETE n`:
// only the node whose property actually equals a list element is deleted, and
// every other node, relationship, label and property is untouched.
func TestDeleteWithWithLimitMembershipMixedTypesPublic(t *testing.T) {
	const query = "MATCH (n:Thing) WHERE n.code IN $codes WITH n LIMIT 1 DETACH DELETE n RETURN count(n)"

	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })

			exec, store := newTestExecutor(t, "delete_with_limit_mixed")
			ctx := context.Background()
			_, err := exec.Execute(ctx, `
CREATE (num:Thing {code: 1, name: 'numeric'}),
       (str:Thing {code: '1', name: 'string'}),
       (bool:Thing {code: true, name: 'boolean'}),
       (boolStr:Thing {code: 'true', name: 'booleanString'}),
       (absent:Thing {name: 'absent'}),
       (keeper:Thing {code: 'keep', name: 'keeper'})-[:LINKS]->(target:Target {name: 'target'})
`, nil)
			require.NoError(t, err)

			count := func(query string) int64 {
				t.Helper()
				got, err := exec.Execute(ctx, query, nil)
				require.NoError(t, err)
				require.Len(t, got.Rows, 1)
				require.Len(t, got.Rows[0], 1)
				value, ok := got.Rows[0][0].(int64)
				require.True(t, ok, "count returns an integer: %T", got.Rows[0][0])
				return value
			}

			// A string list element matches only the string property.
			result, err := exec.Execute(ctx, query, map[string]interface{}{"codes": []interface{}{"1"}})
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
			require.EqualValues(t, 1, result.Stats.NodesDeleted)

			require.EqualValues(t, 0, count("MATCH (n:Thing {code: '1'}) RETURN count(n)"))
			require.EqualValues(t, 1, count("MATCH (n:Thing {name: 'numeric'}) RETURN count(n)"))
			require.EqualValues(t, 1, count("MATCH (n:Thing {name: 'boolean'}) RETURN count(n)"))
			require.EqualValues(t, 1, count("MATCH (n:Thing {name: 'booleanString'}) RETURN count(n)"))
			require.EqualValues(t, 1, count("MATCH (n:Thing {name: 'absent'}) RETURN count(n)"))
			// The healthy node, its relationship and its far end are untouched
			// with their properties intact.
			require.EqualValues(t, 1, count("MATCH (:Thing {name: 'keeper'})-[r:LINKS]->(:Target {name: 'target'}) RETURN count(r)"))
			numeric, err := exec.Execute(ctx, "MATCH (n:Thing {name: 'numeric'}) RETURN n.code, n.name", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(1), "numeric"}}, numeric.Rows)

			// A null list element is unknown for every property, so nothing is
			// deleted and the absent-property node survives.
			result, err = exec.Execute(ctx, query, map[string]interface{}{"codes": []interface{}{nil}})
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)
			require.EqualValues(t, 0, result.Stats.NodesDeleted)
			require.EqualValues(t, 1, count("MATCH (n:Thing {name: 'absent'}) RETURN count(n)"))

			// The healthy boolean match still deletes.
			result, err = exec.Execute(ctx, query, map[string]interface{}{"codes": []interface{}{true}})
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
			require.EqualValues(t, 0, count("MATCH (n:Thing {name: 'boolean'}) RETURN count(n)"))
			require.EqualValues(t, 1, count("MATCH (n:Thing {name: 'numeric'}) RETURN count(n)"))

			// The two deleted Things are gone (seven entities created); every
			// other node and the single relationship remain.
			nodes, err := store.AllNodes(ctx)
			require.NoError(t, err)
			require.Len(t, nodes, 5)
			edges, err := store.AllEdges(ctx)
			require.NoError(t, err)
			require.Len(t, edges, 1)
		})
	}
}
