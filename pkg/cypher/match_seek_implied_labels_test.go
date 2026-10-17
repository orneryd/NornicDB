package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A property index on one label serves a lookup that names another label
// too, when the statement requires the indexed label: in the pattern, in any
// order, or as a top-level WHERE label test. Every node such a MATCH returns
// carries the indexed label, so the index lists it.
//
// The tests prove the seek by hiding one node from the index
// (PropertyIndexDelete): a seek misses it, a label scan finds it. Statements
// the index can't answer (the indexed label not required) must still find it.
func TestSeekThroughIndexOfImpliedLabel(t *testing.T) {
	for _, ddl := range []string{
		"CREATE CONSTRAINT graph_id FOR (n:GraphNode) REQUIRE n.id IS UNIQUE",
		"CREATE INDEX graph_id FOR (n:GraphNode) ON (n.id)",
	} {
		for _, explicit := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/explicit=%v", ddl[:17], explicit), func(t *testing.T) {
				exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "implied"))
				ctx := context.Background()
				_, err := exec.Execute(ctx, ddl, nil)
				require.NoError(t, err)
				_, err = exec.Execute(ctx, `UNWIND range(0, 49) AS i CREATE (:GraphNode:Identity {id: 'id' + toString(i), n: i})`, nil)
				require.NoError(t, err)
				_, err = exec.Execute(ctx, `CREATE (:Identity:Other {id: 'solo', n: -1}), (:GraphNode:Doc {id: 'doc', n: -2})`, nil)
				require.NoError(t, err)

				// Hide id7 from the GraphNode index.
				schema := exec.storage.GetSchema()
				hidden := schema.PropertyIndexLookup("GraphNode", "id", "id7")
				require.Len(t, hidden, 1)
				require.NoError(t, schema.PropertyIndexDelete("GraphNode", "id", hidden[0], "id7"))
				exec.ClearQueryCaches()

				if explicit {
					_, err = exec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
					t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })
				}
				params := map[string]interface{}{"hidden": "id7", "x": "id12", "solo": "solo", "items": []interface{}{
					map[string]interface{}{"id": "id12"}, map[string]interface{}{"id": "id7"}, map[string]interface{}{"id": "missing"},
				}}
				rows := func(query string) [][]interface{} {
					t.Helper()
					result, err := exec.Execute(ctx, query, params)
					require.NoError(t, err, query)
					return result.Rows
				}

				// Seeks through the GraphNode index: the hidden node is not found.
				for _, query := range []string{
					"MATCH (n:GraphNode:Identity {id: $hidden}) RETURN n.n",
					"MATCH (n:Identity:GraphNode {id: $hidden}) RETURN n.n",
					"MATCH (n:Identity {id: $hidden}) WHERE n:GraphNode RETURN n.n",
					"MATCH (n:Identity) WHERE n:GraphNode AND n.id = $hidden RETURN n.n",
					"MATCH (n:Identity) WHERE n.id = $hidden AND n:GraphNode RETURN n.n",
					"MATCH (n:Identity) WHERE n.id = $hidden AND (n:GraphNode) RETURN n.n",
					"MATCH (n:Identity) WHERE n.id = $hidden AND n:GraphNode:Identity RETURN n.n",
					"MATCH (n:Identity) WHERE n.id = $hidden AND n:GraphNode&Identity RETURN n.n",
					"MATCH (n:Identity) WHERE n.id IN [$hidden] AND n:GraphNode RETURN n.n",
				} {
					require.Empty(t, rows(query), "seek expected: %s", query)
				}

				// The same seeks find what the index lists, with the other
				// labels checked on every candidate.
				for _, test := range []struct {
					query string
					want  [][]interface{}
				}{
					{"MATCH (n:Identity:GraphNode {id: $x}) RETURN n.n", [][]interface{}{{int64(12)}}},
					{"MATCH (n:Identity {id: $x}) WHERE n:GraphNode RETURN n.n", [][]interface{}{{int64(12)}}},
					{"MATCH (n:Identity) WHERE n.id = $x AND n:GraphNode RETURN n.n", [][]interface{}{{int64(12)}}},
					{"MATCH (n:Identity:GraphNode {id: 'doc'}) RETURN n.n", nil},
					{"MATCH (n:Identity) WHERE n.id = 'doc' AND n:GraphNode RETURN n.n", nil},
					{"MATCH (n:Identity) WHERE n.id = $x AND n:GraphNode:Other RETURN n.n", nil},
					{"UNWIND $items AS item MATCH (n:Identity:GraphNode {id: item.id}) RETURN n.n", [][]interface{}{{int64(12)}}},
					{"UNWIND $items AS item OPTIONAL MATCH (n:Identity:GraphNode {id: item.id}) RETURN n.n", [][]interface{}{{int64(12)}, {nil}, {nil}}},
				} {
					require.Equal(t, test.want, nilIfEmptyRows(rows(test.query)), test.query)
				}

				// The GraphNode label is not required: the index can't answer,
				// and the hidden node and the Identity-only node are found.
				for _, test := range []struct {
					query string
					want  [][]interface{}
				}{
					{"MATCH (n:Identity {id: $hidden}) RETURN n.n", [][]interface{}{{int64(7)}}},
					{"MATCH (n:Identity) WHERE n.id = $hidden RETURN n.n", [][]interface{}{{int64(7)}}},
					{"MATCH (n:Identity {id: $hidden}) WHERE n:GraphNode OR n:Other RETURN n.n", [][]interface{}{{int64(7)}}},
					{"MATCH (n:Identity {id: $solo}) WHERE n:GraphNode OR n:Other RETURN n.n", [][]interface{}{{int64(-1)}}},
					{"MATCH (n:Identity {id: $solo}) WHERE NOT n:GraphNode RETURN n.n", [][]interface{}{{int64(-1)}}},
					{"MATCH (n:Identity) WHERE n.id = $solo AND NOT n:GraphNode RETURN n.n", [][]interface{}{{int64(-1)}}},
					{"MATCH (n:Identity {id: $solo}) WHERE n:GraphNode RETURN n.n", nil},
					{"MATCH (n:Identity) WHERE n.id = $hidden AND n:GraphNode|Other RETURN n.n", [][]interface{}{{int64(7)}}},
				} {
					require.Equal(t, test.want, nilIfEmptyRows(rows(test.query)), test.query)
				}
			})
		}
	}
}

func nilIfEmptyRows(rows [][]interface{}) [][]interface{} {
	if len(rows) == 0 {
		return nil
	}
	return rows
}

func TestWhereRequiredLabels(t *testing.T) {
	for _, test := range []struct {
		where string
		want  []string
	}{
		{"n:A", []string{"A"}},
		{"(n:A)", []string{"A"}},
		{"n:A:B", []string{"A", "B"}},
		{"n:A&B", []string{"A", "B"}},
		{"n:`Odd Label` AND n.id = 1", []string{"Odd Label"}},
		{"n.id = 1 AND n:A AND m:B", []string{"A"}},
		{"n.id = 1 AND (n:A AND n:B)", nil},
		{"n:A OR n:B", nil},
		{"n:A|B", nil},
		{"NOT n:A", nil},
		{"n:A = true", nil},
		{"n.id = 1", nil},
		{"", nil},
	} {
		require.Equal(t, test.want, whereRequiredLabels("n", test.where), test.where)
	}
}
