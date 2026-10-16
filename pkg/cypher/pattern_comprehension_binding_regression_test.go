package cypher

import (
	"context"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestPatternComprehensionBindingAndIteratorRegressions(t *testing.T) {
	for _, tc := range []struct {
		name, query, code string
		want              interface{}
	}{
		{"heterogeneous_node_binding", `MATCH (n:Q {id:1}) RETURN [e IN [n,1] | [(e)-->(w) | w.id]] AS v`, "Neo.ClientError.Statement.TypeError", nil},
		{"heterogeneous_ordinary_node_marker_map", `MATCH (n:Q {id:1}) RETURN [e IN [n,{_nodeId:'binding_regression:missing'}] | [(e)-->(w) | w.id]] AS v`, "Neo.ClientError.Statement.TypeError", nil},
		{"parenthesized_iterator", `MATCH (n:Q {id:1}) RETURN [(n)-->(z) | [(z.id) IN [2] | z]] AS v`, "Neo.ClientError.Statement.SyntaxError", nil},
		{"empty_iterator", `MATCH (n:Q {id:1}) RETURN [(n)-->(z) | [ IN [2] | z]] AS v`, "Neo.ClientError.Statement.SyntaxError", nil},
		{"commented_relationship_head", `MATCH (n:Q {id:1}) RETURN [(n) /* edge */-->(w) WHERE w.id IN [2] | w.id] AS v`, "", []interface{}{int64(2)}},
		{"mixed_null_binding", `MATCH (n:Q {id:1}) RETURN [e IN [n,null] | [(e)-->(w) | w.id]] AS v`, "", []interface{}{[]interface{}{int64(2)}, []interface{}{}}},
		{"null_binding", `WITH null AS e RETURN [(e)-->(w) | w.id] AS v`, "", []interface{}{}},
		{"local_node_binding", `MATCH (n:Q {id:1}) RETURN [e IN [n] | [(e)-->(w) | w.id]] AS v`, "", []interface{}{[]interface{}{int64(2)}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "binding_regression"))
			_, err := e.Execute(context.Background(), `CREATE (:Q {id:1})-[:R]->(:Q {id:2})`, nil)
			require.NoError(t, err)
			result, err := e.Execute(context.Background(), tc.query, nil)
			if tc.code != "" {
				require.Error(t, err)
				code, _ := nornicerrors.Neo4jStatus(err)
				require.Equal(t, tc.code, code)
			} else {
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{tc.want}}, result.Rows)
			}
		})
	}
}
