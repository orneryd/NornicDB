package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestGH450_ReturnPropertyAliasedToVariableAfterWith(t *testing.T) {
	cases := []struct {
		name  string
		query string
		want  [][]interface{}
	}{
		{
			name:  "alias matches WITH variable",
			query: "MATCH (p:P)-[:WORKS_AT]->(c:C) WITH c, count(p) AS n RETURN c.name AS c, n ORDER BY n DESC",
			want:  [][]interface{}{{"Acme", int64(2)}, {"Bolt", int64(1)}},
		},
		{
			name:  "different alias control",
			query: "MATCH (p:P)-[:WORKS_AT]->(c:C) WITH c, count(p) AS n RETURN c.name AS company, n ORDER BY n DESC",
			want:  [][]interface{}{{"Acme", int64(2)}, {"Bolt", int64(1)}},
		},
		{
			name:  "no WITH control",
			query: "MATCH (c:C) RETURN c.name AS c ORDER BY c",
			want:  [][]interface{}{{"Acme"}, {"Bolt"}},
		},
	}

	for _, explicit := range []bool{false, true} {
		mode := "autocommit"
		if explicit {
			mode = "explicit transaction"
		}
		t.Run(mode, func(t *testing.T) {
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "gh450")
			for _, node := range []*storage.Node{
				{ID: "ann", Labels: []string{"P"}, Properties: map[string]interface{}{"name": "Ann", "age": 30}},
				{ID: "bob", Labels: []string{"P"}, Properties: map[string]interface{}{"name": "Bob", "age": 25}},
				{ID: "cid", Labels: []string{"P"}, Properties: map[string]interface{}{"name": "Cid", "age": 35}},
				{ID: "dee", Labels: []string{"P"}, Properties: map[string]interface{}{"name": "Dee", "city": "Riga"}},
				{ID: "acme", Labels: []string{"C"}, Properties: map[string]interface{}{"name": "Acme"}},
				{ID: "bolt", Labels: []string{"C"}, Properties: map[string]interface{}{"name": "Bolt"}},
			} {
				_, err := store.CreateNode(node)
				require.NoError(t, err)
			}
			for _, edge := range []*storage.Edge{
				{ID: "ann-acme", StartNode: "ann", EndNode: "acme", Type: "WORKS_AT"},
				{ID: "bob-acme", StartNode: "bob", EndNode: "acme", Type: "WORKS_AT"},
				{ID: "cid-bolt", StartNode: "cid", EndNode: "bolt", Type: "WORKS_AT"},
			} {
				require.NoError(t, store.CreateEdge(edge))
			}
			exec := NewStorageExecutor(store)
			ctx := context.Background()
			if explicit {
				_, err := exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
			}

			for _, testCase := range cases {
				t.Run(testCase.name, func(t *testing.T) {
					result, err := exec.Execute(ctx, testCase.query, nil)
					require.NoError(t, err)
					require.Equal(t, testCase.want, result.Rows)
				})
			}

			if explicit {
				_, err := exec.Execute(ctx, "COMMIT", nil)
				require.NoError(t, err)
			}
		})
	}
}
