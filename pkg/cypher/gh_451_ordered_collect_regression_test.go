package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestGH451_WithOrderByBeforeGroupedCollect(t *testing.T) {
	want := [][]interface{}{
		{"Acme", []interface{}{"Ann", "Bob"}},
		{"Bolt", []interface{}{"Cid"}},
	}
	queries := []struct {
		name  string
		query string
	}{
		{
			name:  "ordered WITH before collect",
			query: "MATCH (p:P)-[:WORKS_AT]->(c:C) WITH c, p ORDER BY p.name RETURN c.name AS company, collect(p.name) AS people ORDER BY company",
		},
		{
			name:  "no WITH control",
			query: "MATCH (p:P)-[:WORKS_AT]->(c:C) RETURN c.name AS company, collect(p.name) AS people ORDER BY company",
		},
	}

	for _, explicit := range []bool{false, true} {
		mode := "autocommit"
		if explicit {
			mode = "explicit transaction"
		}
		t.Run(mode, func(t *testing.T) {
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "gh451")
			for _, node := range []*storage.Node{
				{ID: "ann", Labels: []string{"P"}, Properties: map[string]interface{}{"name": "Ann", "age": 30}},
				{ID: "bob", Labels: []string{"P"}, Properties: map[string]interface{}{"name": "Bob", "age": 25}},
				{ID: "cid", Labels: []string{"P"}, Properties: map[string]interface{}{"name": "Cid", "age": 35}},
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

			for _, testCase := range queries {
				t.Run(testCase.name, func(t *testing.T) {
					result, err := exec.Execute(ctx, testCase.query, nil)
					require.NoError(t, err)
					require.Equal(t, want, result.Rows)
				})
			}

			if explicit {
				_, err := exec.Execute(ctx, "COMMIT", nil)
				require.NoError(t, err)
			}
		})
	}
}
