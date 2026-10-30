package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestGH455_RelationshipMergeOnCreatePersistsAndMatches(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		mode := "autocommit"
		if explicit {
			mode = "explicit transaction"
		}
		t.Run(mode, func(t *testing.T) {
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "gh455")
			for _, node := range []*storage.Node{
				{ID: "ann", Labels: []string{"P"}, Properties: map[string]interface{}{"name": "Ann", "age": 30}},
				{ID: "dee", Labels: []string{"P"}, Properties: map[string]interface{}{"name": "Dee", "city": "Riga"}},
			} {
				_, err := store.CreateNode(node)
				require.NoError(t, err)
			}
			exec := NewStorageExecutor(store)
			ctx := context.Background()
			if explicit {
				_, err := exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
			}

			result, err := exec.Execute(ctx,
				"MATCH (a:P {name:'Ann'}), (d:P {name:'Dee'}) MERGE (a)-[r:KNOWS]->(d) ON CREATE SET r.w = 1 RETURN r.w AS w", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)

			result, err = exec.Execute(ctx,
				"MATCH (a:P {name:'Ann'}), (d:P {name:'Dee'}) MERGE (a)-[r:KNOWS]->(d) ON CREATE SET r.w = 1 ON MATCH SET r.w = r.w + 1 RETURN r.w AS w", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows)

			if explicit {
				_, err := exec.Execute(ctx, "COMMIT", nil)
				require.NoError(t, err)
			}
			readback, err := exec.Execute(ctx,
				"MATCH (:P {name:'Ann'})-[r:KNOWS]->(:P {name:'Dee'}) RETURN r.w AS w, count(*) AS c", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(2), int64(1)}}, readback.Rows)
		})
	}
}
