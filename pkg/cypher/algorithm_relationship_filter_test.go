package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestAlgorithmRelationshipFilterMatchesPhysicalSubgraph(t *testing.T) {
	fixture := func(types ...string) *StorageExecutor {
		store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "filtered_algo")
		for _, id := range []string{"a", "b", "c", "d"} {
			_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(id), Labels: []string{"P"}, Properties: map[string]interface{}{"name": id}})
			require.NoError(t, err)
		}
		for _, kind := range types {
			pairs := [][2]string{{"a", "b"}, {"b", "c"}}
			if kind == "L" { pairs = [][2]string{{"a", "c"}, {"c", "d"}} }
			for _, pair := range pairs {
				require.NoError(t, store.CreateEdge(&storage.Edge{ID: storage.EdgeID(kind+pair[0]+pair[1]), StartNode: storage.NodeID(pair[0]), EndNode: storage.NodeID(pair[1]), Type: kind}))
			}
		}
		return NewStorageExecutor(store)
	}
	full := fixture("K", "L")
	for _, kind := range []string{"K", "L"} {
		physical := fixture(kind)
		for _, proc := range []string{"pageRank", "betweenness", "closeness", "louvain", "labelPropagation", "wcc"} {
			t.Run(proc+"/"+kind, func(t *testing.T) {
				column := "score"
				if proc == "louvain" || proc == "labelPropagation" { column = "community" }
				if proc == "wcc" { column = "componentId" }
				suffix := ") YIELD node, "+column+" RETURN node.name, "+column+" ORDER BY node.name"
				want, err := physical.Execute(context.Background(), "CALL apoc.algo."+proc+"(['P']"+suffix, nil)
				require.NoError(t, err)
				got, err := full.Execute(context.Background(), "CALL apoc.algo."+proc+"(['P'], ['"+kind+"']"+suffix, nil)
				require.NoError(t, err)
				require.Len(t, got.Rows, len(want.Rows))
				for i := range want.Rows {
					require.Equal(t, want.Rows[i][0], got.Rows[i][0])
					if column == "score" {
						require.InDelta(t, want.Rows[i][1], got.Rows[i][1], 1e-10)
					} else {
						for j := range want.Rows {
							require.Equal(t, want.Rows[i][1] == want.Rows[j][1], got.Rows[i][1] == got.Rows[j][1], "partition of %v/%v", want.Rows[i][0], want.Rows[j][0])
						}
					}
				}
			})
		}
	}
}
