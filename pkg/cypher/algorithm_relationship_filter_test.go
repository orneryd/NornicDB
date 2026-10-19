package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func algorithmRelationshipFixture(t *testing.T, types ...string) *StorageExecutor {
	t.Helper()
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

func algorithmFilterRowsEqual(t *testing.T, proc string, want, got [][]interface{}) {
	t.Helper()
	require.Len(t, got, len(want))
	for i := range want {
		require.Equal(t, want[i][0], got[i][0])
		if proc == "pageRank" || proc == "betweenness" || proc == "closeness" {
			require.InDelta(t, want[i][1], got[i][1], 1e-10)
		} else {
			for j := range want {
				require.Equal(t, want[i][1] == want[j][1], got[i][1] == got[j][1], "partition of %v/%v", want[i][0], want[j][0])
			}
		}
	}
}

func TestAlgorithmRelationshipFilterContract(t *testing.T) {
	full := algorithmRelationshipFixture(t, "K", "L")
	empty := algorithmRelationshipFixture(t)
	for _, proc := range []string{"pageRank", "betweenness", "closeness", "louvain", "labelPropagation", "wcc"} {
		t.Run(proc, func(t *testing.T) {
			column := "score"
			if proc == "louvain" || proc == "labelPropagation" { column = "community" }
			if proc == "wcc" { column = "componentId" }
			read := func(exec *StorageExecutor, arguments string) [][]interface{} {
				result, err := exec.Execute(context.Background(), "CALL apoc.algo."+proc+"("+arguments+") YIELD node, "+column+" RETURN node.name, "+column+" ORDER BY node.name", nil)
				require.NoError(t, err, arguments)
				return result.Rows
			}
			all := read(full, "['P']")
			for _, arguments := range []string{"['P'], null", "['P'], ['*']", "['P'], ['K','L']", "['P'], ['*','K']", "['P'], {}"} {
				algorithmFilterRowsEqual(t, proc, all, read(full, arguments))
			}
			noEdges := read(empty, "['P']")
			for _, arguments := range []string{"['P'], []", "['P'], ['missing']"} {
				algorithmFilterRowsEqual(t, proc, noEdges, read(full, arguments))
			}
			algorithmFilterRowsEqual(t, proc, read(full, "['P'], ['K']"), read(full, "['P'], ['K','K']"))
			for _, arguments := range []string{"['P'], 1", "['P'], 'K'", "['P'], [1]", "['P'], [null]", "['P'], ['*',1]", "['P'], [], 1", "['P'], {}, {}", "['P'], [], {}, 1"} {
				_, err := full.Execute(context.Background(), "CALL apoc.algo."+proc+"("+arguments+") YIELD node RETURN node", nil)
				require.Error(t, err, arguments)
			}
			result, err := full.Execute(context.Background(), "MATCH (n:P) WHERE n.name IN ['a','b'] WITH collect(n) AS chosen CALL apoc.algo."+proc+"(chosen, ['K']) YIELD node RETURN node.name ORDER BY node.name", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"a"},{"b"}}, result.Rows)
		})
	}
}

func TestAlgorithmRelationshipFilterKeepsConfig(t *testing.T) {
	full := algorithmRelationshipFixture(t, "K", "L")
	physical := algorithmRelationshipFixture(t, "K")
	for _, proc := range []string{"pageRank", "louvain"} {
		column, config := "score", "{iterations: 1, dampingFactor: 0.5}"
		if proc == "louvain" { column, config = "community", "{weightProperty: 'w'}" }
		suffix := ") YIELD node, "+column+" RETURN node.name, "+column+" ORDER BY node.name"
		want, err := physical.Execute(context.Background(), "CALL apoc.algo."+proc+"(['P'], "+config+suffix, nil)
		require.NoError(t, err)
		got, err := full.Execute(context.Background(), "CALL apoc.algo."+proc+"(['P'], ['K'], "+config+suffix, nil)
		require.NoError(t, err)
		algorithmFilterRowsEqual(t, proc, want.Rows, got.Rows)
		all, err := full.Execute(context.Background(), "CALL apoc.algo."+proc+"(['P'], null, "+config+suffix, nil)
		require.NoError(t, err)
		legacy, err := full.Execute(context.Background(), "CALL apoc.algo."+proc+"(['P'], "+config+suffix, nil)
		require.NoError(t, err)
		algorithmFilterRowsEqual(t, proc, legacy.Rows, all.Rows)
	}
}

func TestAlgorithmRelationshipFilterMatchesPhysicalSubgraph(t *testing.T) {
	full := algorithmRelationshipFixture(t, "K", "L")
	for _, kind := range []string{"K", "L"} {
		physical := algorithmRelationshipFixture(t, kind)
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
