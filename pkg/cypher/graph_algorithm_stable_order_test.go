package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// newStableOrderGraph is two triangles joined by one relationship, and a node
// on its own.
func newStableOrderGraph(t *testing.T) *StorageExecutor {
	t.Helper()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	_, err := exec.Execute(context.Background(), `
		CREATE (a:N {name: 'a'}), (b:N {name: 'b'}), (c:N {name: 'c'}),
		       (d:N {name: 'd'}), (e:N {name: 'e'}), (f:N {name: 'f'}),
		       (g:N {name: 'g'}),
		       (a)-[:R]->(b), (b)-[:R]->(c), (c)-[:R]->(a),
		       (d)-[:R]->(e), (e)-[:R]->(f), (f)-[:R]->(d),
		       (c)-[:R]->(d)`, nil)
	require.NoError(t, err)
	return exec
}

// stableOrderRows runs query and gives its rows as text, the node as its name.
func stableOrderRows(t *testing.T, exec *StorageExecutor, query string) []string {
	t.Helper()
	result, err := exec.Execute(context.Background(), query, nil)
	require.NoError(t, err)
	rows := make([]string, 0, len(result.Rows))
	for _, row := range result.Rows {
		rows = append(rows, fmt.Sprintf("%v %v", row...))
	}
	return rows
}

// TestGraphAlgorithmsGiveTheSameRowsEveryCall: rows in the given nodes' order
// and community IDs numbered by each community's first node in that order,
// not map order (#1183).
func TestGraphAlgorithmsGiveTheSameRowsEveryCall(t *testing.T) {
	exec := newStableOrderGraph(t)
	const nodes = "MATCH (n:N) WITH n ORDER BY n.name DESC WITH collect(n) AS nodes "
	for _, tc := range []struct{ name, query string }{
		{"louvain", nodes + "CALL apoc.algo.louvain(nodes) YIELD node, community RETURN node.name, community"},
		{"labelPropagation", nodes + "CALL apoc.algo.labelPropagation(nodes) YIELD node, community RETURN node.name, community"},
		{"wcc", nodes + "CALL apoc.algo.wcc(nodes) YIELD node, componentId RETURN node.name, componentId"},
		{"betweenness", nodes + "CALL apoc.algo.betweenness(nodes) YIELD node, score RETURN node.name, score"},
		{"closeness", nodes + "CALL apoc.algo.closeness(nodes) YIELD node, score RETURN node.name, score"},
		{"pageRank", nodes + "CALL apoc.algo.pageRank(nodes) YIELD node, score RETURN node.name, score"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			first := stableOrderRows(t, exec, tc.query)
			require.Len(t, first, 7)
			for i, name := range []string{"g", "f", "e", "d", "c", "b", "a"} {
				require.Regexp(t, "^"+name+" ", first[i], "rows follow the given nodes' order")
			}
			for run := 0; run < 20; run++ {
				require.Equal(t, first, stableOrderRows(t, exec, tc.query))
			}
		})
	}

	t.Run("community IDs by first node", func(t *testing.T) {
		for _, query := range []string{
			nodes + "CALL apoc.algo.louvain(nodes) YIELD node, community RETURN node.name, community",
			nodes + "CALL apoc.algo.labelPropagation(nodes) YIELD node, community RETURN node.name, community",
		} {
			// IDs 0, 1, 2, … in the order each community's first node comes;
			// g is alone, so community 0.
			result, err := exec.Execute(context.Background(), query, nil)
			require.NoError(t, err)
			next := int64(0)
			for _, row := range result.Rows {
				community, ok := row[1].(int64)
				if !ok {
					community = int64(row[1].(int))
				}
				require.LessOrEqual(t, community, next, query)
				if community == next {
					next++
				}
			}
			require.Equal(t, "g 0", stableOrderRows(t, exec, query)[0], query)
		}
	})

	t.Run("every node", func(t *testing.T) {
		// Read from storage, the nodes are in ID order on every call.
		query := "CALL apoc.algo.wcc() YIELD node, componentId RETURN node.name, componentId"
		first := stableOrderRows(t, exec, query)
		require.Len(t, first, 7)
		for run := 0; run < 20; run++ {
			require.Equal(t, first, stableOrderRows(t, exec, query))
		}
		require.Equal(t, first, stableOrderRows(t, exec, "CALL apoc.algo.wcc('N') YIELD node, componentId RETURN node.name, componentId"))
	})

	t.Run("fastRP", func(t *testing.T) {
		_, err := exec.Execute(context.Background(), "CALL gds.graph.project('stable', 'N', 'R')", nil)
		require.NoError(t, err)
		query := "CALL gds.fastRP.stream('stable', {embeddingDimension: 4, randomSeed: 7}) YIELD nodeId, embedding RETURN nodeId, embedding"
		first := stableOrderRows(t, exec, query)
		require.Len(t, first, 7)
		for run := 0; run < 20; run++ {
			require.Equal(t, first, stableOrderRows(t, exec, query))
		}
	})
}

func TestAlgorithmRowsListANodeOnce(t *testing.T) {
	a, b := &storage.Node{ID: "a"}, &storage.Node{ID: "b"}
	rows := algorithmRows(algorithmNativeNodes([]*storage.Node{b, a, b, {ID: "c"}}, nil), map[graphEntityKey]int{"a": 1, "b": 2})
	require.Equal(t, [][]interface{}{{b, 2}, {a, 1}}, rows)
}

func TestRenumberInNodeOrderSkipsNodesWithoutCommunity(t *testing.T) {
	a, b, c := &storage.Node{ID: "a"}, &storage.Node{ID: "b"}, &storage.Node{ID: "c"}
	numbers := renumberInNodeOrder(algorithmNativeNodes([]*storage.Node{c, a, b}, nil), map[graphEntityKey]int{"a": 7, "b": 3})
	require.Equal(t, map[graphEntityKey]int{"a": 0, "b": 1}, numbers)
}
