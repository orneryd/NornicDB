package cypher

import (
	"context"
	"sort"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestApocPathSpanningTree pins APOC 5.26's apoc.path.spanningTree rows
// (#907), read from Neo4j 5.26.30 with APOC: one path from the start node A
// to each node it reaches, the start's own zero-length path included, each
// path written as its node names.
func TestApocPathSpanningTree(t *testing.T) {
	for _, test := range []struct {
		name   string
		graph  string
		config string
		paths  []string
	}{
		{"tree", "CREATE (a:Node {name:'A'})-[:CONNECTS]->(b:Node {name:'B'})-[:CONNECTS]->(c:Node {name:'C'}), (a)-[:CONNECTS]->(d:Node {name:'D'})",
			"{}", []string{"A", "AB", "ABC", "AD"}},
		{"a cycle reaches each node once", "CREATE (a:Node {name:'A'})-[:CONNECTS]->(b:Node {name:'B'})-[:CONNECTS]->(c:Node {name:'C'})-[:CONNECTS]->(a), (a)-[:CONNECTS]->(d:Node {name:'D'})",
			"{}", []string{"A", "AB", "AC", "AD"}},
		{"maxLevel", "CREATE (a:Node {name:'A'})-[:CONNECTS]->(b:Node {name:'B'})-[:CONNECTS]->(c:Node {name:'C'})-[:CONNECTS]->(d:Node {name:'D'})",
			"{maxLevel: 2}", []string{"A", "AB", "ABC"}},
		{"relationshipFilter", "CREATE (a:Node {name:'A'})-[:FRIEND]->(b:Node {name:'B'})-[:FRIEND]->(c:Node {name:'C'}), (a)-[:COLLEAGUE]->(d:Node {name:'D'})",
			"{relationshipFilter: 'FRIEND'}", []string{"A", "AB", "ABC"}},
		{"depth first", "CREATE (a:Node {name:'A'})-[:CONNECTS]->(b:Node {name:'B'}), (a)-[:CONNECTS]->(:Node {name:'C'}), (b)-[:CONNECTS]->(:Node {name:'D'}), (b)-[:CONNECTS]->(:Node {name:'E'})",
			"{bfs: false}", []string{"A", "AB", "ABD", "ABE", "AC"}},
		{"labelFilter", "CREATE (a:Start {name:'A'})-[:CONNECTS]->(b:Good {name:'B'})-[:CONNECTS]->(c:Good {name:'C'}), (a)-[:CONNECTS]->(d:Bad {name:'D'})",
			"{labelFilter: '+Good'}", []string{"A", "AB", "ABC"}},
		{"another component is not reached", "CREATE (a:Node {name:'A'})-[:CONNECTS]->(b:Node {name:'B'}), (c:Node {name:'C'})-[:CONNECTS]->(d:Node {name:'D'})",
			"{}", []string{"A", "AB"}},
		{"outgoing, mark first", "CREATE (a:Node {name:'A'})-[:POINTS_TO]->(b:Node {name:'B'})-[:POINTS_TO]->(c:Node {name:'C'}), (d:Node {name:'D'})-[:POINTS_TO]->(a)",
			"{relationshipFilter: '>POINTS_TO'}", []string{"A", "AB", "ABC"}},
		{"incoming, mark last", "CREATE (a:Node {name:'A'})-[:POINTS_TO]->(b:Node {name:'B'})-[:POINTS_TO]->(c:Node {name:'C'}), (d:Node {name:'D'})-[:POINTS_TO]->(a)",
			"{relationshipFilter: 'POINTS_TO<'}", []string{"A", "AD"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
			ctx := context.Background()
			_, err := exec.Execute(ctx, test.graph, nil)
			require.NoError(t, err)
			result, err := exec.Execute(ctx, "MATCH (s {name: 'A'}) CALL apoc.path.spanningTree(s, "+test.config+") YIELD path RETURN [n IN nodes(path) | n.name] AS names", nil)
			require.NoError(t, err)
			var paths []string
			for _, row := range result.Rows {
				var names []string
				for _, name := range row[0].([]interface{}) {
					names = append(names, name.(string))
				}
				paths = append(paths, strings.Join(names, ""))
			}
			sort.Strings(paths)
			require.Equal(t, test.paths, paths)
		})
	}

	t.Run("limit", func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
		ctx := context.Background()
		_, err := exec.Execute(ctx, "CREATE (a:Node {name:'A'}) WITH a UNWIND ['B', 'C', 'D', 'E'] AS name CREATE (a)-[:CONNECTS]->(:Node {name: name})", nil)
		require.NoError(t, err)
		// The start's path, then one neighbour: which one follows the
		// store's relationship order, which Neo4j doesn't specify.
		result, err := exec.Execute(ctx, "MATCH (s {name: 'A'}) CALL apoc.path.spanningTree(s, {limit: 2}) YIELD path RETURN length(path) AS length", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(0)}, {int64(1)}}, result.Rows)
	})

	t.Run("a map is not a start node", func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
		_, err := exec.Execute(context.Background(), "CALL apoc.path.spanningTree({name: 'A'}, {}) YIELD path RETURN path", nil)
		require.ErrorContains(t, err, "Failed to invoke procedure `apoc.path.spanningTree`")
		requireStatusCode(t, err, "Neo.ClientError.Procedure.ProcedureCallFailed")
	})
}
