package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// internalCountFixture is the population every count shape below reads: one
// user label whose hidden nodes (labels starting with "_") sort around the
// visible ones, and a relationship out of each kind of node. Ordered by id the
// nodes are 1 (hidden), 2, 3, 5 (hidden), 6, and the relationships are
// 1->2, 2->3 and 5->6, so a hidden endpoint's relationships are stored but
// never a MATCH row.
func internalCountFixture(t *testing.T, ctx context.Context, exec *StorageExecutor) {
	t.Helper()
	for _, statement := range []string{
		`CREATE (a:Nor177Count:_internal {id: 1}), (b:Nor177Count {id: 2}),
			(c:Nor177Count {id: 3}), (d:Nor177Count:_internal {id: 5}),
			(e:Nor177Count {id: 6}),
			(a)-[:NOR177_LINKED {w: 1}]->(b), (b)-[:NOR177_LINKED {w: 2}]->(c),
			(d)-[:NOR177_LINKED {w: 3}]->(e)`,
		"CREATE INDEX nor177_count_id FOR (n:Nor177Count) ON (n.id)",
	} {
		_, err := exec.Execute(ctx, statement, nil)
		require.NoError(t, err, statement)
	}
}

// TestMatchCountOfInternalLabelIsZero reads the count shapes whose pattern
// names an internal label through the public executor. Every node and every
// endpoint such a pattern accepts is one a MATCH row hides, so the count is 0
// on a user database whatever the store counters hold; the rows of the same
// pattern are empty too. Each parser configuration answers every shape.
func TestMatchCountOfInternalLabelIsZero(t *testing.T) {
	ctx := context.Background()
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })

			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor177count"))
			internalCountFixture(t, ctx, exec)

			for _, tc := range []struct {
				query string
				rows  [][]interface{}
			}{
				{"MATCH (n:_internal) RETURN n.id AS id", [][]interface{}{}},
				{"MATCH (n:_internal) RETURN count(n) AS c", [][]interface{}{{int64(0)}}},
				{"MATCH (n:_internal) RETURN count(*) AS c", [][]interface{}{{int64(0)}}},
				{"MATCH (a:_internal)-[r:NOR177_LINKED]->() RETURN r.w AS w", [][]interface{}{}},
				{"MATCH (a:_internal)-[r:NOR177_LINKED]->() RETURN count(r) AS c", [][]interface{}{{int64(0)}}},
				{"MATCH (a:_internal)-[r:NOR177_LINKED]->() RETURN count(*) AS c", [][]interface{}{{int64(0)}}},
				{"MATCH ()-[r:NOR177_LINKED]->(b:_internal) RETURN count(r) AS c", [][]interface{}{{int64(0)}}},
			} {
				t.Run(tc.query, func(t *testing.T) {
					result, err := exec.Execute(ctx, tc.query, nil)
					require.NoError(t, err, tc.query)
					require.Equal(t, tc.rows, result.Rows, tc.query)
				})
			}
		})
	}
}

// seedInternalCountPopulation writes the same population as
// internalCountFixture without a statement: the system database refuses graph
// statements, so its privileged side is observed through the store.
func seedInternalCountPopulation(t *testing.T, ctx context.Context, store storage.Engine) {
	t.Helper()
	nodes := []*storage.Node{
		{ID: "n1", Labels: []string{"Nor177Count", "_internal"}, Properties: map[string]interface{}{"id": int64(1)}},
		{ID: "n2", Labels: []string{"Nor177Count"}, Properties: map[string]interface{}{"id": int64(2)}},
		{ID: "n3", Labels: []string{"Nor177Count"}, Properties: map[string]interface{}{"id": int64(3)}},
		{ID: "n5", Labels: []string{"Nor177Count", "_internal"}, Properties: map[string]interface{}{"id": int64(5)}},
		{ID: "n6", Labels: []string{"Nor177Count"}, Properties: map[string]interface{}{"id": int64(6)}},
	}
	for _, node := range nodes {
		_, err := store.CreateNode(ctx, node)
		require.NoError(t, err, node.ID)
	}
	for _, edge := range []*storage.Edge{
		{ID: "e1", Type: "NOR177_LINKED", StartNode: "n1", EndNode: "n2", Properties: map[string]interface{}{"w": int64(1)}},
		{ID: "e2", Type: "NOR177_LINKED", StartNode: "n2", EndNode: "n3", Properties: map[string]interface{}{"w": int64(2)}},
		{ID: "e3", Type: "NOR177_LINKED", StartNode: "n5", EndNode: "n6", Properties: map[string]interface{}{"w": int64(3)}},
	} {
		require.NoError(t, store.CreateEdge(ctx, edge), edge.ID)
	}
}

// TestCountStoreKeepsSystemNamespacePrivileged pins the other half of the
// visibility decision: the system database hides nothing, so its counters
// count what it reads, internal labels included. The same count owners answer
// 0 for the internal-label shapes every MATCH row hides, and they decline a
// user read whose scope holds an internal-label node, because the counters
// count the labels and relationships of a node the read hides: that read's
// count is left to its rows.
func TestCountStoreKeepsSystemNamespacePrivileged(t *testing.T) {
	ctx := context.Background()
	systemStore := storage.NewNamespacedEngine(newTestMemoryEngine(t), "system")
	seedInternalCountPopulation(t, ctx, systemStore)
	system := NewStorageExecutor(systemStore)
	userStore := storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor177count")
	seedInternalCountPopulation(t, ctx, userStore)
	user := NewStorageExecutor(userStore)

	internal := nodePatternInfo{variable: "n", labels: []string{"_internal"}}
	count, ok, err := system.nodeCountFromStore(ctx, internal, returnItem{expr: "count(n)"})
	require.NoError(t, err)
	require.True(t, ok, "the system database reads the internal nodes the counters count")
	require.Equal(t, int64(2), count)

	count, ok, err = user.nodeCountFromStore(ctx, internal, returnItem{expr: "count(n)"})
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int64(0), count, "a user database reads no internal node")

	internalStart := user.parseTraversalPattern(ctx, "(:_internal)-[r:NOR177_LINKED]->()")
	count, ok, err = user.relationshipCountFromStore(ctx, internalStart, returnItem{expr: "count(r)"})
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int64(0), count, "a relationship row needs a visible endpoint")

	visibleStart := user.parseTraversalPattern(ctx, "(:Nor177Count)-[r:NOR177_LINKED]->()")
	count, ok, err = system.relationshipCountFromStore(ctx, visibleStart, returnItem{expr: "count(r)"})
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int64(3), count, "the system database counts every stored relationship")
	_, ok, err = user.relationshipCountFromStore(ctx, visibleStart, returnItem{expr: "count(r)"})
	require.NoError(t, err)
	require.False(t, ok, "the counters count the relationships of the hidden endpoints too")
	result, err := user.Execute(ctx, "MATCH (:Nor177Count)-[r:NOR177_LINKED]->() RETURN count(r) AS v", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows, "only the relationship between two visible nodes is a row")
}
