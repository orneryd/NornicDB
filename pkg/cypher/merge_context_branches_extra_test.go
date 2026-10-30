package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestExecuteMergeWithContext_OnCreateOnMatchAndContextProps(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "merge_ctx_branches")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	source := &storage.Node{ID: storage.NodeID("src"), Labels: []string{"Source"}, Properties: map[string]interface{}{"name": "source-name"}}
	_, err := store.CreateNode(source)
	require.NoError(t, err)

	q := "MATCH (s:Source) MERGE (n:Doc {k: s.name}) ON CREATE SET n.created = true ON MATCH SET n.seen = true RETURN n.k AS k, n.created AS created, n.seen AS seen"
	res, err := exec.Execute(ctx, q, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"k", "created", "seen"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "source-name", res.Rows[0][0])
	require.Equal(t, true, res.Rows[0][1])
	require.Nil(t, res.Rows[0][2])
	require.EqualValues(t, 1, res.Stats.NodesCreated)

	res, err = exec.Execute(ctx, q, nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "source-name", res.Rows[0][0])
	require.Equal(t, true, res.Rows[0][1])
	require.Equal(t, true, res.Rows[0][2])
	require.EqualValues(t, 0, res.Stats.NodesCreated)
}

func TestExecuteMergeWithContext_StandaloneSetAndRelationshipMerge(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "merge_ctx_chain")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	res, err := exec.Execute(ctx, "MERGE (a:Person {id:'a'}) SET a.name = 'Alice' RETURN a.name AS name", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"name"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "Alice", res.Rows[0][0])

	b := &storage.Node{ID: storage.NodeID("b"), Labels: []string{"Person"}, Properties: map[string]interface{}{"id": "b"}}
	_, err = store.CreateNode(b)
	require.NoError(t, err)

	res, err = exec.Execute(ctx, "MATCH (a:Person {id: 'a'}), (b:Person {id: 'b'}) MERGE (a)-[:KNOWS]->(b) RETURN count(*) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"c"}, res.Columns)
	require.Len(t, res.Rows, 1)

	verify, err := exec.Execute(ctx, "MATCH (a:Person {id:'a'})-[:KNOWS]->(b:Person {id:'b'}) RETURN count(*)", nil)
	require.NoError(t, err)
	require.Len(t, verify.Rows, 1)
	require.EqualValues(t, 1, verify.Rows[0][0])
}

func findNodeByProp(t *testing.T, store storage.Engine, label, prop string, value interface{}) *storage.Node {
	t.Helper()
	nodes, err := store.GetNodesByLabel(label)
	require.NoError(t, err)
	for _, n := range nodes {
		if n != nil && n.Properties[prop] == value {
			return n
		}
	}
	t.Fatalf("node with %s.%s=%v not found", label, prop, value)
	return nil
}

func TestCompoundMatchMergeWithWindowPreservesBindings(t *testing.T) {
	for _, testCase := range []struct {
		name    string
		window  string
		ids     []int64
		wantErr bool
	}{
		{name: "bounded", window: " SKIP 1 LIMIT 2", ids: []int64{3}},
		{name: "zero", window: " LIMIT 0"},
		{name: "past_end", window: " SKIP 5 LIMIT 1"},
		{name: "no_window", ids: []int64{1, 3}},
		{name: "invalid_limit", window: " LIMIT -1", wantErr: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "merge_window")
			exec := NewStorageExecutor(store)
			ctx := context.Background()
			_, err := exec.Execute(ctx, "CREATE (:WindowSource {id:1})-[:WINDOW_EDGE]->(:WindowEnd {id:1}), (:WindowSource)-[:WINDOW_EDGE]->(:WindowEnd {id:2}), (:WindowSource {id:3})-[:WINDOW_EDGE]->(:WindowEnd {id:3})", nil)
			require.NoError(t, err)

			query := "MATCH (n:WindowSource)-[r:WINDOW_EDGE]->(m:WindowEnd) WHERE n.id IS NOT NULL WITH n, r, m ORDER BY n.id" + testCase.window + " MERGE (target:WindowTarget {id:n.id}) RETURN n.id AS id, type(r) AS relationship, m.id AS endpoint ORDER BY id"
			result, err := exec.Execute(ctx, query, nil)
			if testCase.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, []string{"id", "relationship", "endpoint"}, result.Columns)
				require.Len(t, result.Rows, len(testCase.ids))
				require.EqualValues(t, len(testCase.ids), result.Stats.NodesCreated)
				for index, id := range testCase.ids {
					require.Equal(t, []interface{}{id, "WINDOW_EDGE", id}, result.Rows[index])
				}
			}
			targets, err := store.GetNodesByLabel("WindowTarget")
			require.NoError(t, err)
			require.Len(t, targets, len(testCase.ids))
			for _, id := range testCase.ids {
				require.EqualValues(t, id, findNodeByProp(t, store, "WindowTarget", "id", id).Properties["id"])
			}
		})
	}
}
