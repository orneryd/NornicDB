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

	nodeCtx := map[string]*storage.Node{"s": source}
	relCtx := map[string]*storage.Edge{}

	q := "MERGE (n:Doc {k: s.name}) ON CREATE SET n.created = true ON MATCH SET n.seen = true RETURN n.k AS k, n.created AS created, n.seen AS seen"
	res, err := exec.executeMergeWithContext(ctx, q, nodeCtx, relCtx)
	require.NoError(t, err)
	require.Equal(t, []string{"k", "created", "seen"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "source-name", res.Rows[0][0])
	require.Equal(t, true, res.Rows[0][1])
	require.Nil(t, res.Rows[0][2])
	require.EqualValues(t, 1, res.Stats.NodesCreated)

	res, err = exec.executeMergeWithContext(ctx, q, nodeCtx, relCtx)
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

	res, err := exec.executeMergeWithContext(ctx,
		"MERGE (a:Person {id:'a'}) SET a.name = 'Alice' RETURN a.name AS name",
		map[string]*storage.Node{}, map[string]*storage.Edge{},
	)
	require.NoError(t, err)
	require.Equal(t, []string{"name"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "Alice", res.Rows[0][0])

	b := &storage.Node{ID: storage.NodeID("b"), Labels: []string{"Person"}, Properties: map[string]interface{}{"id": "b"}}
	_, err = store.CreateNode(b)
	require.NoError(t, err)

	res, err = exec.executeMergeWithContext(ctx,
		"MERGE (a)-[:KNOWS]->(b) RETURN count(*) AS c",
		map[string]*storage.Node{"a": findNodeByProp(t, store, "Person", "id", "a"), "b": b},
		map[string]*storage.Edge{},
	)
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

func TestMergeTrailingWindowAndApplyContextWindow(t *testing.T) {
	varName, skip, limit, ok := parseTrailingWithWindow("MATCH (n:Person) WITH n SKIP 1 LIMIT 2")
	require.True(t, ok)
	require.Equal(t, "n", varName)
	require.Equal(t, 1, skip)
	require.Equal(t, 2, limit)

	_, _, _, ok = parseTrailingWithWindow("MATCH (n:Person) WITH n LIMIT -1")
	require.False(t, ok)
	_, _, _, ok = parseTrailingWithWindow("MATCH (n:Person) WITH n")
	require.False(t, ok)

	n1 := &storage.Node{ID: storage.NodeID("n1")}
	n2 := &storage.Node{ID: storage.NodeID("n2")}
	n3 := &storage.Node{ID: storage.NodeID("n3")}
	contexts := []map[string]*storage.Node{
		{"n": n1},
		{"x": n2},
		{"n": n3},
	}
	e3 := &storage.Edge{ID: storage.EdgeID("e3")}
	relationships := []map[string]*storage.Edge{{}, {}, {"r": e3}}
	window, windowRelationships := applyContextWindow(contexts, relationships, "n", 1, 2)
	require.Len(t, window, 1)
	require.Equal(t, n3, window[0]["n"])
	// The relationship rows stay aligned with the node rows.
	require.Equal(t, []map[string]*storage.Edge{{"r": e3}}, windowRelationships)

	empty, emptyRelationships := applyContextWindow(contexts, relationships, "n", 0, 0)
	require.Nil(t, empty)
	require.Nil(t, emptyRelationships)
	past, pastRelationships := applyContextWindow(contexts, relationships, "n", 5, 1)
	require.Nil(t, past)
	require.Nil(t, pastRelationships)
}
