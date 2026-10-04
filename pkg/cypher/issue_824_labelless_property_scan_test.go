package cypher

// Coverage for #824: a property match without a label scans every node with
// only the pattern's properties decoded, and reads the whole node only for a
// match; the transaction view streams every node (it streamed none).

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestIssue824LabellessPropertyMatch(t *testing.T) {
	exec, ctx := newUnitExecutor(t)
	run := func(tx bool, query string) [][]interface{} {
		t.Helper()
		if tx {
			_, err := exec.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
		}
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		if tx {
			_, err = exec.Execute(ctx, "COMMIT", nil)
			require.NoError(t, err)
		}
		return result.Rows
	}
	run(false, "CREATE INDEX code_id FOR (n:Code) ON (n.id)")
	run(false, "CREATE (:Code {id: 'a', body: 'x'}), (:Code {id: 'b'}), (:Other {id: 'a'}), ({id: 'a', n: 1})")
	for _, tx := range []bool{false, true} {
		require.Equal(t, [][]interface{}{{int64(3)}}, run(tx, "MATCH (n {id: 'a'}) RETURN count(n)"))
		require.Equal(t, [][]interface{}{{"x"}}, run(tx, "MATCH (n {id: 'a', body: 'x'}) RETURN n.body"))
		require.Equal(t, [][]interface{}{{int64(1)}}, run(tx, "MATCH (n {id: 'a', n: 1}) RETURN n.n"))
		require.Empty(t, run(tx, "MATCH (n {id: 'none'}) RETURN n"))
	}
	run(true, "MATCH (a {id: 'b'}), (b {body: 'x'}) MERGE (a)-[:CALLS]->(b)")
	require.Equal(t, [][]interface{}{{int64(1)}}, run(false, "MATCH (:Code {id: 'b'})-[r:CALLS]->(:Code {id: 'a'}) RETURN count(r)"))
	// The transaction sees its own pending nodes.
	_, err := exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE ({id: 'pending'})", nil)
	require.NoError(t, err)
	result, err := exec.Execute(ctx, "MATCH (n {id: 'pending'}) RETURN count(n)", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
	_, err = exec.Execute(ctx, "ROLLBACK", nil)
	require.NoError(t, err)
}

// vanishingNodeEngine fails or loses the full read of scanned nodes.
type vanishingNodeEngine struct {
	*storage.NamespacedEngine
	getNodeErr error
}

func (v *vanishingNodeEngine) GetNode(id storage.NodeID) (*storage.Node, error) {
	return nil, v.getNodeErr
}

func TestLabellessPropertyMatchFullReadOutcomes(t *testing.T) {
	inner := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	_, err := inner.CreateNode(&storage.Node{ID: "n1", Properties: map[string]interface{}{"id": "a"}})
	require.NoError(t, err)

	gone := &vanishingNodeEngine{NamespacedEngine: inner, getNodeErr: storage.ErrNotFound}
	nodes, err := NewStorageExecutor(gone).collectNodesWithStreaming(context.Background(), nil, map[string]interface{}{"id": "a"}, "", "", -1)
	require.NoError(t, err)
	require.Empty(t, nodes, "a node deleted between the scan and its read is not a match")

	failure := errors.New("read failed")
	failing := &vanishingNodeEngine{NamespacedEngine: inner, getNodeErr: failure}
	_, err = NewStorageExecutor(failing).collectNodesWithStreaming(context.Background(), nil, map[string]interface{}{"id": "a"}, "", "", -1)
	require.ErrorIs(t, err, failure)
}
