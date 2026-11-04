package cypher

// Coverage for #824: a property match without a label scans every node, decodes
// the properties the row and the match need, and keeps the node the scan
// supplied as the row's node; the transaction view streams every node (it
// streamed none).

import (
	"context"
	"errors"
	"sync/atomic"
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

// vanishingNodeEngine fails the full read of a node and counts the point reads
// a statement performs: a property match supplies its row from the scan.
type vanishingNodeEngine struct {
	*storage.NamespacedEngine
	getNodeErr error
	reads      atomic.Int64
}

func (v *vanishingNodeEngine) GetNode(ctx context.Context, id storage.NodeID) (*storage.Node, error) {
	v.reads.Add(1)
	return nil, v.getNodeErr
}

// A label-less property match keeps the node the scan supplied. It reads no
// second copy, so a store whose point read would lose or fail the node still
// returns the scanned match, and a failing point read is never performed.
func TestLabellessPropertyMatchFullReadOutcomes(t *testing.T) {
	inner := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	_, err := inner.CreateNode(t.Context(), &storage.Node{ID: "n1", Properties: map[string]interface{}{"id": "a", "body": "x"}})
	require.NoError(t, err)

	gone := &vanishingNodeEngine{NamespacedEngine: inner, getNodeErr: storage.ErrNotFound}
	nodes, err := NewStorageExecutor(gone).collectNodesWithStreamingProjection(
		context.Background(), nil, map[string]interface{}{"id": "a"}, "", "", -1, nil,
	)
	require.NoError(t, err)
	require.Len(t, nodes, 1, "the node the scan supplied is the match")
	require.Equal(t, storage.NodeID("n1"), nodes[0].ID)
	require.Equal(t, map[string]interface{}{"id": "a", "body": "x"}, nodes[0].Properties)
	require.Zero(t, gone.reads.Load(), "the match costs no second read")

	failure := errors.New("read failed")
	failing := &vanishingNodeEngine{NamespacedEngine: inner, getNodeErr: failure}
	nodes, err = NewStorageExecutor(failing).collectNodesWithStreamingProjection(
		context.Background(), nil, map[string]interface{}{"id": "a"}, "", "", -1, nil,
	)
	require.NoError(t, err, "the failing point read is never performed")
	require.Len(t, nodes, 1)
	require.Zero(t, failing.reads.Load())
}
