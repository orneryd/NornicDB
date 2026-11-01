package cypher

import (
	"context"
	"fmt"
	"testing"
	"time"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// gds.util.asNodes resolves every list member with its own point lookup. A
// canceled statement must end that iteration with the caller's error and no
// value: the result list is produced once.

// gdsCancellationNodes creates count nodes and their cypher ids in list order.
func gdsCancellationNodes(t *testing.T, count int) (storage.Engine, []interface{}) {
	t.Helper()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "gds_value_cancellation")
	ids := make([]interface{}, count)
	for index := 0; index < count; index++ {
		id, err := store.CreateNode(&storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("gds-cancel-%03d", index)),
			Properties: map[string]interface{}{"index": int64(index)},
		})
		require.NoError(t, err)
		ids[index] = string(id)
	}
	return store, ids
}

// gdsCancellationScope evaluates the list argument from the table.
func gdsCancellationScope(store storage.Engine, ids []interface{}) cypherfn.Context {
	return cypherfn.Context{
		Storage: store,
		Eval:    func(string) (interface{}, error) { return ids, nil },
	}
}

// gdsCancellationIndexes is the resolved nodes' property indexes in list
// order: gds.util.asNodes keeps the list's order.
func gdsCancellationIndexes(t *testing.T, value interface{}, count int) []int64 {
	t.Helper()
	nodes, ok := value.([]interface{})
	require.True(t, ok)
	require.Len(t, nodes, count)
	indexes := make([]int64, count)
	for position, node := range nodes {
		resolved, isNode := node.(*storage.Node)
		require.True(t, isNode, "list member %d", position)
		indexes[position] = resolved.Properties["index"].(int64)
	}
	return indexes
}

// gdsCancelingNodeEngine cancels the statement from the first node lookup, so
// the next iteration of gds.util.asNodes observes the caller's cancellation.
// Every other storage method stays the underlying engine's.
type gdsCancelingNodeEngine struct {
	storage.Engine
	cancel context.CancelFunc
	reads  int
}

func (e *gdsCancelingNodeEngine) GetNode(id storage.NodeID) (*storage.Node, error) {
	node, err := e.Engine.GetNode(id)
	e.reads++
	if e.reads == 1 {
		e.cancel()
	}
	return node, err
}

// TestGDSAsNodesStopsAtCanceledStatement: gds.util.asNodes returns the
// caller's cancellation or deadline error and no value, and the same call
// under the statement's healthy context returns every node in order.
func TestGDSAsNodesStopsAtCanceledStatement(t *testing.T) {
	const count = 32
	store, ids := gdsCancellationNodes(t, count)
	want := make([]int64, count)
	for index := range want {
		want[index] = int64(index)
	}

	t.Run("healthy statement", func(t *testing.T) {
		value, found, err := cypherfn.EvaluateFunction(context.Background(), "gds.util.asnodes", []string{"ids"}, gdsCancellationScope(store, ids))
		require.True(t, found)
		require.NoError(t, err)
		require.Equal(t, want, gdsCancellationIndexes(t, value, count))
	})

	t.Run("already canceled", func(t *testing.T) {
		workCtx, cancel := context.WithCancel(context.Background())
		cancel()
		value, found, err := cypherfn.EvaluateFunction(workCtx, "gds.util.asnodes", []string{"ids"}, gdsCancellationScope(store, ids))
		require.True(t, found)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, value)
	})

	t.Run("deadline already exceeded", func(t *testing.T) {
		workCtx, cancel := context.WithDeadline(context.Background(), time.Now())
		defer cancel()
		require.ErrorIs(t, workCtx.Err(), context.DeadlineExceeded)
		value, found, err := cypherfn.EvaluateFunction(workCtx, "gds.util.asnodes", []string{"ids"}, gdsCancellationScope(store, ids))
		require.True(t, found)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Nil(t, value)
	})

	t.Run("canceled inside the list iteration", func(t *testing.T) {
		workCtx, cancel := context.WithCancel(context.Background())
		defer cancel()
		canceling := &gdsCancelingNodeEngine{Engine: store, cancel: cancel}
		value, found, err := cypherfn.EvaluateFunction(workCtx, "gds.util.asnodes", []string{"ids"}, gdsCancellationScope(canceling, ids))
		require.True(t, found)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, value)
		require.Equal(t, 1, canceling.reads, "the iteration resolved a node before it observed the cancellation")
	})

	t.Run("healthy statement through the wrapping engine", func(t *testing.T) {
		wrapping := &gdsCancelingNodeEngine{Engine: store, cancel: func() {}}
		value, found, err := cypherfn.EvaluateFunction(context.Background(), "gds.util.asnodes", []string{"ids"}, gdsCancellationScope(wrapping, ids))
		require.True(t, found)
		require.NoError(t, err)
		require.Equal(t, want, gdsCancellationIndexes(t, value, count))
	})
}
