package cypher

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Delay only relay callback delivery, while the real parent Done/Err are canceled.
// Hiding Value prevents context from unwrapping the test wrapper to its cancelCtx.
type delayedTraversalRelay struct{ context.Context }

func (c delayedTraversalRelay) Value(any) any                { return nil }
func (c delayedTraversalRelay) AfterFunc(func()) func() bool { return func() bool { return true } }

func TestTraversalObservesParentBeforeStatementRelay(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	statement := &statementContext{parent: delayedTraversalRelay{parent}}
	defer statement.cancel(context.Canceled)
	state := &TraversalContext{cancelCtx: statement}
	require.False(t, state.traversalCancelled())
	cancel()
	select {
	case <-statement.Done():
		t.Fatal("test requires pending relay")
	default:
	}
	require.True(t, state.traversalCancelled(), "already-canceled parent must not wait for callback delivery")
}

func TestCancelledVariableLengthStatementReturnsError(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		root := &storage.Node{ID: "root", Labels: []string{"CancelRoot"}}
		engine := &cancellationTraversalEngine{Engine: storage.NewNamespacedEngine(newTestMemoryEngine(t), "cancel"), nodes: map[storage.NodeID]*storage.Node{"root": root}, cancel: cancel, cancelAt: 3}
		_, err := engine.Engine.CreateNode(root)
		require.NoError(t, err)
		for i := 0; i < 64; i++ {
			id := storage.NodeID(fmt.Sprintf("n%d", i))
			engine.nodes[id] = &storage.Node{ID: id}
			engine.edges = append(engine.edges, &storage.Edge{ID: storage.EdgeID(fmt.Sprintf("e%d", i)), StartNode: "root", EndNode: id, Type: "R"})
		}
		e := NewStorageExecutor(engine)
		_, err = e.Execute(ctx, "MATCH (s:CancelRoot) MATCH p=(s)-[:R*1..3]->(n) RETURN p", nil)
		require.GreaterOrEqual(t, engine.reads, 3, "fixture must reach cancellation during traversal")
		require.ErrorIs(t, err, context.Canceled, "canceled traversal must not return a successful partial result")
	})
}

func TestTraversalParentRelayHasBoundedWork(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		t.Run(fmt.Sprint(deadline), func(t *testing.T) {
			root := &storage.Node{ID: "root"}
			engine := &cancellationTraversalEngine{Engine: newTestMemoryEngine(t), nodes: map[storage.NodeID]*storage.Node{"root": root}, cancelAt: 2}
			for i := 0; i < 1024; i++ {
				id := storage.NodeID(fmt.Sprintf("n%d", i))
				engine.nodes[id] = &storage.Node{ID: id}
				engine.edges = append(engine.edges, &storage.Edge{ID: storage.EdgeID(fmt.Sprintf("e%d", i)), StartNode: "root", EndNode: id, Type: "R"})
			}
			parent, cancel := context.WithCancel(context.Background())
			want := error(context.Canceled)
			if deadline {
				cancel()
				parent, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
				want = context.DeadlineExceeded
				engine.cancel = func() { <-parent.Done() }
			} else {
				engine.cancel = cancel
			}
			defer cancel()
			statement := &statementContext{parent: delayedTraversalRelay{parent}}
			defer statement.cancel(context.Canceled)
			e := NewStorageExecutor(engine)
			state := e.newTraversalContext(statement, root, &RelationshipPattern{Direction: "outgoing", MinHops: 1, MaxHops: 3})
			paths := e.findPaths(state, root, []*storage.Node{root}, nil, 0, nil)
			require.ErrorIs(t, parent.Err(), want)
			require.LessOrEqual(t, engine.reads, 3, "canceled parent must bound work before its relay callback")
			require.LessOrEqual(t, len(paths), 1)
			for _, used := range state.usedEdges {
				require.False(t, used)
			}
		})
	}
}
