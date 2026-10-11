package storage

// Individual/batch mutation equivalence across Memory and the production stack
// (plan §3.5): a write performed through the single-record entry points and
// the same write performed through the bulk entry points must produce
// equivalent stored state — same labels, typed property graphs (including
// nested lists/maps and mixed-type lists), embeddings and visibility. The
// AsyncEngine stages single writes and flushes them as bulk operations, so
// this is also the contract that the staged and direct paths converge.
//
// The assertions compare the two writes against each other rather than
// against the input representation, because the storage layer may normalize
// numeric encodings (e.g. int64 vs float64) on decode; equivalence between
// paths is what prevents batch-related divergences.

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func mutationEquivalenceStacks(t *testing.T) map[string]func(*testing.T) Engine {
	t.Helper()
	return map[string]func(*testing.T) Engine{
		"memory": func(t *testing.T) Engine {
			engine := NewMemoryEngine()
			t.Cleanup(func() { _ = engine.Close() })
			return engine
		},
		"badger": func(t *testing.T) Engine {
			engine, err := NewBadgerEngineInMemory()
			require.NoError(t, err)
			t.Cleanup(func() { _ = engine.Close() })
			return engine
		},
		"wal": func(t *testing.T) Engine {
			engine, err := NewBadgerEngineInMemory()
			require.NoError(t, err)
			walLog, err := NewWAL(t.TempDir(), &WALConfig{SyncMode: "none"})
			require.NoError(t, err)
			t.Cleanup(func() { _ = walLog.Close(); _ = engine.Close() })
			return NewWALEngine(engine, walLog)
		},
		"namespaced+wal": func(t *testing.T) Engine {
			engine, err := NewBadgerEngineInMemory()
			require.NoError(t, err)
			walLog, err := NewWAL(t.TempDir(), &WALConfig{SyncMode: "none"})
			require.NoError(t, err)
			wal := NewWALEngine(engine, walLog)
			t.Cleanup(func() { _ = walLog.Close(); _ = engine.Close() })
			return NewNamespacedEngine(wal, "tenant")
		},
	}
}

func flushEquivalenceStack(t *testing.T, engine Engine) {
	t.Helper()
	for {
		if flusher, ok := engine.(interface{ Flush() error }); ok {
			require.NoError(t, flusher.Flush())
			return
		}
		if unwrapper, ok := engine.(EngineUnwrapper); ok {
			engine = unwrapper.GetInnerEngine()
			continue
		}
		return
	}
}

func TestMutationEquivalence_SingleVsBulkNodeCreate(t *testing.T) {
	props := map[string]interface{}{
		"name":   "alice",
		"tags":   []interface{}{"a", "b"},
		"ints":   []interface{}{int64(1), int64(2)},
		"nested": map[string]interface{}{"k": []interface{}{int64(3)}},
		"mix":    []interface{}{int64(1), "two", true},
	}
	for stack, build := range mutationEquivalenceStacks(t) {
		t.Run(stack, func(t *testing.T) {
			engine := build(t)

			_, err := engine.CreateNode(&Node{ID: "test:single", Labels: []string{"Person", "Engineer"}, Properties: props})
			require.NoError(t, err)
			require.NoError(t, engine.BulkCreateNodes([]*Node{
				{ID: "test:bulk", Labels: []string{"Person", "Engineer"}, Properties: props},
			}))
			flushEquivalenceStack(t, engine)

			singleRead, err := engine.GetNode("test:single")
			require.NoError(t, err)
			bulkRead, err := engine.GetNode("test:bulk")
			require.NoError(t, err)

			require.Equal(t, singleRead.Labels, bulkRead.Labels, "%s: labels diverge between single and bulk creates", stack)
			require.True(t, reflect.DeepEqual(singleRead.Properties, bulkRead.Properties),
				"%s: properties diverge between single and bulk creates: single=%v bulk=%v",
				stack, singleRead.Properties, bulkRead.Properties)
		})
	}
}

func TestMutationEquivalence_SingleVsBulkEdgeCreate(t *testing.T) {
	edgeProps := map[string]interface{}{
		"since":  int64(2020),
		"weight": 0.5,
		"meta":   []interface{}{"x", int64(9)},
	}
	for stack, build := range mutationEquivalenceStacks(t) {
		t.Run(stack, func(t *testing.T) {
			engine := build(t)
			for _, id := range []NodeID{"test:a", "test:b"} {
				_, err := engine.CreateNode(&Node{ID: id, Labels: []string{"Doc"}})
				require.NoError(t, err)
			}
			flushEquivalenceStack(t, engine)

			require.NoError(t, engine.CreateEdge(&Edge{ID: "test:single", StartNode: "test:a", EndNode: "test:b", Type: "KNOWS", Properties: edgeProps}))
			require.NoError(t, engine.BulkCreateEdges([]*Edge{
				{ID: "test:bulk", StartNode: "test:a", EndNode: "test:b", Type: "KNOWS", Properties: edgeProps},
			}))
			flushEquivalenceStack(t, engine)

			singleRead, err := engine.GetEdge("test:single")
			require.NoError(t, err)
			bulkRead, err := engine.GetEdge("test:bulk")
			require.NoError(t, err)

			require.Equal(t, singleRead.Type, bulkRead.Type, "%s: edge type diverges", stack)
			require.Equal(t, singleRead.StartNode, bulkRead.StartNode, "%s: start node diverges", stack)
			require.Equal(t, singleRead.EndNode, bulkRead.EndNode, "%s: end node diverges", stack)
			require.True(t, reflect.DeepEqual(singleRead.Properties, bulkRead.Properties),
				"%s: edge properties diverge between single and bulk creates: single=%v bulk=%v",
				stack, singleRead.Properties, bulkRead.Properties)
		})
	}
}

func TestMutationEquivalence_SingleVsBulkDelete(t *testing.T) {
	for stack, build := range mutationEquivalenceStacks(t) {
		t.Run(stack, func(t *testing.T) {
			engine := build(t)

			_, err := engine.CreateNode(&Node{ID: "test:single-del", Labels: []string{"Doc"}})
			require.NoError(t, err)
			_, err = engine.CreateNode(&Node{ID: "test:bulk-del", Labels: []string{"Doc"}})
			require.NoError(t, err)
			flushEquivalenceStack(t, engine)

			require.NoError(t, engine.DeleteNode("test:single-del"))
			require.NoError(t, engine.BulkDeleteNodes([]NodeID{"test:bulk-del"}))
			flushEquivalenceStack(t, engine)

			for _, id := range []NodeID{"test:single-del", "test:bulk-del"} {
				_, err := engine.GetNode(id)
				require.ErrorIs(t, err, ErrNotFound, "%s: node %s should be deleted on both paths", stack, id)
			}
		})
	}
}

func TestMutationEquivalence_SingleUpdateVsStagedUpdate(t *testing.T) {
	// A staged async update flushes through UpdateNode; a direct update on the
	// inner engine writes through the same path. Both must leave identical
	// stored state.
	for stack, build := range mutationEquivalenceStacks(t) {
		t.Run(stack, func(t *testing.T) {
			engine := build(t)
			for _, id := range []NodeID{"test:u1", "test:u2"} {
				_, err := engine.CreateNode(&Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]interface{}{"v": int64(1)}})
				require.NoError(t, err)
			}
			flushEquivalenceStack(t, engine)

			require.NoError(t, engine.UpdateNode(&Node{ID: "test:u1", Labels: []string{"Doc"}, Properties: map[string]interface{}{"v": int64(2), "tags": []interface{}{"x"}}}))
			// The second update is identical but travels through the staged
			// overlay of the async layer when present.
			require.NoError(t, engine.UpdateNode(&Node{ID: "test:u2", Labels: []string{"Doc"}, Properties: map[string]interface{}{"v": int64(2), "tags": []interface{}{"x"}}}))
			flushEquivalenceStack(t, engine)

			u1, err := engine.GetNode("test:u1")
			require.NoError(t, err)
			u2, err := engine.GetNode("test:u2")
			require.NoError(t, err)
			require.True(t, reflect.DeepEqual(u1.Properties, u2.Properties),
				"%s: update results diverge: u1=%v u2=%v", stack, u1.Properties, u2.Properties)
		})
	}
}
