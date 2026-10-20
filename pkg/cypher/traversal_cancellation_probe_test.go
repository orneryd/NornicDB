package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type countingTraversalContext struct {
	context.Context
	probes int
}

func (ctx *countingTraversalContext) Err() error { ctx.probes++; return ctx.Context.Err() }

func TestLiveTraversalCancellationProbeWork(t *testing.T) {
	for _, count := range []int{1, 1024} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			base, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx := &countingTraversalContext{Context: base}
			root := &storage.Node{ID: "root"}
			engine := &cancellationTraversalEngine{Engine: newTestMemoryEngine(t), nodes: map[storage.NodeID]*storage.Node{"root": root}, cancel: cancel, cancelAt: -1}
			for i := 0; i < count; i++ {
				id := storage.NodeID(fmt.Sprintf("n%d", i))
				engine.nodes[id] = &storage.Node{ID: id}
				engine.edges = append(engine.edges, &storage.Edge{ID: storage.EdgeID(fmt.Sprintf("e%d", i)), StartNode: "root", EndNode: id, Type: "R"})
			}
			exec := NewStorageExecutor(engine)
			state := exec.newTraversalContext(ctx, root, &RelationshipPattern{Direction: "outgoing", MinHops: 1, MaxHops: 1})
			paths := exec.relationshipExecutionFor(nil, "", "").findPaths(state, root, []*storage.Node{root}, nil, 0, nil)
			require.Len(t, paths, count)
			require.LessOrEqual(t, ctx.probes, 2, "healthy traversal must not call Context.Err per frame/candidate")
		})
	}
}

func BenchmarkLiveTraversalCancellationProbe(b *testing.B) {
	base, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx := &countingTraversalContext{Context: base}
	root := &storage.Node{ID: "root"}
	engine := &cancellationTraversalEngine{Engine: newTestMemoryEngine(b), nodes: map[storage.NodeID]*storage.Node{"root": root}, cancel: cancel, cancelAt: -1}
	for i := 0; i < 1024; i++ {
		id := storage.NodeID(fmt.Sprintf("n%d", i))
		engine.nodes[id] = &storage.Node{ID: id}
		engine.edges = append(engine.edges, &storage.Edge{ID: storage.EdgeID(fmt.Sprintf("e%d", i)), StartNode: "root", EndNode: id, Type: "R"})
	}
	exec := NewStorageExecutor(engine)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		state := exec.newTraversalContext(ctx, root, &RelationshipPattern{Direction: "outgoing", MinHops: 1, MaxHops: 1})
		paths := exec.relationshipExecutionFor(nil, "", "").findPaths(state, root, []*storage.Node{root}, nil, 0, nil)
		if len(paths) != 1024 {
			b.Fatal(len(paths))
		}
	}
}

func TestTraversalCancellationState(t *testing.T) {
	require.False(t, (&TraversalContext{}).traversalCancelled())
	background := &TraversalContext{cancelCtx: context.Background()}
	require.False(t, background.traversalCancelled())
	require.False(t, background.traversalCancelled())
	base, cancel := context.WithCancel(context.Background())
	live := &TraversalContext{cancelCtx: base}
	require.False(t, live.traversalCancelled())
	cancel()
	require.True(t, live.traversalCancelled())
	require.True(t, live.traversalCancelled(), "observed cancellation remains latched for ancestor unwind")
	initial, stop := context.WithCancel(context.Background())
	stop()
	require.True(t, (&TraversalContext{cancelCtx: initial}).traversalCancelled())
}
