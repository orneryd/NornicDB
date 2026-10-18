package search

import (
	"context"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// edgeStreamGateEngine reads a snapshot of the stored edges for the explicit
// build's relationship stream, then parks before applying it: a snapshot
// iterator a live edge write could overtake.
type edgeStreamGateEngine struct {
	*storage.MemoryEngine
	entered, release chan struct{}
	once             sync.Once
}

func (e *edgeStreamGateEngine) StreamEdges(ctx context.Context, fn func(edge *storage.Edge) error) error {
	var snapshot []*storage.Edge
	if err := e.MemoryEngine.StreamEdges(ctx, func(edge *storage.Edge) error {
		copied := *edge
		copied.Properties = make(map[string]any, len(edge.Properties))
		for k, v := range edge.Properties {
			copied.Properties[k] = v
		}
		snapshot = append(snapshot, &copied)
		return nil
	}); err != nil {
		return err
	}
	if edgeStreamCallerOnStack("buildRelationshipVectorIndexes") {
		parked := false
		e.once.Do(func() { parked = true; close(e.entered) })
		if parked {
			<-e.release
		}
	}
	for _, edge := range snapshot {
		if err := fn(edge); err != nil {
			return err
		}
	}
	return nil
}

func edgeStreamCallerOnStack(name string) bool {
	pcs := make([]uintptr, 64)
	frames := runtime.CallersFrames(pcs[:runtime.Callers(2, pcs)])
	for {
		frame, more := frames.Next()
		if strings.Contains(frame.Function, name) {
			return true
		}
		if !more {
			return false
		}
	}
}

// TestBuildRelationshipStreamDoesNotOvertakeLiveEdgeWrites: the explicit
// build owns buildMu, and a live edge write made while it owns buildMu is
// handed to it, like node writes. An update or removal made while the build's
// relationship stream runs returns at once, and the build applies it after
// the stream's copy, so it wins (no lost update, no resurrection).
func TestBuildRelationshipStreamDoesNotOvertakeLiveEdgeWrites(t *testing.T) {
	ctx := context.Background()
	for _, removal := range []bool{false, true} {
		t.Run(map[bool]string{false: "update_not_lost", true: "removal_not_resurrected"}[removal], func(t *testing.T) {
			engine := &edgeStreamGateEngine{MemoryEngine: storage.NewMemoryEngine(), entered: make(chan struct{}), release: make(chan struct{})}
			release := sync.OnceFunc(func() { close(engine.release) })
			t.Cleanup(func() { release(); _ = engine.MemoryEngine.Close() })
			for _, id := range []storage.NodeID{"nornic:s", "nornic:e"} {
				_, err := engine.CreateNode(&storage.Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"text": "node"}})
				require.NoError(t, err)
			}
			edge := &storage.Edge{ID: "nornic:x", StartNode: "nornic:s", EndNode: "nornic:e", Type: "REL", Properties: map[string]any{"v": []float32{1, 0, 0}}}
			require.NoError(t, engine.CreateEdge(edge))
			svc := NewServiceWithDimensions(engine, 3)
			t.Cleanup(func() { release(); _ = svc.Close() })
			// Fixed dimensions: with open ones the stream's stored edge chooses
			// them first, so a live edge vector waits for it
			// (TestBuildStoredVectorChoosesTheDimensionBeforeQueuedVectors).
			svc.FixVectorDimensions()

			built := make(chan error, 1)
			go func() { built <- svc.BuildIndexes(ctx) }()
			select {
			case <-engine.entered:
			case err := <-built:
				t.Fatalf("the build finished without streaming relationships: %v", err)
			case <-time.After(time.Minute):
				t.Fatal("the build's relationship stream did not start")
			}

			live := make(chan error, 1)
			if removal {
				require.NoError(t, engine.DeleteEdge(edge.ID))
				go func() { live <- svc.RemoveEdge(edge.ID) }()
			} else {
				updated := &storage.Edge{ID: edge.ID, StartNode: edge.StartNode, EndNode: edge.EndNode, Type: edge.Type, Properties: map[string]any{"v": []float32{0, 1, 0}}}
				require.NoError(t, engine.UpdateEdge(updated))
				go func() { live <- svc.IndexEdge(updated) }()
			}
			time.Sleep(200 * time.Millisecond)
			finishedDuringStream := len(live) > 0
			release()
			require.NoError(t, <-built)
			select {
			case err := <-live:
				require.NoError(t, err)
			case <-time.After(30 * time.Second):
				t.Fatal("the live edge write did not finish after the build")
			}

			svc.mu.RLock()
			got, present := svc.edgePropVector[string(edge.ID)]
			svc.mu.RUnlock()
			if removal {
				require.False(t, present, "a removed edge was resurrected by the build's stream")
			} else {
				require.True(t, present)
				require.Equal(t, []float32{0, 1, 0}, got["v"], "the live edge update was lost to the build's stream")
			}
			require.True(t, finishedDuringStream, "a live edge write is handed to the explicit build and returns at once, like node writes")
		})
	}
}
