package cypher

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// heldIncidentReadEngine parks the traversal's adjacency read in read
// admission: it waits for the context the traversal actually handed to
// storage, or for the test to release it. A traversal that keeps the caller's
// context out of storage leaves only the release, so its read cannot end when
// the caller cancels.
type heldIncidentReadEngine struct {
	storage.Engine
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	reads   atomic.Int32
}

func (e *heldIncidentReadEngine) hold(ctx context.Context) error {
	e.reads.Add(1)
	e.once.Do(func() { close(e.entered) })
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-e.release:
		return nil
	}
}

func (e *heldIncidentReadEngine) GetOutgoingEdges(ctx context.Context, id storage.NodeID) ([]*storage.Edge, error) {
	if err := e.hold(ctx); err != nil {
		return nil, err
	}
	return e.Engine.GetOutgoingEdges(ctx, id)
}

func (e *heldIncidentReadEngine) GetIncomingEdges(ctx context.Context, id storage.NodeID) ([]*storage.Edge, error) {
	if err := e.hold(ctx); err != nil {
		return nil, err
	}
	return e.Engine.GetIncomingEdges(ctx, id)
}

// TestPublicUndirectedTraversalCancellation reproduces an ordinary public
// undirected MATCH whose storage read is parked in read admission, cancels the
// caller after that read is entered, and requires the read to end because the
// traversal passed the caller's context. It covers both public undirected
// routes: variable-length paths (findPaths) and the single-hop label-scan plan
// (traversalScanPlan.visitEdges). Both reach storage through the incident
// helpers; neither may substitute Background for the caller's context.
func TestPublicUndirectedTraversalCancellation(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		base := storage.NewNamespacedEngine(newTestMemoryEngine(t), "undirected_context")
		_, err := base.CreateNode(t.Context(), &storage.Node{ID: "hub", Labels: []string{"UndirectedHub"}})
		require.NoError(t, err)
		const leaves = 64
		for i := 0; i < leaves; i++ {
			id := storage.NodeID(fmt.Sprintf("leaf%d", i))
			_, err = base.CreateNode(t.Context(), &storage.Node{ID: id, Labels: []string{"UndirectedLeaf"}})
			require.NoError(t, err)
			require.NoError(t, base.CreateEdge(t.Context(), &storage.Edge{ID: storage.EdgeID(fmt.Sprintf("r%d", i)), StartNode: "hub", EndNode: id, Type: "R"}))
		}

		for _, route := range []struct {
			name  string
			query string
		}{
			{name: "variable-length", query: "MATCH p=(h:UndirectedHub)-[:R*1..2]-(n) RETURN n"},
			{name: "single-hop", query: "MATCH (h:UndirectedHub)-[r:R]-(n) RETURN n"},
		} {
			t.Run(route.name, func(t *testing.T) {
				held := &heldIncidentReadEngine{Engine: base, entered: make(chan struct{}), release: make(chan struct{})}
				var releaseOnce sync.Once
				release := func() { releaseOnce.Do(func() { close(held.release) }) }
				exec := NewStorageExecutor(held, QueryCachePolicy{})

				ctx, cancel := context.WithCancel(context.Background())
				type outcome struct {
					rows int
					err  error
				}
				done := make(chan outcome, 1)
				// The probe's goroutine closes joined when it exits. One cleanup
				// owns cancel, release and join on every exit path, including
				// the entry timeout below, so no goroutine outlives the test.
				joined := make(chan struct{})
				go func() {
					defer close(joined)
					result, err := exec.Execute(ctx, route.query, nil)
					if err != nil {
						done <- outcome{err: err}
						return
					}
					done <- outcome{rows: len(result.Rows)}
				}()
				t.Cleanup(func() {
					cancel()
					release()
					select {
					case <-joined:
					case <-time.After(10 * time.Second):
						t.Error("the public query goroutine did not join after its storage read was released")
					}
				})

				select {
				case <-held.entered:
				case <-time.After(10 * time.Second):
					t.Fatal("the public query never entered the undirected storage read")
				}
				// Cancel only after the read is inside read admission, so the
				// read's own wait decides whether it observes the caller.
				cancel()
				out, finished := outcome{}, false
				select {
				case out = <-done:
					finished = true
				case <-time.After(100 * time.Millisecond):
				}
				release()
				if !finished {
					select {
					case out = <-done:
						finished = true
					case <-time.After(10 * time.Second):
						t.Fatal("the public query did not finish after the storage read was released")
					}
				}
				<-joined
				require.True(t, finished, "the undirected storage read ignored the caller's cancellation: it stayed blocked past 100ms and ended only when the test released read admission")
				require.ErrorIs(t, out.err, context.Canceled)
				require.Positive(t, held.reads.Load(), "the query must reach the undirected storage read")

				// The same executor and store stay healthy for a normal caller.
				result, err := exec.Execute(context.Background(), route.query, nil)
				require.NoError(t, err)
				require.Len(t, result.Rows, leaves)
			})
		}
	})
}
