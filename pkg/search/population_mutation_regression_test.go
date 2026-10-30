package search

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type pausedPopulationEngine struct {
	storage.Engine
	captured chan struct{}
	resume   chan struct{}
}

func (e *pausedPopulationEngine) IterateNodes(fn func(*storage.Node) bool) error {
	nodes, err := e.Engine.AllNodes()
	if err != nil {
		return err
	}
	close(e.captured)
	<-e.resume
	for _, n := range nodes {
		if !fn(n) {
			break
		}
	}
	return nil
}

func TestPopulationCannotReplaceLiveMutation(t *testing.T) {
	for _, remove := range []bool{false, true} {
		name := "update"
		if remove {
			name = "delete"
		}
		t.Run(name, func(t *testing.T) {
			base := newNamespacedEngine(t)
			node := &storage.Node{ID: "point", ChunkEmbeddings: [][]float32{{1, 0, 0, 0}}}
			_, err := base.CreateNode(node)
			require.NoError(t, err)
			engine := &pausedPopulationEngine{Engine: base, captured: make(chan struct{}), resume: make(chan struct{})}
			svc := NewService(engine, 4, nil)
			buildDone := make(chan error, 1)
			go func() { buildDone <- svc.BuildIndexes(context.Background()) }()
			<-engine.captured
			mutationDone := make(chan error, 1)
			if remove {
				require.NoError(t, base.DeleteNode(node.ID))
			} else {
				node = &storage.Node{ID: "point", ChunkEmbeddings: [][]float32{{0, 1, 0, 0}}}
				require.NoError(t, base.UpdateNode(storage.NodeUpdateRequest{After: node}))
			}
			go func() {
				if remove {
					mutationDone <- svc.RemoveNode(node.ID)
				} else {
					mutationDone <- svc.IndexNode(node)
				}
			}()
			// Let the pre-fix mutation complete against the paused, older population.
			select {
			case err := <-mutationDone:
				require.NoError(t, err)
				mutationDone <- nil
			case <-time.After(50 * time.Millisecond):
			}
			close(engine.resume)
			require.NoError(t, <-buildDone)
			require.NoError(t, <-mutationDone)
			hits, err := svc.VectorQueryNodes(context.Background(), []float32{0, 1, 0, 0}, VectorQuerySpec{Limit: 1, Similarity: "dot"})
			require.NoError(t, err)
			if remove {
				require.Empty(t, hits)
			} else {
				require.Len(t, hits, 1)
				require.InDelta(t, 1, hits[0].Score, 1e-6)
			}
		})
	}
}
