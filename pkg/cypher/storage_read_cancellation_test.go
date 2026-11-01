package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type d09CancelAtAdjacency struct {
	storage.Engine
	cancel          context.CancelFunc
	afterCancelRows int
	calls           int
	cancelLabel     bool
}

func (e *d09CancelAtAdjacency) GetOutgoingEdges(ctx context.Context, id storage.NodeID) ([]*storage.Edge, error) {
	e.calls++
	if e.cancel != nil && !e.cancelLabel {
		e.cancel()
	}
	edges, err := e.Engine.GetOutgoingEdges(ctx, id)
	if e.cancel != nil && !e.cancelLabel {
		e.afterCancelRows += len(edges)
	}
	return edges, err
}

func (e *d09CancelAtAdjacency) GetNodesByLabel(ctx context.Context, label string) ([]*storage.Node, error) {
	if e.cancel != nil && e.cancelLabel {
		e.calls++
		e.cancel()
	}
	nodes, err := e.Engine.GetNodesByLabel(ctx, label)
	if e.cancel != nil && e.cancelLabel {
		e.afterCancelRows += len(nodes)
	}
	return nodes, err
}

func TestStorageBackedPublicReadCancellation(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		base := storage.NewNamespacedEngine(newTestMemoryEngine(t), "d09")
		_, err := base.CreateNode(&storage.Node{ID: "hub", Labels: []string{"D09Hub"}})
		require.NoError(t, err)
		const size = 1024
		for i := 0; i < size; i++ {
			id := storage.NodeID(fmt.Sprintf("n%d", i))
			_, err = base.CreateNode(&storage.Node{ID: id, Labels: []string{"D09Leaf"}})
			require.NoError(t, err)
			require.NoError(t, base.CreateEdge(&storage.Edge{ID: storage.EdgeID(fmt.Sprintf("r%d", i)), StartNode: "hub", EndNode: id, Type: "R"}))
		}
		for _, label := range []bool{false, true} {
			t.Run(fmt.Sprintf("label=%v", label), func(t *testing.T) {
				boundary := &d09CancelAtAdjacency{Engine: base, cancelLabel: label}
				exec := NewStorageExecutor(boundary, QueryCachePolicy{})
				query := "MATCH (h:D09Hub) MATCH p=(h)-[:R*1..1]->(n) RETURN n"
				if label {
					query = "MATCH (n:D09Leaf) RETURN n"
				}
				result, err := exec.Execute(context.Background(), query, nil)
				require.NoError(t, err)
				require.Len(t, result.Rows, size)
				boundary.calls = 0
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				boundary.cancel = cancel
				result, err = exec.Execute(ctx, query, nil)
				require.ErrorIs(t, ctx.Err(), context.Canceled, "fixture must cancel actual caller at storage boundary")
				require.ErrorIs(t, err, context.Canceled)
				require.Nil(t, result)
				require.Positive(t, boundary.calls, "caller must reach actual storage owner")
				require.Zero(t, boundary.afterCancelRows, "native storage must not materialize the whole population after caller cancellation")
			})
		}
	})
}
