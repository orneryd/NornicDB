package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type cancellationTraversalEngine struct {
	storage.Engine
	nodes           map[storage.NodeID]*storage.Node
	edges           []*storage.Edge
	reads, cancelAt int
	cancel          context.CancelFunc
}

func (s *cancellationTraversalEngine) GetNode(ctx context.Context, id storage.NodeID) (*storage.Node, error) {
	return s.nodes[id], nil
}

// adjacent reads one node's relationships for the traversal's direction: it
// counts the read, cancels at the configured read, then applies that
// direction's first-edge endpoint test to the root's fan-out.
func (s *cancellationTraversalEngine) adjacent(id storage.NodeID, incoming bool) ([]*storage.Edge, error) {
	s.reads++
	if s.reads == s.cancelAt {
		s.cancel()
	}
	if id != "root" {
		return nil, nil
	}
	if len(s.edges) > 0 {
		endpoint := s.edges[0].StartNode
		if incoming {
			endpoint = s.edges[0].EndNode
		}
		if endpoint != id {
			return nil, nil
		}
	}
	return s.edges, nil
}

func (s *cancellationTraversalEngine) GetOutgoingEdges(ctx context.Context, id storage.NodeID) ([]*storage.Edge, error) {
	return s.adjacent(id, false)
}

func (s *cancellationTraversalEngine) GetIncomingEdges(ctx context.Context, id storage.NodeID) ([]*storage.Edge, error) {
	return s.adjacent(id, true)
}

// A high-fanout DFS must stop sibling expansion after cancellation. Counting
// storage reads avoids a machine-dependent timeout and catches recursive calls
// that return only at every 256th probe while their callers keep expanding.
func TestReleasePlainTraversalCancellation(t *testing.T) {
	for _, direction := range []string{"outgoing", "incoming", "both"} {
		for _, at := range []int{0, 1, 2, -1} {
			t.Run(fmt.Sprintf("%s/cancelAt%d", direction, at), func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				root := &storage.Node{ID: "root"}
				engine := &cancellationTraversalEngine{Engine: newTestMemoryEngine(t), nodes: map[storage.NodeID]*storage.Node{"root": root}, cancel: cancel, cancelAt: at}
				for i := 0; i < 1024; i++ {
					id := storage.NodeID(fmt.Sprintf("n%d", i))
					engine.nodes[id] = &storage.Node{ID: id}
					edge := &storage.Edge{ID: storage.EdgeID(fmt.Sprintf("e%d", i)), StartNode: "root", EndNode: id, Type: "R"}
					if direction == "incoming" {
						edge.StartNode, edge.EndNode = id, "root"
					}
					engine.edges = append(engine.edges, edge)
				}
				if at == 0 {
					cancel()
				}
				exec := NewStorageExecutor(engine)
				state := exec.newTraversalContext(ctx, root, &RelationshipPattern{Direction: direction, MinHops: 1, MaxHops: 3})
				paths := exec.findPaths(state, root, []*storage.Node{root}, nil, 0, nil)
				if at < 0 {
					require.Equal(t, 1024, len(paths))
					require.NoError(t, ctx.Err())
					return
				}
				require.ErrorIs(t, ctx.Err(), context.Canceled)
				// Undirected adjacency reads each direction, so one extra read can finish
				// inside the same in-flight adjacency operation; no sibling may expand.
				allowance := at
				if direction == "both" && at > 0 {
					allowance++
				}
				require.LessOrEqual(t, engine.reads, allowance, "cancelled DFS continued reading sibling adjacency")
				require.LessOrEqual(t, len(paths), 1, "cancelled DFS kept materializing sibling paths")
				for _, used := range state.usedEdges {
					require.False(t, used, "unwind must release traversal edge ownership")
				}
			})
		}
	}
}
