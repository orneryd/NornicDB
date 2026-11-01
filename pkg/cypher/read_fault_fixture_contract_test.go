package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A configured error must preempt even binding methods on a nil backend.
func TestDirectionalReadFaultPreemptsNilBackend(t *testing.T) {
	fault := errors.New("selected directional failure")
	for _, incoming := range []bool{false, true} {
		engine := &directionalReadFaultEngine{outgoingErr: fault, incomingErr: fault}
		var rows []*storage.Edge
		var err error
		if incoming {
			rows, err = engine.GetIncomingEdges(t.Context(), "node")
		} else {
			rows, err = engine.GetOutgoingEdges(t.Context(), "node")
		}
		require.Nil(t, rows)
		require.Same(t, fault, err)
	}
}

// A selected first-call nil result is a substitution, not a missing fault.
func TestSingleShotLookupPreservesSelectedNilResult(t *testing.T) {
	for _, operation := range []string{"node", "outgoing", "incoming"} {
		t.Run(operation, func(t *testing.T) {
			store := newTestMemoryEngine(t)
			for _, id := range []storage.NodeID{"nornic:a", "nornic:b"} {
				_, err := store.CreateNode(&storage.Node{ID: id, Labels: []string{"Source"}})
				require.NoError(t, err)
			}
			require.NoError(t, store.CreateEdge(&storage.Edge{ID: "nornic:e", StartNode: "nornic:a", EndNode: "nornic:b", Type: "R"}))
			engine := &subqueryLookupFailureEngine{Engine: store, operation: operation}
			read := func() (int, error) {
				if operation == "node" {
					rows, err := engine.GetNodesByLabel(context.Background(), "Source")
					return len(rows), err
				}
				if operation == "outgoing" {
					rows, err := engine.GetOutgoingEdges(context.Background(), "nornic:a")
					return len(rows), err
				}
				rows, err := engine.GetIncomingEdges(context.Background(), "nornic:b")
				return len(rows), err
			}
			first, err := read()
			require.NoError(t, err)
			require.Zero(t, first)
			second, err := read()
			require.NoError(t, err)
			expected := 1
			if operation == "node" {
				expected = 2
			}
			require.Equal(t, expected, second)
			require.Equal(t, int64(2), engine.calls.Load())
		})
	}
}
