// Author: Lane #1644 (Codex). Endpoint reads select a physical writer;
// an unread earlier source must never route a successful write to a later one.
package storage_test

import (
	"errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

type foundationEndpointFault struct {
	storage.Engine
	failed storage.NodeID
	cause  error
	visits int
}

func (v *foundationEndpointFault) GetNode(id storage.NodeID) (*storage.Node, error) {
	if id == v.failed {
		v.visits++
		return nil, v.cause
	}
	return v.Engine.GetNode(id)
}

func TestFoundationPublicCompositeWriteOutcome(t *testing.T) {
	for _, failed := range []storage.NodeID{"n", "m"} {
		t.Run(string(failed), func(t *testing.T) {
			base := storage.NewMemoryEngine()
			defer base.Close()
			a := storage.NewNamespacedEngine(base, "a")
			b := storage.NewNamespacedEngine(base, "b")
			for _, view := range []storage.Engine{a, b} {
				for _, id := range []storage.NodeID{"n", "m"} {
					_, err := view.CreateNode(&storage.Node{ID: id})
					require.NoError(t, err)
				}
			}
			cause := errors.New("required composite endpoint read failed")
			fault := &foundationEndpointFault{Engine: a, cause: cause}
			otherFault := &foundationEndpointFault{Engine: b, cause: cause}
			composite := storage.NewCompositeEngine(map[string]storage.Engine{"a": fault, "b": otherFault}, map[string]string{"a": "a", "b": "b"}, map[string]string{"a": "read_write", "b": "read_write"})
			edge := &storage.Edge{ID: "e", StartNode: "n", EndNode: "m", Type: "R"}
			require.NoError(t, composite.CreateEdge(edge))
			require.NoError(t, composite.DeleteEdge("e"))
			fault.failed = failed
			otherFault.failed = failed
			err := composite.CreateEdge(edge)
			require.Positive(t, fault.visits+otherFault.visits)
			require.ErrorIs(t, err, cause)
			for _, child := range []storage.Engine{a, b} {
				_, err = child.GetEdge("e")
				require.ErrorIs(t, err, storage.ErrNotFound, "a failed required endpoint cannot select a physical writer")
			}
		})
	}
}
