package storage_test

import (
	"reflect"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type unavailableHistoricalPairEngine struct {
	*storage.MemoryEngine
	optionalCalls int
}

func (*unavailableHistoricalPairEngine) ReadCapabilityAvailable(reflect.Type) bool { return false }
func (e *unavailableHistoricalPairEngine) GetEdgesBetweenVisibleAt(storage.NodeID, storage.NodeID, storage.MVCCVersion) ([]*storage.Edge, error) {
	e.optionalCalls++
	return nil, storage.ErrNotImplemented
}

func TestReadCapabilityQualifiedCompositePairUsesAvailableView(t *testing.T) {
	base := storage.NewMemoryEngineWithMVCCHistory()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	conditional := &unavailableHistoricalPairEngine{MemoryEngine: base}
	namespace := storage.NewNamespacedEngine(conditional, "tenant")
	for _, id := range []storage.NodeID{"a", "b"} {
		_, err := namespace.CreateNode(&storage.Node{ID: id})
		require.NoError(t, err)
	}
	head, err := base.GetNodeCurrentHead("tenant:b")
	require.NoError(t, err)
	require.False(t, head.Version.IsZero())
	require.NoError(t, namespace.CreateEdge(&storage.Edge{ID: "edge", StartNode: "a", EndNode: "b", Type: "LINK"}))
	for name, wrapped := range map[string]storage.Engine{
		"namespace":  namespace,
		"traced-wal": storage.NewTracedEngine(storage.NewWALEngine(namespace, nil)),
		"wal-traced": storage.NewWALEngine(storage.NewTracedEngine(namespace), nil),
	} {
		t.Run(name, func(t *testing.T) {
			composite := storage.NewCompositeEngine(map[string]storage.Engine{"tenant": wrapped}, map[string]string{"tenant": "tenant"}, map[string]string{"tenant": "read_write"})
			edges, err := composite.GetEdgesBetweenVisibleAt("4:tenant:a", "4:tenant:b", head.Version)
			require.ErrorIs(t, err, storage.ErrNotImplemented)
			require.Empty(t, edges, "unavailable history must not return the later LINK")
			require.Zero(t, conditional.optionalCalls)
		})
	}
}
