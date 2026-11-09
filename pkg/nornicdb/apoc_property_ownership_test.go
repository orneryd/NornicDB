package nornicdb

import (
	"testing"

	apocstorage "github.com/orneryd/nornicdb/apoc/storage"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestAPOCPropertyUpdatesPersistThroughNativeBackends(t *testing.T) {
	for _, backend := range []string{"memory", "badger"} {
		t.Run(backend, func(t *testing.T) {
			var base storage.Engine
			if backend == "memory" {
				base = fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
			} else {
				engine, err := storage.NewBadgerEngineInMemory()
				require.NoError(t, err)
				base = fixtureowner.Own(t, fixtureowner.Track(t, engine, storage.ErrStorageClosed))
			}
			native := storage.NewNamespacedEngine(base, "apoc_property_ownership")
			_, err := native.CreateNode(t.Context(), &storage.Node{
				ID: "1", Labels: []string{"Person"},
				Properties: map[string]interface{}{"old": []string{"value"}, "keep": true},
			})
			require.NoError(t, err)
			_, err = native.CreateNode(t.Context(), &storage.Node{ID: "2"})
			require.NoError(t, err)
			require.NoError(t, native.CreateEdge(t.Context(), &storage.Edge{
				ID: "1", StartNode: "1", EndNode: "2", Type: "LINK",
				Properties: map[string]interface{}{"old": []string{"value"}, "keep": true},
			}))
			adapter := NewAPOCStorageAdapter(native)
			require.NoError(t, adapter.UpdateNode(t.Context(), 1, apocstorage.PropertyUpdate{
				Remove: []string{"old"}, Set: map[string]interface{}{"new": []string{"value"}},
			}))
			require.NoError(t, adapter.UpdateRelationship(t.Context(), 1, apocstorage.PropertyUpdate{
				Remove: []string{"old"}, Set: map[string]interface{}{"new": []string{"value"}},
			}))
			require.NoError(t, adapter.UpdateNode(t.Context(), 1, apocstorage.PropertyUpdate{
				Remove: []string{"new"}, Set: map[string]interface{}{"new": []string{"value"}},
			}))

			node, err := native.GetNode(t.Context(), "1")
			require.NoError(t, err)
			edge, err := native.GetEdge(t.Context(), "1")
			require.NoError(t, err)
			want := map[string]interface{}{"new": []string{"value"}, "keep": true}
			require.Equal(t, want, node.Properties)
			require.Equal(t, want, edge.Properties)
			before, err := adapter.GetNode(t.Context(), 2)
			require.NoError(t, err)
			require.NoError(t, adapter.UpdateNode(t.Context(), 2, apocstorage.PropertyUpdate{}))
			after, err := adapter.GetNode(t.Context(), 2)
			require.NoError(t, err)
			require.Equal(t, before.Properties, after.Properties, "zero patch preserves the existing empty/nil shape")

			owned := map[string]interface{}{"nested": []string{"accepted"}}
			require.NoError(t, adapter.UpdateRelationship(t.Context(), 1, apocstorage.PropertyUpdate{Set: owned}))
			owned["nested"].([]string)[0] = "caller changed"
			edge, err = native.GetEdge(t.Context(), "1")
			require.NoError(t, err)
			require.Equal(t, []string{"accepted"}, edge.Properties["nested"])
			require.Equal(t, true, edge.Properties["keep"], "a patch preserves unrelated properties")
		})
	}
}
