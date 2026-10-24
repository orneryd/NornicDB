package cypher_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type changingProjectedRead struct {
	storage.Engine
	available               bool
	reads, unavailableReads int
}

func (e *changingProjectedRead) ReadCapabilityAvailable(reflect.Type) bool { return e.available }
func (e *changingProjectedRead) StreamNodesByLabelProjected(label string, properties []string, visit func(*storage.Node) error) error {
	if !e.available {
		e.unavailableReads++
		return storage.ErrNotImplemented
	}
	e.reads++
	return e.Engine.StreamNodesWithOptions(context.Background(), storage.StreamNodesOptions{Label: label, Projection: properties, ApplyDecayFilter: true}, visit)
}

// Bind the executor while acceleration is available, then change support
// without changing the engine or statement. Parameter changes force fresh
// results; a retained optional reader must not outlive its availability.
func TestEngineDecoratorsAvailabilityChangesAfterBinding(t *testing.T) {
	for _, placement := range []string{"inside_namespace", "outside_namespace"} {
		for _, order := range []string{"WT", "TW"} {
			t.Run(placement+"/"+order, func(t *testing.T) {
				base := storage.NewMemoryEngine()
				t.Cleanup(func() { require.NoError(t, base.Close()) })
				tenant := storage.NewNamespacedEngine(base, "tenant")
				_, err := tenant.CreateNode(&storage.Node{ID: "n", Labels: []string{"LiveAvailability"}, Properties: map[string]interface{}{"v": int64(1)}})
				require.NoError(t, err)
				other := storage.NewNamespacedEngine(base, "other")
				_, err = other.CreateNode(&storage.Node{ID: "n", Labels: []string{"LiveAvailability"}, Properties: map[string]interface{}{"v": int64(99)}})
				require.NoError(t, err)
				conditional := &changingProjectedRead{Engine: base, available: true}
				var view storage.Engine = storage.NewNamespacedEngine(conditional, "tenant")
				if placement == "outside_namespace" {
					conditional.Engine = tenant
					view = conditional
				}
				if order == "WT" {
					view = storage.NewWALEngine(storage.NewTracedEngine(view), nil)
				} else {
					view = storage.NewTracedEngine(storage.NewWALEngine(view, nil))
				}
				executor := cypher.NewStorageExecutor(view)
				for nonce, available := range []bool{true, false, true} {
					conditional.available = available
					previous := conditional.reads
					result, err := executor.Execute(context.Background(), "MATCH (n:LiveAvailability) RETURN n.v", map[string]interface{}{"nonce": int64(nonce)})
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
					if available {
						require.Greater(t, conditional.reads, previous, "positive control must exercise projection")
					} else {
						require.Equal(t, previous, conditional.reads)
					}
					require.Zero(t, conditional.unavailableReads)
				}
			})
		}
	}
}
