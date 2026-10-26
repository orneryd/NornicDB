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
	beforeRead              func()
}

func (e *changingProjectedRead) ReadCapabilityAvailable(reflect.Type) bool { return e.available }
func (e *changingProjectedRead) StreamNodesByLabelProjected(label string, properties []string, visit func(*storage.Node) error) error {
	if e.beforeRead != nil {
		e.beforeRead()
	}
	if !e.available {
		e.unavailableReads++
		return storage.ErrNotImplemented
	}
	e.reads++
	return e.Engine.StreamNodesWithOptions(context.Background(), storage.StreamNodesOptions{Label: label, Projection: properties, ApplyDecayFilter: true}, visit)
}

func TestEngineDecoratorsAvailabilityRefusedInsideRead(t *testing.T) {
	for _, order := range []string{"WT", "TW"} {
		t.Run(order, func(t *testing.T) {
			base := storage.NewMemoryEngine()
			t.Cleanup(func() { require.NoError(t, base.Close()) })
			conditional := &changingProjectedRead{Engine: base, available: true}
			tenant := storage.NewNamespacedEngine(conditional, "tenant")
			for _, namespace := range []*storage.NamespacedEngine{tenant, storage.NewNamespacedEngine(base, "other")} {
				_, err := namespace.CreateNode(&storage.Node{ID: "n", Labels: []string{"LiveAvailability"}, Properties: map[string]interface{}{"v": int64(1)}})
				require.NoError(t, err)
			}
			var view storage.Engine = storage.NewTracedEngine(storage.NewWALEngine(tenant, nil))
			if order == "WT" {
				view = storage.NewWALEngine(storage.NewTracedEngine(tenant), nil)
			}
			_, err := tenant.CreateNode(&storage.Node{ID: "target", Labels: []string{"EndAvailability"}, Properties: map[string]interface{}{"v": int64(2)}})
			require.NoError(t, err)
			require.NoError(t, tenant.CreateEdge(&storage.Edge{ID: "link", StartNode: "n", EndNode: "target", Type: "LINK"}))
			for _, test := range []struct {
				query     string
				value     int64
				traversal bool
			}{
				{"MATCH (n:LiveAvailability) RETURN n.v", 1, false},
				{"MATCH (n:LiveAvailability)-[:LINK]->(b) RETURN b.v LIMIT 2", 2, true},
				{"MATCH (n:LiveAvailability) WHERE n.v = 1 RETURN count(n)", 1, false},
			} {
				read, queryView := conditional, view
				if test.traversal {
					// A namespace offers a complete query-node read, which routes
					// traversal through core options. Put this refusing provider
					// outside it to exercise the projected start-node operation.
					read = &changingProjectedRead{Engine: tenant, available: true}
					queryView = storage.NewTracedEngine(storage.NewWALEngine(read, nil))
					if order == "WT" {
						queryView = storage.NewWALEngine(storage.NewTracedEngine(read), nil)
					}
				}
				read.available, read.unavailableReads = true, 0
				read.beforeRead = func() { read.available = false }
				result, err := cypher.NewStorageExecutor(queryView).Execute(context.Background(), test.query, nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{test.value}}, result.Rows)
				require.Equal(t, 1, read.unavailableReads, "the read itself must exercise refusal after selection: %s", test.query)
			}
		})
	}
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
