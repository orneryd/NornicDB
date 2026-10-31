package multidb

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"strconv"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// BenchmarkSizeTrackingFreshCreate is the paired A/B guard for the ordinary
// no-conflict path. The hidden-ID classifier must not add a predecessor point
// lookup to fresh creates; the relationship case also measures the O(1)
// accounting outcome counters used by bulk compensation.
func BenchmarkSizeTrackingFreshCreate(b *testing.B) {
	b.Run("node", func(b *testing.B) {
		_, engine := benchmarkSizeTrackingEngine(b)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			id := storage.NodeID("fresh-node-" + strconv.Itoa(i))
			if _, err := engine.CreateNode(&storage.Node{ID: id, Labels: []string{"Fresh"}}); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("relationship", func(b *testing.B) {
		_, engine := benchmarkSizeTrackingEngine(b)
		for _, id := range []storage.NodeID{"start", "end"} {
			if _, err := engine.CreateNode(&storage.Node{ID: id}); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			edge := &storage.Edge{
				ID:        storage.EdgeID("fresh-edge-" + strconv.Itoa(i)),
				StartNode: "start",
				EndNode:   "end",
				Type:      "FRESH",
			}
			if err := engine.CreateEdge(edge); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func benchmarkSizeTrackingEngine(b *testing.B) (*DatabaseManager, storage.Engine) {
	b.Helper()
	manager, err := NewDatabaseManager(b.Context(), runtimeFixtureOwner.Own(b, runtimeFixtureOwner.Track(b, storage.NewMemoryEngine(), storage.ErrStorageClosed)), nil)
	if err != nil {
		b.Fatal(err)
	}
	runtimeFixtureOwner.Own(b, manager)
	engine, err := manager.GetStorage("nornic")
	if err != nil {
		b.Fatal(err)
	}
	return manager, engine
}
