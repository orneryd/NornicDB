package multidb

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

const startupBenchmarkEntityCount = 2000

func newStartupBenchmarkEngine(b *testing.B, includeEdges bool) *storage.BadgerEngine {
	b.Helper()
	dataDir := b.TempDir()
	engine, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](b, storage.ErrStorageClosed)(storage.NewBadgerEngine(dataDir))
	if err != nil {
		b.Fatal(err)
	}
	payload := strings.Repeat("x", 1024)
	for index := 0; index < startupBenchmarkEntityCount; index++ {
		nodeID := storage.NodeID(fmt.Sprintf("nornic:node-%06d", index))
		_, err := engine.CreateNode(&storage.Node{
			ID:         nodeID,
			Labels:     []string{"Application"},
			Properties: map[string]interface{}{"payload": payload, "index": int64(index)},
		})
		if err != nil {
			b.Fatal(err)
		}
		if includeEdges {
			err = engine.CreateEdge(&storage.Edge{
				ID:         storage.EdgeID(fmt.Sprintf("nornic:edge-%06d", index)),
				StartNode:  nodeID,
				EndNode:    nodeID,
				Type:       "SELF",
				Properties: map[string]interface{}{"index": int64(index)},
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	}
	if err := engine.Close(); err != nil {
		b.Fatal(err)
	}

	reopened, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](b, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{
		DataDir:             dataDir,
		NodeCacheMaxEntries: 512,
	}))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = reopened.Close() })
	return reopened
}

func BenchmarkStartupLeakedSystemCleanup(b *testing.B) {
	engine := newStartupBenchmarkEngine(b, false)
	manager := &DatabaseManager{
		inner: engine,
		databases: map[string]*DatabaseInfo{
			"nornic": {Name: "nornic", Type: "standard"},
			"system": {Name: "system", Type: "system"},
		},
		config: DefaultConfig(),
	}

	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		manager.cleanupLeakedSystemNodes()
	}
}

func BenchmarkStartupStorageSizeReconciliation(b *testing.B) {
	engine := newStartupBenchmarkEngine(b, true)
	namespaced := storage.NewNamespacedEngine(engine, "nornic")
	checker := &databaseLimitChecker{}

	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		if _, _, err := checker.calculateCurrentStorageSize(namespaced); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkStartupDatabaseManagerWithoutSizeScan measures the normal restart
// path. Storage size reconciliation remains available as the benchmark above,
// but is deferred until a byte limit or explicit size query needs it.
func BenchmarkStartupDatabaseManagerWithoutSizeScan(b *testing.B) {
	engine := newStartupBenchmarkEngine(b, true)
	if _, err := NewDatabaseManager(engine, nil); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		manager, err := NewDatabaseManager(engine, nil)
		if err != nil {
			b.Fatal(err)
		}
		manager.mu.RLock()
		info := manager.databases[manager.DefaultDatabaseName()]
		manager.mu.RUnlock()
		if info == nil || info.sizeInitialized {
			b.Fatal("restart eagerly initialized storage size")
		}
	}
}
