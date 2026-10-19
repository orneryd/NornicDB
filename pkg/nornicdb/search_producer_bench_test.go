package nornicdb

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func openProducerBenchDB(b *testing.B, decay bool) (*DB, *dbSearchService) {
	b.Helper()
	ctx := context.Background()
	config := DefaultConfig()
	config.Memory.EmbeddingEnabled = false
	config.Memory.AutoLinksEnabled = false
	config.Memory.DecayEnabled = decay
	config.Memory.SearchRelationshipsEnabled = false
	config.Memory.EmbeddingDimensions = 4
	config.Database.AsyncWritesEnabled = false
	db, err := Open(b.TempDir(), config)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = db.Close() })
	if _, err := db.EnsureSearchIndexesBuilt(ctx, db.defaultDatabaseName(), nil); err != nil {
		b.Fatal(err)
	}
	db.searchServicesMu.RLock()
	entry := db.searchServices[db.defaultDatabaseName()]
	db.searchServicesMu.RUnlock()
	entry.pendingFlushDelay = time.Microsecond
	return db, entry
}

func waitDrained(entry *dbSearchService) {
	for {
		entry.pendingFlushMu.Lock()
		done := !entry.pendingFlushRunning && entry.pendingFlushTimer == nil && !entry.hasPending()
		entry.pendingFlushMu.Unlock()
		if done {
			return
		}
		runtime.Gosched()
	}
}

// Storage relationship updates (replication, APOC, import) with relationship
// search off; one operation updates 256 relationships and drains search.
func BenchmarkSearchRefreshEdgeUpdatesRelationshipTextOff(b *testing.B) {
	db, entry := openProducerBenchDB(b, false)
	for _, id := range []storage.NodeID{"a", "b"} {
		if _, err := db.storage.CreateNode(&storage.Node{ID: id}); err != nil {
			b.Fatal(err)
		}
	}
	edges := make([]*storage.Edge, 256)
	for i := range edges {
		edges[i] = &storage.Edge{ID: storage.EdgeID(fmt.Sprintf("r%d", i)), Type: "CITES", StartNode: "a", EndNode: "b", Properties: map[string]any{"vec": []float32{1, 0, 0, float32(i)}}}
		if err := db.storage.CreateEdge(edges[i]); err != nil {
			b.Fatal(err)
		}
	}
	waitDrained(entry)
	b.ReportAllocs()
	b.ResetTimer()
	for n := range b.N {
		for i, edge := range edges {
			edge.Properties = map[string]any{"vec": []float32{1, float32(n % 2), 0, float32(i)}}
			if err := db.storage.UpdateEdge(edge); err != nil {
				b.Fatal(err)
			}
		}
		waitDrained(entry)
	}
}

// Hiding and showing a node and a relationship through storage, drained into
// search; one operation is both round trips.
func BenchmarkSearchRefreshSuppressionFlips(b *testing.B) {
	db, entry := openProducerBenchDB(b, false)
	for _, id := range []storage.NodeID{"a", "b"} {
		if _, err := db.storage.CreateNode(&storage.Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"content": "word " + string(id)}}); err != nil {
			b.Fatal(err)
		}
	}
	if err := db.storage.CreateEdge(&storage.Edge{ID: "r", Type: "CITES", StartNode: "a", EndNode: "b", Properties: map[string]any{"vec": []float32{1, 0, 0, 0}}}); err != nil {
		b.Fatal(err)
	}
	waitDrained(entry)
	suppressor := db.storage.(storage.ManualSuppressionEngine)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		for _, hidden := range []bool{true, false} {
			if err := suppressor.SetNodeManuallySuppressed("a", hidden); err != nil {
				b.Fatal(err)
			}
			if err := suppressor.SetEdgeManuallySuppressed("r", hidden); err != nil {
				b.Fatal(err)
			}
			waitDrained(entry)
		}
	}
}

// A decay flip to suppressed and back (a policy toggle re-evaluating the
// node), drained into search; one operation is both flips.
func BenchmarkSearchRefreshDecayFlips(b *testing.B) {
	db, entry := openProducerBenchDB(b, true)
	base := db.GetBaseStorageForManager()
	be := unwrapToBadgerEngine(base)
	schema := base.(storage.NamespaceSchemaProvider).GetSchemaForNamespace(db.defaultDatabaseName())
	if err := schema.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{Name: "short_doc", Scope: knowledgepolicy.ScopeNode, Function: knowledgepolicy.DecayFunctionExponential, HalfLifeSeconds: 1, VisibilityThreshold: 0.10, ScoreFrom: knowledgepolicy.ScoreFromCreated, Enabled: true, DecayEnabled: true}); err != nil {
		b.Fatal(err)
	}
	if err := schema.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{Name: "short_doc_binding", ProfileRef: "short_doc", TargetLabels: []string{"Doc"}}); err != nil {
		b.Fatal(err)
	}
	old := time.Now().Add(-72 * time.Hour)
	if _, err := db.storage.CreateNode(&storage.Node{ID: "a", Labels: []string{"Doc"}, Properties: map[string]any{"content": "decayword"}, CreatedAt: old, UpdatedAt: old}); err != nil {
		b.Fatal(err)
	}
	id := db.defaultDatabaseName() + ":a"
	waitDrained(entry)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		for _, decay := range []bool{true, false} {
			if err := schema.AlterDecayProfile("short_doc", map[string]any{"decayEnabled": decay}); err != nil {
				b.Fatal(err)
			}
			if _, err := be.EnqueueDeindexIfSuppressed(id, false); err != nil {
				b.Fatal(err)
			}
			waitDrained(entry)
		}
	}
}
