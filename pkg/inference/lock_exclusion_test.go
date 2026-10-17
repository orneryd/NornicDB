package inference

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// requireWaitsFor holds a set's owning lock and requires op (an update or a
// snapshot of that set) to wait for it: op must not finish while the lock is
// held, and must finish once it is released. An op that bypasses the owner
// (separately atomic counters, a read after releasing it) finishes at once
// and fails; a correct op never finishes early, so the check cannot fail a
// correct implementation, whatever the scheduling.
func requireWaitsFor(t *testing.T, name string, owner sync.Locker, op func()) {
	t.Helper()
	owner.Lock()
	done := make(chan struct{})
	go func() {
		defer close(done)
		op()
	}()
	select {
	case <-done:
		owner.Unlock()
		t.Fatalf("%s finished while its owning lock was held: it doesn't go through that lock", name)
	case <-time.After(100 * time.Millisecond):
	}
	owner.Unlock()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatalf("%s did not finish after its owning lock was released", name)
	}
}

// Every statistics set read as a set has one owning lock, which both its
// updates and its snapshots go through.
func TestStatisticsSetsGoThroughTheirOwningLock(t *testing.T) {
	t.Run("cooldown checks", func(t *testing.T) {
		defer config.WithCooldownEnabled()()
		ct := NewCooldownTable()
		requireWaitsFor(t, "CanMaterialize", &ct.counters.mu, func() { ct.CanMaterialize("a", "b", "relates_to") })
		requireWaitsFor(t, "Stats", &ct.counters.mu, func() { ct.Stats() })
		requireWaitsFor(t, "Clear", &ct.counters.mu, func() { ct.Clear() })
	})
	t.Run("cluster searches", func(t *testing.T) {
		ci := NewClusterIntegration(testGPUManager(), testClusterConfig(), testKMeansConfig(3), testEmbConfig())
		require.NoError(t, ci.AddEmbedding("n", generateRandomEmbedding(64)))
		requireWaitsFor(t, "Search", &ci.searches.mu, func() { _, _ = ci.Search(context.Background(), generateRandomEmbedding(64), 1) })
		requireWaitsFor(t, "Stats", &ci.searches.mu, func() { ci.Stats() })
	})
	t.Run("topology builds", func(t *testing.T) {
		store := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "test")
		setupTestGraph(t, store)
		cfg := DefaultTopologyConfig()
		cfg.Enabled = true
		topo := NewTopologyIntegration(store, cfg)
		requireWaitsFor(t, "SuggestTopological (rebuild)", &topo.graphMu, func() { _, _ = topo.SuggestTopological(context.Background(), "alice") })
		requireWaitsFor(t, "Stats", &topo.graphMu, func() { topo.Stats() })
	})
	t.Run("evidence", func(t *testing.T) {
		eb := NewEvidenceBuffer()
		requireWaitsFor(t, "AddEvidence", &eb.mu, func() { eb.AddEvidence("a", "b", "relates_to", 0.5, "coaccess", "s") })
		requireWaitsFor(t, "Stats", &eb.mu, func() { eb.Stats() })
	})
	t.Run("edge decay scans", func(t *testing.T) {
		cfg := DefaultEdgeDecayConfig()
		cfg.Enabled = true
		ed := NewEdgeDecay(cfg, storage.NewMemoryEngine())
		requireWaitsFor(t, "TriggerScan", &ed.stats.mu, func() { _, _, _, _ = ed.TriggerScan(context.Background()) })
		requireWaitsFor(t, "GetStats", &ed.stats.mu, func() { ed.GetStats() })
	})
	t.Run("heimdall reviews", func(t *testing.T) {
		defer config.WithAutoTLPLLMQCEnabled()()
		cfg := DefaultHeimdallQCConfig()
		cfg.Enabled = true
		qc := NewHeimdallQC(func(context.Context, string) (string, error) { return `{"approved": []}`, nil }, cfg)
		requireWaitsFor(t, "ReviewBatch", &qc.stats.mu, func() {
			_, _, _ = qc.ReviewBatch(context.Background(), NodeSummary{ID: "a"}, []EdgeSuggestion{{SourceID: "a", TargetID: "b", Confidence: 0.99}}, nil)
		})
		requireWaitsFor(t, "GetStats", &qc.stats.mu, func() { qc.GetStats() })
	})
	t.Run("kalman suggestions", func(t *testing.T) {
		ka := NewKalmanAdapter(New(DefaultConfig()), DefaultKalmanAdapterConfig())
		requireWaitsFor(t, "OnAccess", &ka.mu, func() { _, _ = ka.OnAccess(context.Background(), "a") })
		requireWaitsFor(t, "GetStats", &ka.mu, func() { ka.GetStats() })
	})
	t.Run("engine components", func(t *testing.T) {
		e := New(DefaultConfig())
		for i, op := range []func(){
			func() { e.SetCooldownTable(NewCooldownTable()) },
			func() { e.GetCooldownTable() },
			func() { e.ProcessSuggestion(EdgeSuggestion{SourceID: "a", TargetID: "b", Type: "RELATES_TO"}, "s") },
			func() { e.CleanupTier1() },
		} {
			requireWaitsFor(t, fmt.Sprintf("engine component access %d", i), &e.mu, op)
		}
	})
}
