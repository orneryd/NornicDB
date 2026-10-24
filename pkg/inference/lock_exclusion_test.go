package inference

import (
	"context"
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// goroutineID returns the calling goroutine's ID, from its stack header
// ("goroutine 42 [running]:").
func goroutineID() string {
	buf := make([]byte, 64)
	buf = buf[:runtime.Stack(buf, false)]
	return strings.Fields(string(buf))[1]
}

// lockWaitCaller reports whether goroutine id is parked acquiring a sync
// lock, and if so, the first frame outside the runtime and sync packages:
// the function that is waiting for the lock.
func lockWaitCaller(id string) (caller string, parked bool) {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	for _, g := range strings.Split(string(buf), "\n\n") {
		header, frames, _ := strings.Cut(g, "\n")
		if !strings.HasPrefix(header, "goroutine "+id+" ") || !strings.Contains(header, "[") {
			continue
		}
		status := header[strings.Index(header, "[")+1:] // "sync.Mutex.Lock]:", "sync.RWMutex.RLock, 2 minutes]:"
		if !strings.HasPrefix(status, "sync.") && !strings.HasPrefix(status, "semacquire") {
			return "", false
		}
		for _, line := range strings.Split(frames, "\n") {
			// A frame is "path/pkg.(*T).method(args)" followed by an
			// indented file:line; the last line names the creator.
			if line == "" || strings.HasPrefix(line, "\t") || strings.HasPrefix(line, "created by ") || !strings.HasSuffix(line, ")") {
				continue
			}
			fn := line[:strings.LastIndex(line, "(")]
			fn = fn[strings.LastIndex(fn, "/")+1:] // "inference.(*T).method", "sync.(*Mutex).lockSlow"
			if !strings.HasPrefix(fn, "runtime.") && !strings.HasPrefix(fn, "sync.") {
				return fn, true
			}
		}
		return "", true
	}
	return "", false
}

// requireWaitsFor holds a set's owning lock and requires op (an update or a
// snapshot of that set) to wait for it, with a handshake rather than a
// timeout: op runs on its own goroutine, and the test waits until that
// goroutine is parked acquiring a lock in function at, the guarded access.
// The test holds no lock but owner, so that lock is owner. Only then is the
// owner released, and op must finish. An op that bypasses the owner finishes
// without ever parking there and fails; one parked in another function fails
// too. The outcome doesn't depend on scheduling.
//
// This shows the access goes through the owner. That every access to the set
// happens inside one critical section of it is checked structurally, by
// TestSharedStateHoldsItsOwningLock (stateTuples).
func requireWaitsFor(t *testing.T, name string, owner sync.Locker, at string, op func()) {
	t.Helper()
	owner.Lock()
	ids := make(chan string, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ids <- goroutineID()
		op()
	}()
	id := <-ids
	for deadline := time.Now().Add(30 * time.Second); ; runtime.Gosched() {
		select {
		case <-done:
			owner.Unlock()
			t.Fatalf("%s finished while its owning lock was held: it doesn't go through that lock", name)
		default:
		}
		if caller, parked := lockWaitCaller(id); parked {
			if caller != at {
				owner.Unlock()
				<-done
				t.Fatalf("%s waited for the owning lock in %s, not in %s", name, caller, at)
			}
			break
		}
		if time.Now().After(deadline) {
			owner.Unlock()
			t.Fatalf("%s neither finished nor reached %s", name, at)
		}
	}
	owner.Unlock()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatalf("%s did not finish after its owning lock was released", name)
	}
}

// Every statistics set read as a set has one owning lock, which both its
// updates and its snapshots go through, at the access itself.
func TestStatisticsSetsGoThroughTheirOwningLock(t *testing.T) {
	t.Run("cooldown checks", func(t *testing.T) {
		defer config.WithCooldownEnabled()()
		ct := NewCooldownTable()
		requireWaitsFor(t, "CanMaterialize", &ct.counters.mu, "inference.(*checkCounters).record", func() { ct.CanMaterialize("a", "b", "relates_to") })
		requireWaitsFor(t, "Stats", &ct.counters.mu, "inference.(*checkCounters).snapshot", func() { ct.Stats() })
		requireWaitsFor(t, "Clear", &ct.counters.mu, "inference.(*checkCounters).reset", func() { ct.Clear() })
	})
	t.Run("cluster searches", func(t *testing.T) {
		ci := NewClusterIntegration(testGPUManager(), testClusterConfig(), testKMeansConfig(3), testEmbConfig())
		require.NoError(t, ci.AddEmbedding("n", generateRandomEmbedding(64)))
		requireWaitsFor(t, "Search", &ci.searches.mu, "inference.(*searchCounters).record", func() { _, _ = ci.Search(context.Background(), generateRandomEmbedding(64), 1) })
		requireWaitsFor(t, "Stats", &ci.searches.mu, "inference.(*searchCounters).snapshot", func() { ci.Stats() })
	})
	t.Run("topology builds", func(t *testing.T) {
		store := storage.NewNamespacedEngine(fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), "test")
		setupTestGraph(t, store)
		cfg := DefaultTopologyConfig()
		cfg.Enabled = true
		topo := NewTopologyIntegration(store, cfg)
		requireWaitsFor(t, "SuggestTopological (rebuild)", &topo.graphMu, "inference.(*TopologyIntegration).ensureGraph", func() { _, _ = topo.SuggestTopological(context.Background(), "alice") })
		requireWaitsFor(t, "Stats", &topo.graphMu, "inference.(*TopologyIntegration).Stats", func() { topo.Stats() })
	})
	t.Run("evidence", func(t *testing.T) {
		eb := NewEvidenceBuffer()
		requireWaitsFor(t, "AddEvidence", &eb.mu, "inference.(*EvidenceBuffer).AddEvidence", func() { eb.AddEvidence("a", "b", "relates_to", 0.5, "coaccess", "s") })
		requireWaitsFor(t, "Stats", &eb.mu, "inference.(*EvidenceBuffer).Stats", func() { eb.Stats() })
	})
	t.Run("edge decay scans", func(t *testing.T) {
		cfg := DefaultEdgeDecayConfig()
		cfg.Enabled = true
		ed := NewEdgeDecay(cfg, fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)))
		requireWaitsFor(t, "TriggerScan", &ed.stats.mu, "inference.(*EdgeDecay).scanAndDecay", func() { _, _, _, _ = ed.TriggerScan(context.Background()) })
		requireWaitsFor(t, "GetStats", &ed.stats.mu, "inference.(*EdgeDecay).GetStats", func() { ed.GetStats() })
	})
	t.Run("heimdall reviews", func(t *testing.T) {
		defer config.WithAutoTLPLLMQCEnabled()()
		cfg := DefaultHeimdallQCConfig()
		cfg.Enabled = true
		qc := NewHeimdallQC(func(context.Context, string) (string, error) { return `{"approved": []}`, nil }, cfg)
		requireWaitsFor(t, "ReviewBatch", &qc.stats.mu, "inference.(*HeimdallQC).processBatch", func() {
			_, _, _ = qc.ReviewBatch(context.Background(), NodeSummary{ID: "a"}, []EdgeSuggestion{{SourceID: "a", TargetID: "b", Confidence: 0.99}}, nil)
		})
		requireWaitsFor(t, "GetStats", &qc.stats.mu, "inference.(*HeimdallQC).GetStats", func() { qc.GetStats() })
	})
	t.Run("kalman suggestions", func(t *testing.T) {
		ka := NewKalmanAdapter(New(DefaultConfig()), DefaultKalmanAdapterConfig())
		requireWaitsFor(t, "OnAccess", &ka.mu, "inference.(*KalmanAdapter).recordAccess", func() { _, _ = ka.OnAccess(context.Background(), "a") })
		requireWaitsFor(t, "GetStats", &ka.mu, "inference.(*KalmanAdapter).GetStats", func() { ka.GetStats() })
	})
	t.Run("engine components", func(t *testing.T) {
		e := New(DefaultConfig())
		for i, op := range []struct {
			at string
			fn func()
		}{
			{"inference.(*Engine).SetCooldownTable", func() { e.SetCooldownTable(NewCooldownTable()) }},
			{"inference.(*Engine).components", func() { e.GetCooldownTable() }},
			{"inference.(*Engine).components", func() { e.ProcessSuggestion(EdgeSuggestion{SourceID: "a", TargetID: "b", Type: "RELATES_TO"}, "s") }},
			{"inference.(*Engine).components", func() { e.CleanupTier1() }},
		} {
			requireWaitsFor(t, fmt.Sprintf("engine component access %d", i), &e.mu, op.at, op.fn)
		}
	})
}
