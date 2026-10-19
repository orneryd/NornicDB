package search

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type maintenanceTransitionLog struct {
	entered, release chan struct{}
	once             sync.Once
}

func (h *maintenanceTransitionLog) Enabled(context.Context, slog.Level) bool { return true }
func (h *maintenanceTransitionLog) WithAttrs([]slog.Attr) slog.Handler       { return h }
func (h *maintenanceTransitionLog) WithGroup(string) slog.Handler            { return h }
func (h *maintenanceTransitionLog) Handle(_ context.Context, r slog.Record) error {
	if strings.Contains(r.Message, "[HNSW] Building from in-memory index:") {
		h.once.Do(func() { close(h.entered); <-h.release })
	}
	return nil
}

func TestHNSWMaintenanceOverlaySurvivesSharedGraphTransition(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "3")
	svc := NewServiceWithDimensions(newNamespacedEngine(t), 3)
	t.Cleanup(func() { _ = svc.Close() })
	for _, id := range []string{"anchor", "delete", "trigger"} {
		sourceRetirementNode(t, svc, id, []float32{1, 0, 0})
	}
	old, err := svc.getOrCreateHNSWIndex(context.Background(), 3)
	require.NoError(t, err)
	require.NoError(t, svc.RemoveNode("trigger"))
	_, err = svc.getOrCreateVectorPipeline(context.Background())
	require.NoError(t, err)
	require.Equal(t, strategyModeBruteCPU, svc.currentPipelineStrategy())
	svc.runtimeStrategyTransitions.Store(true)
	h := &maintenanceTransitionLog{entered: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(h.release) }) }
	defer release()
	svc.SetLogger(slog.New(h))
	done := make(chan error, 1)
	go func() { done <- svc.maybeRebuildHNSW(context.Background(), 0, 1, time.Nanosecond) }()
	select {
	case <-h.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("maintenance did not finish its source snapshot")
	}
	// The actual launcher reserves this state before starting its wrapper.
	// Both public mutations belong to the active maintenance and strategy logs.
	svc.strategyTransitionMu.Lock()
	svc.strategyTransitionInProgress = true
	svc.strategyTransitionMu.Unlock()
	sourceRetirementNode(t, svc, "late", []float32{0, 1, 0})
	require.NoError(t, svc.RemoveNode("delete"))
	pending := svc.hnswRebuildOverlay.snapshot()
	require.Contains(t, pending.Vectors, "late")
	require.Contains(t, pending.Tombstones, "delete")
	svc.runStrategyTransition(strategyModeHNSW)
	svc.hnswMu.RLock()
	adopted := svc.hnswIndex
	svc.hnswMu.RUnlock()
	require.Same(t, old, adopted, "wrapper must exercise adoption of the shared ready graph")
	pending = svc.hnswRebuildOverlay.snapshot()
	assert.Contains(t, pending.Vectors, "late", "wrapper discarded another builder's insertion")
	assert.Contains(t, pending.Tombstones, "delete", "wrapper discarded another builder's deletion")
	release()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("maintenance did not finish")
	}
	svc.hnswMu.RLock()
	rebuilt := svc.hnswIndex
	svc.hnswMu.RUnlock()
	require.NotSame(t, old, rebuilt)
	rebuilt.mu.RLock()
	_, hasLate := rebuilt.idToInternal["late"]
	_, hasDeleted := rebuilt.idToInternal["delete"]
	rebuilt.mu.RUnlock()
	assert.True(t, hasLate, "replacement graph lost the insertion")
	assert.False(t, hasDeleted, "replacement graph resurrected a deleted ID")
	results, err := rebuilt.Search(context.Background(), []float32{0, 1, 0}, 1, 0)
	require.NoError(t, err)
	assert.Contains(t, results, ANNResult{ID: "late", Score: 1})
	assert.Empty(t, svc.hnswRebuildOverlay.snapshot().Vectors, "the builder owns final overlay cleanup")
	assert.Empty(t, svc.hnswRebuildOverlay.snapshot().Tombstones)
}
