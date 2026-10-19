package search

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type reviewerR4BuildLog struct {
	started, release chan struct{}
	calls            atomic.Int32
}

func (h *reviewerR4BuildLog) Enabled(context.Context, slog.Level) bool { return true }
func (h *reviewerR4BuildLog) WithAttrs([]slog.Attr) slog.Handler       { return h }
func (h *reviewerR4BuildLog) WithGroup(string) slog.Handler            { return h }
func (h *reviewerR4BuildLog) Handle(_ context.Context, r slog.Record) error {
	if strings.Contains(r.Message, "[HNSW]") && strings.Contains(r.Message, "Building from") {
		if h.calls.Add(1) == 1 {
			close(h.started)
			<-h.release
		}
	}
	return nil
}

// A first vector request expires during graph construction. The second request
// must reuse the same service-owned work rather than start construction again.
func TestReviewerR4RequestCancellationDoesNotRestartHNSW(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
	t.Setenv("NORNICDB_VECTOR_ANN_QUALITY", "fast")
	engine := newNamespacedEngine(t)
	svc := NewServiceWithDimensions(engine, 3)
	svc.SetRuntimeStrategyTransitionsEnabled(false)
	gate := &reviewerR4BuildLog{started: make(chan struct{}), release: make(chan struct{})}
	svc.SetLogger(slog.New(gate))
	opened := false
	t.Cleanup(func() {
		if !opened {
			close(gate.release)
		}
		_ = svc.Close()
		_ = engine.Close()
	})
	for _, id := range []string{"a", "b", "c"} {
		n := &storage.Node{ID: storage.NodeID(id), ChunkEmbeddings: [][]float32{{1, 0, 0}}}
		_, err := engine.CreateNode(n)
		require.NoError(t, err)
		require.NoError(t, svc.IndexNode(n))
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	minimum := -1.0
	go func() {
		_, err := svc.Search(ctx, "", []float32{1, 0, 0}, &SearchOptions{Limit: 2, MinSimilarity: &minimum})
		done <- err
	}()
	select {
	case <-gate.started:
	case <-time.After(5 * time.Second):
		t.Fatal("no initial HNSW construction")
	}
	cancel()
	close(gate.release)
	opened = true
	select {
	case err := <-done:
		t.Logf("first request outcome: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled request stuck")
	}
	_, err := svc.Search(context.Background(), "", []float32{1, 0, 0}, &SearchOptions{Limit: 2, MinSimilarity: &minimum})
	require.NoError(t, err)
	require.Eventually(t, func() bool { svc.hnswMu.RLock(); defer svc.hnswMu.RUnlock(); return svc.servingHNSW() != nil }, 5*time.Second, time.Millisecond)
	require.Equal(t, int32(1), gate.calls.Load(), "the second request restarted HNSW after the first request cancelled its build")
}
