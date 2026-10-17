package search

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// cancelOnMessage cancels a build when it logs text, without blocking it.
type cancelOnMessage struct {
	text     string
	delay    time.Duration // cancel this long after text, inside the phase
	cancel   context.CancelFunc
	once     sync.Once
	cancelAt atomic.Int64
}

func (h *cancelOnMessage) Enabled(context.Context, slog.Level) bool { return true }
func (h *cancelOnMessage) WithAttrs([]slog.Attr) slog.Handler       { return h }
func (h *cancelOnMessage) WithGroup(string) slog.Handler            { return h }
func (h *cancelOnMessage) Handle(_ context.Context, r slog.Record) error {
	if strings.Contains(r.Message, h.text) {
		h.once.Do(func() {
			fire := func() { h.cancelAt.Store(time.Now().UnixNano()); h.cancel() }
			if h.delay > 0 {
				time.AfterFunc(h.delay, fire)
				return
			}
			fire()
		})
	}
	return nil
}

// TestBuildIndexesObservesCancellationInEveryPhase: Close cancels an explicit
// build, and a write waiting on the build (buildMu) is released only when the
// build returns. Every build phase therefore observes cancellation promptly:
// a build cancelled as a phase starts returns the cancellation well before the
// phase could finish over this corpus.
func TestBuildIndexesObservesCancellationInEveryPhase(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
	t.Setenv("NORNICDB_GPU_BACKEND", "cpu")
	t.Setenv("NORNICDB_HNSW_BUILD_GPU_ENABLED", "false")
	const nodes, dims = 6000, 32
	engine := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	for i := 0; i < nodes; i++ {
		vec := make([]float32, dims)
		for d := range vec {
			vec[d] = float32(math.Sin(float64(i*(d+1)) * 0.37))
		}
		_, err := engine.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("nornic:n%d", i)), Labels: []string{"Doc"},
			Properties: map[string]any{"text": fmt.Sprintf("term%d shared", i%97)}, NamedEmbeddings: map[string][]float32{"v": vec}})
		require.NoError(t, err)
	}
	build := func(handler slog.Handler, ctx context.Context) (time.Duration, error) {
		svc := NewServiceWithDimensions(engine, dims)
		t.Cleanup(func() { _ = svc.Close() })
		svc.SetLogger(slog.New(handler))
		start := time.Now()
		err := svc.BuildIndexes(ctx)
		return time.Since(start), err
	}
	full, err := build(slog.DiscardHandler, context.Background())
	require.NoError(t, err)
	t.Logf("uncancelled build: %v", full)

	for _, tc := range []struct {
		name, phase string
		delay       time.Duration
	}{
		{name: "node iteration", phase: "BuildIndexes: building from storage"},
		{name: "after node phase", phase: "base index iteration complete"},
		{name: "vector warmup", phase: "starting vector pipeline warmup"},
		{name: "graph construction start", phase: "[HNSW] Building from"},
		{name: "inside graph construction", phase: "[HNSW] Building from", delay: 200 * time.Millisecond},
	} {
		phase := tc.phase
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			h := &cancelOnMessage{text: phase, cancel: cancel, delay: tc.delay}
			_, err := build(h, ctx)
			at := h.cancelAt.Load()
			if at == 0 {
				t.Skipf("phase %q not logged by this build", phase)
			}
			if tc.delay > 0 && err == nil {
				t.Skipf("the phase finished within %v; cancellation came after the build", tc.delay)
			}
			after := time.Since(time.Unix(0, at))
			t.Logf("returned %v after cancellation (uncancelled build %v)", after, full)
			require.ErrorIs(t, err, context.Canceled, "a build cancelled at %q must report it", phase)
			require.Less(t, after, max(time.Second, full/4), "phase %q ran on after cancellation", phase)
		})
	}
}
