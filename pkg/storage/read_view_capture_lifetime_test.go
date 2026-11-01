package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A capture callback that panics is a defect, not a result: the panic must
// unwind unchanged, and the read transaction, read timestamp and engine close
// barrier it was using must be released before it does. Before this test, a
// panicking capture left the view open, so Engine.Close waited forever on a
// read whose stack was gone (seen in the storage suite as a hung Close after a
// panicking overlay copy).
func TestCaptureReadViewPanicReleasesReadView(t *testing.T) {
	engine := createTestBadgerEngine(t)

	require.PanicsWithValue(t, "capture panic", func() {
		_, _ = engine.captureReadView(func(*WriteBehindBuffer) { panic("capture panic") })
	})

	closed := make(chan error, 1)
	go func() { closed <- engine.Close() }()
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Engine.Close did not return: the panicking capture leaked its read view")
	}
}

// WriteBehindBuffer's own overlay copy must observe the read's cancellation.
// Checking once when the copy starts and then materialising an unbounded
// buffered population leaves a canceled read doing work nobody will consume,
// so every candidate and deletion loop the copy runs polls the read's context.
func TestBufferedOverlayCopyReportsCallerCancellation(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour)
	const buffered = 600
	for i := 0; i < buffered; i++ {
		bufferedCreate(t, engine, &Node{
			ID:         NodeID(prefixTestID(fmt.Sprintf("cancel-copy-%d", i))),
			Labels:     []string{"CancelCopy"},
			Properties: map[string]any{"v": int64(i)},
		})
	}
	w := engine.writeBehind
	require.NotNil(t, w)

	// A live context copies the whole overlay, so the canceled run below stops
	// the same copy rather than an empty one.
	w.mu.RLock()
	whole, wholeTouched, err := w.labelOverlayLocked(context.Background(), "CancelCopy")
	w.mu.RUnlock()
	require.NoError(t, err)
	require.Len(t, whole, buffered)
	require.Len(t, wholeTouched, buffered)

	// Actual mid-enumeration cancellation is asserted by the candidate visitor
	// regression below. These public copy owners must propagate a stopped caller
	// without publishing a partial result.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	w.mu.RLock()
	nodes, touched, err := w.labelOverlayLocked(ctx, "CancelCopy")
	w.mu.RUnlock()
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, nodes)
	require.Nil(t, touched)
	w.mu.RLock()
	canceled, err := engine.readBufferedEntities(ctx, w, storedRead, bufferedEntityRead{kind: bufferedLabel, label: "CancelCopy"})
	w.mu.RUnlock()
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, canceled.nodes)

}

// Retirement includes candidate enumeration, not just suppressing the visit
// callback while still walking the rest of a buffered generation.
func TestBufferedOverlayCancellationStopsCandidateEnumeration(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour)
	const total = 2 * bulkReadCancellationInterval
	for i := 0; i < total; i++ {
		bufferedCreate(t, engine, &Node{ID: NodeID(prefixTestID(fmt.Sprintf("retire-%d", i)))})
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	candidates, delivered := 0, 0
	w := engine.writeBehind
	w.mu.RLock()
	err := w.visitEntityOverlayLocked(ctx, bufferedNodeEntity, func(b *CommitBuffer, emit func(string) bool) {
		for id := range b.nodes {
			candidates++
			if !emit(string(id)) {
				return
			}
		}
	}, func(_ string, _ bufferedEntityState) {
		delivered++
		if delivered == 1 {
			cancel()
		}
	})
	w.mu.RUnlock()
	require.ErrorIs(t, err, context.Canceled)
	require.Positive(t, delivered)
	require.LessOrEqual(t, candidates, bulkReadCancellationInterval+1, "canceled enumeration must retire, not merely suppress delivery")
}
