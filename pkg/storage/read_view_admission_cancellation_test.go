package storage

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestOpenReadSnapshotCanceledRetirementAdmission(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour)
	engine.writeBehind.Close()
	w := engine.writeBehind
	ctx, cancel := context.WithCancel(t.Context())
	waited := make(chan context.Context, 1)
	t.Cleanup(setAdmissionWaitObserver(func(owner any, waitCtx context.Context) {
		if owner == &w.mu {
			select {
			case waited <- waitCtx:
			default:
			}
		}
	}))
	w.mu.Lock()
	var unlocked sync.Once
	unlock := func() { unlocked.Do(w.mu.Unlock) }
	done := make(chan error, 1)
	workerJoined := make(chan struct{})
	// Register both the unlock and actual goroutine join before launching it.
	t.Cleanup(func() {
		cancel()
		unlock()
		select {
		case <-workerJoined:
		case <-time.After(5 * time.Second):
			t.Error("snapshot admission worker did not join after lock release")
		}
	})
	go func() {
		defer close(workerJoined)
		view, err := engine.OpenReadSnapshot(ctx)
		if view != nil {
			view.Close()
		}
		done <- err
	}()
	var admission context.Context
	select {
	case admission = <-waited:
	case <-time.After(5 * time.Second):
		t.Fatal("snapshot did not reach held write-behind retirement admission")
	}
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Error("canceled snapshot admission stayed blocked until retirement lock release")
		unlock()
		select {
		case err := <-done:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(5 * time.Second):
			t.Fatal("snapshot did not report completion after lock release")
		}
	}
	select {
	case <-workerJoined:
	case <-time.After(5 * time.Second):
		t.Fatal("snapshot worker did not join after reporting its result")
	}
	require.True(t, admission == ctx, "retirement admission discarded the public caller context")
	unlock()
	healthy, err := engine.OpenReadSnapshot(t.Context())
	require.NoError(t, err)
	healthy.Close()
}

func TestCaptureReadViewPreCanceledAdmission(t *testing.T) {
	for _, cached := range []bool{false, true} {
		name := "strict"
		if cached {
			name = "cached"
		}
		t.Run(name, func(t *testing.T) {
			engine := newTestEngine(t)
			require.Nil(t, engine.writeBehind)
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			called := false
			opts := readViewOptions{}
			if cached {
				opts.cached = func(*WriteBehindBuffer) (bool, error) { called = true; return true, nil }
			}
			view, err := engine.captureReadView(ctx, func(*WriteBehindBuffer) { called = true }, opts)
			if view != nil {
				view.Close()
			}
			require.ErrorIs(t, err, context.Canceled)
			require.Nil(t, view)
			require.False(t, called, "canceled admission ran read callbacks")
		})
	}
}

func TestCaptureReadViewCacheCancellationAndDetachedControl(t *testing.T) {
	engine := newTestEngine(t)
	for _, complete := range []bool{false, true} {
		ctx, cancel := context.WithCancel(t.Context())
		captured := false
		view, err := engine.captureReadView(ctx, func(*WriteBehindBuffer) { captured = true }, readViewOptions{cached: func(*WriteBehindBuffer) (bool, error) { cancel(); return complete, nil }})
		if view != nil {
			view.Close()
		}
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, view)
		require.False(t, captured)
	}
	opts := readViewOptions{cached: func(*WriteBehindBuffer) (bool, error) { return true, nil }}
	for _, ctx := range []context.Context{context.Background(), t.Context()} {
		allocations := testing.AllocsPerRun(100, func() {
			cached, err := engine.captureReadView(ctx, nil, opts)
			if err != nil || cached != nil {
				panic("healthy cache admission failed")
			}
		})
		require.Zero(t, allocations, "completed cache admission allocated")
	}
}

func TestCaptureReadViewBusyCacheCancellation(t *testing.T) {
	engine := snapshotOverlayFixture(t)
	_, idle := engine.writeBehind.idleReadEpoch("")
	require.False(t, idle)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	called := false
	view, err := engine.captureReadView(ctx, nil, readViewOptions{cached: func(w *WriteBehindBuffer) (bool, error) {
		called = true
		require.Same(t, engine.writeBehind, w)
		cancel()
		return true, nil
	}})
	if view != nil {
		view.Close()
	}
	require.True(t, called)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, view)
	healthy, err := engine.OpenReadSnapshot(t.Context())
	require.NoError(t, err)
	healthy.Close()
}
