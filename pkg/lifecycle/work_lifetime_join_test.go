package lifecycle

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWorkLifetimeJoinStoppedWaitsForAcceptedWorkAndRetirement(t *testing.T) {
	var w WorkLifetime
	require.True(t, w.Enter())
	var leave sync.Once
	finishWork := func() { leave.Do(w.Leave) }
	t.Cleanup(finishWork)
	disposed, release := make(chan struct{}), make(chan struct{})
	var open sync.Once
	finishDispose := func() { open.Do(func() { close(release) }) }
	t.Cleanup(finishDispose)
	cause := errors.New("actual resource disposal")
	var calls atomic.Int64
	joined := make(chan error, 1)
	go func() { joined <- w.JoinStopped() }()
	require.False(t, w.Closing(), "joining never starts shutdown")
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, w.WaitStopped(canceled), context.Canceled, "bounded wait is still independent")
	w.BeginStop(func() error { calls.Add(1); close(disposed); <-release; return cause })
	require.ErrorIs(t, w.Context().Err(), context.Canceled)
	select {
	case err := <-joined:
		t.Fatalf("join abandoned accepted work: %v", err)
	default:
	}
	select {
	case <-disposed:
		t.Fatal("resource disposed while accepted work still held")
	default:
	}
	finishWork()
	onceContractReceive(t, disposed)
	select {
	case err := <-joined:
		t.Fatalf("join abandoned actual resource disposal: %v", err)
	default:
	}
	finishDispose()
	require.ErrorIs(t, onceContractReceive(t, joined), cause)
	require.ErrorIs(t, w.JoinStopped(), cause)
	require.ErrorIs(t, w.WaitStopped(t.Context()), cause)
	w.BeginStop(func() error { t.Fatal("retirement replayed"); return nil })
	require.ErrorIs(t, w.JoinStopped(), cause)
	require.Equal(t, int64(1), calls.Load())
}

func TestWorkLifetimeUnboundedJoinAllocations(t *testing.T) {
	legacy := func(w *WorkLifetime) error { return w.WaitStopped(context.Background()) }
	owned := func(w *WorkLifetime) error { return w.JoinStopped() }
	tests := []struct {
		name    string
		measure func(func(*WorkLifetime) error) float64
	}{
		{"warm", func(join func(*WorkLifetime) error) float64 {
			var w WorkLifetime
			w.BeginStop(nil)
			_ = w.JoinStopped()
			return testing.AllocsPerRun(200, func() { _ = join(&w) })
		}},
		{"empty stop", func(join func(*WorkLifetime) error) float64 {
			return testing.AllocsPerRun(200, func() { var w WorkLifetime; w.BeginStop(nil); _ = join(&w) })
		}},
		{"accepted work and disposal", func(join func(*WorkLifetime) error) float64 {
			return testing.AllocsPerRun(200, func() {
				var w WorkLifetime
				w.Enter()
				w.BeginStop(func() error { return nil })
				w.Leave()
				_ = join(&w)
			})
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			before, after := test.measure(legacy), test.measure(owned)
			t.Logf("legacy=%.0f owned=%.0f allocs/op", before, after)
			require.Equal(t, before, after, "unbounded owner join must not add allocation")
		})
	}
	t.Logf("public Close allocs/op=%.0f", testing.AllocsPerRun(200, func() { var w WorkLifetime; _ = w.Close() }))
}
