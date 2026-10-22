package memory

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWireRequestScopeOwnershipAndActualSize(t *testing.T) {
	policy := NewPolicy(1<<20, 0, time.Minute, func() (Sample, error) { return Sample{Used: 1}, nil }, time.Now)
	require.NoError(t, policy.Refresh())
	wire := NewWireRequest(policy)
	want := errors.New("consumer failed")
	err := wire.Run(func() error {
		require.NoError(t, wire.Grow(64<<10, 64<<10))
		require.EqualValues(t, 512<<10, policy.Reserved())
		return want
	})
	require.ErrorIs(t, err, want)
	require.Zero(t, policy.Reserved())
	wire.Close()
	policy.mu.Lock()
	released := policy.recovery.released
	policy.mu.Unlock()
	require.EqualValues(t, 1, released, "cleanup releases the underlying lease once")
	wire = NewWireRequest(policy)
	require.NoError(t, wire.Grow(1))
	queued := wire.Move()
	wire.Close()
	require.EqualValues(t, 64<<10, policy.Reserved(), "queue transfer retains its credit")
	require.Panics(t, func() { _ = queued.Run(func() error { panic("decoder") }) })
	require.Zero(t, policy.Reserved())
	lease, err := policy.ReserveWireRequest(300<<10, 16<<10)
	lease.Release()
	require.ErrorIs(t, err, ErrLimitExceeded, "known wire bytes must not be clamped to an unrelated cap")
	require.EqualValues(t, 1<<63-1, WireSize(1<<63-1, 1))
}

func TestWireRequestRetainedWorkerOwnsUntilReturn(t *testing.T) {
	for _, panicWorker := range []bool{false, true} {
		policy := NewPolicy(1<<20, 0, time.Minute, func() (Sample, error) { return Sample{Used: 1}, nil }, time.Now)
		require.NoError(t, policy.Refresh())
		wire := NewWireRequest(policy)
		require.NoError(t, wire.Grow(1))
		worker := wire.RetainHandler(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			require.NoError(t, wire.Grow(2))
			if panicWorker {
				panic("decoder")
			}
		}))
		wire.Close()
		wire.Close()
		require.Positive(t, policy.Reserved(), "retain happens before worker scheduling")
		run := func() { worker.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil)) }
		if panicWorker {
			require.Panics(t, run)
		} else {
			run()
		}
		require.Zero(t, policy.Reserved())
		wire.Close()
		policy.mu.Lock()
		released := policy.recovery.released
		policy.mu.Unlock()
		require.EqualValues(t, 1, released)
	}
}
