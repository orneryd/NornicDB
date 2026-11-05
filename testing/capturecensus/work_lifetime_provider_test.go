package capturecensus

import (
	"context"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/lifecycle"
	"github.com/stretchr/testify/require"
)

// This checks the real provider's non-nil result contract. It does not approve
// owner-context substitution, direct reads of arbitrary ctx fields, or the
// nullable result contracts of beginServe/beginRead wrappers.
func TestWorkLifetimeContextProviderContract(t *testing.T) {
	for _, constructed := range []bool{false, true} {
		t.Run(map[bool]string{false: "zero value", true: "constructor"}[constructed], func(t *testing.T) {
			var zero lifecycle.WorkLifetime
			work := &zero
			parent, cancel := context.WithCancel(t.Context())
			defer cancel()
			if constructed {
				work = lifecycle.NewWorkLifetime(parent)
			}
			t.Cleanup(func() { require.NoError(t, work.Close()) })
			const readers = 8
			contexts := make(chan context.Context, readers)
			var joined sync.WaitGroup
			for i := 0; i < readers; i++ {
				joined.Add(1)
				go func() { defer joined.Done(); contexts <- work.Context() }()
			}
			joined.Wait()
			close(contexts)
			actual := work.Context()
			require.NotNil(t, actual)
			for got := range contexts {
				require.Equal(t, actual, got, "all concurrent initializers observe the same owned context")
			}
			require.NoError(t, actual.Err())
			if constructed {
				cancel()
				require.ErrorIs(t, actual.Err(), context.Canceled)
			}
			work.Cancel()
			require.NotNil(t, work.Context())
			require.ErrorIs(t, work.Context().Err(), context.Canceled)
			work.CloseAdmission()
			request := t.Context()
			rejected, release, accepted := work.Acquire(request)
			require.False(t, accepted)
			require.Equal(t, request, rejected, "rejected Acquire retains its non-nil input; nullable wrappers require separate result rules")
			release()
			require.NoError(t, work.Close())
			require.NotNil(t, work.Context())
			require.ErrorIs(t, work.Context().Err(), context.Canceled)
		})
	}
	require.Panics(t, func() { lifecycle.NewWorkLifetime(nil) }, "a nil parent cannot create a normally returning provider")
	var work lifecycle.WorkLifetime
	t.Cleanup(func() { require.NoError(t, work.Close()) })
	require.Panics(t, func() { work.Acquire(nil) }, "Acquire rejects nil input even when the receiver is zero-valued")
}
