package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/internal/queryprofile"
	"github.com/stretchr/testify/require"
)

func TestProfileObservationOnLifetimeFreezesAttributionWithoutRequestValues(t *testing.T) {
	type requestOnlyKey struct{}
	collector := newProfileRuntimeCollector()
	source, cancelSource := context.WithCancel(context.WithValue(withProfileRuntimeCollector(t.Context(), collector), requestOnlyKey{}, "request-only"))
	defer cancelSource()
	source, invocation := profileRuntimeInvocation(source, "ProcedureCall", "owned-refresh", 0)
	lifetime, cancelLifetime := context.WithCancel(t.Context())
	defer cancelLifetime()
	owned := queryprofile.OnLifetime(lifetime, source)
	require.Nil(t, owned.Value(requestOnlyKey{}))
	require.Nil(t, owned.Value(profileRuntimeFrameKey{}), "a frame pointer is not transferred into storage work")
	cancelSource()
	require.NoError(t, owned.Err())
	queryprofile.Access(owned, "ObservedRead", "owned").Add(1, 1)
	queryprofile.Callback(owned, "owned callback", "")
	nextLifetime, cancelNext := context.WithCancel(t.Context())
	defer cancelNext()
	rebound := queryprofile.OnLifetime(nextLifetime, owned)
	require.Nil(t, rebound.Value(requestOnlyKey{}))
	queryprofile.Access(rebound, "ObservedRead", "rebound").Add(1, 1)
	observations := collector.accessObservation()
	require.Len(t, observations, 3)
	for _, read := range observations {
		require.Equal(t, invocation.id, read.Parent)
	}
	cancelLifetime()
	require.ErrorIs(t, owned.Err(), context.Canceled)
	require.NoError(t, rebound.Err(), "the new lifetime owns transferred work")
	cancelNext()
	require.ErrorIs(t, rebound.Err(), context.Canceled)
}

func TestObservationLifetimeTransferWithoutObserverAllocatesNothing(t *testing.T) {
	lifetime, source := t.Context(), t.Context()
	allocations := testing.AllocsPerRun(100, func() {
		if queryprofile.OnLifetime(lifetime, source) != lifetime {
			t.Fatal("ordinary observation transfer changed lifetime")
		}
	})
	require.Zero(t, allocations)
}
