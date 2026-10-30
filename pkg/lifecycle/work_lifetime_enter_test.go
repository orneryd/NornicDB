package lifecycle

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// Enter is the owner's allocation-free root admission: Shutdown's dispose
// waits for every entered unit, and admission after the seal is refused.
func TestWorkLifetimeEnterJoinsShutdownWithoutAllocating(t *testing.T) {
	var w WorkLifetime
	require.True(t, w.Enter())

	disposed := make(chan struct{})
	stopped := make(chan error, 1)
	go func() {
		stopped <- w.Shutdown(context.Background(), func() error {
			close(disposed)
			return nil
		})
	}()
	<-w.Context().Done() // the stop has sealed admission and signalled
	require.False(t, w.Enter(), "admission after the seal must be refused")
	select {
	case <-disposed:
		t.Fatal("dispose ran while an entered unit was still active")
	default:
	}
	w.Leave()
	require.NoError(t, <-stopped)
	<-disposed

	var open WorkLifetime
	open.init()
	allocs := testing.AllocsPerRun(1000, func() {
		if open.Enter() {
			open.Leave()
		}
	})
	require.Zero(t, allocs)
}

func TestWorkLifetimeLeaveWithoutEnterPanics(t *testing.T) {
	var w WorkLifetime
	w.init()
	require.PanicsWithValue(t, "lifecycle: WorkLifetime.Leave without Enter", w.Leave)
}
