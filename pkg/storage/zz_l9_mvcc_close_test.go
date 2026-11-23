package storage

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestL9MVCCPersistCloseOwner(t *testing.T) {
	t.Run("persist_before_close", func(t *testing.T) {
		engine, err := NewBadgerEngine(t.TempDir())
		require.NoError(t, err)
		require.NoError(t, engine.EnsureNamespaceMVCC("l9"))
		state, err := engine.namespaceMVCC("l9")
		require.NoError(t, err)
		floor := state.seq.Add(1)
		require.NoError(t, engine.ensureMVCCSequenceBlock(context.Background(), state, floor))
		require.NoError(t, engine.Close())
	})
	t.Run("persist_at_close", func(t *testing.T) {
		engine, err := NewBadgerEngine(t.TempDir())
		require.NoError(t, err)
		require.NoError(t, engine.EnsureNamespaceMVCC("l9"))
		state, err := engine.namespaceMVCC("l9")
		require.NoError(t, err)
		start := make(chan struct{})
		var ready, done sync.WaitGroup
		for range 8 {
			ready.Add(1)
			done.Add(1)
			go func() {
				defer done.Done()
				ready.Done()
				<-start
				for range 512 {
					floor := state.seq.Add(1)
					_ = engine.ensureMVCCSequenceBlock(context.Background(), state, floor)
				}
			}()
		}
		ready.Wait()
		close(start)
		closeErr := engine.Close()
		done.Wait()
		require.NoError(t, closeErr)
	})
}
