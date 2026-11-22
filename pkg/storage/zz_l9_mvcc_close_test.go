package storage

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestL9MVCCPersistCloseOwner(t *testing.T) {
	t.Run("namespace_state_before_close", func(t *testing.T) {
		engine, err := NewBadgerEngine(t.TempDir())
		require.NoError(t, err)
		_, err = engine.namespaceMVCC("l9")
		require.NoError(t, err)
		require.NoError(t, engine.Close())
	})
	t.Run("namespace_state_at_close", func(t *testing.T) {
		engine, err := NewBadgerEngine(t.TempDir())
		require.NoError(t, err)
		require.NoError(t, engine.EnsureNamespaceMVCC("l9"))
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
					_, _ = engine.namespaceMVCC("l9")
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
