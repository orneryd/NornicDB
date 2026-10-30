package txsession

import (
	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

func TestManagerR4JoinsArmedExpiryCallback(t *testing.T) {
	engine := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	manager := NewManager(10*time.Millisecond, func(string) (*cypher.StorageExecutor, error) { return cypher.NewStorageExecutor(engine), nil })
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	// This constructor dependency still uses the real timer. The prefix gate
	// observes a fired callback before the managed callback can enter its locks.
	// The identical gate is overlaid on the old product without ownership fixes.
	manager.armAfterFunc = func(delay time.Duration, callback func()) *time.Timer {
		return time.AfterFunc(delay, func() { close(entered); <-release; callback() })
	}
	_, err := manager.OpenForOwner(context.Background(), "nornic", "r4")
	require.NoError(t, err)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("real expiry timer did not fire")
	}
	require.NoError(t, manager.CloseContext(context.Background()))
	waitCtx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, manager.WaitClosed(waitCtx), context.DeadlineExceeded, "the owner reported completion while its armed callback was still executing")
	unblock()
	finalCtx, finalCancel := context.WithTimeout(context.Background(), time.Second)
	defer finalCancel()
	require.NoError(t, manager.WaitClosed(finalCtx))
}
