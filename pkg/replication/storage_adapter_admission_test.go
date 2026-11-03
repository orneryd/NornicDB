package replication

import (
	"context"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Run the callback contract in a child so an accidentally reintroduced lock
// across listener delivery fails boundedly without leaking a hung native owner.
func TestStorageAdapterNativeListenerCanCloseWALOwner(t *testing.T) {
	const childMode = "NORNIC_ADAPTER_LISTENER_CLOSE_TEST"
	if mode := os.Getenv(childMode); mode != "" {
		f := setupTestAdapter(t, adapterFixtureOptions{standalone: mode == "leader"})
		a := f.adapter
		require.NoError(t, storage.SubscribeEvents(a.engine, storage.EventHandlers{NodeCreated: func(*storage.Node) { require.NoError(t, a.Close()) }}))
		node := &storage.Node{ID: "test:listener-close"}
		if mode == "leader" {
			_, err := f.engine.CreateNode(t.Context(), node)
			require.NoError(t, err)
		} else {
			require.NoError(t, applyQueued(t, a, &Command{Type: CmdCreateNode, Data: fixtureowner.Encode(t, storage.EncodeNodePayload, node), Timestamp: time.Now()}))
		}
		require.NoError(t, a.Close())
		require.ErrorIs(t, applyQueued(t, a, &Command{Type: CmdCreateNode, Data: fixtureowner.Encode(t, storage.EncodeNodePayload, &storage.Node{ID: "test:after-close"}), Timestamp: time.Now()}), ErrClosed)
		require.ErrorIs(t, a.FlushWAL(), ErrClosed)
		count, err := a.engine.NodeCount(t.Context())
		require.NoError(t, err)
		require.Equal(t, int64(1), count)
		return
	}
	for _, mode := range []string{"raw-state-machine", "leader"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestStorageAdapterNativeListenerCanCloseWALOwner$", "-test.v")
			child.Env = append(os.Environ(), childMode+"="+mode, "TMPDIR="+t.TempDir())
			output, err := child.CombinedOutput()
			require.NoError(t, ctx.Err(), "native callback Close blocked: %s", output)
			require.NoError(t, err, "%s", output)
		})
	}
}

func TestStorageAdapterCloseJoinsOwnedBarrierIO(t *testing.T) {
	a := setupTestAdapter(t).adapter
	a.walMu.Lock()
	var once sync.Once
	unlock := func() { once.Do(a.walMu.Unlock) }
	t.Cleanup(unlock)
	require.NoError(t, applyQueued(t, a, &Command{Type: CmdCreateNode, Data: fixtureowner.Encode(t, storage.EncodeNodePayload, &storage.Node{ID: "barrier-close"}), Timestamp: time.Now()}))
	flushed, closed := make(chan error, 1), make(chan error, 1)
	go func() { flushed <- a.FlushWAL() }()
	require.Eventually(t, func() bool {
		if a.calls.TryLock() {
			a.calls.Unlock()
			return false
		}
		return true
	}, 5*time.Second, time.Millisecond, "barrier must hold its actual admission while physical I/O is blocked")
	go func() { closed <- a.Close() }()
	select {
	case err := <-closed:
		t.Errorf("Close retired admitted WAL barrier before I/O completed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	unlock()
	require.NoError(t, <-flushed)
	require.NoError(t, <-closed)
	pos, err := a.GetWALPosition()
	require.NoError(t, err)
	require.Equal(t, uint64(1), pos)
	for range 3 {
		require.NoError(t, a.Close())
	}
}
