package storage

import (
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// Author: #1548 implementation lane (Codex). Trigger the real cleanup worker
// through a public delete, then observe its throw-away marker being dropped.
func TestResidualCleanupKeepsUnrelatedCacheWarm(t *testing.T) {
	b, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = b.Close() })
	for _, id := range []NodeID{"test:keep-cleanup", "test:delete-cleanup"} {
		_, err := b.CreateNode(&Node{ID: id})
		require.NoError(t, err)
	}
	_, err = b.GetStoredNode("test:keep-cleanup")
	require.NoError(t, err)
	b.nodeCacheMu.RLock()
	before := b.nodeCache["test:keep-cleanup"]
	b.nodeCacheMu.RUnlock()
	require.NotNil(t, before)
	// The worker is waiting for its first wake. Sending that wake publishes
	// these test settings before the worker reads them.
	b.deleteCleanup.threshold, b.deleteCleanup.quiet = 1, 0
	seenMarker := false
	dropped := make(chan struct{})
	var once sync.Once
	t.Cleanup(setDurableCacheHook(func(uint64) {
		read := b.db.DB.NewTransactionAt(b.db.DB.MaxVersion(), false)
		_, err := read.Get(deleteCleanupMarkerKey)
		read.Discard()
		if err == nil {
			seenMarker = true
		} else if err == badger.ErrKeyNotFound && seenMarker {
			once.Do(func() { close(dropped) })
		}
	}))
	require.NoError(t, b.DeleteNode("test:delete-cleanup"))
	select {
	case <-dropped:
	case <-time.After(3 * time.Second):
		t.Fatal("delete cleanup did not drop its durable marker")
	}
	b.nodeCacheMu.RLock()
	after := b.nodeCache["test:keep-cleanup"]
	b.nodeCacheMu.RUnlock()
	require.Same(t, before, after, "throw-away cleanup metadata cannot revoke an unrelated body")
}
