package replication

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestStorageAdapterClosePersistsAlreadyAcceptedQueue(t *testing.T) {
	fixture := setupTestAdapter(t)
	a := fixture.adapter
	a.walMu.Lock()
	var unlockOnce sync.Once
	unlock := func() { unlockOnce.Do(a.walMu.Unlock) }
	t.Cleanup(unlock)
	const accepted = 129
	for i := 0; i < accepted; i++ {
		node := &storage.Node{ID: storage.NodeID(fmt.Sprintf("close-backlog-%d", i)), Labels: []string{"CloseBacklog"}}
		err := applyQueued(t, a, &Command{Type: CmdCreateNode, Data: fixtureowner.Encode(t, storage.EncodeNodePayload, node), Timestamp: time.Now()})
		require.NoError(t, err, "public native ApplyCommand must have completed before Close")
	}
	// The real writer has removed a batch and is held at the existing physical
	// WAL mutex; additional accepted requests remain in its real buffered queue.
	require.Eventually(t, func() bool {
		a.walBatchMu.Lock()
		defer a.walBatchMu.Unlock()
		return len(a.walBatch) == 0 && len(a.walQueue) > 0
	}, 5*time.Second, time.Millisecond)
	pendingAtStop := len(a.walQueue)
	closed := make(chan error, 1)
	go func() { closed <- a.Close() }()
	require.Eventually(t, a.work.Closing, 5*time.Second, time.Millisecond)
	<-a.work.Context().Done()
	unlock()
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("adapter close did not join native WAL writer")
	}
	nativeCount, err := a.engine.NodeCount(t.Context())
	require.NoError(t, err)
	require.Equal(t, int64(accepted), nativeCount)
	raw, err := os.ReadFile(filepath.Join(fixture.walDir, "wal.log"))
	require.NoError(t, err)
	reopened, err := NewStorageAdapterWithWAL(a.engine, fixture.walDir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	entries, err := reopened.GetWALEntries(0, accepted+1)
	require.NoError(t, err)
	position, err := reopened.GetWALPosition()
	require.NoError(t, err)
	t.Logf("native=%d pending_at_stop=%d reopened=%d position=%d wal_bytes=%d", nativeCount, pendingAtStop, len(entries), position, len(raw))
	require.Len(t, entries, accepted, "graceful Close must retain accepted commands for real WAL restart/streaming")
	require.Equal(t, uint64(accepted), position)
	require.Empty(t, a.walQueue, "graceful Close must consume already-accepted replication records")
}
