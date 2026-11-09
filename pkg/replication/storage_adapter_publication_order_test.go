package replication

import (
	"fmt"
	"sync"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestStorageAdapterOverflowKeepsPublishedPositionsOrdered(t *testing.T) {
	a := setupTestAdapter(t).adapter
	command := func(i int) *Command {
		return &Command{Type: CmdCreateNode, Data: fixtureowner.Encode(t, storage.EncodeNodePayload, &storage.Node{ID: storage.NodeID(fmt.Sprintf("publication-%d", i))}), Timestamp: time.Now()}
	}
	a.memWALMu.Lock()
	var once sync.Once
	unlock := func() { once.Do(a.memWALMu.Unlock) }
	t.Cleanup(unlock)
	for i := 0; i < 100; i++ {
		require.NoError(t, applyQueued(t, a, command(i)))
	}
	require.Eventually(t, func() bool { return a.wal.Sequence() > 0 }, 5*time.Second, time.Millisecond, "first real native batch must reach disk before memory publication")
	free := cap(a.walQueue) - len(a.walQueue)
	for i := 100; i < 100+free; i++ {
		require.NoError(t, applyQueued(t, a, command(i)))
	}
	require.Equal(t, cap(a.walQueue), len(a.walQueue))
	overflow := make(chan error, 1)
	go func() { overflow <- applyQueued(t, a, command(100+free)) }()
	// Neither path may finish while its streaming publication is blocked.
	select {
	case err := <-overflow:
		t.Fatalf("overflow returned before publication was released: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	unlock()
	select {
	case err := <-overflow:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("overflow apply did not join")
	}
	require.NoError(t, a.FlushWAL())
	a.memWALMu.RLock()
	first := a.memWAL[0].Position
	a.memWALMu.RUnlock()
	entries, err := a.GetWALEntries(first, 2000)
	require.NoError(t, err)
	require.Len(t, entries, 100+free, "every accepted command after first cursor must occur once")
	prior := first
	for _, entry := range entries {
		if entry.Position <= prior {
			t.Fatalf("public streaming positions decreased: %d after %d (first cursor%d)", entry.Position, prior, first)
		}
		prior = entry.Position
	}
}
