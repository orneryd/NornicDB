package replication

import (
	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

func TestStorageAdapterAsyncWALPublishesOnceBeforeApplyReturns(t *testing.T) {
	adapter := setupTestAdapter(t).adapter
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	releaseListener := func() { once.Do(func() { close(release) }) }
	require.NoError(t, storage.SubscribeEvents(adapter.engine, storage.EventHandlers{NodeCreated: func(*storage.Node) { close(entered); <-release }}))
	command := &Command{Type: CmdCreateNode, Data: fixtureowner.Encode(t, storage.EncodeNodePayload, &storage.Node{ID: "once", Labels: []string{"Once"}}), Timestamp: time.Now()}
	joined := make(chan struct{})
	var applyErr error
	go func() { defer close(joined); applyErr = adapter.ApplyCommand(context.Background(), command) }()
	t.Cleanup(func() {
		releaseListener()
		select {
		case <-joined:
		case <-time.After(5 * time.Second):
			t.Error("ApplyCommand listener did not join")
		}
	})
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("native event was not reached")
	}
	// This public barrier proves queued position+memory publication finished
	// before the native operation returns to ApplyCommand's status tail.
	require.NoError(t, adapter.FlushWAL())
	releaseListener()
	select {
	case <-joined:
		require.NoError(t, applyErr)
	case <-time.After(5 * time.Second):
		t.Fatal("ApplyCommand did not return")
	}
	adapter.memWALMu.RLock()
	entries := append([]*WALEntry(nil), adapter.memWAL...)
	adapter.memWALMu.RUnlock()
	require.Len(t, entries, 1, "one physical append must have one streamed entry")
	require.Same(t, command, entries[0].Command)
	require.Equal(t, adapter.wal.Sequence(), entries[0].Position)
	records, err := storage.ReadWALEntriesFromDir(adapter.wal.Config().Dir)
	require.NoError(t, err)
	require.Len(t, records, 1)
	node, err := storage.GetStoredNode(adapter.engine, "once")
	require.NoError(t, err)
	require.Equal(t, []string{"Once"}, node.Labels)
}
