package replication

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// This component admission/disposal gate holds an owner ticket against a real
// WAL. It does not simulate ApplyCommand or an actual queued writer request.
func TestStorageAdapterCloseJoinsAcceptedWorkBeforeWALRetirement(t *testing.T) {
	adapter := setupTestAdapter(t).adapter
	require.True(t, adapter.work.Enter())
	var once sync.Once
	release := func() { once.Do(adapter.work.Leave) }
	t.Cleanup(release)
	cause := errors.New("accepted adapter work outcome")
	closed := make(chan error, 2)
	go func() { closed <- adapter.Close() }()
	require.Eventually(t, adapter.work.Closing, time.Second, time.Millisecond)
	go func() { closed <- adapter.Close() }()
	select {
	case err := <-closed:
		t.Fatalf("Close abandoned accepted work: %v", err)
	default:
	}
	adapter.walMu.RLock()
	walLive := adapter.wal != nil
	adapter.walMu.RUnlock()
	require.True(t, walLive, "physical WAL stays live while its accepted user remains")
	adapter.work.RecordError("", cause)
	release()
	for i := 0; i < 2; i++ {
		select {
		case err := <-closed:
			require.ErrorIs(t, err, cause)
		case <-time.After(3 * time.Second):
			t.Fatal("Close did not join accepted work")
		}
	}
	adapter.walMu.RLock()
	walRetired := adapter.wal == nil
	adapter.walMu.RUnlock()
	require.True(t, walRetired, "physical WAL retired before Close reports completion")
	require.ErrorIs(t, adapter.Close(), cause, "repeated close keeps actual accepted-work outcome")
}
