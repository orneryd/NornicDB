package replication

import (
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestStorageAdapterCloseRetainsAcceptedWALError(t *testing.T) {
	a := setupTestAdapter(t).adapter
	require.NoError(t, a.wal.Close()) // real closed native WAL, no injected error callback
	command := &Command{Type: CmdCreateNode, Data: fixtureowner.Encode(t, storage.EncodeNodePayload, &storage.Node{ID: "closed-wal", Labels: []string{"WALFault"}}), Timestamp: time.Now()}
	require.NoError(t, applyQueued(t, a, command), "existing async apply result remains native graph result")
	require.ErrorIs(t, a.FlushWAL(), storage.ErrWALClosed)
	results := make(chan error, 4)
	for range 4 {
		go func() { results <- a.Close() }()
	}
	for range 4 {
		select {
		case err := <-results:
			require.ErrorIs(t, err, storage.ErrWALClosed, "every close must retain accepted native WAL failure")
		case <-time.After(5 * time.Second):
			t.Fatal("concurrent close did not join")
		}
	}
	require.ErrorIs(t, a.Close(), storage.ErrWALClosed, "repeated close must retain same native outcome")
}
