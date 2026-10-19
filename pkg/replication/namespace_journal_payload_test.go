package replication

import (
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Committed log entries may be serialized by heartbeat/streaming readers while
// the state machine applies them. Application must consume the admitted bytes.
func TestNamespaceCommittedJournalPayloadIsImmutable(t *testing.T) {
	base := storage.NewMemoryEngine()
	defer base.Close()
	_, err := base.CreateNode(&storage.Node{ID: "immutable_a:seed"})
	require.NoError(t, err)
	adapter, err := NewStorageAdapterWithWAL(base, t.TempDir())
	require.NoError(t, err)
	defer adapter.Close()
	data, err := encodeGob(namespaceDropCommand{Prefix: "immutable_a:", Retirement: 1, RevisionAssigned: true})
	require.NoError(t, err)
	cmd := &Command{Type: CmdDeleteByPrefix, Data: data}
	ready, stop := make(chan struct{}), make(chan struct{})
	var reader sync.WaitGroup
	reader.Add(1)
	go func() {
		defer reader.Done()
		first := true
		for {
			select {
			case <-stop:
				return
			default:
			}
			var request namespaceDropCommand
			if err := decodeGob(cmd.Data, &request); err != nil {
				t.Error(err)
			}
			if first {
				close(ready)
				first = false
			}
		}
	}()
	<-ready
	err = applyCommittedStorageCommand(adapter, cmd)
	close(stop)
	reader.Wait()
	require.NoError(t, err)
	require.Equal(t, data, cmd.Data, "admitted command bytes stay immutable during local/replica application")
	_, err = base.GetNode("immutable_a:seed")
	require.ErrorIs(t, err, storage.ErrNotFound)
}
