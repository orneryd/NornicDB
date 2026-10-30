package replication

import (
	"context"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// nestedNodePayload builds the current node-image MessagePack framing directly
// so the receiver is tested independently of the sender's depth validation.
func nestedNodePayload(depth int) []byte {
	data := []byte{0x82, 0xa2, 'I', 'D', 0xa6, 't', 'e', 's', 't', ':', 'n', 0xaa, 'P', 'r', 'o', 'p', 'e', 'r', 't', 'i', 'e', 's', 0x81, 0xa1, 'x'}
	for range depth {
		data = append(data, 0x92, 0x18, 0x91) // [list tag, one-element list]
	}
	return append(data, 0x92, 0x00, 0xc0) // [nil tag, nil]
}

func TestReplicationRejectsPathologicalPropertyNesting(t *testing.T) {
	engine := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	adapter, err := NewStorageAdapterWithWAL(engine, t.TempDir())
	require.NoError(t, err)
	fixtureowner.OwnChecked(t, adapter)
	err = adapter.ApplyCommand(context.Background(), &Command{Type: CmdCreateNode, Data: nestedNodePayload(1025)})
	require.ErrorContains(t, err, "nesting depth")
	_, err = engine.GetNode("test:n")
	require.ErrorIs(t, err, storage.ErrNotFound)
	require.NoError(t, adapter.FlushWAL())
	entries, err := adapter.GetWALEntries(0, 10)
	require.NoError(t, err)
	require.Empty(t, entries, "rejected input must not be replayed to another replica")

	value := any(nil)
	for range 1025 {
		value = []any{value}
	}
	_, err = storage.EncodeNodePayload(&storage.Node{ID: "test:n", Properties: map[string]any{"x": value}})
	require.ErrorContains(t, err, "nesting depth")
}

func TestReplicationAcceptsPropertyNestingAtLimit(t *testing.T) {
	decoded, err := storage.DecodeNodePayload(nestedNodePayload(1024))
	require.NoError(t, err)
	require.Equal(t, storage.NodeID("test:n"), decoded.ID)
	value := any(nil)
	for range 1024 {
		value = []any{value}
	}
	data, err := storage.EncodeNodePayload(&storage.Node{ID: "test:n", Properties: map[string]any{"x": value}})
	require.NoError(t, err)
	_, err = storage.DecodeNodePayload(data)
	require.NoError(t, err)
}
