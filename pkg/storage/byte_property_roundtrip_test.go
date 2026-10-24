package storage

import (
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestBytePropertyRoundTrip(t *testing.T) {
	dir := t.TempDir()
	disk, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(dir))
	if disk != nil {
		fixtureowner.Own(t, disk)
	}
	require.NoError(t, err)
	values := [][]byte{{0, 127, 128, 255}, {}}
	write := func(engine Engine) {
		store := NewNamespacedEngine(engine, "bytes")
		for index, value := range values {
			id := NodeID(string(rune('a' + index)))
			_, err := store.CreateNode(&Node{ID: id, Labels: []string{"ByteProperty"}, Properties: map[string]any{"v": value}})
			require.NoError(t, err)
		}
		require.NoError(t, store.CreateEdge(&Edge{ID: "edge", StartNode: "a", EndNode: "b", Type: "BYTES", Properties: map[string]any{"v": values[0]}}))
	}
	read := func(engine Engine) {
		store := NewNamespacedEngine(engine, "bytes")
		for index, want := range values {
			n, err := store.GetNode(NodeID(string(rune('a' + index))))
			require.NoError(t, err)
			// A nil byte slice has the same empty array content on the wire.
			require.IsType(t, []byte{}, n.Properties["v"])
			require.Equal(t, len(want), len(n.Properties["v"].([]byte)))
			if len(want) > 0 {
				require.Equal(t, want, n.Properties["v"])
			}
		}
		e, err := store.GetEdge("edge")
		require.NoError(t, err)
		require.Equal(t, values[0], e.Properties["v"])
	}
	mem := newTestMemoryEngine(t)
	defer mem.Close()
	write(mem)
	read(mem)
	write(disk)
	read(disk)
	require.NoError(t, disk.Close())
	reopened, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(dir))
	require.NoError(t, err)
	defer reopened.Close()
	read(reopened)
}

func TestBytePropertySerialization(t *testing.T) {
	value := []byte{0, 127, 128, 255}
	node := &Node{ID: "bytes:n", Properties: map[string]any{"v": value}}
	encoded, _, err := encodeNodeV1(node)
	require.NoError(t, err)
	decoded, err := decodeNodeV1(encoded)
	require.NoError(t, err)
	require.Equal(t, value, decoded.Properties["v"])
	edge := &Edge{ID: "bytes:e", StartNode: "bytes:n", EndNode: "bytes:b", Type: "BYTES", Properties: map[string]any{"v": value}}
	encoded, err = encodeEdge(edge)
	require.NoError(t, err)
	decodedEdge, err := decodeEdge(encoded)
	require.NoError(t, err)
	require.Equal(t, value, decodedEdge.Properties["v"])
}
