package linkpredict

import (
	"bytes"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestDecodeGraphCacheValidatesCollectionLengths(t *testing.T) {
	for _, tc := range []struct {
		name string
		data []byte
	}{
		{"outer map", []byte{0xdf, 0xff, 0xff, 0xff, 0xff}},
		{"graph map", []byte{0x81, 0xa5, 'g', 'r', 'a', 'p', 'h', 0xdf, 0xff, 0xff, 0xff, 0xff}},
		{"neighbor array", []byte{0x81, 0xa5, 'g', 'r', 'a', 'p', 'h', 0x81, 0xa1, 'n', 0xdd, 0xff, 0xff, 0xff, 0xff}},
		{"truncated neighbor", []byte{0x81, 0xa5, 'g', 'r', 'a', 'p', 'h', 0x81, 0xa1, 'n', 0x91}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := decodeGraphCache(msgpack.NewDecoder(bytes.NewReader(tc.data)), int64(len(tc.data)))
			require.Error(t, err)
		})
	}
	stamp := time.Unix(1700000000, 123).UTC()
	encoded, err := msgpack.Marshal(CachedGraph{Graph: map[string][]string{"n": {"a", "b"}, "empty": {}}, Timestamp: stamp, NodeCount: 2, EdgeCount: 2})
	require.NoError(t, err)
	graph, restored, err := decodeGraphCache(msgpack.NewDecoder(bytes.NewReader(encoded)), int64(len(encoded)))
	require.NoError(t, err)
	require.True(t, stamp.Equal(restored))
	require.Equal(t, Graph{
		"n":     NodeSet{storage.NodeID("a"): {}, storage.NodeID("b"): {}},
		"empty": NodeSet{},
	}, graph)
}
