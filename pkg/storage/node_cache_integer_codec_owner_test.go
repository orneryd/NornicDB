package storage

import (
	"bytes"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestNodeCacheIntegerCodecOwner(t *testing.T) {
	for name, input := range map[string]any{
		"native": int(3), "positive-boundary": int(128), "negative-boundary": int(-129),
		"int8": int8(3), "int16": int16(3), "int32": int32(3), "int64": int64(3),
		"uint": uint(3), "uint8": uint8(3), "uint16": uint16(3), "uint32": uint32(3), "uint64": uint64(3),
		"max-signed": int64(math.MaxInt64), "max-unsigned": uint64(math.MaxUint64),
		"array": []int{3, 128, -129}, "int32-array": []int32{3, 128, -129},
		"nested": map[string]any{"values": []any{"mixed", int(3), map[string]any{"v": int(128)}}},
	} {
		t.Run(name, func(t *testing.T) {
			encoded, err := msgpack.Marshal(input)
			require.NoError(t, err)
			decoded, err := decodeStrictTypedValue(msgpack.NewDecoder(bytes.NewReader(encoded)))
			require.NoError(t, err)
			var want any = int64(3)
			switch name {
			case "positive-boundary":
				want = int64(128)
			case "negative-boundary":
				want = int64(-129)
			case "max-signed":
				want = int64(math.MaxInt64)
			case "max-unsigned":
				want = uint64(math.MaxUint64)
			case "array", "int32-array":
				want = []int64{3, 128, -129}
			case "nested":
				want = map[string]any{"values": []any{"mixed", int64(3), map[string]any{"v": int64(128)}}}
			}
			require.Equal(t, want, normalizePropertyValueShape(input))
			require.Equal(t, want, decoded, "legacy compact and typed encodings use the cache's canonical property type")
			if name == "native" {
				require.Equal(t, int64(3), decoded)
			}
		})
	}
}

func TestNodeCacheIntegerHistoryUsesCodecOwner(t *testing.T) {
	props := map[string]any{"revision": int(3), "nested": map[string]any{"revision": int(128)}}
	want := map[string]any{"revision": int64(3), "nested": map[string]any{"revision": int64(128)}}
	nodeBytes, err := encodeMVCCNodeRecord(&Node{ID: "test:numeric", Properties: props}, false)
	require.NoError(t, err)
	node, err := decodeMVCCNodeRecord(nodeBytes)
	require.NoError(t, err)
	require.Equal(t, want, node.Node.Properties)
	edgeBytes, err := encodeMVCCEdgeRecord(&Edge{ID: "test:numeric-edge", Properties: props}, false)
	require.NoError(t, err)
	edge, err := decodeMVCCEdgeRecord(edgeBytes)
	require.NoError(t, err)
	require.Equal(t, want, edge.Edge.Properties)
	nodeUndoBytes, err := encodeMVCCUndo(&mvccNodeUndo{Restore: props})
	require.NoError(t, err)
	nodeUndo, err := decodeMVCCNodeUndo(nodeUndoBytes)
	require.NoError(t, err)
	require.Equal(t, want, nodeUndo.Restore)
	edgeUndoBytes, err := encodeMVCCUndo(&mvccEdgeUndo{Restore: props})
	require.NoError(t, err)
	edgeUndo, err := decodeMVCCEdgeUndo(edgeUndoBytes)
	require.NoError(t, err)
	require.Equal(t, want, edgeUndo.Restore)
}
