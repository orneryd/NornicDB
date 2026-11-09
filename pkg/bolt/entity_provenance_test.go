package bolt

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestBoltEntityProvenance(t *testing.T) {
	for _, database := range []string{"one", "two"} {
		node := &storage.Node{ID: "same"}
		node.SetRuntimeMetadata(storage.NodeRuntimeMetadata{Database: database})
		edge := &storage.Edge{ID: "rel", Database: database, StartNode: "same", EndNode: "target", Type: "R"}
		nodeWire, encodeErr := encodeRecordValueInto(t.Context(), nil, node, true, true, "comp")
		require.NoError(t, encodeErr)
		offset := 2
		read := func(wire []byte) interface{} {
			value, used, err := decodePackStreamValue(wire, offset)
			require.NoError(t, err)
			offset += used
			return value
		}
		for i := 0; i < 3; i++ {
			read(nodeWire)
		}
		require.Equal(t, "4:"+database+":same", read(nodeWire))
		edgeWire, encodeErr := encodeRecordValueInto(t.Context(), nil, edge, true, true, "comp")
		require.NoError(t, encodeErr)
		offset = 2
		for i := 0; i < 5; i++ {
			read(edgeWire)
		}
		for _, want := range []string{"5:" + database + ":rel", "4:" + database + ":same", "4:" + database + ":target"} {
			require.Equal(t, want, read(edgeWire))
		}
	}
}
