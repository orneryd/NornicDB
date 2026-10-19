package bolt

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestBoltEntityProvenance(t *testing.T) {
	for _, database := range []string{"one", "two"} {
		node := &storage.Node{ID: "same", Database: database}
		edge := &storage.Edge{ID: "rel", Database: database, StartNode: "same", EndNode: "target", Type: "R"}
		nodeWire := encodeRecordValueInto(nil, node, true, true, "comp")
		_, offset, err := decodePackStreamValue(nodeWire, 2)
		require.NoError(t, err)
		_, offset, err = decodePackStreamValue(nodeWire, offset)
		require.NoError(t, err)
		_, offset, err = decodePackStreamValue(nodeWire, offset)
		require.NoError(t, err)
		id, _, err := decodePackStreamValue(nodeWire, offset)
		require.NoError(t, err)
		require.Equal(t, "4:"+database+":same", id)
		edgeWire := encodeRecordValueInto(nil, edge, true, true, "comp")
		offset = 2
		for i := 0; i < 5; i++ {
			_, offset, err = decodePackStreamValue(edgeWire, offset)
			require.NoError(t, err)
		}
		for _, want := range []string{"5:"+database+":rel", "4:"+database+":same", "4:"+database+":target"} {
			id, offset, err = decodePackStreamValue(edgeWire, offset)
			require.NoError(t, err)
			require.Equal(t, want, id)
		}
	}
}
