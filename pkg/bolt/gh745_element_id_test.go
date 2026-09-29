package bolt

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestSelectBoltVersion(t *testing.T) {
	// Wire proposals are [0x00, back, minor, major].
	wire := func(back, minor, major byte) uint32 {
		return uint32(back)<<16 | uint32(minor)<<8 | uint32(major)
	}
	t.Run("neo4j_go_driver_proposals", func(t *testing.T) {
		version, reply, ok := selectBoltVersion([]uint32{
			wire(0, 1, 0xFF), // manifest marker
			wire(8, 8, 5),    // 5.8
			wire(2, 4, 4),    // 4.4
			wire(0, 0, 3),    // 3.0
		})
		require.True(t, ok)
		require.Equal(t, uint32(BoltV4_4), version)
		require.Equal(t, wire(2, 4, 4), reply)
	})
	t.Run("bolt_50_proposal", func(t *testing.T) {
		version, reply, ok := selectBoltVersion([]uint32{wire(0, 0, 5), wire(0, 4, 4), wire(0, 3, 4), wire(0, 2, 4)})
		require.True(t, ok)
		require.Equal(t, uint32(BoltV5_0), version)
		require.Equal(t, wire(0, 0, 5), reply)
	})
	t.Run("v4_older_only", func(t *testing.T) {
		version, _, ok := selectBoltVersion([]uint32{wire(0, 2, 4), wire(0, 1, 4), wire(0, 0, 4), wire(0, 0, 3)})
		require.True(t, ok)
		require.Equal(t, uint32(BoltV4_2), version)
	})
	t.Run("no_mutual_version", func(t *testing.T) {
		_, _, ok := selectBoltVersion([]uint32{wire(8, 8, 5), wire(8, 7, 5), wire(8, 6, 5), wire(8, 5, 5)})
		require.False(t, ok)
	})
}

func TestEncodeRecordV5ElementIDs(t *testing.T) {
	node := &storage.Node{ID: "node-1", Labels: []string{"EID"}, Properties: map[string]any{"k": int64(1)}}
	edge := &storage.Edge{ID: "edge-1", Type: "R", StartNode: "node-1", EndNode: "node-2", Properties: map[string]any{"w": int64(5)}}

	// Bolt 4.x: the three-field node structure keeps no element id (#745 §1).
	// Index 0 is the record list header (0x91 for one field); the struct
	// marker follows.
	v4 := encodeRecordListInto(nil, []any{node}, true, false, "testdb")
	require.Equal(t, byte(0x91), v4[0], "record list header for one field")
	require.Equal(t, byte(0xB3), v4[1], "4.x node is a 3-field struct")
	require.NotContains(t, string(v4), "4:testdb:node-1")

	// Bolt 5.0: the node is a 4-field struct ending in the element id.
	v5 := encodeRecordListInto(nil, []any{node}, true, true, "testdb")
	require.Equal(t, []byte{0xB4, 0x4E}, v5[1:3], "5.0 node is a 4-field struct")
	require.Contains(t, string(v5), "4:testdb:node-1")

	// Bolt 5.0 relationships: 6 fields, element id last.
	v5rel := encodeRecordListInto(nil, []any{edge}, true, true, "testdb")
	require.Equal(t, []byte{0xB6, 0x52}, v5rel[1:3], "5.0 relationship is a 6-field struct")
	require.Contains(t, string(v5rel), "5:testdb:edge-1")

	// Paths in 5.0 carry element ids on their nodes.
	node2 := &storage.Node{ID: "node-2", Labels: []string{"EID"}, Properties: map[string]any{}}
	pathNodes := []*storage.Node{node, node2}
	pathRels := []*storage.Edge{edge}
	v5path := encodePathV5Into(nil, pathNodes, pathRels, "testdb")
	require.Equal(t, []byte{0xB3, 0x50}, v5path[:2], "path stays a 3-field struct")
	require.Contains(t, string(v5path), "4:testdb:node-1")
	require.Contains(t, string(v5path), "4:testdb:node-2")
}
