package bolt

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestSelectBoltVersion(t *testing.T) {
	// Wire proposals are [0x00, back, minor, major]: a range covering
	// [minor-back, minor]. The reply must be one concrete version
	// [0x00, 0x00, minor, major] — never a range echo.
	wire := func(back, minor, major byte) uint32 {
		return uint32(back)<<16 | uint32(minor)<<8 | uint32(major)
	}
	t.Run("neo4j_go_driver_proposals", func(t *testing.T) {
		version, reply, ok := selectBoltVersion([]uint32{
			wire(0, 1, 0xFF), // manifest marker
			wire(8, 8, 5),    // 5.8 with back=8: range 5.0..5.8
			wire(2, 4, 4),    // 4.4 with back=2: range 4.2..4.4
			wire(0, 0, 3),    // 3.0
		})
		require.True(t, ok)
		require.Equal(t, uint32(BoltV5_0), version, "the 5.x range covers 5.0")
		require.Equal(t, wire(0, 0, 5), reply, "reply is the concrete selected version, back cleared")
	})
	t.Run("range_proposal_replies_concrete_version", func(t *testing.T) {
		// A Python-driver-style 4.4/back=2 offer selects 4.4 and replies
		// 00 00 04 04, not the echoed range 00 02 04 04.
		version, reply, ok := selectBoltVersion([]uint32{wire(2, 4, 4), wire(0, 0, 3), wire(0, 0, 2), wire(0, 0, 1)})
		require.True(t, ok)
		require.Equal(t, uint32(BoltV4_4), version)
		require.Equal(t, wire(0, 4, 4), reply)
	})
	t.Run("range_lower_bound_selected_when_higher_unsupported", func(t *testing.T) {
		// 4.4 with back=2 covers 4.2..4.4: all supported, so 4.4 wins.
		version, reply, ok := selectBoltVersion([]uint32{wire(2, 4, 4)})
		require.True(t, ok)
		require.Equal(t, uint32(BoltV4_4), version)
		require.Equal(t, wire(0, 4, 4), reply)
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
	t.Run("range_covering_50_selects_50", func(t *testing.T) {
		// 5.8 with back=8 covers 5.0..5.8: 5.0 is inside the range.
		version, reply, ok := selectBoltVersion([]uint32{wire(8, 8, 5)})
		require.True(t, ok)
		require.Equal(t, uint32(BoltV5_0), version)
		require.Equal(t, wire(0, 0, 5), reply)
	})
	t.Run("no_mutual_version", func(t *testing.T) {
		_, _, ok := selectBoltVersion([]uint32{wire(0, 4, 5), wire(0, 3, 5), wire(0, 2, 5), wire(0, 1, 5)})
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

	// Bolt 5.0 relationships: 8 fields (B8 52) — id, start, end, type,
	// properties, element_id, start_node_element_id, end_node_element_id.
	v5rel := encodeRecordListInto(nil, []any{edge}, true, true, "testdb")
	require.Equal(t, []byte{0xB8, 0x52}, v5rel[1:3], "5.0 relationship is an 8-field struct")
	require.Contains(t, string(v5rel), "5:testdb:edge-1")
	require.Contains(t, string(v5rel), "4:testdb:node-1")
	require.Contains(t, string(v5rel), "4:testdb:node-2")

	// Paths in 5.0 carry element ids on their nodes and unbound
	// relationships (B4 72: id, type, properties, element_id).
	node2 := &storage.Node{ID: "node-2", Labels: []string{"EID"}, Properties: map[string]any{}}
	pathNodes := []*storage.Node{node, node2}
	pathRels := []*storage.Edge{edge}
	v5path := encodePathV5Into(nil, pathNodes, pathRels, "testdb")
	require.Equal(t, []byte{0xB3, 0x50}, v5path[:2], "path stays a 3-field struct")
	require.Contains(t, string(v5path), "4:testdb:node-1")
	require.Contains(t, string(v5path), "4:testdb:node-2")
	// The path's relationship list starts after the nodes list; scan for
	// the unbound-relationship marker and its element id.
	require.Contains(t, string(v5path), "5:testdb:edge-1", "path relationships carry their element id (B4 72)")

	// Entities nested in maps and lists keep their Bolt 5.0 structures.
	nested := map[string]any{"n": node}
	v5nested := encodeRecordListInto(nil, []any{nested}, true, true, "testdb")
	require.Contains(t, string(v5nested), "4:testdb:node-1")
	require.NotContains(t, string(v5nested), string(byte(0xB3))+string(byte(0x4E))+"\x03", "nested node must not use the Bolt 4 3-field struct")

	deepList := []any{[]any{map[string]any{"e": edge}}}
	v5deep := encodeRecordListInto(nil, []any{deepList}, true, true, "testdb")
	require.Contains(t, string(v5deep), "5:testdb:edge-1")
	require.Contains(t, string(v5deep), "4:testdb:node-2")
}
