package storage

// A filtered scan that repeats remembers the label's nodes once it has walked
// the whole label, also when its visitor stopped it early, and the next scans
// read that list (NOR-407). The label index stays the one owner of label
// membership: the list is good only until a commit writes or removes an entry
// of the label.

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRepeatedFilteredLabelScanRemembersTheLabel(t *testing.T) {
	const documents = 40
	engine, _ := filteredLabelScanFixture(t, BadgerOptions{}, documents)
	remembered := func() []NodeID {
		return engine.rememberedLabelNodes("a:", "Doc", visibleRead, math.MaxUint64)
	}
	// first is the scan EXISTS { MATCH (:Doc {id: $id}) } runs: it stops at
	// its first row.
	first := func(id string) *Node {
		t.Helper()
		var row *Node
		require.NoError(t, engine.StreamNodesWithOptions(context.Background(), StreamNodesOptions{
			Prefix:                   "a:",
			Label:                    "Doc",
			QueryNodes:               true,
			ApplyDecayFilter:         true,
			PropertyFilter:           func(properties map[string]interface{}) bool { return properties["id"] == id },
			PropertyFilterProjection: []string{"id"},
		}, func(node *Node) error {
			row = node
			return ErrIterationStopped
		}))
		return row
	}
	found := func(id string, node NodeID) {
		t.Helper()
		row := first(id)
		require.NotNil(t, row, id)
		require.Equal(t, node, row.ID)
	}

	// A first scan remembers the label alone; the scan that repeats walks the
	// rest of the label index after its visitor stopped.
	found("d3", "a:d3")
	require.Nil(t, remembered())
	found("d3", "a:d3")
	require.Len(t, remembered(), documents)
	for _, id := range []string{"d0", "d17", "d39"} {
		found(id, NodeID("a:"+id))
	}
	require.Nil(t, first("none"))
	require.Len(t, remembered(), documents)
	// A read from a view older than the walk does not use it.
	require.Nil(t, engine.rememberedLabelNodes("a:", "Doc", visibleRead, 0))

	// A write that leaves the label's index entries alone leaves the list.
	_, err := engine.CreateNode(t.Context(), &Node{ID: "a:other", Labels: []string{"Other"}, Properties: map[string]interface{}{"id": "d3"}})
	require.NoError(t, err)
	require.Len(t, remembered(), documents)
	found("d3", "a:d3")

	// A node of the label rewritten, with the label as it was: the label's
	// floor rises with every write to one of its nodes, so the next scan
	// walks the label again and finds the same nodes.
	require.NoError(t, engine.UpdateNode(t.Context(), NodeUpdateRequest{After: &Node{ID: "a:d5", Labels: []string{"Doc"}, Properties: map[string]interface{}{"id": "d5", "title": "rewritten"}}}))
	require.Nil(t, remembered())
	found("d5", "a:d5")
	require.Len(t, remembered(), documents)

	// A node created with the label: the list is the label's no longer, and
	// the next scan walks the label again.
	_, err = engine.CreateNode(t.Context(), &Node{ID: "a:new", Labels: []string{"Doc"}, Properties: map[string]interface{}{"id": "new", "title": "t"}})
	require.NoError(t, err)
	require.Nil(t, remembered())
	found("new", "a:new")
	require.Len(t, remembered(), documents+1)

	// A node of the label deleted.
	require.NoError(t, engine.DeleteNode(t.Context(), "a:d3"))
	require.Nil(t, remembered())
	require.Nil(t, first("d3"))
	require.Len(t, remembered(), documents)

	// A label removed, and a label added.
	require.NoError(t, engine.UpdateNode(t.Context(), NodeUpdateRequest{After: &Node{ID: "a:d17", Labels: []string{"Gone"}, Properties: map[string]interface{}{"id": "d17", "title": "t"}}}))
	require.Nil(t, remembered())
	require.Nil(t, first("d17"), "a node that lost the label")
	require.Len(t, remembered(), documents-1)
	require.NoError(t, engine.UpdateNode(t.Context(), NodeUpdateRequest{After: &Node{ID: "a:other", Labels: []string{"Other", "Doc"}, Properties: map[string]interface{}{"id": "added"}}}))
	require.Nil(t, remembered())
	found("added", "a:other")
	require.Len(t, remembered(), documents)

	// A transaction: its writes are no one else's until it commits, and a
	// rolled back transaction changes nothing.
	tx, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	_, err = tx.CreateNode(t.Context(), &Node{ID: "a:pending", Labels: []string{"Doc"}, Properties: map[string]interface{}{"id": "pending"}})
	require.NoError(t, err)
	require.Nil(t, first("pending"))
	require.Len(t, remembered(), documents)
	require.NoError(t, tx.Rollback())
	require.Nil(t, first("pending"))
	tx, err = engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	_, err = tx.CreateNode(t.Context(), &Node{ID: "a:committed", Labels: []string{"Doc"}, Properties: map[string]interface{}{"id": "committed"}})
	require.NoError(t, err)
	require.NoError(t, tx.DeleteGraphNode(t.Context(), "a:d39", CascadeEdges))
	found("d39", "a:d39")
	require.NoError(t, tx.Commit())
	require.Nil(t, remembered())
	found("committed", "a:committed")
	require.Nil(t, first("d39"))
	require.Len(t, remembered(), documents)

	// Another database has its own label, and dropping it leaves this one.
	_, err = engine.CreateNode(t.Context(), &Node{ID: "b:d0", Labels: []string{"Doc"}, Properties: map[string]interface{}{"id": "d0"}})
	require.NoError(t, err)
	found("d0", "a:d0")
	found("d0", "a:d0")
	_, _, err = engine.DeleteByPrefix("b:")
	require.NoError(t, err)
	require.Nil(t, remembered(), "a drop's effects are not known: every list goes")
	found("d0", "a:d0")
	found("d1", "a:d1")
	require.Len(t, remembered(), documents)
}
